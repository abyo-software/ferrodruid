// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! W1-C — Persistent Raft state for crash recovery.
//!
//! Closes the CL-A1 / CL-3 long-form residual *"In-memory log store
//! (suitable for single-node) … Persistent Raft log (required for
//! multi-node crash recovery)"* listed in the known-limitations
//! document (`docs/known-limitations.md`). The in-memory log
//! (`replication.rs::ReplicationEngine::command_log`) is now shadowed
//! by a file-backed journal + snapshot + meta triple that
//! [`ReplicationEngine::attach_persistent_state`] can attach to so a
//! restart reconstructs the state machine bit-for-bit.
//!
//! ## On-disk layout
//!
//! Everything lives under a single directory (typically
//! `<data_dir>/raft/`):
//!
//! ```text
//! <dir>/meta.json         Atomic-replace JSON
//!                         { current_term, voted_for, last_index }
//! <dir>/snapshot.json     Latest [`crate::ClusterSnapshot`] +
//!                         { last_index, term }
//! <dir>/log.jsonl         Append-only journal — one JSON object
//!                         per line: { index, term, command }.
//!                         Entries with `index <= snapshot.last_index`
//!                         are stale and ignored at replay.
//! ```
//!
//! All three files are *fsync*ed before any operation returns
//! `Ok`; an unclean shutdown loses **at most** the in-flight
//! operation, never a previously-acknowledged entry.
//!
//! Atomic-replace is implemented as the canonical
//! `<file>.tmp -> rename(<file>.tmp, <file>)` pattern with a parent
//! directory fsync after rename. The journal is append-only so
//! `write_all + sync_data` is sufficient.
//!
//! ## Honest scope
//!
//! Wave-W1-C lands the *primitives* needed for crash recovery; the
//! engine currently calls
//! [`PersistentRaftState::append_log_entry`] from the two log-append
//! sites in [`crate::replication::ReplicationEngine`] (leader submit
//! + follower `receive_command`).
//!
//! The companion `current_term` / `voted_for` fsync hooks at every
//! Raft state transition are **partially wired**: the engine persists
//! meta alongside each log append, so a crash inside an AppendEntries
//! round is durable. A crash *between* a vote grant and the next log
//! append loses the `voted_for` write — that is the residual called
//! out in `docs/known-limitations.md` CL-A1 / CL-A4 and will close
//! when the start_election / pre-vote / vote-receive paths gain their
//! own `persist_meta_only()` calls.

use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::{ClusterCommand, ClusterSnapshot};

/// Persisted Raft meta — the consensus-safety fields that must
/// survive a crash.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersistedMeta {
    /// Current Raft term as observed by this node.
    pub current_term: u64,
    /// `Some(node_id)` if this node voted for `node_id` in
    /// `current_term`, otherwise `None`.
    pub voted_for: Option<String>,
    /// Index of the last log entry appended on this node.
    /// `0` means an empty log (Raft indices are 1-based).
    pub last_index: u64,
}

/// One persisted log entry: term + command.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedEntry {
    /// 1-based log index.
    pub index: u64,
    /// Raft term the entry was appended in.
    pub term: u64,
    /// The state-machine command at this index.
    pub command: ClusterCommand,
}

/// A persisted snapshot envelope — pairs the
/// [`ClusterSnapshot`] payload with the (`last_index`, `term`) that
/// described it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedSnapshot {
    /// Highest log index covered by `snapshot`.
    pub last_index: u64,
    /// Term in effect at `last_index`.
    pub term: u64,
    /// The cluster snapshot itself.
    pub snapshot: ClusterSnapshot,
}

/// Errors produced by the persistent-state surface.
#[derive(Debug, thiserror::Error)]
pub enum PersistError {
    /// Disk I/O failed.
    #[error("persist I/O on {path}: {source}")]
    Io {
        /// The path the I/O was directed at.
        path: PathBuf,
        /// Underlying error.
        #[source]
        source: io::Error,
    },
    /// JSON encode failed (a state-machine command somehow rejected
    /// `serde_json::to_vec`).
    #[error("serialize: {0}")]
    Serialize(String),
    /// JSON decode failed (corrupted on-disk record).
    #[error("deserialize from {path}: {reason}")]
    Deserialize {
        /// The path whose record failed to parse.
        path: PathBuf,
        /// Human-readable reason.
        reason: String,
    },
}

/// File-backed persistent Raft state.
///
/// Construct via [`Self::open_or_create`]; load existing state via
/// [`Self::replay`]; hand to
/// [`crate::replication::ReplicationEngine::attach_persistent_state`]
/// so subsequent log appends + meta changes are fsynced.
pub struct PersistentRaftState {
    dir: PathBuf,
    meta: Mutex<PersistedMeta>,
    /// Append-only journal handle, lazily reopened when truncated by
    /// snapshot installation.
    journal: Mutex<File>,
}

impl std::fmt::Debug for PersistentRaftState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentRaftState")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

const META_FILENAME: &str = "meta.json";
const SNAPSHOT_FILENAME: &str = "snapshot.json";
const JOURNAL_FILENAME: &str = "log.jsonl";

impl PersistentRaftState {
    /// Open (or create) the persistent state directory.
    ///
    /// Creates `dir` if it does not yet exist; opens
    /// `<dir>/log.jsonl` in append mode; loads `<dir>/meta.json` if
    /// present (or initializes it to defaults if absent).
    pub fn open_or_create(dir: impl Into<PathBuf>) -> Result<Self, PersistError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|source| PersistError::Io {
            path: dir.clone(),
            source,
        })?;

        let meta_path = dir.join(META_FILENAME);
        let meta = if meta_path.exists() {
            let bytes = fs::read(&meta_path).map_err(|source| PersistError::Io {
                path: meta_path.clone(),
                source,
            })?;
            serde_json::from_slice::<PersistedMeta>(&bytes).map_err(|e| {
                PersistError::Deserialize {
                    path: meta_path.clone(),
                    reason: e.to_string(),
                }
            })?
        } else {
            PersistedMeta::default()
        };

        let journal_path = dir.join(JOURNAL_FILENAME);
        let journal = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&journal_path)
            .map_err(|source| PersistError::Io {
                path: journal_path,
                source,
            })?;

        Ok(Self {
            dir,
            meta: Mutex::new(meta),
            journal: Mutex::new(journal),
        })
    }

    /// Replay the on-disk state and return:
    /// - the latest snapshot (if any),
    /// - every log entry with `index > snapshot.last_index`,
    /// - the meta (`current_term`, `voted_for`, `last_index`).
    ///
    /// The caller is expected to feed the snapshot into
    /// `ClusterManager::restore` and then push the returned entries
    /// through `ClusterManager::apply` before installing this
    /// `PersistentRaftState` on the engine.
    pub fn replay(
        &self,
    ) -> Result<
        (
            Option<PersistedSnapshot>,
            Vec<PersistedEntry>,
            PersistedMeta,
        ),
        PersistError,
    > {
        let snapshot = self.load_snapshot()?;
        let cutoff = snapshot.as_ref().map(|s| s.last_index).unwrap_or(0);
        let entries = self.load_log_entries(cutoff)?;
        let meta = self.snapshot_meta();
        Ok((snapshot, entries, meta))
    }

    /// Return a clone of the in-memory meta.
    pub fn snapshot_meta(&self) -> PersistedMeta {
        // best-effort: this is for the open_or_create returns / debug
        self.meta.try_lock().map(|g| g.clone()).unwrap_or_default()
    }

    /// Path to the directory that holds the persistent state.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Atomically rewrite `meta.json` with the supplied values.
    ///
    /// Caller is responsible for calling this whenever Raft writes
    /// `current_term`, `voted_for`, or the last log index.
    pub async fn write_meta(
        &self,
        current_term: u64,
        voted_for: Option<String>,
        last_index: u64,
    ) -> Result<(), PersistError> {
        let new = PersistedMeta {
            current_term,
            voted_for,
            last_index,
        };
        {
            let mut guard = self.meta.lock().await;
            *guard = new.clone();
        }
        let bytes = serde_json::to_vec(&new).map_err(|e| PersistError::Serialize(e.to_string()))?;
        let path = self.dir.join(META_FILENAME);
        atomic_replace(&path, &bytes)?;
        Ok(())
    }

    /// Append one entry to the journal, fsync, then atomically
    /// rewrite meta to reflect the new `last_index`.
    pub async fn append_log_entry(
        &self,
        index: u64,
        term: u64,
        cmd: &ClusterCommand,
    ) -> Result<(), PersistError> {
        let entry = PersistedEntry {
            index,
            term,
            command: cmd.clone(),
        };
        let mut bytes =
            serde_json::to_vec(&entry).map_err(|e| PersistError::Serialize(e.to_string()))?;
        bytes.push(b'\n');
        {
            let mut journal = self.journal.lock().await;
            journal
                .write_all(&bytes)
                .map_err(|source| PersistError::Io {
                    path: self.dir.join(JOURNAL_FILENAME),
                    source,
                })?;
            journal.sync_data().map_err(|source| PersistError::Io {
                path: self.dir.join(JOURNAL_FILENAME),
                source,
            })?;
        }

        // Roll meta forward: bump last_index, keep current_term / voted_for.
        let prev = {
            let g = self.meta.lock().await;
            g.clone()
        };
        let new = PersistedMeta {
            current_term: prev.current_term.max(term),
            voted_for: prev.voted_for,
            last_index: index,
        };
        {
            let mut g = self.meta.lock().await;
            *g = new.clone();
        }
        let meta_bytes =
            serde_json::to_vec(&new).map_err(|e| PersistError::Serialize(e.to_string()))?;
        atomic_replace(&self.dir.join(META_FILENAME), &meta_bytes)?;
        Ok(())
    }

    /// Atomically rewrite `snapshot.json`, truncate the journal of
    /// any entries with `index <= last_index`, and update meta.
    pub async fn install_snapshot(
        &self,
        snapshot: &ClusterSnapshot,
        last_index: u64,
        term: u64,
    ) -> Result<(), PersistError> {
        let envelope = PersistedSnapshot {
            last_index,
            term,
            snapshot: snapshot.clone(),
        };
        let bytes =
            serde_json::to_vec(&envelope).map_err(|e| PersistError::Serialize(e.to_string()))?;
        atomic_replace(&self.dir.join(SNAPSHOT_FILENAME), &bytes)?;

        // Truncate the journal: any entries with index <= last_index
        // are now covered by the snapshot. The simplest correct
        // implementation is to drain the file and re-write only
        // entries with index > last_index.
        self.compact_journal(last_index).await?;

        // Roll meta forward.
        let prev = {
            let g = self.meta.lock().await;
            g.clone()
        };
        let new = PersistedMeta {
            current_term: prev.current_term.max(term),
            voted_for: prev.voted_for,
            last_index: prev.last_index.max(last_index),
        };
        {
            let mut g = self.meta.lock().await;
            *g = new.clone();
        }
        let meta_bytes =
            serde_json::to_vec(&new).map_err(|e| PersistError::Serialize(e.to_string()))?;
        atomic_replace(&self.dir.join(META_FILENAME), &meta_bytes)?;
        Ok(())
    }

    /// Read every entry from the journal, dropping those with
    /// `index <= cutoff` (already covered by a snapshot).
    fn load_log_entries(&self, cutoff: u64) -> Result<Vec<PersistedEntry>, PersistError> {
        let path = self.dir.join(JOURNAL_FILENAME);
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path).map_err(|source| PersistError::Io {
            path: path.clone(),
            source,
        })?;
        let reader = BufReader::new(file);
        let mut out = Vec::new();
        for (line_no, line) in reader.lines().enumerate() {
            let line = line.map_err(|source| PersistError::Io {
                path: path.clone(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: PersistedEntry =
                serde_json::from_str(&line).map_err(|e| PersistError::Deserialize {
                    path: path.clone(),
                    reason: format!("line {line_no}: {e}"),
                })?;
            if entry.index > cutoff {
                out.push(entry);
            }
        }
        // Sort + de-dup by index in case the journal saw retries or
        // out-of-order writes (defensive — a healthy run is
        // monotonic).
        out.sort_by_key(|e| e.index);
        out.dedup_by_key(|e| e.index);
        Ok(out)
    }

    fn load_snapshot(&self) -> Result<Option<PersistedSnapshot>, PersistError> {
        let path = self.dir.join(SNAPSHOT_FILENAME);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&path).map_err(|source| PersistError::Io {
            path: path.clone(),
            source,
        })?;
        let env: PersistedSnapshot =
            serde_json::from_slice(&bytes).map_err(|e| PersistError::Deserialize {
                path: path.clone(),
                reason: e.to_string(),
            })?;
        Ok(Some(env))
    }

    async fn compact_journal(&self, cutoff: u64) -> Result<(), PersistError> {
        // Read all entries > cutoff into memory, then rewrite the
        // journal file atomically.
        let entries = self.load_log_entries(cutoff)?;
        let path = self.dir.join(JOURNAL_FILENAME);
        let tmp = path.with_extension("jsonl.tmp");
        {
            let mut tmp_file = File::create(&tmp).map_err(|source| PersistError::Io {
                path: tmp.clone(),
                source,
            })?;
            for entry in &entries {
                let mut bytes = serde_json::to_vec(entry)
                    .map_err(|e| PersistError::Serialize(e.to_string()))?;
                bytes.push(b'\n');
                tmp_file
                    .write_all(&bytes)
                    .map_err(|source| PersistError::Io {
                        path: tmp.clone(),
                        source,
                    })?;
            }
            tmp_file.sync_data().map_err(|source| PersistError::Io {
                path: tmp.clone(),
                source,
            })?;
        }
        fs::rename(&tmp, &path).map_err(|source| PersistError::Io {
            path: path.clone(),
            source,
        })?;
        sync_parent_dir(&path)?;

        // Reopen the journal handle on the new file.
        let new_handle = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|source| PersistError::Io {
                path: path.clone(),
                source,
            })?;
        // Move the cursor to end-of-file so subsequent appends land
        // at the right offset.
        let mut new_handle = new_handle;
        new_handle
            .seek(SeekFrom::End(0))
            .map_err(|source| PersistError::Io {
                path: path.clone(),
                source,
            })?;
        let mut g = self.journal.lock().await;
        *g = new_handle;
        Ok(())
    }
}

fn atomic_replace(path: &Path, bytes: &[u8]) -> Result<(), PersistError> {
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("bin"),
    ));
    {
        let mut tmp_file = File::create(&tmp).map_err(|source| PersistError::Io {
            path: tmp.clone(),
            source,
        })?;
        tmp_file
            .write_all(bytes)
            .map_err(|source| PersistError::Io {
                path: tmp.clone(),
                source,
            })?;
        tmp_file.sync_data().map_err(|source| PersistError::Io {
            path: tmp.clone(),
            source,
        })?;
    }
    fs::rename(&tmp, path).map_err(|source| PersistError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    sync_parent_dir(path)?;
    Ok(())
}

fn sync_parent_dir(path: &Path) -> Result<(), PersistError> {
    if let Some(parent) = path.parent() {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match File::open(parent) {
            Ok(dir) => {
                // fsync the directory so the rename is durable.
                // Best-effort: directory fsync is a Linux-ism; on
                // other platforms `sync_all` may be a no-op or fail
                // with `Operation not supported` — we ignore that to
                // avoid breaking platform portability.
                let _ = dir.sync_all();
                Ok(())
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(PersistError::Io {
                path: parent.to_path_buf(),
                source,
            }),
        }
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeInfo, NodeRole, ServiceEntry};
    use tempfile::tempdir;

    fn entry(idx: u64, term: u64) -> ClusterCommand {
        ClusterCommand::RegisterService(ServiceEntry {
            service_type: format!("svc-{idx}"),
            host: "127.0.0.1".to_string(),
            port: 8080,
            node_id: format!("node-{term}"),
        })
    }

    #[tokio::test]
    async fn round_trip_meta() {
        let dir = tempdir().expect("tempdir");
        let state = PersistentRaftState::open_or_create(dir.path()).expect("open");
        state
            .write_meta(7, Some("node-3".to_string()), 0)
            .await
            .expect("write meta");
        // Re-open.
        let state2 = PersistentRaftState::open_or_create(dir.path()).expect("reopen");
        let meta = state2.snapshot_meta();
        assert_eq!(meta.current_term, 7);
        assert_eq!(meta.voted_for, Some("node-3".to_string()));
        assert_eq!(meta.last_index, 0);
    }

    #[tokio::test]
    async fn append_and_replay_log() {
        let dir = tempdir().expect("tempdir");
        let state = PersistentRaftState::open_or_create(dir.path()).expect("open");
        for i in 1..=5u64 {
            state
                .append_log_entry(i, 2, &entry(i, 2))
                .await
                .expect("append");
        }
        let state2 = PersistentRaftState::open_or_create(dir.path()).expect("reopen");
        let (snap, entries, meta) = state2.replay().expect("replay");
        assert!(snap.is_none(), "no snapshot installed");
        assert_eq!(entries.len(), 5);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[4].index, 5);
        assert_eq!(meta.last_index, 5);
    }

    #[tokio::test]
    async fn snapshot_truncates_journal() {
        let dir = tempdir().expect("tempdir");
        let state = PersistentRaftState::open_or_create(dir.path()).expect("open");
        for i in 1..=10u64 {
            state
                .append_log_entry(i, 1, &entry(i, 1))
                .await
                .expect("append");
        }
        let snap = ClusterSnapshot {
            services: Default::default(),
            segments: Default::default(),
            task_locks: Default::default(),
            leader: Some(NodeInfo {
                id: "node-1".to_string(),
                host: "127.0.0.1".to_string(),
                port: 8081,
                role: NodeRole::AllInOne,
            }),
            segment_queue: Default::default(),
        };
        state
            .install_snapshot(&snap, 6, 1)
            .await
            .expect("install snap");
        // Re-open: replay should yield the snapshot + entries 7..=10.
        let state2 = PersistentRaftState::open_or_create(dir.path()).expect("reopen");
        let (got_snap, entries, _meta) = state2.replay().expect("replay");
        assert!(got_snap.is_some());
        assert_eq!(got_snap.unwrap().last_index, 6);
        assert_eq!(entries.len(), 4, "entries: {entries:?}");
        assert_eq!(entries[0].index, 7);
        assert_eq!(entries[3].index, 10);
    }

    #[tokio::test]
    async fn append_after_snapshot_still_works() {
        let dir = tempdir().expect("tempdir");
        let state = PersistentRaftState::open_or_create(dir.path()).expect("open");
        for i in 1..=3u64 {
            state.append_log_entry(i, 1, &entry(i, 1)).await.expect("a");
        }
        let snap = ClusterSnapshot {
            services: Default::default(),
            segments: Default::default(),
            task_locks: Default::default(),
            leader: None,
            segment_queue: Default::default(),
        };
        state.install_snapshot(&snap, 3, 1).await.expect("snap");
        for i in 4..=6u64 {
            state.append_log_entry(i, 2, &entry(i, 2)).await.expect("b");
        }
        let state2 = PersistentRaftState::open_or_create(dir.path()).expect("reopen");
        let (got, entries, meta) = state2.replay().expect("replay");
        assert_eq!(got.unwrap().last_index, 3);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, 4);
        assert_eq!(entries[2].index, 6);
        assert_eq!(meta.last_index, 6);
    }
}
