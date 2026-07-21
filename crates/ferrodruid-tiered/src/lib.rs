// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Tiered storage (Hot/Warm/Cold/Frozen) for FerroDruid.
//!
//! Segments are assigned to tiers and can be promoted/demoted between them.
//! The hot tier uses local NVMe/SSD with mmap, warm uses local HDD or
//! compressed SSD, cold uses deep storage (S3) with an LRU block cache,
//! and frozen uses deep storage only with no local cache.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from tiered storage operations.
#[derive(Debug, Error)]
pub enum TieredStorageError {
    /// Segment not found in any tier.
    #[error("segment not found: {0}")]
    NotFound(String),
    /// Segment move failed.
    #[error("segment move failed: {0}")]
    MoveFailed(String),
    /// Invalid tier transition.
    #[error("invalid tier transition: {0}")]
    InvalidTransition(String),
    /// The required tier directory is not configured.
    #[error("tier not configured: {0:?}")]
    TierNotConfigured(StorageTier),
    /// I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// ---------------------------------------------------------------------------
// StorageTier
// ---------------------------------------------------------------------------

/// Storage tier identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    /// Hot tier — local NVMe/SSD, mmap'd, fastest queries.
    Hot,
    /// Warm tier — local HDD or compressed SSD, moderate latency.
    Warm,
    /// Cold tier — deep storage (S3), with LRU block cache.
    Cold,
    /// Frozen tier — deep storage only, no local cache.
    Frozen,
}

impl StorageTier {
    /// Return the numeric rank (lower = hotter).
    fn rank(self) -> u8 {
        match self {
            Self::Hot => 0,
            Self::Warm => 1,
            Self::Cold => 2,
            Self::Frozen => 3,
        }
    }
}

// ---------------------------------------------------------------------------
// TieredStorageManager
// ---------------------------------------------------------------------------

/// Manages segment placement across storage tiers.
///
/// The hot directory is always required. Warm and cold cache directories
/// are optional and enabled via builder methods.
pub struct TieredStorageManager {
    hot_dir: PathBuf,
    warm_dir: Option<PathBuf>,
    cold_cache_dir: Option<PathBuf>,
    cold_cache_max_bytes: u64,
    cold_cache_used: AtomicU64,
    segment_tiers: RwLock<HashMap<String, StorageTier>>,
}

impl TieredStorageManager {
    /// Create a new manager with only the hot tier directory.
    pub fn new(hot_dir: PathBuf) -> Self {
        Self {
            hot_dir,
            warm_dir: None,
            cold_cache_dir: None,
            cold_cache_max_bytes: 0,
            cold_cache_used: AtomicU64::new(0),
            segment_tiers: RwLock::new(HashMap::new()),
        }
    }

    /// Enable the warm tier with the given directory.
    pub fn with_warm(mut self, warm_dir: PathBuf) -> Self {
        self.warm_dir = Some(warm_dir);
        self
    }

    /// Enable the cold LRU cache with the given directory and byte limit.
    pub fn with_cold_cache(mut self, cache_dir: PathBuf, max_bytes: u64) -> Self {
        self.cold_cache_dir = Some(cache_dir);
        self.cold_cache_max_bytes = max_bytes;
        self
    }

    /// Place a segment in a specific tier.
    pub fn assign_tier(
        &self,
        segment_id: &str,
        tier: StorageTier,
    ) -> Result<(), TieredStorageError> {
        self.ensure_tier_configured(&tier)?;
        let mut tiers = self.segment_tiers.write().expect("lock poisoned");
        tiers.insert(segment_id.to_string(), tier);
        tracing::info!(segment_id, ?tier, "assigned segment to tier");
        Ok(())
    }

    /// Get the tier for a segment.
    pub fn get_tier(&self, segment_id: &str) -> Option<StorageTier> {
        let tiers = self.segment_tiers.read().expect("lock poisoned");
        tiers.get(segment_id).copied()
    }

    /// Get the local path for a segment.
    ///
    /// For hot/warm tiers, returns the directory path. For cold, returns
    /// the cache directory path (the caller is responsible for triggering
    /// download if the file is not present). For frozen, returns `None`
    /// (no local path).
    pub fn get_segment_path(&self, segment_id: &str) -> Option<PathBuf> {
        let tiers = self.segment_tiers.read().expect("lock poisoned");
        let tier = tiers.get(segment_id)?;
        match tier {
            StorageTier::Hot => Some(self.hot_dir.join(segment_id)),
            StorageTier::Warm => self.warm_dir.as_ref().map(|d| d.join(segment_id)),
            StorageTier::Cold => self.cold_cache_dir.as_ref().map(|d| d.join(segment_id)),
            StorageTier::Frozen => None,
        }
    }

    /// Evict a segment from its tier.
    pub fn evict(&self, segment_id: &str) -> Result<(), TieredStorageError> {
        let mut tiers = self.segment_tiers.write().expect("lock poisoned");
        if tiers.remove(segment_id).is_none() {
            return Err(TieredStorageError::NotFound(segment_id.to_string()));
        }
        tracing::info!(segment_id, "evicted segment");
        Ok(())
    }

    /// Return all segments currently assigned to the given tier.
    pub fn segments_in_tier(&self, tier: &StorageTier) -> Vec<String> {
        let tiers = self.segment_tiers.read().expect("lock poisoned");
        tiers
            .iter()
            .filter(|(_, t)| *t == tier)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return cold cache usage as (used_bytes, max_bytes).
    pub fn cold_cache_usage(&self) -> (u64, u64) {
        (
            self.cold_cache_used.load(Ordering::Relaxed),
            self.cold_cache_max_bytes,
        )
    }

    /// Promote a segment to a hotter tier.
    ///
    /// The target tier must be hotter (lower rank) than the current tier.
    pub fn promote(&self, segment_id: &str, target: StorageTier) -> Result<(), TieredStorageError> {
        self.ensure_tier_configured(&target)?;
        let mut tiers = self.segment_tiers.write().expect("lock poisoned");
        let current = tiers
            .get(segment_id)
            .ok_or_else(|| TieredStorageError::NotFound(segment_id.to_string()))?;

        if target.rank() >= current.rank() {
            return Err(TieredStorageError::InvalidTransition(format!(
                "promote requires a hotter tier: {current:?} -> {target:?}"
            )));
        }
        tiers.insert(segment_id.to_string(), target);
        tracing::info!(segment_id, ?target, "promoted segment");
        Ok(())
    }

    /// Demote a segment to a colder tier.
    ///
    /// The target tier must be colder (higher rank) than the current tier.
    pub fn demote(&self, segment_id: &str, target: StorageTier) -> Result<(), TieredStorageError> {
        self.ensure_tier_configured(&target)?;
        let mut tiers = self.segment_tiers.write().expect("lock poisoned");
        let current = tiers
            .get(segment_id)
            .ok_or_else(|| TieredStorageError::NotFound(segment_id.to_string()))?;

        if target.rank() <= current.rank() {
            return Err(TieredStorageError::InvalidTransition(format!(
                "demote requires a colder tier: {current:?} -> {target:?}"
            )));
        }
        tiers.insert(segment_id.to_string(), target);
        tracing::info!(segment_id, ?target, "demoted segment");
        Ok(())
    }

    /// Add bytes to the cold cache usage counter.
    pub fn add_cold_cache_usage(&self, bytes: u64) {
        self.cold_cache_used.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Subtract bytes from the cold cache usage counter.
    pub fn sub_cold_cache_usage(&self, bytes: u64) {
        self.cold_cache_used.fetch_sub(bytes, Ordering::Relaxed);
    }

    // -- internal helpers --

    fn ensure_tier_configured(&self, tier: &StorageTier) -> Result<(), TieredStorageError> {
        match tier {
            StorageTier::Hot => Ok(()), // always configured
            StorageTier::Warm => {
                if self.warm_dir.is_some() {
                    Ok(())
                } else {
                    Err(TieredStorageError::TierNotConfigured(StorageTier::Warm))
                }
            }
            StorageTier::Cold => {
                if self.cold_cache_dir.is_some() {
                    Ok(())
                } else {
                    Err(TieredStorageError::TierNotConfigured(StorageTier::Cold))
                }
            }
            StorageTier::Frozen => Ok(()), // no local dir needed
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn mgr_all_tiers() -> TieredStorageManager {
        TieredStorageManager::new(PathBuf::from("/data/hot"))
            .with_warm(PathBuf::from("/data/warm"))
            .with_cold_cache(PathBuf::from("/data/cold-cache"), 1_000_000)
    }

    #[test]
    fn assign_and_get_tier() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        assert_eq!(m.get_tier("seg-1"), Some(StorageTier::Hot));
        assert_eq!(m.get_tier("nonexistent"), None);
    }

    #[test]
    fn assign_tier_not_configured() {
        let m = TieredStorageManager::new(PathBuf::from("/data/hot"));
        let err = m
            .assign_tier("seg-1", StorageTier::Warm)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::TierNotConfigured(_)));
    }

    #[test]
    fn get_segment_path_hot() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        assert_eq!(
            m.get_segment_path("seg-1"),
            Some(Path::new("/data/hot/seg-1").to_path_buf())
        );
    }

    #[test]
    fn get_segment_path_warm() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-2", StorageTier::Warm).expect("assign");
        assert_eq!(
            m.get_segment_path("seg-2"),
            Some(Path::new("/data/warm/seg-2").to_path_buf())
        );
    }

    #[test]
    fn get_segment_path_cold() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-3", StorageTier::Cold).expect("assign");
        assert_eq!(
            m.get_segment_path("seg-3"),
            Some(Path::new("/data/cold-cache/seg-3").to_path_buf())
        );
    }

    #[test]
    fn get_segment_path_frozen_is_none() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-4", StorageTier::Frozen).expect("assign");
        assert_eq!(m.get_segment_path("seg-4"), None);
    }

    #[test]
    fn evict_segment() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        m.evict("seg-1").expect("evict");
        assert_eq!(m.get_tier("seg-1"), None);
    }

    #[test]
    fn evict_nonexistent_segment() {
        let m = mgr_all_tiers();
        let err = m.evict("nope").expect_err("should fail");
        assert!(matches!(err, TieredStorageError::NotFound(_)));
    }

    #[test]
    fn segments_in_tier() {
        let m = mgr_all_tiers();
        m.assign_tier("a", StorageTier::Hot).expect("assign");
        m.assign_tier("b", StorageTier::Hot).expect("assign");
        m.assign_tier("c", StorageTier::Warm).expect("assign");

        let mut hot = m.segments_in_tier(&StorageTier::Hot);
        hot.sort();
        assert_eq!(hot, vec!["a", "b"]);
        assert_eq!(m.segments_in_tier(&StorageTier::Warm), vec!["c"]);
        assert!(m.segments_in_tier(&StorageTier::Cold).is_empty());
    }

    #[test]
    fn cold_cache_usage_tracking() {
        let m = mgr_all_tiers();
        assert_eq!(m.cold_cache_usage(), (0, 1_000_000));

        m.add_cold_cache_usage(500);
        assert_eq!(m.cold_cache_usage(), (500, 1_000_000));

        m.add_cold_cache_usage(300);
        assert_eq!(m.cold_cache_usage(), (800, 1_000_000));

        m.sub_cold_cache_usage(200);
        assert_eq!(m.cold_cache_usage(), (600, 1_000_000));
    }

    #[test]
    fn promote_cold_to_hot() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Cold).expect("assign");
        m.promote("seg-1", StorageTier::Hot).expect("promote");
        assert_eq!(m.get_tier("seg-1"), Some(StorageTier::Hot));
    }

    #[test]
    fn promote_warm_to_hot() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Warm).expect("assign");
        m.promote("seg-1", StorageTier::Hot).expect("promote");
        assert_eq!(m.get_tier("seg-1"), Some(StorageTier::Hot));
    }

    #[test]
    fn promote_to_same_tier_fails() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        let err = m
            .promote("seg-1", StorageTier::Hot)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::InvalidTransition(_)));
    }

    #[test]
    fn promote_to_colder_tier_fails() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        let err = m
            .promote("seg-1", StorageTier::Warm)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::InvalidTransition(_)));
    }

    #[test]
    fn demote_hot_to_cold() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Hot).expect("assign");
        m.demote("seg-1", StorageTier::Cold).expect("demote");
        assert_eq!(m.get_tier("seg-1"), Some(StorageTier::Cold));
    }

    #[test]
    fn demote_to_hotter_tier_fails() {
        let m = mgr_all_tiers();
        m.assign_tier("seg-1", StorageTier::Cold).expect("assign");
        let err = m
            .demote("seg-1", StorageTier::Hot)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::InvalidTransition(_)));
    }

    #[test]
    fn promote_nonexistent_segment() {
        let m = mgr_all_tiers();
        let err = m
            .promote("nope", StorageTier::Hot)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::NotFound(_)));
    }

    #[test]
    fn demote_nonexistent_segment() {
        let m = mgr_all_tiers();
        let err = m
            .demote("nope", StorageTier::Cold)
            .expect_err("should fail");
        assert!(matches!(err, TieredStorageError::NotFound(_)));
    }

    #[test]
    fn storage_tier_serde_roundtrip() {
        let tier = StorageTier::Cold;
        let json = serde_json::to_string(&tier).expect("serialize");
        assert_eq!(json, "\"cold\"");
        let parsed: StorageTier = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, StorageTier::Cold);
    }

    #[test]
    fn frozen_tier_needs_no_config() {
        // Frozen can always be assigned even without any optional dirs.
        let m = TieredStorageManager::new(PathBuf::from("/data/hot"));
        m.assign_tier("seg-1", StorageTier::Frozen)
            .expect("assign frozen");
        assert_eq!(m.get_tier("seg-1"), Some(StorageTier::Frozen));
    }
}
