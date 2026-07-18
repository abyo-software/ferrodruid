// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Theta sketch for set-operation cardinality estimation.
//!
//! A Theta sketch retains a set of hash values below a dynamically adjusted
//! threshold *theta*.  Because theta is shared across sketches, set operations
//! (union, intersection, difference) can be performed directly on the retained
//! hash sets.
//!
//! # Hash spaces (native vs Druid-origin)
//!
//! A natively built sketch ([`ThetaSketch::add`]) hashes values with FNV-1a
//! and normalises hashes over the full `u64` range.  A sketch decoded from an
//! Apache DataSketches compact image ([`ThetaSketch::from_druid_compact`],
//! the on-disk form of a Druid `thetaSketch` metric) instead carries
//! MurmurHash3-space hashes.  The two spaces hash the SAME logical value to
//! DIFFERENT points, so mixing them in one sketch would silently double-count
//! (or drop) values.  Druid-origin sketches are therefore **union-only**:
//! they refuse raw adds, and set operations refuse to combine a non-empty
//! Druid-origin sketch with a non-empty native one (see
//! [`SketchError::HashSpaceMismatch`]).  Unioning Druid-origin sketches
//! among themselves is exact-correct when they share the SAME update seed
//! (same space); each decoded sketch therefore also carries the compact
//! preamble's 16-bit seed hash, and set operations refuse to combine
//! non-empty Druid-origin sketches with different seed hashes (different
//! seeds place the same logical value at different MurmurHash3 points, so
//! a cross-seed union would double-count).  The cardinality estimate
//! `retained / theta` is hash-function-independent.

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeSet;
use std::io::Cursor;

use crate::{Result, SketchError};

/// Serialization format version for native-origin sketches (the historical
/// layout — emitted byte-for-byte unchanged for every non-Druid sketch).
const VERSION: u8 = 1;

/// Serialization format version carrying an origin-flags byte (emitted only
/// for Druid-origin sketches, so pre-existing serialized bytes never change).
const VERSION_DRUID: u8 = 2;

/// Bit 0 of the version-2 origin-flags byte: the sketch is Druid-origin
/// (MurmurHash3 hash space, union-only).  All other bits except
/// [`ORIGIN_FLAG_SEED_HASH`] are reserved and must be zero.
const ORIGIN_FLAG_DRUID: u8 = 0x01;

/// Bit 1 of the version-2 origin-flags byte: a 16-bit DataSketches seed
/// hash (little-endian) follows the exact `theta_bound`.  Emitted whenever
/// the sketch carries a known seed hash — which every NON-empty
/// Druid-origin sketch does (decode captured it), so the bit may be absent
/// only on a genuinely EMPTY (seed-neutral) image; a non-empty v2 image
/// without it is rejected on deserialize (a non-empty sketch is never
/// seed-neutral — treating it as such would let cross-seed sketches union
/// silently and double-count).
const ORIGIN_FLAG_SEED_HASH: u8 = 0x02;

/// Default maximum retained-hash size (also used for decoded degenerate
/// Druid sketches whose preamble carries no nominal size).
const DEFAULT_MAX_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Apache DataSketches compact-theta constants (decoded directly from the
// public serialized format — no datasketches dependency).
// ---------------------------------------------------------------------------

/// Serial version of the DataSketches theta family (byte 1).
const DS_SERIAL_VERSION: u8 = 3;
/// Family id of a compact theta sketch (byte 2).
const DS_FAMILY_COMPACT: u8 = 3;
/// Flags byte (byte 5): the sketch is empty.
const DS_FLAG_EMPTY: u8 = 1 << 2;
/// Flags byte (byte 5): the retained hashes are stored in strictly
/// ascending order (every COMPACT+ORDERED image the DataSketches library
/// writes — and Druid segments carry — sets this).
const DS_FLAG_ORDERED: u8 = 1 << 4;
/// Flags byte (byte 5): the image holds exactly one retained hash.
const DS_FLAG_SINGLE_ITEM: u8 = 1 << 5;
/// Largest lgNomLongs (lgK) the DataSketches theta family supports.
const DS_MAX_LG_NOM_LONGS: u8 = 26;
/// Hard cap on the retained-hash count of a decoded compact image
/// (2^26 — the largest legal nominal size; a bigger declared count can
/// never be legitimate and must not size an allocation).  Also the upper
/// bound [`ThetaSketch::new`] clamps a requested retention budget to: a
/// bigger budget can never be legitimate either, and an unclamped
/// `usize` budget used to truncate through the `u32` wire field (2^32
/// silently became 0, trimming EVERY retained hash after a round-trip).
const DS_MAX_RETAINED: usize = 1 << 26;
/// 2^63 as `f64` — the denominator of the DataSketches theta fraction
/// (`thetaLong` is a fraction of `Long.MAX_VALUE`; `Long.MAX_VALUE as
/// double` rounds to exactly 2^63, so `thetaLong == Long.MAX_VALUE`
/// yields exactly `1.0` here).
const DS_THETA_DENOMINATOR: f64 = 9_223_372_036_854_775_808.0;
/// 2^64 as `f64` — the denominator relating a Druid-origin sketch's
/// STORED-space integer retention threshold to its f64 theta.  Every
/// constructor keeps the pair coherent as `theta == theta_bound / 2^64`
/// BIT-EXACTLY: the decode sets `theta = thetaLong / 2^63` with
/// `theta_bound = thetaLong << 1` (a power-of-two shift preserves the f64
/// rounding, so the two quotients are identical), [`ThetaSketch::trim`]
/// sets `theta = normalise(largest)` with `theta_bound = largest` (and
/// `u64::MAX as f64` rounds to exactly 2^64, so [`normalise`] divides by
/// this same denominator), and the neutral empty bound `u64::MAX` maps to
/// exactly 1.0.  The v2 wire theta is therefore REDUNDANT and is
/// validated against this relationship on deserialize (see
/// [`druid_theta_from_bound`]).
const STORED_THETA_DENOMINATOR: f64 = 18_446_744_073_709_551_616.0;

/// Theta sketch for set-operation cardinality estimation.
#[derive(Debug, Clone)]
pub struct ThetaSketch {
    /// Threshold in `(0.0, 1.0]`.  Only hashes whose normalised value is
    /// strictly below theta are retained.
    theta: f64,
    /// Exclusive integer retention threshold in the STORED (`u64`) hash
    /// space — the exact integer image of `theta` under the same rounding
    /// used when the hashes were retained.  For a Druid-origin sketch this
    /// is `thetaLong << 1` (stored hashes are `h << 1`), so
    /// retained-vs-threshold comparisons stay integer-exact: at the 63-bit
    /// boundary `thetaLong = i64::MAX` and a retained `h = i64::MAX - 1`
    /// BOTH round to `1.0` in f64, and an f64 comparison would drop a hash
    /// the decoder retained (cardinality 1 silently became 0).  The v2
    /// partial-state wire carries this EXACT integer (reconstructing it
    /// from the f64 theta widened it, ADMITTING hashes the decoder had
    /// excluded).  Native (FNV) sketches keep their historical f64
    /// comparisons — their retention decision itself is the f64 one — and
    /// carry the neutral `u64::MAX` here.
    theta_bound: u64,
    /// Maximum number of retained hashes before theta is lowered.
    max_size: usize,
    /// Retained hash values (sorted via `BTreeSet`).
    hashes: BTreeSet<u64>,
    /// Whether the retained hashes live in the Apache DataSketches
    /// (MurmurHash3) hash space rather than FerroDruid's native FNV space
    /// (see the module docs).  Druid-origin sketches are union-only.
    druid_origin: bool,
    /// The 16-bit DataSketches `seedHash` (a hash of the MurmurHash3
    /// update seed) captured from preamble bytes 6-7 of a decoded compact
    /// image.  Two sketches built with DIFFERENT update seeds hash the
    /// same logical value to DIFFERENT points, so unioning them
    /// double-counts — set operations therefore require matching seed
    /// hashes on non-empty Druid-origin sides (a mismatch gets the
    /// [`SketchError::HashSpaceMismatch`] treatment).  `None` ONLY for
    /// native sketches and for EMPTY (seed-neutral) Druid-origin sketches
    /// such as [`ThetaSketch::empty_druid_origin`]: every non-empty
    /// Druid-origin sketch carries the seed its decode captured, the v2
    /// wire always transports it, and [`ThetaSketch::deserialize`] rejects
    /// a non-empty Druid-origin image without one (a seed-less non-empty
    /// sketch would union with ANY seed and silently double-count).
    druid_seed_hash: Option<u16>,
}

impl ThetaSketch {
    /// Create a new Theta sketch with the given maximum retained-hash size.
    ///
    /// `max_size` is clamped to the theta family's largest nominal size
    /// (2^26, [`DS_MAX_RETAINED`]): a bigger retention budget can never be
    /// legitimate, and an unclamped budget silently truncated through the
    /// serialized `u32` field — a query-controlled `size = 2^32` became 0
    /// on the wire, and the 0 budget then trimmed every retained hash on
    /// merge (cardinality collapsed to 0).
    pub fn new(max_size: usize) -> Self {
        Self {
            theta: 1.0,
            theta_bound: u64::MAX,
            max_size: max_size.min(DS_MAX_RETAINED),
            hashes: BTreeSet::new(),
            druid_origin: false,
            druid_seed_hash: None,
        }
    }

    /// Create a new Theta sketch with the default size of 4096.
    pub fn default_size() -> Self {
        Self::new(DEFAULT_MAX_SIZE)
    }

    /// An EMPTY Druid-origin sketch: estimates 0, unions as a no-op, and —
    /// like every Druid-origin sketch — refuses raw adds.  Used to
    /// represent null/absent rows of a migrated `thetaSketch` column so the
    /// whole column stays uniformly union-only.
    #[must_use]
    pub fn empty_druid_origin() -> Self {
        Self {
            theta: 1.0,
            // Neutral: an empty sketch must never lower a union's
            // integer threshold (mirrors its theta of 1.0).
            theta_bound: u64::MAX,
            max_size: DEFAULT_MAX_SIZE,
            hashes: BTreeSet::new(),
            druid_origin: true,
            // Seed-neutral: an empty sketch retains nothing, so it adopts
            // the other side's seed in any set operation.
            druid_seed_hash: None,
        }
    }

    /// Whether this sketch was decoded from (or unioned with) an Apache
    /// DataSketches compact image and therefore holds MurmurHash3-space
    /// hashes (union-only; see the module docs).
    #[must_use]
    pub fn is_druid_origin(&self) -> bool {
        self.druid_origin
    }

    /// The 16-bit DataSketches seed hash this Druid-origin sketch was
    /// decoded with, when known (`None` for native sketches and for
    /// seed-neutral / unknown-seed Druid-origin sketches — see the field
    /// docs on the struct).  Set operations require matching seed hashes
    /// between non-empty Druid-origin sketches.
    #[must_use]
    pub fn druid_seed_hash(&self) -> Option<u16> {
        self.druid_seed_hash
    }

    /// Number of retained hashes.
    #[must_use]
    pub fn retained(&self) -> usize {
        self.hashes.len()
    }

    /// Decode a serialized **Apache DataSketches compact Theta sketch** —
    /// the per-row on-disk form of a Druid `thetaSketch` metric column —
    /// directly from its public binary layout (no datasketches crate).
    ///
    /// Observed little-endian layout: an 8-byte preamble
    /// `[preLongs][serVer=3][familyId=3][lgNomLongs][lgArrLongs][flags]
    /// [seedHash u16]`, optionally more preamble longs, then the retained
    /// hash array:
    ///
    /// * `EMPTY` flag → must be the CANONICAL empty image: exactly the
    ///   8-byte `preLongs = 1` preamble (no curCount field, no retained
    ///   hashes); decodes to 0 retained hashes, theta = 1.0.  An
    ///   EMPTY-flagged image on any other shape (e.g. a `preLongs = 3`
    ///   preamble declaring `curCount = 1`) is contradictory and is
    ///   rejected loudly rather than silently decoded as cardinality 0;
    /// * single-item → exactly one hash follows the preamble,
    ///   theta = 1.0.  Recognised by SHAPE (`preLongs = 1`, non-EMPTY,
    ///   exactly 16 bytes), not by flag: Apache's historical serializers
    ///   omitted the `SINGLE_ITEM` flag (writing e.g. flags `0x1A` =
    ///   READ_ONLY|COMPACT|ORDERED — `SingleItemSketch` accepts that form
    ///   for compatibility), so the flag is a hint, not a requirement.  A
    ///   `SINGLE_ITEM`-flagged image of any OTHER shape is contradictory
    ///   and rejected;
    /// * `preLongs == 1` → legal ONLY for the empty and single-item forms
    ///   (theta implicitly 1.0): the bare 8-byte preamble (0 retained
    ///   hashes) or the 16-byte single-item image above.  A retained
    ///   count is NEVER inferred from any longer trailing length: a
    ///   multi-item image must declare its count in a `preLongs >= 2`
    ///   preamble, so `[preLongs=1, …, hash1, hash2]` is rejected loudly
    ///   instead of decoding as cardinality 2;
    /// * `preLongs == 2` → `[curCount i32][unused i32]` at long 1,
    ///   theta = 1.0 (exact mode);
    /// * `preLongs == 3` → additionally `[thetaLong i64]` at long 2;
    ///   theta = `thetaLong / 2^63` (a fraction of `Long.MAX_VALUE` —
    ///   NOT of `u64::MAX`, which is FerroDruid's native scaling).
    ///
    /// Every DataSketches hash is a positive `i64` (63-bit, uniform over
    /// `[1, 2^63)`), while this sketch normalises hashes over the full
    /// `u64` range — so each decoded hash is rescaled as `hash << 1`
    /// (lossless: the top bit is always clear) to make `retained / theta`,
    /// the union threshold filter, and trim() all consistent with the
    /// decoded `thetaLong / 2^63` theta.
    ///
    /// The returned sketch is **Druid-origin** (union-only with other
    /// Druid-origin sketches; refuses raw adds — see the module docs).
    /// The preamble's 16-bit `seedHash` (bytes 6-7, a hash of the
    /// MurmurHash3 update seed) is captured into the sketch identity:
    /// FerroDruid never re-hashes values into a decoded sketch, so the
    /// seed cannot be verified against data — but set operations require
    /// MATCHING seed hashes, because sketches built with different seeds
    /// place the same logical value at different hash points and unioning
    /// them would double-count (see [`Self::druid_seed_hash`]).
    ///
    /// The retained hashes must be DISTINCT (a genuine compact sketch's
    /// always are — a duplicate would silently halve `retained / theta`),
    /// and when the image's `ORDERED` flag (bit 4) is set they must be
    /// strictly ascending; violations are rejected loudly, never
    /// silently deduplicated.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on any truncated, oversized,
    /// or malformed image (never guesses, never over-allocates).
    pub fn from_druid_compact(data: &[u8]) -> Result<Self> {
        const PREAMBLE: usize = 8;
        let fail = |what: &str| -> SketchError {
            SketchError::Serialization(format!("druid compact theta: {what}"))
        };
        if data.len() < PREAMBLE {
            return Err(fail(&format!(
                "{} bytes is shorter than the 8-byte preamble",
                data.len()
            )));
        }
        // Byte 0: the low 6 bits are the preamble-long count (the high 2
        // bits carry a resize factor on non-compact images; masked out).
        let pre_longs = (data[0] & 0x3F) as usize;
        let ser_ver = data[1];
        let family = data[2];
        let lg_nom_longs = data[3];
        // data[4] = lgArrLongs (hash-table sizing; irrelevant to a compact
        // image).
        let flags = data[5];
        // data[6..8] = seed hash: captured into the sketch identity so
        // cross-seed set operations can be refused (see the doc comment).
        let seed_hash = u16::from_le_bytes([data[6], data[7]]);
        if ser_ver != DS_SERIAL_VERSION {
            return Err(fail(&format!(
                "unsupported serial version {ser_ver} (expected {DS_SERIAL_VERSION})"
            )));
        }
        if family != DS_FAMILY_COMPACT {
            return Err(fail(&format!(
                "unsupported family id {family} (expected compact theta, {DS_FAMILY_COMPACT})"
            )));
        }
        if lg_nom_longs > DS_MAX_LG_NOM_LONGS {
            return Err(fail(&format!(
                "lgNomLongs {lg_nom_longs} exceeds the family maximum {DS_MAX_LG_NOM_LONGS}"
            )));
        }
        // Degenerate images may carry lgNomLongs = 0; keep a sane union
        // budget for them.
        let nominal = if lg_nom_longs == 0 {
            DEFAULT_MAX_SIZE
        } else {
            1usize << lg_nom_longs
        };

        if flags & DS_FLAG_EMPTY != 0 {
            // Canonical-empty invariant: an Apache EMPTY compact image is
            // EXACTLY the 8-byte preLongs-1 preamble — no curCount field
            // (so none can declare retained state), no thetaLong, and no
            // retained-hash bytes.  An EMPTY flag on any other shape
            // (e.g. a preLongs-3 preamble declaring curCount 1) is
            // contradictory: decoding it as cardinality 0 silently
            // discarded the declared state.  Fail loud, never guess.
            if pre_longs != 1 || data.len() != PREAMBLE {
                return Err(fail(&format!(
                    "EMPTY-flagged image must be the canonical {PREAMBLE}-byte \
                     preLongs-1 preamble (curCount 0, no retained hashes); got \
                     preLongs {pre_longs} with {} bytes",
                    data.len()
                )));
            }
            return Ok(Self {
                theta: 1.0,
                theta_bound: druid_theta_bound(i64::MAX),
                max_size: nominal,
                hashes: BTreeSet::new(),
                druid_origin: true,
                druid_seed_hash: Some(seed_hash),
            });
        }
        // Single-item image: one preamble long + EXACTLY one hash.
        // Recognised by SHAPE, not by flag: Apache's historical
        // serializers wrote this 16-byte image WITHOUT the SINGLE_ITEM
        // flag (e.g. flags 0x1A = READ_ONLY|COMPACT|ORDERED —
        // `SingleItemSketch` accepts that form for compatibility), so any
        // non-EMPTY `preLongs = 1` image of exactly 16 bytes decodes as
        // single-item whether or not the flag is present.  A flagged
        // image of any OTHER shape is contradictory and rejected below
        // (the exact-length check is the "exactly 1 retained hash"
        // invariant; the shared reader validates the hash range).
        if flags & DS_FLAG_SINGLE_ITEM != 0 || (pre_longs == 1 && data.len() == 16) {
            if pre_longs != 1 || data.len() != 16 {
                return Err(fail(&format!(
                    "single-item sketch must be 16 bytes with preLongs 1, got {} bytes \
                     with preLongs {pre_longs}",
                    data.len()
                )));
            }
            let mut cursor = Cursor::new(&data[PREAMBLE..]);
            let hashes = read_retained_hashes(
                &mut cursor,
                1,
                false,
                |h| rescale_druid_hash(h, i64::MAX),
                fail,
            )?;
            return Ok(Self {
                theta: 1.0,
                theta_bound: druid_theta_bound(i64::MAX),
                max_size: nominal.max(1),
                hashes,
                druid_origin: true,
                druid_seed_hash: Some(seed_hash),
            });
        }

        // General compact image: [preamble longs][curCount × u64 hashes].
        let (cur_count, theta_long, hashes_at) = match pre_longs {
            1 => {
                // Apache compact-theta permits a 1-long preamble ONLY for
                // the empty and single-item forms (theta implicitly 1.0);
                // the EMPTY-flagged and 16-byte single-item shapes
                // (flagged or historical flag-less) both returned above.
                // The sole remaining legal image is the flag-less EMPTY
                // one: exactly the 8-byte preamble, zero retained hashes.
                // A multi-item image MUST declare its count in a
                // `preLongs >= 2` preamble — inferring a count from the
                // trailing length would accept illegal
                // `[preLongs=1, …, hash1, hash2]` images as cardinality 2.
                // Fail loud, never guess.
                if data.len() != PREAMBLE {
                    return Err(fail(&format!(
                        "preLongs 1 without the empty/single-item flag must be exactly \
                         the {PREAMBLE}-byte preamble (0 retained hashes), got {} bytes \
                         — a multi-item sketch requires preLongs >= 2",
                        data.len()
                    )));
                }
                (0, i64::MAX, PREAMBLE)
            }
            2 | 3 => {
                if data.len() < pre_longs * 8 {
                    return Err(fail(&format!(
                        "preLongs {pre_longs} image truncated at {} bytes",
                        data.len()
                    )));
                }
                let raw_count = i32::from_le_bytes([data[8], data[9], data[10], data[11]]);
                let Ok(cur_count) = usize::try_from(raw_count) else {
                    return Err(fail(&format!("negative retained count {raw_count}")));
                };
                let theta_long = if pre_longs == 3 {
                    i64::from_le_bytes([
                        data[16], data[17], data[18], data[19], data[20], data[21], data[22],
                        data[23],
                    ])
                } else {
                    i64::MAX
                };
                (cur_count, theta_long, pre_longs * 8)
            }
            other => {
                return Err(fail(&format!("unsupported preamble-long count {other}")));
            }
        };
        if theta_long <= 0 {
            return Err(fail(&format!("non-positive thetaLong {theta_long}")));
        }
        if cur_count > DS_MAX_RETAINED {
            return Err(fail(&format!(
                "retained count {cur_count} exceeds the family maximum {DS_MAX_RETAINED}"
            )));
        }
        // Exact-length check (checked arithmetic) BEFORE anything is sized
        // from the declared count: the image must hold exactly the declared
        // hashes, nothing more, nothing less.
        let expected_len = cur_count
            .checked_mul(8)
            .and_then(|body| hashes_at.checked_add(body))
            .ok_or_else(|| fail("retained count overflows the addressable length"))?;
        if data.len() != expected_len {
            return Err(fail(&format!(
                "expected exactly {expected_len} bytes for {cur_count} retained hashes, \
                 got {}",
                data.len()
            )));
        }
        // theta = thetaLong / 2^63 (see the doc comment; thetaLong ==
        // Long.MAX_VALUE yields exactly 1.0).
        #[allow(clippy::cast_precision_loss)]
        let theta = theta_long as f64 / DS_THETA_DENOMINATOR;
        // The retained hashes of a genuine compact sketch are DISTINCT
        // (and strictly ascending when the ORDERED flag is set) — the
        // shared reader enforces both, plus the `[1, min(2^63,
        // thetaLong))` range of every hash via `rescale_druid_hash`.
        let ordered = flags & DS_FLAG_ORDERED != 0;
        let mut cursor = Cursor::new(&data[hashes_at..]);
        let hashes = read_retained_hashes(
            &mut cursor,
            cur_count,
            ordered,
            |h| rescale_druid_hash(h, theta_long),
            fail,
        )?;
        Ok(Self {
            theta,
            theta_bound: druid_theta_bound(theta_long),
            max_size: nominal.max(cur_count),
            hashes,
            druid_origin: true,
            druid_seed_hash: Some(seed_hash),
        })
    }

    /// Add a value by hashing it internally.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::HashSpaceMismatch`] when this sketch is
    /// Druid-origin (union-only; see the module docs) — the sketch is left
    /// unchanged.
    pub fn add(&mut self, value: &[u8]) -> Result<()> {
        let hash = hash64(value);
        self.add_hash(hash)
    }

    /// Add a pre-computed 64-bit hash.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::HashSpaceMismatch`] when this sketch is
    /// Druid-origin (union-only; see the module docs) — the sketch is left
    /// unchanged.
    pub fn add_hash(&mut self, hash: u64) -> Result<()> {
        if self.druid_origin {
            return Err(SketchError::HashSpaceMismatch(
                "cannot add a raw value to a Druid-origin theta sketch (its retained \
                 hashes are MurmurHash3-space; adding would mix in FNV-space hashes). \
                 A migrated theta column is union-only."
                    .to_string(),
            ));
        }
        let norm = normalise(hash);
        if norm >= self.theta {
            return Ok(());
        }
        self.hashes.insert(hash);
        self.trim();
        Ok(())
    }

    /// Estimate the cardinality.
    ///
    /// When theta is still 1.0 (sketch has not yet reached capacity) the
    /// estimate is simply the number of retained hashes.  Otherwise it is
    /// `retained / theta`.
    pub fn estimate(&self) -> f64 {
        if self.hashes.is_empty() {
            return 0.0;
        }
        if (self.theta - 1.0).abs() < f64::EPSILON {
            return self.hashes.len() as f64;
        }
        self.hashes.len() as f64 / self.theta
    }

    /// Resolve the hash-space origin AND the Druid seed hash of a
    /// two-sketch operation, refusing a genuine mix (both sides non-empty
    /// in DIFFERENT spaces, or non-empty Druid-origin sides carrying
    /// DIFFERENT seed hashes — different update seeds place the same
    /// logical value at different MurmurHash3 points, so combining them
    /// would double-count).  An empty sketch carries no hashes, so it is
    /// both space- and seed-neutral: it adopts the other side's space and
    /// seed; two empty sketches resolve to the conservative union-only
    /// marking.  ONLY an empty sketch is ever seed-neutral: a non-empty
    /// Druid-origin sketch always carries its decoded seed (the v2 wire
    /// transports it and [`ThetaSketch::deserialize`] rejects a non-empty
    /// image without one), so the `None` arms below for non-empty sides
    /// are defensive, not a bypass.
    fn resolved_identity(&self, other: &ThetaSketch, op: &str) -> Result<(bool, Option<u16>)> {
        let origin = if self.druid_origin == other.druid_origin {
            self.druid_origin
        } else {
            match (self.hashes.is_empty(), other.hashes.is_empty()) {
                (true, false) => other.druid_origin,
                (false, true) => self.druid_origin,
                (true, true) => true,
                (false, false) => {
                    return Err(SketchError::HashSpaceMismatch(format!(
                        "cannot {op} a Druid-origin theta sketch (MurmurHash3 hash space) \
                         with a native FerroDruid theta sketch (FNV hash space); a sketch \
                         decoded from a migrated Druid segment is union-only with other \
                         Druid-origin sketches"
                    )));
                }
            }
        };
        let seed = match (self.hashes.is_empty(), other.hashes.is_empty()) {
            (true, false) => other.druid_seed_hash,
            (false, true) | (true, true) => self.druid_seed_hash.or(other.druid_seed_hash),
            (false, false) => match (self.druid_seed_hash, other.druid_seed_hash) {
                (Some(a), Some(b)) if a != b => {
                    return Err(SketchError::HashSpaceMismatch(format!(
                        "cannot {op} Druid-origin theta sketches decoded with different \
                         update seeds (seed hash {a:#06x} vs {b:#06x}); the same logical \
                         value hashes to different MurmurHash3 points under different \
                         seeds, so combining them would corrupt the estimate"
                    )));
                }
                (a, b) => a.or(b),
            },
        };
        // A native result never carries a Druid seed (an empty Druid side
        // may have contributed one above).
        Ok((origin, if origin { seed } else { None }))
    }

    /// Resolve the retention threshold `(theta, theta_bound)` of a
    /// two-sketch set operation: the minimum across the NON-empty
    /// operands.  An EMPTY operand retains nothing, so its threshold
    /// constrains nothing — it contributes the NEUTRAL element instead of
    /// joining the `min` (mirrors the seed/space neutrality of
    /// [`Self::resolved_identity`]).  Honouring an empty side's threshold
    /// let a degenerate empty-but-estimating sketch (0 retained, tiny
    /// theta — decodable from a legal `curCount = 0` compact image) lower
    /// the result's bound below the non-empty side's retained hashes and
    /// filter ALL of them out: the estimate of any query it merged into
    /// silently became 0.  Two empty operands resolve to the neutral
    /// threshold itself.
    fn resolved_threshold(&self, other: &ThetaSketch) -> (f64, u64) {
        match (self.hashes.is_empty(), other.hashes.is_empty()) {
            (false, false) => (
                self.theta.min(other.theta),
                self.theta_bound.min(other.theta_bound),
            ),
            (true, false) => (other.theta, other.theta_bound),
            (false, true) => (self.theta, self.theta_bound),
            (true, true) => (1.0, u64::MAX),
        }
    }

    /// Return the union of two sketches.
    ///
    /// An EMPTY operand is threshold-neutral: it never lowers the
    /// result's theta/bound (see [`Self::resolved_threshold`]), so
    /// unioning with an empty sketch leaves the other side's retained
    /// hashes — and its cardinality estimate — unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::HashSpaceMismatch`] when one side is
    /// Druid-origin and the other is a non-empty native sketch (see the
    /// module docs) — hash spaces must never mix — or when both sides are
    /// non-empty Druid-origin sketches carrying DIFFERENT seed hashes
    /// (cross-seed unions double-count; see [`Self::druid_seed_hash`]).
    pub fn union(&self, other: &ThetaSketch) -> Result<ThetaSketch> {
        let (druid_origin, druid_seed_hash) = self.resolved_identity(other, "union")?;
        let (theta, theta_bound) = self.resolved_threshold(other);
        let max_size = self.max_size.max(other.max_size);
        let mut result = ThetaSketch {
            theta,
            theta_bound,
            max_size,
            hashes: BTreeSet::new(),
            druid_origin,
            druid_seed_hash,
        };
        for &h in self.hashes.iter().chain(other.hashes.iter()) {
            if result.below_threshold(h) {
                result.hashes.insert(h);
            }
        }
        result.trim();
        Ok(result)
    }

    /// Return the intersection of two sketches.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::HashSpaceMismatch`] on a hash-space or
    /// seed-hash mix (see [`Self::union`]).
    pub fn intersect(&self, other: &ThetaSketch) -> Result<ThetaSketch> {
        let (druid_origin, druid_seed_hash) = self.resolved_identity(other, "intersect")?;
        let (theta, theta_bound) = self.resolved_threshold(other);
        let mut result = ThetaSketch {
            theta,
            theta_bound,
            max_size: self.max_size.max(other.max_size),
            hashes: BTreeSet::new(),
            druid_origin,
            druid_seed_hash,
        };
        for &h in &self.hashes {
            if other.hashes.contains(&h) && result.below_threshold(h) {
                result.hashes.insert(h);
            }
        }
        Ok(result)
    }

    /// Return the set difference (A not B).
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::HashSpaceMismatch`] on a hash-space or
    /// seed-hash mix (see [`Self::union`]).
    pub fn difference(&self, other: &ThetaSketch) -> Result<ThetaSketch> {
        let (druid_origin, druid_seed_hash) = self.resolved_identity(other, "difference")?;
        let (theta, theta_bound) = self.resolved_threshold(other);
        let mut result = ThetaSketch {
            theta,
            theta_bound,
            max_size: self.max_size,
            hashes: BTreeSet::new(),
            druid_origin,
            druid_seed_hash,
        };
        for &h in &self.hashes {
            if !other.hashes.contains(&h) && result.below_threshold(h) {
                result.hashes.insert(h);
            }
        }
        Ok(result)
    }

    /// Serialize the sketch to bytes.
    ///
    /// Native-origin format (version 1, byte-identical to the historical
    /// layout): `[version: u8][theta: f64 LE][max_size: u32 LE]
    /// [count: u32 LE][hashes: count × u64 LE]`.  A Druid-origin sketch
    /// serializes as version 2 (transient partial-aggregation state, never
    /// persisted to a segment), which inserts after the version byte one
    /// origin-flags byte, the EXACT integer retention threshold, and — for
    /// every sketch with a known DataSketches seed hash, i.e. every
    /// NON-empty one — the [`ORIGIN_FLAG_SEED_HASH`] bit plus a 16-bit LE
    /// seed hash: `[2][flags: u8][theta_bound: u64 LE][seedHash: u16 LE if
    /// flags bit 1][theta]…`.  The full Druid-origin identity
    /// `(theta_bound, seed_hash)` thus survives the wire round-trip
    /// EXACTLY: reconstructing `theta_bound` from the f64 theta widened it
    /// (a hash excluded before serialization was admitted after), and
    /// omitting the seed made a non-empty sketch spuriously seed-neutral
    /// (silent cross-seed unions double-count).  The trailing f64 theta of
    /// a v2 image is REDUNDANT with the exact bound (`theta ==
    /// theta_bound / 2^64` bit-for-bit — see [`druid_theta_from_bound`])
    /// and [`Self::deserialize`] validates the relationship, rejecting
    /// any image whose two fields disagree.
    pub fn serialize(&self) -> Vec<u8> {
        // Every constructor bounds `max_size` (<= 2^26 via `new` and the
        // Druid decode caps, <= u32::MAX via `deserialize`) and every
        // hash-set growth path is trimmed to it or reads a u32 count, so
        // both fields always fit `u32`.  If a future code path ever breaks
        // that invariant, SATURATE instead of wrapping: the historical
        // `as u32` cast turned a query-controlled `size = 2^32` into a 0
        // budget that trimmed every retained hash on the next merge.
        let count = u32::try_from(self.hashes.len()).unwrap_or(u32::MAX);
        let max_size = u32::try_from(self.max_size).unwrap_or(u32::MAX);
        let mut buf = Vec::with_capacity(2 + 8 + 2 + 8 + 4 + 4 + self.hashes.len() * 8);
        if self.druid_origin {
            let _ = buf.write_u8(VERSION_DRUID);
            let flags = if self.druid_seed_hash.is_some() {
                ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH
            } else {
                ORIGIN_FLAG_DRUID
            };
            let _ = buf.write_u8(flags);
            let _ = buf.write_u64::<LittleEndian>(self.theta_bound);
            if let Some(seed) = self.druid_seed_hash {
                let _ = buf.write_u16::<LittleEndian>(seed);
            }
        } else {
            let _ = buf.write_u8(VERSION);
        }
        let _ = buf.write_f64::<LittleEndian>(self.theta);
        let _ = buf.write_u32::<LittleEndian>(max_size);
        let _ = buf.write_u32::<LittleEndian>(count);
        for &h in &self.hashes {
            let _ = buf.write_u64::<LittleEndian>(h);
        }
        buf
    }

    /// Deserialize a sketch from bytes (accepts both the version-1 native
    /// layout and the version-2 origin-flagged layout — see
    /// [`Self::serialize`]).
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on invalid data — including
    /// a NON-empty version-2 (Druid-origin) image that carries no seed
    /// hash: a non-empty Druid-origin sketch always has a known seed
    /// ([`Self::serialize`] always writes it), so a seed-less non-empty
    /// image is malformed and must not deserialize into a seed-neutral
    /// sketch that would silently union across seeds.  A version-2 image
    /// whose redundant fields DISAGREE is also rejected as incoherent: a
    /// wire f64 theta that does not equal the theta implied by the exact
    /// `theta_bound` (see [`druid_theta_from_bound`]), a zero
    /// `theta_bound` (retains nothing while theta must be positive), or a
    /// retained hash at/above `theta_bound` (retention is strictly-below
    /// everywhere).  Accepting such an image poisoned every union it
    /// merged into: a crafted `theta_bound = 0` won the `min(bound)`
    /// selection and filtered out EVERY retained hash of the other side,
    /// silently zeroing the estimate.  On BOTH wire versions the retained
    /// hashes must be DISTINCT (the serializer writes from a set, so a
    /// duplicate can only be corruption — silently deduplicating it
    /// halved `retained / theta`); a duplicate is rejected loudly via the
    /// shared [`read_retained_hashes`] reader.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(SketchError::Serialization(
                "data too short for Theta header".into(),
            ));
        }
        let (druid_origin, druid_seed_hash, theta_bound, header_len) = match data[0] {
            VERSION => (false, None, u64::MAX, 1usize),
            VERSION_DRUID => {
                // v2 always carries `[flags: u8][theta_bound: u64 LE]`
                // right after the version byte — the EXACT integer
                // retention threshold.  An image too short for the bound
                // is malformed (never reconstructed from the f64 theta:
                // the conservative widening ADMITTED hashes the decoder
                // had excluded).
                if data.len() < 10 {
                    return Err(SketchError::Serialization(
                        "data too short for Theta v2 origin flags + theta bound".into(),
                    ));
                }
                let flags = data[1];
                if flags & ORIGIN_FLAG_DRUID == 0
                    || flags & !(ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH) != 0
                {
                    return Err(SketchError::Serialization(format!(
                        "unsupported Theta v2 origin flags {flags:#04x}"
                    )));
                }
                let bound = u64::from_le_bytes([
                    data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9],
                ]);
                if flags & ORIGIN_FLAG_SEED_HASH != 0 {
                    if data.len() < 12 {
                        return Err(SketchError::Serialization(
                            "data too short for Theta v2 seed hash".into(),
                        ));
                    }
                    (
                        true,
                        Some(u16::from_le_bytes([data[10], data[11]])),
                        bound,
                        12usize,
                    )
                } else {
                    // Seed-less v2: legal ONLY for an EMPTY (seed-neutral)
                    // sketch — enforced against the count below.
                    (true, None, bound, 10usize)
                }
            }
            other => {
                return Err(SketchError::Serialization(format!(
                    "unsupported Theta version {other}"
                )));
            }
        };
        let min_header = header_len + 8 + 4 + 4; // header + theta + max_size + count
        if data.len() < min_header {
            return Err(SketchError::Serialization(
                "data too short for Theta header".into(),
            ));
        }
        let mut cursor = Cursor::new(&data[header_len..]);
        let wire_theta = cursor
            .read_f64::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?;
        let max_size = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))?
            as usize;
        let count = cursor
            .read_u32::<LittleEndian>()
            .map_err(|e| SketchError::Serialization(e.to_string()))? as usize;
        // A Druid-origin sketch's wire theta is REDUNDANT with the exact
        // integer bound (`theta == theta_bound / 2^64` bit-for-bit — see
        // STORED_THETA_DENOMINATOR), so DERIVE the canonical value and
        // reject a disagreeing wire f64: the two fields form one identity
        // and can never legitimately diverge.  An unchecked pair let a
        // crafted `[theta_bound = 0, theta = 1.0]` image through, and its
        // 0 bound then won the union's `min(bound)` selection and
        // filtered out EVERY retained hash of the other side — the
        // estimate of any query it merged into silently became 0.
        let theta = if druid_origin {
            if theta_bound == 0 {
                return Err(SketchError::Serialization(
                    "incoherent Druid-origin Theta v2 image: theta_bound 0 retains \
                     nothing, while theta must be positive — a 0 bound would filter \
                     out every retained hash of any union it merges into"
                        .into(),
                ));
            }
            let derived = druid_theta_from_bound(theta_bound);
            if wire_theta.to_bits() != derived.to_bits() {
                return Err(SketchError::Serialization(format!(
                    "incoherent Druid-origin Theta v2 image: wire theta {wire_theta} \
                     disagrees with the theta implied by the exact theta_bound \
                     {theta_bound:#018x} ({derived}) — the serializer keeps the pair \
                     coherent, so a mismatch is a corrupt or crafted image"
                )));
            }
            derived
        } else {
            wire_theta
        };
        // A NON-empty Druid-origin sketch always has a known seed (decode
        // captured it and serialize always writes it), so a non-empty v2
        // image without one is malformed.  Deserializing it as seed-neutral
        // would let two different-seed sketches union silently and
        // double-count — fail loud instead.  Only a genuinely EMPTY sketch
        // (0 retained) is seed-neutral.
        if druid_origin && count > 0 && druid_seed_hash.is_none() {
            return Err(SketchError::Serialization(format!(
                "non-empty Druid-origin Theta v2 image ({count} retained hashes) \
                 without a seed hash — a non-empty sketch is never seed-neutral"
            )));
        }

        // DD R32: checked length arithmetic so an untrusted `count` cannot wrap
        // `count * 8` (32-bit) and bypass the length guard.
        let expected_len = count
            .checked_mul(8)
            .and_then(|body| min_header.checked_add(body))
            .ok_or_else(|| {
                SketchError::Serialization(
                    "theta sketch hash count overflows the addressable length".to_string(),
                )
            })?;
        if data.len() < expected_len {
            return Err(SketchError::Serialization(format!(
                "expected at least {expected_len} bytes but got {}",
                data.len()
            )));
        }
        // The shared reader enforces the distinct-retained invariant on
        // BOTH wire versions: the serializer writes from a set, so a
        // declared `count = 2` image carrying `[h, h]` can only be corrupt
        // or crafted — silently collapsing it in the BTreeSet halved
        // `retained / theta`.  The per-hash closure adds the v2 threshold
        // coherence check: retention is strictly-below EVERYWHERE (decode
        // refuses `h >= thetaLong`, unions filter `h < bound`), so a v2
        // hash at/above the bound is incoherent — a crafted image bypasses
        // the decode-time guard, so the invariant must hold on the wire
        // too.
        let hashes = read_retained_hashes(
            &mut cursor,
            count,
            false,
            |h| {
                if druid_origin && h >= theta_bound {
                    return Err(SketchError::Serialization(format!(
                        "incoherent Druid-origin Theta v2 image: retained hash {h:#018x} \
                         is not strictly below the retention threshold theta_bound \
                         {theta_bound:#018x}"
                    )));
                }
                Ok(h)
            },
            |what| SketchError::Serialization(what.to_string()),
        )?;
        Ok(Self {
            theta,
            theta_bound,
            max_size,
            hashes,
            druid_origin,
            druid_seed_hash,
        })
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    /// Whether a stored hash is strictly below the retention threshold —
    /// in the SAME comparison space as the decision that retained it.
    ///
    /// Druid-origin sketches compare exact integers: decode retains
    /// `h < thetaLong`, i.e. stored `h << 1 < thetaLong << 1`
    /// (= `theta_bound`).  Round-tripping through f64 collapses distinct
    /// values at the 63-bit boundary (`thetaLong = i64::MAX` and stored
    /// `(i64::MAX - 1) << 1` both round to 1.0), which made union/trim
    /// drop hashes the decoder had retained.  Native sketches keep the
    /// historical f64 comparison — their retention decision
    /// ([`Self::add_hash`]) is the f64 one, so f64-vs-f64 is coherent.
    fn below_threshold(&self, hash: u64) -> bool {
        if self.druid_origin {
            hash < self.theta_bound
        } else {
            normalise(hash) < self.theta
        }
    }

    /// Trim the set to `max_size` by lowering theta.
    fn trim(&mut self) {
        while self.hashes.len() > self.max_size {
            // Remove the largest hash and update theta.  The removed hash
            // itself is the new EXCLUSIVE integer bound — kept coherent
            // with the f64 theta so Druid-origin comparisons stay
            // integer-exact.
            if let Some(&largest) = self.hashes.iter().next_back() {
                self.hashes.remove(&largest);
                self.theta = normalise(largest);
                self.theta_bound = largest;
            }
        }
    }
}

/// Normalise a `u64` hash to `[0.0, 1.0)`.
fn normalise(hash: u64) -> f64 {
    hash as f64 / u64::MAX as f64
}

/// Read the declared number of retained hashes from `cursor`, enforcing
/// the invariants EVERY byte→[`ThetaSketch`] decode path shares.  The
/// compact-image single-item and multi-item branches of
/// [`ThetaSketch::from_druid_compact`] AND the v1/v2 wire loop of
/// [`ThetaSketch::deserialize`] all read their hashes through this one
/// function, so no path (present or future) can silently skip a check:
///
/// * every stored hash is DISTINCT and the final set holds exactly
///   `count` hashes — a duplicate would silently collapse in the
///   `BTreeSet`, halving `retained / theta`, so it is rejected loudly
///   instead of deduplicated;
/// * when `ordered` is set (a compact image's ORDERED flag) the raw wire
///   stream must be strictly ascending;
/// * `decode_hash` applies the per-path range/threshold validation — and
///   any rescaling — to each raw hash BEFORE it is stored (compact
///   images validate `[1, min(2^63, thetaLong))` via
///   [`rescale_druid_hash`]; the v2 wire validates `h < theta_bound`;
///   native v1 hashes pass through).  Every caller's mapping is
///   injective (identity or `h << 1`), so a stored duplicate is always a
///   raw duplicate.
///
/// `fail` wraps a description into the caller's error context.
fn read_retained_hashes(
    cursor: &mut Cursor<&[u8]>,
    count: usize,
    ordered: bool,
    mut decode_hash: impl FnMut(u64) -> Result<u64>,
    fail: impl Fn(&str) -> SketchError,
) -> Result<BTreeSet<u64>> {
    let mut hashes = BTreeSet::new();
    let mut prev: Option<u64> = None;
    for _ in 0..count {
        let raw = cursor
            .read_u64::<LittleEndian>()
            .map_err(|e| fail(&e.to_string()))?;
        if ordered
            && let Some(p) = prev
            && raw <= p
        {
            return Err(fail(&format!(
                "ORDERED image is not strictly ascending (hash {raw:#018x} follows \
                 {p:#018x})"
            )));
        }
        prev = Some(raw);
        let stored = decode_hash(raw)?;
        if !hashes.insert(stored) {
            return Err(fail(&format!(
                "duplicate retained hash {raw:#018x} in an image declaring {count} \
                 retained hashes — a genuine sketch's retained hashes are distinct"
            )));
        }
    }
    if hashes.len() != count {
        // Unreachable while every insertion above is individually checked;
        // kept explicit so the `retained == declared count` invariant
        // survives future refactors of the loop.
        return Err(fail(&format!(
            "decoded {} distinct retained hashes but the image declared {count}",
            hashes.len()
        )));
    }
    Ok(hashes)
}

/// Rescale a decoded DataSketches hash (uniform over `[1, 2^63)`, always
/// strictly below `theta_long`) into this sketch's full-`u64` hash space:
/// `hash << 1` — lossless (the top bit is always clear) and consistent with
/// the decoded `thetaLong / 2^63` theta under [`normalise`].
fn rescale_druid_hash(hash: u64, theta_long: i64) -> Result<u64> {
    #[allow(clippy::cast_sign_loss)]
    let theta_u = theta_long as u64; // validated positive by the caller
    if hash == 0 || hash >= (1u64 << 63) || hash >= theta_u {
        return Err(SketchError::Serialization(format!(
            "druid compact theta: retained hash {hash:#018x} outside the valid \
             range [1, min(2^63, thetaLong {theta_long}))"
        )));
    }
    Ok(hash << 1)
}

/// Exclusive integer retention threshold of a decoded Druid image in the
/// STORED-hash space: `thetaLong << 1`, the exact image of the decode
/// retention test `h < thetaLong` under the `h << 1` rescaling.  Cannot
/// overflow: `thetaLong <= i64::MAX`, so the result is at most
/// `u64::MAX - 1`.
fn druid_theta_bound(theta_long: i64) -> u64 {
    #[allow(clippy::cast_sign_loss)]
    let theta_u = theta_long as u64; // validated positive by the callers
    theta_u << 1
}

/// The f64 theta implied by a Druid-origin sketch's STORED-space integer
/// retention threshold: `theta_bound / 2^64`.  Every legitimately-built
/// Druid-origin sketch satisfies `theta == druid_theta_from_bound
/// (theta_bound)` bit-for-bit (see [`STORED_THETA_DENOMINATOR`]), which
/// makes the redundant v2 wire theta verifiable — a crafted image whose
/// f64 theta disagrees with its exact integer bound is incoherent and
/// rejected on deserialize.
fn druid_theta_from_bound(theta_bound: u64) -> f64 {
    #[allow(clippy::cast_precision_loss)]
    let bound = theta_bound as f64;
    bound / STORED_THETA_DENOMINATOR
}

/// 64-bit FNV-1a hash (deterministic).
fn hash64(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Seed hash written by [`compact_image`] (the default-seed bytes every
    /// pre-existing fixture used: `0x1E, 0x93` little-endian).
    const TEST_SEED_HASH: u16 = 0x931E;

    /// Test-side helper: build a well-formed DataSketches compact image.
    /// `pre_longs` picks the layout; `theta_long` is only written for
    /// preLongs 3.
    fn compact_image(pre_longs: u8, flags: u8, theta_long: i64, hashes: &[u64]) -> Vec<u8> {
        compact_image_seeded(pre_longs, flags, theta_long, TEST_SEED_HASH, hashes)
    }

    /// [`compact_image`] with an explicit 16-bit seed hash (preamble bytes
    /// 6-7, little-endian).
    fn compact_image_seeded(
        pre_longs: u8,
        flags: u8,
        theta_long: i64,
        seed_hash: u16,
        hashes: &[u64],
    ) -> Vec<u8> {
        let seed = seed_hash.to_le_bytes();
        let mut buf = vec![
            pre_longs,
            DS_SERIAL_VERSION,
            DS_FAMILY_COMPACT,
            12, // lgNomLongs (k = 4096)
            13, // lgArrLongs
            flags,
            seed[0],
            seed[1],
        ];
        if pre_longs >= 2 {
            buf.extend_from_slice(&(hashes.len() as i32).to_le_bytes());
            buf.extend_from_slice(&0u32.to_le_bytes()); // unused / p
        }
        if pre_longs >= 3 {
            buf.extend_from_slice(&theta_long.to_le_bytes());
        }
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    /// Test-side helper: assemble a raw version-1 native image
    /// byte-for-byte (`[1][theta f64 LE][max_size u32 LE][count u32 LE]
    /// [hashes…]`) — used to craft images the legitimate serializer
    /// (which writes from a set) can never produce.
    fn v1_image(theta: f64, hashes: &[u64]) -> Vec<u8> {
        let mut buf = vec![VERSION];
        buf.extend_from_slice(&theta.to_le_bytes());
        buf.extend_from_slice(&4096u32.to_le_bytes());
        let count = u32::try_from(hashes.len()).expect("test count fits u32");
        buf.extend_from_slice(&count.to_le_bytes());
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    /// Test-side helper: assemble a raw version-2 partial-state image
    /// byte-for-byte (`[2][flags][theta_bound u64 LE][seedHash u16 LE if
    /// flags bit 1][theta f64 LE][max_size u32 LE][count u32 LE]
    /// [hashes…]`) — used to craft INCOHERENT images the legitimate
    /// serializer can never produce.
    fn v2_image(
        flags: u8,
        theta_bound: u64,
        seed_hash: Option<u16>,
        theta: f64,
        hashes: &[u64],
    ) -> Vec<u8> {
        let mut buf = vec![VERSION_DRUID, flags];
        buf.extend_from_slice(&theta_bound.to_le_bytes());
        if let Some(seed) = seed_hash {
            buf.extend_from_slice(&seed.to_le_bytes());
        }
        buf.extend_from_slice(&theta.to_le_bytes());
        buf.extend_from_slice(&4096u32.to_le_bytes());
        let count = u32::try_from(hashes.len()).expect("test count fits u32");
        buf.extend_from_slice(&count.to_le_bytes());
        for &h in hashes {
            buf.extend_from_slice(&h.to_le_bytes());
        }
        buf
    }

    #[test]
    fn empty_sketch_estimate_zero() {
        let sketch = ThetaSketch::default_size();
        assert!((sketch.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn five_thousand_values() {
        let mut sketch = ThetaSketch::default_size();
        for i in 0_u32..5000 {
            sketch.add(&i.to_le_bytes()).expect("native add");
        }
        let est = sketch.estimate();
        let error = (est - 5000.0).abs() / 5000.0;
        assert!(
            error < 0.10,
            "estimate {est} should be within 10% of 5000 (error={error})"
        );
    }

    #[test]
    fn union_disjoint_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        for i in 0_u32..2000 {
            a.add(&i.to_le_bytes()).expect("native add");
        }
        for i in 2000_u32..4000 {
            b.add(&i.to_le_bytes()).expect("native add");
        }
        let u = a.union(&b).expect("same-space union");
        let est = u.estimate();
        let error = (est - 4000.0).abs() / 4000.0;
        assert!(
            error < 0.15,
            "union estimate {est} should be near 4000 (error={error})"
        );
    }

    #[test]
    fn intersection_of_overlapping_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        // A = 0..3000, B = 1000..4000, overlap = 1000..3000 (2000 items)
        for i in 0_u32..3000 {
            a.add(&i.to_le_bytes()).expect("native add");
        }
        for i in 1000_u32..4000 {
            b.add(&i.to_le_bytes()).expect("native add");
        }
        let inter = a.intersect(&b).expect("same-space intersect");
        let est = inter.estimate();
        // Intersection should be approximately 2000.  Theta-sketch
        // intersection can have higher variance, so we allow 30%.
        let error = (est - 2000.0).abs() / 2000.0;
        assert!(
            error < 0.30,
            "intersection estimate {est} should be near 2000 (error={error})"
        );
    }

    #[test]
    fn difference_of_sets() {
        let mut a = ThetaSketch::new(4096);
        let mut b = ThetaSketch::new(4096);
        // A = 0..3000, B = 2000..4000 → A \ B ≈ 2000
        for i in 0_u32..3000 {
            a.add(&i.to_le_bytes()).expect("native add");
        }
        for i in 2000_u32..4000 {
            b.add(&i.to_le_bytes()).expect("native add");
        }
        let diff = a.difference(&b).expect("same-space difference");
        let est = diff.estimate();
        let error = (est - 2000.0).abs() / 2000.0;
        assert!(
            error < 0.30,
            "difference estimate {est} should be near 2000 (error={error})"
        );
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut sketch = ThetaSketch::new(512);
        for i in 0_u32..1000 {
            sketch.add(&i.to_le_bytes()).expect("native add");
        }
        let bytes = sketch.serialize();
        // Native sketches keep the historical version-1 leading byte
        // (byte-for-byte wire compatibility).
        assert_eq!(bytes[0], VERSION);
        let restored = ThetaSketch::deserialize(&bytes).expect("deserialize should succeed");
        assert!((sketch.estimate() - restored.estimate()).abs() < f64::EPSILON);
        assert!(!restored.is_druid_origin());
    }

    #[test]
    fn deserialize_too_short() {
        assert!(ThetaSketch::deserialize(&[]).is_err());
    }

    // --- Druid compact decoding (compat-8 sketch #2) ---

    #[test]
    fn druid_compact_empty_decodes_to_zero() {
        let img = compact_image(1, DS_FLAG_EMPTY, 0, &[]);
        let s = ThetaSketch::from_druid_compact(&img).expect("decode empty");
        assert_eq!(s.retained(), 0);
        assert!((s.estimate() - 0.0).abs() < f64::EPSILON);
        assert!(s.is_druid_origin());
    }

    #[test]
    fn druid_compact_single_item_decodes_to_one() {
        let img = compact_image(1, DS_FLAG_SINGLE_ITEM, 0, &[0x1234_5678_9ABC_DEF0 >> 1]);
        let s = ThetaSketch::from_druid_compact(&img).expect("decode single item");
        assert_eq!(s.retained(), 1);
        assert!((s.estimate() - 1.0).abs() < f64::EPSILON);
        assert!(s.is_druid_origin());
    }

    #[test]
    fn druid_compact_multi_hash_exact_mode() {
        // preLongs 2 (explicit count, implicit theta = 1.0): the estimate
        // below capacity is EXACT.
        let hashes = [100u64, 2_000, 30_000, 400_000];
        let img = compact_image(2, 0, 0, &hashes);
        let s = ThetaSketch::from_druid_compact(&img).expect("decode exact");
        assert_eq!(s.retained(), 4);
        assert!((s.estimate() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn druid_compact_pre_longs_1_multi_item_is_rejected() {
        // Apache compact-theta permits preLongs 1 ONLY for the empty and
        // single-item forms.  A count must never be inferred from the
        // trailing length: `[preLongs=1, …, hash1, hash2]` used to decode
        // as cardinality 2.
        let err = ThetaSketch::from_druid_compact(&compact_image(1, 0, 0, &[7, 9]))
            .expect_err("preLongs 1 with 2 trailing hashes must be rejected");
        assert!(err.to_string().contains("preLongs >= 2"), "got: {err}");
        // A SINGLE_ITEM-flagged image of any shape other than the 16-byte
        // one is contradictory: rejected with 0 hashes and with 2 hashes.
        assert!(
            ThetaSketch::from_druid_compact(&compact_image(1, DS_FLAG_SINGLE_ITEM, 0, &[]))
                .is_err()
        );
        assert!(
            ThetaSketch::from_druid_compact(&compact_image(1, DS_FLAG_SINGLE_ITEM, 0, &[7, 9]))
                .is_err()
        );
    }

    #[test]
    fn druid_compact_flagless_single_item_decodes_to_one() {
        // Apache's historical serializers wrote the 16-byte single-item
        // image WITHOUT the SINGLE_ITEM flag (flags 0x1A =
        // READ_ONLY|COMPACT|ORDERED — `SingleItemSketch` accepts that form
        // for compatibility): recognition is by SHAPE (preLongs 1,
        // non-EMPTY, exactly 8-byte preamble + 1 hash), the flag is only
        // a hint.  This image used to be rejected, which made the whole
        // theta column unreadable.
        let hash = 0x1234_5678_9ABC_DEF0u64 >> 1;
        let s = ThetaSketch::from_druid_compact(&compact_image(1, 0x1A, 0, &[hash]))
            .expect("historical flag-less single item");
        assert_eq!(s.retained(), 1);
        assert!((s.estimate() - 1.0).abs() < f64::EPSILON);
        assert!(s.is_druid_origin());
        // The entirely flag-less 16-byte shape decodes too (shape wins).
        let s = ThetaSketch::from_druid_compact(&compact_image(1, 0, 0, &[hash]))
            .expect("flag-less single item");
        assert_eq!(s.retained(), 1);
        assert!((s.estimate() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn druid_compact_pre_longs_1_flagless_empty_decodes() {
        // The bare 8-byte preamble (no flags, no hashes) is the only legal
        // flag-less preLongs-1 shape: an EMPTY sketch.
        let s = ThetaSketch::from_druid_compact(&compact_image(1, 0, 0, &[]))
            .expect("flag-less empty preLongs 1");
        assert_eq!(s.retained(), 0);
        assert!((s.estimate() - 0.0).abs() < f64::EPSILON);
        assert!(s.is_druid_origin());
    }

    #[test]
    fn druid_compact_explicit_theta_scales_estimate() {
        // preLongs 3: thetaLong = 2^62 → theta = 0.5 exactly; 4 retained
        // hashes (all below thetaLong) → estimate = 4 / 0.5 = 8.
        let theta_long = 1i64 << 62;
        let hashes = [100u64, 2_000, 30_000, 400_000];
        let img = compact_image(3, 0, theta_long, &hashes);
        let s = ThetaSketch::from_druid_compact(&img).expect("decode estimating");
        assert_eq!(s.retained(), 4);
        assert!(
            (s.estimate() - 8.0).abs() < 1e-9,
            "theta = thetaLong / 2^63 must scale the estimate (got {})",
            s.estimate()
        );
    }

    #[test]
    fn druid_boundary_theta_hash_survives_union() {
        // thetaLong = i64::MAX with retained h = i64::MAX - 1: theta and
        // normalise(h << 1) BOTH round to 1.0 in f64, so the old f64
        // union filter dropped a hash the integer decode had retained —
        // cardinality 1 silently became 0.  The retained-vs-threshold
        // comparison is now integer-exact (`h << 1 < thetaLong << 1`).
        let h = u64::try_from(i64::MAX - 1).expect("positive");
        let img = compact_image(3, 0, i64::MAX, &[h]);
        let s = ThetaSketch::from_druid_compact(&img).expect("decode boundary image");
        assert_eq!(s.retained(), 1);
        let u = s.union(&ThetaSketch::empty_druid_origin()).expect("union");
        assert_eq!(u.retained(), 1, "boundary hash must survive the union");
        assert!((u.estimate() - 1.0).abs() < 1e-9);
        // …and survive the partial-state wire round-trip too.
        let restored = ThetaSketch::deserialize(&s.serialize()).expect("round-trip");
        let u2 = restored.union(&s).expect("post-wire union");
        assert_eq!(u2.retained(), 1);
        assert!((u2.estimate() - 1.0).abs() < 1e-9);
    }

    #[test]
    fn druid_hash_exactly_at_threshold_is_excluded_consistently() {
        // Sketch A estimates with thetaLong = 2^62; sketch B is exact-mode
        // and holds h = 2^62 — exactly A's threshold.  The union adopts
        // the min threshold and must EXCLUDE the at-threshold hash in the
        // same integer space the decoder uses (decode itself refuses
        // `h >= thetaLong`), keeping only the strictly-below hash.
        let theta_long = 1i64 << 62;
        let below = (1u64 << 62) - 1;
        let a = ThetaSketch::from_druid_compact(&compact_image(3, 0, theta_long, &[below]))
            .expect("decode a");
        let b = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1u64 << 62]))
            .expect("decode b");
        let u = a.union(&b).expect("druid union");
        assert_eq!(u.retained(), 1, "the at-threshold hash must be excluded");
        let u_rev = b.union(&a).expect("druid union (reversed)");
        assert_eq!(u_rev.retained(), 1);
        // 1 retained / theta 0.5 = 2.
        assert!((u.estimate() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn theta_v2_exact_bound_round_trip_admits_nothing_new() {
        // thetaLong = i64::MAX - 1023 (= 2^63 - 1024): the exact stored
        // bound is 2^64 - 2048 — the LARGEST f64-representable integer
        // below 2^64 — so the old conservative f64 reconstruction
        // (one-ULP widening + ceil) landed at 2^64 and clamped to
        // u64::MAX: a hash EXACTLY at thetaLong (excluded pre-serialize)
        // was ADMITTED after deserialize→union, and retained count 1
        // silently became 2, skewing the estimate.  The v2 wire now
        // carries the exact integer bound instead.
        let theta_long = i64::MAX - 1023;
        let below = u64::try_from(theta_long - 1).expect("positive");
        let at = u64::try_from(theta_long).expect("positive");
        let a = ThetaSketch::from_druid_compact(&compact_image(3, 0, theta_long, &[below]))
            .expect("decode a");
        // Exact-mode sketch holding a hash exactly AT a's threshold.
        let b = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[at])).expect("decode b");
        // Pre-serialize: the at-threshold hash is excluded, the
        // just-below one is retained.
        let u = a.union(&b).expect("pre-wire union");
        assert_eq!(u.retained(), 1, "at-threshold hash excluded pre-wire");
        // Post-round-trip the union must behave IDENTICALLY: neither
        // admit the at-threshold hash nor drop the just-below one.
        let ra = ThetaSketch::deserialize(&a.serialize()).expect("round-trip a");
        assert_eq!(ra.theta_bound, a.theta_bound, "exact bound on the wire");
        let u2 = ra.union(&b).expect("post-wire union");
        assert_eq!(
            u2.retained(),
            1,
            "the wire round-trip must not admit the at-threshold hash \
             (exact bound, not an f64-widened one)"
        );
        assert!((u2.estimate() - u.estimate()).abs() < f64::EPSILON);
        // The just-below boundary hash is the one retained and survives a
        // further round-trip of the union itself.
        let u3 = ThetaSketch::deserialize(&u2.serialize()).expect("round-trip union");
        assert_eq!(u3.retained(), 1);
        assert!(
            u3.hashes.contains(&(below << 1)),
            "just-below hash survives"
        );
    }

    #[test]
    fn theta_v2_wire_preserves_exact_bound_and_seed() {
        // The full Druid-origin identity `(theta_bound, seed_hash)` must
        // survive the partial-state wire EXACTLY.
        let theta_long = (1i64 << 62) + 12345;
        let s = ThetaSketch::from_druid_compact(&compact_image_seeded(
            3,
            0,
            theta_long,
            0xCAFE,
            &[10, 20, 30],
        ))
        .expect("decode");
        let restored = ThetaSketch::deserialize(&s.serialize()).expect("round-trip");
        assert_eq!(restored.theta_bound, s.theta_bound, "exact theta_bound");
        assert_eq!(restored.theta_bound, druid_theta_bound(theta_long));
        assert_eq!(restored.druid_seed_hash(), Some(0xCAFE), "exact seed hash");
        assert_eq!(restored.retained(), 3);
        assert!((restored.estimate() - s.estimate()).abs() < f64::EPSILON);
    }

    // --- v2 coherence guards (a crafted image must not poison unions) ---

    #[test]
    fn theta_v2_incoherent_bound_vs_theta_rejected() {
        // `[theta_bound=0, theta=1.0, count=0]` used to deserialize fine;
        // unioning it selected `min(bounds) = 0`, which filtered out EVERY
        // retained hash of the other side — a crafted image silently
        // zeroed any query it merged into.  theta is REDUNDANT with the
        // exact bound (`theta == theta_bound / 2^64`), so a disagreeing
        // pair is incoherent and must be rejected loudly.
        let err = ThetaSketch::deserialize(&v2_image(ORIGIN_FLAG_DRUID, 0, None, 1.0, &[]))
            .expect_err("bound 0 with theta 1.0 is incoherent");
        assert!(err.to_string().contains("theta_bound"), "got: {err}");
        // bound = 0 is rejected even with the "matching" wire theta 0.0:
        // a zero threshold retains nothing, while theta must be positive.
        assert!(ThetaSketch::deserialize(&v2_image(ORIGIN_FLAG_DRUID, 0, None, 0.0, &[])).is_err());
        // Non-zero but mismatched: the bound says theta 0.5, the wire f64
        // says 1.0.
        assert!(
            ThetaSketch::deserialize(&v2_image(
                ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH,
                1u64 << 63,
                Some(0x1234),
                1.0,
                &[2],
            ))
            .is_err()
        );
        // A NaN wire theta can never match a derived value.
        assert!(
            ThetaSketch::deserialize(&v2_image(ORIGIN_FLAG_DRUID, u64::MAX, None, f64::NAN, &[]))
                .is_err()
        );
    }

    #[test]
    fn theta_v2_retained_hash_at_or_above_bound_rejected() {
        // A coherent header (bound = 2^63 ⇔ theta = 0.5) carrying a
        // retained hash AT the bound: retention is strictly-below
        // everywhere (decode refuses `h >= thetaLong`, unions filter
        // `h < bound`), so such an image is incoherent — a crafted one
        // bypasses the decode-time guard and must be caught on the wire.
        let flags = ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH;
        let (bound, theta) = (1u64 << 63, 0.5);
        let err = ThetaSketch::deserialize(&v2_image(flags, bound, Some(0x1234), theta, &[bound]))
            .expect_err("retained hash at the bound is incoherent");
        assert!(err.to_string().contains("strictly below"), "got: {err}");
        // …and above the bound.
        assert!(
            ThetaSketch::deserialize(&v2_image(flags, bound, Some(0x1234), theta, &[bound + 2]))
                .is_err()
        );
        // The same coherent header with a strictly-below hash deserializes
        // and estimates 1 / 0.5 = 2.
        let s =
            ThetaSketch::deserialize(&v2_image(flags, bound, Some(0x1234), theta, &[bound - 2]))
                .expect("coherent image deserializes");
        assert_eq!(s.retained(), 1);
        assert!((s.estimate() - 2.0).abs() < 1e-9);
    }

    #[test]
    fn theta_v2_theta_always_derivable_from_bound() {
        // Every legitimately-built Druid-origin sketch keeps
        // `theta == theta_bound / 2^64` BIT-EXACTLY (decode:
        // `thetaLong / 2^63 == (thetaLong << 1) / 2^64`; the neutral
        // `u64::MAX` bound maps to exactly 1.0) — including the
        // rounding-tight thetaLong values at the f64 boundary — so the
        // wire round-trip always passes the coherence check.
        for theta_long in [1i64, 2, (1 << 62) + 12345, i64::MAX - 1023, i64::MAX] {
            let s = ThetaSketch::from_druid_compact(&compact_image(3, 0, theta_long, &[]))
                .unwrap_or_else(|e| panic!("decode thetaLong {theta_long}: {e}"));
            assert_eq!(
                s.theta.to_bits(),
                druid_theta_from_bound(s.theta_bound).to_bits(),
                "decode coherence at thetaLong {theta_long}"
            );
            let r = ThetaSketch::deserialize(&s.serialize())
                .unwrap_or_else(|e| panic!("round-trip thetaLong {theta_long}: {e}"));
            assert_eq!(r.theta_bound, s.theta_bound);
            assert_eq!(r.theta.to_bits(), s.theta.to_bits());
        }
        let e = ThetaSketch::empty_druid_origin();
        assert_eq!(
            e.theta.to_bits(),
            druid_theta_from_bound(e.theta_bound).to_bits()
        );
    }

    #[test]
    fn theta_empty_operand_bound_is_neutral_in_set_ops() {
        // A DEGENERATE empty-but-estimating image (curCount 0, tiny
        // thetaLong) is coherent and legal on the wire; before the fix
        // its tiny bound joined the `min(bound)` selection and filtered
        // out EVERY retained hash of the non-empty side (estimate
        // silently 0).  An empty operand retains nothing, so it must
        // never lower the result's threshold.
        let degenerate = ThetaSketch::from_druid_compact(&compact_image(3, 0, 1, &[]))
            .expect("decode degenerate empty");
        assert_eq!(degenerate.retained(), 0);
        let s = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2, 3]))
            .expect("decode non-empty");
        for u in [
            s.union(&degenerate).expect("s ∪ degenerate"),
            degenerate.union(&s).expect("degenerate ∪ s"),
        ] {
            assert_eq!(u.retained(), 3, "empty operand must not drop hashes");
            assert!(
                (u.estimate() - 3.0).abs() < f64::EPSILON,
                "cardinality unchanged (never 0), got {}",
                u.estimate()
            );
        }
        // difference/intersect: same neutrality (A \ ∅ used to drop the
        // hashes at/above the empty side's bound).
        let d = s.difference(&degenerate).expect("s \\ degenerate");
        assert_eq!(d.retained(), 3);
        assert!((d.estimate() - 3.0).abs() < f64::EPSILON);
        let i = s.intersect(&degenerate).expect("s ∩ degenerate");
        assert_eq!(i.retained(), 0);
        // The degenerate sketch survives the wire (it IS coherent) and
        // still cannot poison a post-wire union.
        let rd = ThetaSketch::deserialize(&degenerate.serialize()).expect("round-trip degenerate");
        let u = s.union(&rd).expect("post-wire union");
        assert!((u.estimate() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn oversized_max_size_is_bounded_never_zero_on_the_wire() {
        // A query-controlled size beyond u32::MAX used to truncate to 0 in
        // the partial-state bytes (`as u32`), and the 0 budget then
        // trimmed EVERY retained hash on the next merge.  The budget is
        // now clamped at construction and the wire field cannot wrap.
        let oversize = usize::try_from(u64::from(u32::MAX) + 1).expect("64-bit usize");
        let mut sketch = ThetaSketch::new(oversize);
        for i in 0_u32..10 {
            sketch.add(&i.to_le_bytes()).expect("native add");
        }
        let restored = ThetaSketch::deserialize(&sketch.serialize()).expect("round-trip");
        assert_eq!(restored.max_size, DS_MAX_RETAINED, "clamped, not truncated");
        assert_ne!(restored.max_size, 0, "no silent zero budget");
        // A post-round-trip merge keeps every hash instead of trimming
        // the whole set.
        let merged = restored.union(&ThetaSketch::default_size()).expect("union");
        assert!((merged.estimate() - 10.0).abs() < f64::EPSILON);
    }

    #[test]
    fn druid_compact_unions_are_exact_in_druid_space() {
        // Two Druid-origin sketches over overlapping hash sets: the union
        // deduplicates in the SAME hash space, so the estimate is exact.
        let a =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2, 3])).expect("decode a");
        let b =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[3, 4])).expect("decode b");
        let u = a.union(&b).expect("druid ∪ druid");
        assert!((u.estimate() - 4.0).abs() < f64::EPSILON);
        assert!(u.is_druid_origin());
    }

    #[test]
    fn druid_compact_truncated_and_malformed_fail_closed() {
        // Truncated preamble.
        assert!(ThetaSketch::from_druid_compact(&[1, 3, 3]).is_err());
        // Declared count not backed by bytes.
        let mut img = compact_image(2, 0, 0, &[1, 2, 3]);
        img.truncate(img.len() - 8);
        assert!(ThetaSketch::from_druid_compact(&img).is_err());
        // Trailing garbage after the declared hashes.
        let mut img = compact_image(2, 0, 0, &[1, 2, 3]);
        img.extend_from_slice(&[0xAB; 4]);
        assert!(ThetaSketch::from_druid_compact(&img).is_err());
        // Wrong serial version / family.
        let mut img = compact_image(2, 0, 0, &[1]);
        img[1] = 2;
        assert!(ThetaSketch::from_druid_compact(&img).is_err());
        let mut img = compact_image(2, 0, 0, &[1]);
        img[2] = 4;
        assert!(ThetaSketch::from_druid_compact(&img).is_err());
        // A hash with the top bit set can never be a DataSketches hash.
        assert!(ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1u64 << 63])).is_err());
        // A hash at/beyond thetaLong is inconsistent with retention.
        assert!(
            ThetaSketch::from_druid_compact(&compact_image(3, 0, 1 << 62, &[1u64 << 62])).is_err()
        );
        // Non-positive thetaLong.
        assert!(ThetaSketch::from_druid_compact(&compact_image(3, 0, 0, &[1])).is_err());
        // Unsupported preamble-long count.
        assert!(ThetaSketch::from_druid_compact(&compact_image(4, 0, 0, &[])).is_err());
    }

    // --- Malformed-image guards: duplicate / non-ascending retained hashes ---

    #[test]
    fn druid_compact_duplicate_retained_hashes_rejected() {
        // A genuine Apache compact sketch has DISTINCT retained hashes; a
        // malformed `curCount = 2` image carrying `[7, 7]` used to collapse
        // to ONE retained hash in the BTreeSet, silently halving
        // `retained / theta`.  It must fail loudly instead.
        let err = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[7, 7]))
            .expect_err("duplicate retained hashes must be rejected");
        assert!(err.to_string().contains("duplicate"), "got: {err}");
        // Same in estimating (preLongs 3) mode.
        assert!(ThetaSketch::from_druid_compact(&compact_image(3, 0, 1 << 62, &[9, 9])).is_err());
    }

    #[test]
    fn druid_compact_empty_flag_must_be_canonical() {
        // A canonical Apache EMPTY compact sketch is EXACTLY the 8-byte
        // preLongs-1 preamble (curCount 0, theta 1.0, no hashes).  An
        // EMPTY-flagged preLongs-3 image declaring curCount = 1 with no
        // retained-hash bytes (24 bytes = 3 preamble longs) used to
        // decode silently as cardinality 0, discarding the declared
        // state.  A noncanonical EMPTY image is corrupt — fail loud.
        let mut img = compact_image(3, DS_FLAG_EMPTY, i64::MAX, &[]);
        assert_eq!(img.len(), 24);
        img[8] = 1; // declare curCount = 1 in the preLongs-3 preamble
        let err = ThetaSketch::from_druid_compact(&img)
            .expect_err("EMPTY flag with declared curCount 1 must be rejected");
        assert!(err.to_string().contains("EMPTY"), "got: {err}");
        // EMPTY on ANY preamble shape other than the canonical 8-byte
        // preLongs-1 form is rejected, even with curCount = 0.
        assert!(ThetaSketch::from_druid_compact(&compact_image(2, DS_FLAG_EMPTY, 0, &[])).is_err());
        assert!(
            ThetaSketch::from_druid_compact(&compact_image(3, DS_FLAG_EMPTY, i64::MAX, &[]))
                .is_err()
        );
        // EMPTY with trailing retained-hash bytes is rejected.
        let mut img = compact_image(1, DS_FLAG_EMPTY, 0, &[]);
        img.extend_from_slice(&7u64.to_le_bytes());
        assert!(ThetaSketch::from_druid_compact(&img).is_err());
        // The canonical preLongs-1 8-byte EMPTY image still decodes.
        let s = ThetaSketch::from_druid_compact(&compact_image(1, DS_FLAG_EMPTY, 0, &[]))
            .expect("canonical empty decodes");
        assert_eq!(s.retained(), 0);
        assert!((s.estimate() - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn theta_v2_duplicate_retained_hashes_rejected() {
        // A coherent Druid-origin v2 wire image declaring count = 2 with
        // hashes [2, 2] used to collapse to ONE retained hash in the
        // BTreeSet, silently halving `retained / theta`.  A duplicate is
        // corrupt — fail loud, never silently deduplicate.
        let flags = ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH;
        let (bound, theta) = (1u64 << 63, 0.5);
        let err = ThetaSketch::deserialize(&v2_image(flags, bound, Some(0x1234), theta, &[2, 2]))
            .expect_err("duplicate v2 retained hashes must be rejected");
        assert!(err.to_string().contains("duplicate"), "got: {err}");
        // A legit distinct set still deserializes with the full declared
        // count and the unhalved estimate (2 / 0.5 = 4).
        let s = ThetaSketch::deserialize(&v2_image(flags, bound, Some(0x1234), theta, &[2, 4]))
            .expect("distinct v2 image deserializes");
        assert_eq!(s.retained(), 2);
        assert!((s.estimate() - 4.0).abs() < 1e-9);
        // …and round-trips unchanged through the legitimate serializer.
        let r = ThetaSketch::deserialize(&s.serialize()).expect("round-trip");
        assert_eq!(r.retained(), 2);
        assert!((r.estimate() - 4.0).abs() < 1e-9);
    }

    #[test]
    fn theta_v1_duplicate_retained_hashes_rejected() {
        // The native v1 wire path shares the distinct-retained invariant:
        // a count = 2 image carrying [7, 7] is corrupt (the serializer
        // writes from a set and can never produce it) — fail loud.
        let err = ThetaSketch::deserialize(&v1_image(1.0, &[7, 7]))
            .expect_err("duplicate v1 retained hashes must be rejected");
        assert!(err.to_string().contains("duplicate"), "got: {err}");
        // A distinct v1 image still deserializes in full.
        let s = ThetaSketch::deserialize(&v1_image(1.0, &[7, 9])).expect("distinct v1 image");
        assert_eq!(s.retained(), 2);
        assert!(!s.is_druid_origin());
        assert!((s.estimate() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn druid_compact_ordered_flag_requires_strictly_ascending() {
        // A COMPACT+ORDERED image is strictly ascending by construction; a
        // non-ascending one is malformed.
        let err = ThetaSketch::from_druid_compact(&compact_image(2, DS_FLAG_ORDERED, 0, &[7, 5]))
            .expect_err("non-ascending ORDERED image must be rejected");
        assert!(err.to_string().contains("ascending"), "got: {err}");
        // A legit distinct ascending ORDERED image still decodes.
        let s = ThetaSketch::from_druid_compact(&compact_image(2, DS_FLAG_ORDERED, 0, &[5, 7]))
            .expect("ascending ORDERED image decodes");
        assert_eq!(s.retained(), 2);
        assert!((s.estimate() - 2.0).abs() < f64::EPSILON);
        // Without the ORDERED flag, distinct non-ascending hashes are legal.
        let s = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[9, 3]))
            .expect("unordered distinct image decodes");
        assert_eq!(s.retained(), 2);
    }

    // --- Seed-hash guard (cross-seed unions corrupt the estimate) ---

    #[test]
    fn druid_cross_seed_set_ops_are_refused() {
        // Two sketches built with DIFFERENT update seeds hash the same
        // logical value to different MurmurHash3 points: unioning them
        // double-counts (estimate 2 for one logical value).  A seed-hash
        // mismatch gets the HashSpaceMismatch treatment.
        let a = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2])).expect("a");
        let b = ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0xBEEF, &[1, 2]))
            .expect("b");
        for (op, res) in [
            ("union", a.union(&b).err()),
            ("intersect", a.intersect(&b).err()),
            ("difference", a.difference(&b).err()),
        ] {
            let err = res.unwrap_or_else(|| panic!("cross-seed {op} must be refused"));
            assert!(matches!(err, SketchError::HashSpaceMismatch(_)), "{err}");
        }
    }

    #[test]
    fn druid_same_seed_union_and_empty_seed_adoption() {
        // Same seed hash on both sides → the union proceeds normally.
        let a = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2])).expect("a");
        let b = ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[2, 3])).expect("b");
        let u = a.union(&b).expect("same-seed union");
        assert!((u.estimate() - 3.0).abs() < f64::EPSILON);
        assert_eq!(u.druid_seed_hash(), Some(TEST_SEED_HASH));
        // An empty sketch is seed-neutral: it adopts the other side's
        // seed, in either operand order.
        let seeded =
            ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0xABCD, &[1, 2]))
                .expect("seeded");
        let u = ThetaSketch::empty_druid_origin()
            .union(&seeded)
            .expect("empty ∪ seeded");
        assert_eq!(u.druid_seed_hash(), Some(0xABCD));
        let u = seeded
            .union(&ThetaSketch::empty_druid_origin())
            .expect("seeded ∪ empty");
        assert_eq!(u.druid_seed_hash(), Some(0xABCD));
        // …and an empty NATIVE side adopts the Druid seed too.
        let u = ThetaSketch::default_size()
            .union(&seeded)
            .expect("native-empty ∪ seeded");
        assert_eq!(u.druid_seed_hash(), Some(0xABCD));
        assert!(u.is_druid_origin());
    }

    #[test]
    fn druid_seed_hash_survives_wire_round_trip() {
        let s = ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0xABCD, &[1, 2]))
            .expect("decode");
        assert_eq!(s.druid_seed_hash(), Some(0xABCD));
        let restored = ThetaSketch::deserialize(&s.serialize()).expect("round-trip");
        assert!(restored.is_druid_origin());
        assert_eq!(
            restored.druid_seed_hash(),
            Some(0xABCD),
            "seed survives the wire"
        );
        // The restored sketch still refuses a cross-seed union…
        let other_seed =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[9])).expect("decode");
        assert!(restored.union(&other_seed).is_err());
        // …and unions normally with a same-seed sketch.
        let same_seed =
            ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0xABCD, &[9]))
                .expect("decode");
        let u = restored
            .union(&same_seed)
            .expect("same-seed post-wire union");
        assert_eq!(u.retained(), 3);
    }

    #[test]
    fn theta_v2_seedless_wire_is_legal_only_when_empty() {
        // An EMPTY Druid-origin sketch has no seed → the v2 image carries
        // no seed-hash bit; empty means seed-neutral, so it deserializes.
        let bytes = ThetaSketch::empty_druid_origin().serialize();
        assert_eq!(bytes[0], VERSION_DRUID);
        assert_eq!(bytes[1], ORIGIN_FLAG_DRUID);
        let restored = ThetaSketch::deserialize(&bytes).expect("empty seed-less v2");
        assert!(restored.is_druid_origin());
        assert_eq!(restored.druid_seed_hash(), None);
        assert_eq!(restored.retained(), 0);
        // A hand-crafted NON-empty v2 image WITHOUT a seed hash (spliced
        // from a seeded one: drop the seed bit + the 2 seed bytes after
        // the u64 theta bound) must be REJECTED: a non-empty Druid-origin
        // sketch always has a known seed, and deserializing it as
        // seed-neutral let two different-seed sketches union silently
        // (double-count).
        let seeded =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2])).expect("decode");
        let mut seedless_bytes = seeded.serialize();
        assert_eq!(seedless_bytes[1], ORIGIN_FLAG_DRUID | ORIGIN_FLAG_SEED_HASH);
        seedless_bytes[1] = ORIGIN_FLAG_DRUID;
        seedless_bytes.drain(10..12); // seed hash sits after [ver][flags][bound u64]
        let err = ThetaSketch::deserialize(&seedless_bytes)
            .expect_err("non-empty seed-less v2 must be rejected");
        assert!(err.to_string().contains("seed"), "got: {err}");
    }

    #[test]
    fn theta_v2_cross_seed_refusal_survives_wire_round_trip() {
        // Two different-seed non-empty sketches must STILL refuse to union
        // after both sides pass through the partial-state wire — no
        // seed-neutral bypass exists on the round trip.
        let a = ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0x1111, &[1, 2]))
            .expect("a");
        let b = ThetaSketch::from_druid_compact(&compact_image_seeded(2, 0, 0, 0x2222, &[1, 2]))
            .expect("b");
        let ra = ThetaSketch::deserialize(&a.serialize()).expect("round-trip a");
        let rb = ThetaSketch::deserialize(&b.serialize()).expect("round-trip b");
        assert_eq!(ra.druid_seed_hash(), Some(0x1111));
        assert_eq!(rb.druid_seed_hash(), Some(0x2222));
        let err = ra
            .union(&rb)
            .expect_err("cross-seed post-wire union must be refused");
        assert!(matches!(err, SketchError::HashSpaceMismatch(_)), "{err}");
    }

    #[test]
    fn druid_same_seed_a_to_b_constants_preserved() {
        // Mirror of the real-Druid A→B harness (`sketch_rollup_day`): two
        // US rows whose user sets overlap ({h1,h2} and {h2}) and one JP
        // row ({h3,h4}).  Every row shares Druid's default seed hash, so
        // the unions proceed normally: US=2, JP=2, total=4.
        let (h1, h2, h3, h4) = (1_000u64, 2_000, 3_000, 4_000);
        let us1 =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h1, h2])).expect("US row 1");
        let us2 =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h2])).expect("US row 2");
        let jp =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h3, h4])).expect("JP row");
        let us = us1.union(&us2).expect("same-seed US union");
        assert!((us.estimate() - 2.0).abs() < f64::EPSILON, "US=2");
        assert!((jp.estimate() - 2.0).abs() < f64::EPSILON, "JP=2");
        let total = us.union(&jp).expect("same-seed total union");
        assert!((total.estimate() - 4.0).abs() < f64::EPSILON, "total=4");
        assert_eq!(total.druid_seed_hash(), Some(TEST_SEED_HASH));
    }

    #[test]
    fn druid_a_to_b_constants_survive_wire_round_trip() {
        // The same A→B harness constants (US=2, JP=2, total=4) with EVERY
        // sketch pushed through the partial-state wire before each union —
        // mirrors a broker merge of serialized per-row state.
        let wire = |s: &ThetaSketch| ThetaSketch::deserialize(&s.serialize()).expect("round-trip");
        let (h1, h2, h3, h4) = (1_000u64, 2_000, 3_000, 4_000);
        let us1 =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h1, h2])).expect("US row 1");
        let us2 =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h2])).expect("US row 2");
        let jp =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[h3, h4])).expect("JP row");
        let us = wire(&us1).union(&wire(&us2)).expect("post-wire US union");
        assert!((us.estimate() - 2.0).abs() < f64::EPSILON, "US=2");
        let jp = wire(&jp);
        assert!((jp.estimate() - 2.0).abs() < f64::EPSILON, "JP=2");
        let total = wire(&us).union(&jp).expect("post-wire total union");
        assert!((total.estimate() - 4.0).abs() < f64::EPSILON, "total=4");
        assert_eq!(total.druid_seed_hash(), Some(TEST_SEED_HASH));
    }

    // --- Hash-space guard (union-only Druid-origin sketches) ---

    #[test]
    fn druid_origin_refuses_raw_adds() {
        let mut s =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2])).expect("decode");
        let err = s.add(b"new value").expect_err("raw add must be refused");
        assert!(matches!(err, SketchError::HashSpaceMismatch(_)), "{err}");
        assert_eq!(s.retained(), 2, "the sketch must be left unchanged");
        let err = s.add_hash(42).expect_err("raw hash must be refused");
        assert!(matches!(err, SketchError::HashSpaceMismatch(_)), "{err}");
    }

    #[test]
    fn cross_origin_union_of_non_empty_sketches_is_refused() {
        let druid =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2])).expect("decode");
        let mut native = ThetaSketch::default_size();
        native.add(b"x").expect("native add");
        for (op, res) in [
            ("union", native.union(&druid).err()),
            ("intersect", native.intersect(&druid).err()),
            ("difference", native.difference(&druid).err()),
        ] {
            let err = res.unwrap_or_else(|| panic!("{op} across hash spaces must be refused"));
            assert!(matches!(err, SketchError::HashSpaceMismatch(_)), "{err}");
        }
    }

    #[test]
    fn empty_native_side_unions_into_druid_space() {
        // The aggregator's fresh (empty, native) sketch must union cleanly
        // with a Druid-origin envelope — the empty side is space-neutral.
        let druid =
            ThetaSketch::from_druid_compact(&compact_image(2, 0, 0, &[1, 2, 3])).expect("decode");
        let empty_native = ThetaSketch::default_size();
        let u = empty_native.union(&druid).expect("empty-native ∪ druid");
        assert!((u.estimate() - 3.0).abs() < f64::EPSILON);
        assert!(u.is_druid_origin(), "the union adopts the Druid space");
    }

    #[test]
    fn druid_origin_survives_serialize_round_trip() {
        let s = ThetaSketch::from_druid_compact(&compact_image(3, 0, 1 << 62, &[10, 20]))
            .expect("decode");
        let bytes = s.serialize();
        assert_eq!(bytes[0], VERSION_DRUID, "druid-origin serializes as v2");
        let restored = ThetaSketch::deserialize(&bytes).expect("deserialize v2");
        assert!(restored.is_druid_origin(), "origin must survive the wire");
        assert!((restored.estimate() - s.estimate()).abs() < f64::EPSILON);
        // Bad origin flags fail closed.
        let mut bad = bytes;
        bad[1] = 0x02;
        assert!(ThetaSketch::deserialize(&bad).is_err());
    }
}
