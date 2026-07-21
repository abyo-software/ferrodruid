// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Decoded Apache Druid `hyperUnique` HyperLogLog sketches (W-A, v1.5.0).
//!
//! [`DruidHyperUnique`] holds the DECODED register state of one on-disk
//! Druid `hyperUnique` complex-metric blob.  The format and the estimator
//! below were reverse-engineered strictly BLACK-BOX, by iterating against
//! captured oracle fixtures (real Druid 31.0.2 segments + `dump-segment`
//! output + native/SQL query answers recorded under
//! `tests/segment-compat/fixtures/hyperunique_druid31/`) plus the public
//! HyperLogLog literature (Flajolet et al., 2007) for the estimator math.
//! No Apache Druid source code was read (clean room).
//!
//! # Observed blob format (fixture-pinned)
//!
//! Every blob starts with a 7-byte header:
//!
//! | bytes | meaning (observed) |
//! |---|---|
//! | 0 | version, always `0x01` |
//! | 1 | register offset — `0x00` in every captured blob; a non-zero value is REJECTED loudly (never observed, semantics unverified) |
//! | 2-3 | big-endian u16 count of non-zero 4-bit registers |
//! | 4 | max-overflow VALUE (`0x00` = none; observed `0x10` = one past the 4-bit ceiling) |
//! | 5-6 | big-endian u16 max-overflow REGISTER index (< 2048) |
//!
//! The body is one of two shapes:
//!
//! * **dense** — exactly 1024 bytes: a packed register page holding 2048
//!   4-bit registers, two per byte;
//! * **sparse** — exactly `declared_non_zero_register_count × 3` bytes:
//!   3-byte `(big-endian u16 position, packed register byte)` entries,
//!   ONE PER NON-ZERO PAGE BYTE, strictly ascending, followed by
//!   all-zero PADDING entries whenever a page byte holds both of its
//!   4-bit registers (entry count < register count).  Positions are
//!   BUFFER-relative — they count from the start of the whole blob,
//!   header included, so the first register-page byte is position 7 and
//!   the last is 7 + 1023 = 1030.  An entry carries BOTH 4-bit registers
//!   of its page byte.
//!
//!   Oracle-pinned (2026-07-20, Druid 31.0.2, fixtures under
//!   `tests/segment-compat/fixtures/hyperunique_sparse_druid31/`): a
//!   3000-single-user census spans positions EXACTLY `7..=1030` (the
//!   page-relative-only zone `0..=6` is empty, twenty-two positions land
//!   past 1023), a captured 900-user dense page nibble-covers every
//!   constituent user's single-user sparse value at `page[position - 7]`
//!   (900/900; at `page[position]` only 202/900 — chance), and the fold
//!   of two users sharing one page byte produced
//!   `01 00 0002 00 0000 | 00 08 11 | 00 00 00` — one both-nibbles
//!   entry, declared count 2, one zero-padded tail entry.
//!
//! Which 4-bit nibble of a page byte is the even-indexed register is NOT
//! pinned by the oracle (both assignments reproduce every captured answer,
//! because the estimator below is register-ORDER independent and the one
//! overflow-adjacent page byte in the fixtures is `0x00`).  This decoder
//! fixes the LOW nibble as the even register — an arbitrary but internally
//! consistent choice with no externally observable effect on estimates or
//! merges.
//!
//! # Overflow pair is estimator-inert (fixture-pinned)
//!
//! The dense US fixture blob carries `maxOverflowValue = 16` at register
//! 1453 while that register's stored nibble is 0 — and the oracle's
//! estimate for that sketch (`707.1747087253059`) is reproduced ONLY when
//! register 1453 counts as ZERO in the estimator.  The merged JP∪US oracle
//! total (`1190.8281757103275`) likewise requires the fold to keep that
//! register zero.  The overflow pair is therefore decoded, validated and
//! carried through merges as BOOKKEEPING, but never materialized into the
//! register array.
//!
//! # Estimator (fixture-pinned in the linear-counting regime)
//!
//! With `m = 2048` registers, register values `v_r`, `V = |{r : v_r = 0}|`
//! and `S = Σ_r 2^-v_r`:
//!
//! * `E_raw = α_m · m² / S` with `α_m = 0.7213 / (1 + 1.079/m)` (the
//!   standard HyperLogLog bias constant for m ≥ 128 from the public
//!   literature);
//! * when `E_raw ≤ 2.5·m` AND `V > 0` the estimate is the linear-counting
//!   value `m · ln(m / V)` — this branch reproduces EVERY captured oracle
//!   double bit-for-bit (12.03529418544122 across three segmentations,
//!   per-day 8.015665809687173 / 10.024493827539368, per-country
//!   6.008806266444944 / 8.015665809687173, dense 694.502272279783 /
//!   707.1747087253059 / merged 1190.8281757103275);
//! * otherwise the estimate is `E_raw` itself.  No captured fixture
//!   reaches this branch (all stay in the linear-counting regime), so the
//!   raw branch — and the exact `α_m` constant — follows the public
//!   literature and is NOT oracle-verified.  Documented honest limitation.
//!
//! # Merge-only, never mixed
//!
//! Druid hashes raw values with an unknown (to this clean-room decoder)
//! hash function, so a decoded sketch can NEVER accept a raw value add —
//! the type simply has no `add` method, making the mix impossible at
//! compile time.  It must also never be merged with FerroDruid's native
//! FNV-space [`crate::HllSketch`] or a DataSketches `HLLSketch`: those
//! types are unrelated and carry no conversion into this one.  Merging two
//! [`DruidHyperUnique`] sketches is the register-wise MAX — fixture-pinned:
//! the captured rollup-folded blobs' register sets are exactly the
//! pair-wise max/union of their constituent single-user blobs.

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

use crate::{Result, SketchError};

/// Number of 4-bit registers in a Druid `hyperUnique` sketch (fixture:
/// dense page = 1024 bytes = 2048 nibbles; sparse buffer-relative
/// positions span `7..=1030`, one per page byte).
pub const NUM_REGISTERS: usize = 2048;

/// Bytes in the dense packed-register page (two 4-bit registers per byte).
const REGISTER_PAGE_BYTES: usize = 1024;

/// Length of the blob header (version, offset, numNonZero, overflow pair).
const HEADER_BYTES: usize = 7;

/// The only observed blob version byte.
const BLOB_VERSION: u8 = 0x01;

/// Serialized wire version for the FerroDruid partial-state round-trip
/// (never written to segments; segments carry the Druid blob form).
const WIRE_VERSION: u8 = 1;

/// Exact wire image length: `[version][overflow value][overflow register
/// LE u16]` + one byte per register.
const WIRE_LEN: usize = 4 + NUM_REGISTERS;

/// Largest value a packed 4-bit register can hold.
const MAX_NIBBLE: u8 = 0x0F;

/// Register count as `f64` (`m` in the HyperLogLog literature; exact).
const M_F64: f64 = 2048.0;

/// HyperLogLog bias-correction constant `α_m = 0.7213 / (1 + 1.079/m)`
/// for `m = 2048` (standard approximation for m ≥ 128 from the public
/// literature).  Only exercised by the raw (non-linear-counting) branch,
/// which no oracle fixture reaches — see the module docs.
const ALPHA_M: f64 = 0.7213 / (1.0 + 1.079 / M_F64);

/// The `maxOverflowValue`/`maxOverflowRegister` bookkeeping pair of a
/// decoded blob (see the module docs — estimator-inert).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Overflow {
    /// The overflow value — always at least one past the 4-bit ceiling
    /// (`> 15`); a smaller "overflow" would fit a register nibble and is
    /// rejected as incoherent.
    value: u8,
    /// The register index the overflow belongs to (< [`NUM_REGISTERS`]).
    register: u16,
}

/// A DECODED Apache Druid `hyperUnique` HyperLogLog sketch (merge-only —
/// see the module docs; there is deliberately NO way to add a raw value).
#[derive(Clone, PartialEq, Eq)]
pub struct DruidHyperUnique {
    /// Normalized per-register values.  With the only accepted register
    /// offset (0), each value is the stored 4-bit nibble (`0..=15`).
    registers: Box<[u8; NUM_REGISTERS]>,
    /// Estimator-inert max-overflow bookkeeping (see the module docs).
    overflow: Option<Overflow>,
}

impl std::fmt::Debug for DruidHyperUnique {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DruidHyperUnique")
            .field("non_zero_registers", &self.num_non_zero())
            .field("overflow", &self.overflow)
            .finish_non_exhaustive()
    }
}

impl Default for DruidHyperUnique {
    fn default() -> Self {
        Self::empty()
    }
}

impl DruidHyperUnique {
    /// An EMPTY sketch: every register zero, estimate exactly `0.0`, and a
    /// merge no-op.  Represents a null/absent row of a migrated
    /// `hyperUnique` column so the whole column stays uniformly typed.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            registers: Box::new([0u8; NUM_REGISTERS]),
            overflow: None,
        }
    }

    /// Whether every register is zero (and no overflow is recorded).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.overflow.is_none() && self.registers.iter().all(|&v| v == 0)
    }

    /// Number of non-zero registers.
    #[must_use]
    pub fn num_non_zero(&self) -> usize {
        self.registers.iter().filter(|&&v| v != 0).count()
    }

    /// Decode an on-disk Druid `hyperUnique` blob (the per-row payload of
    /// a `COMPLEX<hyperUnique>` metric column, exactly as `dump-segment`
    /// base64-renders it).  Accepts the two observed body shapes — dense
    /// (1024-byte register page) and sparse (BUFFER-relative 3-byte
    /// entries sized by the declared register count, zero-padded — see
    /// the module docs) — and fails LOUDLY on every shape the oracle
    /// fixtures did not pin.
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on: a truncated header; an
    /// unknown version byte; a NON-ZERO register-offset byte (never
    /// observed — its semantics are unverified, so decoding it would risk
    /// silently wrong estimates); a body that is neither the 1024-byte
    /// dense page nor a whole number of 3-byte sparse entries; a sparse
    /// body whose length differs from `declared_non_zero_count × 3`;
    /// sparse entries out of order, duplicated, positioned inside the
    /// 7-byte header or past buffer position 1030, or carrying a zero
    /// value byte; a non-padding entry after zero padding began; a
    /// declared non-zero-register count that disagrees with the decoded
    /// registers; or an incoherent overflow pair (value in `1..=15`,
    /// register index out of range, or a dangling register index with
    /// value 0).
    pub fn from_druid_blob(data: &[u8]) -> Result<Self> {
        let fail = |what: &str| -> SketchError {
            SketchError::Serialization(format!("druid hyperUnique blob: {what}"))
        };
        if data.len() < HEADER_BYTES {
            return Err(fail(&format!(
                "{} bytes is shorter than the {HEADER_BYTES}-byte header",
                data.len()
            )));
        }
        if data[0] != BLOB_VERSION {
            return Err(fail(&format!(
                "unsupported version byte {:#04x} (only {BLOB_VERSION:#04x} observed)",
                data[0]
            )));
        }
        let register_offset = data[1];
        if register_offset != 0 {
            // Never observed in any captured Druid segment.  The byte is
            // BELIEVED to be a register offset (raising the base of every
            // stored nibble at very high per-sketch cardinality), but with
            // no oracle fixture pinning that semantics, decoding it would
            // risk silently wrong estimates — fail loud instead.
            return Err(fail(&format!(
                "non-zero register-offset byte {register_offset} is not supported: \
                 only offset 0 was ever observed in the captured oracle segments, \
                 so the offset semantics (and the estimator behaviour in that \
                 regime) are unverified"
            )));
        }
        let declared_non_zero = usize::from(u16::from_be_bytes([data[2], data[3]]));
        let overflow_value = data[4];
        let overflow_register = u16::from_be_bytes([data[5], data[6]]);
        let overflow = decode_overflow(overflow_value, overflow_register, fail)?;

        let body = &data[HEADER_BYTES..];
        let mut registers = Box::new([0u8; NUM_REGISTERS]);
        if body.len() == REGISTER_PAGE_BYTES {
            // Dense page: two 4-bit registers per byte (LOW nibble = even
            // register — an arbitrary but fixed choice, see module docs).
            for (byte_pos, &b) in body.iter().enumerate() {
                registers[2 * byte_pos] = b & MAX_NIBBLE;
                registers[2 * byte_pos + 1] = b >> 4;
            }
        } else if body.len().is_multiple_of(3) {
            // Sparse entries: one 3-byte (BE u16 position, packed byte)
            // entry per non-zero PAGE BYTE, strictly ascending.  Positions
            // are BUFFER-relative (oracle-pinned — module docs): the
            // 7-byte header is included, so the first page byte is at
            // position 7 and the last at 7 + 1023 = 1030.  The body is
            // sized by the DECLARED REGISTER count; when a page byte
            // holds both nibbles there are fewer entries than registers
            // and the tail is zero-padded.
            if body.len() != declared_non_zero * 3 {
                return Err(fail(&format!(
                    "sparse body of {} bytes does not match the declared \
                     non-zero-register count {declared_non_zero} × 3 (every \
                     observed blob sizes the body by the declared count, \
                     zero-padding past the last entry)",
                    body.len()
                )));
            }
            let mut prev: Option<u16> = None;
            let mut padding = false;
            for entry in body.chunks_exact(3) {
                if entry == [0, 0, 0] {
                    // Zero padding — only ever observed as a suffix.
                    padding = true;
                    continue;
                }
                if padding {
                    return Err(fail(
                        "sparse entry after zero padding began (padding is \
                         a pure suffix in every observed blob)",
                    ));
                }
                let buf_pos = u16::from_be_bytes([entry[0], entry[1]]);
                if usize::from(buf_pos) < HEADER_BYTES {
                    return Err(fail(&format!(
                        "sparse entry position {buf_pos} lies inside the \
                         {HEADER_BYTES}-byte header (positions are \
                         buffer-relative: 7..=1030)"
                    )));
                }
                let byte_pos = usize::from(buf_pos) - HEADER_BYTES;
                if byte_pos >= REGISTER_PAGE_BYTES {
                    return Err(fail(&format!(
                        "sparse entry position {buf_pos} is past the end of \
                         the register page (buffer-relative maximum 1030)"
                    )));
                }
                if let Some(p) = prev
                    && buf_pos <= p
                {
                    return Err(fail(&format!(
                        "sparse entry positions must be strictly ascending \
                         (position {buf_pos} follows {p})"
                    )));
                }
                prev = Some(buf_pos);
                let value_byte = entry[2];
                if value_byte == 0 {
                    return Err(fail(&format!(
                        "sparse entry at position {buf_pos} carries a zero \
                         value byte (never observed — entries exist to record \
                         non-zero page bytes)"
                    )));
                }
                registers[2 * byte_pos] = value_byte & MAX_NIBBLE;
                registers[2 * byte_pos + 1] = value_byte >> 4;
            }
        } else {
            return Err(fail(&format!(
                "body of {} bytes is neither the {REGISTER_PAGE_BYTES}-byte dense \
                 register page nor a whole number of 3-byte sparse entries",
                body.len()
            )));
        }

        let actual_non_zero = registers.iter().filter(|&&v| v != 0).count();
        if actual_non_zero != declared_non_zero {
            return Err(fail(&format!(
                "declared non-zero-register count {declared_non_zero} disagrees \
                 with the {actual_non_zero} non-zero registers decoded from the \
                 body (consistent in every observed blob — a mismatch means a \
                 corrupt or unsupported image)"
            )));
        }
        Ok(Self {
            registers,
            overflow,
        })
    }

    /// Merge `other` into `self`: the register-wise MAX of the two register
    /// arrays (fixture-pinned — Druid's own rollup fold produces exactly
    /// the pair-wise union/max of its inputs), keeping the LARGER-valued
    /// overflow pair as bookkeeping (estimator-inert; see the module
    /// docs).  Infallible: both sides are always decoded-Druid state in
    /// the same register space.
    pub fn merge_in_place(&mut self, other: &Self) {
        for (dst, &src) in self.registers.iter_mut().zip(other.registers.iter()) {
            if src > *dst {
                *dst = src;
            }
        }
        self.overflow = match (self.overflow, other.overflow) {
            (Some(a), Some(b)) => Some(if b.value > a.value { b } else { a }),
            (a, b) => a.or(b),
        };
    }

    /// Return the merge of two sketches (see [`Self::merge_in_place`]).
    #[must_use]
    pub fn merged(&self, other: &Self) -> Self {
        let mut out = self.clone();
        out.merge_in_place(other);
        out
    }

    /// Druid-parity cardinality estimate (raw double, NOT rounded — the
    /// native `hyperUnique` aggregation value).  See the module docs for
    /// the fixture-pinned math; the empty sketch estimates exactly `0.0`.
    #[must_use]
    pub fn estimate(&self) -> f64 {
        let mut zeros = 0usize;
        let mut sum = 0.0_f64;
        for &v in self.registers.iter() {
            if v == 0 {
                zeros += 1;
                sum += 1.0;
            } else {
                sum += pow2_neg(v);
            }
        }
        let e_raw = ALPHA_M * M_F64 * M_F64 / sum;
        if e_raw <= 2.5 * M_F64 && zeros != 0 {
            // Linear counting — the branch every oracle fixture pins.
            // `zeros` is at most 2048, so the cast is exact.
            #[allow(clippy::cast_precision_loss)]
            let v = zeros as f64;
            return M_F64 * (M_F64 / v).ln();
        }
        e_raw
    }

    /// The estimate rounded to an integer the way Druid's SQL
    /// `APPROX_COUNT_DISTINCT` renders it (Java `Math.round` semantics:
    /// `floor(x + 0.5)` — fixture-pinned: `694.502… → 695`,
    /// `707.174… → 707`, `1190.828… → 1191`, `12.035… → 12`).
    #[must_use]
    pub fn estimate_rounded(&self) -> i64 {
        // The estimate is a non-negative cardinality far below 2^63; the
        // cast saturates defensively rather than wrapping.
        #[allow(clippy::cast_possible_truncation)]
        let rounded = (self.estimate() + 0.5).floor() as i64;
        rounded
    }

    /// Serialize the sketch for the FerroDruid partial-state wire (broker
    /// scatter/gather round-trip; never written to a segment): `[version
    /// 1][overflow value u8][overflow register u16 LE][2048 register
    /// bytes]`.
    #[must_use]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(WIRE_LEN);
        let _ = buf.write_u8(WIRE_VERSION);
        let (ov, oreg) = match self.overflow {
            Some(o) => (o.value, o.register),
            None => (0, 0),
        };
        let _ = buf.write_u8(ov);
        let _ = buf.write_u16::<byteorder::LittleEndian>(oreg);
        buf.extend_from_slice(self.registers.as_slice());
        buf
    }

    /// Deserialize a partial-state wire image written by
    /// [`Self::serialize`].
    ///
    /// # Errors
    ///
    /// Returns [`SketchError::Serialization`] on a wrong length or
    /// version, a register value past the 4-bit ceiling (the only
    /// accepted register offset is 0, so no legitimate register exceeds
    /// 15), or an incoherent overflow pair.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        let fail = |what: &str| -> SketchError {
            SketchError::Serialization(format!("druid hyperUnique wire: {what}"))
        };
        if data.len() != WIRE_LEN {
            return Err(fail(&format!(
                "expected exactly {WIRE_LEN} bytes, got {}",
                data.len()
            )));
        }
        let mut cursor = Cursor::new(data);
        let version = cursor.read_u8().map_err(|e| fail(&e.to_string()))?;
        if version != WIRE_VERSION {
            return Err(fail(&format!("unsupported wire version {version}")));
        }
        let overflow_value = cursor.read_u8().map_err(|e| fail(&e.to_string()))?;
        let overflow_register = cursor
            .read_u16::<byteorder::LittleEndian>()
            .map_err(|e| fail(&e.to_string()))?;
        let overflow = decode_overflow(overflow_value, overflow_register, fail)?;
        let mut registers = Box::new([0u8; NUM_REGISTERS]);
        let body = &data[4..];
        for (i, &v) in body.iter().enumerate() {
            if v > MAX_NIBBLE {
                return Err(fail(&format!(
                    "register {i} value {v} exceeds the 4-bit ceiling (the only \
                     accepted register offset is 0)"
                )));
            }
            registers[i] = v;
        }
        Ok(Self {
            registers,
            overflow,
        })
    }
}

/// Validate and normalize a `(maxOverflowValue, maxOverflowRegister)`
/// header pair — shared by the blob decode and the wire decode.
fn decode_overflow(
    value: u8,
    register: u16,
    fail: impl Fn(&str) -> SketchError,
) -> Result<Option<Overflow>> {
    if value == 0 {
        if register != 0 {
            return Err(fail(&format!(
                "overflow value 0 with a dangling overflow register {register} \
                 (every observed no-overflow header carries 00 0000)"
            )));
        }
        return Ok(None);
    }
    if value <= MAX_NIBBLE {
        return Err(fail(&format!(
            "overflow value {value} fits a 4-bit register and can therefore \
             never legitimately overflow (observed overflows start one past \
             the ceiling, at 16)"
        )));
    }
    if usize::from(register) >= NUM_REGISTERS {
        return Err(fail(&format!(
            "overflow register {register} is past the {NUM_REGISTERS}-register \
             page"
        )));
    }
    Ok(Some(Overflow { value, register }))
}

/// Exact `2^-v` for a register value (`v <= 15` under the accepted offset,
/// but exact for any `v < 64`): bit-constructed, so the estimator sum is
/// free of powi/exp2 rounding concerns.
fn pow2_neg(v: u8) -> f64 {
    debug_assert!(v < 64, "register value {v} out of range");
    f64::from_bits((1023 - u64::from(v.min(63))) << 52)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a sparse blob from `(BUFFER-relative position, packed value
    /// byte)` entries, zero-padding the body to `non_zero * 3` bytes the
    /// way Druid does (see the module docs).
    fn sparse_blob(non_zero: u16, overflow: (u8, u16), entries: &[(u16, u8)]) -> Vec<u8> {
        let mut buf = vec![BLOB_VERSION, 0];
        buf.extend_from_slice(&non_zero.to_be_bytes());
        buf.push(overflow.0);
        buf.extend_from_slice(&overflow.1.to_be_bytes());
        for &(pos, val) in entries {
            buf.extend_from_slice(&pos.to_be_bytes());
            buf.push(val);
        }
        while buf.len() < HEADER_BYTES + usize::from(non_zero) * 3 {
            buf.push(0);
        }
        buf
    }

    /// Build a dense blob from a full register page.
    fn dense_blob(non_zero: u16, overflow: (u8, u16), page: &[u8; REGISTER_PAGE_BYTES]) -> Vec<u8> {
        let mut buf = vec![BLOB_VERSION, 0];
        buf.extend_from_slice(&non_zero.to_be_bytes());
        buf.push(overflow.0);
        buf.extend_from_slice(&overflow.1.to_be_bytes());
        buf.extend_from_slice(page);
        buf
    }

    #[test]
    fn empty_sketch_estimates_zero() {
        let s = DruidHyperUnique::empty();
        assert!(s.is_empty());
        assert_eq!(s.estimate().to_bits(), 0.0_f64.to_bits());
        assert_eq!(s.estimate_rounded(), 0);
    }

    #[test]
    fn single_register_sparse_blob_decodes() {
        // The observed single-user shape: one entry, one nibble set
        // (`03e0 20` — buffer position 992 = page byte 985, high nibble 2).
        let blob = sparse_blob(1, (0, 0), &[(0x03E0, 0x20)]);
        let s = DruidHyperUnique::from_druid_blob(&blob).expect("decode");
        assert_eq!(s.num_non_zero(), 1);
        assert_eq!(s.registers[2 * 985 + 1], 2, "buffer-relative mapping");
        // Linear counting with one occupied register.
        let want = 2048.0_f64 * (2048.0_f64 / 2047.0).ln();
        assert_eq!(s.estimate().to_bits(), want.to_bits());
    }

    #[test]
    fn sparse_positions_map_buffer_relative() {
        // Boundary positions: 7 = first page byte (registers 0/1), 1030 =
        // last page byte (registers 2046/2047) — both oracle-observed
        // (probe census min/max; 1030 appears in the captured mix blob).
        let first = DruidHyperUnique::from_druid_blob(&sparse_blob(2, (0, 0), &[(7, 0x21)]))
            .expect("position 7");
        assert_eq!(first.registers[0], 1);
        assert_eq!(first.registers[1], 2);
        let last = DruidHyperUnique::from_druid_blob(&sparse_blob(1, (0, 0), &[(1030, 0x0F)]))
            .expect("position 1030");
        assert_eq!(last.registers[2046], 15);
        assert_eq!(last.registers[2047], 0);
    }

    #[test]
    fn both_nibbles_entry_is_zero_padded() {
        // The oracle pair shape: declared count 2, ONE both-nibbles entry,
        // body padded to 2 × 3 bytes (`00 08 11 | 00 00 00`).
        let blob = sparse_blob(2, (0, 0), &[(8, 0x11)]);
        assert_eq!(&blob[10..13], &[0, 0, 0], "helper zero-pads like Druid");
        let s = DruidHyperUnique::from_druid_blob(&blob).expect("decode");
        assert_eq!(s.num_non_zero(), 2);
        assert_eq!(s.registers[2], 1);
        assert_eq!(s.registers[3], 1);
    }

    #[test]
    fn zero_pair_sparse_blob_is_empty() {
        let blob = sparse_blob(0, (0, 0), &[]);
        let s = DruidHyperUnique::from_druid_blob(&blob).expect("decode");
        assert!(s.is_empty());
        assert_eq!(s.estimate().to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn merge_is_register_wise_max() {
        let a = DruidHyperUnique::from_druid_blob(&sparse_blob(
            2,
            (0, 0),
            &[(8, 0x21)], // page byte 1: registers 2 → 1, 3 → 2
        ))
        .expect("a");
        let b = DruidHyperUnique::from_druid_blob(&sparse_blob(
            2,
            (0, 0),
            &[(8, 0x13)], // page byte 1: registers 2 → 3, 3 → 1
        ))
        .expect("b");
        let m = a.merged(&b);
        assert_eq!(m.registers[2], 3, "max(1, 3)");
        assert_eq!(m.registers[3], 2, "max(2, 1)");
        assert_eq!(m.num_non_zero(), 2);
        // Merge is commutative.
        assert_eq!(b.merged(&a), m);
        // Merging with empty is a no-op.
        assert_eq!(m.merged(&DruidHyperUnique::empty()), m);
    }

    #[test]
    fn dense_blob_round_trips_registers() {
        let mut page = [0u8; REGISTER_PAGE_BYTES];
        page[0] = 0x21; // register 0 → 1, register 1 → 2
        page[1023] = 0x0F; // register 2046 → 15
        let s = DruidHyperUnique::from_druid_blob(&dense_blob(3, (0, 0), &page)).expect("decode");
        assert_eq!(s.registers[0], 1);
        assert_eq!(s.registers[1], 2);
        assert_eq!(s.registers[2046], 15);
        assert_eq!(s.registers[2047], 0);
        assert_eq!(s.num_non_zero(), 3);
    }

    #[test]
    fn overflow_pair_is_estimator_inert_and_survives_merge() {
        let plain = sparse_blob(1, (0, 0), &[(10, 0x02)]);
        let with_overflow = sparse_blob(1, (16, 1453), &[(10, 0x02)]);
        let a = DruidHyperUnique::from_druid_blob(&plain).expect("plain");
        let b = DruidHyperUnique::from_druid_blob(&with_overflow).expect("overflow");
        // Fixture-pinned: the overflow register does NOT count as occupied.
        assert_eq!(a.estimate().to_bits(), b.estimate().to_bits());
        // …but the bookkeeping survives a merge (larger value wins).
        let m = a.merged(&b);
        assert_eq!(
            m.overflow,
            Some(Overflow {
                value: 16,
                register: 1453
            })
        );
        let c = DruidHyperUnique::from_druid_blob(&sparse_blob(1, (17, 7), &[(10, 0x02)]))
            .expect("bigger overflow");
        assert_eq!(
            m.merged(&c).overflow,
            Some(Overflow {
                value: 17,
                register: 7
            })
        );
    }

    #[test]
    fn wire_round_trip_preserves_state() {
        let s = DruidHyperUnique::from_druid_blob(&sparse_blob(
            3,
            (16, 1453),
            &[(8, 0x21), (100, 0x03)],
        ))
        .expect("decode");
        let rt = DruidHyperUnique::deserialize(&s.serialize()).expect("round trip");
        assert_eq!(rt, s);
        assert_eq!(rt.estimate().to_bits(), s.estimate().to_bits());
    }

    #[test]
    fn wire_rejects_malformed_images() {
        assert!(DruidHyperUnique::deserialize(&[]).is_err(), "empty");
        let mut good = DruidHyperUnique::empty().serialize();
        good[0] = 9;
        assert!(DruidHyperUnique::deserialize(&good).is_err(), "bad version");
        let mut over = DruidHyperUnique::empty().serialize();
        over[4] = 16; // register 0 value past the nibble ceiling
        assert!(DruidHyperUnique::deserialize(&over).is_err(), "register 16");
        let mut short = DruidHyperUnique::empty().serialize();
        short.pop();
        assert!(DruidHyperUnique::deserialize(&short).is_err(), "short");
    }

    #[test]
    fn blob_rejects_unobserved_shapes() {
        // Non-zero register offset (byte 1): unverified semantics.
        let mut offset = sparse_blob(1, (0, 0), &[(8, 0x01)]);
        offset[1] = 1;
        assert!(DruidHyperUnique::from_druid_blob(&offset).is_err());
        // Wrong version byte.
        let mut version = sparse_blob(1, (0, 0), &[(8, 0x01)]);
        version[0] = 2;
        assert!(DruidHyperUnique::from_druid_blob(&version).is_err());
        // Truncated header.
        assert!(DruidHyperUnique::from_druid_blob(&[0x01, 0x00]).is_err());
        // Body neither dense nor whole sparse entries.
        let mut ragged = sparse_blob(1, (0, 0), &[(8, 0x01)]);
        ragged.push(0);
        assert!(DruidHyperUnique::from_druid_blob(&ragged).is_err());
        // Sparse body not sized declared × 3 (a both-nibbles entry with
        // the padding STRIPPED — Druid always sizes by the register count).
        let mut unpadded = sparse_blob(1, (0, 0), &[(8, 0x11)]);
        unpadded[3] = 2; // declare 2 registers over the 3-byte body
        assert!(DruidHyperUnique::from_druid_blob(&unpadded).is_err());
        // Unsorted sparse entries.
        let unsorted = sparse_blob(2, (0, 0), &[(9, 0x01), (8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&unsorted).is_err());
        // Duplicate sparse position.
        let dup = sparse_blob(2, (0, 0), &[(8, 0x01), (8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&dup).is_err());
        // Positions are buffer-relative: 0..=6 lie inside the header.
        for pos in [0u16, 6] {
            let inside = sparse_blob(1, (0, 0), &[(pos, 0x01)]);
            assert!(
                DruidHyperUnique::from_druid_blob(&inside).is_err(),
                "position {pos} must be rejected"
            );
        }
        // Position past the buffer end (1030 is the last page byte).
        let past = sparse_blob(1, (0, 0), &[(1031, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&past).is_err());
        // Zero value byte on a real (non-padding) entry.
        let zero = sparse_blob(1, (0, 0), &[(8, 0x00)]);
        assert!(DruidHyperUnique::from_druid_blob(&zero).is_err());
        // A real entry AFTER zero padding began.
        let resumed = sparse_blob(3, (0, 0), &[(0, 0x00), (8, 0x11)]);
        assert!(DruidHyperUnique::from_druid_blob(&resumed).is_err());
        // Declared non-zero count disagreeing with the body.
        let wrong_count = sparse_blob(5, (0, 0), &[(8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&wrong_count).is_err());
        // Overflow value inside the nibble range.
        let small_overflow = sparse_blob(1, (5, 3), &[(8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&small_overflow).is_err());
        // Overflow register past the page.
        let far_overflow = sparse_blob(1, (16, 2048), &[(8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&far_overflow).is_err());
        // Dangling overflow register with value 0.
        let dangling = sparse_blob(1, (0, 9), &[(8, 0x01)]);
        assert!(DruidHyperUnique::from_druid_blob(&dangling).is_err());
    }

    #[test]
    fn estimate_rounding_matches_java_math_round() {
        // floor(x + 0.5): the fixture-pinned SQL renderings.
        for (est, want) in [
            (694.502_f64, 695_i64),
            (707.4999, 707),
            (0.5, 1),
            (0.499, 0),
        ] {
            #[allow(clippy::cast_possible_truncation)]
            let got = (est + 0.5).floor() as i64;
            assert_eq!(got, want, "round({est})");
        }
    }
}
