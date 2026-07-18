// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Compression codecs for Druid segment data (LZ4, LZF, Zstd).
//!
//! Druid segments use block-based compression. Each block is stored as:
//!
//! ```text
//! [4-byte compressed size (BE)][4-byte uncompressed size (BE)][compressed data]
//! ```
//!
//! This crate provides both raw compress/decompress functions and the
//! block-level wrappers that match the on-disk format.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Compression / decompression errors.
#[derive(Error, Debug)]
pub enum CompressionError {
    /// The codec encountered corrupt or truncated input.
    #[error("decompression error: {0}")]
    Decompress(String),
    /// A compression operation failed.
    #[error("compression error: {0}")]
    Compress(String),
    /// The block header is too short or otherwise malformed.
    #[error("invalid block header: {0}")]
    InvalidBlock(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, CompressionError>;

// ---------------------------------------------------------------------------
// Codec enum
// ---------------------------------------------------------------------------

/// Supported compression codecs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Codec {
    /// No compression — pass-through.
    None,
    /// LZ4 block compression (via `lz4_flex`).
    Lz4,
    /// LZF compression (Druid legacy format, implemented in pure Rust here).
    Lzf,
    /// Zstandard compression.
    Zstd,
}

// ---------------------------------------------------------------------------
// Public API — raw compress / decompress
// ---------------------------------------------------------------------------

/// Compress `input` with the given `codec`.
pub fn compress(codec: Codec, input: &[u8]) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(input.to_vec()),
        Codec::Lz4 => Ok(lz4_flex::compress_prepend_size(input)),
        Codec::Lzf => lzf::compress(input),
        Codec::Zstd => zstd::stream::encode_all(input, 3)
            .map_err(|e| CompressionError::Compress(e.to_string())),
    }
}

/// Decompress `input` that was compressed with `codec`.
///
/// `expected_len` is a hard cap on the decompressed size:
///
/// * For `Codec::Lz4`, the leading 4-byte little-endian size prefix is parsed
///   and rejected if it exceeds `expected_len` — this prevents a 4-byte
///   attacker-controlled header from forcing a multi-GiB pre-allocation
///   inside `lz4_flex` (24 h fuzz finding `oom-571cae97…`, 4 B input
///   `0a 0a 00 fd` claiming ~4.24 GiB output).
/// * For `Codec::Zstd`, the streaming decoder is fed through a `take`
///   adapter that aborts once `expected_len + 1` output bytes have been
///   produced, so a malicious frame cannot drive unbounded allocation
///   either.
/// * For `Codec::Lzf` it is honored as the output buffer pre-size and
///   the decoder errors if the back-references would overrun it.
/// * For `Codec::None` it is ignored (input length is the natural cap).
pub fn decompress(codec: Codec, input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
    match codec {
        Codec::None => Ok(input.to_vec()),
        Codec::Lz4 => {
            // `lz4_flex::decompress_size_prepended` reads a 4-byte LE size
            // prefix from `input` and `Vec::with_capacity`s that many bytes
            // before any actual decompression — a 4-byte attacker-controlled
            // header can therefore drive a multi-GiB up-front allocation.
            // Validate the header against `expected_len` ourselves before
            // delegating so a malicious caller cannot bypass the cap.
            if input.len() < 4 {
                return Err(CompressionError::Decompress(
                    "LZ4: input shorter than 4-byte size prefix".into(),
                ));
            }
            let claimed = u32::from_le_bytes([input[0], input[1], input[2], input[3]]) as usize;
            if claimed > expected_len {
                return Err(CompressionError::Decompress(format!(
                    "LZ4: size prefix {claimed} exceeds max_decompressed {expected_len}"
                )));
            }
            lz4_flex::decompress_size_prepended(input)
                .map_err(|e| CompressionError::Decompress(e.to_string()))
        }
        Codec::Lzf => lzf::decompress(input, expected_len),
        Codec::Zstd => {
            // `zstd::stream::decode_all` reads the frame header and may
            // pre-allocate based on `frameContentSize`; cap the *output*
            // by reading at most `expected_len + 1` bytes from a streaming
            // decoder, then erroring if more are produced. The trailing
            // `+ 1` lets us distinguish "exactly the cap" from "overflowed".
            use std::io::Read;
            let cap = expected_len.saturating_add(1);
            let mut decoder = zstd::stream::Decoder::new(input)
                .map_err(|e| CompressionError::Decompress(e.to_string()))?;
            let mut out = Vec::new();
            (&mut decoder)
                .take(cap as u64)
                .read_to_end(&mut out)
                .map_err(|e| CompressionError::Decompress(e.to_string()))?;
            if out.len() > expected_len {
                return Err(CompressionError::Decompress(format!(
                    "Zstd: decompressed size exceeds max_decompressed {expected_len}"
                )));
            }
            Ok(out)
        }
    }
}

// ---------------------------------------------------------------------------
// Public API — raw-block decompress (no header, exact size known)
// ---------------------------------------------------------------------------

/// Decompress a **raw** compressed block whose exact decompressed length is
/// known from surrounding metadata and that carries no size header of its
/// own.
///
/// Segments written by upstream Apache Druid store column values as raw LZ4
/// blocks inside an indexed container; the block's decompressed size is
/// derived from the container's value count and per-value width, so unlike
/// [`decompress`] there is no 4-byte size prefix to validate. `exact_len`
/// doubles as the allocation cap: the output buffer is exactly that size and
/// the codec errors out if the stream produces more or fewer bytes.
///
/// Only `Codec::None` and `Codec::Lz4` are supported — the only codecs
/// observed in real upstream segments so far. Other codecs return an error
/// rather than guessing at an unverified framing.
pub fn decompress_raw(codec: Codec, input: &[u8], exact_len: usize) -> Result<Vec<u8>> {
    match codec {
        Codec::None => {
            if input.len() != exact_len {
                return Err(CompressionError::Decompress(format!(
                    "raw block: expected {exact_len} bytes, have {}",
                    input.len()
                )));
            }
            Ok(input.to_vec())
        }
        Codec::Lz4 => {
            let mut out = vec![0u8; exact_len];
            let n = lz4_flex::block::decompress_into(input, &mut out)
                .map_err(|e| CompressionError::Decompress(format!("raw LZ4 block: {e}")))?;
            if n != exact_len {
                return Err(CompressionError::Decompress(format!(
                    "raw LZ4 block decoded to {n} bytes, expected {exact_len}"
                )));
            }
            Ok(out)
        }
        Codec::Lzf | Codec::Zstd => Err(CompressionError::Decompress(
            "raw-block decompression is only supported for LZ4/None".to_string(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Public API — block compress / decompress
// ---------------------------------------------------------------------------

/// Compress `input` into a Druid-style block.
///
/// Block layout: `[4B compressed_size BE][4B uncompressed_size BE][data]`.
pub fn compress_block(codec: Codec, input: &[u8]) -> Result<Vec<u8>> {
    let compressed = compress(codec, input)?;
    let comp_len: u32 = compressed
        .len()
        .try_into()
        .map_err(|_| CompressionError::Compress("compressed size exceeds u32".into()))?;
    let uncomp_len: u32 = input
        .len()
        .try_into()
        .map_err(|_| CompressionError::Compress("uncompressed size exceeds u32".into()))?;

    let mut out = Vec::with_capacity(8 + compressed.len());
    out.extend_from_slice(&comp_len.to_be_bytes());
    out.extend_from_slice(&uncomp_len.to_be_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress a single Druid-style block.
///
/// Reads the 8-byte header, then decompresses the payload.
///
/// Both `comp_len` and `uncomp_len` are validated against the input size:
///
/// * `comp_len` must fit inside the remaining buffer.
/// * `uncomp_len` must not exceed `comp_len × MAX_BLOCK_EXPANSION_RATIO`
///   — a header that claims gigabytes from a 5 B compressed payload is
///   structurally impossible and the previous code happily forwarded the
///   number to `lz4_flex::decompress_size_prepended` which then tried to
///   `Vec::with_capacity` it (24 h fuzz finding `oom-13fdfe71…`).
pub fn decompress_block(codec: Codec, input: &[u8]) -> Result<Vec<u8>> {
    /// No real codec expands more than ~256× per compressed byte. Pick a
    /// generous cap that still keeps the worst-case allocation bounded
    /// by `O(input.len())` rather than `O(2^32)`.
    const MAX_BLOCK_EXPANSION_RATIO: usize = 256;

    if input.len() < 8 {
        return Err(CompressionError::InvalidBlock(format!(
            "block too short: {} bytes (need >= 8)",
            input.len()
        )));
    }
    let comp_len = u32::from_be_bytes([input[0], input[1], input[2], input[3]]) as usize;
    let uncomp_len = u32::from_be_bytes([input[4], input[5], input[6], input[7]]) as usize;

    let data = &input[8..];
    if data.len() < comp_len {
        return Err(CompressionError::InvalidBlock(format!(
            "block data truncated: have {} bytes, header says {}",
            data.len(),
            comp_len,
        )));
    }

    // Reject impossible expansion ratios up front so we never forward
    // a multi-GiB `uncomp_len` from a tiny `comp_len` to the codec
    // backend (which would `Vec::with_capacity` it before checking).
    let max_uncomp = comp_len.saturating_mul(MAX_BLOCK_EXPANSION_RATIO);
    if uncomp_len > max_uncomp {
        return Err(CompressionError::InvalidBlock(format!(
            "block uncomp_len {uncomp_len} exceeds {MAX_BLOCK_EXPANSION_RATIO}× comp_len {comp_len} \
             ({max_uncomp} max); refusing to decompress"
        )));
    }

    let out = decompress(codec, &data[..comp_len], uncomp_len)?;
    // DD R46: a block declares its EXACT uncompressed length; verify the codec
    // produced exactly that many bytes so a corrupt/short/over-long stream is
    // rejected rather than silently returning a wrong-length block.
    if out.len() != uncomp_len {
        return Err(CompressionError::InvalidBlock(format!(
            "block decoded to {} bytes but the header declared uncomp_len {uncomp_len}",
            out.len()
        )));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// LZF implementation (pure Rust, no unsafe)
// ---------------------------------------------------------------------------

mod lzf {
    use super::{CompressionError, Result};

    /// LZF compress `input`.
    ///
    /// Uses a simple hash-chain approach. Output is a stream of literal and
    /// back-reference chunks.
    pub(crate) fn compress(input: &[u8]) -> Result<Vec<u8>> {
        if input.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(input.len());
        // Hash table: maps 3-byte hash → position in input.
        const HTAB_SIZE: usize = 1 << 16;
        let mut htab = vec![0u32; HTAB_SIZE];

        let len = input.len();
        let mut ip: usize = 0; // current input position
        let mut lit_start: usize = 0; // start of current literal run
        let mut lit_len: usize = 0; // length of current literal run

        #[inline]
        fn hash(data: &[u8], pos: usize) -> usize {
            let v = (u32::from(data[pos]) << 16)
                | (u32::from(data[pos + 1]) << 8)
                | u32::from(data[pos + 2]);
            // Knuth multiplicative hash.
            ((v.wrapping_mul(0x01000193)) >> 16) as usize & 0xFFFF
        }

        // We need at least 3 bytes to form a hash / back-reference.
        while ip + 2 < len {
            let h = hash(input, ip);
            let ref_pos = htab[h] as usize;
            htab[h] = ip as u32;

            let off = ip.wrapping_sub(ref_pos).wrapping_sub(1);
            // Check match: offset must be < 8192 and three bytes must match.
            if off < 8192
                && ref_pos < ip
                && ip + 2 < len
                && input[ref_pos] == input[ip]
                && input[ref_pos + 1] == input[ip + 1]
                && input[ref_pos + 2] == input[ip + 2]
            {
                // Flush pending literals.
                flush_literals(&mut out, &input[lit_start..lit_start + lit_len]);
                lit_len = 0;

                // Compute match length (min 3).
                let mut mlen: usize = 3;
                let max_ml = std::cmp::min(len - ip, 264); // type 1 max=7+2=9? type 2 max=255+9=264
                while mlen < max_ml && input[ref_pos + mlen] == input[ip + mlen] {
                    mlen += 1;
                }

                if mlen < 9 {
                    // Type 1 short back-reference.
                    let l = (mlen - 2) as u8; // 1..7
                    let o = off;
                    out.push((l << 5) | ((o >> 8) as u8 & 0x1f));
                    out.push((o & 0xff) as u8);
                } else {
                    // Type 2 long back-reference.
                    let o = off;
                    out.push(0xe0 | ((o >> 8) as u8 & 0x1f)); // 111ooooo
                    out.push((o & 0xff) as u8);
                    out.push((mlen - 9) as u8);
                }

                ip += mlen;
                lit_start = ip;
                // Update hash for positions within the match (improves ratio).
                if ip + 2 < len {
                    htab[hash(input, ip)] = ip as u32;
                }
            } else {
                lit_len += 1;
                ip += 1;
                // Flush if literal run reaches maximum (32 bytes per chunk).
                if lit_len == 32 {
                    flush_literals(&mut out, &input[lit_start..lit_start + lit_len]);
                    lit_start = ip;
                    lit_len = 0;
                }
            }
        }

        // Remaining bytes are always literals.
        while ip < len {
            lit_len += 1;
            ip += 1;
            if lit_len == 32 {
                flush_literals(&mut out, &input[lit_start..lit_start + lit_len]);
                lit_start = ip;
                lit_len = 0;
            }
        }
        if lit_len > 0 {
            flush_literals(&mut out, &input[lit_start..lit_start + lit_len]);
        }

        Ok(out)
    }

    /// Write a literal chunk: `[0LLLLLLL][L+1 bytes]`.
    fn flush_literals(out: &mut Vec<u8>, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        debug_assert!(data.len() <= 32);
        out.push((data.len() - 1) as u8); // top bit 0, lower 7 = len-1
        out.extend_from_slice(data);
    }

    /// LZF decompress `input`, expecting `expected_len` output bytes.
    ///
    /// `expected_len` is treated as a hard upper bound — both the initial
    /// output buffer and any literal/back-reference write must fit within
    /// it. This prevents an attacker-controlled `expected_len` (which in
    /// the block format comes straight from a 4-byte header) from forcing
    /// a multi-GiB up-front allocation (24 h fuzz finding
    /// `fuzz_block_decompress/oom-50560daa…`, 8 B `00 00 00 00 b5 7d 0a 87`,
    /// `uncomp_len = 0xb57d0a87 ≈ 3 GiB`).
    ///
    /// To keep the up-front allocation bounded by *actual input size* we
    /// also cap the initial reservation to `8 × input.len()` (LZF cannot
    /// expand more than that per byte under any control sequence). Output
    /// is still allowed to grow to `expected_len` via the normal `Vec`
    /// growth path; what we refuse to do is *pre-allocate* gigabytes from
    /// a tiny header.
    pub(crate) fn decompress(input: &[u8], expected_len: usize) -> Result<Vec<u8>> {
        // Maximum LZF expansion: each control byte produces at most
        // `0xff + 9 = 264` output bytes, so a strict upper bound on the
        // achievable output for an N-byte input is `264 * N`. We round up
        // to a power-of-two-ish cap that's still cheap to allocate.
        const MAX_LZF_EXPANSION_PER_BYTE: usize = 264;
        let input_cap = input.len().saturating_mul(MAX_LZF_EXPANSION_PER_BYTE);
        let initial_cap = expected_len.min(input_cap);
        let mut out = Vec::with_capacity(initial_cap);
        let mut ip: usize = 0;

        while ip < input.len() {
            let ctrl = input[ip] as usize;
            ip += 1;

            if ctrl < 0x20 {
                // Type 0: literal run.  Length = ctrl + 1.
                let lit_len = ctrl + 1;
                let end = ip + lit_len;
                if end > input.len() {
                    return Err(CompressionError::Decompress(
                        "LZF: literal run exceeds input".into(),
                    ));
                }
                // DD R46: enforce the `expected_len` cap before writing — LZF
                // literal/back-ref writes previously ignored it, so a stream
                // could decode to more bytes than the block header declared
                // (corrupting the length contract / over-allocating).
                if out.len() + lit_len > expected_len {
                    return Err(CompressionError::Decompress(
                        "LZF: decoded output exceeds expected length".into(),
                    ));
                }
                out.extend_from_slice(&input[ip..end]);
                ip = end;
            } else if ctrl < 0xe0 {
                // Type 1: short back-reference.
                let mlen = (ctrl >> 5) + 2; // 2..9 (but max 7+2=9 is type2 territory; here max 6+2=8? actually 1..7 → +2 = 3..9, but 7 maps to mlen=9 which is the boundary)
                if ip >= input.len() {
                    return Err(CompressionError::Decompress(
                        "LZF: truncated short backref".into(),
                    ));
                }
                let off = (((ctrl & 0x1f) << 8) | input[ip] as usize) + 1;
                ip += 1;

                if off > out.len() {
                    return Err(CompressionError::Decompress(format!(
                        "LZF: back-reference offset {} exceeds output length {}",
                        off,
                        out.len()
                    )));
                }
                // DD R46: enforce the `expected_len` cap before expanding a
                // back-reference, so the output cannot exceed the declared length.
                if out.len() + mlen > expected_len {
                    return Err(CompressionError::Decompress(
                        "LZF: decoded output exceeds expected length".into(),
                    ));
                }
                let start = out.len() - off;
                for i in 0..mlen {
                    out.push(out[start + i]);
                }
            } else {
                // Type 2: long back-reference.
                if ip + 1 >= input.len() {
                    return Err(CompressionError::Decompress(
                        "LZF: truncated long backref".into(),
                    ));
                }
                let off = (((ctrl & 0x1f) << 8) | input[ip] as usize) + 1;
                let mlen = input[ip + 1] as usize + 9;
                ip += 2;

                if off > out.len() {
                    return Err(CompressionError::Decompress(format!(
                        "LZF: back-reference offset {} exceeds output length {}",
                        off,
                        out.len()
                    )));
                }
                // DD R46: enforce the `expected_len` cap before expanding a
                // back-reference, so the output cannot exceed the declared length.
                if out.len() + mlen > expected_len {
                    return Err(CompressionError::Decompress(
                        "LZF: decoded output exceeds expected length".into(),
                    ));
                }
                let start = out.len() - off;
                for i in 0..mlen {
                    out.push(out[start + i]);
                }
            }
        }

        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- Round-trip tests for each codec ------------------------------------

    fn round_trip(codec: Codec, data: &[u8]) {
        let compressed = compress(codec, data).expect("compress");
        let expected_len = data.len();
        let decompressed = decompress(codec, &compressed, expected_len).expect("decompress");
        assert_eq!(decompressed, data, "round-trip failed for {codec:?}");
    }

    fn round_trip_block(codec: Codec, data: &[u8]) {
        let block = compress_block(codec, data).expect("compress_block");
        let decompressed = decompress_block(codec, &block).expect("decompress_block");
        assert_eq!(decompressed, data, "block round-trip failed for {codec:?}");
    }

    #[test]
    fn none_round_trip() {
        round_trip(Codec::None, b"hello world");
        round_trip_block(Codec::None, b"hello world");
    }

    #[test]
    fn lz4_round_trip() {
        round_trip(Codec::Lz4, b"hello world");
        round_trip(Codec::Lz4, &[0u8; 10_000]);
        round_trip_block(Codec::Lz4, b"the quick brown fox jumps over the lazy dog");
    }

    #[test]
    fn lzf_round_trip_small() {
        round_trip(Codec::Lzf, b"hello world");
    }

    #[test]
    fn lzf_round_trip_repeated() {
        // Repeated data exercises back-references.
        let data: Vec<u8> = b"abcabcabcabcabcabcabcabc".repeat(100);
        round_trip(Codec::Lzf, &data);
    }

    #[test]
    fn lzf_round_trip_zeros() {
        let data = vec![0u8; 4096];
        round_trip(Codec::Lzf, &data);
    }

    #[test]
    fn lzf_round_trip_random_ish() {
        // Pseudorandom data — exercises literal paths.
        let mut data = Vec::with_capacity(1024);
        let mut v: u32 = 0xDEAD_BEEF;
        for _ in 0..1024 {
            v = v.wrapping_mul(1103515245).wrapping_add(12345);
            data.push((v >> 16) as u8);
        }
        round_trip(Codec::Lzf, &data);
    }

    #[test]
    fn lzf_round_trip_block() {
        let data = b"block compression test data with some repetition repetition repetition";
        round_trip_block(Codec::Lzf, data);
    }

    #[test]
    fn zstd_round_trip() {
        round_trip(Codec::Zstd, b"hello world");
        round_trip(Codec::Zstd, &[42u8; 20_000]);
        round_trip_block(Codec::Zstd, b"block compression with zstd");
    }

    #[test]
    fn lzf_empty() {
        let compressed = compress(Codec::Lzf, b"").expect("compress empty");
        assert!(compressed.is_empty());
        let decompressed = decompress(Codec::Lzf, &compressed, 0).expect("decompress empty");
        assert!(decompressed.is_empty());
    }

    // -- Block format tests ------------------------------------------------

    #[test]
    fn block_header_structure() {
        let data = b"test data";
        let block = compress_block(Codec::None, data).expect("compress_block");
        // For Codec::None, compressed == uncompressed.
        let comp_len = u32::from_be_bytes([block[0], block[1], block[2], block[3]]);
        let uncomp_len = u32::from_be_bytes([block[4], block[5], block[6], block[7]]);
        assert_eq!(comp_len as usize, data.len());
        assert_eq!(uncomp_len as usize, data.len());
        assert_eq!(&block[8..], data);
    }

    #[test]
    fn block_too_short() {
        let result = decompress_block(Codec::None, &[0u8; 4]);
        assert!(result.is_err());
    }

    #[test]
    fn block_truncated_data() {
        // Header claims 100 bytes but only 2 are present.
        let mut block = vec![0, 0, 0, 100, 0, 0, 0, 100];
        block.extend_from_slice(&[0u8; 2]);
        let result = decompress_block(Codec::None, &block);
        assert!(result.is_err());
    }

    // -- Known-vector tests ------------------------------------------------

    #[test]
    fn lzf_known_literal_only() {
        // A single literal chunk: ctrl=0x02 means 3 literal bytes.
        let compressed: &[u8] = &[0x02, b'a', b'b', b'c'];
        let out = lzf::decompress(compressed, 3).expect("decompress known");
        assert_eq!(out, b"abc");
    }

    #[test]
    fn lz4_compresses() {
        // Verify LZ4 actually compresses highly-repetitive data.
        let data = vec![0u8; 10_000];
        let compressed = compress(Codec::Lz4, &data).expect("compress");
        assert!(
            compressed.len() < data.len() / 2,
            "LZ4 should compress zeros well, got {} -> {}",
            data.len(),
            compressed.len()
        );
    }

    #[test]
    fn zstd_compresses() {
        let data = vec![0u8; 10_000];
        let compressed = compress(Codec::Zstd, &data).expect("compress");
        assert!(
            compressed.len() < data.len() / 2,
            "Zstd should compress zeros well, got {} -> {}",
            data.len(),
            compressed.len()
        );
    }

    #[test]
    fn lzf_compresses_repeated() {
        let data = b"abcdefgh".repeat(500);
        let compressed = compress(Codec::Lzf, &data).expect("compress");
        assert!(
            compressed.len() < data.len() / 2,
            "LZF should compress repeated data, got {} -> {}",
            data.len(),
            compressed.len()
        );
    }

    #[test]
    fn all_codecs_large_round_trip() {
        let mut data = Vec::with_capacity(100_000);
        for i in 0u32..25_000 {
            data.extend_from_slice(&i.to_le_bytes());
        }
        for codec in [Codec::None, Codec::Lz4, Codec::Lzf, Codec::Zstd] {
            round_trip(codec, &data);
            round_trip_block(codec, &data);
        }
    }

    // -- DoS regression tests (24 h fuzz, 2026-05-03) ----------------------

    /// Regression for fuzz finding
    /// `fuzz_compression_lz4/oom-571cae976c254433cfe552ac5ae7541c56dd3905`.
    ///
    /// 4-byte input `0a 0a 00 fd` is interpreted by
    /// `lz4_flex::decompress_size_prepended` as a size prefix of
    /// `0xfd00_0a0a` ≈ 4.24 GiB and pre-allocates a `Vec` of that size,
    /// triggering an OOM kill — even though the caller passed
    /// `expected_len = 65536`. The fix parses the size prefix ourselves
    /// and rejects it before delegating.
    #[test]
    fn lz4_decompress_rejects_oversized_size_prefix() {
        let crash: &[u8] = &[0x0a, 0x0a, 0x00, 0xfd];
        let result = decompress(Codec::Lz4, crash, 65_536);
        assert!(
            result.is_err(),
            "expected error for size prefix > expected_len, got {result:?}"
        );
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("size prefix") && msg.contains("max_decompressed"),
            "error message should mention the cap: {msg}"
        );
    }

    /// LZ4: input shorter than the 4-byte size prefix is rejected before
    /// reaching `lz4_flex` (which would slice-panic on a < 4-byte slice
    /// in some versions).
    #[test]
    fn lz4_decompress_rejects_truncated_prefix() {
        for n in 0..4 {
            let buf = vec![0xffu8; n];
            assert!(decompress(Codec::Lz4, &buf, 1024).is_err());
        }
    }

    /// LZ4: a size prefix exactly at the cap is allowed (round-trip still
    /// works for inputs whose true output equals `expected_len`).
    #[test]
    fn lz4_decompress_at_cap_succeeds() {
        let payload = vec![0u8; 1_024];
        let compressed = compress(Codec::Lz4, &payload).expect("compress");
        let out = decompress(Codec::Lz4, &compressed, 1_024).expect("decompress at cap");
        assert_eq!(out, payload);
    }

    /// Regression for fuzz finding
    /// `fuzz_block_decompress/oom-50560daa6be52e053cc63854c85e2840e981c411`.
    ///
    /// 8-byte input `00 00 00 00 b5 7d 0a 87` is a Druid block header
    /// claiming `comp_len = 0`, `uncomp_len = 0xb57d0a87 ≈ 3.04 GiB`.
    /// The previous LZF path called `Vec::with_capacity(uncomp_len)`
    /// up-front, OOMing the process. The fix caps the initial reservation
    /// to `264 × input.len()` so a tiny input cannot drive a multi-GiB
    /// pre-allocation.
    #[test]
    fn block_decompress_rejects_oversized_uncomp_len() {
        let crash: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0xb5, 0x7d, 0x0a, 0x87];
        // All three real codecs must complete in bounded memory; some
        // return Ok(empty) (LZF on empty payload), others Err (LZ4/Zstd
        // on truncated frame). None may OOM.
        for codec in [Codec::Lz4, Codec::Zstd, Codec::Lzf] {
            let result = decompress_block(codec, crash);
            // We don't care whether it's Ok or Err — only that it
            // returned without exhausting memory.
            let _ = result;
        }
    }

    /// Regression for fuzz finding
    /// `fuzz_block_decompress/oom-13fdfe71d970dae9836f2df462f166b1fbe24357`
    /// (discovered during the post-fix 5 min smoke run, 2026-05-03).
    ///
    /// 20-byte input `00 00 00 05 ff ff ff 00 00 00 00 ff ff ff ff 07 00 fb f5 00`
    /// is a Druid block header claiming `comp_len = 5`,
    /// `uncomp_len = 0xFFFFFF00 ≈ 4.29 GiB`. The compressed payload then
    /// embeds an LZ4 size-prefix of `0xFF000000 ≈ 4.27 GiB`, which sits
    /// just *under* the `expected_len` cap from the block header — so
    /// our previous LZ4 size-prefix check honored the (huge) cap and
    /// happily allocated 4.27 GiB inside `lz4_flex`.
    ///
    /// Fix: `decompress_block` now rejects `uncomp_len` that exceeds
    /// `comp_len × MAX_BLOCK_EXPANSION_RATIO` (256). A 5 B compressed
    /// payload cannot legitimately decompress to 4 GiB.
    #[test]
    fn block_decompress_rejects_impossible_expansion_ratio() {
        let crash: &[u8] = &[
            0x00, 0x00, 0x00, 0x05, // comp_len = 5
            0xff, 0xff, 0xff, 0x00, // uncomp_len = 0xFFFFFF00 ≈ 4.29 GiB
            0x00, 0x00, 0x00, 0xff,
            0xff, // 5 B compressed payload (LZ4 size prefix 0xFF000000)
            0xff, 0xff, 0xff, 0x07, 0x00, 0xfb, 0xf5, 0x00, // trailing bytes
        ];
        for codec in [Codec::Lz4, Codec::Zstd, Codec::Lzf] {
            let result = decompress_block(codec, crash);
            assert!(
                result.is_err(),
                "expected expansion-ratio rejection for {codec:?}, got {result:?}"
            );
            let msg = result.unwrap_err().to_string();
            assert!(
                msg.contains("uncomp_len") && msg.contains("comp_len"),
                "error should explain the ratio cap: {msg}"
            );
        }
    }

    /// LZF directly: a tiny input with a huge `expected_len` must not
    /// pre-allocate gigabytes.
    #[test]
    fn lzf_decompress_with_huge_expected_len_is_bounded() {
        // Input is 4 bytes of literal-run header + 3 bytes of payload.
        let input: &[u8] = &[0x02, b'a', b'b', b'c'];
        // Caller claims 4 GiB output — must not allocate that much.
        let out = lzf::decompress(input, 4 * 1024 * 1024 * 1024).expect("decompress");
        assert_eq!(out, b"abc");
    }

    #[test]
    fn lzf_decompress_enforces_expected_len_cap() {
        // DD R46: the same 3-byte-producing stream with an expected_len of 2 must
        // be rejected (literal/back-ref writes previously ignored the cap and
        // returned 3 bytes, breaking the block length contract).
        let input: &[u8] = &[0x02, b'a', b'b', b'c'];
        assert!(
            lzf::decompress(input, 2).is_err(),
            "decoding past the expected length cap must be rejected"
        );
        // A block whose codec output does not match the declared uncomp_len is
        // rejected by decompress_block.
        let block = compress_block(Codec::Lzf, b"abc").expect("compress block");
        let mut corrupt = block.clone();
        // Shrink the declared uncomp_len (bytes 4..8, big-endian) to 2.
        corrupt[4..8].copy_from_slice(&2u32.to_be_bytes());
        assert!(
            decompress_block(Codec::Lzf, &corrupt).is_err(),
            "a block whose decoded length != declared uncomp_len must be rejected"
        );
    }

    /// Zstd: a frame whose `frameContentSize` claims gigabytes must not
    /// drive an unbounded read into the output `Vec`.
    #[test]
    fn zstd_decompress_caps_output_at_expected_len() {
        // Real Zstd-encoded data of length 1024.
        let payload = vec![0u8; 1_024];
        let compressed = compress(Codec::Zstd, &payload).expect("compress");
        // Cap at 100 bytes — must error, not allocate 1024.
        let result = decompress(Codec::Zstd, &compressed, 100);
        assert!(result.is_err(), "expected cap rejection, got {result:?}");
        // Cap at exactly the size — must succeed.
        let out = decompress(Codec::Zstd, &compressed, 1_024).expect("at cap");
        assert_eq!(out, payload);
    }
}
