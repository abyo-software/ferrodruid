// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Avro input format support for batch ingestion.
//!
//! Reads Avro Object Container Files (OCF) into the row representation used by
//! the rest of the ingestion pipeline: one [`serde_json::Value`] object per
//! record, mapping top-level field name to value. The resulting rows are fed to
//! [`crate::BatchIngester::ingest`].
//!
//! Druid wire names:
//! * `{"type":"avro_ocf"}` — self-describing Object Container File (schema
//!   embedded in the file header). Implemented here.
//! * `{"type":"avro_stream"}` — bare Avro datum stream that relies on an
//!   externally-supplied reader schema. Recognised by the [`crate::InputFormat`]
//!   enum but **not** decoded by this module (no embedded schema); see crate
//!   limitations.
//!
//! # Type mapping
//!
//! Avro `Record` fields are mapped as follows:
//! null → `Null`; boolean → `Bool`; int/long → integer number; float/double →
//! float number; string/enum → string; `Union(_, inner)` is unwrapped to its
//! inner value (so the common `["null", T]` nullable pattern works); arrays map
//! to JSON arrays; date / timestamp logical types map to their underlying
//! integer. Maps, fixed, bytes, decimal, duration and uuid are surfaced as a
//! best-effort representation (string for uuid, integer for time logicals).
//!
//! # Limitations
//!
//! Nested records inside fields are flattened to a JSON object but the
//! ingestion pipeline only consumes top-level keys, so nested record fields are
//! not addressable without a flattenSpec (unsupported). `avro_stream` requires
//! a dep/transport decision for the external schema and is not decoded.

use apache_avro::Reader;
use apache_avro::types::Value as AvroValue;
use ferrodruid_common::error::{DruidError, Result};

/// Maximum number of records a single Avro OCF parse may accumulate into memory.
///
/// A crafted-but-valid OCF can decode into an arbitrarily large number of
/// records; since every record is materialized into one in-memory
/// [`serde_json::Value`], unbounded accumulation is a resource-exhaustion (OOM)
/// hazard. This cap bounds the worst case, mirroring the `MAX_*` style used by
/// the segment column decoders (`MAX_STRING_COLUMN_ROWS`). Exceeding it is
/// reported as [`DruidError::Ingestion`].
///
/// Lowered from 64 Mi to 8 Mi as part of DD R13: even a single empty
/// `serde_json::Value::Object` map header is on the order of ~50 bytes, so
/// 64 Mi rows is a multi-GiB materialized `Vec<Value>` that a tiny zero-width
/// OCF could drive (see [`MAX_AVRO_ZEROWIDTH_OBJECTS`]). 8 Mi rows is still
/// generous for legitimate batch files (a real OCF carrying 8 Mi records is
/// itself large) while bounding the worst-case `Vec<Value>` to a few hundred
/// MiB rather than multiple GiB.
const MAX_AVRO_ROWS: usize = 8 * 1024 * 1024;

/// Strict ceiling on `object_count` for a data block whose record schema has a
/// *minimum encoded width of zero* (root `null`, an empty record, or a record
/// of only zero-width fields).
///
/// DD R13 finding #1: for a zero-width datum schema each record consumes ZERO
/// payload bytes, so the "declared count must be backed by remaining bytes"
/// invariant that bounds positive-width blocks does not apply — a 0-byte block
/// payload can legitimately encode `object_count` zero-width datums. The
/// upstream `apache_avro::Reader` then yields that many zero-byte records, each
/// of which `parse_avro_ocf_bytes_capped` materializes into a
/// `serde_json::Value`, turning a handful of header bytes into a multi-GiB
/// `Vec<Value>`. The per-byte invariant cannot catch this, so we apply this
/// strict absolute cap instead: a block of zero-width datums may declare at
/// most this many objects. 65 536 is far above any plausible legitimate
/// all-null/empty-record block while keeping the materialized `Vec` tiny.
const MAX_AVRO_ZEROWIDTH_OBJECTS: usize = 64 * 1024;

/// Total budget on the number of schema nodes the writer-schema parser may
/// construct, counting EVERY node (primitives, record fields, union branches,
/// array item / map value nodes, named types), not just named types.
///
/// DD R13 finding #3: the prior `MAX_AVRO_SCHEMA_NODES` bound was enforced only
/// via `named.len()`, leaving unnamed nodes, record field count and union
/// branch count unbounded — a shallow `avro.schema` with millions of
/// `{"name":"fN","type":"null"}` fields (or a huge union) passes the depth /
/// named-type checks but drives proportional `serde_json::from_str` +
/// `Vec::with_capacity(fields.len())` allocation and per-record field walking.
/// This cap is checked incrementally as each node is built so construction
/// fails closed before a large `Vec` is reserved. 1 Mi nodes is generous for
/// any legitimate schema while keeping the parsed [`AvroSchemaTable`] bounded.
const MAX_AVRO_SCHEMA_TOTAL_NODES: usize = 1024 * 1024;

/// Maximum byte length of the raw `avro.schema` metadata value handed to
/// `serde_json::from_str`.
///
/// DD R13 finding #3: `serde_json::from_str` materializes the whole schema JSON
/// before the node-budget check runs, so an enormous (but structurally valid)
/// `avro.schema` value could drive a large `serde_json::Value` allocation up
/// front. The container pre-validator already bounds this value against the
/// remaining input bytes, but a large *file* could still carry a large schema;
/// this is a generous-but-finite absolute backstop (16 MiB of schema JSON is
/// already far beyond any real schema). Exceeding it is rejected with
/// [`DruidError::Ingestion`] before the JSON is parsed.
const MAX_AVRO_SCHEMA_JSON_BYTES: usize = 16 * 1024 * 1024;

/// Maximum nesting depth followed when converting an Avro value to JSON.
///
/// `avro_to_json` recurses on `Array` / `Map` / `Record` / `Union`. Without an
/// explicit bound, a deeply nested value would rely solely on the upstream
/// decoder's depth handling for stack safety. This fixed limit makes stack
/// safety independent of the dependency: a value nested deeper than this is
/// rejected with [`DruidError::Ingestion`].
const MAX_AVRO_DEPTH: usize = 128;

/// Hard ceiling on any single attacker-controlled length/count field declared in
/// an Avro OCF before the bytes backing it are present in the input.
///
/// The upstream `apache_avro` decoder sizes allocations (`Vec`/`HashMap`
/// `reserve`, block buffers) from zig-zag varint length fields read out of the
/// OCF header and block descriptors. Its built-in `max_allocation_bytes` guard
/// caps an *element count* at 512 MiB, but `reserve(count)` on a map/array
/// multiplies that count by the (multi-byte) entry size, so a tiny file can
/// still drive a single multi-GiB `malloc` before any real data is read (the
/// fuzz corpus `oom-avro-ocf-header-length-unbounded-malloc` triggers a
/// ~10 GiB allocation from 16 bytes). The per-record [`MAX_AVRO_ROWS`] cap does
/// not help because it is applied *after* the offending allocation.
///
/// [`prevalidate_ocf_lengths`] therefore enforces the strong, format-level
/// invariant that **no declared length field may exceed the bytes actually
/// remaining in the input** (a block/map/string can never be larger than the
/// file that contains it). This constant is an additional absolute backstop on
/// the entry count of a single map/array block so that even a large but
/// technically-present-byte-count file cannot request an unreasonable `reserve`.
const MAX_AVRO_BLOCK_ITEMS: usize = 16 * 1024 * 1024;

/// Hard ceiling on the number of elements declared by a single array/map block
/// *inside* a data-block payload (the schema-dependent collection counts that
/// the container-level [`prevalidate_ocf_lengths`] walk cannot see).
///
/// The upstream `apache_avro` array/map decoder reads the collection block count
/// straight out of the payload and calls `Vec::reserve(count)` /
/// `HashMap::reserve(count)` *before* decoding any element (see
/// `apache-avro-0.21.0/src/decode.rs`, the `Schema::Array` / `Schema::Map`
/// arms). The decoder's own `safe_len`/`max_allocation_bytes` guard treats that
/// number as an **element count**, not a byte count, so `reserve(200_000_000)`
/// of a multi-byte `Value` enum is still a multi-GiB `malloc` from a tiny file
/// (empirically a 165-byte OCF declaring a 200 M-element `int` array drives an
/// ~11 GiB allocation that *aborts* the process before any per-record cap
/// applies). The container walk in [`prevalidate_ocf_lengths`] never sees this
/// count because it skips the block payload wholesale.
///
/// [`validate_block_payload`] therefore schema-walks each payload and enforces
/// two invariants on every collection / string / bytes / fixed length it finds:
/// (1) the declared count/length may not exceed the bytes actually remaining in
/// the payload (each element / byte needs at least one byte of backing input,
/// so a collection can never declare more entries than the file can hold), and
/// (2) this absolute element-count backstop. The two together bound the largest
/// `reserve` the upstream decoder can be driven to request. Chosen equal to
/// [`MAX_AVRO_BLOCK_ITEMS`] (16 Mi) for consistency with the container backstop.
const MAX_AVRO_COLLECTION_ITEMS: usize = 16 * 1024 * 1024;

/// Per-data-block budget on the SUM of all collection (array + map) element
/// counts decoded anywhere within a single data block's payload.
///
/// DD R14 (High + the class-closing fix): the per-collection
/// [`MAX_AVRO_COLLECTION_ITEMS`] bound and the per-element byte-backing check in
/// [`check_collection_count`] both reason about ONE collection in isolation, and
/// the byte-backing check is intentionally skipped for *zero-width* elements
/// (`array<null>`, `array<empty-record>`) because such elements legitimately
/// consume no payload bytes. That leaves two amplification paths open:
/// (1) a single zero-width collection declaring up to `MAX_AVRO_COLLECTION_ITEMS`
/// (16 Mi) elements from a tiny payload — each element still materializes into an
/// `AvroValue` plus a `serde_json::Value`, ~1 GiB for one row; and (2) MANY
/// small collections whose counts SUM past any per-collection cap (distributed
/// across fields, map values, or nested arrays) while each individually passes.
///
/// This budget closes the whole CLASS rather than each instance: every array and
/// map block count decoded within a block is ADDED to a running accumulator, and
/// the block is rejected the moment the running total exceeds this value —
/// regardless of how the elements are nested, distributed across collections, or
/// whether they are zero- or positive-width. It is the load-bearing bound for
/// zero-width elements (which the byte-backing check cannot see) and an
/// additional layer over positive-width ones.
///
/// Memory reasoning for the chosen value (2 Mi): the upstream decoder
/// materializes each decoded element into an `apache_avro::types::Value` (a
/// `Vec`/`HashMap`-backed enum) which [`avro_to_json`] then converts into a
/// `serde_json::Value`, so the dominant cost is roughly two enum allocations per
/// element. `serde_json::Value` is 24 bytes and `apache_avro::types::Value` is
/// on the same order (tens of bytes); a string/bytes element adds a small heap
/// buffer. Budgeting 2 Mi total elements per block bounds the per-block
/// element-materialization cost to a few hundred MiB worst case
/// (2 Mi x ~(few x tens of bytes)), independent of how many distinct collections
/// the elements are spread across. A single block of one logical row therefore
/// cannot turn a tiny OCF into a multi-GiB materialization. Legitimate batch
/// data rarely packs millions of collection elements into one ~few-MiB block;
/// 2 Mi is generous while keeping the worst case bounded. Exceeding it is
/// reported as [`DruidError::Ingestion`].
const MAX_AVRO_BLOCK_TOTAL_ELEMENTS: usize = 2 * 1024 * 1024;

/// Conservative ceiling handed to [`apache_avro::max_allocation_bytes`] as
/// defense-in-depth.
///
/// This lowers the upstream decoder's built-in element-count cap (set via
/// `apache_avro::util::max_allocation_bytes`) from its 512 MiB default. It is
/// **not** sufficient on its own — `safe_len` interprets the
/// value as an element count rather than a byte count, so a multi-byte element
/// type still multiplies past it (that is the whole reason the schema-walk in
/// [`validate_block_payload`] is the load-bearing fix). It only narrows the
/// residual window for allocation shapes the schema-walk does not model, and it
/// is process-global / set-once in `apache_avro`, so we set it as early as
/// possible. 64 MiB is generous for legitimate length-prefixed fields while far
/// below the default.
const AVRO_MAX_ALLOCATION_BYTES: usize = 64 * 1024 * 1024;

/// Maximum decompressed size of a single `deflate`-coded OCF data block.
///
/// DD R15 (High): the prevalidation schema-walk must see the *datum* bytes, but
/// for a compressed block the on-disk payload is the compressed stream. We
/// therefore inflate each `deflate` block before walking it (see
/// [`decompress_deflate_block`]). Inflation is itself an amplification vector — a
/// tiny compressed block can decode to gigabytes (a "zip bomb") — so the
/// inflate is hard-bounded to this many output bytes and fails closed past it,
/// *before* the walk or the upstream `Reader` ever materializes the datums. The
/// decompressed bytes are then subject to the same per-block element budget,
/// min-width, and collection caps as an uncompressed block, so this cap only
/// bounds the transient inflate buffer. 256 MiB is generous for a legitimate
/// Avro sync-interval block while keeping the worst case bounded.
const MAX_AVRO_DECOMPRESSED_BLOCK_BYTES: usize = 256 * 1024 * 1024;

/// The OCF block codec, parsed from the header `avro.codec` metadata entry.
///
/// apache-avro's default feature set compiles only the `null` and `deflate`
/// codecs, so those are the only two the downstream [`Reader`] can decode. Any
/// other codec string is rejected here (fail closed) rather than schema-walked
/// as if it were raw datum bytes — doing the latter both false-rejects valid
/// compressed files and lets the compressed payload reach the decoder's
/// codec-specific decompression path unbounded (DD R15).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum AvroCodec {
    /// No compression: the block payload is raw datum bytes.
    Null,
    /// RFC 1951 raw DEFLATE (no zlib/gzip header), per the Avro spec.
    Deflate,
}

impl AvroCodec {
    /// Resolve the raw `avro.codec` value bytes. An absent or empty value means
    /// `null` per the Avro spec; an unknown codec is rejected.
    fn from_value_bytes(val: &[u8]) -> Result<Self> {
        match val {
            b"null" | b"" => Ok(Self::Null),
            b"deflate" => Ok(Self::Deflate),
            other => Err(DruidError::Ingestion(format!(
                "unsupported avro OCF codec {:?}; only `null` and `deflate` are supported",
                String::from_utf8_lossy(other)
            ))),
        }
    }
}

/// Inflate a `deflate`-coded OCF data block, bounding the output to
/// [`MAX_AVRO_DECOMPRESSED_BLOCK_BYTES`] so a decompression bomb cannot exhaust
/// memory. See [`decompress_deflate_block_capped`].
fn decompress_deflate_block(compressed: &[u8]) -> Result<Vec<u8>> {
    decompress_deflate_block_capped(compressed, MAX_AVRO_DECOMPRESSED_BLOCK_BYTES)
}

/// Inflate raw DEFLATE `compressed` bytes, rejecting output larger than `cap`.
///
/// The decoder output is wrapped in a [`std::io::Read::take`] of `cap + 1`, so at
/// most `cap + 1` bytes are ever buffered regardless of how large the stream
/// claims to inflate to; if the limit is reached the block is rejected as a
/// potential decompression bomb. The `_capped` form lets tests exercise the
/// bound cheaply (mirroring [`parse_avro_ocf_bytes_capped`]).
fn decompress_deflate_block_capped(compressed: &[u8], cap: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let limit = u64::try_from(cap)
        .ok()
        .and_then(|c| c.checked_add(1))
        .ok_or_else(|| DruidError::Ingestion("avro OCF deflate cap overflow".to_string()))?;
    let mut limited = flate2::read::DeflateDecoder::new(compressed).take(limit);
    let mut out: Vec<u8> = Vec::new();
    limited.read_to_end(&mut out).map_err(|e| {
        DruidError::Ingestion(format!(
            "avro OCF `deflate` block failed to decompress: {e}"
        ))
    })?;
    if out.len() > cap {
        return Err(DruidError::Ingestion(format!(
            "avro OCF `deflate` block decompresses to more than {cap} bytes (potential \
             decompression-bomb resource exhaustion)"
        )));
    }
    Ok(out)
}

/// Set the upstream `apache_avro` allocation ceiling once, as defense-in-depth.
///
/// `apache_avro::max_allocation_bytes` is set-once (process-global); calling it
/// early makes our conservative value win over the 512 MiB default. This is a
/// backstop only — see [`AVRO_MAX_ALLOCATION_BYTES`]; the load-bearing guard is
/// the schema-walk in [`validate_block_payload`].
fn install_avro_allocation_cap() {
    let _ = apache_avro::util::max_allocation_bytes(AVRO_MAX_ALLOCATION_BYTES);
}

/// Parse an Avro Object Container File byte buffer into ingestion rows.
///
/// Each top-level Avro record becomes a JSON object keyed by field name.
///
/// Accumulation is bounded by [`MAX_AVRO_ROWS`] and per-record nesting by
/// [`MAX_AVRO_DEPTH`]; inputs exceeding either are rejected (see Errors).
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the buffer is not a valid OCF, a record
/// cannot be decoded, the record count exceeds [`MAX_AVRO_ROWS`], or a value
/// nests deeper than [`MAX_AVRO_DEPTH`].
pub fn parse_avro_ocf_bytes(data: &[u8]) -> Result<Vec<serde_json::Value>> {
    parse_avro_ocf_bytes_capped(data, MAX_AVRO_ROWS)
}

/// Parse an OCF byte buffer with an explicit row cap.
///
/// Behaves like [`parse_avro_ocf_bytes`] but uses `max_rows` as the
/// accumulation bound, allowing the cap to be exercised cheaply in tests.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] under the same conditions as
/// [`parse_avro_ocf_bytes`], using `max_rows` as the row bound.
fn parse_avro_ocf_bytes_capped(data: &[u8], max_rows: usize) -> Result<Vec<serde_json::Value>> {
    // Lower the upstream decoder's process-global allocation ceiling as
    // defense-in-depth before any decode happens (set-once; see
    // `AVRO_MAX_ALLOCATION_BYTES`). This is a backstop only — the load-bearing
    // guard is the schema-aware payload walk performed below.
    install_avro_allocation_cap();

    // Reject any OCF that declares a length/count larger than the bytes it
    // actually carries *before* handing the buffer to the upstream decoder,
    // which would otherwise size an allocation from that field (see
    // `MAX_AVRO_BLOCK_ITEMS` for the container envelope and
    // `MAX_AVRO_COLLECTION_ITEMS` for the schema-dependent collection counts
    // inside each block payload). This prevents a tiny input from triggering a
    // multi-GiB `malloc`; `catch_unwind` cannot help here because an allocation
    // failure aborts rather than unwinds.
    prevalidate_ocf_lengths(data)?;

    let reader = Reader::new(data)
        .map_err(|e| DruidError::Ingestion(format!("failed to open avro OCF: {e}")))?;

    let mut rows: Vec<serde_json::Value> = Vec::new();
    for record in reader {
        let value = record.map_err(|e| DruidError::Ingestion(format!("avro decode error: {e}")))?;
        if rows.len() >= max_rows {
            return Err(DruidError::Ingestion(format!(
                "avro OCF exceeds row cap of {max_rows}; refusing to accumulate further \
                 (potential resource exhaustion)"
            )));
        }
        rows.push(record_to_json(value, 0)?);
    }
    Ok(rows)
}

/// A bounded cursor over the OCF bytes used by [`prevalidate_ocf_lengths`].
struct OcfCursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> OcfCursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// Bytes left to read from the current position.
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// Read a fixed number of bytes, advancing the cursor.
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            DruidError::Ingestion("avro OCF length overflow while reading bytes".to_string())
        })?;
        if end > self.data.len() {
            return Err(DruidError::Ingestion(
                "truncated avro OCF: not enough bytes for declared field".to_string(),
            ));
        }
        let slice = &self.data[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    /// Decode one zig-zag varint-encoded `long`, mirroring `apache_avro`'s
    /// `read_long`/`decode_variable`. Returns an error on truncation or an
    /// over-long (more than 10-byte) encoding rather than panicking.
    fn read_long(&mut self) -> Result<i64> {
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        loop {
            let byte = *self
                .take(1)?
                .first()
                .ok_or_else(|| DruidError::Ingestion("truncated avro OCF varint".to_string()))?;
            // A 64-bit varint is at most 10 groups of 7 bits.
            if shift >= 64 {
                return Err(DruidError::Ingestion(
                    "malformed avro OCF: varint longer than 64 bits".to_string(),
                ));
            }
            value |= u64::from(byte & 0x7F) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        // Zig-zag decode (matches `zag_i64`).
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }
}

/// Walk the structural length fields of an Avro OCF and reject any that declare
/// more bytes (or map/array entries) than the input can possibly contain.
///
/// This validates: the 4-byte magic, the header metadata map (`Map<Bytes>`:
/// per-block entry count, then for each entry a varint-prefixed key and a
/// varint-prefixed value, terminated by a zero count), the 16-byte sync marker,
/// and each data block (`object count` + `byte size`). For every count/size we
/// require it to be non-negative and no larger than the bytes actually
/// remaining in the input — a block, string, or byte field can never be larger
/// than the file that holds it — plus an absolute [`MAX_AVRO_BLOCK_ITEMS`]
/// backstop on per-block entry counts. Failing closed here prevents the
/// upstream decoder from sizing an unbounded allocation from these fields.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the buffer is not a structurally valid
/// OCF prefix or declares a length exceeding the remaining input.
fn prevalidate_ocf_lengths(data: &[u8]) -> Result<()> {
    let mut cur = OcfCursor::new(data);

    // Magic: "Obj\x01".
    let magic = cur.take(4)?;
    if magic != [b'O', b'b', b'j', 1u8] {
        return Err(DruidError::Ingestion(
            "not an avro OCF: bad magic bytes".to_string(),
        ));
    }

    // Header metadata: an Avro Map<Bytes>, a sequence of blocks each starting
    // with an entry count, terminated by a zero count. The `avro.schema` entry
    // (the writer schema, JSON) is captured so the data-block payloads can be
    // schema-walked below, and the `avro.codec` entry so compressed payloads are
    // inflated before the walk (DD R15).
    let (schema_json, codec) = validate_map_bytes(&mut cur)?;

    // 16-byte sync marker.
    cur.take(16)?;

    // Parse the writer schema. A self-describing OCF must carry `avro.schema`;
    // its absence is a malformed/non-OCF file we refuse here (rather than let
    // the upstream `Reader::new` size allocations from an unvalidated payload).
    let schema_json = schema_json.ok_or_else(|| {
        DruidError::Ingestion("avro OCF header is missing the `avro.schema` entry".to_string())
    })?;
    let schema = parse_writer_schema(&schema_json)?;

    // Data blocks: each is (object_count: long, block_size: long, <payload>,
    // 16-byte sync). The container framing is bounded by `check_len_within_remaining`,
    // but the schema-dependent collection/string counts live *inside* the
    // payload and are invisible to the framing walk. We therefore schema-walk
    // the payload (`object_count` datums) and bound every collection block
    // count, string/bytes length and fixed size before the upstream decoder can
    // size an allocation from them. Stop at clean end-of-input.
    while cur.remaining() > 0 {
        let object_count = cur.read_long()?;
        let object_count = check_nonneg_count(object_count)?;
        let block_size = cur.read_long()?;
        // `block_size` frames the on-disk payload (compressed bytes for a
        // non-`null` codec), so it is bounded against the remaining *file* bytes.
        let block_size = check_len_within_remaining(block_size, cur.remaining())?;
        let payload = cur.take(block_size)?;
        // Schema-walk the *datum* bytes: for `null` that is the payload as-is;
        // for `deflate` the payload must be inflated (bounded) first, otherwise
        // the walk sees compressed bytes and false-rejects valid files while the
        // payload reaches the decoder's decompression path unbounded (DD R15).
        match codec {
            AvroCodec::Null => validate_block_payload(payload, &schema, object_count)?,
            AvroCodec::Deflate => {
                let datum_bytes = decompress_deflate_block(payload)?;
                validate_block_payload(&datum_bytes, &schema, object_count)?;
            }
        }
        // Trailing sync marker.
        cur.take(16)?;
    }

    Ok(())
}

/// Validate the header `Map<Bytes>` blocks, bounding every declared count and
/// byte length against the remaining input, and capture the `avro.schema` value
/// (the writer schema, JSON-encoded) and the `avro.codec` value if present.
///
/// Returns `(schema_json, codec)`: the `avro.schema` value bytes as a UTF-8
/// `String` when the header carries one (the common case for a self-describing
/// OCF) or `None` otherwise, and the resolved block [`AvroCodec`] (defaulting to
/// [`AvroCodec::Null`] when no `avro.codec` entry is present). An unknown codec
/// is rejected here (DD R15).
fn validate_map_bytes(cur: &mut OcfCursor<'_>) -> Result<(Option<String>, AvroCodec)> {
    let mut schema_json: Option<String> = None;
    let mut codec = AvroCodec::Null;
    loop {
        let mut count = cur.read_long()?;
        if count == 0 {
            // Zero count terminates the map.
            return Ok((schema_json, codec));
        }
        if count < 0 {
            // Negative count: abs(count) entries, followed by a (skippable)
            // block byte-size long. `i64::MIN` has no positive magnitude.
            let block_bytes = cur.read_long()?;
            check_len_within_remaining(block_bytes, cur.remaining())?;
            count = count.checked_neg().ok_or_else(|| {
                DruidError::Ingestion("malformed avro OCF: map count overflow".to_string())
            })?;
        }
        let count = check_nonneg_count(count)?;
        for _ in 0..count {
            // key: string = long length prefix + that many bytes.
            let key_len = cur.read_long()?;
            let key_len = check_len_within_remaining(key_len, cur.remaining())?;
            let key = cur.take(key_len)?;
            // value: bytes = long length prefix + that many bytes.
            let val_len = cur.read_long()?;
            let val_len = check_len_within_remaining(val_len, cur.remaining())?;
            let val = cur.take(val_len)?;
            // Capture the writer schema and the codec; other header entries do
            // not affect the binary layout we validate. Non-UTF-8 schema bytes
            // are malformed and rejected.
            if key == b"avro.schema" {
                let text = std::str::from_utf8(val).map_err(|_| {
                    DruidError::Ingestion(
                        "avro OCF `avro.schema` value is not valid UTF-8".to_string(),
                    )
                })?;
                schema_json = Some(text.to_string());
            } else if key == b"avro.codec" {
                codec = AvroCodec::from_value_bytes(val)?;
            }
        }
    }
}

/// Reject a negative count and the [`MAX_AVRO_BLOCK_ITEMS`] backstop, returning
/// the count as a `usize`.
fn check_nonneg_count(count: i64) -> Result<usize> {
    let count = usize::try_from(count).map_err(|_| {
        DruidError::Ingestion("malformed avro OCF: negative declared count".to_string())
    })?;
    if count > MAX_AVRO_BLOCK_ITEMS {
        return Err(DruidError::Ingestion(format!(
            "avro OCF declares {count} items in one block, exceeding the cap of \
             {MAX_AVRO_BLOCK_ITEMS} (potential resource exhaustion)"
        )));
    }
    Ok(count)
}

/// Reject a negative length or one larger than `remaining` input bytes,
/// returning it as a `usize`.
fn check_len_within_remaining(len: i64, remaining: usize) -> Result<usize> {
    let len = usize::try_from(len).map_err(|_| {
        DruidError::Ingestion("malformed avro OCF: negative declared length".to_string())
    })?;
    if len > remaining {
        return Err(DruidError::Ingestion(format!(
            "avro OCF declares a length of {len} bytes but only {remaining} bytes remain \
             (potential resource exhaustion)"
        )));
    }
    Ok(len)
}

/// A minimal, clean-room representation of an Avro writer schema, sufficient to
/// drive the binary-layout walk in [`validate_block_payload`].
///
/// This intentionally captures only what the binary encoding needs to know at
/// each position — *which* shape the next datum is — not the full Avro type
/// system (names, namespaces, defaults, logical types, doc strings, aliases are
/// all irrelevant to the byte layout and are dropped). Named-type references
/// are resolved to [`AvroNode::Ref`] indices into a side table so recursive
/// schemas (e.g. a tree record) terminate. The binary layout of a logical type
/// is identical to its underlying type, so logical types collapse to that
/// underlying primitive.
enum AvroNode {
    /// Fixed-width or zero-width primitive: null, boolean, int, long, float,
    /// double. These carry no attacker-controlled length prefix.
    Primitive(AvroPrimitive),
    /// Length-prefixed `bytes`.
    Bytes,
    /// Length-prefixed `string`.
    StringType,
    /// `fixed`: exactly `size` bytes, no length prefix (size is from the schema,
    /// not the payload, so it is trusted but still bounded against remaining
    /// input when consumed).
    Fixed(usize),
    /// `enum`: an int index, decoded as a single zig-zag int. No length prefix.
    Enum,
    /// `array`: block-count-framed sequence of `items`.
    Array(Box<AvroNode>),
    /// `map`: block-count-framed sequence of `string` key + `values`.
    Map(Box<AvroNode>),
    /// `union`: an index prefix selecting one of `variants`.
    Union(Vec<AvroNode>),
    /// `record`: an ordered, fixed sequence of field schemas.
    Record(Vec<AvroNode>),
    /// A reference to a previously-defined named type, by index into the schema
    /// table's `named` vector. Resolved at walk time so recursive schemas have a
    /// finite representation.
    Ref(usize),
}

/// The fixed/zero-width Avro primitives, distinguished by how many bytes the
/// walk must skip (varints are decoded; fixed-width little-endian floats are
/// counted).
enum AvroPrimitive {
    /// `null`: zero bytes.
    Null,
    /// `boolean`: exactly one byte.
    Boolean,
    /// `int` / `long`: a single zig-zag varint.
    Varint,
    /// `float`: 4 little-endian bytes.
    Float,
    /// `double`: 8 little-endian bytes.
    Double,
}

/// A parsed writer schema: the root node plus the table of named types it may
/// reference (records, enums, fixed) so [`AvroNode::Ref`] can be resolved.
struct AvroSchemaTable {
    root: AvroNode,
    named: Vec<AvroNode>,
}

/// Bound on the number of named types and on schema-parse recursion depth. A
/// crafted schema JSON could otherwise be deeply nested or define an absurd
/// number of named types; both are rejected well before they matter.
const MAX_AVRO_SCHEMA_NODES: usize = 4096;

/// Parse the writer schema JSON into the minimal [`AvroSchemaTable`] used by the
/// payload walk.
///
/// Only the binary-layout-relevant structure is retained. Constructs that do not
/// change the byte layout (names beyond reference resolution, namespaces,
/// defaults, doc, aliases, logical-type annotations) are ignored. Unknown or
/// malformed type constructs are rejected with [`DruidError::Ingestion`] (fail
/// closed) rather than guessed.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the JSON is not parseable or describes a
/// schema construct this minimal model does not understand.
fn parse_writer_schema(json: &str) -> Result<AvroSchemaTable> {
    // DD R13 finding #3: `serde_json::from_str` materializes the whole schema
    // JSON before any node-budget check runs, so bound the raw schema byte
    // length up front. The container pre-validator already bounds this value
    // against remaining input; this is an additional absolute backstop.
    if json.len() > MAX_AVRO_SCHEMA_JSON_BYTES {
        return Err(DruidError::Ingestion(format!(
            "avro OCF `avro.schema` is {} bytes, exceeding the cap of {MAX_AVRO_SCHEMA_JSON_BYTES} \
             (potential resource exhaustion)",
            json.len()
        )));
    }
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| {
        DruidError::Ingestion(format!("avro OCF `avro.schema` is not valid JSON: {e}"))
    })?;
    let mut builder = SchemaBuilder {
        named: Vec::new(),
        name_index: std::collections::HashMap::new(),
        total_nodes: 0,
    };
    let root = builder.build(&value, None, 0)?;
    Ok(AvroSchemaTable {
        root,
        named: builder.named,
    })
}

/// Internal accumulator threaded through schema construction. `named` holds the
/// resolved body of each named type in definition order; `name_index` maps a
/// fully-or-simply-qualified type name to its slot for [`AvroNode::Ref`].
struct SchemaBuilder {
    named: Vec<AvroNode>,
    name_index: std::collections::HashMap<String, usize>,
    /// Running count of every node built (DD R13 finding #3), bounded by
    /// [`MAX_AVRO_SCHEMA_TOTAL_NODES`] so unnamed nodes, record fields and union
    /// branches cannot grow the schema without limit.
    total_nodes: usize,
}

impl SchemaBuilder {
    /// Account for one schema node, failing closed once the total node budget is
    /// exhausted. Called for every node built so unnamed structural nodes,
    /// record fields and union branches all count against the budget.
    fn count_node(&mut self) -> Result<()> {
        self.total_nodes = self.total_nodes.saturating_add(1);
        if self.total_nodes > MAX_AVRO_SCHEMA_TOTAL_NODES {
            return Err(DruidError::Ingestion(format!(
                "avro OCF schema defines more than {MAX_AVRO_SCHEMA_TOTAL_NODES} nodes \
                 (potential resource exhaustion)"
            )));
        }
        Ok(())
    }

    /// Recursively translate a JSON schema fragment into an [`AvroNode`].
    ///
    /// `enclosing_namespace` carries the namespace inherited from the parent
    /// named type so unqualified `Ref` names resolve the way Avro defines.
    /// `depth` bounds recursion against pathological JSON.
    fn build(
        &mut self,
        value: &serde_json::Value,
        enclosing_namespace: Option<&str>,
        depth: usize,
    ) -> Result<AvroNode> {
        if depth > MAX_AVRO_DEPTH || self.named.len() > MAX_AVRO_SCHEMA_NODES {
            return Err(DruidError::Ingestion(
                "avro OCF schema is too deeply nested or defines too many types".to_string(),
            ));
        }
        // Count every node against the total-node budget (DD R13 finding #3).
        self.count_node()?;
        match value {
            // A bare type name: `"int"`, `"string"`, or a reference to a
            // previously-defined named type.
            serde_json::Value::String(name) => {
                self.build_named_or_primitive(name, enclosing_namespace)
            }
            // A union is a JSON array of branch schemas.
            serde_json::Value::Array(branches) => {
                // Cap the branch count against the remaining node budget BEFORE
                // reserving a `Vec` for it (DD R13 finding #3): a huge union
                // array is otherwise unbounded `Vec::with_capacity`.
                if branches.len() > MAX_AVRO_SCHEMA_TOTAL_NODES {
                    return Err(DruidError::Ingestion(format!(
                        "avro OCF union declares {} branches, exceeding the cap of \
                         {MAX_AVRO_SCHEMA_TOTAL_NODES} (potential resource exhaustion)",
                        branches.len()
                    )));
                }
                let mut variants = Vec::with_capacity(branches.len());
                for branch in branches {
                    variants.push(self.build(branch, enclosing_namespace, depth + 1)?);
                }
                Ok(AvroNode::Union(variants))
            }
            // A complex type object with a `"type"` discriminator.
            serde_json::Value::Object(map) => self.build_object(map, enclosing_namespace, depth),
            other => Err(DruidError::Ingestion(format!(
                "avro OCF schema fragment is not a string, array or object: {other}"
            ))),
        }
    }

    /// Resolve a bare type name to a primitive node or a reference to a named
    /// type defined earlier in the schema.
    fn build_named_or_primitive(
        &self,
        name: &str,
        enclosing_namespace: Option<&str>,
    ) -> Result<AvroNode> {
        Ok(match name {
            "null" => AvroNode::Primitive(AvroPrimitive::Null),
            "boolean" => AvroNode::Primitive(AvroPrimitive::Boolean),
            "int" | "long" => AvroNode::Primitive(AvroPrimitive::Varint),
            "float" => AvroNode::Primitive(AvroPrimitive::Float),
            "double" => AvroNode::Primitive(AvroPrimitive::Double),
            "bytes" => AvroNode::Bytes,
            "string" => AvroNode::StringType,
            other => {
                // A reference to a named type. Try the namespace-qualified name
                // first, then the bare name.
                if let Some(ns) = enclosing_namespace {
                    let qualified = format!("{ns}.{other}");
                    if let Some(&idx) = self.name_index.get(&qualified) {
                        return Ok(AvroNode::Ref(idx));
                    }
                }
                let idx = self.name_index.get(other).copied().ok_or_else(|| {
                    DruidError::Ingestion(format!(
                        "avro OCF schema references unknown type `{other}`"
                    ))
                })?;
                AvroNode::Ref(idx)
            }
        })
    }

    /// Translate a complex-type JSON object (one with a `"type"` field).
    fn build_object(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
        enclosing_namespace: Option<&str>,
        depth: usize,
    ) -> Result<AvroNode> {
        let type_value = map.get("type").ok_or_else(|| {
            DruidError::Ingestion("avro OCF schema object is missing a `type`".to_string())
        })?;
        let type_name = match type_value {
            serde_json::Value::String(s) => s.as_str(),
            // DD R16: a `"type"` whose value is itself a complex type — a nested
            // object (`{"type":{"type":"record",...}}`) or a union array
            // (`{"type":["null","string"]}`). apache-avro accepts these wrappers,
            // so we must too rather than false-reject. The wrapper carries no name
            // of its own, so it introduces no namespace; parse the inner type with
            // the enclosing namespace. `depth + 1` keeps recursion bounded.
            other => return self.build(other, enclosing_namespace, depth + 1),
        };

        // The namespace this object introduces, if any, inherited by children.
        let namespace = map
            .get("namespace")
            .and_then(serde_json::Value::as_str)
            .or(enclosing_namespace);

        match type_name {
            // Primitive types may also appear in object form, optionally with a
            // logical-type annotation; the binary layout is the underlying type.
            "null" | "boolean" | "int" | "long" | "float" | "double" | "bytes" | "string" => {
                self.build_named_or_primitive(type_name, enclosing_namespace)
            }
            "array" => {
                let items = map.get("items").ok_or_else(|| {
                    DruidError::Ingestion("avro OCF array schema missing `items`".to_string())
                })?;
                let inner = self.build(items, namespace, depth + 1)?;
                Ok(AvroNode::Array(Box::new(inner)))
            }
            "map" => {
                let values = map.get("values").ok_or_else(|| {
                    DruidError::Ingestion("avro OCF map schema missing `values`".to_string())
                })?;
                let inner = self.build(values, namespace, depth + 1)?;
                Ok(AvroNode::Map(Box::new(inner)))
            }
            "record" | "error" => self.build_record(map, namespace, depth),
            "enum" => {
                self.register_named(map, namespace, AvroNode::Enum)?;
                Ok(AvroNode::Enum)
            }
            "fixed" => {
                let size = map
                    .get("size")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| {
                        DruidError::Ingestion(
                            "avro OCF fixed schema missing non-negative `size`".to_string(),
                        )
                    })?;
                let size = usize::try_from(size).map_err(|_| {
                    DruidError::Ingestion("avro OCF fixed `size` is too large".to_string())
                })?;
                if size > MAX_AVRO_BLOCK_ITEMS {
                    return Err(DruidError::Ingestion(format!(
                        "avro OCF fixed `size` of {size} exceeds the cap of {MAX_AVRO_BLOCK_ITEMS}"
                    )));
                }
                self.register_named(map, namespace, AvroNode::Fixed(size))?;
                Ok(AvroNode::Fixed(size))
            }
            other => Err(DruidError::Ingestion(format!(
                "avro OCF schema uses unsupported type `{other}`"
            ))),
        }
    }

    /// Build a record node, registering its name *before* its fields so a field
    /// may reference the record recursively.
    fn build_record(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
        namespace: Option<&str>,
        depth: usize,
    ) -> Result<AvroNode> {
        // Reserve a slot for this record name first (records may recurse).
        let slot = self.register_placeholder(map, namespace)?;
        let fields = map
            .get("fields")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                DruidError::Ingestion("avro OCF record schema missing `fields` array".to_string())
            })?;
        // Cap the field count against the node budget BEFORE reserving a `Vec`
        // for it (DD R13 finding #3): a shallow record with millions of fields
        // is otherwise unbounded `Vec::with_capacity`.
        if fields.len() > MAX_AVRO_SCHEMA_TOTAL_NODES {
            return Err(DruidError::Ingestion(format!(
                "avro OCF record declares {} fields, exceeding the cap of \
                 {MAX_AVRO_SCHEMA_TOTAL_NODES} (potential resource exhaustion)",
                fields.len()
            )));
        }
        let mut field_nodes = Vec::with_capacity(fields.len());
        for field in fields {
            let field_type = field
                .as_object()
                .and_then(|f| f.get("type"))
                .ok_or_else(|| {
                    DruidError::Ingestion("avro OCF record field missing `type`".to_string())
                })?;
            field_nodes.push(self.build(field_type, namespace, depth + 1)?);
        }
        let record = AvroNode::Record(field_nodes);
        // Fill the reserved slot (if one was registered) with the real body so
        // references resolve to the full record.
        if let Some(idx) = slot {
            self.named[idx] = clone_node(&record);
        }
        Ok(record)
    }

    /// Register the fully-qualified name of a named type, pointing at a freshly
    /// pushed placeholder slot, and return its index. Returns `None` when the
    /// type carries no `name` (which is malformed for a record/enum/fixed but we
    /// fail closed elsewhere).
    fn register_placeholder(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
        namespace: Option<&str>,
    ) -> Result<Option<usize>> {
        let Some(name) = map.get("name").and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        if self.named.len() >= MAX_AVRO_SCHEMA_NODES {
            return Err(DruidError::Ingestion(
                "avro OCF schema defines too many named types".to_string(),
            ));
        }
        let idx = self.named.len();
        // Placeholder body; overwritten once the real body is built.
        self.named.push(AvroNode::Primitive(AvroPrimitive::Null));
        self.name_index.insert(name.to_string(), idx);
        if let Some(ns) = namespace {
            self.name_index.insert(format!("{ns}.{name}"), idx);
        }
        Ok(Some(idx))
    }

    /// Register an already-built named node (enum / fixed, which cannot recurse
    /// into themselves) in the name table.
    fn register_named(
        &mut self,
        map: &serde_json::Map<String, serde_json::Value>,
        namespace: Option<&str>,
        node: AvroNode,
    ) -> Result<()> {
        let Some(name) = map.get("name").and_then(serde_json::Value::as_str) else {
            return Ok(());
        };
        if self.named.len() >= MAX_AVRO_SCHEMA_NODES {
            return Err(DruidError::Ingestion(
                "avro OCF schema defines too many named types".to_string(),
            ));
        }
        let idx = self.named.len();
        self.named.push(node);
        self.name_index.insert(name.to_string(), idx);
        if let Some(ns) = namespace {
            self.name_index.insert(format!("{ns}.{name}"), idx);
        }
        Ok(())
    }
}

/// Deep-clone an [`AvroNode`] (used to fill a record's reserved name slot).
fn clone_node(node: &AvroNode) -> AvroNode {
    match node {
        AvroNode::Primitive(p) => AvroNode::Primitive(match p {
            AvroPrimitive::Null => AvroPrimitive::Null,
            AvroPrimitive::Boolean => AvroPrimitive::Boolean,
            AvroPrimitive::Varint => AvroPrimitive::Varint,
            AvroPrimitive::Float => AvroPrimitive::Float,
            AvroPrimitive::Double => AvroPrimitive::Double,
        }),
        AvroNode::Bytes => AvroNode::Bytes,
        AvroNode::StringType => AvroNode::StringType,
        AvroNode::Fixed(n) => AvroNode::Fixed(*n),
        AvroNode::Enum => AvroNode::Enum,
        AvroNode::Array(inner) => AvroNode::Array(Box::new(clone_node(inner))),
        AvroNode::Map(inner) => AvroNode::Map(Box::new(clone_node(inner))),
        AvroNode::Union(variants) => AvroNode::Union(variants.iter().map(clone_node).collect()),
        AvroNode::Record(fields) => AvroNode::Record(fields.iter().map(clone_node).collect()),
        AvroNode::Ref(idx) => AvroNode::Ref(*idx),
    }
}

/// The minimum number of payload BYTES any single datum of `node` can occupy in
/// Avro binary encoding. This is the unifying quantity behind the DD R13 fixes:
/// it lets the payload walk distinguish a *zero-width* datum/element (which a
/// 0-byte payload can legitimately encode many of) from a positive-width one
/// (whose count must be backed by remaining bytes).
///
/// Per the Avro binary spec the minimum widths are:
/// * `null` = 0
/// * `boolean` = 1
/// * `int` / `long` = 1 (a single varint byte, the encoding of `0`)
/// * `float` = 4, `double` = 8 (fixed little-endian)
/// * `bytes` / `string` = 1 (the length varint of a zero-length value)
/// * `fixed(n)` = `n`
/// * `enum` = 1 (the index varint)
/// * `array` / `map` = 1 (a single `count == 0` terminator block byte for an
///   EMPTY collection)
/// * `record` = the sum of its fields' minimum widths
/// * `union` = ONE branch-index varint byte (>= 1, always present in the Avro
///   binary union encoding) PLUS the MIN over its branches
///
/// DD R14 (Medium): a union datum ALWAYS encodes a zig-zag branch-index varint
/// (>= 1 byte) *before* the selected branch payload, so its minimum width is
/// `1 + min_branch_width`, never the bare branch minimum. Omitting the index
/// byte computed `["null", T]` as 0 (real min 1) and `["int", "long"]` as 1
/// (real min 2), which both *false-rejected* valid nullable-union blocks (their
/// records were mis-classified as zero-width / under-backed) and let an
/// `array<["null", T]>` be treated as a zero-width element. Adding the index
/// byte fixes both: a nullable union is now correctly positive-width.
///
/// Recursive named references are handled with a `visiting` stack: re-entering a
/// reference already on the stack means that path cannot bottom out at a finite
/// minimum, so it contributes [`usize::MAX`] (saturating). For a `union` the
/// MIN then naturally prefers a terminating branch (the `["null", Ref]` shape
/// resolves to its `null` branch, giving `1 + 0 == 1`); for a `record` a
/// non-terminating field makes
/// the record's minimum saturate, which is a safe *positive* width. All
/// arithmetic saturates so the result is always a usable bound and never
/// overflows.
fn min_encoded_width(
    node: &AvroNode,
    schema: &AvroSchemaTable,
    visiting: &mut Vec<usize>,
) -> usize {
    match node {
        AvroNode::Primitive(AvroPrimitive::Null) => 0,
        AvroNode::Primitive(AvroPrimitive::Boolean)
        | AvroNode::Primitive(AvroPrimitive::Varint)
        | AvroNode::Enum
        | AvroNode::Bytes
        | AvroNode::StringType
        | AvroNode::Array(_)
        | AvroNode::Map(_) => 1,
        AvroNode::Primitive(AvroPrimitive::Float) => 4,
        AvroNode::Primitive(AvroPrimitive::Double) => 8,
        AvroNode::Fixed(n) => *n,
        AvroNode::Record(fields) => {
            let mut total: usize = 0;
            for field in fields {
                total = total.saturating_add(min_encoded_width(field, schema, visiting));
            }
            total
        }
        AvroNode::Union(variants) => variants
            .iter()
            .map(|v| min_encoded_width(v, schema, visiting))
            .min()
            // A union always encodes a branch-index varint (>= 1 byte) before
            // the selected branch, so the minimum is 1 + the smallest branch
            // width (DD R14 Medium). `saturating_add` keeps a saturated branch
            // minimum (recursive-only) at `usize::MAX`.
            .map(|min_branch| 1usize.saturating_add(min_branch))
            // A union with no branches cannot encode any datum; treat as a
            // positive (saturated) width so it is never classified zero-width.
            .unwrap_or(usize::MAX),
        AvroNode::Ref(idx) => {
            if visiting.contains(idx) {
                // Cycle: this path does not terminate at a finite minimum.
                return usize::MAX;
            }
            match schema.named.get(*idx) {
                Some(resolved) => {
                    visiting.push(*idx);
                    let w = min_encoded_width(resolved, schema, visiting);
                    visiting.pop();
                    w
                }
                // An unresolved reference is treated as positive width (safe:
                // the payload walk will reject it on its own merits).
                None => usize::MAX,
            }
        }
    }
}

/// Schema-walk one data block's payload, validating every attacker-controlled
/// collection count and length field against the bytes remaining in the payload
/// and against the absolute [`MAX_AVRO_COLLECTION_ITEMS`] backstop.
///
/// This is the load-bearing OOM guard: the upstream `apache_avro` array/map
/// decoder reserves `Vec`/`HashMap` capacity from collection block counts read
/// out of the payload *before* decoding any element, so an unvalidated count can
/// drive a multi-GiB allocation even from a tiny file. By walking the payload
/// with the writer schema and rejecting any count/length that cannot be backed
/// by the remaining payload bytes, no such allocation can be requested.
///
/// The walk decodes `object_count` datums (one record per object) and must
/// consume them within the payload; it does not require consuming the payload
/// exactly (the upstream decoder is the source of truth for trailing bytes), it
/// only bounds the length fields it encounters.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if any length/count exceeds its bound or the
/// payload is truncated relative to the schema.
fn validate_block_payload(
    payload: &[u8],
    schema: &AvroSchemaTable,
    object_count: usize,
) -> Result<()> {
    // DD R13 finding #1: a zero-width record schema (root `null`, an empty
    // record, or a record of only zero-width fields) lets a tiny 0-byte payload
    // legitimately encode an arbitrary `object_count`; the upstream decoder then
    // yields that many zero-byte records, each materialized into a
    // `serde_json::Value`, turning a handful of header bytes into a multi-GiB
    // `Vec<Value>`. The per-byte invariant cannot catch this because the datums
    // consume no bytes. Classify the block by its record's minimum encoded width
    // and bound `object_count` accordingly:
    // * min_width == 0: apply the strict absolute `MAX_AVRO_ZEROWIDTH_OBJECTS`.
    // * min_width  > 0: each datum needs at least `min_width` payload bytes, so
    //   the block can hold at most `payload.len() / min_width` of them.
    let mut visiting: Vec<usize> = Vec::new();
    let min_width = min_encoded_width(&schema.root, schema, &mut visiting);
    if min_width == 0 {
        if object_count > MAX_AVRO_ZEROWIDTH_OBJECTS {
            return Err(DruidError::Ingestion(format!(
                "avro OCF data block declares {object_count} zero-width datums, exceeding the \
                 cap of {MAX_AVRO_ZEROWIDTH_OBJECTS} (potential resource exhaustion: zero-width \
                 records consume no payload bytes)"
            )));
        }
    } else if let Some(max_objects) = payload.len().checked_div(min_width)
        && object_count > max_objects
    {
        return Err(DruidError::Ingestion(format!(
            "avro OCF data block declares {object_count} datums but its {} payload bytes \
             can back at most {max_objects} (minimum datum width {min_width}; potential \
             resource exhaustion)",
            payload.len()
        )));
    }

    // DD R14 (High + class-closer): bound the SUM of all collection element
    // counts decoded anywhere in this block against `MAX_AVRO_BLOCK_TOTAL_ELEMENTS`.
    // This is the load-bearing guard for zero-width elements (the byte-backing
    // check in `check_collection_count` is intentionally skipped for them) and it
    // also catches the "many small collections summing past the cap" distributed
    // case that no per-collection bound can see. The accumulator covers the whole
    // block because the upstream `Reader` decodes and materializes a block's
    // datums together.
    let mut cur = OcfCursor::new(payload);
    let mut total_elements: usize = 0;
    for _ in 0..object_count {
        walk_datum(&mut cur, &schema.root, schema, 0, &mut total_elements)?;
    }
    Ok(())
}

/// Walk a single datum of the given node, advancing `cur` past its bytes and
/// validating every embedded length/count. `depth` bounds recursion (shared
/// [`MAX_AVRO_DEPTH`]). `total_elements` is the running per-block accumulator of
/// every array/map element count decoded so far, bounded by
/// [`MAX_AVRO_BLOCK_TOTAL_ELEMENTS`] (DD R14).
fn walk_datum(
    cur: &mut OcfCursor<'_>,
    node: &AvroNode,
    schema: &AvroSchemaTable,
    depth: usize,
    total_elements: &mut usize,
) -> Result<()> {
    if depth > MAX_AVRO_DEPTH {
        return Err(DruidError::Ingestion(format!(
            "avro OCF payload nests deeper than the limit of {MAX_AVRO_DEPTH}"
        )));
    }
    match node {
        AvroNode::Primitive(AvroPrimitive::Null) => {}
        AvroNode::Primitive(AvroPrimitive::Boolean) => {
            cur.take(1)?;
        }
        AvroNode::Primitive(AvroPrimitive::Varint) => {
            cur.read_long()?;
        }
        AvroNode::Primitive(AvroPrimitive::Float) => {
            cur.take(4)?;
        }
        AvroNode::Primitive(AvroPrimitive::Double) => {
            cur.take(8)?;
        }
        AvroNode::Enum => {
            // An int index; a single zig-zag varint.
            cur.read_long()?;
        }
        AvroNode::Fixed(size) => {
            cur.take(*size)?;
        }
        AvroNode::Bytes | AvroNode::StringType => {
            let len = cur.read_long()?;
            let len = check_len_within_remaining(len, cur.remaining())?;
            cur.take(len)?;
        }
        AvroNode::Array(items) => {
            // DD R13 finding #2: the "count <= remaining bytes" element-backing
            // check is only valid when each element needs >= 1 payload byte.
            // For a zero-width item schema (`null`, empty record, record of only
            // zero-width fields) a valid array of N elements encodes as
            // `count=N` then a `count=0` terminator with NO per-element bytes, so
            // the byte check would false-reject valid data. Pass the element's
            // minimum width so the byte check is skipped (relying solely on
            // `MAX_AVRO_COLLECTION_ITEMS`) exactly when it is zero-width.
            let mut visiting: Vec<usize> = Vec::new();
            let item_min_width = min_encoded_width(items, schema, &mut visiting);
            walk_collection(
                cur,
                schema,
                depth,
                item_min_width,
                total_elements,
                |cur, schema, depth, total_elements| {
                    walk_datum(cur, items, schema, depth + 1, total_elements)
                },
            )?;
        }
        AvroNode::Map(values) => {
            // A map entry is always a non-empty string key (length varint, >= 1
            // byte) followed by the value datum, so each entry consumes at least
            // one payload byte: the byte-backing check stays valid for maps.
            walk_collection(
                cur,
                schema,
                depth,
                1,
                total_elements,
                |cur, schema, depth, total_elements| {
                    // Each map entry is a string key followed by the value datum.
                    let key_len = cur.read_long()?;
                    let key_len = check_len_within_remaining(key_len, cur.remaining())?;
                    cur.take(key_len)?;
                    walk_datum(cur, values, schema, depth + 1, total_elements)
                },
            )?;
        }
        AvroNode::Union(variants) => {
            let index = cur.read_long()?;
            let index = usize::try_from(index).map_err(|_| {
                DruidError::Ingestion("avro OCF payload union index is negative".to_string())
            })?;
            let variant = variants.get(index).ok_or_else(|| {
                DruidError::Ingestion(format!(
                    "avro OCF payload union index {index} out of range ({} variants)",
                    variants.len()
                ))
            })?;
            walk_datum(cur, variant, schema, depth + 1, total_elements)?;
        }
        AvroNode::Record(fields) => {
            for field in fields {
                walk_datum(cur, field, schema, depth + 1, total_elements)?;
            }
        }
        AvroNode::Ref(idx) => {
            let resolved = schema.named.get(*idx).ok_or_else(|| {
                DruidError::Ingestion("avro OCF schema reference is unresolved".to_string())
            })?;
            walk_datum(cur, resolved, schema, depth + 1, total_elements)?;
        }
    }
    Ok(())
}

/// Walk a block-count-framed collection (array or map), bounding every block
/// count against the remaining payload bytes and the absolute element cap, then
/// invoking `walk_entry` once per element. Terminates at a zero-count block.
///
/// Avro encodes a collection as a series of blocks; each block is a long count
/// `n`: `n == 0` ends the collection; `n > 0` means `n` entries follow; `n < 0`
/// means `abs(n)` entries follow *and* a block byte-size long precedes them
/// (used for skipping). Both forms are validated here — the upstream decoder
/// reserves `abs(n)` capacity before reading any entry, so `abs(n)` is exactly
/// the attacker-controlled value we must bound.
fn walk_collection<F>(
    cur: &mut OcfCursor<'_>,
    schema: &AvroSchemaTable,
    depth: usize,
    element_min_width: usize,
    total_elements: &mut usize,
    mut walk_entry: F,
) -> Result<()>
where
    F: FnMut(&mut OcfCursor<'_>, &AvroSchemaTable, usize, &mut usize) -> Result<()>,
{
    loop {
        let raw = cur.read_long()?;
        if raw == 0 {
            return Ok(());
        }
        let count = if raw < 0 {
            // Negative count: `abs(raw)` entries preceded by a block byte size.
            let block_bytes = cur.read_long()?;
            check_len_within_remaining(block_bytes, cur.remaining())?;
            raw.checked_neg().ok_or_else(|| {
                DruidError::Ingestion("avro OCF payload collection count overflow".to_string())
            })?
        } else {
            raw
        };
        let count = check_collection_count(count, cur.remaining(), element_min_width)?;
        // DD R14: add this block's element count to the per-block running total
        // and fail closed if the SUM exceeds the budget. This is checked BEFORE
        // iterating the elements, so a huge (especially zero-width) count is
        // rejected cheaply without materializing anything; it also catches many
        // small collections whose counts sum past the budget. `saturating_add`
        // cannot overflow, and the cap is far below `usize::MAX`.
        *total_elements = total_elements.saturating_add(count);
        if *total_elements > MAX_AVRO_BLOCK_TOTAL_ELEMENTS {
            return Err(DruidError::Ingestion(format!(
                "avro OCF data block decodes more than {MAX_AVRO_BLOCK_TOTAL_ELEMENTS} total \
                 collection elements (running total {total}; potential resource exhaustion: \
                 the sum of all array/map element counts in a block is bounded regardless of \
                 how they are nested, distributed, or whether they are zero-width)",
                total = *total_elements
            )));
        }
        for _ in 0..count {
            walk_entry(cur, schema, depth, total_elements)?;
        }
    }
}

/// Reject a collection element count that exceeds the bytes the payload can back
/// or the absolute [`MAX_AVRO_COLLECTION_ITEMS`] backstop, returning it as a
/// `usize`.
///
/// This is the check the prior container-only validator was missing: it sees the
/// schema-dependent array/map block count that lives *inside* the data-block
/// payload, the exact value the upstream decoder uses to size its `reserve`.
///
/// DD R13 finding #2: the byte-backing bound (`count` elements each need at
/// least `element_min_width` payload bytes) is only valid when
/// `element_min_width > 0`. For a zero-width array item schema (`null`, empty
/// record, record of only zero-width fields) a valid array encodes its count
/// with NO per-element bytes, so applying a per-element byte bound would
/// false-reject valid data. When `element_min_width == 0` we therefore rely
/// solely on the absolute `MAX_AVRO_COLLECTION_ITEMS` cap (which still caps the
/// upstream `reserve`); when it is positive the per-byte bound applies and is
/// the load-bearing R12 defense. (Maps always pass `element_min_width >= 1`
/// because each entry carries a string-key length varint.)
fn check_collection_count(count: i64, remaining: usize, element_min_width: usize) -> Result<usize> {
    let count = usize::try_from(count).map_err(|_| {
        DruidError::Ingestion("avro OCF payload declares a negative collection count".to_string())
    })?;
    // `checked_div` is `None` only for `element_min_width == 0`, which is the
    // zero-width case that intentionally skips the per-byte bound (DD R13
    // finding #2). Otherwise each element needs at least `element_min_width`
    // payload bytes, so the payload can back at most
    // `remaining / element_min_width` of them.
    if let Some(max_elems) = remaining.checked_div(element_min_width)
        && count > max_elems
    {
        return Err(DruidError::Ingestion(format!(
            "avro OCF payload declares a collection of {count} elements but only {remaining} \
             payload bytes remain (minimum element width {element_min_width}; potential \
             resource exhaustion)"
        )));
    }
    if count > MAX_AVRO_COLLECTION_ITEMS {
        return Err(DruidError::Ingestion(format!(
            "avro OCF payload declares a collection of {count} elements, exceeding the cap of \
             {MAX_AVRO_COLLECTION_ITEMS} (potential resource exhaustion)"
        )));
    }
    Ok(count)
}

/// Read an Avro OCF from disk into ingestion rows.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] if the file cannot be read or parsed.
pub fn parse_avro_ocf_file(path: &std::path::Path) -> Result<Vec<serde_json::Value>> {
    let data = std::fs::read(path).map_err(|e| {
        DruidError::Ingestion(format!("failed to read avro file {}: {e}", path.display()))
    })?;
    parse_avro_ocf_bytes(&data)
}

/// Convert a top-level Avro value (expected to be a `Record`) into a JSON
/// object. Non-record top-level values are wrapped under a `"value"` key so the
/// pipeline still receives a JSON object.
///
/// `depth` is the current nesting level; it is checked against
/// [`MAX_AVRO_DEPTH`] so recursion cannot overflow the stack independent of the
/// upstream decoder.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] when nesting exceeds [`MAX_AVRO_DEPTH`].
fn record_to_json(value: AvroValue, depth: usize) -> Result<serde_json::Value> {
    if depth > MAX_AVRO_DEPTH {
        return Err(DruidError::Ingestion(format!(
            "avro value nests deeper than the limit of {MAX_AVRO_DEPTH}"
        )));
    }
    match value {
        AvroValue::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (name, field) in fields {
                obj.insert(name, avro_to_json(field, depth + 1)?);
            }
            Ok(serde_json::Value::Object(obj))
        }
        AvroValue::Union(_, inner) => record_to_json(*inner, depth + 1),
        other => {
            let mut obj = serde_json::Map::new();
            obj.insert("value".to_string(), avro_to_json(other, depth + 1)?);
            Ok(serde_json::Value::Object(obj))
        }
    }
}

/// Convert an arbitrary Avro value to a JSON value.
///
/// `depth` is the current nesting level; recursion into `Array` / `Map` /
/// `Record` / `Union` increments it and a value nesting deeper than
/// [`MAX_AVRO_DEPTH`] is rejected, keeping stack usage bounded independent of
/// the upstream decoder.
///
/// # Errors
///
/// Returns [`DruidError::Ingestion`] when nesting exceeds [`MAX_AVRO_DEPTH`].
fn avro_to_json(value: AvroValue, depth: usize) -> Result<serde_json::Value> {
    if depth > MAX_AVRO_DEPTH {
        return Err(DruidError::Ingestion(format!(
            "avro value nests deeper than the limit of {MAX_AVRO_DEPTH}"
        )));
    }
    let v = match value {
        AvroValue::Null => serde_json::Value::Null,
        AvroValue::Boolean(b) => serde_json::Value::Bool(b),
        AvroValue::Int(i) => json_i64(i64::from(i)),
        AvroValue::Long(l) => json_i64(l),
        AvroValue::Float(f) => json_f64(f64::from(f)),
        AvroValue::Double(d) => json_f64(d),
        AvroValue::String(s) | AvroValue::Enum(_, s) => serde_json::Value::String(s),
        AvroValue::Bytes(b) | AvroValue::Fixed(_, b) => {
            serde_json::Value::Array(b.into_iter().map(|x| json_i64(i64::from(x))).collect())
        }
        // Unwrap unions (the `["null", T]` nullable pattern collapses to T).
        AvroValue::Union(_, inner) => avro_to_json(*inner, depth + 1)?,
        AvroValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(avro_to_json(item, depth + 1)?);
            }
            serde_json::Value::Array(out)
        }
        AvroValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k, avro_to_json(v, depth + 1)?);
            }
            serde_json::Value::Object(obj)
        }
        AvroValue::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (name, field) in fields {
                obj.insert(name, avro_to_json(field, depth + 1)?);
            }
            serde_json::Value::Object(obj)
        }
        // Date / time / timestamp logical types: surface their integer payload.
        AvroValue::Date(d) | AvroValue::TimeMillis(d) => json_i64(i64::from(d)),
        AvroValue::TimeMicros(l)
        | AvroValue::TimestampMillis(l)
        | AvroValue::TimestampMicros(l)
        | AvroValue::TimestampNanos(l)
        | AvroValue::LocalTimestampMillis(l)
        | AvroValue::LocalTimestampMicros(l)
        | AvroValue::LocalTimestampNanos(l) => json_i64(l),
        AvroValue::Uuid(u) => serde_json::Value::String(u.to_string()),
        // Decimal / BigDecimal / Duration have no lossless scalar JSON form;
        // render as their debug string rather than dropping the column.
        AvroValue::Decimal(d) => serde_json::Value::String(format!("{d:?}")),
        AvroValue::BigDecimal(d) => serde_json::Value::String(d.to_string()),
        AvroValue::Duration(d) => serde_json::Value::String(format!("{d:?}")),
    };
    Ok(v)
}

fn json_i64(v: i64) -> serde_json::Value {
    serde_json::Value::Number(serde_json::Number::from(v))
}

fn json_f64(v: f64) -> serde_json::Value {
    match serde_json::Number::from_f64(v) {
        Some(n) => serde_json::Value::Number(n),
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Schema, Writer};

    const SAMPLE_SCHEMA: &str = r#"
    {
      "type": "record",
      "name": "Event",
      "fields": [
        {"name": "ts", "type": "long"},
        {"name": "region", "type": ["null", "string"], "default": null},
        {"name": "count", "type": "int"},
        {"name": "revenue", "type": "double"},
        {"name": "active", "type": "boolean"}
      ]
    }
    "#;

    /// Write a small OCF buffer with a nullable union field and mixed types.
    fn write_sample_ocf() -> Vec<u8> {
        let schema = Schema::parse_str(SAMPLE_SCHEMA).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());

        let mut rec1 = apache_avro::types::Record::new(writer.schema()).expect("rec");
        rec1.put("ts", 1000_i64);
        rec1.put("region", Some("us"));
        rec1.put("count", 10_i32);
        rec1.put("revenue", 1.5_f64);
        rec1.put("active", true);
        writer.append(rec1).expect("append1");

        let mut rec2 = apache_avro::types::Record::new(writer.schema()).expect("rec");
        rec2.put("ts", 2000_i64);
        // Null region (the `null` branch of the union).
        rec2.put("region", None::<String>);
        rec2.put("count", 20_i32);
        rec2.put("revenue", 2.5_f64);
        rec2.put("active", false);
        writer.append(rec2).expect("append2");

        writer.into_inner().expect("into_inner")
    }

    #[test]
    fn parse_avro_ocf_roundtrip_types() {
        let buf = write_sample_ocf();
        let rows = parse_avro_ocf_bytes(&buf).expect("parse");
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0]["ts"], serde_json::json!(1000));
        // Union-with-null unwrapped to the inner string.
        assert_eq!(rows[0]["region"], serde_json::json!("us"));
        assert_eq!(rows[0]["count"], serde_json::json!(10));
        assert_eq!(rows[0]["revenue"], serde_json::json!(1.5));
        assert_eq!(rows[0]["active"], serde_json::json!(true));

        // Null branch of the union becomes JSON null.
        assert_eq!(rows[1]["region"], serde_json::Value::Null);
        assert_eq!(rows[1]["active"], serde_json::json!(false));
    }

    #[test]
    fn parse_avro_ocf_feeds_ingester() {
        let buf = write_sample_ocf();
        let rows = parse_avro_ocf_bytes(&buf).expect("parse");

        let ingester = crate::BatchIngester::new(
            "ds".into(),
            "ts".into(),
            vec!["region".into()],
            vec![serde_json::json!({"type": "doubleSum", "name": "revenue"})],
        );
        let result = ingester.ingest(rows).expect("ingest avro rows");
        assert_eq!(result.num_rows, 2);
        assert_eq!(result.segment_data.dimensions, vec!["region"]);
    }

    #[test]
    fn parse_avro_ocf_array_field() {
        let schema_str = r#"
        {
          "type": "record",
          "name": "WithArray",
          "fields": [
            {"name": "ts", "type": "long"},
            {"name": "tags", "type": {"type": "array", "items": "string"}}
          ]
        }
        "#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        rec.put("ts", 1000_i64);
        rec.put(
            "tags",
            AvroValue::Array(vec![
                AvroValue::String("a".into()),
                AvroValue::String("b".into()),
            ]),
        );
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        let rows = parse_avro_ocf_bytes(&buf).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["tags"], serde_json::json!(["a", "b"]));
    }

    #[test]
    fn parse_avro_invalid_buffer() {
        let err = parse_avro_ocf_bytes(&[0u8, 1, 2, 3]).unwrap_err();
        assert!(err.to_string().contains("avro"));
    }

    #[test]
    fn parse_avro_row_cap_rejected() {
        // The sample OCF has 2 records; a cap of 1 must reject rather than
        // accumulate unbounded rows.
        let buf = write_sample_ocf();
        let err = parse_avro_ocf_bytes_capped(&buf, 1).unwrap_err();
        match err {
            DruidError::Ingestion(msg) => {
                assert!(
                    msg.contains("row cap"),
                    "expected row-cap error, got: {msg}"
                );
            }
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn parse_avro_row_cap_within_limit_ok() {
        let buf = write_sample_ocf();
        let rows = parse_avro_ocf_bytes_capped(&buf, 2).expect("parse within cap");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn avro_to_json_depth_limit_rejected() {
        // Build an AvroValue nested deeper than MAX_AVRO_DEPTH using arrays.
        // Each Array adds one level of recursion. The conversion must return an
        // Err (not overflow the stack).
        let mut v = AvroValue::Long(1);
        for _ in 0..(MAX_AVRO_DEPTH + 5) {
            v = AvroValue::Array(vec![v]);
        }
        let err = avro_to_json(v, 0).unwrap_err();
        match err {
            DruidError::Ingestion(msg) => {
                assert!(
                    msg.contains("nests deeper"),
                    "expected depth error, got: {msg}"
                );
            }
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_to_json_within_depth_ok() {
        // A modestly nested value within the limit converts successfully.
        let mut v = AvroValue::Long(7);
        for _ in 0..8 {
            v = AvroValue::Array(vec![v]);
        }
        let json = avro_to_json(v, 0).expect("within depth");
        // Drill back down to confirm the leaf survived.
        let mut cur = &json;
        for _ in 0..8 {
            cur = cur
                .as_array()
                .and_then(|a| a.first())
                .expect("nested array");
        }
        assert_eq!(*cur, serde_json::json!(7));
    }

    #[test]
    fn parse_avro_ocf_header_length_oom_rejected() {
        // Regression for fuzz crash
        // `oom-avro-ocf-header-length-unbounded-malloc`: a 16-byte OCF whose
        // header map declares an enormous entry count drove a single ~10 GiB
        // allocation inside `apache_avro::Reader::new`, BEFORE the per-record
        // `MAX_AVRO_ROWS` cap could apply. The pre-validator must reject it
        // cheaply (no huge allocation) with an Ingestion error.
        let crash: &[u8] = &[
            0x4f, 0x62, 0x6a, 0x01, 0xd1, 0xd1, 0xd1, 0x62, 0xd1, 0x62, 0xd1, 0xd1, 0x6a, 0x01,
            0x2d, 0xf0,
        ];
        let err = parse_avro_ocf_bytes(crash).expect_err("must reject, not OOM");
        match err {
            DruidError::Ingestion(_) => {}
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn prevalidate_rejects_declared_length_exceeding_input() {
        // Construct an equivalent attack deterministically: a valid magic +
        // a map block whose first entry declares a key length far larger than
        // the input, which must be rejected before any allocation.
        let mut buf: Vec<u8> = Vec::new();
        buf.extend_from_slice(&[b'O', b'b', b'j', 1u8]);
        // Map block: count = 1 (zig-zag long 1 -> 0x02).
        buf.push(0x02);
        // Key length = a huge zig-zag long (encode 2^40). zig-zag(n) = n<<1.
        let huge: u64 = (1u64 << 40) << 1;
        let mut z = huge;
        loop {
            if z <= 0x7F {
                buf.push((z & 0x7F) as u8);
                break;
            }
            buf.push((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
        let err = parse_avro_ocf_bytes(&buf).expect_err("must reject oversized length");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("remain") || msg.contains("exceed"),
                "expected length-exceeds-input error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn prevalidate_accepts_real_ocf() {
        // The pre-validator must not reject a legitimately-written OCF.
        let buf = write_sample_ocf();
        prevalidate_ocf_lengths(&buf).expect("real OCF passes pre-validation");
    }

    /// Zig-zag encode `n` as an Avro varint into `buf`.
    fn put_varint(buf: &mut Vec<u8>, n: i64) {
        let mut z = ((n << 1) ^ (n >> 63)) as u64;
        loop {
            if z <= 0x7F {
                buf.push((z & 0x7F) as u8);
                break;
            }
            buf.push((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
    }

    /// Build an OCF for `schema_str` consisting of a real writer header/sync
    /// (with zero records) followed by one hand-crafted data block whose payload
    /// is `payload` and whose declared `object_count` is `object_count`. This
    /// lets a test inject a malicious payload (e.g. a huge collection block
    /// count) past the container framing while keeping a valid header the
    /// upstream `Reader` would accept.
    fn ocf_with_block(schema_str: &str, object_count: i64, payload: &[u8]) -> Vec<u8> {
        let schema = Schema::parse_str(schema_str).expect("schema");
        let writer = Writer::new(&schema, Vec::new());
        let header = writer.into_inner().expect("header");
        // The writer emits magic + header map + 16-byte sync (and, with zero
        // appends, no data blocks). Reuse its sync marker for our block.
        let sync = header[header.len() - 16..].to_vec();

        let mut block = Vec::new();
        put_varint(&mut block, object_count);
        put_varint(&mut block, payload.len() as i64);
        block.extend_from_slice(payload);
        block.extend_from_slice(&sync);

        let mut ocf = header;
        ocf.extend_from_slice(&block);
        ocf
    }

    const ARRAY_INT_SCHEMA: &str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"int"}}]}"#;

    #[test]
    fn avro_huge_array_count_rejected() {
        // Regression for DD R12: the container-only pre-validator skipped each
        // data-block payload, so a schema-dependent array block count *inside*
        // the payload was never validated. apache_avro decodes that count and
        // calls `items.reserve(count)` BEFORE reading any element. A 165-byte
        // OCF declaring a 200 M-element `int` array therefore drove an ~11 GiB
        // allocation that ABORTS the process (verified standalone:
        // `memory allocation of 11200000000 bytes failed`, SIGABRT) before any
        // per-record cap could apply. The schema-aware payload walk must reject
        // it cheaply with no large allocation.
        let mut payload = Vec::new();
        put_varint(&mut payload, 200_000_000); // array block count, no elements
        let ocf = ocf_with_block(ARRAY_INT_SCHEMA, 1, &payload);
        assert!(ocf.len() < 256, "trigger OCF is tiny: {} bytes", ocf.len());

        let err = parse_avro_ocf_bytes(&ocf).expect_err("must reject, not OOM");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("collection") || msg.contains("remain"),
                "expected collection-count error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_huge_array_negative_count_rejected() {
        // The negative-count (block-byte-size) array encoding form must also be
        // bounded: -200_000_000 means abs() entries follow, and the decoder
        // still reserves that many.
        let mut payload = Vec::new();
        put_varint(&mut payload, -200_000_000); // negative block count
        put_varint(&mut payload, 4); // claimed block byte size
        let ocf = ocf_with_block(ARRAY_INT_SCHEMA, 1, &payload);

        let err = parse_avro_ocf_bytes(&ocf).expect_err("must reject negative huge count");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("collection") || msg.contains("remain"),
                "expected collection-count error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_huge_map_count_rejected() {
        // Same bypass via a map field: HashMap::reserve(count) of a multi-byte
        // entry from a tiny payload.
        let map_schema = r#"{"type":"record","name":"R","fields":[{"name":"m","type":{"type":"map","values":"int"}}]}"#;
        let mut payload = Vec::new();
        put_varint(&mut payload, 200_000_000); // map block count, no entries
        let ocf = ocf_with_block(map_schema, 1, &payload);

        let err = parse_avro_ocf_bytes(&ocf).expect_err("must reject huge map count");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("collection") || msg.contains("remain"),
                "expected collection-count error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_huge_string_length_rejected() {
        // A string field whose length prefix inside the payload claims far more
        // bytes than the payload holds. apache_avro does `vec![0u8; len]` for
        // strings; our walk must reject the over-large declared length.
        let str_schema = r#"{"type":"record","name":"R","fields":[{"name":"s","type":"string"}]}"#;
        let mut payload = Vec::new();
        put_varint(&mut payload, 2_000_000_000); // string length, no bytes follow
        let ocf = ocf_with_block(str_schema, 1, &payload);

        let err = parse_avro_ocf_bytes(&ocf).expect_err("must reject huge string length");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("remain") || msg.contains("length"),
                "expected length-exceeds-input error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_real_large_array_within_input_ok() {
        // A legitimately "large" array whose element count actually matches the
        // elements present must still parse: this confirms the fix does not
        // false-reject real OCFs (the count is bounded by the bytes that back
        // it, and these elements are all present).
        let schema = Schema::parse_str(ARRAY_INT_SCHEMA).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        let elements: Vec<AvroValue> = (0..1000).map(AvroValue::Int).collect();
        rec.put("a", AvroValue::Array(elements));
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        let rows = parse_avro_ocf_bytes(&buf).expect("real large array parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["a"].as_array().expect("array").len(),
            1000,
            "all 1000 real elements survive"
        );
    }

    #[test]
    fn prevalidate_accepts_map_and_nested_records() {
        // Exercise the schema walk on a richer real schema (nested record, map,
        // union, enum, fixed) round-tripped through the writer so we confirm the
        // walk consumes a real payload without false rejection.
        let schema_str = r#"
        {
          "type": "record",
          "name": "Outer",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "labels", "type": {"type": "map", "values": "string"}},
            {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}},
            {"name": "tag", "type": {"type": "fixed", "name": "Tag", "size": 4}},
            {"name": "maybe", "type": ["null", "string"]},
            {"name": "inner", "type": {"type": "record", "name": "Inner",
               "fields": [{"name": "x", "type": "int"}]}}
          ]
        }
        "#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        rec.put("id", 7_i64);
        let mut m = std::collections::HashMap::new();
        m.insert("k".to_string(), AvroValue::String("v".to_string()));
        rec.put("labels", AvroValue::Map(m));
        rec.put("kind", AvroValue::Enum(1, "B".to_string()));
        rec.put("tag", AvroValue::Fixed(4, vec![1, 2, 3, 4]));
        rec.put("maybe", Some("hi"));
        rec.put(
            "inner",
            AvroValue::Record(vec![("x".to_string(), AvroValue::Int(9))]),
        );
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        // Pre-validation passes and the row decodes.
        prevalidate_ocf_lengths(&buf).expect("rich real OCF passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("rich OCF parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], serde_json::json!(7));
    }

    #[test]
    fn avro_zerowidth_object_count_amplification_rejected() {
        // DD R13 finding #1: a zero-width record schema (here an empty record)
        // lets a 0-byte block payload declare an enormous `object_count`; the
        // upstream decoder would then yield that many zero-byte records, each
        // materialized into a `serde_json::Value`, turning a handful of header
        // bytes into a multi-GiB `Vec<Value>`. The block validator must reject
        // it cheaply (no huge Vec) because the datums consume no payload bytes.
        let empty_record = r#"{"type":"record","name":"R","fields":[]}"#;
        // 16 Mi objects, zero-byte payload.
        let ocf = ocf_with_block(empty_record, 16 * 1024 * 1024, &[]);
        assert!(ocf.len() < 256, "trigger OCF is tiny: {} bytes", ocf.len());

        let err = parse_avro_ocf_bytes(&ocf).expect_err("must reject zero-width amplification");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("zero-width"),
                "expected zero-width-object error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }

        // The same attack via a root `null` schema (every datum is zero bytes).
        let ocf_null = ocf_with_block(r#""null""#, 16 * 1024 * 1024, &[]);
        let err = parse_avro_ocf_bytes(&ocf_null).expect_err("must reject root-null amplification");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("zero-width"),
                "expected zero-width-object error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_array_of_null_items_accepted() {
        // DD R13 finding #2 regression: an array whose item schema is `null`
        // (zero-width) with 10 nulls encodes as `count=10` then `count=0`
        // (terminator) — only ONE byte after the count. The prior
        // `count > remaining` check false-rejected this VALID OCF. Build it via
        // the real apache_avro writer so it is genuinely valid, and confirm it
        // now parses rather than being rejected.
        let schema_str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"null"}}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        let nulls: Vec<AvroValue> = (0..10).map(|_| AvroValue::Null).collect();
        rec.put("a", AvroValue::Array(nulls));
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        // Must NOT be rejected (this pins the false-reject fix).
        prevalidate_ocf_lengths(&buf).expect("array<null> passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("array<null> parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]["a"].as_array().expect("array").len(),
            10,
            "all 10 null elements survive"
        );
    }

    #[test]
    fn avro_array_of_empty_record_items_accepted() {
        // Companion to the array<null> case: an array of empty records is also
        // zero-width per element and must parse rather than false-reject.
        let schema_str = r#"{"type":"record","name":"Outer","fields":[{"name":"a","type":{"type":"array","items":{"type":"record","name":"Empty","fields":[]}}}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        let empties: Vec<AvroValue> = (0..5).map(|_| AvroValue::Record(vec![])).collect();
        rec.put("a", AvroValue::Array(empties));
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        let rows = parse_avro_ocf_bytes(&buf).expect("array<empty record> parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["a"].as_array().expect("array").len(), 5);
    }

    #[test]
    fn avro_schema_with_millions_of_fields_rejected() {
        // DD R13 finding #3: a shallow record with a huge `fields` array (or a
        // huge union) passes the depth / named-type checks but drives
        // proportional `serde_json::from_str` + `Vec::with_capacity` allocation.
        // The total-node / field-count budget must reject it before the large
        // `Vec` is reserved.
        // Just over MAX_AVRO_SCHEMA_TOTAL_NODES (1 Mi), using compact nameless
        // fields so the JSON stays under MAX_AVRO_SCHEMA_JSON_BYTES (16 MiB) and
        // the FIELD-COUNT budget — not the byte cap — is what rejects it.
        let n = MAX_AVRO_SCHEMA_TOTAL_NODES + 16;
        let mut json = String::with_capacity(n * 16);
        json.push_str(r#"{"type":"record","name":"R","fields":["#);
        for i in 0..n {
            if i > 0 {
                json.push(',');
            }
            // `{"type":"int"}` — 14 bytes; well under the JSON byte cap in total.
            json.push_str(r#"{"type":"int"}"#);
        }
        json.push_str("]}");
        assert!(
            json.len() < MAX_AVRO_SCHEMA_JSON_BYTES,
            "field-count test must stay under the byte cap to exercise the field budget: {} bytes",
            json.len()
        );

        match parse_writer_schema(&json) {
            Err(DruidError::Ingestion(msg)) => assert!(
                msg.contains("fields") || msg.contains("nodes"),
                "expected field/node-budget error, got: {msg}"
            ),
            Err(other) => panic!("expected DruidError::Ingestion, got {other:?}"),
            Ok(_) => panic!("must reject huge field count"),
        }

        // Same via a giant union branch list, also kept under the JSON byte cap.
        let mut union = String::with_capacity(n * 8);
        union.push('[');
        for i in 0..n {
            if i > 0 {
                union.push(',');
            }
            union.push_str(r#""null""#);
        }
        union.push(']');
        assert!(
            union.len() < MAX_AVRO_SCHEMA_JSON_BYTES,
            "union-branch test must stay under the byte cap: {} bytes",
            union.len()
        );
        match parse_writer_schema(&union) {
            Err(DruidError::Ingestion(msg)) => assert!(
                msg.contains("branches") || msg.contains("nodes"),
                "expected branch/node-budget error, got: {msg}"
            ),
            Err(other) => panic!("expected DruidError::Ingestion, got {other:?}"),
            Ok(_) => panic!("must reject huge union"),
        }
    }

    #[test]
    fn avro_schema_json_byte_cap_rejected() {
        // The raw `avro.schema` byte length is bounded before `from_str` so an
        // enormous schema string cannot drive a large `serde_json::Value` up
        // front. Build a string just over the byte cap.
        let big = "x".repeat(MAX_AVRO_SCHEMA_JSON_BYTES + 1);
        let json = format!("\"{big}\"");
        match parse_writer_schema(&json) {
            Err(DruidError::Ingestion(msg)) => assert!(
                msg.contains("exceeding the cap"),
                "expected schema-json-byte-cap error, got: {msg}"
            ),
            Err(other) => panic!("expected DruidError::Ingestion, got {other:?}"),
            Ok(_) => panic!("must reject oversized schema json"),
        }
    }

    #[test]
    fn parse_avro_ocf_file_roundtrip() {
        let buf = write_sample_ocf();
        let dir = tempfile::tempdir().expect("tmpdir");
        let path = dir.path().join("data.avro");
        std::fs::write(&path, &buf).expect("write");
        let rows = parse_avro_ocf_file(&path).expect("parse file");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn avro_zerowidth_array_total_budget_rejected() {
        // DD R14 (High): a single `array<null>` declaring `MAX_AVRO_COLLECTION_ITEMS`
        // (16 Mi) elements from a tiny payload passes the per-collection cap and
        // the per-element byte-backing check (skipped for zero-width elements),
        // yet would materialize ~1 GiB of `AvroValue` + `serde_json::Value` in one
        // row. The per-block total-element budget must reject it cheaply.
        let schema_str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":"null"}}]}"#;
        let mut payload = Vec::new();
        // One array block declaring 16 Mi zero-width elements, then a terminator.
        put_varint(&mut payload, MAX_AVRO_COLLECTION_ITEMS as i64);
        put_varint(&mut payload, 0);
        let ocf = ocf_with_block(schema_str, 1, &payload);
        assert!(ocf.len() < 256, "trigger OCF is tiny: {} bytes", ocf.len());

        let err =
            parse_avro_ocf_bytes(&ocf).expect_err("must reject zero-width array total budget");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("total"),
                "expected total-element-budget error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_many_small_zerowidth_arrays_total_budget_rejected() {
        // DD R14 (class-closer): MANY arrays each well under the per-collection
        // cap, but whose element counts SUM past the per-block budget, must be
        // rejected. This proves the budget closes the DISTRIBUTED case, not just a
        // single huge collection. The schema is a record with an array-of-array
        // field; the OUTER array has many blocks (each a separate `walk_collection`
        // count) and the INNER arrays each carry a chunk of zero-width nulls.
        let schema_str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":{"type":"array","items":"null"}}}]}"#;

        // Build a payload: outer array as a series of single-element blocks, each
        // inner array declaring `chunk` zero-width nulls. Total inner elements
        // exceed MAX_AVRO_BLOCK_TOTAL_ELEMENTS while each inner count is tiny
        // relative to MAX_AVRO_COLLECTION_ITEMS.
        let chunk: usize = 64 * 1024; // each inner array: well under per-collection cap
        let inner_arrays = (MAX_AVRO_BLOCK_TOTAL_ELEMENTS / chunk) + 8;
        let mut payload = Vec::new();
        for _ in 0..inner_arrays {
            // Outer array block: one element (one inner array) follows.
            put_varint(&mut payload, 1);
            // Inner array block: `chunk` zero-width nulls, then terminator.
            put_varint(&mut payload, chunk as i64);
            put_varint(&mut payload, 0);
        }
        // Outer array terminator.
        put_varint(&mut payload, 0);
        let ocf = ocf_with_block(schema_str, 1, &payload);

        let err = parse_avro_ocf_bytes(&ocf)
            .expect_err("must reject distributed zero-width arrays summing past budget");
        match err {
            DruidError::Ingestion(msg) => assert!(
                msg.contains("total"),
                "expected total-element-budget error, got: {msg}"
            ),
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }
    }

    #[test]
    fn avro_nullable_union_large_block_accepted() {
        // DD R14 (Medium) regression: a SINGLE data block of > 65 536 all-null
        // `["null","int"]` records. Each record encodes a union branch-index
        // varint (1 byte for the null branch, value 0 -> 0x00), so the record is
        // POSITIVE-width (min width 1), NOT zero-width. Before the union-index-byte
        // fix `min_encoded_width` computed 0 for `["null","int"]`, classifying the
        // whole block as zero-width and capping its `object_count` at
        // MAX_AVRO_ZEROWIDTH_OBJECTS (64 Ki) — a FALSE-REJECT of this valid block.
        //
        // We hand-craft ONE block (the real writer would split into < 64 Ki-object
        // blocks and mask the bug) whose payload is exactly `n` 0x00 bytes (each
        // the null-branch index for one record). With the fix the root record is
        // positive-width (min 1), so `payload.len() / 1 == n` objects are backed
        // and the block parses; without it the block is rejected as zero-width.
        let schema_str =
            r#"{"type":"record","name":"R","fields":[{"name":"u","type":["null","int"]}]}"#;
        let n = MAX_AVRO_ZEROWIDTH_OBJECTS + 1; // 65 537: > the zero-width cap.
        // Payload: `n` union-index varints, each 0x00 (selects branch 0 = null).
        let payload = vec![0u8; n];
        let ocf = ocf_with_block(schema_str, n as i64, &payload);

        // Must NOT false-reject; the pre-validator and full parse both accept it.
        prevalidate_ocf_lengths(&ocf).expect("nullable-union block passes pre-validation");
        let rows = parse_avro_ocf_bytes(&ocf).expect("nullable-union block parses");
        assert_eq!(rows.len(), n, "all nullable-union records survive");
        assert_eq!(rows[0]["u"], serde_json::Value::Null);
    }

    #[test]
    fn avro_array_of_nullable_union_accepted() {
        // DD R14 (Medium): `array<["null","int"]>` with real elements parses. Each
        // element is now correctly positive-width (the union index byte), so the
        // array is not mis-classified as zero-width and the real elements survive.
        let schema_str = r#"{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"array","items":["null","int"]}}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        let items = vec![
            AvroValue::Union(1, Box::new(AvroValue::Int(7))),
            AvroValue::Union(0, Box::new(AvroValue::Null)),
            AvroValue::Union(1, Box::new(AvroValue::Int(9))),
        ];
        rec.put("a", AvroValue::Array(items));
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        prevalidate_ocf_lengths(&buf).expect("array<nullable union> passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("array<nullable union> parses");
        assert_eq!(rows.len(), 1);
        let arr = rows[0]["a"].as_array().expect("array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], serde_json::json!(7));
        assert_eq!(arr[1], serde_json::Value::Null);
        assert_eq!(arr[2], serde_json::json!(9));
    }

    #[test]
    fn avro_deflate_codec_ocf_accepted() {
        // DD R15 (High): a valid `deflate`-coded OCF — the common production case
        // — must be ACCEPTED, not false-rejected. Before the codec-aware fix the
        // prevalidator schema-walked the *compressed* block bytes as if they were
        // raw datum bytes, rejecting this file with a bogus length error even
        // though the upstream `Reader` decodes it fine.
        use apache_avro::{Codec, DeflateSettings};
        let schema_str = r#"{"type":"record","name":"R","fields":[
            {"name":"id","type":"long"},{"name":"s","type":"string"}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::with_codec(
            &schema,
            Vec::new(),
            Codec::Deflate(DeflateSettings::default()),
        );
        for i in 0..5_i64 {
            let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
            rec.put("id", i);
            rec.put("s", format!("row-{i}"));
            writer.append(rec).expect("append");
        }
        let buf = writer.into_inner().expect("into_inner");

        // Sanity: the upstream decoder accepts it (the differential Codex used).
        let upstream = Reader::new(&buf[..]).expect("reader").count();
        assert_eq!(upstream, 5);

        prevalidate_ocf_lengths(&buf).expect("deflate OCF passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("deflate OCF parses");
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0]["id"], serde_json::json!(0));
        assert_eq!(rows[4]["s"], serde_json::json!("row-4"));
    }

    #[test]
    fn avro_unsupported_codec_rejected() {
        // DD R15: a codec the downstream decoder cannot handle (e.g. `snappy`,
        // which is not compiled into apache-avro's default feature set) must be
        // rejected with a clear error rather than schema-walked as raw bytes.
        assert!(matches!(
            AvroCodec::from_value_bytes(b"snappy"),
            Err(DruidError::Ingestion(_))
        ));
        assert_eq!(
            AvroCodec::from_value_bytes(b"null").unwrap(),
            AvroCodec::Null
        );
        assert_eq!(AvroCodec::from_value_bytes(b"").unwrap(), AvroCodec::Null);
        assert_eq!(
            AvroCodec::from_value_bytes(b"deflate").unwrap(),
            AvroCodec::Deflate
        );
    }

    #[test]
    fn avro_deflate_bomb_rejected() {
        // DD R15: inflating a `deflate` block is itself an amplification vector.
        // A run of highly-compressible bytes inflates far past a small cap and
        // must be rejected before the buffer is materialized. Exercised via the
        // `_capped` form so the bound is hit cheaply (a tiny cap), proving the
        // bounded read rejects rather than allocating the full output.
        use std::io::Write;
        let original = vec![0u8; 4096]; // compresses to a handful of bytes
        let mut enc = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::best());
        enc.write_all(&original).expect("compress");
        let compressed = enc.finish().expect("finish");
        assert!(compressed.len() < original.len());

        // Cap below the real inflated size -> rejected as a bomb.
        let err = decompress_deflate_block_capped(&compressed, 64)
            .expect_err("must reject inflation past the cap");
        match err {
            DruidError::Ingestion(msg) => {
                assert!(msg.contains("decompress"), "unexpected message: {msg}");
            }
            other => panic!("expected DruidError::Ingestion, got {other:?}"),
        }

        // Cap at/above the real size -> accepted and byte-exact.
        let out = decompress_deflate_block_capped(&compressed, original.len())
            .expect("within cap inflates");
        assert_eq!(out, original);
    }

    #[test]
    fn avro_object_wrapped_type_accepted() {
        // DD R16 (Medium): a field whose `"type"` is itself a complex object
        // (`{"type":{"type":"record",...}}`) is valid Avro and accepted by the
        // upstream Reader; the prevalidation schema-walk must not false-reject it.
        let schema_str = r#"{"type":"record","name":"Outer","fields":[
            {"name":"inner","type":{"type":"record","name":"Inner","fields":[
                {"name":"x","type":"long"}]}}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        // Construct the nested record value directly to avoid Record-builder
        // schema plumbing for the inner type.
        let inner = AvroValue::Record(vec![("x".to_string(), AvroValue::Long(42))]);
        let outer = AvroValue::Record(vec![("inner".to_string(), inner)]);
        writer.append(outer).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        assert_eq!(Reader::new(&buf[..]).expect("reader").count(), 1);
        prevalidate_ocf_lengths(&buf).expect("object-wrapped type passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("object-wrapped type parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["inner"]["x"], serde_json::json!(42));
    }

    #[test]
    fn avro_array_wrapped_union_type_accepted() {
        // DD R16 (Medium): a field whose `"type"` value is a union array
        // (`{"name":"u","type":["null","string"]}`) is the ubiquitous nullable
        // form — it must not be false-rejected by the schema-walk.
        let schema_str = r#"{"type":"record","name":"R","fields":[
            {"name":"u","type":["null","string"]}]}"#;
        let schema = Schema::parse_str(schema_str).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new());
        let mut rec = apache_avro::types::Record::new(writer.schema()).expect("rec");
        rec.put(
            "u",
            AvroValue::Union(1, Box::new(AvroValue::String("hi".into()))),
        );
        writer.append(rec).expect("append");
        let buf = writer.into_inner().expect("into_inner");

        assert_eq!(Reader::new(&buf[..]).expect("reader").count(), 1);
        prevalidate_ocf_lengths(&buf).expect("array-wrapped union passes pre-validation");
        let rows = parse_avro_ocf_bytes(&buf).expect("array-wrapped union parses");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["u"], serde_json::json!("hi"));
    }
}
