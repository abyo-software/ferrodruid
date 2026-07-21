// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid SQL built-in functions (time functions, string functions, math functions).
//!
//! These functions extend the standard SQL dialect with Druid-specific
//! capabilities such as `TIME_FLOOR`, `TIME_CEIL`, and `APPROX_COUNT_DISTINCT`.

use serde::{Deserialize, Serialize};

use crate::parser::SqlExpr;

// ---------------------------------------------------------------------------
// TimeUnit — sub-field extraction from a timestamp
// ---------------------------------------------------------------------------

/// Time unit for `TIME_EXTRACT` / `EXTRACT`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeUnit {
    /// Seconds since epoch.
    Epoch,
    /// Second of the minute (0-59).
    Second,
    /// Minute of the hour (0-59).
    Minute,
    /// Hour of the day (0-23).
    Hour,
    /// Day of the month (1-31).
    Day,
    /// ISO day of the week (1 = Monday).
    Dow,
    /// Day of the year (1-366).
    Doy,
    /// ISO week number.
    Week,
    /// Month of the year (1-12).
    Month,
    /// Quarter of the year (1-4).
    Quarter,
    /// Year.
    Year,
}

impl TimeUnit {
    /// Parse a time unit from a string (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "EPOCH" => Some(Self::Epoch),
            "SECOND" => Some(Self::Second),
            "MINUTE" => Some(Self::Minute),
            "HOUR" => Some(Self::Hour),
            "DAY" => Some(Self::Day),
            "DOW" => Some(Self::Dow),
            "DOY" => Some(Self::Doy),
            "WEEK" => Some(Self::Week),
            "MONTH" => Some(Self::Month),
            "QUARTER" => Some(Self::Quarter),
            "YEAR" => Some(Self::Year),
            _ => None,
        }
    }

    /// Convert this time unit to the ISO-8601 period string that Druid uses.
    pub fn to_iso_period(&self) -> Option<&'static str> {
        match self {
            Self::Second => Some("PT1S"),
            Self::Minute => Some("PT1M"),
            Self::Hour => Some("PT1H"),
            Self::Day => Some("P1D"),
            Self::Week => Some("P1W"),
            Self::Month => Some("P1M"),
            Self::Quarter => Some("P3M"),
            Self::Year => Some("P1Y"),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// DruidFunction — recognized Druid SQL functions
// ---------------------------------------------------------------------------

/// A recognised Druid SQL function call that has been resolved from the AST.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DruidFunction {
    // ----- Time functions -----
    /// `TIME_FLOOR(expr, period [, origin, timezone])` — floor a timestamp to a period boundary.
    TimeFloor {
        /// The timestamp expression.
        expr: Box<SqlExpr>,
        /// ISO-8601 period string (e.g. `"PT1H"`, `"P1D"`).
        period: String,
        /// Optional timezone name.
        timezone: Option<String>,
    },
    /// `TIME_CEIL(expr, period [, origin, timezone])` — ceil a timestamp to a period boundary.
    TimeCeil {
        /// The timestamp expression.
        expr: Box<SqlExpr>,
        /// ISO-8601 period string.
        period: String,
        /// Optional timezone name.
        timezone: Option<String>,
    },
    /// `TIME_SHIFT(expr, period, step)` — shift a timestamp by N periods.
    TimeShift {
        /// The timestamp expression.
        expr: Box<SqlExpr>,
        /// ISO-8601 period string.
        period: String,
        /// Number of periods to shift (can be negative).
        step: i64,
    },
    /// `TIME_FORMAT(expr, format)` — format a timestamp as a string.
    TimeFormat {
        /// The timestamp expression.
        expr: Box<SqlExpr>,
        /// A Joda-Time or ISO format string.
        format: String,
    },
    /// `TIME_PARSE(expr [, format])` — parse a string to a timestamp.
    TimeParse {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// Optional format string.
        format: Option<String>,
    },
    /// `TIME_EXTRACT(expr, unit)` — extract a field from a timestamp.
    TimeExtract {
        /// The timestamp expression.
        expr: Box<SqlExpr>,
        /// The field to extract.
        unit: TimeUnit,
    },
    /// `TIMESTAMPDIFF(unit, ts1, ts2)` — difference between two timestamps in the given unit.
    TimestampDiff {
        /// The time unit for the result.
        unit: TimeUnit,
        /// The start timestamp.
        start: Box<SqlExpr>,
        /// The end timestamp.
        end: Box<SqlExpr>,
    },
    /// `CURRENT_TIMESTAMP` — the current UTC timestamp.
    CurrentTimestamp,

    // ----- Aggregate functions (Druid-specific) -----
    /// `APPROX_COUNT_DISTINCT(expr)` — approximate distinct count via HyperLogLog.
    ApproxCountDistinct {
        /// The column expression.
        expr: Box<SqlExpr>,
    },
    /// `APPROX_QUANTILE_DS(expr, probability)` — approximate quantile via sketch.
    ApproxQuantileDs {
        /// The column expression.
        expr: Box<SqlExpr>,
        /// The probability (0.0 to 1.0).
        probability: f64,
    },
    /// `EARLIEST(expr)` — the earliest (first by time) value.
    Earliest(Box<SqlExpr>),
    /// `LATEST(expr)` — the latest (last by time) value.
    Latest(Box<SqlExpr>),
    /// `EARLIEST_BY(expr, timeCol)` — the earliest value of `expr` ordered by
    /// `timeCol` rather than `__time`.  Druid's `EARLIEST(expr, timeCol)`
    /// two-argument form (CL-4 / W1-D); kept as a distinct variant so the
    /// planner can validate the timestamp dimension separately from the
    /// single-arg `__time` flavour.
    EarliestBy {
        /// The value column.
        expr: Box<SqlExpr>,
        /// The timestamp column to order on (non-`__time`).
        time_col: String,
    },
    /// `LATEST_BY(expr, timeCol)` — the latest value of `expr` ordered by
    /// `timeCol` rather than `__time`.  Druid's `LATEST(expr, timeCol)`
    /// two-argument form (CL-4 / W1-D).
    LatestBy {
        /// The value column.
        expr: Box<SqlExpr>,
        /// The timestamp column to order on (non-`__time`).
        time_col: String,
    },
    /// `ANY_VALUE(expr)` — any arbitrary value (typically first).
    AnyValue(Box<SqlExpr>),

    // ----- Aggregate additions (Calcite / Druid SQL CL-4) -----
    /// `ARRAY_AGG([DISTINCT] expr [, size_limit])` — collect values into an
    /// ARRAY.  Druid materialises this as an `expressionLambda` over the
    /// segment scan; FerroDruid's native query engine does not yet have a
    /// matching primitive so the planner fails closed with a precise
    /// message.
    ArrayAgg {
        /// The value expression.
        expr: Box<SqlExpr>,
        /// Whether `DISTINCT` was requested.
        distinct: bool,
        /// Optional accumulator size cap.  `None` means use Druid's default.
        size_limit: Option<usize>,
    },
    /// `LISTAGG(expr [, separator [, size_limit]])` — string-join values.
    ///
    /// Druid 33+ aliases this to `STRING_AGG`.
    Listagg {
        /// The value expression.
        expr: Box<SqlExpr>,
        /// Separator string; defaults to `","` per ANSI.
        separator: String,
        /// Optional accumulator size cap.
        size_limit: Option<usize>,
    },
    /// `STRING_AGG(expr, separator [, size_limit])` — string-join values.
    StringAgg {
        /// The value expression.
        expr: Box<SqlExpr>,
        /// Separator string.
        separator: String,
        /// Optional accumulator size cap.
        size_limit: Option<usize>,
    },
    /// `BLOOM_FILTER(expr, num_entries)` — build a bloom filter over `expr`
    /// values.  Native query primitive in Druid; FerroDruid's native engine
    /// does not yet have a bloom-filter aggregator so the planner fails
    /// closed.  Parsed for surface coverage (CL-4 / W1-D).
    BloomFilter {
        /// The value expression to build the filter from.
        expr: Box<SqlExpr>,
        /// Estimated number of distinct entries (sizes the bloom filter).
        num_entries: i64,
    },
    /// `GROUPING(col1 [, col2, ...])` — bitmask indicator function for
    /// `GROUPING SETS` / `CUBE` / `ROLLUP` queries.  Druid emits a long
    /// where bit `i` is set when column `i` is absent from the current
    /// grouping set.
    Grouping(Vec<SqlExpr>),

    // ----- Druid filter / multi-value functions -----
    /// `BLOOM_FILTER_TEST(expr, base64_filter)` — SQL filter form that
    /// tests `expr` against a serialised bloom filter literal.  Used in
    /// `WHERE` clauses for fast probabilistic membership pruning.
    BloomFilterTest {
        /// The value to test.
        expr: Box<SqlExpr>,
        /// Base64-encoded bloom-filter sketch literal.
        encoded_filter: String,
    },
    /// `MV_FILTER_ONLY(col, ARRAY[v1, v2, ...])` — keep only the listed
    /// values inside a multi-value column.
    MvFilterOnly {
        /// The multi-value column.
        column: Box<SqlExpr>,
        /// Values to keep.
        values: Vec<SqlExpr>,
    },
    /// `MV_FILTER_NONE(col, ARRAY[v1, v2, ...])` — drop the listed values
    /// from a multi-value column.
    MvFilterNone {
        /// The multi-value column.
        column: Box<SqlExpr>,
        /// Values to drop.
        values: Vec<SqlExpr>,
    },

    // ----- Numeric functions -----
    /// `ABS(expr)` — absolute value.
    Abs(Box<SqlExpr>),
    /// `CEIL(expr)` — ceiling.
    Ceil(Box<SqlExpr>),
    /// `FLOOR(expr)` — floor.
    Floor(Box<SqlExpr>),
    /// `ROUND(expr [, digits])` — round to N decimal places.
    Round {
        /// The numeric expression.
        expr: Box<SqlExpr>,
        /// Number of decimal places (default 0).
        digits: i32,
    },
    /// `POWER(base, exponent)` — exponentiation.
    Power {
        /// The base.
        base: Box<SqlExpr>,
        /// The exponent.
        exponent: Box<SqlExpr>,
    },
    /// `SQRT(expr)` — square root.
    Sqrt(Box<SqlExpr>),
    /// `LOG10(expr)` — base-10 logarithm.
    Log10(Box<SqlExpr>),
    /// `LN(expr)` — natural logarithm.
    Ln(Box<SqlExpr>),
    /// `MOD(a, b)` — modulo.
    Mod {
        /// The dividend.
        a: Box<SqlExpr>,
        /// The divisor.
        b: Box<SqlExpr>,
    },
    /// `TRUNCATE(expr [, digits])` — truncate to N decimal places.
    Truncate {
        /// The numeric expression.
        expr: Box<SqlExpr>,
        /// Number of decimal places (default 0).
        digits: i32,
    },
    /// `GREATEST(a, b, ...)` — the maximum of the arguments.
    Greatest(Vec<SqlExpr>),
    /// `LEAST(a, b, ...)` — the minimum of the arguments.
    Least(Vec<SqlExpr>),

    // ----- String functions -----
    /// `CONCAT(expr, ...)` — string concatenation.
    Concat(Vec<SqlExpr>),
    /// `LENGTH(expr)` or `CHAR_LENGTH(expr)` — string length.
    Length(Box<SqlExpr>),
    /// `LOWER(expr)` — lowercase.
    Lower(Box<SqlExpr>),
    /// `UPPER(expr)` — uppercase.
    Upper(Box<SqlExpr>),
    /// `TRIM(expr)` — trim whitespace.
    Trim(Box<SqlExpr>),
    /// `LTRIM(expr)` — trim leading whitespace.
    Ltrim(Box<SqlExpr>),
    /// `RTRIM(expr)` — trim trailing whitespace.
    Rtrim(Box<SqlExpr>),
    /// `SUBSTRING(expr, start [, length])` — substring extraction.
    Substring {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// 1-based start position.
        start: usize,
        /// Optional length.
        length: Option<usize>,
    },
    /// `REPLACE(expr, pattern, replacement)` — string replacement.
    Replace {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// The substring to find.
        pattern: String,
        /// The replacement string.
        replacement: String,
    },
    /// `REGEXP_EXTRACT(expr, pattern [, index])` — extract regex group.
    RegexpExtract {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// The regex pattern.
        pattern: String,
        /// The capture group index (default 0 = entire match).
        index: usize,
    },
    /// `LOOKUP(dimension, 'lookup_name')` — lookup table function.
    Lookup {
        /// The dimension expression.
        expr: Box<SqlExpr>,
        /// The lookup name.
        lookup_name: String,
    },
    /// `LPAD(expr, length, pad)` — left-pad a string.
    Lpad {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// Target length.
        length: usize,
        /// Pad character(s).
        pad: String,
    },
    /// `RPAD(expr, length, pad)` — right-pad a string.
    Rpad {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// Target length.
        length: usize,
        /// Pad character(s).
        pad: String,
    },
    /// `REVERSE(expr)` — reverse a string.
    Reverse(Box<SqlExpr>),
    /// `REPEAT(expr, n)` — repeat a string N times.
    Repeat {
        /// The string expression.
        expr: Box<SqlExpr>,
        /// Number of repetitions.
        count: usize,
    },
    /// `POSITION(substr IN str)` — find position of substring (1-based, 0 if not found).
    Position {
        /// The substring to search for.
        substr: Box<SqlExpr>,
        /// The string to search in.
        string: Box<SqlExpr>,
    },

    // ----- Conditional functions -----
    /// `CASE WHEN ... THEN ... ELSE ... END` — conditional expression.
    Case {
        /// The operand to compare (for simple CASE), or None for searched CASE.
        operand: Option<Box<SqlExpr>>,
        /// List of (condition, result) pairs.
        when_clauses: Vec<(SqlExpr, SqlExpr)>,
        /// The ELSE result, if any.
        else_result: Option<Box<SqlExpr>>,
    },
    /// `COALESCE(a, b, ...)` — return the first non-null argument.
    Coalesce(Vec<SqlExpr>),
    /// `NULLIF(a, b)` — return NULL if a = b, otherwise a.
    Nullif {
        /// First expression.
        a: Box<SqlExpr>,
        /// Second expression.
        b: Box<SqlExpr>,
    },
    /// `NVL(a, b)` — return a if not null, otherwise b (Oracle-style COALESCE).
    Nvl {
        /// Primary expression.
        a: Box<SqlExpr>,
        /// Fallback expression.
        b: Box<SqlExpr>,
    },
}

// ---------------------------------------------------------------------------
// Window function types (Druid 33+)
// ---------------------------------------------------------------------------

/// Window function specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFunction {
    /// The window function to apply.
    pub function: WindowFunctionType,
    /// PARTITION BY columns.
    pub partition_by: Vec<String>,
    /// ORDER BY expressions within the window.
    pub order_by: Vec<crate::parser::OrderByExpr>,
    /// Optional window frame clause.
    pub frame: Option<WindowFrame>,
}

/// The type of window function being applied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowFunctionType {
    /// `ROW_NUMBER()` — assigns a unique sequential integer to each row.
    RowNumber,
    /// `RANK()` — assigns a rank with gaps for ties.
    Rank,
    /// `DENSE_RANK()` — assigns a rank without gaps for ties.
    DenseRank,
    /// `LAG(column, offset, default)` — accesses a previous row value.
    Lag {
        /// The column to reference.
        column: String,
        /// Number of rows to look back.
        offset: usize,
        /// Default value if no row exists at that offset.
        default: Option<serde_json::Value>,
    },
    /// `LEAD(column, offset, default)` — accesses a subsequent row value.
    Lead {
        /// The column to reference.
        column: String,
        /// Number of rows to look forward.
        offset: usize,
        /// Default value if no row exists at that offset.
        default: Option<serde_json::Value>,
    },
    /// `FIRST_VALUE(column)` — returns the first value in the window frame.
    FirstValue {
        /// The column to reference.
        column: String,
    },
    /// `LAST_VALUE(column)` — returns the last value in the window frame.
    LastValue {
        /// The column to reference.
        column: String,
    },
    /// `SUM(column)` as a window function.
    Sum {
        /// The column to sum.
        column: String,
    },
    /// `COUNT(...)` as a window function.
    ///
    /// Wave 36-G3 (Wave 37B High `parser.rs:1256-1305`): the prior variant
    /// was a unit `Count` and silently collapsed `COUNT(*)`,
    /// `COUNT(col)`, and `COUNT(DISTINCT col)` into the same row-count
    /// semantic. The argument and distinct flag are now preserved so that
    /// null-sensitive (`COUNT(col)`) and distinct (`COUNT(DISTINCT col)`)
    /// forms can be executed correctly or rejected explicitly downstream.
    Count {
        /// The column being counted, or `None` for `COUNT(*)`.
        column: Option<String>,
        /// Whether the `DISTINCT` modifier was present.
        distinct: bool,
    },
    /// `AVG(column)` as a window function.
    Avg {
        /// The column to average.
        column: String,
    },
    /// `MIN(column)` as a window function.
    Min {
        /// The column to find the minimum of.
        column: String,
    },
    /// `MAX(column)` as a window function.
    Max {
        /// The column to find the maximum of.
        column: String,
    },
    /// `NTH_VALUE(column, n)` — returns the n-th value in the window frame.
    NthValue {
        /// The column to reference.
        column: String,
        /// The 1-based position within the frame.
        n: usize,
    },
    /// `NTILE(n)` — bucket each row into one of `n` equally-sized tiles.
    Ntile {
        /// Number of tiles.
        tiles: usize,
    },
    /// `CUME_DIST()` — cumulative distribution (`rank / total_rows`).
    CumeDist,
    /// `PERCENT_RANK()` — `(rank - 1) / (total_rows - 1)`.
    PercentRank,
}

/// Window frame specification (ROWS or RANGE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowFrame {
    /// The frame mode (ROWS or RANGE).
    pub mode: FrameMode,
    /// The start bound of the frame.
    pub start: FrameBound,
    /// The end bound of the frame.
    pub end: FrameBound,
}

/// Window frame mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameMode {
    /// `ROWS` — physical row-based frame.
    Rows,
    /// `RANGE` — logical value-based frame.
    Range,
}

/// Window frame bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrameBound {
    /// `UNBOUNDED PRECEDING`.
    UnboundedPreceding,
    /// `N PRECEDING`.
    Preceding(usize),
    /// `CURRENT ROW`.
    CurrentRow,
    /// `N FOLLOWING`.
    Following(usize),
    /// `UNBOUNDED FOLLOWING`.
    UnboundedFollowing,
}

/// Returns `true` if the name is a known window function.
pub fn is_window_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "ROW_NUMBER"
            | "RANK"
            | "DENSE_RANK"
            | "LAG"
            | "LEAD"
            | "FIRST_VALUE"
            | "LAST_VALUE"
            | "NTH_VALUE"
            | "NTILE"
            | "CUME_DIST"
            | "PERCENT_RANK"
    )
}

/// Attempt to recognise a function name as a Druid SQL function.
///
/// Returns `true` if the name is a known Druid SQL function.
pub fn is_druid_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "TIME_FLOOR"
            | "TIME_CEIL"
            | "TIME_SHIFT"
            | "TIME_FORMAT"
            | "TIME_PARSE"
            | "TIME_EXTRACT"
            | "TIMESTAMPDIFF"
            | "CURRENT_TIMESTAMP"
            | "APPROX_COUNT_DISTINCT"
            | "APPROX_QUANTILE_DS"
            | "EARLIEST"
            | "LATEST"
            | "ANY_VALUE"
            | "ABS"
            | "CEIL"
            | "FLOOR"
            | "ROUND"
            | "POWER"
            | "SQRT"
            | "LOG10"
            | "LN"
            | "MOD"
            | "TRUNCATE"
            | "GREATEST"
            | "LEAST"
            | "CONCAT"
            | "LENGTH"
            | "CHAR_LENGTH"
            | "LOWER"
            | "UPPER"
            | "TRIM"
            | "LTRIM"
            | "RTRIM"
            | "SUBSTRING"
            | "SUBSTR"
            | "REPLACE"
            | "REGEXP_EXTRACT"
            | "LOOKUP"
            | "LPAD"
            | "RPAD"
            | "REVERSE"
            | "REPEAT"
            | "POSITION"
            | "COALESCE"
            | "NULLIF"
            | "NVL"
            // ----- CL-4 / W1-D additions -----
            | "ARRAY_AGG"
            | "LISTAGG"
            | "STRING_AGG"
            | "BLOOM_FILTER"
            | "BLOOM_FILTER_TEST"
            | "MV_FILTER_ONLY"
            | "MV_FILTER_NONE"
            | "GROUPING"
    )
}

/// Convert an ISO-8601 period string to the corresponding simple granularity name, if possible.
pub fn period_to_granularity(period: &str) -> Option<&'static str> {
    match period {
        "PT1S" => Some("second"),
        "PT1M" => Some("minute"),
        "PT5M" => Some("five_minute"),
        "PT10M" => Some("ten_minute"),
        "PT15M" => Some("fifteen_minute"),
        "PT30M" => Some("thirty_minute"),
        "PT1H" => Some("hour"),
        "PT6H" => Some("six_hour"),
        "P1D" => Some("day"),
        "P1W" => Some("week"),
        "P1M" => Some("month"),
        "P3M" => Some("quarter"),
        "P1Y" => Some("year"),
        _ => None,
    }
}

/// Convert a single-field fixed-length ISO-8601 time period (`PT<n>S`,
/// `PT<n>M`, `PT<n>H`) to its length in milliseconds, if it is one.
///
/// This is the fallback for `TIME_FLOOR` periods with no named-granularity
/// mapping (e.g. Superset's `PT5S` / `PT30S` grains): Druid floors such
/// fixed periods to `origin + floor((t - origin) / period) * period` with
/// the epoch as the default origin — exactly the native `duration`
/// granularity. Compound periods (`PT1H30M`) and calendar-designator
/// multiples (`P2D`, `P2W`, `P2M`, ...) do NOT truncate as one fixed span
/// in Druid's period-granularity semantics, so they return `None` here and
/// stay fail-closed at the planner.
pub fn period_to_fixed_millis(period: &str) -> Option<u64> {
    let rest = period.strip_prefix("PT")?;
    let unit = rest.chars().next_back()?;
    let digits = &rest[..rest.len() - unit.len_utf8()];
    let unit_ms: u64 = match unit {
        'S' => 1_000,
        'M' => 60_000,
        'H' => 3_600_000,
        _ => return None,
    };
    // Strict ASCII-digit check: `u64::from_str` would also accept a leading
    // `+`, which is not valid ISO-8601 here.
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    if n == 0 {
        // A zero-length bucket is meaningless (and the executor's
        // granularity validation rejects periodMs == 0).
        return None;
    }
    n.checked_mul(unit_ms)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_unit_parse() {
        assert_eq!(TimeUnit::parse("HOUR"), Some(TimeUnit::Hour));
        assert_eq!(TimeUnit::parse("hour"), Some(TimeUnit::Hour));
        assert_eq!(TimeUnit::parse("Day"), Some(TimeUnit::Day));
        assert_eq!(TimeUnit::parse("epoch"), Some(TimeUnit::Epoch));
        assert!(TimeUnit::parse("invalid").is_none());
    }

    #[test]
    fn time_unit_to_period() {
        assert_eq!(TimeUnit::Hour.to_iso_period(), Some("PT1H"));
        assert_eq!(TimeUnit::Day.to_iso_period(), Some("P1D"));
        assert_eq!(TimeUnit::Month.to_iso_period(), Some("P1M"));
        assert!(TimeUnit::Epoch.to_iso_period().is_none());
    }

    #[test]
    fn known_functions() {
        assert!(is_druid_function("TIME_FLOOR"));
        assert!(is_druid_function("time_floor"));
        assert!(is_druid_function("APPROX_COUNT_DISTINCT"));
        assert!(is_druid_function("LOWER"));
        assert!(is_druid_function("SUBSTR"));
        assert!(!is_druid_function("RANDOM_FUNC"));
    }

    #[test]
    fn known_new_functions() {
        assert!(is_druid_function("TIME_SHIFT"));
        assert!(is_druid_function("TIMESTAMPDIFF"));
        assert!(is_druid_function("APPROX_QUANTILE_DS"));
        assert!(is_druid_function("EARLIEST"));
        assert!(is_druid_function("LATEST"));
        assert!(is_druid_function("ANY_VALUE"));
        assert!(is_druid_function("ROUND"));
        assert!(is_druid_function("POWER"));
        assert!(is_druid_function("SQRT"));
        assert!(is_druid_function("LOG10"));
        assert!(is_druid_function("LN"));
        assert!(is_druid_function("MOD"));
        assert!(is_druid_function("TRUNCATE"));
        assert!(is_druid_function("GREATEST"));
        assert!(is_druid_function("LEAST"));
        assert!(is_druid_function("LTRIM"));
        assert!(is_druid_function("RTRIM"));
        assert!(is_druid_function("REGEXP_EXTRACT"));
        assert!(is_druid_function("LOOKUP"));
        assert!(is_druid_function("LPAD"));
        assert!(is_druid_function("RPAD"));
        assert!(is_druid_function("REVERSE"));
        assert!(is_druid_function("REPEAT"));
        assert!(is_druid_function("POSITION"));
        assert!(is_druid_function("COALESCE"));
        assert!(is_druid_function("NULLIF"));
        assert!(is_druid_function("NVL"));
    }

    // ----- CL-4 / W1-D: Calcite + Druid-specific surface additions -----

    #[test]
    fn cl4_functions_registered() {
        // Aggregate-family additions parsed as scalar Druid functions.
        for name in [
            "ARRAY_AGG",
            "LISTAGG",
            "STRING_AGG",
            "BLOOM_FILTER",
            "BLOOM_FILTER_TEST",
            "MV_FILTER_ONLY",
            "MV_FILTER_NONE",
            "GROUPING",
        ] {
            assert!(
                is_druid_function(name),
                "CL-4 function `{name}` must be recognised by is_druid_function"
            );
            // Lower-cased recognition for case-insensitive parsing.
            assert!(
                is_druid_function(&name.to_lowercase()),
                "CL-4 function `{name}` must be case-insensitively recognised"
            );
        }
    }

    #[test]
    fn cl4_window_functions_registered() {
        for name in ["NTH_VALUE", "NTILE", "CUME_DIST", "PERCENT_RANK"] {
            assert!(
                is_window_function(name),
                "CL-4 window function `{name}` must be recognised"
            );
            assert!(
                is_window_function(&name.to_lowercase()),
                "CL-4 window function `{name}` must be case-insensitively recognised"
            );
        }
    }

    #[test]
    fn cl4_recognition_does_not_swallow_unknown_names() {
        // A nearby misspelling must still be rejected.
        assert!(!is_druid_function("ARRAY_AG"));
        assert!(!is_druid_function("BLOOMFILTER"));
        assert!(!is_druid_function("MV_FILTER"));
        assert!(!is_window_function("NTH"));
        assert!(!is_window_function("PERCENTILE_RANK"));
    }

    #[test]
    fn period_to_gran() {
        assert_eq!(period_to_granularity("PT1H"), Some("hour"));
        assert_eq!(period_to_granularity("P1D"), Some("day"));
        assert_eq!(period_to_granularity("P1Y"), Some("year"));
        assert!(period_to_granularity("PT7H").is_none());
    }

    #[test]
    fn period_to_gran_extended() {
        assert_eq!(period_to_granularity("PT1S"), Some("second"));
        assert_eq!(period_to_granularity("PT1M"), Some("minute"));
        assert_eq!(period_to_granularity("PT5M"), Some("five_minute"));
        assert_eq!(period_to_granularity("PT10M"), Some("ten_minute"));
        assert_eq!(period_to_granularity("PT15M"), Some("fifteen_minute"));
        assert_eq!(period_to_granularity("PT30M"), Some("thirty_minute"));
        assert_eq!(period_to_granularity("PT6H"), Some("six_hour"));
        assert_eq!(period_to_granularity("P1W"), Some("week"));
        assert_eq!(period_to_granularity("P1M"), Some("month"));
        assert_eq!(period_to_granularity("P3M"), Some("quarter"));
    }

    #[test]
    fn period_to_fixed_millis_single_field_time_periods() {
        // Superset's sub-minute grains and other fixed multiples.
        assert_eq!(period_to_fixed_millis("PT5S"), Some(5_000));
        assert_eq!(period_to_fixed_millis("PT10S"), Some(10_000));
        assert_eq!(period_to_fixed_millis("PT15S"), Some(15_000));
        assert_eq!(period_to_fixed_millis("PT30S"), Some(30_000));
        assert_eq!(period_to_fixed_millis("PT1S"), Some(1_000));
        assert_eq!(period_to_fixed_millis("PT90S"), Some(90_000));
        assert_eq!(period_to_fixed_millis("PT15M"), Some(900_000));
        assert_eq!(period_to_fixed_millis("PT45M"), Some(2_700_000));
        assert_eq!(period_to_fixed_millis("PT2H"), Some(7_200_000));
        assert_eq!(period_to_fixed_millis("PT7H"), Some(25_200_000));
    }

    #[test]
    fn period_to_fixed_millis_rejects_non_fixed_and_malformed() {
        // Compound time periods do not truncate as one fixed span in Druid.
        assert_eq!(period_to_fixed_millis("PT1H30M"), None);
        // Calendar designators (before the T) are not fixed-length.
        assert_eq!(period_to_fixed_millis("P1D"), None);
        assert_eq!(period_to_fixed_millis("P2W"), None);
        assert_eq!(period_to_fixed_millis("P2M"), None);
        assert_eq!(period_to_fixed_millis("P1DT5S"), None);
        // Malformed / degenerate forms.
        assert_eq!(period_to_fixed_millis("PT0S"), None);
        assert_eq!(period_to_fixed_millis("PT"), None);
        assert_eq!(period_to_fixed_millis("PTS"), None);
        assert_eq!(period_to_fixed_millis("PT+5S"), None);
        assert_eq!(period_to_fixed_millis("PT-5S"), None);
        assert_eq!(period_to_fixed_millis("PT5X"), None);
        assert_eq!(period_to_fixed_millis("PT5.5S"), None);
        assert_eq!(period_to_fixed_millis("pt5s"), None);
        assert_eq!(period_to_fixed_millis(""), None);
        // Overflow-checked: u64::MAX seconds * 1000 must not wrap.
        assert_eq!(period_to_fixed_millis("PT18446744073709551615S"), None);
    }
}
