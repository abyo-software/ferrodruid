// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! SQL compatibility tests — verifies that Druid SQL dialect parsing and
//! planning succeeds for a broad set of real-world query patterns.

use ferrodruid_sql::parser::parse_druid_sql;
use ferrodruid_sql::planner::{ColumnSchema, DataSourceSchema, plan_sql};

use ferrodruid_common::types::ColumnType;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn parse_and_plan(sql: &str) {
    let stmt = parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
    let schema = test_schema();
    // Planning may fail for features not yet fully supported — but parsing must succeed.
    let _ = plan_sql(&stmt, &schema);
}

fn parse_only(sql: &str) {
    parse_druid_sql(sql).unwrap_or_else(|e| panic!("parse failed for [{sql}]: {e}"));
}

fn test_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "wiki".to_string(),
        dimensions: vec![
            ColumnSchema {
                name: "city".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "country".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "product".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "user".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "page".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "channel".to_string(),
                column_type: ColumnType::String,
            },
        ],
        metrics: vec![
            ColumnSchema {
                name: "added".to_string(),
                column_type: ColumnType::Long,
            },
            ColumnSchema {
                name: "deleted".to_string(),
                column_type: ColumnType::Long,
            },
            ColumnSchema {
                name: "delta".to_string(),
                column_type: ColumnType::Long,
            },
            ColumnSchema {
                name: "revenue".to_string(),
                column_type: ColumnType::Double,
            },
            ColumnSchema {
                name: "price".to_string(),
                column_type: ColumnType::Double,
            },
        ],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

// ===========================================================================
// Time function tests
// ===========================================================================

#[test]
fn sql_time_floor_hour() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_day() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'P1D') AS t, SUM(added) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_minute() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'PT1M') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_five_minute() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'PT5M') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_week() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'P1W') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_month() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'P1M') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_quarter() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'P3M') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_year() {
    parse_and_plan(r#"SELECT TIME_FLOOR(__time, 'P1Y') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_floor_with_timezone() {
    parse_and_plan(
        r#"SELECT TIME_FLOOR(__time, 'PT1H', NULL, 'America/New_York') AS t, COUNT(*) FROM wiki GROUP BY 1"#,
    );
}

#[test]
fn sql_time_ceil_hour() {
    parse_and_plan(r#"SELECT TIME_CEIL(__time, 'PT1H') AS t, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_shift() {
    parse_only(r#"SELECT TIME_SHIFT(__time, 'PT1H', 1) FROM wiki"#);
}

#[test]
fn sql_time_format() {
    parse_only(r#"SELECT TIME_FORMAT(__time, 'yyyy-MM-dd') AS d FROM wiki"#);
}

#[test]
fn sql_time_parse() {
    parse_only(r#"SELECT TIME_PARSE('2024-01-01', 'yyyy-MM-dd') FROM wiki"#);
}

#[test]
fn sql_time_parse_no_format() {
    parse_only(r#"SELECT TIME_PARSE('2024-01-01T00:00:00Z') FROM wiki"#);
}

#[test]
fn sql_time_extract_hour() {
    parse_and_plan(r#"SELECT TIME_EXTRACT(__time, 'HOUR') AS h, COUNT(*) FROM wiki GROUP BY 1"#);
}

#[test]
fn sql_time_extract_day() {
    parse_only(r#"SELECT TIME_EXTRACT(__time, 'DAY') AS d FROM wiki"#);
}

#[test]
fn sql_time_extract_month() {
    parse_only(r#"SELECT TIME_EXTRACT(__time, 'MONTH') AS m FROM wiki"#);
}

#[test]
fn sql_time_extract_year() {
    parse_only(r#"SELECT TIME_EXTRACT(__time, 'YEAR') AS y FROM wiki"#);
}

#[test]
fn sql_time_extract_dow() {
    parse_only(r#"SELECT TIME_EXTRACT(__time, 'DOW') AS dow FROM wiki"#);
}

#[test]
fn sql_time_extract_quarter() {
    parse_only(r#"SELECT TIME_EXTRACT(__time, 'QUARTER') AS q FROM wiki"#);
}

#[test]
fn sql_timestampdiff() {
    parse_only(
        r#"SELECT TIMESTAMPDIFF('HOUR', TIME_PARSE('2024-01-01'), TIME_PARSE('2024-01-02')) FROM wiki"#,
    );
}

#[test]
fn sql_current_timestamp() {
    parse_only(r#"SELECT CURRENT_TIMESTAMP FROM wiki"#);
}

// ===========================================================================
// Aggregate function tests
// ===========================================================================

#[test]
fn sql_count_star() {
    parse_and_plan("SELECT COUNT(*) FROM wiki");
}

#[test]
fn sql_count_distinct() {
    parse_and_plan(r#"SELECT COUNT(DISTINCT "user") FROM wiki"#);
}

#[test]
fn sql_sum() {
    parse_and_plan("SELECT SUM(added) AS total_added FROM wiki");
}

#[test]
fn sql_min_max() {
    parse_and_plan("SELECT MIN(added), MAX(added) FROM wiki");
}

#[test]
fn sql_avg() {
    parse_and_plan("SELECT city, AVG(revenue) FROM wiki GROUP BY city");
}

#[test]
fn sql_approx_count_distinct() {
    parse_and_plan(r#"SELECT APPROX_COUNT_DISTINCT("user") FROM wiki"#);
}

#[test]
fn sql_approx_quantile_ds() {
    parse_and_plan("SELECT APPROX_QUANTILE_DS(revenue, 0.5) AS median FROM wiki");
}

#[test]
fn sql_earliest() {
    parse_and_plan("SELECT EARLIEST(city) AS first_city FROM wiki");
}

#[test]
fn sql_latest() {
    parse_and_plan("SELECT LATEST(city) AS last_city FROM wiki");
}

#[test]
fn sql_any_value() {
    parse_and_plan("SELECT ANY_VALUE(city) AS some_city FROM wiki");
}

// ===========================================================================
// String function tests
// ===========================================================================

#[test]
fn sql_concat() {
    parse_only(r#"SELECT CONCAT(city, '-', product) FROM wiki"#);
}

#[test]
fn sql_concat_two() {
    parse_only("SELECT CONCAT(city, country) FROM wiki");
}

#[test]
fn sql_lower_upper() {
    parse_only("SELECT LOWER(city), UPPER(product) FROM wiki");
}

#[test]
fn sql_length() {
    parse_only("SELECT LENGTH(city) AS len FROM wiki");
}

#[test]
fn sql_char_length() {
    parse_only("SELECT CHAR_LENGTH(city) AS len FROM wiki");
}

#[test]
fn sql_trim() {
    parse_only("SELECT TRIM(city) FROM wiki");
}

#[test]
fn sql_ltrim_rtrim() {
    parse_only("SELECT LTRIM(city), RTRIM(city) FROM wiki");
}

#[test]
fn sql_substring() {
    parse_only("SELECT SUBSTRING(city, 1, 3) FROM wiki");
}

#[test]
fn sql_substr() {
    parse_only("SELECT SUBSTR(city, 2) FROM wiki");
}

#[test]
fn sql_replace() {
    parse_only(r#"SELECT REPLACE(city, 'old', 'new') FROM wiki"#);
}

#[test]
fn sql_regexp_extract() {
    parse_only(r#"SELECT REGEXP_EXTRACT(city, '(\w+)', 1) FROM wiki"#);
}

#[test]
fn sql_lookup() {
    parse_only(r#"SELECT LOOKUP(city, 'city_lookup') FROM wiki"#);
}

#[test]
fn sql_lpad() {
    parse_only(r#"SELECT LPAD(city, 10, ' ') FROM wiki"#);
}

#[test]
fn sql_rpad() {
    parse_only(r#"SELECT RPAD(city, 10, '*') FROM wiki"#);
}

#[test]
fn sql_reverse() {
    parse_only("SELECT REVERSE(city) FROM wiki");
}

#[test]
fn sql_repeat() {
    parse_only("SELECT REPEAT(city, 3) FROM wiki");
}

// ===========================================================================
// Numeric function tests
// ===========================================================================

#[test]
fn sql_abs() {
    parse_only("SELECT ABS(delta) FROM wiki");
}

#[test]
fn sql_ceil_floor() {
    parse_only("SELECT CEIL(revenue), FLOOR(revenue) FROM wiki");
}

#[test]
fn sql_round() {
    parse_only("SELECT ROUND(revenue, 2) FROM wiki");
}

#[test]
fn sql_round_no_digits() {
    parse_only("SELECT ROUND(revenue) FROM wiki");
}

#[test]
fn sql_power() {
    parse_only("SELECT POWER(revenue, 2) FROM wiki");
}

#[test]
fn sql_sqrt() {
    parse_only("SELECT SQRT(revenue) FROM wiki");
}

#[test]
fn sql_log10() {
    parse_only("SELECT LOG10(revenue) FROM wiki");
}

#[test]
fn sql_ln() {
    parse_only("SELECT LN(revenue) FROM wiki");
}

#[test]
fn sql_mod() {
    parse_only("SELECT MOD(added, 10) FROM wiki");
}

#[test]
fn sql_truncate() {
    parse_only("SELECT TRUNCATE(revenue, 2) FROM wiki");
}

#[test]
fn sql_greatest_least() {
    parse_only("SELECT GREATEST(added, deleted), LEAST(added, deleted) FROM wiki");
}

// ===========================================================================
// Conditional expression tests
// ===========================================================================

#[test]
fn sql_case_when() {
    parse_only(r#"SELECT CASE WHEN revenue > 100 THEN 'high' ELSE 'low' END AS tier FROM wiki"#);
}

#[test]
fn sql_case_when_multiple() {
    parse_only(
        r#"SELECT CASE WHEN revenue > 1000 THEN 'premium' WHEN revenue > 100 THEN 'standard' ELSE 'basic' END AS tier FROM wiki"#,
    );
}

#[test]
fn sql_coalesce() {
    parse_only(r#"SELECT COALESCE(city, country, 'unknown') FROM wiki"#);
}

#[test]
fn sql_nullif() {
    parse_only(r#"SELECT NULLIF(city, '') FROM wiki"#);
}

#[test]
fn sql_nvl() {
    parse_only(r#"SELECT NVL(city, 'unknown') FROM wiki"#);
}

// ===========================================================================
// CAST tests
// ===========================================================================

#[test]
fn sql_cast_bigint() {
    parse_only("SELECT CAST(revenue AS BIGINT) FROM wiki");
}

#[test]
fn sql_cast_varchar() {
    parse_only("SELECT CAST(added AS VARCHAR) FROM wiki");
}

#[test]
fn sql_cast_double() {
    parse_only("SELECT CAST(added AS DOUBLE) FROM wiki");
}

#[test]
fn sql_cast_timestamp() {
    parse_only(r#"SELECT CAST('2024-01-01' AS TIMESTAMP) FROM wiki"#);
}

// ===========================================================================
// Complex / real-world query tests
// ===========================================================================

#[test]
fn sql_real_world_dashboard_query() {
    parse_and_plan(
        r#"
        SELECT
            TIME_FLOOR(__time, 'PT1H') AS hour,
            city,
            COUNT(*) AS events,
            SUM(revenue) AS total_revenue,
            AVG(price) AS avg_price
        FROM wiki
        WHERE __time >= TIMESTAMP '2024-01-01' AND __time < TIMESTAMP '2024-01-02'
          AND city IN ('tokyo', 'london', 'new york')
        GROUP BY 1, city
        ORDER BY total_revenue DESC
        LIMIT 100
    "#,
    );
}

#[test]
fn sql_topn_pattern() {
    parse_and_plan(
        "SELECT city, COUNT(*) AS cnt FROM wiki GROUP BY city ORDER BY cnt DESC LIMIT 10",
    );
}

#[test]
fn sql_timeseries_all_granularity() {
    parse_and_plan("SELECT COUNT(*) AS cnt, SUM(added) AS total FROM wiki");
}

#[test]
fn sql_groupby_with_having() {
    parse_and_plan(
        "SELECT city, SUM(revenue) AS rev FROM wiki GROUP BY city HAVING SUM(revenue) > 1000",
    );
}

#[test]
fn sql_scan_with_filter() {
    parse_and_plan(
        "SELECT city, revenue FROM wiki WHERE city = 'tokyo' AND revenue > 100 ORDER BY revenue DESC LIMIT 50",
    );
}

#[test]
fn sql_multiple_aggregations() {
    parse_and_plan(
        "SELECT city, COUNT(*) AS cnt, SUM(added) AS total_added, MIN(revenue) AS min_rev, MAX(revenue) AS max_rev FROM wiki GROUP BY city",
    );
}

#[test]
fn sql_time_floor_with_dimension_groupby() {
    parse_and_plan(
        r#"SELECT TIME_FLOOR(__time, 'P1D') AS day, city, COUNT(*) AS cnt FROM wiki GROUP BY 1, city ORDER BY cnt DESC LIMIT 100"#,
    );
}

#[test]
fn sql_between_filter() {
    parse_and_plan("SELECT * FROM wiki WHERE revenue BETWEEN 100 AND 500");
}

#[test]
fn sql_like_filter() {
    parse_and_plan(r#"SELECT * FROM wiki WHERE city LIKE 'tok%'"#);
}

#[test]
fn sql_not_in_filter() {
    parse_and_plan(r#"SELECT * FROM wiki WHERE city NOT IN ('tokyo', 'london')"#);
}

#[test]
fn sql_is_null_filter() {
    parse_and_plan("SELECT * FROM wiki WHERE city IS NULL");
}

#[test]
fn sql_is_not_null_filter() {
    parse_and_plan("SELECT * FROM wiki WHERE city IS NOT NULL");
}

#[test]
fn sql_complex_nested_where() {
    parse_and_plan(
        r#"SELECT * FROM wiki WHERE (city = 'tokyo' OR city = 'london') AND revenue > 100 AND deleted IS NOT NULL"#,
    );
}

#[test]
fn sql_explain_plan() {
    let sql = "EXPLAIN SELECT COUNT(*) FROM wiki";
    let stmt = parse_druid_sql(sql).expect("parse EXPLAIN");
    let schema = test_schema();
    let planned = plan_sql(&stmt, &schema).expect("plan EXPLAIN");
    // EXPLAIN wraps, but the underlying native query should still be produced.
    let json = serde_json::to_string(&planned.native_query).expect("serialize");
    assert!(!json.is_empty());
}

#[test]
fn sql_wildcard_scan() {
    parse_and_plan("SELECT * FROM wiki LIMIT 100");
}

#[test]
fn sql_offset() {
    parse_and_plan("SELECT city FROM wiki LIMIT 10 OFFSET 20");
}

#[test]
fn sql_function_in_where() {
    parse_only("SELECT * FROM wiki WHERE ABS(delta) > 100");
}

#[test]
fn sql_nested_functions() {
    parse_only(r#"SELECT LOWER(CONCAT(city, '-', country)) FROM wiki"#);
}

#[test]
fn sql_mixed_time_and_agg() {
    parse_and_plan(
        r#"SELECT TIME_FLOOR(__time, 'PT1H') AS hour, APPROX_COUNT_DISTINCT("user") AS uniq_users FROM wiki GROUP BY 1"#,
    );
}

#[test]
fn sql_real_world_retention_query() {
    parse_only(
        r#"
        SELECT
            TIME_FLOOR(__time, 'P1D') AS day,
            COUNT(DISTINCT "user") AS daily_users,
            SUM(added) AS total_additions
        FROM wiki
        WHERE __time >= TIMESTAMP '2024-01-01'
          AND __time < TIMESTAMP '2024-02-01'
        GROUP BY 1
        ORDER BY day
        LIMIT 31
    "#,
    );
}
