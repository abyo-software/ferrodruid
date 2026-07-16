// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! TPC-H Star Schema Benchmark data generator for FerroDruid.
//!
//! Produces deterministic lineitem-style rows suitable for benchmarking
//! FerroDruid native query execution against Druid-compatible queries.
//! The generator uses a simple PRNG seeded with a constant so that results
//! are reproducible across runs.

/// Ship-mode dimension values (TPC-H LINEITEM.L_SHIPMODE).
const SHIP_MODES: &[&str] = &["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];

/// Return-flag dimension values (TPC-H LINEITEM.L_RETURNFLAG).
const RETURN_FLAGS: &[&str] = &["A", "N", "R"];

/// Line-status dimension values (TPC-H LINEITEM.L_LINESTATUS).
const LINE_STATUSES: &[&str] = &["F", "O"];

/// Order priority dimension values (TPC-H ORDERS.O_ORDERPRIORITY).
const PRIORITIES: &[&str] = &["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

/// Nation dimension values (TPC-H NATION.N_NAME, all 25 TPC-H nations).
const NATIONS: &[&str] = &[
    "UNITED STATES",
    "CHINA",
    "JAPAN",
    "GERMANY",
    "FRANCE",
    "BRAZIL",
    "INDIA",
    "RUSSIA",
    "CANADA",
    "UNITED KINGDOM",
    "INDONESIA",
    "EGYPT",
    "ARGENTINA",
    "PERU",
    "ETHIOPIA",
    "IRAN",
    "JORDAN",
    "KENYA",
    "MOROCCO",
    "MOZAMBIQUE",
    "ROMANIA",
    "SAUDI ARABIA",
    "VIETNAM",
    "ALGERIA",
    "IRAQ",
];

/// Region dimension values (TPC-H REGION.R_NAME, 5 regions, 5 nations each).
const REGIONS: &[&str] = &["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"];

/// Generate TPC-H lineitem-style rows for benchmarking.
///
/// Scale factor 1 in TPC-H produces ~6M rows. This function generates
/// `num_rows` rows proportionally, with deterministic PRNG output.
///
/// Each row is a JSON object with the following fields:
/// - `__time` (i64): epoch milliseconds, spread across 1992-01-01 to 1998-12-31
/// - `l_orderkey`, `l_partkey`, `l_suppkey`, `l_linenumber` (integer keys)
/// - `l_quantity`, `l_extendedprice` (numeric measures)
/// - `l_discount`, `l_tax` (floating-point measures)
/// - `l_returnflag`, `l_linestatus`, `l_shipmode`, `l_shipinstruct` (string dims)
/// - `n_name`, `r_name` (denormalized nation/region for star-schema joins)
pub fn generate_lineitem_rows(num_rows: usize) -> Vec<serde_json::Value> {
    let mut rows = Vec::with_capacity(num_rows);
    let mut rng_state: u64 = 42; // deterministic PRNG seed

    for i in 0..num_rows {
        rng_state = rng_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);

        // Helper: extract a pseudo-random value in [0, max) from the current state.
        let rng = |max: usize| -> usize { ((rng_state >> 33) as usize) % max };

        // Timestamp: spread across 7 years (1992-01-01 to 1998-12-31).
        let base_ms: i64 = 694_224_000_000; // 1992-01-01T00:00:00Z epoch ms
        let range_ms: i64 = 220_752_000_000; // ~7 years in ms
        // i128 intermediate: at 60M rows `i * range_ms` overflows i64
        // (60e6 * 2.2e11 = 1.3e19 > i64::MAX), which would wrap and break the
        // monotonic (sorted) __time invariant that interval pruning and Druid
        // segment placement both rely on.
        let ts = if num_rows > 1 {
            base_ms
                + (i128::from(i as i64) * i128::from(range_ms)
                    / i128::from((num_rows as i64 - 1).max(1))) as i64
        } else {
            base_ms
        };

        let quantity = (rng(50) + 1) as i64;
        let extended_price = quantity * (rng(900) + 100) as i64; // $1-$10 * qty
        let discount = (rng(11) as f64) / 100.0; // 0-10%
        let tax = (rng(9) as f64) / 100.0; // 0-8%

        let nation_idx = rng(NATIONS.len());
        let region_idx = (nation_idx / 5).min(REGIONS.len() - 1);

        rows.push(serde_json::json!({
            "__time": ts,
            "l_orderkey": i / 4 + 1,
            "l_partkey": rng(200_000) + 1,
            "l_suppkey": rng(10_000) + 1,
            "l_linenumber": (i % 4) + 1,
            "l_quantity": quantity,
            "l_extendedprice": extended_price,
            "l_discount": discount,
            "l_tax": tax,
            "l_returnflag": RETURN_FLAGS[rng(RETURN_FLAGS.len())],
            "l_linestatus": LINE_STATUSES[rng(LINE_STATUSES.len())],
            "l_shipmode": SHIP_MODES[rng(SHIP_MODES.len())],
            "l_shipinstruct": PRIORITIES[rng(PRIORITIES.len())],
            "n_name": NATIONS[nation_idx],
            "r_name": REGIONS[region_idx],
        }));

        // Advance PRNG for next iteration.
        rng_state = rng_state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
    }
    rows
}

/// Standard TPC-H queries adapted for Druid Native Query JSON format.
///
/// Returns a list of `(description, query_json)` pairs. Each query is a valid
/// Druid native query JSON value targeting a datasource named `"lineitem"`.
pub fn tpch_queries() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "Q1: Pricing Summary Report",
            serde_json::json!({
                "queryType": "groupBy",
                "dataSource": {"type": "table", "name": "lineitem"},
                "intervals": ["1992-01-01/1999-01-01"],
                "granularity": "all",
                "dimensions": [
                    {
                        "type": "default",
                        "dimension": "l_returnflag",
                        "output_name": "l_returnflag",
                        "output_type": "STRING"
                    },
                    {
                        "type": "default",
                        "dimension": "l_linestatus",
                        "output_name": "l_linestatus",
                        "output_type": "STRING"
                    }
                ],
                "aggregations": [
                    {"type": "longSum", "name": "sum_qty", "fieldName": "l_quantity"},
                    {"type": "doubleSum", "name": "sum_base_price", "fieldName": "l_extendedprice"},
                    {"type": "count", "name": "count_order"}
                ]
            }),
        ),
        (
            "Q3: Shipping Priority",
            serde_json::json!({
                "queryType": "topN",
                "dataSource": {"type": "table", "name": "lineitem"},
                "intervals": ["1995-03-01/1995-03-31"],
                "granularity": "all",
                "dimension": {
                    "type": "default",
                    "dimension": "l_orderkey",
                    "output_name": "l_orderkey",
                    "output_type": "STRING"
                },
                "threshold": 10,
                "metric": {"type": "numeric", "metric": "revenue"},
                "aggregations": [
                    {"type": "doubleSum", "name": "revenue", "fieldName": "l_extendedprice"}
                ]
            }),
        ),
        (
            "Q6: Forecasting Revenue Change",
            serde_json::json!({
                "queryType": "timeseries",
                "dataSource": {"type": "table", "name": "lineitem"},
                "intervals": ["1994-01-01/1995-01-01"],
                "granularity": "all",
                "filter": {
                    "type": "and",
                    "fields": [
                        {
                            "type": "bound",
                            "dimension": "l_quantity",
                            "lower": "24",
                            "upper": "24",
                            "ordering": "numeric"
                        },
                        {
                            "type": "bound",
                            "dimension": "l_discount",
                            "lower": "0.05",
                            "upper": "0.07",
                            "ordering": "numeric"
                        }
                    ]
                },
                "aggregations": [
                    {"type": "doubleSum", "name": "revenue", "fieldName": "l_extendedprice"}
                ]
            }),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_1000_rows() {
        let rows = generate_lineitem_rows(1000);
        assert_eq!(rows.len(), 1000);
    }

    #[test]
    fn generate_single_row() {
        let rows = generate_lineitem_rows(1);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].get("__time").is_some());
    }

    #[test]
    fn generate_zero_rows() {
        let rows = generate_lineitem_rows(0);
        assert!(rows.is_empty());
    }

    #[test]
    fn all_rows_have_required_fields() {
        let rows = generate_lineitem_rows(500);
        let required = [
            "__time",
            "l_orderkey",
            "l_partkey",
            "l_suppkey",
            "l_linenumber",
            "l_quantity",
            "l_extendedprice",
            "l_discount",
            "l_tax",
            "l_returnflag",
            "l_linestatus",
            "l_shipmode",
            "l_shipinstruct",
            "n_name",
            "r_name",
        ];
        for (i, row) in rows.iter().enumerate() {
            for field in &required {
                assert!(row.get(*field).is_some(), "row {i} missing field {field}");
            }
        }
    }

    #[test]
    fn deterministic_output() {
        let a = generate_lineitem_rows(100);
        let b = generate_lineitem_rows(100);
        assert_eq!(a, b, "PRNG must produce identical output across calls");
    }

    #[test]
    fn timestamps_span_expected_range() {
        let rows = generate_lineitem_rows(1000);
        let first_ts = rows[0]["__time"].as_i64().expect("__time");
        let last_ts = rows[999]["__time"].as_i64().expect("__time");
        // 1992-01-01 epoch ms
        assert!(first_ts >= 694_224_000_000);
        // Should not exceed ~1999
        assert!(last_ts < 920_000_000_000);
        assert!(last_ts > first_ts);
    }

    #[test]
    fn tpch_queries_parse_count() {
        let queries = tpch_queries();
        assert_eq!(queries.len(), 3);
    }

    #[test]
    fn tpch_queries_have_required_fields() {
        for (name, q) in tpch_queries() {
            assert!(q.get("queryType").is_some(), "{name}: missing queryType");
            assert!(q.get("dataSource").is_some(), "{name}: missing dataSource");
            assert!(
                q.get("aggregations").is_some(),
                "{name}: missing aggregations"
            );
        }
    }

    #[test]
    fn quantity_within_range() {
        let rows = generate_lineitem_rows(200);
        for row in &rows {
            let qty = row["l_quantity"].as_i64().expect("l_quantity");
            assert!((1..=50).contains(&qty), "quantity {qty} out of range 1-50");
        }
    }

    #[test]
    fn discount_within_range() {
        let rows = generate_lineitem_rows(200);
        for row in &rows {
            let disc = row["l_discount"].as_f64().expect("l_discount");
            assert!(
                (0.0..=0.10).contains(&disc),
                "discount {disc} out of range 0.0-0.10"
            );
        }
    }

    #[test]
    fn nation_and_region_consistent() {
        let rows = generate_lineitem_rows(500);
        for row in &rows {
            let nation = row["n_name"].as_str().expect("n_name");
            let region = row["r_name"].as_str().expect("r_name");
            let n_idx = NATIONS
                .iter()
                .position(|n| *n == nation)
                .expect("valid nation");
            let expected_region = REGIONS[(n_idx / 5).min(REGIONS.len() - 1)];
            assert_eq!(
                region, expected_region,
                "nation {nation} should map to {expected_region}"
            );
        }
    }
}
