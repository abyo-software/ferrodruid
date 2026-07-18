// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! End-to-end JOIN test: plan a Druid SQL `LEFT JOIN LOOKUP(...)` and execute
//! the lowered join against a real segment, verifying the joined rows.
//!
//! This exercises the full path: SQL parse -> plan (producing a `PlannedJoin`)
//! -> query-layer `execute_join_scan` over a segment + lookup manager.

use std::collections::HashMap;

use ferrodruid_bitmap::DruidBitmap;
use ferrodruid_dict::FrontCodedDictionary;
use ferrodruid_lookup::{LookupManager, LookupTable};
use ferrodruid_query::scan::ScanQuery;
use ferrodruid_query::{JoinRight, execute_join};
use ferrodruid_segment::Interval;
use ferrodruid_segment::SegmentData;
use ferrodruid_segment::column::{ColumnData, StringColumnData};
use ferrodruid_sql::{ColumnSchema, DataSourceSchema, parse_druid_sql, plan_sql};

use ferrodruid_common::types::{ColumnType, DataSource};

/// Build a `sales` segment with a `city` dimension and `revenue` metric.
///
/// Rows: (city=tokyo, revenue=10), (city=osaka, revenue=20),
/// (city=tokyo, revenue=30), (city=kyoto, revenue=40).
fn build_sales_segment() -> SegmentData {
    // dictionary sorted: kyoto=0, osaka=1, tokyo=2
    let dict = FrontCodedDictionary::from_sorted(vec![
        "kyoto".to_string(),
        "osaka".to_string(),
        "tokyo".to_string(),
    ]);
    let encoded_values = vec![2u32, 1, 2, 0]; // tokyo, osaka, tokyo, kyoto
    let mut bm_kyoto = DruidBitmap::new();
    bm_kyoto.insert(3);
    let mut bm_osaka = DruidBitmap::new();
    bm_osaka.insert(1);
    let mut bm_tokyo = DruidBitmap::new();
    bm_tokyo.insert(0);
    bm_tokyo.insert(2);
    let city_col = ColumnData::String(StringColumnData {
        dictionary: dict,
        encoded_values,
        bitmap_indexes: vec![bm_kyoto, bm_osaka, bm_tokyo],
    });

    let mut columns = HashMap::new();
    columns.insert(
        "__time".to_string(),
        ColumnData::Long(vec![100, 100, 100, 100]),
    );
    columns.insert("city".to_string(), city_col);
    columns.insert(
        "revenue".to_string(),
        ColumnData::Double(vec![10.0, 20.0, 30.0, 40.0]),
    );

    SegmentData {
        version: 9,
        num_rows: 4,
        interval: Interval {
            start_millis: 0,
            end_millis: 1000,
        },
        dimensions: vec!["city".to_string()],
        metrics: vec!["revenue".to_string()],
        columns,
        time_sorted: false,
    }
}

fn sales_schema() -> DataSourceSchema {
    DataSourceSchema {
        name: "sales".to_string(),
        dimensions: vec![ColumnSchema {
            name: "city".to_string(),
            column_type: ColumnType::String,
        }],
        metrics: vec![ColumnSchema {
            name: "revenue".to_string(),
            column_type: ColumnType::Double,
        }],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    }
}

#[test]
fn left_join_lookup_end_to_end() {
    // SQL: enrich each sales row with the full city name via a lookup join.
    let sql = "SELECT city, l.v FROM sales \
               LEFT JOIN LOOKUP('city_full') AS l ON sales.city = l.k";
    let stmt = parse_druid_sql(sql).expect("parse");
    let planned = plan_sql(&stmt, &sales_schema()).expect("plan");

    assert_eq!(planned.joins.len(), 1, "one lowered join expected");
    let pj = &planned.joins[0];
    assert_eq!(pj.join.right_prefix, "l.");
    assert!(matches!(pj.join.right, JoinRight::Lookup { .. }));

    // Set up the lookup the SQL referenced.
    let lookups = LookupManager::new();
    let table = LookupTable::new("city_full".to_string(), "v1".to_string());
    table.put("tokyo".to_string(), "Tokyo".to_string());
    table.put("osaka".to_string(), "Osaka".to_string());
    // Note: "kyoto" intentionally absent to exercise LEFT-join null fill.
    lookups.register(table);

    // Materialise the left side via a full scan over the segment, then run the
    // lowered join executor.
    let segment = build_sales_segment();
    let left_scan = ScanQuery {
        data_source: DataSource::Table {
            name: "sales".to_string(),
        },
        intervals: vec!["1970-01-01T00:00:00.000Z/2100-01-01T00:00:00.000Z".to_string()],
        filter: None,
        virtual_columns: None,
        columns: None,
        limit: None,
        offset: None,
        order: Some("none".to_string()),
        result_format: None,
        context: None,
    };
    let left_rows = left_scan.execute(&segment).expect("scan").events;
    let joined = execute_join(&pj.join, &left_rows, &lookups).expect("join");

    // LEFT join keeps all 4 left rows.
    assert_eq!(joined.len(), 4);

    // tokyo rows enrich to "Tokyo".
    let tokyo: Vec<_> = joined
        .iter()
        .filter(|r| r.get("city") == Some(&serde_json::json!("tokyo")))
        .collect();
    assert_eq!(tokyo.len(), 2);
    assert!(
        tokyo
            .iter()
            .all(|r| r.get("l.v") == Some(&serde_json::json!("Tokyo")))
    );

    // kyoto has no lookup entry -> null right column (LEFT semantics).
    let kyoto = joined
        .iter()
        .find(|r| r.get("city") == Some(&serde_json::json!("kyoto")))
        .expect("kyoto row");
    assert_eq!(kyoto.get("l.v"), Some(&serde_json::Value::Null));
}

/// DD R10 A#1: a base-table LEFT JOIN must expose the *full* right column set,
/// not just the join key. Before the fix the lowered join's right
/// `column_names` was `[id]` (the key only), so `dim.region` was invisible.
#[test]
fn base_table_join_materialises_full_right_columns() {
    // The `dim` table has a join key `id` plus a non-key `region` column.
    let dim_schema = DataSourceSchema {
        name: "dim".to_string(),
        dimensions: vec![
            ColumnSchema {
                name: "id".to_string(),
                column_type: ColumnType::String,
            },
            ColumnSchema {
                name: "region".to_string(),
                column_type: ColumnType::String,
            },
        ],
        metrics: vec![],
        time_column: "__time".to_string(),
        join_schemas: Vec::new(),
    };
    let mut schema = DataSourceSchema {
        name: "sales".to_string(),
        dimensions: vec![ColumnSchema {
            name: "region_id".to_string(),
            column_type: ColumnType::String,
        }],
        metrics: vec![ColumnSchema {
            name: "amount".to_string(),
            column_type: ColumnType::Double,
        }],
        time_column: "__time".to_string(),
        join_schemas: vec![dim_schema],
    };

    let sql = "SELECT sales.amount, dim.region FROM sales \
               LEFT JOIN dim ON sales.region_id = dim.id";
    let stmt = parse_druid_sql(sql).expect("parse");
    let planned = plan_sql(&stmt, &schema).expect("plan");

    assert_eq!(planned.joins.len(), 1, "one lowered join expected");
    let column_names = match &planned.joins[0].join.right {
        JoinRight::Rows { column_names, .. } => column_names,
        other => panic!("expected base-table join to lower to Rows, got {other:?}"),
    };
    // The non-key `region` column must be present (this is the regression).
    assert!(
        column_names.iter().any(|c| c == "region"),
        "right column_names must include non-key `region`, got {column_names:?}"
    );
    assert!(column_names.iter().any(|c| c == "id"));

    // Without a known right schema we honestly fall back to the join key only.
    schema.join_schemas.clear();
    let planned_no_catalog = plan_sql(&stmt, &schema).expect("plan");
    match &planned_no_catalog.joins[0].join.right {
        JoinRight::Rows { column_names, .. } => {
            assert_eq!(column_names, &vec!["id".to_string()]);
        }
        other => panic!("expected Rows, got {other:?}"),
    }
}
