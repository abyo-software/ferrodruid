// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! The curated probe battery and its self-describing fixtures.
//!
//! Every SQL shape and every expected value here is lifted from the
//! live Druid ⇄ FerroDruid diff harness
//! (`crates/ferrodruid-rest/tests/druid_diff_test.rs`) and its
//! committed evidence (`tests/druid-compat/RESULTS_*`, live re-run
//! 2026-07-11 against apache/druid 30.0.1-36.0.0). The expectations
//! are therefore MEASURED Apache Druid behavior, not guesses — which
//! is what lets this tool verify Druid-SQL compatibility without a
//! Druid cluster present.
//!
//! Three tiny inline fixtures make the battery zero-setup:
//! - `<prefix>_wiki`: the harness's 10-row `wikipedia_compat` dataset
//!   (rollup at hour grain, longSum metrics);
//! - `<prefix>_null`: the harness's 7-row `nulltest` dataset (typed
//!   double dimension with genuine SQL NULLs, rollup off);
//! - `<prefix>_rollup`: the harness's 6-raw-row `rolluptest` dataset
//!   (rolls to 4 stored rows at hour grain).

use crate::probe::{Expectation, Fixture, Probe, ProbeKind, Section};
use serde_json::{Value, json};

/// Resolved fixture datasource names, derived from the `--datasource`
/// prefix (default `compatcheck`).
#[derive(Debug, Clone)]
pub struct FixtureNames {
    /// 10-row wikipedia-style dataset.
    pub wiki: String,
    /// 7-row null-semantics dataset.
    pub null: String,
    /// 6-raw-row rollup dataset.
    pub rollup: String,
}

impl FixtureNames {
    /// Build the three fixture names from a prefix.
    #[must_use]
    pub fn from_prefix(prefix: &str) -> Self {
        Self {
            wiki: format!("{prefix}_wiki"),
            null: format!("{prefix}_null"),
            rollup: format!("{prefix}_rollup"),
        }
    }

    /// Datasource name for a fixture (`None` for [`Fixture::None`]).
    #[must_use]
    pub fn name_of(&self, fixture: Fixture) -> Option<&str> {
        match fixture {
            Fixture::None => None,
            Fixture::Wiki => Some(&self.wiki),
            Fixture::Null => Some(&self.null),
            Fixture::Rollup => Some(&self.rollup),
        }
    }
}

// ---------------------------------------------------------------------------
// Fixture ingestion specs (inline index_parallel tasks)
// ---------------------------------------------------------------------------

/// The 10 wikipedia-style rows (same data as the harness's
/// `sample_ingestion_spec.json`, JSONL).
const WIKI_ROWS: &str = concat!(
    r##"{"timestamp":"2024-01-01T00:00:00Z","page":"Main_Page","user":"Alice","language":"en","city":"San_Francisco","namespace":"Main","channel":"#en.wikipedia","added":100,"deleted":10,"delta":90}"##,
    "\n",
    r##"{"timestamp":"2024-01-01T01:00:00Z","page":"Talk:Main_Page","user":"Bob","language":"en","city":"New_York","namespace":"Talk","channel":"#en.wikipedia","added":50,"deleted":5,"delta":45}"##,
    "\n",
    r##"{"timestamp":"2024-01-01T02:00:00Z","page":"Accueil","user":"Claude","language":"fr","city":"Paris","namespace":"Main","channel":"#fr.wikipedia","added":200,"deleted":20,"delta":180}"##,
    "\n",
    r##"{"timestamp":"2024-01-01T03:00:00Z","page":"Hauptseite","user":"Diana","language":"de","city":"Berlin","namespace":"Main","channel":"#de.wikipedia","added":150,"deleted":30,"delta":120}"##,
    "\n",
    r##"{"timestamp":"2024-01-01T12:00:00Z","page":"Main_Page","user":"Eve","language":"en","city":"London","namespace":"Main","channel":"#en.wikipedia","added":75,"deleted":25,"delta":50}"##,
    "\n",
    r##"{"timestamp":"2024-01-02T00:00:00Z","page":"Main_Page","user":"Alice","language":"en","city":"San_Francisco","namespace":"Main","channel":"#en.wikipedia","added":120,"deleted":15,"delta":105}"##,
    "\n",
    r##"{"timestamp":"2024-01-02T06:00:00Z","page":"Portal:Current_events","user":"Frank","language":"en","city":"Tokyo","namespace":"Portal","channel":"#en.wikipedia","added":300,"deleted":50,"delta":250}"##,
    "\n",
    r##"{"timestamp":"2024-01-02T12:00:00Z","page":"Accueil","user":"Claude","language":"fr","city":"Paris","namespace":"Main","channel":"#fr.wikipedia","added":180,"deleted":40,"delta":140}"##,
    "\n",
    r##"{"timestamp":"2024-01-03T00:00:00Z","page":"Main_Page","user":"Grace","language":"en","city":"Sydney","namespace":"Main","channel":"#en.wikipedia","added":90,"deleted":10,"delta":80}"##,
    "\n",
    r##"{"timestamp":"2024-01-03T08:00:00Z","page":"Pagina_principale","user":"Heidi","language":"it","city":"Rome","namespace":"Main","channel":"#it.wikipedia","added":110,"deleted":20,"delta":90}"##,
);

/// The 7 null-semantics rows (harness `nulltest` dataset; measured
/// live against Druid 35.0.1 + 36.0.0).
const NULL_ROWS: &str = concat!(
    r#"{"timestamp":"2024-01-01T00:00:00Z","site_id":"site_a","device_id":"d1","value":10.0}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T01:00:00Z","site_id":"site_a","device_id":"d2","value":20.0}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T02:00:00Z","site_id":"site_a","device_id":"d2","value":null}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T03:00:00Z","site_id":"site_b","device_id":"d1","value":30.0}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T04:00:00Z","site_id":"site_b","device_id":null,"value":null}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T05:00:00Z","site_id":"site_b","device_id":"d3","value":null}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T06:00:00Z","site_id":"site_c","device_id":"d3","value":null}"#,
);

/// The 6 raw rollup rows (harness `rolluptest` dataset; two (hour,
/// site) pairs merge, so hour-grain rollup stores 4 rows).
const ROLLUP_ROWS: &str = concat!(
    r#"{"timestamp":"2024-01-01T00:05:00Z","site_id":"site_a","value":10}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T00:40:00Z","site_id":"site_a","value":5}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T00:10:00Z","site_id":"site_b","value":7}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T01:20:00Z","site_id":"site_a","value":3}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T01:30:00Z","site_id":"site_b","value":4}"#,
    "\n",
    r#"{"timestamp":"2024-01-01T01:45:00Z","site_id":"site_b","value":6}"#,
);

fn index_parallel_spec(
    datasource: &str,
    data: &str,
    dimensions: Value,
    metrics: Value,
    query_granularity: &str,
    rollup: bool,
) -> Value {
    json!({
        "type": "index_parallel",
        "spec": {
            "dataSchema": {
                "dataSource": datasource,
                "timestampSpec": {"column": "timestamp", "format": "iso"},
                "dimensionsSpec": {"dimensions": dimensions},
                "metricsSpec": metrics,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": "DAY",
                    "queryGranularity": query_granularity,
                    "rollup": rollup
                }
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {"type": "inline", "data": data},
                "inputFormat": {"type": "json"}
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": 5_000_000,
                "maxRowsInMemory": 25_000
            }
        }
    })
}

/// Inline `index_parallel` task spec for a fixture. `None` for
/// [`Fixture::None`].
#[must_use]
pub fn ingestion_spec(fixture: Fixture, names: &FixtureNames) -> Option<Value> {
    match fixture {
        Fixture::None => None,
        Fixture::Wiki => Some(index_parallel_spec(
            &names.wiki,
            WIKI_ROWS,
            json!(["page", "user", "language", "city", "namespace", "channel"]),
            json!([
                {"type": "count", "name": "count"},
                {"type": "longSum", "name": "added", "fieldName": "added"},
                {"type": "longSum", "name": "deleted", "fieldName": "deleted"},
                {"type": "longSum", "name": "delta", "fieldName": "delta"}
            ]),
            "HOUR",
            true,
        )),
        Fixture::Null => Some(index_parallel_spec(
            &names.null,
            NULL_ROWS,
            json!(["site_id", "device_id", {"type": "double", "name": "value"}]),
            json!([]),
            "NONE",
            false,
        )),
        Fixture::Rollup => Some(index_parallel_spec(
            &names.rollup,
            ROLLUP_ROWS,
            json!(["site_id"]),
            json!([
                {"type": "count", "name": "cnt"},
                {"type": "longSum", "name": "value_sum", "fieldName": "value"}
            ]),
            "hour",
            true,
        )),
    }
}

// ---------------------------------------------------------------------------
// Expected-value helpers
// ---------------------------------------------------------------------------

/// The 10 hour-grain bucket timestamps of the wiki fixture (each holds
/// exactly one row).
const TEN_HOURLY: [&str; 10] = [
    "2024-01-01T00:00:00.000Z",
    "2024-01-01T01:00:00.000Z",
    "2024-01-01T02:00:00.000Z",
    "2024-01-01T03:00:00.000Z",
    "2024-01-01T12:00:00.000Z",
    "2024-01-02T00:00:00.000Z",
    "2024-01-02T06:00:00.000Z",
    "2024-01-02T12:00:00.000Z",
    "2024-01-03T00:00:00.000Z",
    "2024-01-03T08:00:00.000Z",
];

/// Build `[{"__timestamp": ts, "count": n}, ...]` grain rows.
fn grain_rows(buckets: &[(&str, i64)]) -> Value {
    Value::Array(
        buckets
            .iter()
            .map(|(ts, n)| json!({"__timestamp": ts, "count": n}))
            .collect(),
    )
}

fn ten_hourly_grain_rows() -> Value {
    grain_rows(&TEN_HOURLY.map(|ts| (ts, 1)))
}

fn superset_grain_sql(ds: &str, period: &str) -> String {
    format!(
        "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), '{period}') AS __timestamp, \
         COUNT(*) AS \"count\" FROM {ds} \
         WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') \
         GROUP BY 1 ORDER BY 1"
    )
}

fn superset_week_variant_sql(ds: &str, outer_step: i64) -> String {
    format!(
        "SELECT TIME_SHIFT(TIME_FLOOR(TIME_SHIFT(CAST(__time AS TIMESTAMP), 'P1D', 1), \
         'P1W'), 'P1D', {outer_step}) AS __timestamp, COUNT(*) AS \"count\" FROM {ds} \
         WHERE __time >= TIME_PARSE('2024-01-01T00:00:00') GROUP BY 1 ORDER BY 1"
    )
}

// ---------------------------------------------------------------------------
// The battery
// ---------------------------------------------------------------------------

/// Build the full probe battery for the given fixture names.
///
/// Probe names mirror the diff-harness query names where one exists,
/// so a result line here can be cross-referenced against the committed
/// harness evidence directly.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn probe_catalog(names: &FixtureNames) -> Vec<Probe> {
    let wiki = names.wiki.as_str();
    let nulls = names.null.as_str();
    let rollup = names.rollup.as_str();
    let mut probes: Vec<Probe> = Vec::with_capacity(48);

    let assertive =
        |name: &str, section: Section, fixture: Fixture, sql: String, exp: Value| Probe {
            name: name.to_string(),
            section,
            kind: ProbeKind::Assertive,
            sql,
            fixture,
            expect: Expectation::Rows(exp),
            note: None,
        };

    // --- ping -------------------------------------------------------------
    probes.push(assertive(
        "ping_select_1",
        Section::Ping,
        Fixture::None,
        "SELECT 1 AS ok".to_string(),
        json!([{"ok": 1}]),
    ));

    // --- aggregates (harness Section 1) ------------------------------------
    probes.push(assertive(
        "count_star",
        Section::Aggregates,
        Fixture::Wiki,
        format!("SELECT COUNT(*) AS cnt FROM {wiki}"),
        json!([{"cnt": 10}]),
    ));
    probes.push(assertive(
        "min_max_added",
        Section::Aggregates,
        Fixture::Wiki,
        format!("SELECT MIN(\"added\") AS mn, MAX(\"added\") AS mx FROM {wiki}"),
        json!([{"mn": 50, "mx": 300}]),
    ));
    probes.push(assertive(
        "groupby_page_topn",
        Section::Aggregates,
        Fixture::Wiki,
        format!(
            "SELECT \"page\", COUNT(*) AS cnt FROM {wiki} \
             GROUP BY \"page\" ORDER BY cnt DESC LIMIT 10"
        ),
        json!([
            {"page": "Main_Page", "cnt": 4},
            {"page": "Accueil", "cnt": 2},
            {"page": "Hauptseite", "cnt": 1},
            {"page": "Pagina_principale", "cnt": 1},
            {"page": "Portal:Current_events", "cnt": 1},
            {"page": "Talk:Main_Page", "cnt": 1}
        ]),
    ));
    probes.push(assertive(
        "filter_lang_en",
        Section::Aggregates,
        Fixture::Wiki,
        format!("SELECT COUNT(*) AS cnt FROM {wiki} WHERE \"language\" = 'en'"),
        json!([{"cnt": 6}]),
    ));
    probes.push(assertive(
        "sum_delta",
        Section::Aggregates,
        Fixture::Wiki,
        format!("SELECT SUM(\"delta\") AS total_delta FROM {wiki}"),
        json!([{"total_delta": 1150}]),
    ));

    // --- superset (harness Section 6) ---------------------------------------
    probes.push(assertive(
        "superset_infoschema_tables",
        Section::Superset,
        Fixture::Wiki,
        format!(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_SCHEMA = 'druid' AND TABLE_NAME = '{wiki}' \
             ORDER BY TABLE_NAME"
        ),
        json!([{"TABLE_NAME": wiki}]),
    ));
    probes.push(assertive(
        "superset_infoschema_columns",
        Section::Superset,
        Fixture::Wiki,
        format!(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = '{wiki}' ORDER BY COLUMN_NAME"
        ),
        json!([
            {"COLUMN_NAME": "__time"},
            {"COLUMN_NAME": "added"},
            {"COLUMN_NAME": "channel"},
            {"COLUMN_NAME": "city"},
            {"COLUMN_NAME": "count"},
            {"COLUMN_NAME": "deleted"},
            {"COLUMN_NAME": "delta"},
            {"COLUMN_NAME": "language"},
            {"COLUMN_NAME": "namespace"},
            {"COLUMN_NAME": "page"},
            {"COLUMN_NAME": "user"}
        ]),
    ));
    let hourly_tc = || -> Value {
        Value::Array(
            TEN_HOURLY
                .iter()
                .map(|ts| json!({"t": ts, "c": 1}))
                .collect(),
        )
    };
    probes.push(assertive(
        "superset_time_floor_hour",
        Section::Superset,
        Fixture::Wiki,
        format!(
            "SELECT TIME_FLOOR(__time, 'PT1H') AS t, COUNT(*) AS c \
             FROM {wiki} GROUP BY 1 ORDER BY 1"
        ),
        hourly_tc(),
    ));
    probes.push(assertive(
        "superset_date_trunc_hour",
        Section::Superset,
        Fixture::Wiki,
        format!(
            "SELECT DATE_TRUNC('hour', __time) AS t, COUNT(*) AS c \
             FROM {wiki} GROUP BY 1 ORDER BY 1"
        ),
        hourly_tc(),
    ));
    probes.push(Probe {
        name: "superset_preview_limit".to_string(),
        section: Section::Superset,
        kind: ProbeKind::Assertive,
        sql: format!("SELECT * FROM {wiki} ORDER BY __time LIMIT 100"),
        fixture: Fixture::Wiki,
        expect: Expectation::RowsOrdered(preview_rows()),
        note: Some(
            "pins ISO-8601 __time strings, projection column order, and rollup \
             metric placement on the SELECT * wire"
                .to_string(),
        ),
    });
    probes.push(Probe {
        name: "superset_agg_before_dim_alias".to_string(),
        section: Section::Superset,
        kind: ProbeKind::Assertive,
        sql: format!(
            "SELECT COUNT(*) AS c, \"language\" AS s FROM {wiki} \
             GROUP BY \"language\" ORDER BY c DESC, s ASC"
        ),
        fixture: Fixture::Wiki,
        expect: Expectation::RowsOrdered(json!([
            {"c": 6, "s": "en"},
            {"c": 2, "s": "fr"},
            {"c": 1, "s": "de"},
            {"c": 1, "s": "it"}
        ])),
        note: Some(
            "aggregate projected BEFORE an aliased dimension: wire columns must \
             match the SELECT list order exactly (positional pydruid/Superset \
             contract)"
                .to_string(),
        ),
    });
    probes.push(Probe {
        name: "superset_explain_plan_for".to_string(),
        section: Section::Superset,
        kind: ProbeKind::Informational,
        sql: format!("EXPLAIN PLAN FOR SELECT \"language\", COUNT(*) AS c FROM {wiki} GROUP BY 1"),
        fixture: Fixture::Wiki,
        expect: Expectation::NonEmptyArray,
        note: Some(
            "EXPLAIN bodies are engine-internal: FerroDruid returns its own \
             native-query JSON, Apache Druid returns its Calcite plan wrapper. \
             Byte parity is impossible by construction — recorded, not asserted"
                .to_string(),
        ),
    });

    // --- null semantics (harness Section 7; measured live vs Druid 35/36) ---
    probes.push(assertive(
        "null_avg_by_site",
        Section::Null,
        Fixture::Null,
        format!(
            "SELECT site_id, AVG(\"value\") AS avg_v FROM {nulls} \
             GROUP BY site_id ORDER BY site_id"
        ),
        json!([
            {"site_id": "site_a", "avg_v": 15.0},
            {"site_id": "site_b", "avg_v": 30.0},
            {"site_id": "site_c", "avg_v": null}
        ]),
    ));
    probes.push(assertive(
        "null_count_col_by_site",
        Section::Null,
        Fixture::Null,
        format!(
            "SELECT site_id, COUNT(\"value\") AS c FROM {nulls} \
             GROUP BY site_id ORDER BY site_id"
        ),
        json!([
            {"site_id": "site_a", "c": 2},
            {"site_id": "site_b", "c": 1},
            {"site_id": "site_c", "c": 0}
        ]),
    ));
    probes.push(assertive(
        "null_count_distinct_device",
        Section::Null,
        Fixture::Null,
        format!("SELECT COUNT(DISTINCT device_id) AS dc FROM {nulls}"),
        // BIGINT on the wire — the integer typing is part of the assertion.
        json!([{"dc": 3}]),
    ));
    probes.push(assertive(
        "null_approx_count_distinct_device",
        Section::Null,
        Fixture::Null,
        format!("SELECT APPROX_COUNT_DISTINCT(device_id) AS adc FROM {nulls}"),
        json!([{"adc": 3}]),
    ));
    probes.push(assertive(
        "null_round_avg_by_site",
        Section::Null,
        Fixture::Null,
        format!(
            "SELECT site_id, ROUND(AVG(\"value\"), 1) AS r FROM {nulls} \
             GROUP BY site_id ORDER BY site_id"
        ),
        json!([
            {"site_id": "site_a", "r": 15.0},
            {"site_id": "site_b", "r": 30.0},
            {"site_id": "site_c", "r": null}
        ]),
    ));
    probes.push(assertive(
        "null_sum_div_count_star_by_site",
        Section::Null,
        Fixture::Null,
        format!(
            "SELECT site_id, SUM(\"value\") / COUNT(*) AS r FROM {nulls} \
             GROUP BY site_id ORDER BY site_id"
        ),
        // SUM over an all-null group is SQL NULL (not 0) and arithmetic
        // over the null aggregate propagates null — Druid parity.
        json!([
            {"site_id": "site_a", "r": 10.0},
            {"site_id": "site_b", "r": 10.0},
            {"site_id": "site_c", "r": null}
        ]),
    ));

    // --- Superset time grains (harness Section 8) ---------------------------
    // Fine grains: the fixture rows sit on exact hours, so every grain
    // from PT1S to PT1H buckets identically (10 buckets of 1).
    for period in [
        "PT1S", "PT5S", "PT30S", "PT1M", "PT5M", "PT10M", "PT15M", "PT30M", "PT1H",
    ] {
        probes.push(assertive(
            &format!("grain_{}", period.to_lowercase()),
            Section::Grains,
            Fixture::Wiki,
            superset_grain_sql(wiki, period),
            ten_hourly_grain_rows(),
        ));
    }
    probes.push(assertive(
        "grain_pt6h",
        Section::Grains,
        Fixture::Wiki,
        superset_grain_sql(wiki, "PT6H"),
        grain_rows(&[
            ("2024-01-01T00:00:00.000Z", 4),
            ("2024-01-01T12:00:00.000Z", 1),
            ("2024-01-02T00:00:00.000Z", 1),
            ("2024-01-02T06:00:00.000Z", 1),
            ("2024-01-02T12:00:00.000Z", 1),
            ("2024-01-03T00:00:00.000Z", 1),
            ("2024-01-03T06:00:00.000Z", 1),
        ]),
    ));
    let daily = [
        ("2024-01-01T00:00:00.000Z", 5),
        ("2024-01-02T00:00:00.000Z", 3),
        ("2024-01-03T00:00:00.000Z", 2),
    ];
    probes.push(assertive(
        "grain_p1d",
        Section::Grains,
        Fixture::Wiki,
        superset_grain_sql(wiki, "P1D"),
        grain_rows(&daily),
    ));
    // 2024-01-01 is a Monday: ISO week grain buckets on Monday.
    probes.push(assertive(
        "grain_p1w",
        Section::Grains,
        Fixture::Wiki,
        superset_grain_sql(wiki, "P1W"),
        grain_rows(&[("2024-01-01T00:00:00.000Z", 10)]),
    ));
    for period in ["P1M", "P3M", "P1Y"] {
        probes.push(assertive(
            &format!("grain_{}", period.to_lowercase()),
            Section::Grains,
            Fixture::Wiki,
            superset_grain_sql(wiki, period),
            grain_rows(&[("2024-01-01T00:00:00.000Z", 10)]),
        ));
    }
    probes.push(Probe {
        name: "grain_week_ending_saturday".to_string(),
        section: Section::Grains,
        kind: ProbeKind::Informational,
        sql: superset_week_variant_sql(wiki, 5),
        fixture: Fixture::Wiki,
        expect: Expectation::Rows(grain_rows(&[("2024-01-06T00:00:00.000Z", 10)])),
        note: Some(
            "known residual: FerroDruid fails this grain closed by design (the \
             bucket label is the Saturday ENDING the bucket, which a \
             floor-to-origin granularity cannot produce); Apache Druid returns \
             2024-01-06. Recorded, not asserted"
                .to_string(),
        ),
    });
    probes.push(assertive(
        "grain_week_starting_sunday",
        Section::Grains,
        Fixture::Wiki,
        superset_week_variant_sql(wiki, -1),
        grain_rows(&[("2023-12-31T00:00:00.000Z", 10)]),
    ));
    probes.push(assertive(
        "grain_where_cast_time_parse_date",
        Section::Grains,
        Fixture::Wiki,
        format!(
            "SELECT TIME_FLOOR(CAST(__time AS TIMESTAMP), 'P1D') AS __timestamp, \
             COUNT(*) AS \"count\" FROM {wiki} \
             WHERE __time >= CAST(TIME_PARSE('2024-01-01') AS DATE) \
             GROUP BY 1 ORDER BY 1"
        ),
        grain_rows(&daily),
    ));

    // --- ingestion-time rollup (harness Section 9) --------------------------
    probes.push(assertive(
        "rollup_count_star",
        Section::Rollup,
        Fixture::Rollup,
        format!("SELECT COUNT(*) AS cnt FROM {rollup}"),
        // 6 raw rows rolled to 4 stored rows — this is the probe that
        // catches "rollup silently ignored at ingest".
        json!([{"cnt": 4}]),
    ));
    probes.push(assertive(
        "rollup_groupby_site_sums",
        Section::Rollup,
        Fixture::Rollup,
        format!(
            "SELECT site_id, SUM(\"value_sum\") AS total_value, SUM(\"cnt\") AS raw_rows \
             FROM {rollup} GROUP BY site_id ORDER BY site_id"
        ),
        json!([
            {"site_id": "site_a", "total_value": 18, "raw_rows": 3},
            {"site_id": "site_b", "total_value": 17, "raw_rows": 3}
        ]),
    ));
    probes.push(Probe {
        name: "rollup_preview_all".to_string(),
        section: Section::Rollup,
        kind: ProbeKind::Assertive,
        sql: format!("SELECT * FROM {rollup} ORDER BY __time"),
        fixture: Fixture::Rollup,
        expect: Expectation::RowsUnordered(json!([
            {"__time": "2024-01-01T00:00:00.000Z", "site_id": "site_a", "cnt": 2, "value_sum": 15},
            {"__time": "2024-01-01T00:00:00.000Z", "site_id": "site_b", "cnt": 1, "value_sum": 7},
            {"__time": "2024-01-01T01:00:00.000Z", "site_id": "site_a", "cnt": 1, "value_sum": 3},
            {"__time": "2024-01-01T01:00:00.000Z", "site_id": "site_b", "cnt": 2, "value_sum": 10}
        ])),
        note: Some(
            "rows sharing an hour bucket have engine-unspecified relative order \
             (matches the harness's shape-compare convention), so the row set is \
             compared as a multiset"
                .to_string(),
        ),
    });

    probes
}

/// The exact 10 preview rows (`SELECT * ... ORDER BY __time`) both
/// engines returned byte-identically in the 2026-07-11 live run.
fn preview_rows() -> Value {
    let mk = |time: &str,
              page: &str,
              user: &str,
              language: &str,
              city: &str,
              namespace: &str,
              channel: &str,
              added: i64,
              deleted: i64,
              delta: i64| {
        json!({
            "__time": time, "page": page, "user": user, "language": language,
            "city": city, "namespace": namespace, "channel": channel,
            "added": added, "count": 1, "deleted": deleted, "delta": delta
        })
    };
    json!([
        mk(
            "2024-01-01T00:00:00.000Z",
            "Main_Page",
            "Alice",
            "en",
            "San_Francisco",
            "Main",
            "#en.wikipedia",
            100,
            10,
            90
        ),
        mk(
            "2024-01-01T01:00:00.000Z",
            "Talk:Main_Page",
            "Bob",
            "en",
            "New_York",
            "Talk",
            "#en.wikipedia",
            50,
            5,
            45
        ),
        mk(
            "2024-01-01T02:00:00.000Z",
            "Accueil",
            "Claude",
            "fr",
            "Paris",
            "Main",
            "#fr.wikipedia",
            200,
            20,
            180
        ),
        mk(
            "2024-01-01T03:00:00.000Z",
            "Hauptseite",
            "Diana",
            "de",
            "Berlin",
            "Main",
            "#de.wikipedia",
            150,
            30,
            120
        ),
        mk(
            "2024-01-01T12:00:00.000Z",
            "Main_Page",
            "Eve",
            "en",
            "London",
            "Main",
            "#en.wikipedia",
            75,
            25,
            50
        ),
        mk(
            "2024-01-02T00:00:00.000Z",
            "Main_Page",
            "Alice",
            "en",
            "San_Francisco",
            "Main",
            "#en.wikipedia",
            120,
            15,
            105
        ),
        mk(
            "2024-01-02T06:00:00.000Z",
            "Portal:Current_events",
            "Frank",
            "en",
            "Tokyo",
            "Portal",
            "#en.wikipedia",
            300,
            50,
            250
        ),
        mk(
            "2024-01-02T12:00:00.000Z",
            "Accueil",
            "Claude",
            "fr",
            "Paris",
            "Main",
            "#fr.wikipedia",
            180,
            40,
            140
        ),
        mk(
            "2024-01-03T00:00:00.000Z",
            "Main_Page",
            "Grace",
            "en",
            "Sydney",
            "Main",
            "#en.wikipedia",
            90,
            10,
            80
        ),
        mk(
            "2024-01-03T08:00:00.000Z",
            "Pagina_principale",
            "Heidi",
            "it",
            "Rome",
            "Main",
            "#it.wikipedia",
            110,
            20,
            90
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn names() -> FixtureNames {
        FixtureNames::from_prefix("compatcheck")
    }

    #[test]
    fn probe_names_are_unique() {
        let catalog = probe_catalog(&names());
        let mut seen = HashSet::new();
        for p in &catalog {
            assert!(
                seen.insert(p.name.clone()),
                "duplicate probe name {}",
                p.name
            );
        }
        // 1 ping + 5 aggregates + 7 superset + 6 null + 18 grains + 3 rollup.
        assert_eq!(catalog.len(), 40);
    }

    #[test]
    fn every_user_selectable_section_has_probes() {
        let catalog = probe_catalog(&names());
        for section in ["ping", "aggregates", "superset", "null", "grains", "rollup"] {
            let sec = Section::parse(section).expect("parse");
            assert!(
                catalog.iter().any(|p| p.section == sec),
                "no probes in section {section}"
            );
        }
    }

    #[test]
    fn known_divergent_probes_are_informational_only() {
        let catalog = probe_catalog(&names());
        let informational: Vec<&str> = catalog
            .iter()
            .filter(|p| p.kind == ProbeKind::Informational)
            .map(|p| p.name.as_str())
            .collect();
        assert_eq!(
            informational,
            vec!["superset_explain_plan_for", "grain_week_ending_saturday"],
            "exactly the two known-divergent surfaces must be informational"
        );
    }

    #[test]
    fn fixture_specs_are_well_formed_index_parallel_tasks() {
        let n = names();
        for fixture in [Fixture::Wiki, Fixture::Null, Fixture::Rollup] {
            let spec = ingestion_spec(fixture, &n).expect("spec");
            assert_eq!(spec["type"], "index_parallel");
            let ds = spec["spec"]["dataSchema"]["dataSource"]
                .as_str()
                .expect("dataSource");
            assert_eq!(Some(ds), n.name_of(fixture));
            let data = spec["spec"]["ioConfig"]["inputSource"]["data"]
                .as_str()
                .expect("inline data");
            // Every line of the inline payload must be valid JSON.
            for line in data.lines() {
                let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
                assert!(parsed.is_ok(), "bad JSONL line in {fixture:?}: {line}");
            }
        }
        assert!(ingestion_spec(Fixture::None, &n).is_none());
    }

    #[test]
    fn rollup_fixture_rolls_six_raw_rows_to_four() {
        // The rollup probes encode 6 raw rows -> 4 stored rows; keep the
        // fixture and the expectation in lockstep.
        let n = names();
        let spec = ingestion_spec(Fixture::Rollup, &n).expect("spec");
        let data = spec["spec"]["ioConfig"]["inputSource"]["data"]
            .as_str()
            .expect("inline data");
        assert_eq!(data.lines().count(), 6);
        assert_eq!(
            spec["spec"]["dataSchema"]["granularitySpec"]["rollup"],
            true
        );
        let catalog = probe_catalog(&n);
        let count_probe = catalog
            .iter()
            .find(|p| p.name == "rollup_count_star")
            .expect("probe");
        if let Expectation::Rows(v) = &count_probe.expect {
            assert_eq!(v[0]["cnt"], 4);
        } else {
            panic!("rollup_count_star must be an exact-rows expectation");
        }
    }

    #[test]
    fn probes_only_reference_their_fixture_datasource() {
        let n = names();
        for p in probe_catalog(&n) {
            match p.fixture {
                Fixture::None => {}
                f => {
                    let ds = n.name_of(f).expect("name");
                    assert!(
                        p.sql.contains(ds),
                        "probe {} does not reference its fixture {ds}",
                        p.name
                    );
                }
            }
        }
    }
}
