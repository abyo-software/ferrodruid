// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Human-table and machine-JSON rendering of probe results.

use crate::probe::{ProbeKind, Section, Verdict};
use serde_json::{Value, json};

/// One executed (or skipped) probe with its verdict.
#[derive(Debug, Clone)]
pub struct ProbeResult {
    /// Probe name.
    pub name: String,
    /// Battery section.
    pub section: Section,
    /// Assertive or informational.
    pub kind: ProbeKind,
    /// The SQL that was (or would have been) submitted.
    pub sql: String,
    /// Evaluation verdict.
    pub verdict: Verdict,
}

/// Aggregate counts over a result set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    /// Assertive probes that matched.
    pub pass: usize,
    /// Assertive probes that diverged or failed transport.
    pub fail: usize,
    /// Probes skipped (fixture unavailable).
    pub skip: usize,
    /// Informational probe outcomes.
    pub info: usize,
}

impl Summary {
    /// Total number of probe results.
    #[must_use]
    pub fn total(&self) -> usize {
        self.pass + self.fail + self.skip + self.info
    }

    /// Process exit code for this outcome: 0 when nothing failed,
    /// 1 when at least one assertive probe failed.
    #[must_use]
    pub fn exit_code(&self) -> i32 {
        i32::from(self.fail > 0)
    }
}

/// Count verdicts.
#[must_use]
pub fn summarize(results: &[ProbeResult]) -> Summary {
    let mut s = Summary {
        pass: 0,
        fail: 0,
        skip: 0,
        info: 0,
    };
    for r in results {
        match &r.verdict {
            Verdict::Pass => s.pass += 1,
            Verdict::Fail { .. } => s.fail += 1,
            Verdict::Skip { .. } => s.skip += 1,
            Verdict::Info { .. } => s.info += 1,
        }
    }
    s
}

fn verdict_tag(v: &Verdict) -> &'static str {
    match v {
        Verdict::Pass => "PASS",
        Verdict::Fail { .. } => "FAIL",
        Verdict::Skip { .. } => "SKIP",
        Verdict::Info { .. } => "INFO",
    }
}

/// Render the human-readable report table.
#[must_use]
pub fn render_human(results: &[ProbeResult], url: &str) -> String {
    let name_w = results
        .iter()
        .map(|r| r.name.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let mut out = String::new();
    out.push_str(&format!(
        "ferro-compat-check — Druid-SQL compatibility battery against {url}\n"
    ));
    out.push_str(
        "(expected values are live-measured Apache Druid 30-36 behavior; \
         no Druid cluster is contacted)\n\n",
    );
    out.push_str(&format!(
        "{:<name_w$}  {:<10}  {:<6}\n",
        "PROBE", "SECTION", "RESULT"
    ));
    out.push_str(&format!("{}\n", "-".repeat(name_w + 22)));
    for r in results {
        out.push_str(&format!(
            "{:<name_w$}  {:<10}  {:<6}\n",
            r.name,
            r.section.label(),
            verdict_tag(&r.verdict)
        ));
        match &r.verdict {
            Verdict::Fail {
                expected,
                actual,
                hint,
            } => {
                out.push_str(&format!("    sql      : {}\n", r.sql));
                out.push_str(&format!("    expected : {expected}\n"));
                out.push_str(&format!("    actual   : {actual}\n"));
                if let Some(h) = hint {
                    out.push_str(&format!("    hint     : {h}\n"));
                }
            }
            Verdict::Skip { reason } => {
                out.push_str(&format!("    reason   : {reason}\n"));
            }
            Verdict::Info { note } => {
                out.push_str(&format!("    note     : {note}\n"));
            }
            Verdict::Pass => {}
        }
    }
    let s = summarize(results);
    out.push_str(&format!(
        "\nSummary: {} pass / {} fail / {} skip / {} informational ({} probes)\n",
        s.pass,
        s.fail,
        s.skip,
        s.info,
        s.total()
    ));
    out.push_str(if s.fail == 0 {
        "Result: COMPATIBLE — every assertive probe matched live-verified Apache Druid behavior.\n"
    } else {
        "Result: DIVERGENT — at least one assertive probe did not match Apache Druid behavior.\n"
    });
    out
}

/// Render the machine-readable JSON report.
#[must_use]
pub fn render_json(results: &[ProbeResult], url: &str, prefix: &str) -> Value {
    let s = summarize(results);
    let probes: Vec<Value> = results
        .iter()
        .map(|r| {
            let mut obj = json!({
                "name": r.name,
                "section": r.section.label(),
                "kind": match r.kind {
                    ProbeKind::Assertive => "assertive",
                    ProbeKind::Informational => "informational",
                },
                "sql": r.sql,
                "result": verdict_tag(&r.verdict),
            });
            if let Some(m) = obj.as_object_mut() {
                match &r.verdict {
                    Verdict::Fail {
                        expected,
                        actual,
                        hint,
                    } => {
                        m.insert("expected".to_string(), json!(expected));
                        m.insert("actual".to_string(), json!(actual));
                        if let Some(h) = hint {
                            m.insert("hint".to_string(), json!(h));
                        }
                    }
                    Verdict::Skip { reason } => {
                        m.insert("reason".to_string(), json!(reason));
                    }
                    Verdict::Info { note } => {
                        m.insert("note".to_string(), json!(note));
                    }
                    Verdict::Pass => {}
                }
            }
            obj
        })
        .collect();
    json!({
        "tool": "ferro-compat-check",
        "url": url,
        "datasource_prefix": prefix,
        "probes": probes,
        "summary": {
            "pass": s.pass,
            "fail": s.fail,
            "skip": s.skip,
            "informational": s.info,
            "total": s.total(),
        },
        "compatible": s.fail == 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn results() -> Vec<ProbeResult> {
        vec![
            ProbeResult {
                name: "a".to_string(),
                section: Section::Ping,
                kind: ProbeKind::Assertive,
                sql: "SELECT 1".to_string(),
                verdict: Verdict::Pass,
            },
            ProbeResult {
                name: "b".to_string(),
                section: Section::Null,
                kind: ProbeKind::Assertive,
                sql: "SELECT 2".to_string(),
                verdict: Verdict::Fail {
                    expected: "[1]".to_string(),
                    actual: "[2]".to_string(),
                    hint: Some("wire types differ".to_string()),
                },
            },
            ProbeResult {
                name: "c".to_string(),
                section: Section::Grains,
                kind: ProbeKind::Informational,
                sql: "SELECT 3".to_string(),
                verdict: Verdict::Info {
                    note: "recorded".to_string(),
                },
            },
        ]
    }

    #[test]
    fn summary_counts_and_exit_code() {
        let rs = results();
        let s = summarize(&rs);
        assert_eq!((s.pass, s.fail, s.skip, s.info), (1, 1, 0, 1));
        assert_eq!(s.total(), 3);
        assert_eq!(s.exit_code(), 1);
        let all_pass = vec![rs[0].clone()];
        assert_eq!(summarize(&all_pass).exit_code(), 0);
    }

    #[test]
    fn human_report_shows_fail_detail_and_verdict() {
        let text = render_human(&results(), "http://x:8888");
        assert!(text.contains("FAIL"));
        assert!(text.contains("expected : [1]"));
        assert!(text.contains("hint     : wire types differ"));
        assert!(text.contains("1 pass / 1 fail / 0 skip / 1 informational"));
        assert!(text.contains("Result: DIVERGENT"));
    }

    #[test]
    fn json_report_shape() {
        let v = render_json(&results(), "http://x:8888", "compatcheck");
        assert_eq!(v["tool"], "ferro-compat-check");
        assert_eq!(v["compatible"], false);
        assert_eq!(v["summary"]["fail"], 1);
        assert_eq!(v["probes"][1]["result"], "FAIL");
        assert_eq!(v["probes"][1]["hint"], "wire types differ");
        assert_eq!(v["probes"][2]["kind"], "informational");
    }
}
