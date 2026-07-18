// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Load rules — drop / load / broadcast by interval or period for FerroDruid.
//!
//! Rules are evaluated in order (first match wins), matching the behavior of
//! the original Druid coordinator rule chain.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Duration, Utc};
use ferrodruid_common::types::Interval;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by load-rule evaluation.
#[derive(Debug, Error)]
pub enum LoadRuleError {
    /// The rule definition is invalid.
    #[error("invalid load rule: {0}")]
    Invalid(String),
}

/// Convenience result alias.
pub type Result<T> = std::result::Result<T, LoadRuleError>;

// ---------------------------------------------------------------------------
// RuleAction
// ---------------------------------------------------------------------------

/// The action dictated by a load rule for a given segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuleAction {
    /// Load the segment onto one or more tiers with per-tier replica counts.
    ///
    /// DD R24: this carries the FULL `tier_replicants` map (e.g.
    /// `{"cold":1,"hot":1}`), not a single tier. Collapsing a multi-tier rule to
    /// one tier both under-replicated multi-tier datasources and — when the
    /// chosen tier had 0 replicas (`{"cold":0,"hot":1}`) — could drop the only
    /// copy. The map is deterministically ordered (`BTreeMap`).
    Load {
        /// Per-tier replica counts (tier name -> replica count). The coordinator
        /// enforces every tier with a positive count independently.
        tier_replicants: BTreeMap<String, usize>,
    },
    /// Drop (unload) the segment.
    Drop,
    /// Broadcast the segment to all nodes.
    Broadcast,
    /// The rule did not match; try the next rule.
    NoMatch,
}

// ---------------------------------------------------------------------------
// LoadRule
// ---------------------------------------------------------------------------

/// A single load rule in a Druid-compatible rule chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LoadRule {
    /// Keep segments loaded forever on the specified tiers.
    #[serde(rename = "loadForever")]
    LoadForever {
        /// Tier name to replica count mapping.
        tier_replicants: HashMap<String, usize>,
    },
    /// Load segments whose interval overlaps the given interval.
    #[serde(rename = "loadByInterval")]
    LoadByInterval {
        /// ISO-8601 interval string (`start/end`).
        interval: String,
        /// Tier name to replica count mapping.
        tier_replicants: HashMap<String, usize>,
    },
    /// Load segments younger than the given ISO-8601 duration.
    #[serde(rename = "loadByPeriod")]
    LoadByPeriod {
        /// ISO-8601 duration (e.g. `"P30D"`, `"P1Y"`).
        period: String,
        /// Whether to include future segments.
        #[serde(default)]
        include_future: Option<bool>,
        /// Tier name to replica count mapping.
        tier_replicants: HashMap<String, usize>,
    },
    /// Drop all segments unconditionally.
    #[serde(rename = "dropForever")]
    DropForever,
    /// Drop segments whose interval overlaps the given interval.
    #[serde(rename = "dropByInterval")]
    DropByInterval {
        /// ISO-8601 interval string (`start/end`).
        interval: String,
    },
    /// Drop segments older than the given ISO-8601 duration.
    #[serde(rename = "dropByPeriod")]
    DropByPeriod {
        /// ISO-8601 duration (e.g. `"P90D"`).
        period: String,
        /// Whether to include future segments.
        #[serde(default)]
        include_future: Option<bool>,
    },
    /// Broadcast segments to all nodes forever.
    #[serde(rename = "broadcastForever")]
    BroadcastForever,
    /// Broadcast segments whose interval overlaps the given interval.
    #[serde(rename = "broadcastByInterval")]
    BroadcastByInterval {
        /// ISO-8601 interval string (`start/end`).
        interval: String,
    },
    /// Broadcast segments younger than the given ISO-8601 duration.
    #[serde(rename = "broadcastByPeriod")]
    BroadcastByPeriod {
        /// ISO-8601 duration (e.g. `"P7D"`).
        period: String,
        /// Whether to include future segments.
        #[serde(default)]
        include_future: Option<bool>,
    },
}

impl LoadRule {
    /// Check if this rule applies to a segment with the given interval at
    /// the given reference time, and return the resulting action.
    pub fn applies_to_segment(
        &self,
        segment_interval: &Interval,
        now: &DateTime<Utc>,
    ) -> Result<RuleAction> {
        match self {
            // -- Load variants --
            Self::LoadForever { tier_replicants } => Ok(load_action(tier_replicants)),
            Self::LoadByInterval {
                interval,
                tier_replicants,
            } => {
                let rule_iv = parse_interval(interval)?;
                if intervals_overlap(segment_interval, &rule_iv) {
                    Ok(load_action(tier_replicants))
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }
            Self::LoadByPeriod {
                period,
                include_future,
                tier_replicants,
            } => {
                let dur = parse_period(period)?;
                let cutoff = *now - dur;
                let include_fut = include_future.unwrap_or(true);
                if segment_in_period(segment_interval, &cutoff, now, include_fut) {
                    Ok(load_action(tier_replicants))
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }

            // -- Drop variants --
            Self::DropForever => Ok(RuleAction::Drop),
            Self::DropByInterval { interval } => {
                let rule_iv = parse_interval(interval)?;
                if intervals_overlap(segment_interval, &rule_iv) {
                    Ok(RuleAction::Drop)
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }
            Self::DropByPeriod {
                period,
                include_future,
            } => {
                let dur = parse_period(period)?;
                let cutoff = *now - dur;
                let include_fut = include_future.unwrap_or(false);
                // Drop if the segment is OUTSIDE the retention window
                if !segment_in_period(segment_interval, &cutoff, now, include_fut) {
                    Ok(RuleAction::Drop)
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }

            // -- Broadcast variants --
            Self::BroadcastForever => Ok(RuleAction::Broadcast),
            Self::BroadcastByInterval { interval } => {
                let rule_iv = parse_interval(interval)?;
                if intervals_overlap(segment_interval, &rule_iv) {
                    Ok(RuleAction::Broadcast)
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }
            Self::BroadcastByPeriod {
                period,
                include_future,
            } => {
                let dur = parse_period(period)?;
                let cutoff = *now - dur;
                let include_fut = include_future.unwrap_or(true);
                if segment_in_period(segment_interval, &cutoff, now, include_fut) {
                    Ok(RuleAction::Broadcast)
                } else {
                    Ok(RuleAction::NoMatch)
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Rule chain evaluation
// ---------------------------------------------------------------------------

/// Evaluate a chain of rules for a segment. First match wins.
///
/// If no rule matches, the default action is [`RuleAction::Drop`] (matching
/// Druid behavior where unmatched segments are not loaded).
pub fn evaluate_rules(
    rules: &[LoadRule],
    segment_interval: &Interval,
    now: &DateTime<Utc>,
) -> Result<RuleAction> {
    for rule in rules {
        let action = rule.applies_to_segment(segment_interval, now)?;
        if action != RuleAction::NoMatch {
            return Ok(action);
        }
    }
    // Default: drop if no rule matched
    Ok(RuleAction::Drop)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a [`RuleAction::Load`] carrying the FULL tier-replica map (DD R24).
///
/// The whole `tier_replicants` map is preserved (deterministically ordered) so
/// the coordinator can enforce every requested tier; collapsing to a single tier
/// under-replicates multi-tier datasources and, when the picked tier has 0
/// replicas, can drop the segment's only copy. An empty map falls back to a
/// single `_default_tier` replica.
fn load_action(tier_replicants: &HashMap<String, usize>) -> RuleAction {
    if tier_replicants.is_empty() {
        let mut defaults = BTreeMap::new();
        defaults.insert("_default_tier".to_string(), 1);
        return RuleAction::Load {
            tier_replicants: defaults,
        };
    }
    RuleAction::Load {
        tier_replicants: tier_replicants
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect(),
    }
}

/// Parse an ISO-8601 interval string `start/end`.
fn parse_interval(s: &str) -> Result<Interval> {
    let parts: Vec<&str> = s.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(LoadRuleError::Invalid(format!(
            "expected ISO interval 'start/end', got: {s}"
        )));
    }
    let start = parse_interval_bound(parts[0]).map_err(|e| {
        LoadRuleError::Invalid(format!("invalid interval start '{}': {e}", parts[0]))
    })?;
    let end = parse_interval_bound(parts[1])
        .map_err(|e| LoadRuleError::Invalid(format!("invalid interval end '{}': {e}", parts[1])))?;
    // DD R48: reject an inverted / zero-length interval rather than let a
    // malformed metadata rule (start >= end) match segments via intervals_overlap.
    if start >= end {
        return Err(LoadRuleError::Invalid(format!(
            "interval start must be before end: {s}"
        )));
    }
    Ok(Interval { start, end })
}

/// Parse one interval bound, accepting both RFC3339 timestamps and the bare
/// `YYYY-MM-DD` date form (interpreted as UTC midnight) that Druid load/drop
/// rules commonly use (DD R47).
fn parse_interval_bound(s: &str) -> std::result::Result<DateTime<Utc>, String> {
    let s = s.trim();
    if let Ok(dt) = s.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }
    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        && let Some(naive) = date.and_hms_opt(0, 0, 0)
    {
        return Ok(naive.and_utc());
    }
    Err(format!("not an RFC3339 timestamp or YYYY-MM-DD date: {s}"))
}

/// Parse a simplified ISO-8601 duration string (supports `P<n>D`, `P<n>M`,
/// `P<n>Y`, `PT<n>H`).
fn parse_period(s: &str) -> Result<Duration> {
    let s = s.trim();
    if !s.starts_with('P') {
        return Err(LoadRuleError::Invalid(format!(
            "period must start with 'P': {s}"
        )));
    }
    let rest = &s[1..];

    // DD R47: non-panicking chrono constructors + checked multiplication so a
    // malformed metadata rule (e.g. `P9223372036854775807Y`) returns an error
    // instead of panicking (debug) or wrapping to a wrong retention (release).
    let overflow = || LoadRuleError::Invalid(format!("period out of range: {s}"));

    // PT<n>H
    if let Some(h) = rest.strip_prefix('T').and_then(|r| r.strip_suffix('H')) {
        let hours: i64 = h
            .parse()
            .map_err(|_| LoadRuleError::Invalid(format!("invalid hours in period: {s}")))?;
        return Duration::try_hours(hours).ok_or_else(overflow);
    }

    // P<n>D
    if let Some(d) = rest.strip_suffix('D') {
        let days: i64 = d
            .parse()
            .map_err(|_| LoadRuleError::Invalid(format!("invalid days in period: {s}")))?;
        return Duration::try_days(days).ok_or_else(overflow);
    }

    // P<n>M (approximate: 30 days per month)
    if let Some(m) = rest.strip_suffix('M') {
        let months: i64 = m
            .parse()
            .map_err(|_| LoadRuleError::Invalid(format!("invalid months in period: {s}")))?;
        return months
            .checked_mul(30)
            .and_then(Duration::try_days)
            .ok_or_else(overflow);
    }

    // P<n>Y (approximate: 365 days per year)
    if let Some(y) = rest.strip_suffix('Y') {
        let years: i64 = y
            .parse()
            .map_err(|_| LoadRuleError::Invalid(format!("invalid years in period: {s}")))?;
        return years
            .checked_mul(365)
            .and_then(Duration::try_days)
            .ok_or_else(overflow);
    }

    Err(LoadRuleError::Invalid(format!(
        "unsupported period format: {s}"
    )))
}

/// Check whether two intervals overlap ([start, end) semantics).
fn intervals_overlap(a: &Interval, b: &Interval) -> bool {
    a.start < b.end && b.start < a.end
}

/// Check whether a segment falls within the retention window.
fn segment_in_period(
    segment: &Interval,
    cutoff: &DateTime<Utc>,
    now: &DateTime<Utc>,
    include_future: bool,
) -> bool {
    // Segment end must be after the cutoff (i.e. the segment is recent enough)
    if segment.end <= *cutoff {
        return false;
    }
    // If not including future, segment start must be before now
    if !include_future && segment.start > *now {
        return false;
    }
    true
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn iv(start: &str, end: &str) -> Interval {
        Interval {
            start: start.parse().unwrap(),
            end: end.parse().unwrap(),
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2024, 6, 15, 0, 0, 0).unwrap()
    }

    #[test]
    fn load_forever_always_matches() {
        let rule = LoadRule::LoadForever {
            tier_replicants: HashMap::from([("_default_tier".into(), 2)]),
        };
        let seg = iv("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("_default_tier".to_string(), 2)]),
            }
        );
    }

    #[test]
    fn drop_forever_always_matches() {
        let rule = LoadRule::DropForever;
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::Drop);
    }

    #[test]
    fn load_by_interval_match_and_miss() {
        let rule = LoadRule::LoadByInterval {
            interval: "2024-01-01T00:00:00Z/2024-07-01T00:00:00Z".into(),
            tier_replicants: HashMap::from([("hot".into(), 1)]),
        };

        // Overlapping segment
        let seg = iv("2024-03-01T00:00:00Z", "2024-04-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("hot".to_string(), 1)]),
            }
        );

        // Non-overlapping segment
        let seg = iv("2023-01-01T00:00:00Z", "2023-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::NoMatch);
    }

    #[test]
    fn drop_by_interval() {
        let rule = LoadRule::DropByInterval {
            interval: "2023-01-01T00:00:00Z/2023-07-01T00:00:00Z".into(),
        };

        let seg = iv("2023-03-01T00:00:00Z", "2023-04-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::Drop);

        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::NoMatch);
    }

    #[test]
    fn load_by_period_30_days() {
        let rule = LoadRule::LoadByPeriod {
            period: "P30D".into(),
            include_future: None,
            tier_replicants: HashMap::from([("hot".into(), 2)]),
        };
        let ref_now = now();

        // Recent segment (within 30 days)
        let seg = iv("2024-06-01T00:00:00Z", "2024-06-10T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &ref_now).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("hot".to_string(), 2)]),
            }
        );

        // Old segment (older than 30 days)
        let seg = iv("2024-01-01T00:00:00Z", "2024-01-15T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &ref_now).unwrap();
        assert_eq!(action, RuleAction::NoMatch);
    }

    #[test]
    fn drop_by_period_90_days() {
        let rule = LoadRule::DropByPeriod {
            period: "P90D".into(),
            include_future: None,
        };
        let ref_now = now(); // 2024-06-15

        // Old segment (older than 90 days from now = before ~2024-03-17)
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &ref_now).unwrap();
        assert_eq!(action, RuleAction::Drop);

        // Recent segment (within 90 days)
        let seg = iv("2024-05-01T00:00:00Z", "2024-06-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &ref_now).unwrap();
        assert_eq!(action, RuleAction::NoMatch);
    }

    #[test]
    fn broadcast_forever() {
        let rule = LoadRule::BroadcastForever;
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::Broadcast);
    }

    #[test]
    fn broadcast_by_interval() {
        let rule = LoadRule::BroadcastByInterval {
            interval: "2024-01-01T00:00:00Z/2024-07-01T00:00:00Z".into(),
        };
        let seg = iv("2024-03-01T00:00:00Z", "2024-04-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(action, RuleAction::Broadcast);
    }

    #[test]
    fn rule_chain_first_match_wins() {
        let rules = vec![
            // Rule 1: Load recent 30 days on hot tier
            LoadRule::LoadByPeriod {
                period: "P30D".into(),
                include_future: None,
                tier_replicants: HashMap::from([("hot".into(), 2)]),
            },
            // Rule 2: Load last year on cold tier
            LoadRule::LoadByPeriod {
                period: "P365D".into(),
                include_future: None,
                tier_replicants: HashMap::from([("cold".into(), 1)]),
            },
            // Rule 3: Drop everything else
            LoadRule::DropForever,
        ];

        let ref_now = now();

        // Recent segment -> hot
        let seg = iv("2024-06-01T00:00:00Z", "2024-06-10T00:00:00Z");
        let action = evaluate_rules(&rules, &seg, &ref_now).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("hot".to_string(), 2)]),
            }
        );

        // 6-month old segment -> cold
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = evaluate_rules(&rules, &seg, &ref_now).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("cold".to_string(), 1)]),
            }
        );

        // Ancient segment -> drop
        let seg = iv("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z");
        let action = evaluate_rules(&rules, &seg, &ref_now).unwrap();
        assert_eq!(action, RuleAction::Drop);
    }

    #[test]
    fn empty_rules_default_drop() {
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = evaluate_rules(&[], &seg, &now()).unwrap();
        assert_eq!(action, RuleAction::Drop);
    }

    #[test]
    fn serde_roundtrip() {
        let rule = LoadRule::LoadByPeriod {
            period: "P30D".into(),
            include_future: Some(true),
            tier_replicants: HashMap::from([("hot".into(), 2)]),
        };
        let json = serde_json::to_string(&rule).expect("ser");
        let back: LoadRule = serde_json::from_str(&json).expect("de");
        // Verify it still works
        let seg = iv("2024-06-01T00:00:00Z", "2024-06-10T00:00:00Z");
        let action = back.applies_to_segment(&seg, &now()).unwrap();
        assert!(matches!(action, RuleAction::Load { .. }));
    }

    #[test]
    fn invalid_interval_error() {
        let rule = LoadRule::LoadByInterval {
            interval: "not-an-interval".into(),
            tier_replicants: HashMap::new(),
        };
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let err = rule.applies_to_segment(&seg, &now()).unwrap_err();
        assert!(err.to_string().contains("invalid"));
    }

    #[test]
    fn invalid_period_error() {
        let rule = LoadRule::LoadByPeriod {
            period: "not-a-period".into(),
            include_future: None,
            tier_replicants: HashMap::new(),
        };
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let err = rule.applies_to_segment(&seg, &now()).unwrap_err();
        assert!(err.to_string().contains("period must start with 'P'"));
    }

    #[test]
    fn parse_period_variants() {
        assert_eq!(parse_period("P30D").unwrap(), Duration::days(30));
        assert_eq!(parse_period("P3M").unwrap(), Duration::days(90));
        assert_eq!(parse_period("P1Y").unwrap(), Duration::days(365));
        assert_eq!(parse_period("PT24H").unwrap(), Duration::hours(24));
    }

    #[test]
    fn parse_period_overflow_rejected_not_panicked() {
        // DD R47: a malformed huge period must error, not panic (debug) or wrap
        // (release) via Duration::days(years * 365).
        assert!(parse_period("P9223372036854775807Y").is_err());
        assert!(parse_period("P9223372036854775807D").is_err());
        assert!(parse_period("P9223372036854775807M").is_err());
    }

    #[test]
    fn parse_interval_accepts_date_only_bounds() {
        // DD R47: Druid load/drop rules commonly use bare YYYY-MM-DD interval
        // bounds; these must parse (UTC midnight), not error.
        let iv = parse_interval("2024-01-01/2024-02-01").expect("date-only interval");
        assert!(iv.start < iv.end);
        // RFC3339 still works.
        assert!(parse_interval("2024-01-01T00:00:00Z/2024-02-01T00:00:00Z").is_ok());
        // A genuinely malformed bound is still rejected.
        assert!(parse_interval("2024-01-01/not-a-date").is_err());
    }

    #[test]
    fn parse_interval_rejects_inverted_and_zero_length() {
        // DD R48: start >= end must be rejected so a malformed metadata rule
        // cannot match segments via intervals_overlap.
        assert!(
            parse_interval("2024-02-01/2024-01-01").is_err(),
            "inverted interval must be rejected"
        );
        assert!(
            parse_interval("2024-01-01/2024-01-01").is_err(),
            "zero-length interval must be rejected"
        );
    }

    #[test]
    fn load_by_period_exclude_future() {
        let rule = LoadRule::LoadByPeriod {
            period: "P30D".into(),
            include_future: Some(false),
            tier_replicants: HashMap::from([("hot".into(), 1)]),
        };
        let ref_now = now(); // 2024-06-15

        // Future segment
        let seg = iv("2024-07-01T00:00:00Z", "2024-08-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &ref_now).unwrap();
        assert_eq!(action, RuleAction::NoMatch);
    }

    #[test]
    fn empty_tier_replicants_defaults() {
        let rule = LoadRule::LoadForever {
            tier_replicants: HashMap::new(),
        };
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let action = rule.applies_to_segment(&seg, &now()).unwrap();
        assert_eq!(
            action,
            RuleAction::Load {
                tier_replicants: BTreeMap::from([("_default_tier".to_string(), 1)]),
            }
        );
    }
}
