// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Load-rule enforcement glue between the metadata store and the
//! `ferrodruid-loadrules` evaluation engine.
//!
//! The metadata store persists each data source's rule chain as a JSON array
//! (see [`ferrodruid_metadata::MetadataStore::get_rules`]). This module
//! deserializes those JSON values into [`LoadRule`]s and evaluates them against
//! a segment's interval to derive the desired placement: how many replicas go
//! to which tier, or whether the segment should be dropped.

use chrono::{DateTime, Utc};
use ferrodruid_common::types::Interval;
use ferrodruid_common::{DruidError, Result};
use ferrodruid_loadrules::{LoadRule, RuleAction, evaluate_rules};

/// The desired placement for a single segment, derived from its rule chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DesiredPlacement {
    /// Load the segment onto one or more tiers with per-tier replica counts.
    ///
    /// DD R24: carries the full `tier_replicants` map so multi-tier rules
    /// (e.g. `{"cold":1,"hot":1}`) are honored and a 0-replica tier never
    /// causes the only copy to be dropped.
    Load {
        /// Per-tier replica counts (tier name -> replica count), deterministically
        /// ordered. The coordinator enforces every tier with a positive count.
        tier_replicants: std::collections::BTreeMap<String, usize>,
    },
    /// Broadcast the segment to every server (all tiers).
    Broadcast,
    /// The segment should not be loaded anywhere (dropped / unused).
    Drop,
}

/// Parse a metadata rule-chain JSON array into typed [`LoadRule`]s.
///
/// Unknown or malformed rule entries cause an error rather than being silently
/// skipped, so a bad rule chain is surfaced instead of producing wrong
/// placement decisions.
pub fn parse_rules(raw: &[serde_json::Value]) -> Result<Vec<LoadRule>> {
    let mut out = Vec::with_capacity(raw.len());
    for (i, value) in raw.iter().enumerate() {
        let rule: LoadRule = serde_json::from_value(value.clone())
            .map_err(|e| DruidError::Config(format!("invalid load rule at index {i}: {e}")))?;
        out.push(rule);
    }
    Ok(out)
}

/// Evaluate a parsed rule chain for a segment interval and map the resulting
/// [`RuleAction`] onto a [`DesiredPlacement`].
///
/// An empty rule chain yields [`DesiredPlacement::Drop`], matching the
/// `ferrodruid-loadrules` default (unmatched segments are not loaded).
pub fn desired_placement(
    rules: &[LoadRule],
    segment_interval: &Interval,
    now: &DateTime<Utc>,
) -> Result<DesiredPlacement> {
    let action = evaluate_rules(rules, segment_interval, now)
        .map_err(|e| DruidError::Config(format!("rule evaluation failed: {e}")))?;
    Ok(match action {
        RuleAction::Load { tier_replicants } => DesiredPlacement::Load { tier_replicants },
        RuleAction::Broadcast => DesiredPlacement::Broadcast,
        RuleAction::Drop | RuleAction::NoMatch => DesiredPlacement::Drop,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn iv(start: &str, end: &str) -> Interval {
        Interval {
            start: start.parse().expect("start"),
            end: end.parse().expect("end"),
        }
    }

    fn now() -> DateTime<Utc> {
        "2024-06-15T00:00:00Z".parse().expect("now")
    }

    #[test]
    fn parse_load_and_drop_rules() {
        let raw = vec![
            json!({"type": "loadByPeriod", "period": "P30D",
                   "tier_replicants": {"hot": 2}}),
            json!({"type": "dropForever"}),
        ];
        let rules = parse_rules(&raw).expect("parse");
        assert_eq!(rules.len(), 2);
    }

    #[test]
    fn invalid_rule_errors() {
        let raw = vec![json!({"type": "notARealRule"})];
        let err = parse_rules(&raw).expect_err("should fail");
        assert!(err.to_string().contains("invalid load rule"));
    }

    #[test]
    fn load_recent_segment_to_tier() {
        let raw = vec![
            json!({"type": "loadByPeriod", "period": "P30D",
                   "tier_replicants": {"hot": 3}}),
            json!({"type": "dropForever"}),
        ];
        let rules = parse_rules(&raw).expect("parse");
        let seg = iv("2024-06-01T00:00:00Z", "2024-06-10T00:00:00Z");
        let placement = desired_placement(&rules, &seg, &now()).expect("placement");
        assert_eq!(
            placement,
            DesiredPlacement::Load {
                tier_replicants: std::collections::BTreeMap::from([("hot".to_string(), 3)]),
            }
        );
    }

    #[test]
    fn drop_old_segment() {
        let raw = vec![
            json!({"type": "loadByPeriod", "period": "P30D",
                   "tier_replicants": {"hot": 1}}),
            json!({"type": "dropForever"}),
        ];
        let rules = parse_rules(&raw).expect("parse");
        let seg = iv("2020-01-01T00:00:00Z", "2020-02-01T00:00:00Z");
        let placement = desired_placement(&rules, &seg, &now()).expect("placement");
        assert_eq!(placement, DesiredPlacement::Drop);
    }

    #[test]
    fn broadcast_maps_through() {
        let raw = vec![json!({"type": "broadcastForever"})];
        let rules = parse_rules(&raw).expect("parse");
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let placement = desired_placement(&rules, &seg, &now()).expect("placement");
        assert_eq!(placement, DesiredPlacement::Broadcast);
    }

    #[test]
    fn empty_rules_drop() {
        let seg = iv("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z");
        let placement = desired_placement(&[], &seg, &now()).expect("placement");
        assert_eq!(placement, DesiredPlacement::Drop);
    }
}
