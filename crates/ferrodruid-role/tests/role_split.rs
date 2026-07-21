// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Integration tests for the role-split scaffold.
//!
//! These complement the in-crate unit tests by exercising the public
//! API surface a downstream binary (the per-role launchers under
//! `bins/`) sees. Wave 34.T introduced 3 dedicated roles; Wave 38.FF
//! extended this to the complete Druid 6-role topology (router /
//! overlord / middleManager). Together with
//! `crates/ferrodruid-role/src/lib.rs` they cover enum parse /
//! display / round-trip / dispatcher routing / banner content /
//! bind-address validation / Druid default ports / tier predicates.

use std::path::PathBuf;

use ferrodruid_role::{DispatchOutcome, Role, RoleConfig, RoleError, dispatch};

#[test]
fn cli_style_role_strings_round_trip() {
    // Exact strings the per-role binary prints at boot must parse
    // back to the same role. This is the contract the CLI relies on.
    // Wave 38.FF expands this set from 4 to 7 (the full Druid 6-role
    // topology + the all-in-one Standalone mode).
    let pairs = [
        ("broker", Role::Broker),
        ("historical", Role::Historical),
        ("coordinator", Role::Coordinator),
        ("router", Role::Router),
        ("overlord", Role::Overlord),
        ("middlemanager", Role::MiddleManager),
        ("standalone", Role::Standalone),
    ];
    for (s, expected) in pairs {
        let parsed: Role = s.parse().expect("known role string parses");
        assert_eq!(parsed, expected);
        assert_eq!(parsed.as_str(), s);
    }
}

#[test]
fn role_config_banner_carries_role_and_address_for_every_role() {
    for role in Role::all() {
        let cfg = RoleConfig::try_new(
            role,
            "127.0.0.1",
            8888,
            PathBuf::from("/tmp/ferrodruid-role-it"),
        )
        .expect("loopback bind parses");
        let banner = cfg.banner();
        assert!(banner.contains(&format!("role={role}")), "{banner}");
        assert!(banner.contains("127.0.0.1:8888"), "{banner}");
    }
}

#[test]
fn dispatcher_routes_each_role_to_documented_outcome() {
    // Standalone -> launch the existing single-binary boot.
    assert_eq!(
        dispatch(Role::Standalone),
        DispatchOutcome::LaunchSingleBinary,
    );
    // Dedicated roles -> log + exit OK in v0.1.x. This contract is
    // what the per-role binaries rely on; if it changes, every
    // launcher in `bins/` must change with it. Wave 38.FF added
    // router / overlord / middleManager to this set.
    for role in [
        Role::Broker,
        Role::Historical,
        Role::Coordinator,
        Role::Router,
        Role::Overlord,
        Role::MiddleManager,
    ] {
        assert_eq!(dispatch(role), DispatchOutcome::LogAndExitOk, "{role}");
    }
}

#[test]
fn six_role_topology_default_ports_are_unique() {
    // Each Druid-mapped role has its own default port; the binaries
    // can therefore co-host on the same loopback without collisions.
    let mut seen: Vec<u16> = [
        Role::Broker,
        Role::Historical,
        Role::Coordinator,
        Role::Router,
        Role::Overlord,
        Role::MiddleManager,
    ]
    .iter()
    .map(|r| r.druid_default_port().expect("druid role has default port"))
    .collect();
    seen.sort_unstable();
    let original_len = seen.len();
    seen.dedup();
    assert_eq!(seen.len(), original_len, "default ports must be unique");
}

#[test]
fn role_config_for_new_roles_carries_role_name_in_banner() {
    // Wave 38.FF added router / overlord / middleManager. Their
    // banners must contain the exact `role=<canonical>` token the
    // per-role binary prints, so log scrapers can identify them.
    for (role, expected_token) in [
        (Role::Router, "role=router"),
        (Role::Overlord, "role=overlord"),
        (Role::MiddleManager, "role=middlemanager"),
    ] {
        let cfg = RoleConfig::try_new(
            role,
            "127.0.0.1",
            role.druid_default_port().unwrap_or(8888),
            PathBuf::from("/tmp/ferrodruid-role-w2"),
        )
        .expect("loopback bind parses");
        assert!(cfg.banner().contains(expected_token), "{}", cfg.banner());
    }
}

#[test]
fn role_config_rejects_invalid_bind_with_typed_error() {
    let err = RoleConfig::try_new(
        Role::Coordinator,
        "definitely::not::an::ip",
        9999,
        PathBuf::from("/tmp/ferrodruid-role-it-bad-bind"),
    )
    .expect_err("garbage bind must fail");
    match err {
        RoleError::InvalidBind(s, _) => assert_eq!(s, "definitely::not::an::ip"),
        other => panic!("expected InvalidBind, got {other:?}"),
    }
}
