// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! Druid BasicAuthorizer compatible RBAC for FerroDruid.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::collections::HashMap;

use ferrodruid_auth::AuthenticatedUser;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Authorization errors.
#[derive(Debug, Error)]
pub enum AuthzError {
    /// Permission denied.
    #[error("permission denied: {0}")]
    Denied(String),
    /// Role not found.
    #[error("role not found: {0}")]
    RoleNotFound(String),
}

/// Druid resource types.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ResourceType {
    /// Data source resource.
    Datasource,
    /// Config resource.
    Config,
    /// State resource.
    State,
    /// External schema resource.
    ExternalSchema,
}

/// Druid actions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Action {
    /// Read access.
    Read,
    /// Write access.
    Write,
}

/// A resource + action authorization check.
pub struct ResourceAction {
    /// Type of resource being accessed.
    pub resource_type: ResourceType,
    /// Name of the specific resource (e.g. datasource name).
    pub resource_name: String,
    /// Action being performed.
    pub action: Action,
}

/// An RBAC permission entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Permission {
    /// Resource type this permission covers.
    pub resource_type: ResourceType,
    /// Resource name pattern — exact match or `"*"` for wildcard.
    pub resource_pattern: String,
    /// Action this permission grants.
    pub action: Action,
}

/// A role with a set of permissions (legacy API).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name.
    pub name: String,
    /// Permissions granted to this role.
    pub permissions: Vec<Permission>,
}

// ---------------------------------------------------------------------------
// Authorizer
// ---------------------------------------------------------------------------

/// Role-based authorizer that checks user roles against permission grants.
#[derive(Debug)]
pub struct Authorizer {
    role_permissions: HashMap<String, Vec<Permission>>,
}

impl Default for Authorizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Authorizer {
    /// Create a new empty authorizer (denies everything by default).
    pub fn new() -> Self {
        Self {
            role_permissions: HashMap::new(),
        }
    }

    /// Add a permission grant for a role.
    pub fn add_permission(&mut self, role: &str, permission: Permission) {
        self.role_permissions
            .entry(role.to_string())
            .or_default()
            .push(permission);
    }

    /// Check if a user (with roles) is authorized for a resource+action.
    ///
    /// Returns `true` if at least one of the user's roles has a matching
    /// permission.
    pub fn authorize(&self, user: &AuthenticatedUser, check: &ResourceAction) -> bool {
        for role in &user.roles {
            if let Some(perms) = self.role_permissions.get(role) {
                for perm in perms {
                    if perm.resource_type == check.resource_type
                        && perm.action == check.action
                        && pattern_matches(&perm.resource_pattern, &check.resource_name)
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Add a built-in admin role that grants full access to all resource types
    /// and actions.
    pub fn with_admin_role(mut self) -> Self {
        let admin_perms = vec![
            Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "*".to_string(),
                action: Action::Read,
            },
            Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "*".to_string(),
                action: Action::Write,
            },
            Permission {
                resource_type: ResourceType::Config,
                resource_pattern: "*".to_string(),
                action: Action::Read,
            },
            Permission {
                resource_type: ResourceType::Config,
                resource_pattern: "*".to_string(),
                action: Action::Write,
            },
            Permission {
                resource_type: ResourceType::State,
                resource_pattern: "*".to_string(),
                action: Action::Read,
            },
            Permission {
                resource_type: ResourceType::State,
                resource_pattern: "*".to_string(),
                action: Action::Write,
            },
            Permission {
                resource_type: ResourceType::ExternalSchema,
                resource_pattern: "*".to_string(),
                action: Action::Read,
            },
            Permission {
                resource_type: ResourceType::ExternalSchema,
                resource_pattern: "*".to_string(),
                action: Action::Write,
            },
        ];

        self.role_permissions
            .insert("admin".to_string(), admin_perms);
        self
    }
}

/// Check whether a pattern matches a resource name.
///
/// `"*"` matches everything. Otherwise an exact match is required.
fn pattern_matches(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    // Support trailing wildcard: "wiki*" matches "wiki", "wikipedia", etc.
    if let Some(prefix) = pattern.strip_suffix('*') {
        return name.starts_with(prefix);
    }
    pattern == name
}

/// Check whether a user's roles grant access to a resource (legacy API).
pub fn check_access(
    roles: &[Role],
    resource_type: ResourceType,
    resource_name: &str,
    action: Action,
) -> Result<(), AuthzError> {
    for role in roles {
        for perm in &role.permissions {
            if perm.resource_type == resource_type
                && perm.action == action
                && pattern_matches(&perm.resource_pattern, resource_name)
            {
                return Ok(());
            }
        }
    }
    Err(AuthzError::Denied(format!(
        "{resource_type:?}:{resource_name}:{action:?}"
    )))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_user(username: &str, roles: &[&str]) -> AuthenticatedUser {
        AuthenticatedUser {
            username: username.to_string(),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            must_change_password: false,
        }
    }

    #[test]
    fn admin_can_access_everything() {
        let authz = Authorizer::new().with_admin_role();
        let admin = make_user("root", &["admin"]);

        assert!(authz.authorize(
            &admin,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Read,
            }
        ));
        assert!(authz.authorize(
            &admin,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Write,
            }
        ));
        assert!(authz.authorize(
            &admin,
            &ResourceAction {
                resource_type: ResourceType::Config,
                resource_name: "anything".into(),
                action: Action::Write,
            }
        ));
        assert!(authz.authorize(
            &admin,
            &ResourceAction {
                resource_type: ResourceType::State,
                resource_name: "cluster".into(),
                action: Action::Read,
            }
        ));
        assert!(authz.authorize(
            &admin,
            &ResourceAction {
                resource_type: ResourceType::ExternalSchema,
                resource_name: "ext".into(),
                action: Action::Read,
            }
        ));
    }

    #[test]
    fn datasource_read_only() {
        let mut authz = Authorizer::new();
        authz.add_permission(
            "reader",
            Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "wiki".to_string(),
                action: Action::Read,
            },
        );

        let reader = make_user("alice", &["reader"]);

        // Can read wiki.
        assert!(authz.authorize(
            &reader,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Read,
            }
        ));

        // Cannot write wiki.
        assert!(!authz.authorize(
            &reader,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Write,
            }
        ));

        // Cannot read other datasource.
        assert!(!authz.authorize(
            &reader,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "clicks".into(),
                action: Action::Read,
            }
        ));
    }

    #[test]
    fn user_without_role_denied() {
        let authz = Authorizer::new().with_admin_role();
        let nobody = make_user("nobody", &[]);

        assert!(!authz.authorize(
            &nobody,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Read,
            }
        ));
    }

    #[test]
    fn wildcard_pattern_matching() {
        let mut authz = Authorizer::new();
        authz.add_permission(
            "analyst",
            Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "wiki*".to_string(),
                action: Action::Read,
            },
        );

        let analyst = make_user("bob", &["analyst"]);

        // Matches "wiki".
        assert!(authz.authorize(
            &analyst,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wiki".into(),
                action: Action::Read,
            }
        ));

        // Matches "wikipedia".
        assert!(authz.authorize(
            &analyst,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "wikipedia".into(),
                action: Action::Read,
            }
        ));

        // Does not match "clicks".
        assert!(!authz.authorize(
            &analyst,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "clicks".into(),
                action: Action::Read,
            }
        ));
    }

    #[test]
    fn star_wildcard_matches_all() {
        let mut authz = Authorizer::new();
        authz.add_permission(
            "viewer",
            Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "*".to_string(),
                action: Action::Read,
            },
        );

        let viewer = make_user("carol", &["viewer"]);

        assert!(authz.authorize(
            &viewer,
            &ResourceAction {
                resource_type: ResourceType::Datasource,
                resource_name: "anything".into(),
                action: Action::Read,
            }
        ));
    }

    #[test]
    fn legacy_check_access() {
        let roles = vec![Role {
            name: "reader".into(),
            permissions: vec![Permission {
                resource_type: ResourceType::Datasource,
                resource_pattern: "*".into(),
                action: Action::Read,
            }],
        }];

        assert!(check_access(&roles, ResourceType::Datasource, "wiki", Action::Read).is_ok());
        assert!(check_access(&roles, ResourceType::Datasource, "wiki", Action::Write).is_err());
    }
}
