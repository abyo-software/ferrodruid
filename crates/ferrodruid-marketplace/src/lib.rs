// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

#![forbid(unsafe_code)]
#![deny(missing_docs)]

//! AWS Marketplace entitlement verification for the FerroDruid paid
//! **Container** product.
//!
//! # Why this crate exists
//!
//! FerroDruid's source stays Apache-2.0. The paid AWS Marketplace
//! listing sells a hardened, scanned, supported, one-click
//! distribution (the standard open-core model). For the **AMI**
//! product AWS meters usage automatically and no code is required. For
//! the **Container** product AWS requires the running container to
//! call [`RegisterUsage`] once on startup; the call returns an
//! entitlement decision (and a signature that can later anchor metered
//! usage). If the customer is not entitled — never subscribed,
//! subscription expired, wrong product code — the container is
//! expected to **fail closed** (refuse to serve) rather than run for
//! free.
//!
//! This crate implements exactly that startup gate plus a documented,
//! feature-gated hook for *future* usage-based pricing.
//!
//! # Feature flag: `marketplace-metering` (default **OFF**)
//!
//! - **OFF (default):** the crate builds with **no** AWS SDK
//!   dependency compiled, makes **no** network calls, and needs **no**
//!   AWS credentials. Everything in this module — config parsing,
//!   validation, the disable/bypass path, the [`MockVerifier`], and the
//!   error type — is fully exercisable in offline unit tests. This is
//!   what the open-source single-binary build links against, so the
//!   default UX is byte-identical to a build that never knew this crate
//!   existed.
//! - **ON:** compiles the real [`AwsEntitlementVerifier`] which calls
//!   `RegisterUsage` via the official `aws-sdk-marketplacemetering`
//!   crate, plus the [`meter_usage`] hook. This is what the paid
//!   Container image is built with.
//!
//! # Fail-closed contract
//!
//! [`verify_startup_entitlement`] returns `Ok(())` only when the
//! deployment is either (a) explicitly self-hosted **in the OSS build**
//! (the operator set `FERRODRUID_MARKETPLACE_DISABLE`, which logs a
//! **loud** warning) or (b) the AWS `RegisterUsage` call confirmed
//! entitlement. Any AWS failure that is not a transient/unknown
//! condition — and in particular [`EntitlementError::NotEntitled`] — is
//! propagated to the caller so the binary can exit non-zero. The caller
//! (the `ferrodruid` binary) treats *any* `Err` as fatal, i.e. it fails
//! closed.
//!
//! The disable opt-out is a **compile-time property of the build flavor**:
//! it is honored only when `marketplace-metering` is OFF (the OSS image).
//! In the paid image (feature ON) `FERRODRUID_MARKETPLACE_DISABLE` is
//! ignored and entitlement is always verified, so a customer cannot defeat
//! billing by injecting one environment variable.
//!
//! [`RegisterUsage`]: https://docs.aws.amazon.com/marketplacemetering/latest/APIReference/API_RegisterUsage.html

use thiserror::Error;

/// Environment variable holding the AWS Marketplace product code that
/// uniquely identifies this listing. Required unless the deployment is
/// disabled via [`ENV_DISABLE`].
pub const ENV_PRODUCT_CODE: &str = "FERRODRUID_MARKETPLACE_PRODUCT_CODE";

/// Environment variable holding the public-key version handed out by
/// AWS Marketplace for this product. Optional; defaults to
/// [`DEFAULT_PUBLIC_KEY_VERSION`].
pub const ENV_PUBLIC_KEY_VERSION: &str = "FERRODRUID_MARKETPLACE_PUBLIC_KEY_VERSION";

/// Environment variable that, when set to `1`/`true` (case-insensitive),
/// turns the entitlement check into a **loud no-op** for self-hosted /
/// OSS deployments. This is the documented opt-out for running the
/// Apache-2.0 build outside of AWS Marketplace.
pub const ENV_DISABLE: &str = "FERRODRUID_MARKETPLACE_DISABLE";

/// Default `public_key_version` used when [`ENV_PUBLIC_KEY_VERSION`] is
/// unset. AWS Marketplace issues version `1` for new products.
pub const DEFAULT_PUBLIC_KEY_VERSION: i32 = 1;

/// Successful entitlement decision returned by an
/// [`EntitlementVerifier`].
///
/// `RegisterUsage` returns a signature (a JWT) and the timestamp at
/// which the signing key was last rotated. We keep those as opaque
/// strings: v1 does not perform any per-query metering, so nothing
/// downstream needs to interpret them yet. They are surfaced here so a
/// future metering layer (or an operator running `RUST_LOG=debug`) can
/// see that a genuine, signed entitlement was obtained.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EntitlementOk {
    /// Opaque signed entitlement token (a JWT) returned by AWS, if the
    /// backend provided one. The [`MockVerifier`] leaves this `None`.
    pub signature: Option<String>,
    /// Opaque public-key rotation timestamp returned by AWS, if any.
    pub public_key_rotation_timestamp: Option<String>,
}

/// Failure surface of the entitlement gate.
///
/// Every variant carries an operator-friendly `Display` string so the
/// `ferrodruid` binary can print a single clear line and exit non-zero.
/// Variants split deliberately into *definitely-not-entitled*
/// ([`EntitlementError::NotEntitled`]) versus *could-not-prove*
/// ([`EntitlementError::Aws`], [`EntitlementError::Config`]); the caller
/// fails closed on **all** of them, but the distinction makes the log
/// line actionable.
#[derive(Debug, Error)]
pub enum EntitlementError {
    /// AWS Marketplace affirmatively reported that this customer is not
    /// entitled to run the product: they never subscribed, the
    /// subscription has lapsed/expired, or the signature could not be
    /// validated against the supplied product code / public-key
    /// version. This is the canonical *fail-closed* signal.
    #[error(
        "AWS Marketplace says this deployment is NOT entitled to run FerroDruid \
         (not subscribed, subscription expired, or signature/product-code mismatch): {0}. \
         Subscribe to the FerroDruid Container listing on AWS Marketplace and verify the \
         configured product code (FERRODRUID_MARKETPLACE_PRODUCT_CODE) matches the listing. \
         (This is the paid Marketplace build; the self-host opt-out is compiled out here, so \
         FERRODRUID_MARKETPLACE_DISABLE has no effect — use the OSS image to self-host.)"
    )]
    NotEntitled(String),

    /// The AWS `RegisterUsage` call could not be completed for a reason
    /// other than a clear not-entitled verdict — e.g. a throttle, an
    /// internal service error, a transport/credential failure, or an
    /// unexpected response. The caller still fails closed (a paid
    /// container must not serve traffic it cannot prove it is allowed
    /// to serve), but the operator should retry / check IAM + network.
    #[error(
        "AWS Marketplace entitlement check could not be completed (throttled, service \
         error, credentials, or network): {0}. The container will not start. Retry, and \
         verify the task/pod role can call aws-marketplace:RegisterUsage."
    )]
    Aws(String),

    /// The [`MarketplaceConfig`] is invalid — most commonly a missing
    /// product code while the check is enabled.
    #[error("invalid AWS Marketplace configuration: {0}")]
    Config(String),
}

/// Configuration for the startup entitlement gate, normally built from
/// the process environment via [`MarketplaceConfig::from_env`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketplaceConfig {
    /// AWS Marketplace product code. `None` is only valid when
    /// [`MarketplaceConfig::disabled`] is `true`.
    pub product_code: Option<String>,
    /// Public-key version passed to `RegisterUsage`.
    pub public_key_version: i32,
    /// When `true`, the entitlement check is bypassed with a loud
    /// warning (self-hosted / OSS deployment).
    pub disabled: bool,
}

impl Default for MarketplaceConfig {
    fn default() -> Self {
        Self {
            product_code: None,
            public_key_version: DEFAULT_PUBLIC_KEY_VERSION,
            disabled: false,
        }
    }
}

/// Interpret a string as a boolean opt-out flag.
///
/// `1`, `true`, `yes`, and `on` (any case, surrounding whitespace
/// ignored) are treated as `true`; everything else — including the
/// empty string — is `false`. Keeping this permissive avoids an
/// operator setting `FERRODRUID_MARKETPLACE_DISABLE=true` and silently
/// still being gated.
fn parse_bool_flag(raw: &str) -> bool {
    matches!(
        raw.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl MarketplaceConfig {
    /// Build a [`MarketplaceConfig`] from the process environment.
    ///
    /// Reads [`ENV_PRODUCT_CODE`], [`ENV_PUBLIC_KEY_VERSION`]
    /// (defaulting to [`DEFAULT_PUBLIC_KEY_VERSION`]), and
    /// [`ENV_DISABLE`]. The returned config is **not** yet validated —
    /// call [`MarketplaceConfig::validate`] (or just hand it to
    /// [`verify_startup_entitlement`], which validates internally).
    ///
    /// # Errors
    ///
    /// Returns [`EntitlementError::Config`] if
    /// [`ENV_PUBLIC_KEY_VERSION`] is set but is not a valid `i32`.
    pub fn from_env() -> Result<Self, EntitlementError> {
        let lookup = |key: &str| std::env::var(key).ok();
        Self::from_lookup(lookup)
    }

    /// Environment-agnostic core of [`MarketplaceConfig::from_env`].
    ///
    /// Takes a closure mapping a variable name to its optional value so
    /// tests can drive parsing deterministically without mutating the
    /// real (process-global, racy) environment.
    ///
    /// # Errors
    ///
    /// Returns [`EntitlementError::Config`] if the public-key-version
    /// value is present but not a valid `i32`.
    fn from_lookup<F>(lookup: F) -> Result<Self, EntitlementError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let disabled = lookup(ENV_DISABLE)
            .as_deref()
            .map(parse_bool_flag)
            .unwrap_or(false);

        let product_code = lookup(ENV_PRODUCT_CODE).and_then(|raw| {
            let trimmed = raw.trim().to_string();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed)
            }
        });

        let public_key_version = match lookup(ENV_PUBLIC_KEY_VERSION) {
            None => DEFAULT_PUBLIC_KEY_VERSION,
            Some(raw) => {
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    DEFAULT_PUBLIC_KEY_VERSION
                } else {
                    trimmed.parse::<i32>().map_err(|e| {
                        EntitlementError::Config(format!(
                            "{ENV_PUBLIC_KEY_VERSION}=`{trimmed}` is not a valid integer: {e}"
                        ))
                    })?
                }
            }
        };

        Ok(Self {
            product_code,
            public_key_version,
            disabled,
        })
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`EntitlementError::Config`] if the check is enabled
    /// (not disabled) but no product code was supplied, or if the
    /// public-key version is negative (AWS versions are >= 1).
    pub fn validate(&self) -> Result<(), EntitlementError> {
        if self.public_key_version < 1 {
            return Err(EntitlementError::Config(format!(
                "public_key_version must be >= 1, got {}",
                self.public_key_version
            )));
        }
        if !self.disabled && self.product_code.is_none() {
            return Err(EntitlementError::Config(format!(
                "{ENV_PRODUCT_CODE} is required for the paid AWS Marketplace Container \
                 product. Set it to the product code from your Marketplace listing, or \
                 set {ENV_DISABLE}=1 to run the self-hosted Apache-2.0 build."
            )));
        }
        Ok(())
    }
}

/// Abstraction over "ask the entitlement authority whether this
/// deployment is allowed to run".
///
/// Modeled on the repo's `ferrodruid-rpc` client traits: an
/// `async-trait` with a production implementation
/// ([`AwsEntitlementVerifier`], behind the `marketplace-metering`
/// feature) and a test [`MockVerifier`], so [`verify_startup_entitlement`]
/// and any caller can be unit-tested without AWS.
#[async_trait::async_trait]
pub trait EntitlementVerifier: Send + Sync {
    /// Verify entitlement, returning [`EntitlementOk`] on success.
    ///
    /// # Errors
    ///
    /// Returns [`EntitlementError::NotEntitled`] when the authority
    /// affirmatively denies entitlement, or [`EntitlementError::Aws`]
    /// for any other failure to obtain a decision. Callers fail closed
    /// on every `Err`.
    async fn verify(&self) -> Result<EntitlementOk, EntitlementError>;
}

/// In-memory [`EntitlementVerifier`] for unit tests.
///
/// Mirrors the `MockBrokerClient` pattern in `ferrodruid-rpc`: it holds
/// a single canned outcome and replays it from [`EntitlementVerifier::verify`].
/// It never touches the network and is available regardless of the
/// `marketplace-metering` feature, so the fail-closed wiring can be
/// tested in the default build.
#[derive(Debug, Clone)]
pub struct MockVerifier {
    outcome: MockOutcome,
}

#[derive(Debug, Clone)]
enum MockOutcome {
    Entitled(EntitlementOk),
    NotEntitled(String),
    Aws(String),
}

impl MockVerifier {
    /// A mock that reports the deployment **is** entitled.
    #[must_use]
    pub fn entitled() -> Self {
        Self {
            outcome: MockOutcome::Entitled(EntitlementOk::default()),
        }
    }

    /// A mock that reports the deployment is **not** entitled (the
    /// fail-closed path), carrying `reason` in the error message.
    #[must_use]
    pub fn not_entitled(reason: impl Into<String>) -> Self {
        Self {
            outcome: MockOutcome::NotEntitled(reason.into()),
        }
    }

    /// A mock that reports an AWS-side failure (throttle, transport,
    /// credentials, …) that prevented a decision, carrying `reason`.
    #[must_use]
    pub fn aws_error(reason: impl Into<String>) -> Self {
        Self {
            outcome: MockOutcome::Aws(reason.into()),
        }
    }
}

#[async_trait::async_trait]
impl EntitlementVerifier for MockVerifier {
    async fn verify(&self) -> Result<EntitlementOk, EntitlementError> {
        match &self.outcome {
            MockOutcome::Entitled(ok) => Ok(ok.clone()),
            MockOutcome::NotEntitled(reason) => Err(EntitlementError::NotEntitled(reason.clone())),
            MockOutcome::Aws(reason) => Err(EntitlementError::Aws(reason.clone())),
        }
    }
}

/// Verify entitlement at process startup, the public entry point used
/// by the `ferrodruid` binary.
///
/// Behaviour depends on the build flavor, because the self-host disable
/// opt-out ([`ENV_DISABLE`]) is a **compile-time property of the build**,
/// not a runtime-overridable env var in the paid image:
///
/// - **Paid build (`marketplace-metering` ON):** the config is validated
///   (a product code is still required), then the real AWS verifier is
///   constructed and called. On success, return `Ok(())`; on any failure
///   propagate the `Err` so the caller exits non-zero (fail closed).
///   [`ENV_DISABLE`] is **ignored** here — a customer cannot defeat
///   entitlement/billing by injecting one environment variable into the
///   paid Marketplace container; if it is set, a loud `tracing::warn!`
///   notes that it was ignored and verification proceeds anyway.
/// - **OSS build (`marketplace-metering` OFF):** the config is validated,
///   then if `cfg.disabled` is set a **loud** `tracing::warn!`
///   ("AWS Marketplace entitlement check BYPASSED — self-hosted/OSS
///   deployment") is emitted and `Ok(())` is returned (this mirrors the
///   repo's "opt-out is loud" convention and is the only way the OSS
///   build can start without the AWS backend). A non-disabled config
///   returns [`EntitlementError::Config`] explaining that this binary was
///   not built with the entitlement backend — still fail-closed, never
///   silently running as if entitled.
///
/// # Errors
///
/// Returns an [`EntitlementError`] for an invalid config, a
/// not-entitled verdict, or an AWS failure. The default (feature-off,
/// non-disabled) path returns [`EntitlementError::Config`].
pub async fn verify_startup_entitlement(cfg: &MarketplaceConfig) -> Result<(), EntitlementError> {
    // In the PAID build (feature on) the self-host disable opt-out is a
    // compile-time no-op: the env var is honored only by the OSS/feature-off
    // build, so a customer cannot defeat entitlement/billing by injecting one
    // environment variable into the paid Marketplace container. We validate the
    // config *here* (rather than relying on the disable short-circuit below) so
    // the paid build still demands a product code regardless of DISABLE.
    #[cfg(feature = "marketplace-metering")]
    {
        if cfg.disabled {
            tracing::warn!(
                "{ENV_DISABLE} is set but this is the PAID AWS Marketplace build — the \
                 self-host opt-out is compiled out and IGNORED. Entitlement will be verified."
            );
        }
        cfg.validate()?;
        let verifier = aws_impl::AwsEntitlementVerifier::from_config(cfg).await?;
        return verify_with(&verifier).await;
    }

    // OSS / feature-off path: the disable opt-out is honored (it is the only
    // way this build can start), and a non-disabled config fails closed below.
    #[cfg(not(feature = "marketplace-metering"))]
    {
        cfg.validate()?;

        if cfg.disabled {
            tracing::warn!(
                "AWS Marketplace entitlement check BYPASSED — self-hosted/OSS deployment \
                 ({ENV_DISABLE} is set). This build is the Apache-2.0 source; if you obtained \
                 FerroDruid through the paid AWS Marketplace listing you should NOT set this."
            );
            return Ok(());
        }

        Err(EntitlementError::Config(format!(
            "this FerroDruid build was compiled WITHOUT the `marketplace-metering` feature, \
             so it cannot contact AWS Marketplace to verify entitlement, yet {ENV_DISABLE} is \
             not set. Refusing to start (fail closed). Either run the paid Container image \
             (built with the feature) or set {ENV_DISABLE}=1 for the self-hosted Apache-2.0 \
             build."
        )))
    }
}

/// Run an arbitrary [`EntitlementVerifier`] and translate its result
/// into the unit `Ok`/`Err` shape [`verify_startup_entitlement`]
/// returns. Factored out so the success/propagation wiring is testable
/// against a [`MockVerifier`] without the AWS feature.
///
/// Only compiled when the AWS verifier exists (the
/// `marketplace-metering` feature) or under `cfg(test)`; the default
/// feature-off, non-test build has no caller for it.
///
/// # Errors
///
/// Propagates whatever [`EntitlementError`] the verifier returns.
#[cfg(any(feature = "marketplace-metering", test))]
async fn verify_with<V: EntitlementVerifier + ?Sized>(
    verifier: &V,
) -> Result<(), EntitlementError> {
    let ok = verifier.verify().await?;
    tracing::info!(
        has_signature = ok.signature.is_some(),
        "AWS Marketplace entitlement confirmed — deployment is licensed to run"
    );
    Ok(())
}

/// Record a unit of metered usage for a custom pricing dimension.
///
/// **Not wired into any hot path.** v1 of the paid listing prices by
/// the hour (and optionally annually); AWS meters the AMI product
/// automatically and the Container product is gated by the one-shot
/// `RegisterUsage` call in [`verify_startup_entitlement`]. There is **no
/// per-query billing** today. This hook exists so a *future* usage-based
/// dimension (e.g. ingested GB or query units) can be added without a
/// new crate; it is deliberately not called anywhere in the codebase.
///
/// It is only compiled under the `marketplace-metering` feature because
/// it depends on the AWS SDK.
///
/// # Errors
///
/// Returns [`EntitlementError::Aws`] if the SDK call fails. Because no
/// caller invokes this yet, that is currently unreachable in practice.
#[cfg(feature = "marketplace-metering")]
pub async fn meter_usage(dimension: &str, qty: u64) -> Result<(), EntitlementError> {
    aws_impl::meter_usage_impl(dimension, qty).await
}

/// Real AWS-backed implementation, compiled only with the
/// `marketplace-metering` feature so the default build pulls in no AWS
/// SDK, makes no network calls, and needs no credentials.
#[cfg(feature = "marketplace-metering")]
mod aws_impl {
    use aws_sdk_marketplacemetering::Client;

    use super::{EntitlementError, EntitlementOk, EntitlementVerifier, MarketplaceConfig};

    /// Production [`EntitlementVerifier`] that calls AWS Marketplace
    /// `RegisterUsage` using the default credential / region provider
    /// chain (task role on ECS/EKS, instance role on EC2, or the
    /// standard env / profile fallbacks).
    pub struct AwsEntitlementVerifier {
        client: Client,
        product_code: String,
        public_key_version: i32,
    }

    impl AwsEntitlementVerifier {
        /// Build the verifier from a validated [`MarketplaceConfig`].
        ///
        /// # Errors
        ///
        /// Returns [`EntitlementError::Config`] if the product code is
        /// missing (should have been caught by
        /// [`MarketplaceConfig::validate`] already).
        pub async fn from_config(cfg: &MarketplaceConfig) -> Result<Self, EntitlementError> {
            let product_code = cfg.product_code.clone().ok_or_else(|| {
                EntitlementError::Config(
                    "product code missing when constructing AWS verifier".to_string(),
                )
            })?;
            let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
            let client = Client::new(&aws_cfg);
            Ok(Self {
                client,
                product_code,
                public_key_version: cfg.public_key_version,
            })
        }
    }

    #[async_trait::async_trait]
    impl EntitlementVerifier for AwsEntitlementVerifier {
        async fn verify(&self) -> Result<EntitlementOk, EntitlementError> {
            let result = self
                .client
                .register_usage()
                .product_code(self.product_code.clone())
                .public_key_version(self.public_key_version)
                .send()
                .await;

            match result {
                Ok(out) => Ok(EntitlementOk {
                    signature: out.signature().map(ToString::to_string),
                    public_key_rotation_timestamp: out
                        .public_key_rotation_timestamp()
                        .map(|ts| ts.to_string()),
                }),
                Err(sdk_err) => Err(map_register_usage_error(sdk_err)),
            }
        }
    }

    /// Map a `RegisterUsage` SDK error onto [`EntitlementError`].
    ///
    /// `CustomerNotEntitled`, `InvalidProductCode`, and
    /// `InvalidPublicKeyVersion` are treated as definitive
    /// *not-entitled* verdicts (wrong/expired/absent subscription or a
    /// signature/product mismatch) and become
    /// [`EntitlementError::NotEntitled`]. Everything else
    /// (throttling, internal service error, disabled API, region,
    /// platform, transport) becomes [`EntitlementError::Aws`]. Both map
    /// to a fail-closed `Err` at the caller; the split only shapes the
    /// operator log line.
    fn map_register_usage_error(
        sdk_err: aws_sdk_marketplacemetering::error::SdkError<
            aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError,
        >,
    ) -> EntitlementError {
        use aws_sdk_marketplacemetering::operation::register_usage::RegisterUsageError;

        // Render a stable, human-readable description before consuming
        // the error to classify it.
        let rendered = sdk_err.to_string();
        match sdk_err.into_service_error() {
            RegisterUsageError::CustomerNotEntitledException(e) => {
                EntitlementError::NotEntitled(format!("CustomerNotEntitled: {e}"))
            }
            RegisterUsageError::InvalidProductCodeException(e) => {
                EntitlementError::NotEntitled(format!("InvalidProductCode: {e}"))
            }
            RegisterUsageError::InvalidPublicKeyVersionException(e) => {
                EntitlementError::NotEntitled(format!("InvalidPublicKeyVersion: {e}"))
            }
            other => EntitlementError::Aws(format!("{rendered}: {other}")),
        }
    }

    /// Implementation backing the public [`super::meter_usage`] hook.
    ///
    /// Calls `MeterUsage` for a custom pricing dimension. **Unused**
    /// today (see the doc on [`super::meter_usage`]): v1 has no
    /// per-query billing. Kept feature-gated and standalone so adding a
    /// usage-based dimension later is a wiring change, not a new crate.
    ///
    /// # Errors
    ///
    /// Returns [`EntitlementError::Aws`] if the SDK call fails.
    pub async fn meter_usage_impl(dimension: &str, qty: u64) -> Result<(), EntitlementError> {
        let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_cfg);
        let quantity = i32::try_from(qty).map_err(|_| {
            EntitlementError::Aws(format!(
                "usage quantity {qty} for dimension `{dimension}` exceeds the AWS \
                 MeterUsage i32 limit"
            ))
        })?;
        client
            .meter_usage()
            .usage_dimension(dimension)
            .usage_quantity(quantity)
            .timestamp(aws_sdk_marketplacemetering::primitives::DateTime::from_secs(0))
            .send()
            .await
            .map_err(|e| EntitlementError::Aws(format!("MeterUsage failed: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Build a `from_lookup` closure from a static map for
    /// deterministic, non-racy config-parse tests.
    fn lookup_from(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        move |k: &str| map.get(k).cloned()
    }

    #[test]
    fn config_parses_product_code_and_defaults_public_key_version() {
        let cfg = MarketplaceConfig::from_lookup(lookup_from(&[(ENV_PRODUCT_CODE, "prod-abc123")]))
            .expect("parse");
        assert_eq!(cfg.product_code.as_deref(), Some("prod-abc123"));
        assert_eq!(cfg.public_key_version, DEFAULT_PUBLIC_KEY_VERSION);
        assert!(!cfg.disabled);
        cfg.validate().expect("valid: has product code");
    }

    #[test]
    fn config_parses_explicit_public_key_version() {
        let cfg = MarketplaceConfig::from_lookup(lookup_from(&[
            (ENV_PRODUCT_CODE, "prod-abc123"),
            (ENV_PUBLIC_KEY_VERSION, "7"),
        ]))
        .expect("parse");
        assert_eq!(cfg.public_key_version, 7);
    }

    #[test]
    fn config_blank_public_key_version_falls_back_to_default() {
        let cfg = MarketplaceConfig::from_lookup(lookup_from(&[
            (ENV_PRODUCT_CODE, "prod-abc123"),
            (ENV_PUBLIC_KEY_VERSION, "   "),
        ]))
        .expect("parse");
        assert_eq!(cfg.public_key_version, DEFAULT_PUBLIC_KEY_VERSION);
    }

    #[test]
    fn config_non_integer_public_key_version_is_config_error() {
        let err = MarketplaceConfig::from_lookup(lookup_from(&[
            (ENV_PRODUCT_CODE, "prod-abc123"),
            (ENV_PUBLIC_KEY_VERSION, "not-a-number"),
        ]))
        .expect_err("should reject");
        assert!(matches!(err, EntitlementError::Config(_)));
    }

    #[test]
    fn config_missing_product_code_when_enabled_fails_validation() {
        let cfg = MarketplaceConfig::from_lookup(lookup_from(&[])).expect("parse");
        assert_eq!(cfg.product_code, None);
        assert!(!cfg.disabled);
        let err = cfg.validate().expect_err("must require product code");
        assert!(matches!(err, EntitlementError::Config(_)));
    }

    #[test]
    fn config_missing_product_code_when_disabled_is_valid() {
        let cfg =
            MarketplaceConfig::from_lookup(lookup_from(&[(ENV_DISABLE, "1")])).expect("parse");
        assert!(cfg.disabled);
        assert_eq!(cfg.product_code, None);
        cfg.validate()
            .expect("disabled deployments need no product code");
    }

    #[test]
    fn config_disable_accepts_several_truthy_spellings() {
        for truthy in ["1", "true", "TRUE", "Yes", " on "] {
            let cfg = MarketplaceConfig::from_lookup(lookup_from(&[(ENV_DISABLE, truthy)]))
                .expect("parse");
            assert!(cfg.disabled, "`{truthy}` should disable");
        }
        for falsy in ["0", "false", "no", "", "off"] {
            let cfg = MarketplaceConfig::from_lookup(lookup_from(&[(ENV_DISABLE, falsy)]))
                .expect("parse");
            assert!(!cfg.disabled, "`{falsy}` should NOT disable");
        }
    }

    #[test]
    fn config_negative_public_key_version_is_invalid() {
        let cfg = MarketplaceConfig {
            product_code: Some("prod-abc123".into()),
            public_key_version: -1,
            disabled: false,
        };
        let err = cfg.validate().expect_err("negative version invalid");
        assert!(matches!(err, EntitlementError::Config(_)));
    }

    /// OSS build only: the `FERRODRUID_MARKETPLACE_DISABLE` opt-out
    /// short-circuits the gate (it is the only way the feature-off build can
    /// start without the AWS backend). In the paid build the disable flag is
    /// compiled out — see [`paid_build_ignores_disable_and_fails_closed`].
    #[cfg(not(feature = "marketplace-metering"))]
    #[tokio::test]
    async fn disable_bypass_returns_ok() {
        let cfg = MarketplaceConfig {
            product_code: None,
            public_key_version: DEFAULT_PUBLIC_KEY_VERSION,
            disabled: true,
        };
        // Should short-circuit before touching any AWS path and succeed.
        verify_startup_entitlement(&cfg)
            .await
            .expect("disabled deployment is allowed to run");
    }

    /// Paid build only: the disable opt-out must NOT let a customer bypass
    /// entitlement by injecting one env var. With the feature ON and
    /// `disabled = true`, `verify_startup_entitlement` ignores the flag and
    /// proceeds; here there is no product code, so it fails closed with a
    /// [`EntitlementError::Config`] *before* any AWS call — proving the
    /// bypass is gone while still never silently running as if entitled.
    #[cfg(feature = "marketplace-metering")]
    #[tokio::test]
    async fn paid_build_ignores_disable_and_fails_closed() {
        let cfg = MarketplaceConfig {
            product_code: None,
            public_key_version: DEFAULT_PUBLIC_KEY_VERSION,
            disabled: true,
        };
        let err = verify_startup_entitlement(&cfg)
            .await
            .expect_err("paid build must not honor DISABLE without a product code");
        assert!(
            matches!(err, EntitlementError::Config(_)),
            "expected a fail-closed Config error, got {err:?}"
        );
    }

    #[tokio::test]
    async fn mock_entitled_passes_wiring() {
        let verifier = MockVerifier::entitled();
        verify_with(&verifier)
            .await
            .expect("entitled mock should yield Ok");
    }

    #[tokio::test]
    async fn mock_not_entitled_fails_closed() {
        let verifier = MockVerifier::not_entitled("subscription expired");
        let err = verify_with(&verifier)
            .await
            .expect_err("not-entitled must fail closed");
        match err {
            EntitlementError::NotEntitled(msg) => assert!(msg.contains("subscription expired")),
            other => panic!("expected NotEntitled, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn mock_aws_error_fails_closed() {
        let verifier = MockVerifier::aws_error("throttled");
        let err = verify_with(&verifier)
            .await
            .expect_err("aws failure must fail closed");
        assert!(matches!(err, EntitlementError::Aws(_)));
    }

    #[test]
    fn error_display_strings_are_operator_friendly() {
        let not_entitled = EntitlementError::NotEntitled("expired".into());
        let s = not_entitled.to_string();
        assert!(s.contains("NOT entitled"));
        assert!(
            s.contains(ENV_DISABLE),
            "should point operator at the opt-out"
        );
        assert!(s.contains("AWS Marketplace"));

        let aws = EntitlementError::Aws("503".into());
        let s = aws.to_string();
        assert!(s.contains("could not be completed"));
        assert!(s.contains("RegisterUsage"));

        let config = EntitlementError::Config("no product code".into());
        assert!(config.to_string().contains("configuration"));
    }

    #[test]
    fn entitlement_ok_default_is_unsigned() {
        let ok = EntitlementOk::default();
        assert!(ok.signature.is_none());
        assert!(ok.public_key_rotation_timestamp.is_none());
    }
}
