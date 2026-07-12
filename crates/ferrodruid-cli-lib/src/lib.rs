// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! CLI library for FerroDruid admin operations.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use thiserror::Error;

/// CLI errors.
#[derive(Debug, Error)]
pub enum CliError {
    /// HTTP request failed.
    #[error("request failed: {0}")]
    Request(String),
    /// Server returned an error.
    #[error("server error: {status} {body}")]
    Server {
        /// HTTP status code.
        status: u16,
        /// Response body.
        body: String,
    },
    /// JSON error.
    #[error("json error: {0}")]
    Json(String),
}

impl From<reqwest::Error> for CliError {
    fn from(e: reqwest::Error) -> Self {
        CliError::Request(e.to_string())
    }
}

/// Client for interacting with a FerroDruid cluster via REST API.
pub struct DruidClient {
    base_url: String,
    client: reqwest::Client,
}

impl DruidClient {
    /// Create a new client targeting the given base URL.
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Return the base URL.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // ----- Internal helpers ------------------------------------------------

    async fn get_json(&self, path: &str) -> Result<serde_json::Value, CliError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self.client.get(&url).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        serde_json::from_str(&body).map_err(|e| CliError::Json(e.to_string()))
    }

    async fn post_json(
        &self,
        path: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value, CliError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self.client.post(&url).json(payload).send().await?;
        let status = resp.status();
        let body = resp.text().await?;
        if !status.is_success() {
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        if body.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        serde_json::from_str(&body).map_err(|e| CliError::Json(e.to_string()))
    }

    async fn delete(&self, path: &str) -> Result<(), CliError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self.client.delete(&url).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await?;
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    // ----- Query -----------------------------------------------------------

    /// Submit a native (JSON) query.
    pub async fn native_query(
        &self,
        query: &serde_json::Value,
    ) -> Result<serde_json::Value, CliError> {
        self.post_json("/druid/v2", query).await
    }

    /// Submit an SQL query.
    pub async fn sql_query(&self, sql: &str) -> Result<serde_json::Value, CliError> {
        let payload = serde_json::json!({"query": sql});
        self.post_json("/druid/v2/sql", &payload).await
    }

    // ----- Status ----------------------------------------------------------

    /// Get server status.
    pub async fn status(&self) -> Result<serde_json::Value, CliError> {
        self.get_json("/status").await
    }

    /// Health check — returns `true` if the server is healthy.
    pub async fn health(&self) -> Result<bool, CliError> {
        let url = format!("{}/status/health", self.base_url);
        let resp = self.client.get(&url).send().await?;
        Ok(resp.status().is_success())
    }

    // ----- Coordinator -----------------------------------------------------

    /// List all datasource names.
    pub async fn list_datasources(&self) -> Result<Vec<String>, CliError> {
        let val = self.get_json("/druid/coordinator/v1/datasources").await?;
        serde_json::from_value(val).map_err(|e| CliError::Json(e.to_string()))
    }

    /// Get segment metadata for a datasource.
    pub async fn get_datasource_segments(&self, ds: &str) -> Result<serde_json::Value, CliError> {
        self.get_json(&format!("/druid/coordinator/v1/datasources/{ds}/segments"))
            .await
    }

    /// Get load rules for a datasource.
    pub async fn get_rules(&self, ds: &str) -> Result<serde_json::Value, CliError> {
        self.get_json(&format!("/druid/coordinator/v1/rules/{ds}"))
            .await
    }

    /// Set load rules for a datasource.
    pub async fn set_rules(&self, ds: &str, rules: &serde_json::Value) -> Result<(), CliError> {
        let url = format!("{}/druid/coordinator/v1/rules/{ds}", self.base_url);
        let resp = self.client.post(&url).json(rules).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await?;
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    // ----- Overlord --------------------------------------------------------

    /// Submit a task spec.
    pub async fn submit_task(&self, spec: &serde_json::Value) -> Result<String, CliError> {
        let val = self.post_json("/druid/indexer/v1/task", spec).await?;
        val.get("task")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| CliError::Json("missing 'task' in response".to_string()))
    }

    /// Get task status by id.
    pub async fn get_task(&self, id: &str) -> Result<serde_json::Value, CliError> {
        self.get_json(&format!("/druid/indexer/v1/task/{id}")).await
    }

    /// List all tasks.
    pub async fn list_tasks(&self) -> Result<serde_json::Value, CliError> {
        self.get_json("/druid/indexer/v1/tasks").await
    }

    // ----- Supervisors -----------------------------------------------------

    /// Create a supervisor.
    pub async fn create_supervisor(&self, spec: &serde_json::Value) -> Result<String, CliError> {
        let val = self.post_json("/druid/indexer/v1/supervisor", spec).await?;
        val.get("id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| CliError::Json("missing 'id' in response".to_string()))
    }

    /// Get supervisor status by id.
    pub async fn get_supervisor(&self, id: &str) -> Result<serde_json::Value, CliError> {
        self.get_json(&format!("/druid/indexer/v1/supervisor/{id}"))
            .await
    }

    /// List all supervisors.
    pub async fn list_supervisors(&self) -> Result<serde_json::Value, CliError> {
        self.get_json("/druid/indexer/v1/supervisor").await
    }

    /// Shut down a supervisor.
    pub async fn shutdown_supervisor(&self, id: &str) -> Result<(), CliError> {
        let url = format!(
            "{}/druid/indexer/v1/supervisor/{id}/shutdown",
            self.base_url
        );
        let resp = self
            .client
            .post(&url)
            .json(&serde_json::Value::Null)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await?;
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    // ----- Lookups ---------------------------------------------------------

    /// List all lookups.
    pub async fn list_lookups(&self) -> Result<serde_json::Value, CliError> {
        self.get_json("/druid/coordinator/v1/lookups").await
    }

    /// Get a lookup by tier and name.
    pub async fn get_lookup(&self, tier: &str, name: &str) -> Result<serde_json::Value, CliError> {
        self.get_json(&format!("/druid/coordinator/v1/lookups/{tier}/{name}"))
            .await
    }

    /// Create (or update) a lookup.
    pub async fn create_lookup(
        &self,
        tier: &str,
        name: &str,
        spec: &serde_json::Value,
    ) -> Result<(), CliError> {
        let url = format!(
            "{}/druid/coordinator/v1/lookups/{tier}/{name}",
            self.base_url
        );
        let resp = self.client.post(&url).json(spec).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await?;
            return Err(CliError::Server {
                status: status.as_u16(),
                body,
            });
        }
        Ok(())
    }

    /// Delete a lookup.
    pub async fn delete_lookup(&self, tier: &str, name: &str) -> Result<(), CliError> {
        self.delete(&format!("/druid/coordinator/v1/lookups/{tier}/{name}"))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_url_construction() {
        let client = DruidClient::new("http://localhost:8888");
        assert_eq!(client.base_url(), "http://localhost:8888");
    }

    #[test]
    fn client_trims_trailing_slash() {
        let client = DruidClient::new("http://localhost:8888/");
        assert_eq!(client.base_url(), "http://localhost:8888");
    }

    #[test]
    fn client_creation_does_not_panic() {
        let _client = DruidClient::new("http://example.com:9999");
    }

    #[test]
    fn cli_error_display() {
        let err = CliError::Request("connection refused".to_string());
        assert_eq!(err.to_string(), "request failed: connection refused");

        let err = CliError::Server {
            status: 404,
            body: "not found".to_string(),
        };
        assert_eq!(err.to_string(), "server error: 404 not found");

        let err = CliError::Json("unexpected token".to_string());
        assert_eq!(err.to_string(), "json error: unexpected token");
    }
}
