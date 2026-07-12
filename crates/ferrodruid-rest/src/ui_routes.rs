// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! UI route handlers for the FerroDruid Web Console.

use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};

/// `GET /` -- redirect to the unified console.
pub async fn handle_root_redirect() -> Redirect {
    Redirect::permanent("/unified-console.html")
}

/// `GET /unified-console.html` -- main console page.
pub async fn handle_unified_console() -> Response {
    html_response(ferrodruid_ui::render_console_html())
}

/// `GET /console/datasources` -- datasources page.
pub async fn handle_console_datasources() -> Response {
    html_response(ferrodruid_ui::render_datasources_page())
}

/// `GET /console/query` -- SQL query workbench page.
pub async fn handle_console_query() -> Response {
    html_response(ferrodruid_ui::render_query_page())
}

/// `GET /console/segments` -- segments page.
pub async fn handle_console_segments() -> Response {
    html_response(ferrodruid_ui::render_segments_page())
}

/// `GET /console/supervisors` -- supervisors page.
pub async fn handle_console_supervisors() -> Response {
    html_response(ferrodruid_ui::render_supervisors_page())
}

/// `GET /console/tasks` -- tasks page.
pub async fn handle_console_tasks() -> Response {
    html_response(ferrodruid_ui::render_tasks_page())
}

fn html_response(body: String) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        body,
    )
        .into_response()
}
