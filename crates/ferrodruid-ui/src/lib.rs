// SPDX-License-Identifier: BUSL-1.1
// Copyright 2026 abyo software 合同会社 (abyo software LLC)

//! FerroDruid Web Console -- server-side rendered HTML UI.
//!
//! Provides HTML page renderers for the Unified Console, including navigation,
//! datasource management, query workbench, segment viewer, supervisor and task
//! management pages.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

/// CSS styles shared across all pages.
const SHARED_CSS: &str = r#"
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
  background: #f5f6f8; color: #333; display: flex; min-height: 100vh;
}
header {
  position: fixed; top: 0; left: 0; right: 0; height: 48px; z-index: 100;
  background: #1a1a2e; color: #fff; display: flex; align-items: center; padding: 0 20px;
}
header .brand { font-size: 18px; font-weight: 700; letter-spacing: 0.5px; }
header .brand span { color: #4fc3f7; }
header nav { margin-left: 32px; display: flex; gap: 4px; }
header nav a {
  color: #b0bec5; text-decoration: none; padding: 6px 14px; border-radius: 4px;
  font-size: 13px; font-weight: 500; transition: background 0.15s, color 0.15s;
}
header nav a:hover, header nav a.active { background: rgba(255,255,255,0.1); color: #fff; }
.sidebar {
  position: fixed; top: 48px; left: 0; bottom: 0; width: 200px; background: #fff;
  border-right: 1px solid #e0e0e0; padding: 16px 0; overflow-y: auto;
}
.sidebar a {
  display: block; padding: 8px 20px; color: #555; text-decoration: none; font-size: 13px;
  border-left: 3px solid transparent; transition: all 0.15s;
}
.sidebar a:hover, .sidebar a.active { background: #f0f4ff; color: #1565c0; border-left-color: #1565c0; }
.main { margin-top: 48px; margin-left: 200px; padding: 24px; flex: 1; }
.card {
  background: #fff; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  padding: 20px; margin-bottom: 20px;
}
.card h2 { font-size: 16px; margin-bottom: 12px; color: #1a1a2e; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
table th { background: #f5f6f8; padding: 8px 12px; text-align: left; font-weight: 600; border-bottom: 2px solid #e0e0e0; }
table td { padding: 8px 12px; border-bottom: 1px solid #eee; }
table tr:hover { background: #fafbff; }
.btn {
  display: inline-block; padding: 8px 18px; background: #1565c0; color: #fff; border: none;
  border-radius: 4px; cursor: pointer; font-size: 13px; font-weight: 500; transition: background 0.15s;
}
.btn:hover { background: #0d47a1; }
textarea {
  width: 100%; min-height: 120px; padding: 12px; font-family: 'SF Mono', 'Fira Code', monospace;
  font-size: 13px; border: 1px solid #ccc; border-radius: 4px; resize: vertical;
}
.status-badge {
  display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 11px; font-weight: 600;
}
.status-running { background: #e8f5e9; color: #2e7d32; }
.status-pending { background: #fff3e0; color: #e65100; }
.status-success { background: #e8f5e9; color: #2e7d32; }
.status-failed { background: #ffebee; color: #c62828; }
#results-table { margin-top: 12px; overflow-x: auto; }
.spinner { display: none; margin-left: 8px; }
.spinner.active { display: inline-block; animation: spin 0.8s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.refresh-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; }
.refresh-bar select { padding: 4px 8px; font-size: 12px; }
"#;

/// JavaScript for auto-refreshing pages.
///
/// Also defines `esc()` — the single HTML-escaping helper every page routes
/// dynamic values through before concatenating them into `innerHTML`.  It is
/// declared here (in the first `<script>`) so the per-page `extra_js` (the
/// second `<script>`) can call it.  Without it, any datasource / column /
/// cell / segment / task / supervisor value — or the reflected `?ds=` query
/// parameter — containing `<img src=x onerror=...>` would execute as script
/// in the console origin (stored + reflected XSS).
const REFRESH_JS: &str = r#"
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, function(c) {
    return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
  });
}
let refreshTimer = null;
function setRefreshInterval(seconds) {
  if (refreshTimer) clearInterval(refreshTimer);
  if (seconds > 0) {
    refreshTimer = setInterval(() => { if (typeof loadData === 'function') loadData(); }, seconds * 1000);
  }
}
document.addEventListener('DOMContentLoaded', () => {
  const sel = document.getElementById('refresh-interval');
  if (sel) sel.addEventListener('change', (e) => setRefreshInterval(parseInt(e.target.value, 10)));
  if (typeof loadData === 'function') loadData();
  setRefreshInterval(10);
});
"#;

fn wrap_page(title: &str, active_nav: &str, body: &str, extra_js: &str) -> String {
    let nav_items = [
        ("Datasources", "/console/datasources"),
        ("Segments", "/console/segments"),
        ("Supervisors", "/console/supervisors"),
        ("Tasks", "/console/tasks"),
        ("Query", "/console/query"),
    ];

    let nav_html: String = nav_items
        .iter()
        .map(|(label, href)| {
            let class = if *label == active_nav {
                " class=\"active\""
            } else {
                ""
            };
            format!("<a href=\"{href}\"{class}>{label}</a>")
        })
        .collect::<Vec<_>>()
        .join("\n        ");

    let sidebar_html: String = nav_items
        .iter()
        .map(|(label, href)| {
            let class = if *label == active_nav {
                " class=\"active\""
            } else {
                ""
            };
            format!("    <a href=\"{href}\"{class}>{label}</a>")
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title} - FerroDruid Console</title>
  <link rel="icon" href="data:image/svg+xml,%3Csvg%20xmlns='http://www.w3.org/2000/svg'%20viewBox='0%200%2016%2016'%3E%3Crect%20width='16'%20height='16'%20rx='3'%20fill='%231a1a2e'/%3E%3Ctext%20x='8'%20y='12'%20font-size='11'%20font-family='sans-serif'%20font-weight='700'%20text-anchor='middle'%20fill='%234fc3f7'%3EF%3C/text%3E%3C/svg%3E">
  <style>{SHARED_CSS}</style>
</head>
<body>
  <header>
    <div class="brand">Ferro<span>Druid</span></div>
    <nav>
        {nav_html}
    </nav>
  </header>
  <div class="sidebar">
{sidebar_html}
  </div>
  <div class="main">
{body}
  </div>
  <script>{REFRESH_JS}</script>
  <script>{extra_js}</script>
</body>
</html>"#
    )
}

fn refresh_bar() -> &'static str {
    r#"<div class="refresh-bar">
  <button class="btn" onclick="loadData()">Refresh</button>
  <select id="refresh-interval">
    <option value="0">Manual</option>
    <option value="5">5s</option>
    <option value="10" selected>10s</option>
    <option value="30">30s</option>
    <option value="60">60s</option>
  </select>
  <span id="loading" class="spinner">&#9696;</span>
</div>"#
}

/// Generate the main console HTML page (redirects to datasources).
pub fn render_console_html() -> String {
    wrap_page(
        "Console",
        "Datasources",
        &format!(
            r#"{}
<div class="card">
  <h2>Datasources</h2>
  <div id="content"><p>Loading...</p></div>
</div>"#,
            refresh_bar()
        ),
        r#"
function loadData() {
  document.getElementById('loading').classList.add('active');
  fetch('/druid/coordinator/v1/datasources')
    .then(r => r.json())
    .then(data => {
      let html = '<table><thead><tr><th>Datasource</th><th>Actions</th></tr></thead><tbody>';
      if (Array.isArray(data)) {
        data.forEach(ds => {
          const name = typeof ds === 'string' ? ds : (ds.name || JSON.stringify(ds));
          html += '<tr><td>' + esc(name) + '</td><td><a href="/console/segments?ds=' + encodeURIComponent(name) + '">Segments</a></td></tr>';
        });
      }
      if (!data || data.length === 0) html += '<tr><td colspan="2">No datasources found</td></tr>';
      html += '</tbody></table>';
      document.getElementById('content').innerHTML = html;
    })
    .catch(err => { document.getElementById('content').innerHTML = '<p style="color:red">Error: ' + esc(err) + '</p>'; })
    .finally(() => { document.getElementById('loading').classList.remove('active'); });
}
"#,
    )
}

/// Generate the datasources page.
pub fn render_datasources_page() -> String {
    render_console_html()
}

/// Generate the query workbench page.
pub fn render_query_page() -> String {
    wrap_page(
        "Query",
        "Query",
        r#"<div class="card">
  <h2>SQL Query Workbench</h2>
  <textarea id="sql-input" placeholder="SELECT COUNT(*) AS cnt FROM my_datasource">SELECT COUNT(*) AS cnt FROM my_datasource</textarea>
  <div style="margin-top: 8px; display: flex; gap: 8px; align-items: center;">
    <button class="btn" id="run-btn" onclick="runQuery()">Run</button>
    <span id="query-status" style="font-size: 12px; color: #666;"></span>
  </div>
  <div id="results-table"></div>
</div>"#,
        r#"
function runQuery() {
  const sql = document.getElementById('sql-input').value.trim();
  if (!sql) return;
  const status = document.getElementById('query-status');
  const results = document.getElementById('results-table');
  status.textContent = 'Running...';
  results.innerHTML = '';
  const start = Date.now();
  fetch('/druid/v2/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: sql })
  })
  .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
  .then(data => {
    const elapsed = Date.now() - start;
    status.textContent = 'Completed in ' + elapsed + 'ms (' + (Array.isArray(data) ? data.length : 0) + ' rows)';
    if (!Array.isArray(data) || data.length === 0) {
      results.innerHTML = '<p style="margin-top:8px;color:#666;">No results</p>';
      return;
    }
    const cols = Object.keys(data[0]);
    let html = '<table><thead><tr>' + cols.map(c => '<th>' + esc(c) + '</th>').join('') + '</tr></thead><tbody>';
    data.forEach(row => {
      html += '<tr>' + cols.map(c => '<td>' + (row[c] !== null && row[c] !== undefined ? esc(row[c]) : 'null') + '</td>').join('') + '</tr>';
    });
    html += '</tbody></table>';
    results.innerHTML = html;
  })
  .catch(err => {
    status.textContent = 'Error';
    results.innerHTML = '<p style="color:red;margin-top:8px;">' + esc(err) + '</p>';
  });
}
document.getElementById('sql-input').addEventListener('keydown', (e) => {
  if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); runQuery(); }
});
"#,
    )
}

/// Generate the segments page.
pub fn render_segments_page() -> String {
    wrap_page(
        "Segments",
        "Segments",
        &format!(
            r#"{}
<div class="card">
  <h2>Segments</h2>
  <div id="content"><p>Loading...</p></div>
</div>"#,
            refresh_bar()
        ),
        r#"
function loadData() {
  document.getElementById('loading').classList.add('active');
  const params = new URLSearchParams(window.location.search);
  const ds = params.get('ds');
  const url = ds
    ? '/druid/coordinator/v1/datasources/' + encodeURIComponent(ds) + '/segments'
    : '/druid/coordinator/v1/datasources';
  fetch(url)
    .then(r => r.json())
    .then(data => {
      let html;
      if (ds) {
        html = '<h3>Segments for: ' + esc(ds) + '</h3>';
        html += '<table><thead><tr><th>Segment ID</th><th>Interval</th><th>Version</th><th>Size</th></tr></thead><tbody>';
        if (Array.isArray(data)) {
          data.forEach(seg => {
            const id = seg.id || seg.identifier || JSON.stringify(seg);
            const interval = seg.interval || '';
            const version = seg.version || '';
            const size = seg.size !== null && seg.size !== undefined ? seg.size : '';
            html += '<tr><td style="font-size:11px">' + esc(id) + '</td><td>' + esc(interval) + '</td><td>' + esc(version) + '</td><td>' + esc(size) + '</td></tr>';
          });
        }
        if (!data || data.length === 0) html += '<tr><td colspan="4">No segments</td></tr>';
        html += '</tbody></table>';
      } else {
        html = '<p>Select a datasource to view its segments.</p>';
        html += '<table><thead><tr><th>Datasource</th><th>Actions</th></tr></thead><tbody>';
        if (Array.isArray(data)) {
          data.forEach(ds => {
            const name = typeof ds === 'string' ? ds : (ds.name || JSON.stringify(ds));
            html += '<tr><td>' + esc(name) + '</td><td><a href="/console/segments?ds=' + encodeURIComponent(name) + '">View Segments</a></td></tr>';
          });
        }
        if (!data || data.length === 0) html += '<tr><td colspan="2">No datasources</td></tr>';
        html += '</tbody></table>';
      }
      document.getElementById('content').innerHTML = html;
    })
    .catch(err => { document.getElementById('content').innerHTML = '<p style="color:red">Error: ' + esc(err) + '</p>'; })
    .finally(() => { document.getElementById('loading').classList.remove('active'); });
}
"#,
    )
}

/// Generate the supervisors page.
pub fn render_supervisors_page() -> String {
    wrap_page(
        "Supervisors",
        "Supervisors",
        &format!(
            r#"{}
<div class="card">
  <h2>Supervisors</h2>
  <div id="content"><p>Loading...</p></div>
</div>"#,
            refresh_bar()
        ),
        r#"
function loadData() {
  document.getElementById('loading').classList.add('active');
  fetch('/druid/indexer/v1/supervisor')
    .then(r => r.json())
    .then(data => {
      let html = '<table><thead><tr><th>ID</th><th>Type</th><th>Status</th><th>Actions</th></tr></thead><tbody>';
      if (Array.isArray(data)) {
        data.forEach(sup => {
          const id = sup.id || '';
          const type_ = sup.type || sup.spec && sup.spec.type || '';
          const state = sup.state || sup.detailedState || 'RUNNING';
          const badge = state === 'RUNNING' ? 'status-running' : 'status-pending';
          html += '<tr><td>' + esc(id) + '</td><td>' + esc(type_) + '</td><td><span class="status-badge ' + badge + '">' + esc(state) + '</span></td>';
          // The id is carried in a data-* attribute (HTML-escaped) and read
          // back via addEventListener below — never spliced into an inline
          // onclick JS string, which would let an id like `x');evil()//`
          // break out of the handler and execute.
          html += '<td><button class="btn shutdown-sup" style="padding:4px 10px;font-size:11px;" data-sup-id="' + esc(id) + '">Shutdown</button></td></tr>';
        });
      }
      if (!data || data.length === 0) html += '<tr><td colspan="4">No supervisors</td></tr>';
      html += '</tbody></table>';
      document.getElementById('content').innerHTML = html;
      document.querySelectorAll('button.shutdown-sup').forEach(function(btn) {
        btn.addEventListener('click', function() { shutdownSupervisor(btn.getAttribute('data-sup-id')); });
      });
    })
    .catch(err => { document.getElementById('content').innerHTML = '<p style="color:red">Error: ' + esc(err) + '</p>'; })
    .finally(() => { document.getElementById('loading').classList.remove('active'); });
}
function shutdownSupervisor(id) {
  if (!confirm('Shutdown supervisor ' + id + '?')) return;
  fetch('/druid/indexer/v1/supervisor/' + encodeURIComponent(id) + '/shutdown', { method: 'POST' })
    .then(() => loadData())
    .catch(err => alert('Error: ' + err));
}
"#,
    )
}

/// Generate the tasks page.
pub fn render_tasks_page() -> String {
    wrap_page(
        "Tasks",
        "Tasks",
        &format!(
            r#"{}
<div class="card">
  <h2>Running Tasks</h2>
  <div id="running-content"><p>Loading...</p></div>
</div>
<div class="card">
  <h2>Completed Tasks</h2>
  <div id="complete-content"><p>Loading...</p></div>
</div>"#,
            refresh_bar()
        ),
        r#"
function loadData() {
  document.getElementById('loading').classList.add('active');
  Promise.all([
    fetch('/druid/indexer/v1/runningTasks').then(r => r.json()),
    fetch('/druid/indexer/v1/completeTasks').then(r => r.json())
  ]).then(([running, complete]) => {
    document.getElementById('running-content').innerHTML = renderTaskTable(running);
    document.getElementById('complete-content').innerHTML = renderTaskTable(complete);
  })
  .catch(err => {
    document.getElementById('running-content').innerHTML = '<p style="color:red">Error: ' + esc(err) + '</p>';
  })
  .finally(() => { document.getElementById('loading').classList.remove('active'); });
}
function renderTaskTable(tasks) {
  let html = '<table><thead><tr><th>Task ID</th><th>Type</th><th>Datasource</th><th>Status</th><th>Created</th></tr></thead><tbody>';
  if (Array.isArray(tasks)) {
    tasks.forEach(t => {
      const id = t.id || '';
      const type_ = t.type || '';
      const ds = t.dataSource || '';
      const status = t.status || t.statusCode || '';
      const badge = status === 'SUCCESS' ? 'status-success' : status === 'FAILED' ? 'status-failed' : 'status-running';
      const created = t.createdTime || '';
      html += '<tr><td style="font-size:11px">' + esc(id) + '</td><td>' + esc(type_) + '</td><td>' + esc(ds) + '</td>';
      html += '<td><span class="status-badge ' + badge + '">' + esc(status) + '</span></td><td>' + esc(created) + '</td></tr>';
    });
  }
  if (!tasks || tasks.length === 0) html += '<tr><td colspan="5">No tasks</td></tr>';
  html += '</tbody></table>';
  return html;
}
"#,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn console_html_has_doctype() {
        let html = render_console_html();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("<html"));
        assert!(html.contains("<body>"));
        assert!(html.contains("</html>"));
    }

    #[test]
    fn console_html_has_branding() {
        let html = render_console_html();
        assert!(html.contains("FerroDruid"));
    }

    #[test]
    fn datasources_page_has_navigation() {
        let html = render_datasources_page();
        assert!(html.contains("/console/datasources"));
        assert!(html.contains("/console/query"));
        assert!(html.contains("/console/segments"));
        assert!(html.contains("/console/supervisors"));
        assert!(html.contains("/console/tasks"));
    }

    #[test]
    fn query_page_has_textarea() {
        let html = render_query_page();
        assert!(html.contains("<textarea"));
        assert!(html.contains("sql-input"));
        assert!(html.contains("Run"));
    }

    #[test]
    fn query_page_fetches_sql_endpoint() {
        let html = render_query_page();
        assert!(html.contains("/druid/v2/sql"));
    }

    #[test]
    fn segments_page_fetches_datasources() {
        let html = render_segments_page();
        assert!(html.contains("/druid/coordinator/v1/datasources"));
    }

    #[test]
    fn supervisors_page_fetches_supervisor_api() {
        let html = render_supervisors_page();
        assert!(html.contains("/druid/indexer/v1/supervisor"));
    }

    #[test]
    fn tasks_page_fetches_task_apis() {
        let html = render_tasks_page();
        assert!(html.contains("/druid/indexer/v1/runningTasks"));
        assert!(html.contains("/druid/indexer/v1/completeTasks"));
    }

    #[test]
    fn all_pages_valid_html() {
        for html in [
            render_console_html(),
            render_datasources_page(),
            render_query_page(),
            render_segments_page(),
            render_supervisors_page(),
            render_tasks_page(),
        ] {
            assert!(html.contains("<!DOCTYPE html>"));
            assert!(html.contains("<html"));
            assert!(html.contains("</html>"));
            assert!(html.contains("<body>"));
            assert!(html.contains("</body>"));
        }
    }

    #[test]
    fn query_page_has_keyboard_shortcut() {
        let html = render_query_page();
        assert!(html.contains("ctrlKey") || html.contains("metaKey"));
        assert!(html.contains("Enter"));
    }

    #[test]
    fn pages_have_auto_refresh() {
        let html = render_datasources_page();
        assert!(html.contains("refresh-interval"));
        assert!(html.contains("setRefreshInterval"));
    }

    #[test]
    fn console_has_sidebar() {
        let html = render_console_html();
        assert!(html.contains("sidebar"));
    }

    // -----------------------------------------------------------------------
    // XSS regression guards (customer-facing security fix)
    // -----------------------------------------------------------------------

    #[test]
    fn every_page_ships_the_esc_helper() {
        for html in [
            render_console_html(),
            render_datasources_page(),
            render_query_page(),
            render_segments_page(),
            render_supervisors_page(),
            render_tasks_page(),
        ] {
            assert!(
                html.contains("function esc("),
                "page is missing the HTML-escaping helper"
            );
        }
    }

    #[test]
    fn segments_page_escapes_reflected_ds_param() {
        // Finding #1 (reflected XSS): the `?ds=` value must be escaped, never
        // concatenated raw into the `Segments for:` header.
        let html = render_segments_page();
        assert!(
            html.contains("esc(ds)"),
            "reflected ?ds= value must be routed through esc()"
        );
        assert!(
            !html.contains("'Segments for: ' + ds +"),
            "raw unescaped ?ds= concatenation must be gone"
        );
    }

    #[test]
    fn query_results_escape_columns_and_cells() {
        // Finding #2 (stored XSS): SQL column names and cell values are the
        // realistic path — a customer's own data lands here.
        let html = render_query_page();
        assert!(html.contains("esc(c)"), "column headers must be escaped");
        assert!(html.contains("esc(row[c])"), "cell values must be escaped");
    }

    #[test]
    fn supervisor_button_has_no_inline_onclick_injection() {
        // Finding #5: the supervisor id must not be spliced into an inline
        // onclick JS string (a JS-context injection HTML-escaping can't stop).
        let html = render_supervisors_page();
        assert!(
            !html.contains("onclick=\"shutdownSupervisor"),
            "inline onclick with interpolated id must be gone"
        );
        assert!(
            html.contains("data-sup-id=\"' + esc(id) + '\""),
            "supervisor id must be carried in an escaped data-* attribute"
        );
        assert!(
            html.contains("addEventListener"),
            "shutdown must be wired via addEventListener, not inline onclick"
        );
    }

    #[test]
    fn pages_declare_inline_favicon() {
        // No /favicon.ico request (and thus no 404 console error): the icon is
        // an inline data-URI link in <head>.
        let html = render_console_html();
        assert!(html.contains("rel=\"icon\""));
        assert!(html.contains("data:image/svg+xml"));
    }
}
