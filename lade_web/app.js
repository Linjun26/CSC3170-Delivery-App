// ============================================================
// LaDe Web · Application JS
// ============================================================

let DB = null;   // sql.js Database instance

// ---------- Boot ----------
async function boot() {
  try {
    setStatus("loading", "Initializing SQLite engine...");
    const SQL = await initSqlJs({
      locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.10.3/${file}`
    });

    setStatus("loading", "Fetching lade.sqlite...");
    const resp = await fetch("lade.sqlite");
    if (!resp.ok) throw new Error("Could not fetch lade.sqlite");
    const buf = await resp.arrayBuffer();

    DB = new SQL.Database(new Uint8Array(buf));
    setStatus("ready", "SQLite ready · 6 tables loaded");

    // Fire up all panels
    renderStats();
    renderDataBrowser();
    renderPresetQueries();
    renderAnalytics();
    wireLLM();

    setTimeout(() => document.getElementById("loading-overlay").classList.add("hidden"), 300);
  } catch (err) {
    console.error(err);
    setStatus("err", "Failed to load · " + err.message);
    document.getElementById("loading-overlay").innerHTML =
      `<div class="overlay-text" style="color:#c53030;">Failed to load: ${err.message}<br><br>Please serve via a local HTTP server (e.g. <code>python -m http.server</code>) — not <code>file://</code></div>`;
  }
}

function setStatus(state, text) {
  const el = document.getElementById("db-status");
  el.classList.remove("loading", "ready", "err");
  el.classList.add(state);
  document.getElementById("db-status-text").textContent = text;
}

// Helper: exec SQL and return {cols, rows}
function query(sql) {
  const res = DB.exec(sql);
  if (res.length === 0) return { cols: [], rows: [] };
  return { cols: res[0].columns, rows: res[0].values };
}

// ============ TAB SWITCHING ============
document.getElementById("tabs").addEventListener("click", e => {
  const btn = e.target.closest(".tab");
  if (!btn) return;
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById("panel-" + btn.dataset.panel).classList.add("active");
});

// ============ STATS CARDS ============
function renderStats() {
  const stats = [
    { label: "Total Couriers",    sql: "SELECT COUNT(*) FROM Couriers",              unit: "" },
    { label: "Total AOIs",        sql: "SELECT COUNT(*) FROM AOI_Master",            unit: "" },
    { label: "Pickup Orders",     sql: "SELECT COUNT(*) FROM Pickup_Orders",         unit: "" },
    { label: "Delivery Orders",   sql: "SELECT COUNT(*) FROM Delivery_Orders",       unit: "" },
    { label: "GPS Trajectories",  sql: "SELECT COUNT(*) FROM Courier_Trajectories",  unit: "pts" },
    { label: "Road Segments",     sql: "SELECT COUNT(*) FROM Road_Network",          unit: "" },
    { label: "Avg Delivery Time",
      sql: "SELECT ROUND(AVG((julianday(delivery_time)-julianday(accept_time))*1440), 0) FROM Delivery_Orders WHERE delivery_time IS NOT NULL",
      unit: "min" },
    { label: "Active Days",
      sql: "SELECT COUNT(DISTINCT ds) FROM Delivery_Orders",
      unit: "d" },
  ];
  const grid = document.getElementById("stat-grid");
  grid.innerHTML = stats.map(s => {
    const v = query(s.sql).rows[0]?.[0] ?? "-";
    return `<div class="stat">
      <div class="stat-label">${s.label}</div>
      <div class="stat-value">${formatNum(v)}<span class="stat-unit">${s.unit}</span></div>
    </div>`;
  }).join("");
}

function formatNum(n) {
  if (n === null || n === undefined) return "-";
  if (typeof n === "number" && n >= 1000) return n.toLocaleString();
  return String(n);
}

// ============ DATA BROWSER ============
const TABLES = [
  "Couriers",
  "AOI_Master",
  "Pickup_Orders",
  "Delivery_Orders",
  "Courier_Trajectories",
  "Road_Network",
];

function renderDataBrowser() {
  const picker = document.getElementById("table-picker");
  const counts = {};
  TABLES.forEach(t => counts[t] = query(`SELECT COUNT(*) FROM ${t}`).rows[0][0]);

  picker.innerHTML = TABLES.map((t, i) => `
    <button class="chip ${i === 0 ? 'active' : ''}" data-table="${t}">
      ${t}<span class="count">${counts[t].toLocaleString()}</span>
    </button>
  `).join("");

  picker.addEventListener("click", e => {
    const chip = e.target.closest(".chip");
    if (!chip) return;
    picker.querySelectorAll(".chip").forEach(c => c.classList.remove("active"));
    chip.classList.add("active");
    loadTable(chip.dataset.table);
  });

  loadTable(TABLES[0]);
}

function loadTable(table) {
  const { cols, rows } = query(`SELECT * FROM ${table} LIMIT 100`);
  const thead = document.querySelector("#data-table thead");
  const tbody = document.querySelector("#data-table tbody");
  thead.innerHTML = "<tr>" + cols.map(c => `<th>${c}</th>`).join("") + "</tr>";
  tbody.innerHTML = rows.map(r =>
    "<tr>" + r.map(v => {
      let s = v === null ? "<span style='color:#aaa;'>NULL</span>" : String(v);
      if (s.length > 60) s = s.slice(0, 60) + "…";
      return `<td>${s}</td>`;
    }).join("") + "</tr>"
  ).join("");
  const total = query(`SELECT COUNT(*) FROM ${table}`).rows[0][0];
  document.getElementById("data-meta").textContent =
    `Showing first ${rows.length} of ${total.toLocaleString()} rows · table: ${table}`;
}

// ============ SQL CONSOLE ============
const PRESETS = [
  {
    group:  "Operational · Daily Use ",
    queries: [
      {
        title:  "Q1 · Urgent SLA Breach Alert ",
        desc:  "Real-time system-wide monitor for orders approaching their 30-minute deadline that remain incomplete. ",
        sql:  `-- Reference Time: '2023-08-21 14:30:00' (Replace with datetime('now') in production)
SELECT 'Pickup' as order_type, po.order_id, po.courier_id, po.time_window_end, am.aoi_type,
CAST((strftime('%s', po.time_window_end) - strftime('%s', '2023-08-21 14:30:00')) / 60.0 AS INTEGER) as minutes_remaining
FROM Pickup_Orders po JOIN AOI_Master am ON po.aoi_id = am.aoi_id
WHERE po.pickup_time IS NULL AND am.city = 'Jilin' AND po.ds = '821'
AND strftime('%s', po.time_window_end) - strftime('%s', '2023-08-21 14:30:00') <= 1800
AND strftime('%s', po.time_window_end) - strftime('%s', '2023-08-21 14:30:00') > 0
UNION ALL
SELECT 'Delivery' as order_type, do.order_id, do.courier_id, do.delivery_time as time_window_end, am.aoi_type,
CAST((strftime('%s', do.delivery_time) - strftime('%s', '2023-08-21 14:30:00')) / 60.0 AS INTEGER) as minutes_remaining
FROM Delivery_Orders do JOIN AOI_Master am ON do.aoi_id = am.aoi_id
WHERE do.delivery_time IS NULL AND am.city = 'Jilin' AND do.ds = '821'
AND strftime('%s', do.delivery_time) - strftime('%s', '2023-08-21 14:30:00') <= 1800
AND strftime('%s', do.delivery_time) - strftime('%s', '2023-08-21 14:30:00') > 0
ORDER BY minutes_remaining ASC;`
      },
      {
        title:  "Q2 · Courier Immediate Workload Window (1-Hour View) ",
        desc:  "View all tasks accepted within the last hour or due within the next hour for immediate route planning. ",
        sql:  `SELECT 'Pickup' as task_type, po.order_id, po.accept_time, po.time_window_end, am.aoi_type
FROM Pickup_Orders po JOIN AOI_Master am ON po.aoi_id = am.aoi_id
WHERE po.courier_id = '393' AND am.city = 'Jilin' AND po.ds = '821'
AND (strftime('%s', po.accept_time) >= strftime('%s', '2023-08-21 14:30:00') - 3600
OR strftime('%s', po.time_window_end) BETWEEN strftime('%s', '2023-08-21 14:30:00') AND strftime('%s', '2023-08-21 14:30:00') + 3600)
UNION ALL
SELECT 'Delivery' as task_type, do.order_id, do.accept_time, do.delivery_time, am.aoi_type
FROM Delivery_Orders do JOIN AOI_Master am ON do.aoi_id = am.aoi_id
WHERE do.courier_id = '393' AND am.city = 'Jilin' AND do.ds = '821'
AND (strftime('%s', do.accept_time) >= strftime('%s', '2023-08-21 14:30:00') - 3600
OR strftime('%s', do.accept_time) BETWEEN strftime('%s', '2023-08-21 14:30:00') AND strftime('%s', '2023-08-21 14:30:00') + 3600)
ORDER BY accept_time DESC;`
      },
      {
        title:  "Q3 · Live Courier Location & Signal Freshness ",
        desc:  "Retrieve the most recent GPS ping and calculate minutes since last signal to detect offline devices. ",
        sql:  `SELECT ct.courier_id, ct.gps_time, ct.lat, ct.lng,
CAST((strftime('%s', '2023-08-21 14:30:00') - strftime('%s', ct.gps_time)) / 60.0 AS INTEGER) as minutes_ago
FROM Courier_Trajectories ct JOIN Couriers c ON ct.courier_id = c.courier_id
WHERE ct.courier_id = '393' AND c.city_base = 'Jilin' AND ct.ds = '821'
ORDER BY ct.gps_time DESC LIMIT 1;`
      },
      {
        title:  "Q4 · Order Execution Trajectory Audit ",
        desc:  "Investigate courier movement strictly between order acceptance and pickup times for delay verification. ",
        sql:  `-- Part A: Order Static Information
SELECT 'Order_Info' as record_type, po.order_id, po.courier_id, am.aoi_type, am.city, po.accept_time, po.pickup_time
FROM Pickup_Orders po JOIN AOI_Master am ON po.aoi_id = am.aoi_id
WHERE po.order_id = '2444404' AND am.city = 'Jilin';

-- Part B: Trajectory During Execution
SELECT 'Trajectory_Point' as record_type, ct.gps_time, ct.lat, ct.lng
FROM Courier_Trajectories ct JOIN Pickup_Orders po ON ct.courier_id = po.courier_id
WHERE po.order_id = '2444404' AND ct.gps_time BETWEEN po.accept_time AND po.pickup_time
ORDER BY ct.gps_time ASC;`
      }
    ]
  },
  {
    group:  "Analytic · Data Mining ",
    queries: [
      {
        title:  "Q5 · Courier Performance & Punctuality Ranking ",
        desc:  "Rank couriers by composite score (40% efficiency, 40% punctuality, 20% volume) for bonus calculations. ",
        sql:  `SELECT c.courier_id,
COUNT(DISTINCT po.order_id) + COUNT(DISTINCT do.order_id) as total_tasks,
AVG(CASE WHEN po.pickup_time IS NOT NULL
THEN (strftime('%s', po.pickup_time) - strftime('%s', po.accept_time)) / 60.0 ELSE NULL END) as avg_completion_minutes,
SUM(CASE WHEN po.pickup_time <= po.time_window_end THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(po.order_id), 0) as punctuality_rate,
( (SUM(CASE WHEN po.pickup_time <= po.time_window_end THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(po.order_id), 0)) * 0.4 +
  (100.0 / (1.0 + AVG(CASE WHEN po.pickup_time IS NOT NULL THEN (strftime('%s', po.pickup_time) - strftime('%s', po.accept_time)) / 60.0 ELSE 30.0 END))) * 0.4 +
  (MIN(COUNT(DISTINCT po.order_id) + COUNT(DISTINCT do.order_id), 50) / 50.0 * 100) * 0.2 ) as performance_score
FROM Couriers c
LEFT JOIN Pickup_Orders po ON c.courier_id = po.courier_id AND po.ds = '821'
LEFT JOIN Delivery_Orders do ON c.courier_id = do.courier_id AND do.ds = '821'
WHERE c.city_base = 'Jilin'
GROUP BY c.courier_id HAVING total_tasks > 0 ORDER BY performance_score DESC;`
      },
      {
        title:  "Q6 · Regional & AOI Efficiency Benchmarking ",
        desc:  "Compare operational performance across different AOI types using completion time and on-time compliance rates. ",
        sql:  `SELECT am.aoi_type, COUNT(po.order_id) as total_pickups,
AVG(CASE WHEN po.pickup_time IS NOT NULL
THEN (strftime('%s', po.pickup_time) - strftime('%s', po.accept_time)) / 60.0 ELSE NULL END) as avg_completion_minutes,
SUM(CASE WHEN po.pickup_time IS NOT NULL AND po.pickup_time <= po.time_window_end THEN 1 ELSE 0 END) * 100.0
/ NULLIF(SUM(CASE WHEN po.pickup_time IS NOT NULL THEN 1 ELSE 0 END), 0) as on_time_rate
FROM Pickup_Orders po JOIN AOI_Master am ON po.aoi_id = am.aoi_id
WHERE po.ds = '821' AND am.city = 'Jilin'
GROUP BY am.aoi_type HAVING total_pickups >= 5 ORDER BY avg_completion_minutes DESC;`
      },
      {
        title:  "Q7 · Peak Demand Hour Identification ",
        desc:  "Identify hours exceeding 150% of the daily average order volume to optimize shift scheduling. ",
        sql:  `WITH hourly_stats AS (
  SELECT strftime('%H', po.accept_time) as hour_of_day, COUNT(*) as order_count
  FROM Pickup_Orders po JOIN AOI_Master am ON po.aoi_id = am.aoi_id
  WHERE po.ds = '821' AND am.city = 'Jilin' GROUP BY strftime('%H', po.accept_time)
),
daily_average AS (
  SELECT AVG(order_count) as avg_hourly_orders FROM hourly_stats
)
SELECT hs.hour_of_day, hs.order_count,
CAST(hs.order_count * 100.0 / da.avg_hourly_orders AS INTEGER) as volume_percentage,
CASE WHEN hs.order_count > da.avg_hourly_orders * 1.5 THEN 'PEAK'
     WHEN hs.order_count < da.avg_hourly_orders * 0.5 THEN 'LOW'
     ELSE 'NORMAL' END as demand_level
FROM hourly_stats hs, daily_average da WHERE hs.order_count >= 5 ORDER BY hs.order_count DESC;`
      },
      {
        title:  "Q8 · Service Coverage Gap Analysis ",
        desc:  "Detect AOIs in Jilin with no recent pickup or delivery orders to identify potential market gaps. ",
        sql:  `SELECT am.aoi_id, am.aoi_type, 'No Orders' as coverage_status
FROM AOI_Master am
WHERE am.city = 'Jilin' AND am.aoi_type NOT IN ('warehouse', 'depot')
AND am.aoi_id NOT IN (
  SELECT DISTINCT aoi_id FROM Pickup_Orders WHERE ds = '821'
  UNION
  SELECT DISTINCT aoi_id FROM Delivery_Orders WHERE ds = '821'
)
ORDER BY am.aoi_type;`
      }
    ]
  },
  {
    group:  "Schema · Metadata ",
    queries: [
      {
        title:  "Inspect all tables & row counts ",
        desc:  "Query the system catalog to verify table structures and dataset sizes. ",
        sql:  `SELECT name AS table_name,
(SELECT COUNT(*) FROM Couriers) AS n1,
(SELECT COUNT(*) FROM AOI_Master) AS n2,
(SELECT COUNT(*) FROM Pickup_Orders) AS n3,
(SELECT COUNT(*) FROM Delivery_Orders) AS n4,
(SELECT COUNT(*) FROM Courier_Trajectories) AS n5,
(SELECT COUNT(*) FROM Road_Network) AS n6
FROM sqlite_master WHERE type='table' LIMIT 1;`
      },
      {
        title:  "List declared indexes ",
        desc:  "Verify that index plans are correctly in place for query optimization. ",
        sql:  `SELECT name AS index_name, tbl_name AS on_table, sql
FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, name;`
      }
    ]
  }
];

function renderPresetQueries() {
  const list = document.getElementById("preset-list");
  let html = "";
  PRESETS.forEach(group => {
    html += `<div class="preset-tag" style="margin-top:10px;">${group.group}</div>`;
    group.queries.forEach(q => {
      html += `<button class="preset" data-sql='${encodeURIComponent(q.sql)}'>
        <div style="font-weight:500; color: var(--navy); margin-bottom:2px;">${q.title}</div>
        <div style="font-size: 11.5px; color: var(--muted);">${q.desc}</div>
      </button>`;
    });
  });
  list.innerHTML = html;

  list.addEventListener("click", e => {
    const btn = e.target.closest(".preset");
    if (!btn) return;
    list.querySelectorAll(".preset").forEach(p => p.classList.remove("active"));
    btn.classList.add("active");
    const sql = decodeURIComponent(btn.dataset.sql);
    document.getElementById("sql-editor").value = sql;
    runSQL();
  });

  // Prefill with first query
  const first = PRESETS[0].queries[0];
  document.getElementById("sql-editor").value = first.sql;
  list.querySelector(".preset").classList.add("active");
  runSQL();

  document.getElementById("btn-run").addEventListener("click", runSQL);
  document.getElementById("btn-clear").addEventListener("click", () => {
    document.getElementById("sql-editor").value = "";
    document.getElementById("sql-result").innerHTML = "";
  });
  document.getElementById("sql-editor").addEventListener("keydown", e => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runSQL();
    }
  });
}

function runSQL() {
  const sql = document.getElementById("sql-editor").value.trim();
  const result = document.getElementById("sql-result");
  const status = document.getElementById("sql-status");
  if (!sql) { result.innerHTML = ""; return; }

  const t0 = performance.now();
  try {
    const { cols, rows } = query(sql);
    const ms = (performance.now() - t0).toFixed(1);
    status.className = "sql-status ok";
    status.textContent = `✓ ${rows.length} rows · ${ms} ms`;

    if (cols.length === 0) {
      result.innerHTML = `<div class="card"><em style="color: var(--muted);">Query executed, no rows returned.</em></div>`;
      return;
    }

    result.innerHTML = `
      <div class="table-wrap" style="max-height: 440px;">
        <table class="data">
          <thead><tr>${cols.map(c => `<th>${c}</th>`).join("")}</tr></thead>
          <tbody>${rows.map(r =>
            "<tr>" + r.map(v => {
              let s = v === null ? "<span style='color:#aaa;'>NULL</span>" : String(v);
              if (s.length > 80) s = s.slice(0, 80) + "…";
              return `<td>${s}</td>`;
            }).join("") + "</tr>"
          ).join("")}</tbody>
        </table>
      </div>`;
  } catch (err) {
    status.className = "sql-status err";
    status.textContent = "✗ Error";
    result.innerHTML = `<div class="err-box">${err.message}</div>`;
  }
}

// ============ ANALYTICS CHARTS ============
function renderAnalytics() {
  const chartColors = {
    accent: "#ff6b2c",
    accentSoft: "rgba(255,107,44,0.15)",
    navy: "#0b1f3a",
    navy2: "#1a365f",
    line: "#d9d3c6",
  };

  Chart.defaults.font.family = "'Inter', sans-serif";
  Chart.defaults.font.size = 11;
  Chart.defaults.color = "#3b4a63";

  // ----- 1. Hour distribution -----
  const hourData = query(`
    SELECT CAST(substr(accept_time, 12, 2) AS INTEGER) AS h, COUNT(*)
    FROM Delivery_Orders GROUP BY h ORDER BY h
  `);
  new Chart(document.getElementById("chart-hour"), {
    type: "bar",
    data: {
      labels: hourData.rows.map(r => r[0] + ":00"),
      datasets: [{
        data: hourData.rows.map(r => r[1]),
        backgroundColor: chartColors.accent,
        borderRadius: 3,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: chartColors.line } }
      }
    }
  });

  // ----- 2. AOI type distribution -----
  const aoiData = query(`
    SELECT a.aoi_type, COUNT(*) AS c
    FROM Delivery_Orders d JOIN AOI_Master a ON d.aoi_id = a.aoi_id
    GROUP BY a.aoi_type ORDER BY c DESC LIMIT 8
  `);
  const palette = ["#ff6b2c", "#0b1f3a", "#1a365f", "#2f7d5b", "#b77500", "#7c8599", "#d9501a", "#3b4a63"];
  new Chart(document.getElementById("chart-aoi"), {
    type: "doughnut",
    data: {
      labels: aoiData.rows.map(r => "Type " + r[0]),
      datasets: [{
        data: aoiData.rows.map(r => r[1]),
        backgroundColor: palette,
        borderWidth: 2,
        borderColor: "#fff",
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: "right", labels: { boxWidth: 12, padding: 8 } } },
      cutout: "55%"
    }
  });

  // ----- 3. Top 10 couriers -----
  const courierData = query(`
    SELECT courier_id, COUNT(*) AS c FROM Delivery_Orders
    GROUP BY courier_id ORDER BY c DESC LIMIT 10
  `);
  new Chart(document.getElementById("chart-courier"), {
    type: "bar",
    data: {
      labels: courierData.rows.map(r => "#" + r[0]),
      datasets: [{
        data: courierData.rows.map(r => r[1]),
        backgroundColor: chartColors.navy,
        borderRadius: 3,
      }]
    },
    options: {
      indexAxis: "y",
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { color: chartColors.line } },
        y: { grid: { display: false } }
      }
    }
  });

  // ----- 4. Daily trend (by ds) -----
  const dailyData = query(`
    SELECT ds, COUNT(*) FROM Delivery_Orders
    GROUP BY ds ORDER BY ds
  `);
  new Chart(document.getElementById("chart-daily"), {
    type: "line",
    data: {
      labels: dailyData.rows.map(r => r[0]),
      datasets: [{
        data: dailyData.rows.map(r => r[1]),
        borderColor: chartColors.accent,
        backgroundColor: chartColors.accentSoft,
        fill: true,
        tension: 0.3,
        pointRadius: 0,
        borderWidth: 2,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 10 } },
        y: { grid: { color: chartColors.line } }
      }
    }
  });

  // ----- 5. Delivery duration histogram -----
  const etaData = query(`
    SELECT
      CAST((julianday(delivery_time)-julianday(accept_time))*1440 / 30 AS INTEGER) * 30 AS bucket,
      COUNT(*) AS c
    FROM Delivery_Orders
    WHERE delivery_time IS NOT NULL
      AND (julianday(delivery_time)-julianday(accept_time))*1440 BETWEEN 0 AND 600
    GROUP BY bucket ORDER BY bucket
  `);
  new Chart(document.getElementById("chart-eta"), {
    type: "bar",
    data: {
      labels: etaData.rows.map(r => r[0] + "-" + (r[0] + 30)),
      datasets: [{
        data: etaData.rows.map(r => r[1]),
        backgroundColor: chartColors.navy2,
        borderRadius: 3,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: chartColors.line } }
      }
    }
  });

  // ----- 6. Pickup vs Delivery by hour -----
  const pickupHour = query(`
    SELECT CAST(substr(accept_time, 12, 2) AS INTEGER) AS h, COUNT(*)
    FROM Pickup_Orders GROUP BY h ORDER BY h
  `);
  const deliveryHour = query(`
    SELECT CAST(substr(accept_time, 12, 2) AS INTEGER) AS h, COUNT(*)
    FROM Delivery_Orders GROUP BY h ORDER BY h
  `);
  // Merge hours
  const allHours = new Set([...pickupHour.rows.map(r => r[0]), ...deliveryHour.rows.map(r => r[0])]);
  const hours = [...allHours].sort((a,b) => a - b);
  const pmap = Object.fromEntries(pickupHour.rows);
  const dmap = Object.fromEntries(deliveryHour.rows);
  new Chart(document.getElementById("chart-pickup-delivery"), {
    type: "line",
    data: {
      labels: hours.map(h => h + ":00"),
      datasets: [
        { label: "Pickup", data: hours.map(h => pmap[h] || 0),
          borderColor: chartColors.accent, backgroundColor: "transparent",
          borderWidth: 2, tension: 0.3, pointRadius: 2 },
        { label: "Delivery", data: hours.map(h => dmap[h] || 0),
          borderColor: chartColors.navy, backgroundColor: "transparent",
          borderWidth: 2, tension: 0.3, pointRadius: 2 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: "top", align: "end" } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: chartColors.line } }
      }
    }
  });
}

// ============ LLM · Scripted Demo (NL → SQL) ============
//
// This is a curated demo cache — 8 representative question/SQL pairs.
// In production we use Anthropic's Claude API behind a server-side proxy
// (the SCHEMA_PROMPT below is the actual system prompt sent to Claude).
// For an offline classroom presentation, we cache responses to guarantee
// reliability and avoid network/API-key dependencies during the demo.
//
// When the user types a question, we do fuzzy keyword matching against
// the cache; if nothing matches we honestly tell the user.

const SCHEMA_PROMPT = `You are a SQL expert helping users query a SQLite database of last-mile delivery data.

TABLES (SQLite dialect — use julianday() for time diffs, NOT TIMESTAMPDIFF):

Couriers(courier_id TEXT PK, city_base TEXT)
AOI_Master(aoi_id TEXT PK, region_id TEXT, city TEXT, aoi_type TEXT)
Pickup_Orders(order_id TEXT PK, courier_id FK, aoi_id FK,
  stop_lng, stop_lat, time_window_start, time_window_end,
  accept_time, pickup_time, ds, ...)
Delivery_Orders(order_id TEXT PK, courier_id FK, aoi_id FK,
  stop_lng, stop_lat, accept_time, delivery_time, ds, ...)
Courier_Trajectories(trajectory_id INT PK, courier_id FK, gps_time, lat, lng, ds)
Road_Network(road_id TEXT PK, fclass, name, oneway, maxspeed, geometry, ...)

NOTES:
- time fields are TEXT in 'YYYY-MM-DD HH:MM:SS' format
- ds is a date tag like '821' (8/21) or '1015' (10/15) — no year
- For time differences use: (julianday(t2) - julianday(t1)) * 1440 to get minutes
- Data is from Jilin city only

OUTPUT RULES:
- Return ONLY the raw SQL query, no markdown code blocks, no explanation
- Add a LIMIT clause (default LIMIT 50) unless the user asks for all rows or an aggregate
- Use meaningful column aliases`;

const DEMO_CACHE = [
  {
    keywords: ["top", "5", "courier", "most", "order", "best", "busiest"],
    question: "Who are the top 5 couriers by order count?",
    sql: `-- Aggregate orders per courier and rank
SELECT courier_id,
       COUNT(*) AS total_orders
FROM Delivery_Orders
GROUP BY courier_id
ORDER BY total_orders DESC
LIMIT 5;`
  },
  {
    keywords: ["aoi", "type", "most", "popular", "common", "category"],
    question: "Which AOI type has the most orders?",
    sql: `-- JOIN orders with AOI dimension and group by type
SELECT a.aoi_type,
       COUNT(*) AS orders
FROM Delivery_Orders d
JOIN AOI_Master a ON d.aoi_id = a.aoi_id
GROUP BY a.aoi_type
ORDER BY orders DESC
LIMIT 5;`
  },
  {
    keywords: ["august", "21", "8/21", "08-21", "821", "many", "delivery", "day", "date"],
    question: "How many delivery orders were placed on August 21?",
    sql: `-- Filter by date tag (ds) for August 21
SELECT COUNT(*) AS total_orders
FROM Delivery_Orders
WHERE ds = '821';`
  },
  {
    keywords: ["4849", "distinct", "aoi", "courier", "different", "many"],
    question: "How many distinct AOIs has courier 4849 served?",
    sql: `-- COUNT DISTINCT to measure service area diversity
SELECT COUNT(DISTINCT aoi_id) AS distinct_aois
FROM Delivery_Orders
WHERE courier_id = '4849';`
  },
  {
    keywords: ["region", "average", "avg", "delivery", "time", "minute", "duration", "per"],
    question: "What is the average delivery time (in minutes) per region?",
    sql: `-- Time difference via julianday() * 1440 to convert days→minutes
SELECT a.region_id,
       ROUND(AVG((julianday(d.delivery_time) - julianday(d.accept_time)) * 1440), 1) AS avg_minutes,
       COUNT(*) AS order_count
FROM Delivery_Orders d
JOIN AOI_Master a ON d.aoi_id = a.aoi_id
WHERE d.delivery_time IS NOT NULL
GROUP BY a.region_id
ORDER BY avg_minutes DESC;`
  },
  {
    keywords: ["busiest", "hour", "peak", "rush", "time", "of", "day", "when"],
    question: "What are the busiest hours of the day?",
    sql: `-- Extract hour from accept_time and rank
SELECT CAST(substr(accept_time, 12, 2) AS INTEGER) AS hour_of_day,
       COUNT(*) AS orders
FROM Delivery_Orders
GROUP BY hour_of_day
ORDER BY orders DESC
LIMIT 5;`
  },
  {
    keywords: ["slow", "slowest", "longest", "courier", "delivery", "time", "average", "long"],
    question: "Which couriers have the longest average delivery time?",
    sql: `-- Use HAVING to filter out couriers with too few orders
SELECT courier_id,
       ROUND(AVG((julianday(delivery_time) - julianday(accept_time)) * 1440), 1) AS avg_minutes,
       COUNT(*) AS orders
FROM Delivery_Orders
WHERE delivery_time IS NOT NULL
GROUP BY courier_id
HAVING orders >= 50
ORDER BY avg_minutes DESC
LIMIT 10;`
  },
  {
    keywords: ["september", "sep", "9", "month", "courier", "many", "each"],
    question: "How many orders did each courier handle in September?",
    sql: `-- ds starts with '9' and has length 3 → September dates
SELECT courier_id,
       COUNT(*) AS september_orders
FROM Delivery_Orders
WHERE ds LIKE '9%' AND length(ds) = 3
GROUP BY courier_id
ORDER BY september_orders DESC
LIMIT 10;`
  },
];

function findBestMatch(userQuestion) {
  const q = userQuestion.toLowerCase();
  let bestEntry = null;
  let bestScore = 0;

  for (const entry of DEMO_CACHE) {
    let score = 0;
    for (const kw of entry.keywords) {
      if (q.includes(kw.toLowerCase())) score++;
    }
    if (score > bestScore) {
      bestScore = score;
      bestEntry = entry;
    }
  }
  // require at least 2 keyword hits to be confident
  return bestScore >= 2 ? bestEntry : null;
}

function wireLLM() {
  // Suggestion clicks just fill the input
  document.querySelectorAll(".suggestion").forEach(s => {
    s.addEventListener("click", () => {
      document.getElementById("nl-input").value = s.textContent.trim();
    });
  });

  document.getElementById("btn-nl").addEventListener("click", runLLM);

  // Cmd/Ctrl + Enter to submit
  document.getElementById("nl-input").addEventListener("keydown", e => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
      e.preventDefault();
      runLLM();
    }
  });
}

// Backend URL. When the Flask backend (backend.py) is running, we hit this
// endpoint for live DeepSeek-powered SQL generation. If the call fails
// (backend down, network issue, API key invalid), we transparently fall
// back to the DEMO_CACHE — this is the "fault-tolerant" architecture we
// describe in the report.
const BACKEND_URL = "http://localhost:5001/api/nl2sql";

async function tryBackend(question) {
  // Short timeout — if the backend is down we want to fail fast and use cache.
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000);
  try {
    const resp = await fetch(BACKEND_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ question }),
      signal: controller.signal,
    });
    clearTimeout(timeoutId);
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.error || `HTTP ${resp.status}`);
    }
    const data = await resp.json();
    return data.sql;
  } catch (err) {
    clearTimeout(timeoutId);
    throw err;
  }
}

async function runLLM() {
  const input = document.getElementById("nl-input").value.trim();
  if (!input) return;
  const status = document.getElementById("nl-status");
  const sqlBox = document.getElementById("generated-sql");
  const resultBox = document.getElementById("nl-result");

  status.className = "sql-status";
  status.textContent = "Calling DeepSeek...";
  sqlBox.innerHTML =
    '<div class="loading-shimmer" style="width: 90%;"></div>' +
    '<div class="loading-shimmer" style="width: 70%;"></div>' +
    '<div class="loading-shimmer" style="width: 85%;"></div>';
  resultBox.innerHTML = "";

  let sql = null;
  let source = "live";   // "live" = DeepSeek, "cache" = fallback

  // Try the live backend first
  try {
    sql = await tryBackend(input);
  } catch (err) {
    console.warn("Backend unavailable, falling back to cache:", err.message);
    // Fall back to the curated cache
    const match = findBestMatch(input);
    if (match) {
      sql = match.sql;
      source = "cache";
    } else {
      // Neither live nor cache — honest error message
      status.className = "sql-status err";
      status.textContent = "✗ Backend offline + no cache match";
      sqlBox.textContent =
        "// The backend (backend.py) appears to be offline, and this question\n" +
        "// doesn't match any entry in the offline fallback cache.\n" +
        "//\n" +
        "// To enable live DeepSeek generation, start the backend:\n" +
        "//     python3 backend.py\n" +
        "//\n" +
        "// Or try one of the example questions on the left.";
      resultBox.innerHTML =
        `<div class="err-box" style="background:#fff8e6; border-color:#f0c674; color:#7a5b1a;">` +
        `Backend error: <code>${err.message}</code>. No cache match either — paraphrase using keywords like ` +
        `<code>courier</code>, <code>AOI type</code>, <code>region</code>, <code>busiest hour</code>, etc.` +
        `</div>`;
      return;
    }
  }

  // Render the SQL (with a small tag indicating source)
  const sourceTag = source === "live"
    ? '\n-- Generated live by DeepSeek V3.2'
    : '\n-- Served from offline fallback cache (backend unreachable)';
  sqlBox.textContent = sql + sourceTag;

  // Execute the SQL against sql.js
  status.textContent = "Executing...";
  const t0 = performance.now();
  try {
    const { cols, rows } = query(sql);
    const ms = (performance.now() - t0).toFixed(1);
    status.className = "sql-status ok";
    const label = source === "live" ? "live" : "cached";
    status.textContent = `✓ ${rows.length} rows · ${ms} ms · ${label}`;

    if (cols.length === 0) {
      resultBox.innerHTML = `<em style="color: var(--muted);">Query executed — no rows returned.</em>`;
    } else {
      resultBox.innerHTML = `
        <div class="table-wrap" style="max-height: 380px;">
          <table class="data">
            <thead><tr>${cols.map(c => `<th>${c}</th>`).join("")}</tr></thead>
            <tbody>${rows.map(r =>
              "<tr>" + r.map(v => {
                let s = v === null ? "<span style='color:#aaa;'>NULL</span>" : String(v);
                if (s.length > 60) s = s.slice(0, 60) + "…";
                return `<td>${s}</td>`;
              }).join("") + "</tr>"
            ).join("")}</tbody>
          </table>
        </div>`;
    }
  } catch (err) {
    status.className = "sql-status err";
    status.textContent = "✗ SQL execution error";
    resultBox.innerHTML = `<div class="err-box">Generated SQL failed to execute:\n${err.message}</div>`;
  }
}

// ============ Entity detail (bonus polish) ============
document.querySelectorAll(".entity").forEach(el => {
  el.addEventListener("click", () => {
    const table = el.dataset.entity;
    // Jump to data browser
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".panel").forEach(p => p.classList.remove("active"));
    document.querySelector('[data-panel="data"]').classList.add("active");
    document.getElementById("panel-data").classList.add("active");
    // Select the matching chip
    const chip = document.querySelector(`.chip[data-table="${table}"]`);
    if (chip) {
      document.querySelectorAll(".chip").forEach(c => c.classList.remove("active"));
      chip.classList.add("active");
      loadTable(table);
    }
  });
});

// ============ Boot! ============
boot();
