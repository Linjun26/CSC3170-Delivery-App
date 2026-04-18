from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


APP_DIR = Path(__file__).resolve().parent
DEFAULT_DB_PATH = APP_DIR / "data" / "sh_2026_05.db"

DEFAULT_PICKUP_CSV = "/Users/wangxiaoxi/Desktop/CSC3170_LaDe_Streamlit/pickup_sh_2026-05.csv"
DEFAULT_DELIVERY_CSV = "/Users/wangxiaoxi/Desktop/CSC3170_LaDe_Streamlit/delivery_sh_2026-05-01_15.csv"
DEFAULT_YEAR = 2026
DEFAULT_CITY = "Shanghai"


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")


def _parse_iso(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.fromisoformat(s)


def _ensure_parent_dir(p: Path) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)


@st.cache_resource
def get_conn(db_path: str) -> sqlite3.Connection:
    p = Path(db_path).expanduser()
    _ensure_parent_dir(p)
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS city (
            city_id     INTEGER PRIMARY KEY,
            city_name   TEXT NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS courier (
            courier_id      INTEGER PRIMARY KEY,
            city_id         INTEGER NOT NULL,
            courier_phone   TEXT,
            courier_name    TEXT,
            FOREIGN KEY (city_id) REFERENCES city(city_id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS aoi (
            aoi_id      INTEGER PRIMARY KEY,
            city_id     INTEGER NOT NULL,
            aoi_type    INTEGER,
            FOREIGN KEY (city_id) REFERENCES city(city_id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_key           TEXT PRIMARY KEY,
            order_id            INTEGER NOT NULL,
            scenario            TEXT NOT NULL CHECK (scenario IN ('pickup','delivery')),
            region_id           INTEGER,
            city_id             INTEGER NOT NULL,
            courier_id          INTEGER,
            aoi_id              INTEGER,
            order_lng           REAL,
            order_lat           REAL,
            accept_time         TEXT,
            time_window_start   TEXT,
            time_window_end     TEXT,
            fulfill_time        TEXT,
            ds                  TEXT,
            package_weight      REAL,
            FOREIGN KEY (city_id) REFERENCES city(city_id) ON DELETE RESTRICT,
            FOREIGN KEY (courier_id) REFERENCES courier(courier_id) ON DELETE RESTRICT,
            FOREIGN KEY (aoi_id) REFERENCES aoi(aoi_id) ON DELETE RESTRICT
        );

        CREATE TABLE IF NOT EXISTS gps_event (
            event_id    INTEGER PRIMARY KEY,
            order_key   TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            event_time  TEXT,
            lng         REAL,
            lat         REAL,
            FOREIGN KEY (order_key) REFERENCES orders(order_key) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_orders_scenario_time ON orders(scenario, accept_time);
        CREATE INDEX IF NOT EXISTS idx_orders_region ON orders(region_id);
        CREATE INDEX IF NOT EXISTS idx_orders_courier ON orders(courier_id);
        CREATE INDEX IF NOT EXISTS idx_event_order_time ON gps_event(order_key, event_time);
        """
    )
    courier_cols = {row["name"] for row in conn.execute("PRAGMA table_info(courier)").fetchall()}
    if "courier_phone" not in courier_cols:
        conn.execute("ALTER TABLE courier ADD COLUMN courier_phone TEXT")
    if "courier_name" not in courier_cols:
        conn.execute("ALTER TABLE courier ADD COLUMN courier_name TEXT")

    orders_cols = {row["name"] for row in conn.execute("PRAGMA table_info(orders)").fetchall()}
    if "package_weight" not in orders_cols:
        conn.execute("ALTER TABLE orders ADD COLUMN package_weight REAL")

    conn.commit()


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    return int(conn.execute(f"SELECT COUNT(1) AS c FROM {table}").fetchone()["c"])


def ensure_city(conn: sqlite3.Connection) -> int:
    conn.execute("INSERT OR IGNORE INTO city(city_name) VALUES (?)", (DEFAULT_CITY,))
    conn.commit()
    return int(conn.execute("SELECT city_id FROM city WHERE city_name = ?", (DEFAULT_CITY,)).fetchone()["city_id"])


def clear_imported_data(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        DELETE FROM gps_event;
        DELETE FROM orders;
        DELETE FROM aoi;
        DELETE FROM courier;
        DELETE FROM city;
        """
    )
    conn.commit()


def _parse_dt_to_iso(s: Any, year_hint: int | None = None) -> str | None:
    if s is None:
        return None
    txt = str(s).strip()
    if not txt or txt.lower() == "nan":
        return None

    if "/" in txt:
        for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
            try:
                return datetime.strptime(txt, fmt).isoformat(sep=" ", timespec="seconds")
            except Exception:
                pass

    m = re.match(r"^(\d{2})-(\d{2})\s+(\d{2}:\d{2}:\d{2})$", txt)
    if m and year_hint is not None:
        mm, dd, hms = m.group(1), m.group(2), m.group(3)
        try:
            return datetime.strptime(f"{year_hint}-{mm}-{dd} {hms}", "%Y-%m-%d %H:%M:%S").isoformat(sep=" ", timespec="seconds")
        except Exception:
            return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(txt, fmt).isoformat(sep=" ", timespec="seconds")
        except Exception:
            pass

    return None


def import_csvs(conn: sqlite3.Connection, pickup_csv: str, delivery_csv: str, *, year: int) -> None:
    city_id = ensure_city(conn)

    p = Path(pickup_csv).expanduser()
    d = Path(delivery_csv).expanduser()
    if not p.exists():
        raise FileNotFoundError(str(p))
    if not d.exists():
        raise FileNotFoundError(str(d))

    pickup_df = pd.read_csv(str(p))
    delivery_df = pd.read_csv(str(d))

    pickup_df = pickup_df.fillna("")
    delivery_df = delivery_df.fillna("")

    courier_ids = sorted(set(pickup_df["courier_id"].tolist()) | set(delivery_df["courier_id"].tolist()))
    conn.executemany(
        "INSERT OR IGNORE INTO courier(courier_id, city_id) VALUES (?, ?)",
        [(int(cid), int(city_id)) for cid in courier_ids if str(cid).strip() != ""],
    )

    courier_meta: dict[int, tuple[str, str]] = {}
    for df in (pickup_df, delivery_df):
        if not {"courier_id", "courier_phone", "courier_name"}.issubset(set(df.columns)):
            continue
        for row in df[["courier_id", "courier_phone", "courier_name"]].itertuples(index=False):
            if str(row.courier_id).strip() == "":
                continue
            cid = int(row.courier_id)
            phone = str(row.courier_phone).strip()
            name = str(row.courier_name).strip()
            cur_phone, cur_name = courier_meta.get(cid, ("", ""))
            if phone and not cur_phone:
                cur_phone = phone
            if name and not cur_name:
                cur_name = name
            courier_meta[cid] = (cur_phone, cur_name)

    if courier_meta:
        updates = [(ph, nm, int(cid)) for cid, (ph, nm) in courier_meta.items()]
        conn.executemany(
            """
            UPDATE courier
            SET
                courier_phone = COALESCE(NULLIF(?, ''), courier_phone),
                courier_name = COALESCE(NULLIF(?, ''), courier_name)
            WHERE courier_id = ?
            """,
            updates,
        )

    aoi_pairs = set()
    for df in (pickup_df, delivery_df):
        for row in df[["aoi_id", "aoi_type"]].itertuples(index=False):
            if str(row.aoi_id).strip() == "":
                continue
            aoi_pairs.add((int(row.aoi_id), int(row.aoi_type) if str(row.aoi_type).strip() != "" else None))

    conn.executemany(
        "INSERT OR IGNORE INTO aoi(aoi_id, city_id, aoi_type) VALUES (?, ?, ?)",
        [(aid, int(city_id), atp) for (aid, atp) in sorted(aoi_pairs)],
    )

    order_rows: list[tuple[Any, ...]] = []
    event_rows: list[tuple[Any, ...]] = []

    for r in pickup_df.itertuples(index=False):
        order_id = int(getattr(r, "order_id"))
        order_key = f"pickup:{order_id}"
        order_rows.append(
            (
                order_key,
                order_id,
                "pickup",
                int(getattr(r, "region_id")) if str(getattr(r, "region_id")).strip() != "" else None,
                int(city_id),
                int(getattr(r, "courier_id")) if str(getattr(r, "courier_id")).strip() != "" else None,
                int(getattr(r, "aoi_id")) if str(getattr(r, "aoi_id")).strip() != "" else None,
                float(getattr(r, "lng")) if str(getattr(r, "lng")).strip() != "" else None,
                float(getattr(r, "lat")) if str(getattr(r, "lat")).strip() != "" else None,
                _parse_dt_to_iso(getattr(r, "accept_time"), year_hint=year),
                _parse_dt_to_iso(getattr(r, "time_window_start"), year_hint=year),
                _parse_dt_to_iso(getattr(r, "time_window_end"), year_hint=year),
                _parse_dt_to_iso(getattr(r, "pickup_time"), year_hint=year),
                str(getattr(r, "ds")) if str(getattr(r, "ds")).strip() != "" else None,
                float(getattr(r, "package_weight")) if str(getattr(r, "package_weight")).strip() != "" else None,
            )
        )

        for tp in ("accept", "pickup"):
            t = getattr(r, f"{tp}_gps_time")
            lng = getattr(r, f"{tp}_gps_lng")
            lat = getattr(r, f"{tp}_gps_lat")
            iso = _parse_dt_to_iso(t, year_hint=year)
            if iso is None:
                continue
            event_rows.append(
                (
                    order_key,
                    f"{tp}_gps",
                    iso,
                    float(lng) if str(lng).strip() != "" else None,
                    float(lat) if str(lat).strip() != "" else None,
                )
            )

    for r in delivery_df.itertuples(index=False):
        order_id = int(getattr(r, "order_id"))
        order_key = f"delivery:{order_id}"
        order_rows.append(
            (
                order_key,
                order_id,
                "delivery",
                int(getattr(r, "region_id")) if str(getattr(r, "region_id")).strip() != "" else None,
                int(city_id),
                int(getattr(r, "courier_id")) if str(getattr(r, "courier_id")).strip() != "" else None,
                int(getattr(r, "aoi_id")) if str(getattr(r, "aoi_id")).strip() != "" else None,
                float(getattr(r, "lng")) if str(getattr(r, "lng")).strip() != "" else None,
                float(getattr(r, "lat")) if str(getattr(r, "lat")).strip() != "" else None,
                _parse_dt_to_iso(getattr(r, "accept_time")),
                None,
                None,
                _parse_dt_to_iso(getattr(r, "delivery_time")),
                str(getattr(r, "ds")) if str(getattr(r, "ds")).strip() != "" else None,
                float(getattr(r, "package_weight")) if str(getattr(r, "package_weight")).strip() != "" else None,
            )
        )

        for tp in ("accept", "delivery"):
            t = getattr(r, f"{tp}_gps_time")
            lng = getattr(r, f"{tp}_gps_lng")
            lat = getattr(r, f"{tp}_gps_lat")
            iso = _parse_dt_to_iso(t)
            if iso is None:
                continue
            event_rows.append(
                (
                    order_key,
                    f"{tp}_gps",
                    iso,
                    float(lng) if str(lng).strip() != "" else None,
                    float(lat) if str(lat).strip() != "" else None,
                )
            )

    conn.executemany(
        """
        INSERT OR REPLACE INTO orders(
            order_key, order_id, scenario, region_id, city_id, courier_id, aoi_id,
            order_lng, order_lat, accept_time, time_window_start, time_window_end, fulfill_time, ds, package_weight
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        order_rows,
    )

    conn.executemany(
        "INSERT INTO gps_event(order_key, event_type, event_time, lng, lat) VALUES (?, ?, ?, ?, ?)",
        event_rows,
    )

    conn.commit()


def df_query(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)


def exec_write(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> None:
    conn.execute(sql, params)
    conn.commit()


def is_safe_select(sql: str) -> bool:
    s = sql.strip()
    if not s:
        return False

    s_no_trailing = s[:-1].strip() if s.endswith(";") else s
    if ";" in s_no_trailing:
        return False

    head = re.sub(r"\s+", " ", s_no_trailing.lstrip()).lower()
    if not (head.startswith("select ") or head.startswith("select\n") or head.startswith("with ")):
        return False

    forbidden = [
        "insert",
        "update",
        "delete",
        "drop",
        "alter",
        "create",
        "pragma",
        "attach",
        "detach",
        "vacuum",
        "reindex",
        "replace",
    ]
    for kw in forbidden:
        if re.search(rf"\b{kw}\b", head, flags=re.IGNORECASE):
            return False

    return True


def ui_header() -> None:
    st.set_page_config(page_title="CSC3170 Delivery DB", layout="wide")
    st.markdown(
        """
        <style>
          .block-container { padding-top: 1.4rem; padding-bottom: 2.5rem; }
          div[data-testid="stMetric"] { background: rgba(240,242,246,.6); border: 1px solid rgba(49,51,63,.12); padding: 14px; border-radius: 12px; }
          .stDataFrame { border-radius: 12px; overflow: hidden; }
          .stButton button { border-radius: 10px; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("上海（2026/05）Pickup & Delivery 数据库检索")


def sidebar_db_path() -> str:
    st.sidebar.header("设置")
    default = os.environ.get("CSC3170_DB_PATH", str(DEFAULT_DB_PATH))
    db_path = st.sidebar.text_input("SQLite 路径", value=default, help="SQLite 文件路径（本地运行时会自动创建）")
    return db_path


def sidebar_filters(conn: sqlite3.Connection) -> dict[str, Any]:
    st.sidebar.header("筛选")

    scenario = st.sidebar.multiselect("场景", options=["pickup", "delivery"], default=["pickup", "delivery"])

    regions = df_query(
        conn,
        "SELECT DISTINCT region_id FROM orders WHERE region_id IS NOT NULL ORDER BY region_id",
    )
    region_options = [int(x) for x in regions["region_id"].tolist()] if not regions.empty else []
    selected_regions = st.sidebar.multiselect("region_id", options=region_options, default=region_options[:3])

    minmax = df_query(conn, "SELECT MIN(accept_time) AS min_dt, MAX(accept_time) AS max_dt FROM orders WHERE accept_time IS NOT NULL")
    if pd.isna(minmax.loc[0, "min_dt"]):
        return {"scenario": scenario, "region_ids": selected_regions, "date_start": None, "date_end": None}

    min_dt = _parse_iso(str(minmax.loc[0, "min_dt"])).date()
    max_dt = _parse_iso(str(minmax.loc[0, "max_dt"])).date()
    date_range = st.sidebar.date_input("accept_time 日期范围", value=(min_dt, max_dt), min_value=min_dt, max_value=max_dt)
    if isinstance(date_range, tuple) and len(date_range) == 2:
        date_start, date_end = date_range
    else:
        date_start, date_end = min_dt, max_dt

    return {"scenario": scenario, "region_ids": selected_regions, "date_start": date_start, "date_end": date_end}


def page_overview(conn: sqlite3.Connection) -> None:
    st.subheader("概览")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("订单", _table_count(conn, "orders"))
    c2.metric("骑手", _table_count(conn, "courier"))
    c3.metric("AOI", _table_count(conn, "aoi"))
    c4.metric("GPS 事件", _table_count(conn, "gps_event"))

    w = df_query(
        conn,
        """
        SELECT
            ROUND(AVG(package_weight), 2) AS avg_w,
            ROUND(MAX(package_weight), 2) AS max_w,
            COUNT(package_weight) AS n_w
        FROM orders
        WHERE package_weight IS NOT NULL
        """,
    )
    if not w.empty:
        r = w.iloc[0]
        c5, c6, c7 = st.columns(3)
        c5.metric("平均重量(kg)", "-" if pd.isna(r["avg_w"]) else float(r["avg_w"]))
        c6.metric("最大重量(kg)", "-" if pd.isna(r["max_w"]) else float(r["max_w"]))
        c7.metric("有重量记录", int(r["n_w"]))

    st.subheader("每日订单量")
    daily = df_query(
        conn,
        """
        SELECT substr(accept_time, 1, 10) AS day, scenario, COUNT(*) AS orders
        FROM orders
        WHERE accept_time IS NOT NULL
        GROUP BY substr(accept_time, 1, 10), scenario
        ORDER BY day, scenario
        """,
    )
    if not daily.empty:
        pivot = daily.pivot(index="day", columns="scenario", values="orders").fillna(0)
        st.line_chart(pivot)
    else:
        st.info("暂无数据：请先在左侧导入 CSV。")

    st.subheader("骑手 Top（按订单数）")
    top = df_query(
        conn,
        """
        SELECT o.courier_id, c.courier_name, c.courier_phone, o.scenario, COUNT(*) AS orders,
               ROUND(AVG((julianday(o.fulfill_time) - julianday(o.accept_time)) * 24 * 60), 1) AS avg_minutes
        FROM orders o
        LEFT JOIN courier c ON c.courier_id = o.courier_id
        WHERE o.courier_id IS NOT NULL AND o.accept_time IS NOT NULL AND o.fulfill_time IS NOT NULL
        GROUP BY o.courier_id, c.courier_name, c.courier_phone, o.scenario
        ORDER BY orders DESC
        LIMIT 12
        """,
    )
    st.dataframe(top, use_container_width=True, hide_index=True)


def page_search(conn: sqlite3.Connection, flt: dict[str, Any]) -> None:
    st.subheader("检索")
    tab1, tab2, tab3 = st.tabs(["订单检索", "骑手检索", "GPS 事件时间线"])

    with tab1:
        left, right = st.columns([2, 1])
        with left:
            keyword = st.text_input("order_id / courier_id 关键字", value="")
        with right:
            aoi_type = st.selectbox("aoi_type", options=["(all)"] + [str(x) for x in range(0, 20)])

        courier_name_kw = st.text_input("快递员姓名包含", value="")
        weight_min_txt = st.text_input("重量下限(kg)", value="")
        weight_max_txt = st.text_input("重量上限(kg)", value="")

        wmin: float | None = None
        wmax: float | None = None
        if weight_min_txt.strip():
            try:
                wmin = float(weight_min_txt.strip())
            except Exception:
                st.error("重量下限需要是数字。")
        if weight_max_txt.strip():
            try:
                wmax = float(weight_max_txt.strip())
            except Exception:
                st.error("重量上限需要是数字。")

        where = []
        params: list[Any] = []

        if flt.get("scenario"):
            where.append(f"o.scenario IN ({','.join(['?'] * len(flt['scenario']))})")
            params.extend(flt["scenario"])

        if flt.get("region_ids"):
            where.append(f"o.region_id IN ({','.join(['?'] * len(flt['region_ids']))})")
            params.extend([int(x) for x in flt["region_ids"]])

        if flt.get("date_start") and flt.get("date_end"):
            where.append("date(o.accept_time) BETWEEN date(?) AND date(?)")
            params.append(str(flt["date_start"]))
            params.append(str(flt["date_end"]))

        if aoi_type != "(all)":
            where.append("a.aoi_type = ?")
            params.append(int(aoi_type))

        if courier_name_kw.strip():
            where.append("c.courier_name LIKE ?")
            params.append(f"%{courier_name_kw.strip()}%")

        if wmin is not None:
            where.append("o.package_weight >= ?")
            params.append(float(wmin))
        if wmax is not None:
            where.append("o.package_weight <= ?")
            params.append(float(wmax))

        if keyword.strip():
            if keyword.strip().isdigit():
                where.append("(o.order_id = ? OR o.courier_id = ?)")
                params.append(int(keyword.strip()))
                params.append(int(keyword.strip()))

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""
        df = df_query(
            conn,
            f"""
            SELECT
                o.order_key, o.order_id, o.scenario, o.region_id, o.courier_id,
                c.courier_name, c.courier_phone,
                o.aoi_id, a.aoi_type,
                o.accept_time, o.time_window_start, o.time_window_end, o.fulfill_time, o.ds,
                o.package_weight,
                o.order_lng, o.order_lat
            FROM orders o
            LEFT JOIN courier c ON c.courier_id = o.courier_id
            LEFT JOIN aoi a ON a.aoi_id = o.aoi_id
            {where_sql}
            ORDER BY o.accept_time DESC
            LIMIT 500
            """,
            tuple(params),
        )

        st.caption(f"最多显示 500 条；当前 {len(df)} 条")
        st.dataframe(df.drop(columns=["order_lng", "order_lat"], errors="ignore"), use_container_width=True, hide_index=True)

        if not df.empty:
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("下载 CSV", data=csv, file_name="orders.csv", mime="text/csv")

        st.divider()
        st.markdown("地图（订单坐标）")
        map_df = df[["order_lat", "order_lng"]].rename(columns={"order_lat": "lat", "order_lng": "lon"})
        map_df = map_df.dropna()
        if not map_df.empty:
            st.map(map_df)
        else:
            st.info("没有可展示的坐标数据。")

    with tab2:
        couriers = df_query(
            conn,
            """
            SELECT c.courier_id, c.courier_name, c.courier_phone, COUNT(o.order_key) AS orders
            FROM courier c
            JOIN orders o ON o.courier_id = c.courier_id
            GROUP BY c.courier_id, c.courier_name, c.courier_phone
            ORDER BY orders DESC
            LIMIT 800
            """,
        )
        if couriers.empty:
            st.info("暂无数据：请先导入 CSV。")
        else:
            labels = [
                f"{int(r.courier_id)} | {str(r.courier_name or '')} | {str(r.courier_phone or '')} | {int(r.orders)}"
                for r in couriers.itertuples(index=False)
            ]
            selected = st.selectbox("courier", options=labels)
            courier_id = int(selected.split("|")[0].strip())

            courier_row = conn.execute(
                "SELECT courier_id, courier_name, courier_phone FROM courier WHERE courier_id = ?",
                (int(courier_id),),
            ).fetchone()
            st.write(f"courier_id={courier_row['courier_id']} | 姓名={courier_row['courier_name']} | 电话={courier_row['courier_phone']}")

            summary = df_query(
                conn,
                """
                SELECT scenario, COUNT(*) AS orders,
                       ROUND(AVG((julianday(fulfill_time) - julianday(accept_time)) * 24 * 60), 1) AS avg_minutes,
                       ROUND(AVG(package_weight), 2) AS avg_weight
                FROM orders
                WHERE courier_id = ? AND accept_time IS NOT NULL
                GROUP BY scenario
                ORDER BY orders DESC
                """,
                (int(courier_id),),
            )
            st.dataframe(summary, use_container_width=True, hide_index=True)

            df = df_query(
                conn,
                """
                SELECT order_key, order_id, scenario, region_id, aoi_id, accept_time, fulfill_time, package_weight
                FROM orders
                WHERE courier_id = ?
                ORDER BY accept_time DESC
                LIMIT 300
                """,
                (int(courier_id),),
            )
            st.dataframe(df, use_container_width=True, hide_index=True)

    with tab3:
        scenario = st.selectbox("场景", options=["pickup", "delivery"], index=0)
        order_id_text = st.text_input("order_id", value="")
        if not order_id_text.strip().isdigit():
            st.info("请输入一个数字 order_id。")
        else:
            order_key = f"{scenario}:{int(order_id_text.strip())}"
            row = conn.execute(
                """
                SELECT o.order_key, o.accept_time, o.fulfill_time, o.courier_id, c.courier_name, c.courier_phone,
                       o.region_id, o.aoi_id, o.package_weight
                FROM orders o
                LEFT JOIN courier c ON c.courier_id = o.courier_id
                WHERE o.order_key = ?
                """,
                (order_key,),
            ).fetchone()
            if not row:
                st.warning("未找到该订单。")
            else:
                st.write(
                    f"order_key={row['order_key']} | courier_id={row['courier_id']} | 姓名={row['courier_name']} | 电话={row['courier_phone']} | region_id={row['region_id']} | aoi_id={row['aoi_id']} | 重量={row['package_weight']} | accept_time={row['accept_time']} | fulfill_time={row['fulfill_time']}"
                )
                ev = df_query(
                    conn,
                    """
                    SELECT event_type, event_time, lng, lat
                    FROM gps_event
                    WHERE order_key = ?
                    ORDER BY event_time
                    """,
                    (order_key,),
                )
                st.dataframe(ev, use_container_width=True, hide_index=True)


def page_crud(conn: sqlite3.Connection) -> None:
    st.subheader("管理（插入 / 更新 / 删除）")
    tab1, tab2, tab3 = st.tabs(["新增骑手", "新增订单", "更新/删除订单"])

    with tab1:
        with st.form("add_courier", clear_on_submit=True):
            courier_id = st.text_input("courier_id", value="")
            courier_phone = st.text_input("courier_phone（可空）", value="")
            courier_name = st.text_input("courier_name（可空）", value="")
            submitted = st.form_submit_button("插入")
            if submitted:
                if not courier_id.strip().isdigit():
                    st.error("courier_id 必须是数字。")
                else:
                    try:
                        city_id = ensure_city(conn)
                        exec_write(
                            conn,
                            "INSERT OR IGNORE INTO courier(courier_id, city_id) VALUES (?, ?)",
                            (int(courier_id.strip()), int(city_id)),
                        )
                        exec_write(
                            conn,
                            """
                            UPDATE courier
                            SET
                                courier_phone = COALESCE(NULLIF(?, ''), courier_phone),
                                courier_name = COALESCE(NULLIF(?, ''), courier_name)
                            WHERE courier_id = ?
                            """,
                            (courier_phone.strip(), courier_name.strip(), int(courier_id.strip())),
                        )
                        st.success("已插入 courier。")
                    except Exception as e:
                        st.error(f"插入失败：{e}")

    with tab2:
        with st.form("add_order", clear_on_submit=True):
            scenario = st.selectbox("scenario", options=["pickup", "delivery"])
            order_id = st.text_input("order_id", value="")
            region_id = st.text_input("region_id", value="")
            courier_id = st.text_input("courier_id", value="")
            aoi_id = st.text_input("aoi_id", value="")
            aoi_type = st.text_input("aoi_type（可空）", value="")
            order_lng = st.text_input("lng（可空）", value="")
            order_lat = st.text_input("lat（可空）", value="")
            accept_time = st.text_input("accept_time（YYYY-MM-DD HH:MM:SS）", value="")
            time_window_start = st.text_input("time_window_start（可空）", value="")
            time_window_end = st.text_input("time_window_end（可空）", value="")
            fulfill_time = st.text_input("fulfill_time（可空）", value="")
            ds = st.text_input("ds（可空）", value="")
            package_weight = st.text_input("package_weight(kg)（可空）", value="")
            courier_phone = st.text_input("courier_phone（可空）", value="")
            courier_name = st.text_input("courier_name（可空）", value="")

            submitted = st.form_submit_button("插入")
            if submitted:
                if not order_id.strip().isdigit():
                    st.error("order_id 必须是数字。")
                else:
                    try:
                        city_id = ensure_city(conn)
                        if courier_id.strip().isdigit():
                            exec_write(
                                conn,
                                "INSERT OR IGNORE INTO courier(courier_id, city_id) VALUES (?, ?)",
                                (int(courier_id.strip()), int(city_id)),
                            )
                            exec_write(
                                conn,
                                """
                                UPDATE courier
                                SET
                                    courier_phone = COALESCE(NULLIF(?, ''), courier_phone),
                                    courier_name = COALESCE(NULLIF(?, ''), courier_name)
                                WHERE courier_id = ?
                                """,
                                (courier_phone.strip(), courier_name.strip(), int(courier_id.strip())),
                            )

                        if aoi_id.strip().isdigit():
                            exec_write(
                                conn,
                                "INSERT OR IGNORE INTO aoi(aoi_id, city_id, aoi_type) VALUES (?, ?, ?)",
                                (
                                    int(aoi_id.strip()),
                                    int(city_id),
                                    int(aoi_type.strip()) if aoi_type.strip().isdigit() else None,
                                ),
                            )

                        order_key = f"{scenario}:{int(order_id.strip())}"
                        exec_write(
                            conn,
                            """
                            INSERT OR REPLACE INTO orders(
                                order_key, order_id, scenario, region_id, city_id, courier_id, aoi_id,
                                order_lng, order_lat, accept_time, time_window_start, time_window_end, fulfill_time, ds, package_weight
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                order_key,
                                int(order_id.strip()),
                                scenario,
                                int(region_id.strip()) if region_id.strip().isdigit() else None,
                                int(city_id),
                                int(courier_id.strip()) if courier_id.strip().isdigit() else None,
                                int(aoi_id.strip()) if aoi_id.strip().isdigit() else None,
                                float(order_lng.strip()) if order_lng.strip() else None,
                                float(order_lat.strip()) if order_lat.strip() else None,
                                accept_time.strip() or None,
                                time_window_start.strip() or None,
                                time_window_end.strip() or None,
                                fulfill_time.strip() or None,
                                ds.strip() or None,
                                float(package_weight.strip()) if package_weight.strip() else None,
                            ),
                        )
                        st.success("已插入订单。")
                    except Exception as e:
                        st.error(f"插入失败：{e}")

    with tab3:
        df = df_query(
            conn,
            """
            SELECT order_key, order_id, scenario, courier_id, accept_time
            FROM orders
            ORDER BY accept_time DESC
            LIMIT 500
            """,
        )
        if df.empty:
            st.info("暂无订单。")
        else:
            pick = st.selectbox(
                "选择一个订单",
                options=[f"{r.order_key} | courier={r.courier_id} | {r.accept_time}" for r in df.itertuples(index=False)],
            )
            order_key = pick.split("|")[0].strip()

            row = conn.execute(
                "SELECT courier_id, aoi_id, fulfill_time FROM orders WHERE order_key = ?",
                (order_key,),
            ).fetchone()

            col1, col2 = st.columns(2)
            with col1:
                new_courier = st.text_input("更新 courier_id（可空）", value=str(row["courier_id"] or ""))
                new_aoi = st.text_input("更新 aoi_id（可空）", value=str(row["aoi_id"] or ""))
                new_fulfill = st.text_input("更新 fulfill_time（可空）", value=str(row["fulfill_time"] or ""))
                if st.button("更新", type="primary"):
                    try:
                        city_id = ensure_city(conn)
                        if new_courier.strip().isdigit():
                            exec_write(
                                conn,
                                "INSERT OR IGNORE INTO courier(courier_id, city_id) VALUES (?, ?)",
                                (int(new_courier.strip()), int(city_id)),
                            )
                        if new_aoi.strip().isdigit():
                            exec_write(
                                conn,
                                "INSERT OR IGNORE INTO aoi(aoi_id, city_id, aoi_type) VALUES (?, ?, ?)",
                                (int(new_aoi.strip()), int(city_id), None),
                            )
                        exec_write(
                            conn,
                            "UPDATE orders SET courier_id = ?, aoi_id = ?, fulfill_time = ? WHERE order_key = ?",
                            (
                                int(new_courier.strip()) if new_courier.strip().isdigit() else None,
                                int(new_aoi.strip()) if new_aoi.strip().isdigit() else None,
                                new_fulfill.strip() or None,
                                order_key,
                            ),
                        )
                        st.success("已更新。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"更新失败：{e}")
            with col2:
                st.warning("删除会级联删除 gps_event。")
                if st.button("删除", type="secondary"):
                    try:
                        exec_write(conn, "DELETE FROM orders WHERE order_key = ?", (order_key,))
                        st.success("已删除。")
                        st.rerun()
                    except Exception as e:
                        st.error(f"删除失败：{e}")


def page_sql_console(conn: sqlite3.Connection) -> None:
    st.subheader("SQL Console（只允许 SELECT/WITH）")
    st.caption("用于满足课程项目常见的“用户可自定义检索查询”需求；为安全起见禁用写操作。")
    default_sql = """
WITH x AS (
  SELECT
    scenario,
    region_id,
    COUNT(*) AS orders,
    ROUND(AVG((julianday(fulfill_time) - julianday(accept_time)) * 24 * 60), 1) AS avg_minutes,
    ROUND(AVG(package_weight), 2) AS avg_weight
  FROM orders
  GROUP BY scenario, region_id
)
SELECT * FROM x
ORDER BY scenario, orders DESC
""".strip()
    sql = st.text_area("SQL", value=default_sql, height=220)
    limit = st.number_input("最多返回行数", min_value=10, max_value=5000, value=500, step=10)

    if st.button("运行", type="primary"):
        if not is_safe_select(sql):
            st.error("该 SQL 不被允许：仅支持单条 SELECT/WITH，且禁止写入/DDL/PRAGMA 等。")
            return
        q = sql.rstrip().rstrip(";").strip()
        if re.search(r"\blimit\b", q, flags=re.IGNORECASE):
            final = q
        else:
            final = f"{q}\nLIMIT {int(limit)}"
        try:
            df = df_query(conn, final)
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"执行失败：{e}")


def main() -> None:
    ui_header()
    db_path = sidebar_db_path()
    conn = get_conn(db_path)
    init_db(conn)

    st.sidebar.header("数据导入")
    pickup_csv = st.sidebar.text_input("pickup CSV 路径", value=DEFAULT_PICKUP_CSV)
    delivery_csv = st.sidebar.text_input("delivery CSV 路径", value=DEFAULT_DELIVERY_CSV)
    year = int(st.sidebar.number_input("pickup 年份补全", min_value=2000, max_value=2100, value=DEFAULT_YEAR, step=1))
    clear_first = st.sidebar.checkbox("导入前清空数据库", value=True)

    if st.sidebar.button("导入/刷新"):
        try:
            if clear_first:
                clear_imported_data(conn)
            import_csvs(conn, pickup_csv, delivery_csv, year=year)
            st.sidebar.success("导入完成。")
            st.rerun()
        except Exception as e:
            st.sidebar.error(f"导入失败：{e}")

    if _table_count(conn, "orders") == 0:
        st.info("左侧填写两个 CSV 路径后，点击“导入/刷新”。")

    flt = sidebar_filters(conn)
    page = st.sidebar.radio("页面", options=["概览", "检索", "管理", "SQL Console"], index=0)

    if page == "概览":
        page_overview(conn)
    elif page == "检索":
        page_search(conn, flt)
    elif page == "管理":
        page_crud(conn)
    else:
        page_sql_console(conn)


if __name__ == "__main__":
    main()