"""
Build lade.sqlite from the 6 CSV files.
Schema follows Section 4 of 步骤1-8.pdf (3NF design).
"""
import sqlite3
import csv
import os

DB_PATH = "/home/claude/lade_web/lade.sqlite"
CSV_DIR = "/home/claude/lade_web"

if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# ---- Schema ----
cur.executescript("""
CREATE TABLE Couriers (
    courier_id TEXT PRIMARY KEY,
    city_base  TEXT NOT NULL
);

CREATE TABLE AOI_Master (
    aoi_id    TEXT PRIMARY KEY,
    region_id TEXT,
    city      TEXT,
    aoi_type  TEXT
);

CREATE TABLE Road_Network (
    road_id   TEXT PRIMARY KEY,
    code      TEXT,
    fclass    TEXT,
    name      TEXT,
    ref       TEXT,
    oneway    TEXT,
    maxspeed  INTEGER,
    layer     INTEGER,
    bridge    TEXT,
    tunnel    TEXT,
    city      TEXT,
    geometry  TEXT
);

CREATE TABLE Pickup_Orders (
    order_id           TEXT PRIMARY KEY,
    courier_id         TEXT,
    aoi_id             TEXT,
    stop_lng           REAL,
    stop_lat           REAL,
    time_window_start  TEXT,
    time_window_end    TEXT,
    accept_time        TEXT,
    pickup_time        TEXT,
    accept_gps_time    TEXT,
    pickup_gps_time    TEXT,
    accept_gps_lng     REAL,
    accept_gps_lat     REAL,
    pickup_gps_lng     REAL,
    pickup_gps_lat     REAL,
    ds                 TEXT,
    FOREIGN KEY (courier_id) REFERENCES Couriers(courier_id),
    FOREIGN KEY (aoi_id)     REFERENCES AOI_Master(aoi_id)
);

CREATE TABLE Delivery_Orders (
    order_id           TEXT PRIMARY KEY,
    courier_id         TEXT,
    aoi_id             TEXT,
    stop_lng           REAL,
    stop_lat           REAL,
    accept_time        TEXT,
    delivery_time      TEXT,
    accept_gps_time    TEXT,
    delivery_gps_time  TEXT,
    accept_gps_lng     REAL,
    accept_gps_lat     REAL,
    delivery_gps_lng   REAL,
    delivery_gps_lat   REAL,
    ds                 TEXT,
    FOREIGN KEY (courier_id) REFERENCES Couriers(courier_id),
    FOREIGN KEY (aoi_id)     REFERENCES AOI_Master(aoi_id)
);

CREATE TABLE Courier_Trajectories (
    trajectory_id INTEGER PRIMARY KEY,
    courier_id    TEXT,
    gps_time      TEXT,
    lat           REAL,
    lng           REAL,
    ds            TEXT,
    FOREIGN KEY (courier_id) REFERENCES Couriers(courier_id)
);
""")

# ---- Indexes (Step 8 of the design doc) ----
cur.executescript("""
CREATE INDEX idx_traj_courier_time  ON Courier_Trajectories(courier_id, gps_time);
CREATE INDEX idx_pickup_courier     ON Pickup_Orders(courier_id);
CREATE INDEX idx_pickup_aoi         ON Pickup_Orders(aoi_id);
CREATE INDEX idx_pickup_ds          ON Pickup_Orders(ds);
CREATE INDEX idx_delivery_courier   ON Delivery_Orders(courier_id);
CREATE INDEX idx_delivery_aoi       ON Delivery_Orders(aoi_id);
CREATE INDEX idx_delivery_ds        ON Delivery_Orders(ds);
CREATE INDEX idx_aoi_city_region    ON AOI_Master(city, region_id);
""")

# Time-like columns that are stored as "MM-DD HH:MM:SS" in the raw CSV.
# We prepend a virtual year "2024-" so SQLite's julianday() can parse them.
# This is a demo-only normalization documented on the "About" page.
TIME_COLS = {
    "accept_time", "pickup_time", "delivery_time",
    "accept_gps_time", "pickup_gps_time", "delivery_gps_time",
    "time_window_start", "time_window_end", "gps_time",
}

def normalize_time(v):
    if not v or len(v) < 8:
        return v
    # Already has a year? (e.g. "2024-08-21 ...")
    if v[4:5] == "-" and v[:4].isdigit():
        return v
    # Format "MM-DD HH:MM:SS" -> "2024-MM-DD HH:MM:SS"
    if v[2:3] == "-" and v[5:6] == " ":
        return "2024-" + v
    return v

def load_csv(table, csv_name, columns):
    path = os.path.join(CSV_DIR, csv_name)
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = []
        for r in reader:
            row = []
            for c in columns:
                v = r.get(c, "")
                if v == "" or v is None:
                    row.append(None)
                elif c in TIME_COLS:
                    row.append(normalize_time(v))
                else:
                    row.append(v)
            rows.append(tuple(row))
    placeholders = ",".join(["?"] * len(columns))
    col_list = ",".join(columns)
    cur.executemany(f"INSERT OR IGNORE INTO {table} ({col_list}) VALUES ({placeholders})", rows)
    print(f"  {table}: {len(rows)} rows loaded")

print("Loading CSVs into SQLite...")
load_csv("Couriers", "Couriers.csv", ["courier_id", "city_base"])
load_csv("AOI_Master", "AOI_Master.csv", ["aoi_id", "region_id", "city", "aoi_type"])
load_csv("Road_Network", "Road_Network.csv",
         ["road_id", "code", "fclass", "name", "ref", "oneway", "maxspeed",
          "layer", "bridge", "tunnel", "city", "geometry"])
load_csv("Pickup_Orders", "Pickup_Orders.csv",
         ["order_id", "courier_id", "aoi_id", "stop_lng", "stop_lat",
          "time_window_start", "time_window_end", "accept_time", "pickup_time",
          "accept_gps_time", "pickup_gps_time", "accept_gps_lng", "accept_gps_lat",
          "pickup_gps_lng", "pickup_gps_lat", "ds"])
load_csv("Delivery_Orders", "Delivery_Orders.csv",
         ["order_id", "courier_id", "aoi_id", "stop_lng", "stop_lat",
          "accept_time", "delivery_time", "accept_gps_time", "delivery_gps_time",
          "accept_gps_lng", "accept_gps_lat", "delivery_gps_lng", "delivery_gps_lat", "ds"])
load_csv("Courier_Trajectories", "Courier_Trajectories.csv",
         ["trajectory_id", "courier_id", "gps_time", "lat", "lng", "ds"])

conn.commit()

# Verify
print("\n=== Verification ===")
for tbl in ["Couriers", "AOI_Master", "Road_Network",
            "Pickup_Orders", "Delivery_Orders", "Courier_Trajectories"]:
    n = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl}: {n} rows")

# Sanity check a join
print("\n=== Sample join (delivery time per city) ===")
for r in cur.execute("""
    SELECT a.city, COUNT(*) as orders
    FROM Delivery_Orders d JOIN AOI_Master a ON d.aoi_id = a.aoi_id
    GROUP BY a.city
"""):
    print(f"  {r}")

conn.close()

size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
print(f"\nDB file size: {size_mb:.2f} MB  ->  {DB_PATH}")
