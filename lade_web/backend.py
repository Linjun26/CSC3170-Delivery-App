"""
LaDe Web · LLM Backend Proxy
=============================
A minimal Flask server that forwards natural-language questions from
the browser to DeepSeek's API, then returns the generated SQL.

Why a backend? Browsers cannot safely call LLM APIs directly:
  1. CORS — most API providers reject cross-origin calls from browsers
  2. Security — putting your API key in client JS would expose it publicly

This proxy solves both: the API key stays on your machine, and CORS is
handled server-side.

How to run:
  1. pip install flask flask-cors openai python-dotenv
  2. Put your DeepSeek API key in a file called `.env` next to this script:
       DEEPSEEK_API_KEY=sk-xxxxxxxx
  3. python3 backend.py
  4. Leave this terminal running. Open index.html via Live Server as usual.
"""

import os
import re
from flask import Flask, request, jsonify
from flask_cors import CORS
from openai import OpenAI
from dotenv import load_dotenv

# ---- Load API key from .env ----
load_dotenv()
API_KEY = os.getenv("DEEPSEEK_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "DEEPSEEK_API_KEY not found. Create a .env file next to backend.py "
        "with: DEEPSEEK_API_KEY=sk-your-key-here"
    )

# DeepSeek uses an OpenAI-compatible API — we just point the client at their URL
client = OpenAI(
    api_key=API_KEY,
    base_url="https://api.deepseek.com",
)

# ---- Flask app ----
app = Flask(__name__)
CORS(app)   # allow the browser (running on localhost:5500 via Live Server)
            # to call this backend (on localhost:5001)

# ---- System prompt sent to DeepSeek ----
SYSTEM_PROMPT = """You are a SQL expert helping users query a SQLite database of last-mile delivery data.

TABLES (SQLite dialect — use julianday() for time diffs, NOT TIMESTAMPDIFF):

Couriers(courier_id TEXT PK, city_base TEXT)

AOI_Master(aoi_id TEXT PK, region_id TEXT, city TEXT, aoi_type TEXT)

Pickup_Orders(order_id TEXT PK, courier_id FK, aoi_id FK,
  stop_lng, stop_lat, time_window_start, time_window_end,
  accept_time, pickup_time, accept_gps_time, pickup_gps_time,
  accept_gps_lng, accept_gps_lat, pickup_gps_lng, pickup_gps_lat, ds)

Delivery_Orders(order_id TEXT PK, courier_id FK, aoi_id FK,
  stop_lng, stop_lat, accept_time, delivery_time,
  accept_gps_time, delivery_gps_time, accept_gps_lng, accept_gps_lat,
  delivery_gps_lng, delivery_gps_lat, ds)

Courier_Trajectories(trajectory_id INT PK, courier_id FK, gps_time, lat, lng, ds)

Road_Network(road_id TEXT PK, code, fclass, name, ref, oneway, maxspeed, layer, bridge, tunnel, city, geometry)

NOTES:
- time fields are TEXT in 'YYYY-MM-DD HH:MM:SS' format
- ds is a date tag like '821' (Aug 21) or '1015' (Oct 15) — it has no year; length 3 = Jan/Feb/.../Sep, length 4 = Oct/Nov/Dec
- For time differences use: (julianday(t2) - julianday(t1)) * 1440 to get minutes
- Data covers Jilin city only
- courier_id and aoi_id are TEXT, wrap literals in quotes: WHERE courier_id = '393'

OUTPUT RULES:
- Return ONLY the raw SQL query — no markdown code fences, no explanation, no commentary
- Add a LIMIT clause (default LIMIT 50) unless the user asks for all rows or a single aggregate
- Use meaningful column aliases (e.g. total_orders, avg_minutes)
- Never write INSERT / UPDATE / DELETE / DROP — this is a read-only demo
"""


def strip_markdown_fences(text: str) -> str:
    """DeepSeek sometimes wraps SQL in ```sql ... ``` despite instructions.
    Strip those fences defensively."""
    text = text.strip()
    text = re.sub(r"^```\s*sql\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^```", "", text)
    text = re.sub(r"```\s*$", "", text)
    return text.strip()


@app.route("/api/nl2sql", methods=["POST"])
def nl2sql():
    """Receive a natural-language question, return the SQL DeepSeek generates."""
    data = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()

    if not question:
        return jsonify({"error": "Missing 'question' in request body"}), 400

    try:
        response = client.chat.completions.create(
            model="deepseek-chat",        # DeepSeek V3.2 — cheap + capable
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user",   "content": question},
            ],
            temperature=0.0,              # deterministic; we want the same SQL every time
            max_tokens=500,
        )
        raw = response.choices[0].message.content
        sql = strip_markdown_fences(raw)

        # Guardrail: reject any destructive SQL that slipped through
        if re.search(r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE)\b", sql, re.IGNORECASE):
            return jsonify({"error": "Generated SQL is non-readonly. Refusing to execute."}), 400

        return jsonify({"sql": sql})

    except Exception as e:
        # Log server-side, return a friendly message to the browser
        print(f"[DeepSeek error] {type(e).__name__}: {e}")
        return jsonify({"error": f"LLM call failed: {str(e)}"}), 500


@app.route("/api/health", methods=["GET"])
def health():
    """Simple ping endpoint to verify the backend is reachable from the browser."""
    return jsonify({"status": "ok", "model": "deepseek-chat"})


if __name__ == "__main__":
    print("=" * 60)
    print("LaDe LLM backend running on http://localhost:5001")
    print("Keep this terminal open during your presentation.")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5001, debug=False)
