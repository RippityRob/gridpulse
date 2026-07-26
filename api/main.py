"""GridPulse analytics API.

Reads the readings written by the Go collector and exposes them as JSON,
plus an independent anomaly detector that works only from the measured
values — it never looks at the `fault` flag the sensor firmware sets.
That flag is kept as ground truth so detector accuracy can be scored
(see /api/detector-score).

Run:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://gridpulse:gridpulse@localhost:5432/gridpulse?sslmode=disable",
)

# Distribution feeder nameplate values the detector measures against.
NOMINAL_VOLTAGE_V = 11_000.0
NOMINAL_FREQUENCY_HZ = 50.0

# Detection thresholds. The frequency band mirrors the UK statutory
# limit of 50 Hz +/- 0.5 Hz; the voltage band is the common +/-10%
# supply tolerance.
SAG_RATIO = 0.90
SWELL_RATIO = 1.10
FREQ_TOLERANCE_HZ = 0.5
STALE_AFTER_S = 10.0

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="GridPulse Analytics", version="0.1.0")


@contextmanager
def db_cursor():
    """Yield a dict cursor, closing the connection afterwards."""
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            yield cur
    finally:
        conn.close()


@dataclass
class Anomaly:
    sensor_id: int
    kind: str
    detail: str
    severity: str
    at: datetime

    def as_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "kind": self.kind,
            "detail": self.detail,
            "severity": self.severity,
            "at": self.at.isoformat(),
        }


@app.get("/api/health")
def health() -> dict:
    """Liveness probe that also proves the database is reachable."""
    try:
        with db_cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM readings")
            total = cur.fetchone()["n"]
    except psycopg2.Error as exc:
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}")
    return {"status": "ok", "readings": total}


@app.get("/api/sensors")
def sensors(window_s: int = Query(120, ge=10, le=3600)) -> list[dict]:
    """Per-sensor summary over the trailing window."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT sensor_id,
                   count(*)                       AS samples,
                   avg(voltage_v)                 AS avg_voltage_v,
                   min(voltage_v)                 AS min_voltage_v,
                   max(voltage_v)                 AS max_voltage_v,
                   avg(current_a)                 AS avg_current_a,
                   avg(frequency_hz)              AS avg_frequency_hz,
                   count(*) FILTER (WHERE fault)  AS fault_samples,
                   max(time)                      AS last_seen
            FROM readings
            WHERE time > now() - make_interval(secs => %s)
            GROUP BY sensor_id
            ORDER BY sensor_id
            """,
            (window_s,),
        )
        rows = cur.fetchall()

    now = datetime.now(timezone.utc)
    out = []
    for row in rows:
        latest = _latest_reading(row["sensor_id"])
        age = (now - row["last_seen"]).total_seconds()
        out.append(
            {
                "sensor_id": row["sensor_id"],
                "samples": row["samples"],
                "avg_voltage_v": round(float(row["avg_voltage_v"]), 1),
                "min_voltage_v": round(float(row["min_voltage_v"]), 1),
                "max_voltage_v": round(float(row["max_voltage_v"]), 1),
                "avg_current_a": round(float(row["avg_current_a"]), 1),
                "avg_frequency_hz": round(float(row["avg_frequency_hz"]), 3),
                "fault_samples": row["fault_samples"],
                "last_seen": row["last_seen"].isoformat(),
                "age_s": round(age, 1),
                "online": age <= STALE_AFTER_S,
                "latest": latest,
            }
        )
    return out


def _latest_reading(sensor_id: int) -> dict | None:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT time, voltage_v, current_a, frequency_hz, fault
            FROM readings
            WHERE sensor_id = %s
            ORDER BY time DESC
            LIMIT 1
            """,
            (sensor_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "time": row["time"].isoformat(),
        "voltage_v": round(float(row["voltage_v"]), 1),
        "current_a": round(float(row["current_a"]), 1),
        "frequency_hz": round(float(row["frequency_hz"]), 3),
        "fault": row["fault"],
    }


@app.get("/api/readings")
def readings(
    sensor_id: int | None = None,
    seconds: int = Query(60, ge=5, le=3600),
    limit: int = Query(600, ge=1, le=5000),
) -> list[dict]:
    """Raw readings for charting, oldest first."""
    clauses = ["time > now() - make_interval(secs => %s)"]
    params: list = [seconds]
    if sensor_id is not None:
        clauses.append("sensor_id = %s")
        params.append(sensor_id)
    params.append(limit)

    with db_cursor() as cur:
        cur.execute(
            f"""
            SELECT time, sensor_id, voltage_v, current_a, frequency_hz, fault
            FROM (
                SELECT * FROM readings
                WHERE {' AND '.join(clauses)}
                ORDER BY time DESC
                LIMIT %s
            ) recent
            ORDER BY time ASC
            """,
            params,
        )
        rows = cur.fetchall()

    return [
        {
            "time": r["time"].isoformat(),
            "sensor_id": r["sensor_id"],
            "voltage_v": round(float(r["voltage_v"]), 1),
            "current_a": round(float(r["current_a"]), 1),
            "frequency_hz": round(float(r["frequency_hz"]), 3),
            "fault": r["fault"],
        }
        for r in rows
    ]


def _detect(rows: list[dict], now: datetime) -> list[Anomaly]:
    """Classify readings using measured values only, never the fault flag."""
    found: list[Anomaly] = []
    seen: dict[int, datetime] = {}

    for r in rows:
        sid = r["sensor_id"]
        seen[sid] = max(seen.get(sid, r["time"]), r["time"])
        v, f = float(r["voltage_v"]), float(r["frequency_hz"])

        if v < NOMINAL_VOLTAGE_V * SAG_RATIO:
            depth = 100.0 * (1.0 - v / NOMINAL_VOLTAGE_V)
            found.append(
                Anomaly(
                    sid,
                    "voltage_sag",
                    f"{v:,.0f} V — {depth:.0f}% below nominal",
                    "critical" if depth >= 20 else "warning",
                    r["time"],
                )
            )
        elif v > NOMINAL_VOLTAGE_V * SWELL_RATIO:
            rise = 100.0 * (v / NOMINAL_VOLTAGE_V - 1.0)
            found.append(
                Anomaly(sid, "voltage_swell", f"{v:,.0f} V — {rise:.0f}% above nominal",
                        "critical" if rise >= 20 else "warning", r["time"])
            )

        drift = abs(f - NOMINAL_FREQUENCY_HZ)
        if drift > FREQ_TOLERANCE_HZ:
            found.append(
                Anomaly(sid, "frequency_deviation", f"{f:.3f} Hz — {drift:.3f} Hz off nominal",
                        "critical", r["time"])
            )

    for sid, last in seen.items():
        age = (now - last).total_seconds()
        if age > STALE_AFTER_S:
            found.append(
                Anomaly(sid, "sensor_offline", f"no data for {age:.0f} s", "critical", last)
            )

    found.sort(key=lambda a: a.at, reverse=True)
    return found


@app.get("/api/anomalies")
def anomalies(seconds: int = Query(60, ge=5, le=3600), limit: int = Query(50, ge=1, le=500)) -> dict:
    """Anomalies detected in the trailing window, newest first."""
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT time, sensor_id, voltage_v, current_a, frequency_hz, fault
            FROM readings
            WHERE time > now() - make_interval(secs => %s)
            ORDER BY time DESC
            LIMIT 5000
            """,
            (seconds,),
        )
        rows = cur.fetchall()

    found = _detect(rows, datetime.now(timezone.utc))
    counts: dict[str, int] = {}
    for a in found:
        counts[a.kind] = counts.get(a.kind, 0) + 1

    return {
        "window_s": seconds,
        "total": len(found),
        "by_kind": counts,
        "anomalies": [a.as_dict() for a in found[:limit]],
    }


@app.get("/api/detector-score")
def detector_score(seconds: int = Query(300, ge=10, le=86400)) -> dict:
    """Score the detector against the firmware fault flag (ground truth).

    Each reading is a sample: did the detector flag it, and was it in fact
    faulty? Precision is how many flagged samples were genuinely faulty;
    recall is how many faulty samples the detector caught.
    """
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT time, sensor_id, voltage_v, current_a, frequency_hz, fault
            FROM readings
            WHERE time > now() - make_interval(secs => %s)
            ORDER BY time DESC
            LIMIT 20000
            """,
            (seconds,),
        )
        rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="no readings in window")

    now = datetime.now(timezone.utc)
    flagged = {
        (a.sensor_id, a.at)
        for a in _detect(rows, now)
        if a.kind != "sensor_offline"  # not a per-sample condition
    }

    tp = fp = fn = tn = 0
    for r in rows:
        predicted = (r["sensor_id"], r["time"]) in flagged
        actual = r["fault"]
        if predicted and actual:
            tp += 1
        elif predicted and not actual:
            fp += 1
        elif not predicted and actual:
            fn += 1
        else:
            tn += 1

    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    return {
        "window_s": seconds,
        "samples": len(rows),
        "true_positives": tp,
        "false_positives": fp,
        "false_negatives": fn,
        "true_negatives": tn,
        "precision": round(precision, 3),
        "recall": round(recall, 3),
        "f1": round(f1, 3),
    }


# Dashboard. Mounted last so the API routes above take precedence.
if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    def dashboard() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")
