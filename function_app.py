import os
import json
import time
import logging
import datetime as dt

import azure.functions as func
import requests
import pyodbc

app = func.FunctionApp()

IRAIL_LIVEBOARD_URL = "https://api.irail.be/liveboard/"
IRAIL_STATIONS_URL = "https://api.irail.be/stations/"

# Safe throttling under iRail limits (3 req/s). Use 2 req/s by default.
REQUESTS_PER_SECOND = float(os.environ.get("IRAIL_RPS", "2.0"))
SECONDS_PER_REQUEST = 1.0 / REQUESTS_PER_SECOND

# How many stations to process per scheduled run
BATCH_SIZE = int(os.environ.get("IRAIL_BATCH_SIZE", "30"))  # start small for stability


# ----------------------------
# Helpers
# ----------------------------
def _epoch_to_utc_naive(epoch_seconds: int) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(epoch_seconds), tz=dt.timezone.utc).replace(tzinfo=None)


def _train_type_from_vehicle_id(vehicle_id: str | None) -> str | None:
    if not vehicle_id:
        return None
    last = vehicle_id.split(".")[-1]
    for prefix in ("IC", "S", "L", "P"):
        if last.startswith(prefix):
            return prefix
    return None


def _parse_boolish(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if value != 0 else 0
    s = str(value).strip().lower()
    if s in ("true", "1", "yes", "y"):
        return 1
    if s in ("false", "0", "no", "n"):
        return 0
    return None


def _get_sql_driver_name() -> str:
    available = set(pyodbc.drivers())
    if "ODBC Driver 18 for SQL Server" in available:
        return "ODBC Driver 18 for SQL Server"
    if "ODBC Driver 17 for SQL Server" in available:
        return "ODBC Driver 17 for SQL Server"
    raise RuntimeError(f"No SQL Server ODBC driver found. Available: {sorted(available)}")


def _get_sql_connection() -> pyodbc.Connection:
    required = ["SQL_SERVER", "SQL_DATABASE", "SQL_USERNAME", "SQL_PASSWORD"]
    missing = [k for k in required if not os.environ.get(k)]
    if missing:
        raise KeyError(f"Missing environment variables: {missing}")

    server = os.environ["SQL_SERVER"]
    database = os.environ["SQL_DATABASE"]
    username = os.environ["SQL_USERNAME"]
    password = os.environ["SQL_PASSWORD"]

    driver = _get_sql_driver_name()

    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        "Encrypt=yes;"
        "Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str)


def _irail_headers() -> dict:
    user_agent = os.environ.get("IRAIL_USER_AGENT", "irail-azure-pipeline/0.1 (student project)")
    return {"User-Agent": user_agent, "Accept": "application/json"}


def _sleep_for_rate_limit(start_ts: float) -> None:
    elapsed = time.time() - start_ts
    remaining = SECONDS_PER_REQUEST - elapsed
    if remaining > 0:
        time.sleep(remaining)


# ---- ETag cache helpers for Liveboard ----
def _cache_key_liveboard(station: str, lang: str) -> str:
    return f"liveboard::{station}::{lang}"


def _get_cached_etag(cur: pyodbc.Cursor, cache_key: str) -> str | None:
    cur.execute("SELECT etag FROM dbo.ApiCache WHERE cache_key = ?", cache_key)
    row = cur.fetchone()
    return row[0] if row else None


def _upsert_etag(cur: pyodbc.Cursor, cache_key: str, etag: str) -> None:
    cur.execute(
        """
        MERGE dbo.ApiCache AS target
        USING (SELECT ? AS cache_key, ? AS etag) AS source
        ON target.cache_key = source.cache_key
        WHEN MATCHED THEN
            UPDATE SET etag = source.etag, updated_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (cache_key, etag) VALUES (source.cache_key, source.etag);
        """,
        cache_key, etag
    )


def _irail_get_liveboard(station: str, lang: str, etag: str | None) -> requests.Response:
    headers = _irail_headers()
    if etag:
        headers["If-None-Match"] = etag

    params = {
        "station": station,
        "format": "json",
        "lang": lang,
        "arrdep": "departure",
        "alerts": "false",
    }

    last_response: requests.Response | None = None
    for attempt in range(1, 4):
        start_ts = time.time()
        r = requests.get(IRAIL_LIVEBOARD_URL, params=params, headers=headers, timeout=20)
        _sleep_for_rate_limit(start_ts)

        last_response = r

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 2 * attempt
            logging.warning("iRail 429 Too Many Requests. Waiting %s seconds...", wait_s)
            time.sleep(wait_s)
            continue

        return r

    return last_response  # type: ignore[return-value]


def _irail_get_stations(lang: str) -> requests.Response:
    headers = _irail_headers()
    params = {"format": "json", "lang": lang}

    last_response: requests.Response | None = None
    for attempt in range(1, 4):
        start_ts = time.time()
        r = requests.get(IRAIL_STATIONS_URL, params=params, headers=headers, timeout=30)
        _sleep_for_rate_limit(start_ts)

        last_response = r

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 2 * attempt
            logging.warning("iRail 429 Too Many Requests (stations). Waiting %s seconds...", wait_s)
            time.sleep(wait_s)
            continue

        return r

    return last_response  # type: ignore[return-value]


# ----------------------------
# PipelineState (batch cursor)
# ----------------------------
def _get_state(cur: pyodbc.Cursor, key: str, default: str) -> str:
    cur.execute("SELECT state_value FROM dbo.PipelineState WHERE state_key = ?", key)
    row = cur.fetchone()
    return row[0] if row and row[0] is not None else default


def _set_state(cur: pyodbc.Cursor, key: str, value: str) -> None:
    cur.execute(
        """
        MERGE dbo.PipelineState AS t
        USING (SELECT ? AS state_key, ? AS state_value) AS s
        ON t.state_key = s.state_key
        WHEN MATCHED THEN
          UPDATE SET state_value = s.state_value, updated_at_utc = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
          INSERT (state_key, state_value) VALUES (s.state_key, s.state_value);
        """,
        key, value
    )


# ----------------------------
# Shared ingestion logic: StationDim (full refresh)
# ----------------------------
def run_stationdim_sync(language: str) -> dict:
    logging.info("Syncing StationDim [%s]", language)

    response = _irail_get_stations(language)
    if response.status_code != 200:
        raise RuntimeError(f"iRail stations error {response.status_code}: {response.text}")

    payload = response.json()
    stations = payload.get("station", [])
    if not isinstance(stations, list):
        stations = []

    rows_upserted = 0
    with _get_sql_connection() as conn:
        cur = conn.cursor()

        for s in stations:
            station_id = s.get("id")
            station_uri = s.get("@id")
            standard_name = s.get("standardname")
            name = s.get("name")
            longitude = s.get("locationX")
            latitude = s.get("locationY")

            if not station_id:
                continue

            cur.execute(
                """
                MERGE dbo.StationDim AS target
                USING (SELECT ? AS station_id) AS source
                ON target.station_id = source.station_id
                WHEN MATCHED THEN
                    UPDATE SET
                        station_uri = ?,
                        standard_name = ?,
                        name = ?,
                        longitude = ?,
                        latitude = ?,
                        last_updated_utc = SYSUTCDATETIME()
                WHEN NOT MATCHED THEN
                    INSERT (station_id, station_uri, standard_name, name, longitude, latitude, last_updated_utc)
                    VALUES (?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
                """,
                station_id,
                station_uri, standard_name, name, longitude, latitude,
                station_id, station_uri, standard_name, name, longitude, latitude
            )
            rows_upserted += 1

        conn.commit()

    return {"status": "success", "stations_received": len(stations), "rows_upserted": rows_upserted, "lang": language}


def _get_all_belgian_station_names_from_dim(cur: pyodbc.Cursor) -> list[str]:
    """
    We treat StationDim as the source of Belgian stations.
    iRail stations endpoint is Belgian rail, so this should be all you need.
    We'll call liveboard using StationDim.name (or standard_name).
    """
    # Prefer standard_name if present, fallback to name
    cur.execute(
        """
        SELECT
          COALESCE(NULLIF(standard_name, ''), name) AS station_name
        FROM dbo.StationDim
        WHERE COALESCE(NULLIF(standard_name, ''), name) IS NOT NULL
        ORDER BY station_name;
        """
    )
    return [r[0] for r in cur.fetchall()]


# ----------------------------
# Shared ingestion logic: DepartureFact (single station)
# ----------------------------
def run_liveboard_sync(station_name: str, language: str, cur: pyodbc.Cursor) -> dict:
    cache_key = _cache_key_liveboard(station_name, language)
    last_etag = _get_cached_etag(cur, cache_key)

    response = _irail_get_liveboard(station_name, language, last_etag)

    if response.status_code == 304:
        return {"status": "skipped", "station": station_name, "rows_inserted": 0, "rows_skipped": 0, "etag_saved": False}

    if response.status_code != 200:
        return {"status": "error", "station": station_name, "error": f"{response.status_code}", "rows_inserted": 0, "rows_skipped": 0}

    new_etag = response.headers.get("Etag") or response.headers.get("ETag")
    if new_etag:
        _upsert_etag(cur, cache_key, new_etag)

    payload = response.json()

    board_station_name = payload.get("station") or station_name
    stationinfo = payload.get("stationinfo") or {}
    station_id = stationinfo.get("id")
    station_uri = stationinfo.get("@id")

    departures = ((payload.get("departures") or {}).get("departure")) or []
    if not isinstance(departures, list):
        departures = []

    rows_inserted = 0
    rows_skipped = 0

    for d in departures:
        scheduled_time_utc = _epoch_to_utc_naive(d.get("time"))
        delay_seconds = max(0, int(d.get("delay") or 0))

        is_delayed = 1 if delay_seconds > 0 else 0
        realtime_time_utc = scheduled_time_utc + dt.timedelta(seconds=delay_seconds)

        raw_cancel = d.get("canceled")
        if raw_cancel is None:
            raw_cancel = d.get("isCanceled")
        is_cancelled = _parse_boolish(raw_cancel)

        platform = d.get("platform")
        if isinstance(platform, dict):
            platform = platform.get("name") or platform.get("normal")

        vehicle_id = d.get("vehicle")
        train_type = _train_type_from_vehicle_id(vehicle_id)

        vehicleinfo = d.get("vehicleinfo") or {}
        vehicle_uri = vehicleinfo.get("@id")
        if not vehicle_uri and isinstance(vehicle_id, str) and vehicle_id.startswith("http"):
            vehicle_uri = vehicle_id

        destination_name = d.get("station")
        destinfo = d.get("stationinfo") or {}
        destination_id = destinfo.get("id")
        destination_uri = destinfo.get("@id")

        try:
            cur.execute(
                """
                INSERT INTO dbo.DepartureFact
                (station_name, station_id, station_uri,
                 scheduled_time_utc, delay_seconds, platform,
                 vehicle_id, vehicle_uri, train_type,
                 destination_name, destination_id, destination_uri,
                 is_delayed, is_cancelled, realtime_time_utc)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                board_station_name, station_id, station_uri,
                scheduled_time_utc, delay_seconds, platform,
                vehicle_id, vehicle_uri, train_type,
                destination_name, destination_id, destination_uri,
                is_delayed, is_cancelled, realtime_time_utc
            )
            rows_inserted += 1
        except Exception:
            rows_skipped += 1

    return {"status": "success", "station": station_name, "departures_received": len(departures),
            "rows_inserted": rows_inserted, "rows_skipped": rows_skipped, "etag_saved": bool(new_etag)}


# ----------------------------
# Batch: process stations with cursor
# ----------------------------
def run_departurefact_batch(language: str, batch_size: int) -> dict:
    """
    Processes a batch of stations from StationDim each run.
    This is the safe way to cover ALL stations while respecting iRail limits.
    """
    with _get_sql_connection() as conn:
        cur = conn.cursor()

        stations = _get_all_belgian_station_names_from_dim(cur)
        if not stations:
            return {"status": "error", "message": "StationDim is empty. Run StationDim sync first."}

        offset = int(_get_state(cur, "departure_batch_offset", "0"))
        total = len(stations)

        # Build batch slice and next offset
        batch = stations[offset: offset + batch_size]
        next_offset = offset + len(batch)
        if next_offset >= total:
            next_offset = 0

        inserted = 0
        skipped = 0
        errors = 0

        for st in batch:
            res = run_liveboard_sync(st, language, cur)
            if res.get("status") == "success":
                inserted += int(res.get("rows_inserted", 0))
                skipped += int(res.get("rows_skipped", 0))
            elif res.get("status") == "skipped":
                # 304 Not Modified: no inserts
                pass
            else:
                errors += 1

        _set_state(cur, "departure_batch_offset", str(next_offset))
        conn.commit()

        return {
            "status": "success",
            "stations_total": total,
            "stations_processed": len(batch),
            "batch_offset_start": offset,
            "batch_offset_next": next_offset,
            "rows_inserted": inserted,
            "rows_skipped": skipped,
            "stations_errors": errors,
            "rps": REQUESTS_PER_SECOND,
        }


# ----------------------------
# TIMER TRIGGERS
# ----------------------------

# Stations update: weekly Sunday 03:00 UTC
@app.schedule(schedule="0 0 3 * * 0", arg_name="t_st", run_on_startup=False, use_monitor=True)
def TimerStationDim(t_st: func.TimerRequest) -> None:
    lang = os.environ.get("IRAIL_LANG", "en")
    try:
        result = run_stationdim_sync(lang)
        logging.info("StationDim timer result: %s", result)
    except Exception:
        logging.exception("StationDim timer failed")


# Departures: every 10 minutes, process a batch of stations
@app.schedule(schedule="0 */10 * * * *", arg_name="t_dep", run_on_startup=False, use_monitor=True)
def TimerDepartureFactAllStations(t_dep: func.TimerRequest) -> None:
    lang = os.environ.get("IRAIL_LANG", "en")
    try:
        result = run_departurefact_batch(lang, BATCH_SIZE)
        logging.info("Departure batch timer result: %s", result)
    except Exception:
        logging.exception("Departure batch timer failed")


# ----------------------------
# HTTP TRIGGERS (manual)
# ----------------------------
@app.route(route="LoadStations", auth_level=func.AuthLevel.FUNCTION)
def LoadStations(req: func.HttpRequest) -> func.HttpResponse:
    lang = req.params.get("lang") or os.environ.get("IRAIL_LANG", "en")
    try:
        result = run_stationdim_sync(lang)
        return func.HttpResponse(json.dumps(result), mimetype="application/json")
    except Exception as ex:
        logging.exception("Manual StationDim sync failed")
        return func.HttpResponse(f"Manual StationDim sync failed: {ex}", status_code=500)


@app.route(route="RunDepartureBatch", auth_level=func.AuthLevel.FUNCTION)
def RunDepartureBatch(req: func.HttpRequest) -> func.HttpResponse:
    lang = req.params.get("lang") or os.environ.get("IRAIL_LANG", "en")
    batch_size = int(req.params.get("batch_size") or BATCH_SIZE)
    try:
        result = run_departurefact_batch(lang, batch_size)
        return func.HttpResponse(json.dumps(result), mimetype="application/json")
    except Exception as ex:
        logging.exception("Manual departure batch failed")
        return func.HttpResponse(f"Manual departure batch failed: {ex}", status_code=500)
