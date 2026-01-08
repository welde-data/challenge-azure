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
        r = requests.get(IRAIL_LIVEBOARD_URL, params=params, headers=headers, timeout=20)
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
        r = requests.get(IRAIL_STATIONS_URL, params=params, headers=headers, timeout=30)
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
# Shared ingestion logic: DepartureFact
# ----------------------------
def run_liveboard_sync(station_name: str, language: str) -> dict:
    logging.info("Syncing liveboard for: %s [%s]", station_name, language)

    with _get_sql_connection() as conn:
        cur = conn.cursor()

        cache_key = _cache_key_liveboard(station_name, language)
        last_etag = _get_cached_etag(cur, cache_key)

        response = _irail_get_liveboard(station_name, language, last_etag)

        if response.status_code == 304:
            return {
                "status": "skipped",
                "message": "No new data (304)",
                "rows_inserted": 0,
                "rows_skipped": 0,
                "etag_saved": False,
            }

        if response.status_code != 200:
            raise RuntimeError(f"iRail error {response.status_code}: {response.text}")

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

        conn.commit()

        return {
            "status": "success",
            "station": station_name,
            "departures_received": len(departures),
            "rows_inserted": rows_inserted,
            "rows_skipped": rows_skipped,
            "etag_saved": bool(new_etag),
        }


# ----------------------------
# Shared ingestion logic: StationDim
# ----------------------------
def run_stationdim_sync(language: str) -> dict:
    logging.info("Syncing StationDim (stations endpoint) [%s]", language)

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

    return {
        "status": "success",
        "stations_received": len(stations),
        "rows_upserted": rows_upserted,
        "lang": language,
    }


# ----------------------------
# TIMER TRIGGERS
# ----------------------------

# Departures: every 10 minutes (UTC)
@app.schedule(schedule="0 */10 * * * *", arg_name="t1", run_on_startup=False, use_monitor=True)
def TimerDepartureFact(t1: func.TimerRequest) -> None:
    station = os.environ.get("IRAIL_STATION", "Gent-Sint-Pieters")
    lang = os.environ.get("IRAIL_LANG", "en")

    if t1.past_due:
        logging.warning("Departure timer is running late (past_due=True).")

    try:
        result = run_liveboard_sync(station, lang)
        logging.info("Departure timer result: %s", result)
    except Exception:
        logging.exception("Departure timer failed")


# Stations: weekly on Sunday 03:00 UTC
@app.schedule(schedule="0 0 3 * * 0", arg_name="t2", run_on_startup=False, use_monitor=True)
def TimerStationDim(t2: func.TimerRequest) -> None:
    lang = os.environ.get("IRAIL_LANG", "en")

    if t2.past_due:
        logging.warning("StationDim timer is running late (past_due=True).")

    try:
        result = run_stationdim_sync(lang)
        logging.info("StationDim timer result: %s", result)
    except Exception:
        logging.exception("StationDim timer failed")


# ----------------------------
# HTTP TRIGGERS (manual run)
# ----------------------------

@app.route(route="FetchLiveboardToSql", auth_level=func.AuthLevel.FUNCTION)
def FetchLiveboardToSql(req: func.HttpRequest) -> func.HttpResponse:
    station = req.params.get("station") or os.environ.get("IRAIL_STATION", "Gent-Sint-Pieters")
    lang = req.params.get("lang") or os.environ.get("IRAIL_LANG", "en")

    try:
        result = run_liveboard_sync(station, lang)
        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
    except Exception as ex:
        logging.exception("Manual departure sync failed")
        return func.HttpResponse(f"Manual departure sync failed: {ex}", status_code=500)


@app.route(route="LoadStations", auth_level=func.AuthLevel.FUNCTION)
def LoadStations(req: func.HttpRequest) -> func.HttpResponse:
    lang = req.params.get("lang") or os.environ.get("IRAIL_LANG", "en")

    try:
        result = run_stationdim_sync(lang)
        return func.HttpResponse(json.dumps(result), mimetype="application/json", status_code=200)
    except Exception as ex:
        logging.exception("Manual StationDim sync failed")
        return func.HttpResponse(f"Manual StationDim sync failed: {ex}", status_code=500)
