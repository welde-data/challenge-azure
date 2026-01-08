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


def _epoch_to_utc_naive(epoch_seconds: int) -> dt.datetime:
    """Convert UNIX epoch seconds to UTC naive datetime (good for SQL DATETIME2)."""
    return dt.datetime.fromtimestamp(int(epoch_seconds), tz=dt.timezone.utc).replace(tzinfo=None)


def _train_type_from_vehicle_id(vehicle_id: str | None) -> str | None:
    """Extract a basic train type like IC, S, L, P from iRail vehicle id."""
    if not vehicle_id:
        return None
    last = vehicle_id.split(".")[-1]
    for prefix in ("IC", "S", "L", "P"):
        if last.startswith(prefix):
            return prefix
    return None


def _parse_boolish(value) -> int | None:
    """
    Convert various truthy/falsey representations to 1/0.
    Returns None if value is missing/unknown.
    """
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
    """
    Azure Functions on Linux commonly has ODBC Driver 18 installed.
    but local Windows dev often has Driver 17 installed.
    We auto-detect to keep the same code working everywhere.
    """
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

    # Minimal + compatible connection string
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
    """
    Call iRail liveboard endpoint with:
    - proper User-Agent
    - caching (If-None-Match)
    - basic 429 backoff
    """
    user_agent = os.environ.get(
        "IRAIL_USER_AGENT",
        "irail-azure-pipeline/0.1 (no-contact-provided)"
    )

    headers = {"User-Agent": user_agent, "Accept": "application/json"}
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


@app.route(route="FetchLiveboardToSql", auth_level=func.AuthLevel.FUNCTION)
def FetchLiveboardToSql(req: func.HttpRequest) -> func.HttpResponse:
    station = req.params.get("station") or os.environ.get("IRAIL_STATION", "Gent-Sint-Pieters")
    lang = os.environ.get("IRAIL_LANG", "en")

    try:
        with _get_sql_connection() as conn:
            cur = conn.cursor()

            cache_key = _cache_key_liveboard(station, lang)
            last_etag = _get_cached_etag(cur, cache_key)

            r = _irail_get_liveboard(station, lang, last_etag)

            if r.status_code == 304:
                return func.HttpResponse(
                    json.dumps({"station": station, "status": "not_modified", "rows_inserted": 0}),
                    mimetype="application/json",
                    status_code=200
                )

            if r.status_code != 200:
                return func.HttpResponse(f"iRail error {r.status_code}: {r.text}", status_code=502)

            new_etag = r.headers.get("Etag") or r.headers.get("ETag")
            if new_etag:
                _upsert_etag(cur, cache_key, new_etag)

            payload = r.json()

            board_station_name = payload.get("station") or station
            stationinfo = payload.get("stationinfo") or {}
            station_id = stationinfo.get("id")
            station_uri = stationinfo.get("@id")

            departures = ((payload.get("departures") or {}).get("departure")) or []

            rows_inserted = 0
            rows_skipped = 0

            for d in departures:
                scheduled_time_utc = _epoch_to_utc_naive(d.get("time"))
                delay_seconds = max(0, int(d.get("delay") or 0))

                # NEW fields
                is_delayed = 1 if delay_seconds > 0 else 0
                realtime_time_utc = scheduled_time_utc + dt.timedelta(seconds=delay_seconds)

                # Cancellation (only if present; otherwise NULL)
                raw_cancel = d.get("canceled")
                if raw_cancel is None:
                    raw_cancel = d.get("isCanceled")
                is_cancelled = _parse_boolish(raw_cancel)  # None if unknown/missing

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
                         destination_name, destination_id, destination_uri,
                         scheduled_time_utc, delay_seconds, is_delayed, realtime_time_utc, is_cancelled,
                         platform,
                         vehicle_id, vehicle_uri, train_type)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        board_station_name, station_id, station_uri,
                        destination_name, destination_id, destination_uri,
                        scheduled_time_utc, delay_seconds, is_delayed, realtime_time_utc, is_cancelled,
                        platform,
                        vehicle_id, vehicle_uri, train_type
                    )
                    rows_inserted += 1
                except Exception:
                    # likely duplicate due to UNIQUE index
                    rows_skipped += 1

            conn.commit()

            return func.HttpResponse(
                json.dumps({
                    "station": station,
                    "departures_received": len(departures),
                    "rows_inserted": rows_inserted,
                    "rows_skipped": rows_skipped,
                    "etag_saved": bool(new_etag)
                }),
                mimetype="application/json",
                status_code=200
            )

    except Exception as ex:
        logging.exception("SQL insert failed")
        return func.HttpResponse(f"SQL insert failed: {ex}", status_code=500)
    #Http for fetching longitude and latitude of a station


@app.route(route="LoadStations", auth_level=func.AuthLevel.FUNCTION)
def LoadStations(req: func.HttpRequest) -> func.HttpResponse:
    lang = os.environ.get("IRAIL_LANG", "en")

    user_agent = os.environ.get(
        "IRAIL_USER_AGENT",
        "irail-azure-pipeline/0.1 (no-contact-provided)"
    )

    headers = {"User-Agent": user_agent, "Accept": "application/json"}
    params = {"format": "json", "lang": lang}

    r = requests.get(IRAIL_STATIONS_URL, params=params, headers=headers, timeout=30)
    if r.status_code != 200:
        return func.HttpResponse(f"iRail error {r.status_code}: {r.text}", status_code=502)

    payload = r.json()

    # iRail returns a list; some versions use "station" as list, others "stations" wrapper.
    stations = payload.get("station")
    if not isinstance(stations, list):
        # fallback: try common wrappers
        stations = (payload.get("stations") or {}).get("station", [])
    if not isinstance(stations, list):
        stations = []

    rows_upserted = 0

    try:
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
                        INSERT (station_id, station_uri, standard_name, name, longitude, latitude)
                        VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    station_id,
                    station_uri, standard_name, name, longitude, latitude,
                    station_id, station_uri, standard_name, name, longitude, latitude
                )

                rows_upserted += 1

            conn.commit()

    except Exception as ex:
        logging.exception("StationDim upsert failed")
        return func.HttpResponse(f"StationDim upsert failed: {ex}", status_code=500)

    return func.HttpResponse(
        json.dumps({"stations_received": len(stations), "rows_upserted": rows_upserted}),
        mimetype="application/json",
        status_code=200
    )
