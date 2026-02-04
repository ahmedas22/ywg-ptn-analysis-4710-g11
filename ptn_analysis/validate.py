"""
Data validation module using Pandera schemas.

Provides schema definitions and validation functions for GTFS data
to ensure data quality before loading to the database.

Usage:
    python -m ptn_analysis.validate all
    python -m ptn_analysis.validate stops

Attributes:
    SCHEMAS: Registry of available validation schemas.
    SKIP_VALIDATION: Flag to bypass validation (from PTN_SKIP_VALIDATION env).
"""

from __future__ import annotations

import re

from loguru import logger
import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema
import typer

from ptn_analysis.config import SKIP_VALIDATION, WPG_BOUNDS

app = typer.Typer(help="Data validation commands")


def validate_gtfs_time(time_str: str | None) -> bool:
    """Validate GTFS time format (HH:MM:SS), allowing hours > 24."""
    if pd.isna(time_str):
        return True
    pattern = r"^(\d{1,2}):([0-5]\d):([0-5]\d)$"
    match = re.match(pattern, str(time_str))
    if not match:
        return False
    hours = int(match.group(1))
    return hours < 48


def gtfs_time_check(series: pd.Series) -> pd.Series:
    """Pandera check function for GTFS time format."""
    return series.apply(validate_gtfs_time)


StopsSchema = DataFrameSchema(
    columns={
        "stop_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Unique identifier for transit stop",
        ),
        "stop_name": Column(
            str,
            nullable=True,
            coerce=True,
            description="Human-readable stop name",
        ),
        "stop_lat": Column(
            float,
            nullable=False,
            coerce=True,
            checks=[
                pa.Check.ge(WPG_BOUNDS["min_lat"], error="Latitude below Winnipeg bounds"),
                pa.Check.le(WPG_BOUNDS["max_lat"], error="Latitude above Winnipeg bounds"),
            ],
            description="WGS84 latitude within Winnipeg bounds",
        ),
        "stop_lon": Column(
            float,
            nullable=False,
            coerce=True,
            checks=[
                pa.Check.ge(WPG_BOUNDS["min_lon"], error="Longitude below Winnipeg bounds"),
                pa.Check.le(WPG_BOUNDS["max_lon"], error="Longitude above Winnipeg bounds"),
            ],
            description="WGS84 longitude within Winnipeg bounds",
        ),
    },
    name="StopsSchema",
    description="GTFS stops.txt validated against Winnipeg geographic bounds",
    strict=False,
    coerce=True,
)

RoutesSchema = DataFrameSchema(
    columns={
        "route_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Unique identifier for route",
        ),
        "route_short_name": Column(
            str,
            nullable=True,
            coerce=True,
            description="Short public-facing route name (e.g., '11')",
        ),
        "route_long_name": Column(
            str,
            nullable=True,
            coerce=True,
            description="Full route name (e.g., 'Portage Express')",
        ),
        "route_type": Column(
            int,
            nullable=False,
            coerce=True,
            description="GTFS route type (0=tram, 3=bus, etc.)",
        ),
    },
    name="RoutesSchema",
    description="GTFS routes.txt with route identifiers and types",
    strict=False,
    coerce=True,
)

TripsSchema = DataFrameSchema(
    columns={
        "trip_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Unique identifier for trip",
        ),
        "route_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Foreign key to routes.route_id",
        ),
        "service_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Service calendar identifier",
        ),
    },
    name="TripsSchema",
    description="GTFS trips.txt linking routes to scheduled trips",
    strict=False,
    coerce=True,
)

StopTimesSchema = DataFrameSchema(
    columns={
        "trip_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Foreign key to trips.trip_id",
        ),
        "stop_id": Column(
            str,
            nullable=False,
            coerce=True,
            description="Foreign key to stops.stop_id",
        ),
        "stop_sequence": Column(
            int,
            nullable=False,
            coerce=True,
            checks=pa.Check.ge(0),
            description="Order of stop within trip (0-indexed)",
        ),
        "arrival_time": Column(
            str,
            nullable=True,
            coerce=True,
            checks=pa.Check(gtfs_time_check, error="Invalid GTFS time format"),
            description="Arrival time in HH:MM:SS (may exceed 24:00)",
        ),
        "departure_time": Column(
            str,
            nullable=True,
            coerce=True,
            checks=pa.Check(gtfs_time_check, error="Invalid GTFS time format"),
            description="Departure time in HH:MM:SS (may exceed 24:00)",
        ),
    },
    name="StopTimesSchema",
    description="GTFS stop_times.txt with arrival/departure times per stop",
    strict=False,
    coerce=True,
)

SCHEMAS: dict[str, DataFrameSchema] = {
    "stops": StopsSchema,
    "routes": RoutesSchema,
    "trips": TripsSchema,
    "stop_times": StopTimesSchema,
}


def validate_dataframe(
    df: pd.DataFrame,
    schema: DataFrameSchema,
    name: str,
) -> pd.DataFrame:
    """Validate a DataFrame against a Pandera schema."""
    if SKIP_VALIDATION:
        logger.warning(f"Skipping validation for {name} (PTN_SKIP_VALIDATION=1)")
        return df

    logger.info(f"Validating {name} ({len(df):,} rows)")

    try:
        validated_df = schema.validate(df, lazy=True)
        logger.info(f"Validation passed for {name}")
        return validated_df
    except pa.errors.SchemaErrors as e:
        logger.error(f"Validation failed for {name}:")
        for failure in e.failure_cases.head(10).itertuples():
            logger.error(f"  - {failure}")
        raise


def validate_stops(df: pd.DataFrame) -> pd.DataFrame:
    """Validate stops DataFrame against StopsSchema."""
    return validate_dataframe(df, StopsSchema, "stops")


def validate_routes(df: pd.DataFrame) -> pd.DataFrame:
    """Validate routes DataFrame against RoutesSchema."""
    return validate_dataframe(df, RoutesSchema, "routes")


def validate_trips(df: pd.DataFrame) -> pd.DataFrame:
    """Validate trips DataFrame against TripsSchema."""
    return validate_dataframe(df, TripsSchema, "trips")


def validate_stop_times(df: pd.DataFrame) -> pd.DataFrame:
    """Validate stop_times DataFrame against StopTimesSchema."""
    return validate_dataframe(df, StopTimesSchema, "stop_times")


@app.command()
def stops() -> None:
    """Validate stops data from local DuckDB database."""
    from ptn_analysis.data.db import get_duckdb

    conn = get_duckdb()
    try:
        df = conn.execute("SELECT * FROM raw_gtfs_stops").fetchdf()
        validate_stops(df)
        typer.echo(f"Stops validation passed ({len(df):,} rows)")
    except Exception as e:
        typer.echo(f"Stops validation failed: {e}")
        raise typer.Exit(1)


@app.command()
def routes() -> None:
    """Validate routes data from local DuckDB database."""
    from ptn_analysis.data.db import get_duckdb

    conn = get_duckdb()
    try:
        df = conn.execute("SELECT * FROM raw_gtfs_routes").fetchdf()
        validate_routes(df)
        typer.echo(f"Routes validation passed ({len(df):,} rows)")
    except Exception as e:
        typer.echo(f"Routes validation failed: {e}")
        raise typer.Exit(1)


@app.command()
def trips() -> None:
    """Validate trips data from local DuckDB database."""
    from ptn_analysis.data.db import get_duckdb

    conn = get_duckdb()
    try:
        df = conn.execute("SELECT * FROM raw_gtfs_trips").fetchdf()
        validate_trips(df)
        typer.echo(f"Trips validation passed ({len(df):,} rows)")
    except Exception as e:
        typer.echo(f"Trips validation failed: {e}")
        raise typer.Exit(1)


@app.command(name="stop-times")
def stop_times_cmd(
    limit: int = typer.Option(100_000, help="Maximum rows to validate (0 for all)"),
) -> None:
    """Validate stop_times data from local DuckDB database."""
    from ptn_analysis.data.db import get_duckdb

    conn = get_duckdb()
    try:
        query = "SELECT * FROM raw_gtfs_stop_times"
        if limit > 0:
            query += f" LIMIT {limit}"
        df = conn.execute(query).fetchdf()
        validate_stop_times(df)
        suffix = " (sampled)" if limit > 0 else ""
        typer.echo(f"Stop times validation passed ({len(df):,} rows{suffix})")
    except Exception as e:
        typer.echo(f"Stop times validation failed: {e}")
        raise typer.Exit(1)


@app.command(name="all")
def validate_all_cmd(
    stop_times_limit: int = typer.Option(
        100_000, help="Maximum stop_times rows to validate (0 for all)"
    ),
) -> None:
    """Validate all GTFS data from local DuckDB database."""
    from ptn_analysis.data.db import get_duckdb

    if SKIP_VALIDATION:
        typer.echo("Validation skipped (PTN_SKIP_VALIDATION=1)")
        return

    conn = get_duckdb()
    failed: list[str] = []

    tables = [
        ("raw_gtfs_stops", validate_stops, "stops"),
        ("raw_gtfs_routes", validate_routes, "routes"),
        ("raw_gtfs_trips", validate_trips, "trips"),
    ]

    for table, validator, name in tables:
        try:
            df = conn.execute(f"SELECT * FROM {table}").fetchdf()
            validator(df)
            typer.echo(f"  {name}: passed ({len(df):,} rows)")
        except Exception as e:
            typer.echo(f"  {name}: FAILED - {e}")
            failed.append(name)

    try:
        query = "SELECT * FROM raw_gtfs_stop_times"
        if stop_times_limit > 0:
            query += f" LIMIT {stop_times_limit}"
        df = conn.execute(query).fetchdf()
        validate_stop_times(df)
        suffix = " sampled" if stop_times_limit > 0 else ""
        typer.echo(f"  stop_times: passed ({len(df):,} rows{suffix})")
    except Exception as e:
        typer.echo(f"  stop_times: FAILED - {e}")
        failed.append("stop_times")

    if failed:
        typer.echo(f"\nValidation failed for: {', '.join(failed)}")
        raise typer.Exit(1)
    else:
        typer.echo("\nAll validations passed!")


@app.command()
def status() -> None:
    """Show validation configuration and available schemas."""
    typer.echo("Validation Configuration:")
    typer.echo(f"  Skip validation: {SKIP_VALIDATION}")
    typer.echo(f"  Winnipeg bounds: {WPG_BOUNDS}")
    typer.echo(f"  Available schemas: {', '.join(SCHEMAS.keys())}")


if __name__ == "__main__":
    app()
