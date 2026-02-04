"""Table load configuration for GTFS and boundary datasets."""

from dataclasses import dataclass, field


@dataclass
class GTFSTableConfig:
    """Configuration for loading one GTFS table."""

    filename: str
    table_name: str
    columns: list[str]
    ddl: str
    dtypes: dict[str, type] = field(default_factory=dict)
    date_columns: list[str] = field(default_factory=list)
    use_chunking: bool = False
    log_name: str = ""

    def __post_init__(self) -> None:
        if not self.log_name:
            self.log_name = self.filename


@dataclass
class BoundaryTableConfig:
    """Configuration for loading one boundary dataset."""

    dataset_key: str
    table_name: str
    name_fields: list[str]
    area_fields: list[str]
    ddl: str
    log_name: str = ""

    def __post_init__(self) -> None:
        if not self.log_name:
            self.log_name = self.table_name.replace("_", " ")


def _gtfs(
    *,
    filename: str,
    table: str,
    columns: list[str],
    ddl: str,
    dtypes: dict[str, type] | None = None,
    date_columns: list[str] | None = None,
    use_chunking: bool = False,
    log_name: str | None = None,
) -> GTFSTableConfig:
    """Create GTFS table config."""
    return GTFSTableConfig(
        filename=filename,
        table_name=table,
        columns=columns,
        ddl=ddl,
        dtypes=dtypes or {},
        date_columns=date_columns or [],
        use_chunking=use_chunking,
        log_name=log_name or "",
    )


def _boundary(
    *,
    dataset_key: str,
    table: str,
    name_fields: list[str],
    area_fields: list[str],
    ddl: str,
    log_name: str | None = None,
) -> BoundaryTableConfig:
    """Create boundary table config."""
    return BoundaryTableConfig(
        dataset_key=dataset_key,
        table_name=table,
        name_fields=name_fields,
        area_fields=area_fields,
        ddl=ddl,
        log_name=log_name or "",
    )


GTFS_TABLES: list[GTFSTableConfig] = [
    _gtfs(
        filename="stops.txt",
        table="gtfs_stops",
        columns=["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"],
        ddl="""
            CREATE TABLE raw_gtfs_stops (
                stop_id TEXT PRIMARY KEY,
                stop_code TEXT,
                stop_name TEXT,
                stop_lat DOUBLE,
                stop_lon DOUBLE
            )
        """,
        dtypes={"stop_id": str, "stop_code": str},
        log_name="stops",
    ),
    _gtfs(
        filename="routes.txt",
        table="gtfs_routes",
        columns=["route_id", "route_short_name", "route_long_name", "route_type"],
        ddl="""
            CREATE TABLE raw_gtfs_routes (
                route_id TEXT PRIMARY KEY,
                route_short_name TEXT,
                route_long_name TEXT,
                route_type INTEGER
            )
        """,
        dtypes={"route_id": str},
        log_name="routes",
    ),
    _gtfs(
        filename="trips.txt",
        table="gtfs_trips",
        columns=["trip_id", "route_id", "service_id", "trip_headsign", "direction_id"],
        ddl="""
            CREATE TABLE raw_gtfs_trips (
                trip_id TEXT PRIMARY KEY,
                route_id TEXT,
                service_id TEXT,
                trip_headsign TEXT,
                direction_id INTEGER
            )
        """,
        dtypes={"trip_id": str, "route_id": str, "service_id": str},
        log_name="trips",
    ),
    _gtfs(
        filename="stop_times.txt",
        table="gtfs_stop_times",
        columns=["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        ddl="""
            CREATE TABLE raw_gtfs_stop_times (
                trip_id TEXT,
                arrival_time TEXT,
                departure_time TEXT,
                stop_id TEXT,
                stop_sequence INTEGER,
                PRIMARY KEY (trip_id, stop_sequence)
            )
        """,
        dtypes={"trip_id": str, "stop_id": str},
        use_chunking=True,
        log_name="stop_times",
    ),
    _gtfs(
        filename="calendar.txt",
        table="gtfs_calendar",
        columns=[
            "service_id",
            "sunday",
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "start_date",
            "end_date",
        ],
        ddl="""
            CREATE TABLE raw_gtfs_calendar (
                service_id TEXT PRIMARY KEY,
                sunday INTEGER NOT NULL,
                monday INTEGER NOT NULL,
                tuesday INTEGER NOT NULL,
                wednesday INTEGER NOT NULL,
                thursday INTEGER NOT NULL,
                friday INTEGER NOT NULL,
                saturday INTEGER NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL
            )
        """,
        dtypes={"service_id": str},
        date_columns=["start_date", "end_date"],
        log_name="calendar",
    ),
    _gtfs(
        filename="calendar_dates.txt",
        table="gtfs_calendar_dates",
        columns=["service_id", "date", "exception_type"],
        ddl="""
            CREATE TABLE raw_gtfs_calendar_dates (
                service_id TEXT NOT NULL,
                date DATE NOT NULL,
                exception_type INTEGER NOT NULL,
                PRIMARY KEY (service_id, date)
            )
        """,
        dtypes={"service_id": str},
        date_columns=["date"],
        log_name="calendar_dates",
    ),
    _gtfs(
        filename="shapes.txt",
        table="gtfs_shapes",
        columns=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"],
        ddl="""
            CREATE TABLE raw_gtfs_shapes (
                shape_id TEXT,
                shape_pt_lat DOUBLE,
                shape_pt_lon DOUBLE,
                shape_pt_sequence INTEGER,
                PRIMARY KEY (shape_id, shape_pt_sequence)
            )
        """,
        dtypes={"shape_id": str},
        use_chunking=True,
        log_name="shapes",
    ),
    _gtfs(
        filename="feed_info.txt",
        table="gtfs_feed_info",
        columns=[
            "feed_publisher_name",
            "feed_publisher_url",
            "feed_lang",
            "feed_contact_email",
            "feed_start_date",
            "feed_end_date",
        ],
        ddl="""
            CREATE TABLE raw_gtfs_feed_info (
                feed_publisher_name TEXT,
                feed_publisher_url TEXT,
                feed_lang TEXT,
                feed_contact_email TEXT,
                feed_start_date TEXT,
                feed_end_date TEXT
            )
        """,
        log_name="feed_info",
    ),
]


BOUNDARY_TABLES: list[BoundaryTableConfig] = [
    _boundary(
        dataset_key="neighbourhoods",
        table="neighbourhoods",
        name_fields=["name", "NAME"],
        area_fields=["area_km2", "AREA_KM2"],
        ddl="""
            CREATE TABLE raw_neighbourhoods (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                area_km2 DOUBLE,
                geometry GEOMETRY
            )
        """,
        log_name="neighbourhoods",
    ),
    _boundary(
        dataset_key="communities",
        table="community_areas",
        name_fields=["name", "NAME"],
        area_fields=["area_km2", "AREA_KM2"],
        ddl="""
            CREATE TABLE raw_community_areas (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                area_km2 DOUBLE,
                geometry GEOMETRY
            )
        """,
        log_name="community areas",
    ),
]
