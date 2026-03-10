"""
reusable module for downloading WRDS data to local Parquet files.

from wrds_downloader import WRDSDownloader
dl = WRDSDownloader()
dl.download("crsp", "msf")
dl.download("crsp", "msf", date_col="date", start_year=2010, end_year=2023)
dl.download(
    library="compustat",
    table="funda",
    date_col="datadate",
    start_year=2000,
    end_year=2023,
    signal_cols="gvkey, datadate, at, sale, ni",
    extra_query="AND indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'",
)
dl.download("taq", "ct_20230103", chunksize=500_000)
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
logger = logging.getLogger(__name__)


# internal helper functions
def _save_to_parquet(
    df: pd.DataFrame,
    base_dir: Path,
    library: str,
    table: str,
    year: Optional[int],
) -> Path:
    # persist df to <base_dir>/<library>/<table>/<table>[_<year>].parquet
    dest_dir = base_dir / library / table
    dest_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{table}_{year}.parquet" if year else f"{table}.parquet"
    dest = dest_dir / file_name
    df.to_parquet(dest, index=False)
    logger.info("Saved %d rows → %s", len(df), dest)
    return dest

def _build_query(
    library: str,
    table: str,
    columns: str,
    date_col: Optional[str],
    year: Optional[int],
    extra_query: str,
) -> str:
    query = f"SELECT {columns} FROM {library}.{table} WHERE 1=1"
    if date_col and year:
        query += f" AND {date_col} BETWEEN '{year}-01-01' AND '{year}-12-31'"
    if extra_query:
        query += f" {extra_query.strip()}"
    return query

# API
class WRDSDownloader:
    def __init__(
        self,
        wrds_username: Optional[str] = None,
        output_dir: str | Path = "./data/wrds",
        autoconnect: bool = True,
    ) -> None:
        self.output_dir = Path(output_dir)
        self._db: Optional[object] = None  # wrds.Connection

        if autoconnect:
            self.connect(wrds_username)

    # manage connection
    def connect(self, wrds_username: Optional[str] = None) -> None:
        # open wrds postgresql connection
        try:
            import wrds  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "The 'wrds' package is required.  Install it with:\n"
                "    pip install wrds"
            ) from exc

        kwargs = {}
        if wrds_username:
            kwargs["wrds_username"] = wrds_username

        self._db = wrds.Connection(**kwargs)
        logger.info("Connected to WRDS.")
    def disconnect(self) -> None:
        # close  WRDS connection
        if self._db is not None:
            self._db.close()
            self._db = None
            logger.info("WRDS connection closed.")

    # context manager support
    def __enter__(self) -> "WRDSDownloader":
        return self
    def __exit__(self, *_) -> None:
        self.disconnect()

    # download table method
    def download(
        self,
        library: str,
        table: str,
        date_col: Optional[str] = None,
        start_year: int = 1990,
        end_year: int = 2025,
        extra_query: str = "",
        signal_cols: Optional[str] = None,
        chunksize: Optional[int] = None,
    ) -> list[Path]:
        
        if self._db is None:
            raise RuntimeError("Not connected to WRDS.  Call .connect() first.")

        columns = signal_cols if signal_cols else "*"
        years: list[Optional[int]] = list(range(start_year, end_year + 1)) if date_col else [None]
        written: list[Path] = []

        for year in years:
            query = _build_query(library, table, columns, date_col, year, extra_query)
            logger.debug("Query: %s", query)

            df = self._fetch(query, chunksize)

            if df.empty:
                label = f" year={year}" if year else ""
                logger.warning("[SKIP] %s.%s%s — empty result", library, table, label)
                print(f"[SKIP] {library}.{table}{' year=' + str(year) if year else ''} — empty result")
                continue

            path = _save_to_parquet(df, self.output_dir, library, table, year)
            written.append(path)

        return written

    # helper functions and supplement
    def list_libraries(self) -> list[str]:
        # available all WRDS libraries
        self._require_connection()
        return self._db.list_libraries()
    def list_tables(self, library: str) -> list[str]:
        # return all tables in library
        self._require_connection()
        return self._db.list_tables(library=library)
    def describe_table(self, library: str, table: str) -> pd.DataFrame:
        # return a df describing the columns of library.table
        self._require_connection()
        return self._db.describe_table(library=library, table=table)
    def raw_sql(self, query: str, **kwargs) -> pd.DataFrame:
        # execute arbitrary sql and return df
        self._require_connection()
        return self._db.raw_sql(query, **kwargs)
    def _require_connection(self) -> None:
        # ensure connection to wrds
        if self._db is None:
            raise RuntimeError("Not connected to WRDS.  Call .connect() first.")
    def _fetch(self, query: str, chunksize: Optional[int]) -> pd.DataFrame:
        # run query, optionally in chunks, and return a single df
        if chunksize:
            chunks = list(self._db.raw_sql(query, chunksize=chunksize))
            return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        return self._db.raw_sql(query)
