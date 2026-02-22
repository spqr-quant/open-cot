"""In-memory parser for CFTC Disaggregated Futures Only ZIP archives."""

from __future__ import annotations

import io
import zipfile

import polars as pl

from open_cot.constants import RELEVANT_COLUMNS


def parse_cot_zip(
    data: bytes,
    cftc_codes: list[str] | None = None,
) -> pl.DataFrame:
    """Parse a CFTC Disaggregated Futures Only ZIP into a Polars DataFrame.

    The ZIP is extracted entirely in memory. Only the columns listed in
    ``RELEVANT_COLUMNS`` are retained, and the date column is cast to
    ``pl.Date``.

    Args:
        data: Raw ZIP bytes (as returned by ``AsyncDownloader.download``).
        cftc_codes: Optional list of ``CFTC_Contract_Market_Code`` values to
            keep. When *None*, all rows are returned.

    Returns:
        A ``polars.DataFrame`` with the selected columns, filtered and typed.

    Raises:
        zipfile.BadZipFile: If *data* is not a valid ZIP archive.
        FileNotFoundError: If the archive contains no ``.txt`` file.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        txt_names = [n for n in zf.namelist() if n.lower().endswith(".txt")]
        if not txt_names:
            raise FileNotFoundError("No .txt file found inside the ZIP archive")

        csv_bytes = zf.read(txt_names[0])

    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        columns=RELEVANT_COLUMNS,
        try_parse_dates=False,
    )

    # CFTC data has trailing whitespace on string columns
    df = df.with_columns(
        pl.col("CFTC_Contract_Market_Code").str.strip_chars(),
        pl.col("CFTC_Commodity_Code").str.strip_chars(),
        pl.col("Market_and_Exchange_Names").str.strip_chars(),
    )

    df = df.with_columns(
        pl.col("As_of_Date_Form_YYYY-MM-DD").str.to_date("%Y-%m-%d"),
    )

    if cftc_codes is not None:
        df = df.filter(pl.col("CFTC_Contract_Market_Code").is_in(cftc_codes))

    return df
