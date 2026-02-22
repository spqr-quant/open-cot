"""Pydantic models for standardised COT output records."""

from __future__ import annotations

import datetime

import polars as pl
from pydantic import BaseModel


class COTRecord(BaseModel):
    """A single row of Disaggregated Futures Only COT data.

    Field names follow the user-friendly convention rather than the raw
    CFTC column names.
    """

    date: datetime.date
    market_name: str
    cftc_contract_market_code: str
    open_interest: int
    managed_money_long: int
    managed_money_short: int
    managed_money_spread: int
    producer_long: int
    producer_short: int
    swap_long: int
    swap_short: int
    other_reportable_long: int
    other_reportable_short: int

    @classmethod
    def from_polars_row(cls, row: dict[str, object]) -> COTRecord:
        """Construct a ``COTRecord`` from a single Polars row dict.

        Args:
            row: A dictionary produced by iterating over
                ``df.iter_rows(named=True)``.

        Returns:
            A validated ``COTRecord`` instance.
        """
        return cls(
            date=row["As_of_Date_Form_YYYY-MM-DD"],
            market_name=row["Market_and_Exchange_Names"],
            cftc_contract_market_code=row["CFTC_Contract_Market_Code"],
            open_interest=row["Open_Interest_All"],
            managed_money_long=row["M_Money_Positions_Long_All"],
            managed_money_short=row["M_Money_Positions_Short_All"],
            managed_money_spread=row["M_Money_Positions_Spread_All"],
            producer_long=row["Prod_Merc_Positions_Long_All"],
            producer_short=row["Prod_Merc_Positions_Short_All"],
            swap_long=row["Swap_Positions_Long_All"],
            swap_short=row["Swap__Positions_Short_All"],
            other_reportable_long=row["Other_Rept_Positions_Long_All"],
            other_reportable_short=row["Other_Rept_Positions_Short_All"],
        )


def records_from_dataframe(df: pl.DataFrame) -> list[COTRecord]:
    """Convert a full Polars DataFrame into a list of ``COTRecord`` models.

    Args:
        df: A DataFrame as returned by ``parse_cot_zip``.

    Returns:
        A list of validated ``COTRecord`` instances, one per row.
    """
    return [COTRecord.from_polars_row(row) for row in df.iter_rows(named=True)]
