"""Constants for CFTC Commitment of Traders data access."""

BASE_URL: str = "https://www.cftc.gov/files/dea/history/fut_disagg_txt_{year}.zip"
"""URL template for Disaggregated Futures Only ZIP archives by year."""

TICKER_TO_CFTC_CODE: dict[str, str] = {
    "GC": "088691",
    "SI": "084691",
    "HG": "085692",
    "PL": "076651",
}
"""Map of friendly ticker symbols to CFTC_Contract_Market_Code values."""

RELEVANT_COLUMNS: list[str] = [
    "Market_and_Exchange_Names",
    "As_of_Date_Form_YYYY-MM-DD",
    "CFTC_Contract_Market_Code",
    "CFTC_Commodity_Code",
    "Open_Interest_All",
    "Prod_Merc_Positions_Long_All",
    "Prod_Merc_Positions_Short_All",
    "M_Money_Positions_Long_All",
    "M_Money_Positions_Short_All",
    "M_Money_Positions_Spread_All",
    "Swap_Positions_Long_All",
    "Swap__Positions_Short_All",
    "Other_Rept_Positions_Long_All",
    "Other_Rept_Positions_Short_All",
]
"""Subset of CSV columns extracted during parsing for efficiency."""
