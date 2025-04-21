from dataclasses import dataclass
from .ColCleaner import ColCleaner
from typing import Literal


@dataclass
class PhoneCleaner(ColCleaner):
    """
    A class to clean phone numbers in a DataFrame column.

    Attributes:
        col_name (str): The name of the column to clean.
        df (pd.DataFrame): The DataFrame containing the column to clean.
    """

    # Output format to represent gender
    fmt: Literal["malefemale", "mf"] = "malefemale"

    # Should the output format be in lowercase
    lower: bool = False

    def _get_cleaner(self) -> callable:
        pass
