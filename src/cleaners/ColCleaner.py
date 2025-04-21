from dataclasses import dataclass
from typing import Optional
from abc import ABC, abstractmethod
from pyspark.sql import Column
import pyspark.sql.functions as spf


@dataclass
class ColCleaner(ABC):
    # Can the column contain null values
    nullable: bool = True

    # Every value must be unique
    unique: bool = False

    # Convert empty values to null
    empty_to_null: bool = True

    # What to replace null values with
    # NOTE: this value is then parsed by the logic as they were already there
    on_null: Optional[str] = None

    # Rename the column
    rename_to: Optional[str] = None

    # Is this a key column
    key: bool = False

    # The type of the column for the database table
    datatype: str = "string"

    def clean_col(self, col: Column) -> Column:
        """
        Clean the column using the cleaner function.

        :param col: The column to clean.
        :return: The cleaned column.
        """
        cleaner = self._get_cleaner()
        cleaner_udf = spf.udf(cleaner, "string")
        return cleaner_udf(col).cast(self.datatype)

    @abstractmethod
    def _get_cleaner(self) -> callable:
        """
        Get the cleaner function for the column.
        """
        pass
