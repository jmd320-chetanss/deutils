from dataclasses import dataclass
from abc import ABC, abstractmethod


@ABC
@dataclass
class ColCleaner:
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

    # The data type to set in the table
    data_type: str = None

    @abstractmethod
    def get_udf(self):
        """
        Returns the UDF to be used to clean a column
        """
        pass
