import math


def floor_float(value: float, decimal_places: int = 2) -> float:
    """
    Floor a float value to a specified number of decimal places.

    Args:
        value (float): The float value to floor.
        decimal_places (int): The number of decimal places to keep. Defaults to 2.

    Returns:
        float: The floored float value.
    """
    factor = 10**decimal_places
    return math.floor(value * factor) / factor


def parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    try:
        return float(value)
    except ValueError:
        return None
