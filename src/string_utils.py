import re


def to_snake_case(text: str) -> str:
    """
    Convert a given string to snake_case.

    Args:
        text (str): The input string to convert.

    Returns:
        str: The converted string in snake_case.
    """
    text = text.strip()
    text = re.sub(r"([a-z])([A-Z])", r"\1_\2", text)
    text = re.sub(r"\W+", "_", text)
    text = re.sub(r"_+", "_", text)
    text = text.lower()
    return text


def to_pascal_case(text: str) -> str:
    """
    Convert a given string to PascalCase.

    Args:
        text (str): The input string to convert.

    Returns:
        str: The converted string in PascalCase.
    """
    words = re.split(r"[\W_]+", text)
    return str().join(word.capitalize() for word in words if word)


def to_camel_case(text: str) -> str:
    """
    Convert a given string to camelCase.

    Args:
        text (str): The input string to convert.

    Returns:
        str: The converted string in camelCase.
    """
    if text == "":
        return ""
    words = re.split(r"[\W_]+", text)
    return words[0].lower() + "".join(word.capitalize() for word in words[1:] if word)
