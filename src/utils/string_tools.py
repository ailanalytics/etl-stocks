"""
String formatting tools
"""

import re   

def normalize_label(value: str) -> str:

    """
    Normalizes string to return snake case
    
    :param value: String to format
    :type value: str
    :return: Snake case
    :rtype: str
    """

    if not value:
        return None
    
    value = value.casefold().strip()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_")