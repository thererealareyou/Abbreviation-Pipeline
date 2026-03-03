import re
from rapidfuzz import fuzz
from collections import defaultdict


def verify_expansion(abbr: str, expansion: str, chunk_text: str, similarity_threshold: float = 80.0) -> bool:
    """
    Проверяет, является ли предложенная моделью расшифровка достоверной.
    Возвращает True (оставляем) или False (исключаем).
    """
    if not expansion or expansion.lower() == "null":
        return False

    dash_pattern = rf"{re.escape(abbr)}\s*[-—–]"
    if re.search(dash_pattern, expansion, re.IGNORECASE):
        return False

    if expansion.lower().startswith(f"{abbr.lower()} "):
        expansion = expansion[len(abbr):].strip()
    score = fuzz.partial_ratio(expansion.lower(), chunk_text.lower())

    if score >= similarity_threshold:
        return True

    return False


def aggregate_definitions(df):
    """
    Собирает все варианты расшифровок/определений из DataFrame.
    """

    resolved_dict = {}
    conflicts_dict = {}
    for key, value in df.items():
        if len(value) == 1:
            resolved_dict[key] = list(value)[0]
        elif len(value) > 1:
            conflicts_dict[key] = list(value)

    return resolved_dict, conflicts_dict