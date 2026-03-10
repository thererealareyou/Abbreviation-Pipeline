import re
from rapidfuzz import fuzz

def verify_expansion(abbr: str, expansion: str, chunk_text: str, similarity_threshold: float = 80.0) -> bool:
    """Проверяет, является ли предложенная моделью расшифровка достоверной.

    Args:
        abbr (str): Аббревиатура для проверки.
        expansion (str): Предложенная моделью расшифровка (определение) аббревиатуры.
        chunk_text (str): Исходный текстовый фрагмент, на основе которого была сгенерирована расшифровка.
        similarity_threshold (float, optional): Порог схожести для нечеткого поиска (fuzzy matching)
            расшифровки в исходном тексте. По умолчанию 80.0.

    Returns:
        bool: True, если расшифровка признана достоверной (прошла проверки и найдена в тексте),
            или False, если она отклонена (исключаем).
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


def aggregate_definitions(df) -> tuple:
    """Собирает все варианты расшифровок/определений из DataFrame.

    Args:
        df (dict | pd.Series): Структура данных (словарь или объект pandas),
            содержащая сущности (аббревиатуры или термины) в качестве ключей и
            коллекции их вариантов определений (множества или списки) в качестве значений.

    Returns:
        tuple[dict, dict]: Кортеж из двух словарей `(resolved_dict, conflicts_dict)`:

            - `resolved_dict` (dict): Словарь сущностей, для которых найден ровно один (однозначный) вариант определения.
            - `conflicts_dict` (dict): Словарь сущностей, для которых найдено несколько (конфликтующих) вариантов определений.
    """

    resolved_dict = {}
    conflicts_dict = {}
    for key, value in df.items():
        if len(value) == 1:
            resolved_dict[key] = list(value)[0]
        elif len(value) > 1:
            conflicts_dict[key] = list(value)

    return resolved_dict, conflicts_dict