import re
import json
from rapidfuzz import fuzz
from transliterate import translit
from typing import List


def abbr_in_text(abbr: str, text: str) -> bool:
    """Проверяет, что аббревиатура реально встречается в тексте как отдельная единица.

    Работает и для составных вариантов с пробелами, например, 'ЗО КИИ'.

    Args:
        abbr (str): Искомая аббревиатура.
        text (str): Исходный текст, в котором производится поиск.

    Returns:
        bool: True, если аббревиатура найдена как отдельное слово (или фраза), иначе False.
    """

    pattern = r"(?<![0-9A-Za-zА-Яа-яЁё])" + re.escape(abbr) + r"(?![0-9A-Za-zА-Яа-яЁё])"
    return re.search(pattern, text) is not None


def is_pure_abbreviation(candidate: str, threshold: float = 0.6) -> bool:
    """Проверяет, является ли строка-кандидат чистой аббревиатурой.

    Логика: если доля заглавных букв (среди всех буквенных символов) больше или
    равна threshold, кандидат считается аббревиатурой.

    Примеры:
    - "ТД ИТ"         → 4/4  = 100% → аббревиатура
    - "YouTube"       → 1/7  = 14%  → термин
    - "Владелец ТД ИТ" → 3/12 = 25%  → термин
    - "BSS"           → 3/3  = 100% → аббревиатура
    - "КИПиА"         → 4/5  = 80%  → аббревиатура

    Args:
        candidate (str): Строка-кандидат для проверки.
        threshold (float, optional): Пороговое значение доли заглавных букв
            (от 0.0 до 1.0). По умолчанию 0.6.

    Returns:
        bool: True, если кандидат классифицируется как аббревиатура, иначе False.
    """

    letters = [c for c in candidate if c.isalpha()]
    if not letters:
        return False
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    return upper_ratio >= threshold


def term_in_text(term: str, text: str) -> bool:
    """Проверяет, что термин реально встречается в тексте как отдельная единица.

    Args:
        term (str): Искомый термин.
        text (str): Исходный текст, в котором производится поиск.

    Returns:
        bool: True, если термин найден как отдельное слово (или фраза), иначе False.
    """

    pattern = r"(?<![0-9A-Za-zА-Яа-яЁё])" + re.escape(term) + r"(?![0-9A-Za-zА-Яа-яЁё])"
    return re.search(pattern, text) is not None


def verify_expansion_term(abbr: str, expansion: str, chunk_text: str, similarity_threshold: float = 80.0) -> bool:
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


def clean_terms_list(terms: List[str], text: str, threshold: float = 0.6) -> List[str]:
    """Извлекает и фильтрует список терминов из ответа модели.

    Функция парсит JSON-массив из сырого ответа, валидирует каждый термин
    по регулярному выражению (начинается с заглавной буквы, до 60 символов),
    отсеивает чистые аббревиатуры и проверяет фактическое наличие термина в исходном тексте.

    Args:
        terms (List[str]): Массив терминов, полученных моделью.
        text (str): Исходный текст (чанк), в котором проверяется наличие извлеченных терминов.

    Returns:
        List[str]: Отсортированный список валидных терминов, прошедших все проверки.
    """

    # Термин: начинается с заглавной, содержит буквы и пробелы, до 60 символов
    TERM_RE = re.compile(r"[А-ЯЁA-Z][А-Яа-яЁёA-Za-z0-9/\- ]{1,59}")

    result = set()
    for item in terms:
        if not isinstance(item, str):
            continue
        term = item.strip()
        if not term:
            continue

        # Соответствует шаблону термина
        if not TERM_RE.fullmatch(term):
            continue

        # Не является чистой аббревиатурой
        if is_pure_abbreviation(term, threshold):
            continue

        # Реально встречается в тексте
        if not term_in_text(term, text):
            continue

        result.add(term)

    return sorted(result)


def verify_expansion_abbr(abbr: str, expansion: str, chunk_text: str, similarity_threshold: float = 80.0) -> bool:
    """Проверяет, является ли предложенная моделью расшифровка аббревиатуры достоверной.

    Функция отсеивает пустые ответы, исключает рекурсивные определения (где расшифровка
    просто дублирует аббревиатуру через тире) и проверяет наличие расшифровки в тексте
    с помощью нечеткого поиска.

    Args:
        abbr (str): Аббревиатура для проверки (например, 'ИИ').
        expansion (str): Предложенная моделью расшифровка (например, 'Искусственный интеллект').
        chunk_text (str): Исходный текст чанка.
        similarity_threshold (float): Порог схожести для rapidfuzz.

    Returns:
        bool: True, если расшифровка валидна и найдена в тексте.
    """

    if not expansion or expansion.lower() == "null":
        return False

    dash_pattern = rf"{re.escape(abbr)}\s*[-—–]"
    if re.search(dash_pattern, expansion, re.IGNORECASE):
        return False

    if expansion.lower().startswith(f"{abbr.lower()} "):
        expansion = expansion[len(abbr):].strip()

    if expansion.lower() == abbr.lower():
        return False

    score = fuzz.partial_ratio(expansion.lower(), chunk_text.lower())

    if not is_valid_definition(abbr, expansion):
        return False

    if score >= similarity_threshold:
        return True

    return False


def clean_abbr_list(abbrs: List[str], text: str) -> List[str]:
    """Извлекает и фильтрует список аббревиатур из ответа модели.

    Функция парсит JSON-массив из сырого ответа, валидирует каждую аббревиатуру
    по регулярному выражению и проверяет её наличие в исходном тексте.

    Args:
        abbrs (List[str]): Список аббревиатур, полученных моделью.
        text (str): Исходный текст (чанк), в котором проверяется фактическое наличие извлеченных аббревиатур.

    Returns:
        List[str]: Отсортированный список валидных аббревиатур, прошедших все проверки.
    """

    ABBR_RE = re.compile(r"[A-ZА-ЯЁ0-9][A-ZА-ЯЁ0-9/\- ]{1,19}")

    result = set()
    for item in abbrs:
        if not isinstance(item, str):
            continue
        abbr = item.strip()
        if not abbr:
            continue

        if not ABBR_RE.fullmatch(abbr):
            continue

        if not abbr_in_text(abbr, text):
            continue

        if len(item) > 5:
            continue

        result.add(abbr)

    return sorted(result)


def is_valid_definition(word: str, definition: str) -> bool:
    if not definition or not word:
        return False

    w_clean = re.sub(r'[^\w]', '', word.lower())
    d_clean = re.sub(r'[^\w]', '', definition.lower())

    if w_clean == d_clean:
        return False

    try:
        w_translit_ru = translit(w_clean, 'ru')
        if w_translit_ru == d_clean:
            return False

        d_translit_en = translit(d_clean, 'ru', reversed=True)
        if d_translit_en == w_clean:
            return False

    except Exception:
        pass

    if " " not in definition.strip().replace("  ", " "):
        if len(d_clean) < len(w_clean) + 3:
            return False

    return True