import re
import pandas as pd
import pdfplumber
from pathlib import Path
from src.utils.logger import PipelineLogger

logger = PipelineLogger.get_logger(__name__)

def clean_text(text: str) -> str:
    """Базовая очистка текста от мусорных переносов и лишних пробелов.

    Args:
        text (str): Исходная текстовая строка для очистки.

    Returns:
        str: Очищенная строка без лишних пробельных символов и переносов.
    """

    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def split_into_sentences(text: str) -> list[str]:
    """Делитель текста, который режет строго по '.' или ';'. Сохраняет сам разделитель в конце предложения.

    Args:
        text (str): Исходный очищенный текст для разбиения на предложения.

    Returns:
        list[str]: Список предложений, где каждое сохраняет свой оригинальный знак пунктуации на конце.
    """

    text = clean_text(text)

    parts = re.split(r'([.;])', text)
    sentences = []

    for i in range(0, len(parts) - 1, 2):
        sentence = (parts[i] + parts[i + 1]).strip()
        if sentence:
            sentences.append(sentence)

    if len(parts) % 2 != 0 and parts[-1].strip():
        sentences.append(parts[-1].strip())

    return sentences


def sentence_window(
        sentences: list[str],
        min_chars: int = 10,
        max_chars: int = 350,
) -> list[str]:
    """Собирает окна из нескольких предложений. Окно должно быть от min_chars до max_chars символов и заканчиваться на '.' или ';'.

    Args:
        sentences (list[str]): Список предварительно разбитых предложений.
        min_chars (int, optional): Минимально допустимая длина текстового окна в символах. По умолчанию 10.
        max_chars (int, optional): Максимально допустимая длина текстового окна в символах. По умолчанию 350.

    Returns:
        list[str]: Список сформированных текстовых окон (чанков), удовлетворяющих заданным ограничениям по длине.
    """

    windows = []
    current_window = ""

    for sent in sentences:
        if len(sent) > max_chars:
            continue

        if len(current_window) + len(sent) + (1 if current_window else 0) <= max_chars:
            if current_window:
                current_window += " " + sent
            else:
                current_window = sent
        else:
            if len(current_window) >= min_chars:
                windows.append(current_window)
            current_window = sent

    if current_window and len(current_window) >= min_chars:
        if current_window.endswith('. ') or current_window.endswith('; '):
            windows.append(current_window)

    return windows


def extract_windows_from_pdf(
        pdf_path: str | Path,
        min_chars: int = 10,
        max_chars: int = 350,
) -> list[str]:
    """Извлекает текст из PDF и возвращает список окон по предложениям."""

    full_text = ""

    with pdfplumber.open(pdf_path) as doc:
        for page in doc.pages:
            page_text = page.extract_text() or ""
            full_text += page_text + " "

    sentences = split_into_sentences(full_text)
    return sentence_window(sentences, min_chars=min_chars, max_chars=max_chars)


def extract_sentences_from_folder(
        folder_path: str | Path,
        min_chars: int = 10,
        max_chars: int = 350,
) -> list[str]:
    """Читает все PDF-файлы в папке и возвращает объединённый список окон. Каждое окно — 1–N предложений.

    Args:
        folder_path (str | Path): Путь к директории, в которой находятся PDF-файлы.
        min_chars (int, optional): Минимально допустимая длина текстового окна в символах. По умолчанию 10.
        max_chars (int, optional): Максимально допустимая длина текстового окна в символах. По умолчанию 350.

    Returns:
        list[str]: Общий объединенный список текстовых окон, собранный из всех PDF-файлов в указанной папке.
    """

    folder = Path(folder_path)
    pdf_files = sorted(folder.glob("*.pdf"))

    if not pdf_files:
        logger.warning(f"PDF-файлы не найдены в папке: {folder}")
        return []

    all_windows: list[str] = []

    for pdf_path in pdf_files:
        try:
            windows = extract_windows_from_pdf(pdf_path, min_chars=min_chars, max_chars=max_chars)
            all_windows.extend(windows)
            logger.info(f"{pdf_path.name} → {len(windows)} окно(н)")
        except Exception as e:
            logger.error(f"{pdf_path.name}: {e}")

    return all_windows

def extract_fixed_from_excel(file_path: str, column_name: str = 'chunk', n_sentences: int = 5) -> list:
    """
    Извлекает первые N предложений из каждой ячейки указанной колонки Excel-файла.

    Параметры:
        file_path (str): путь к Excel-файлу.
        column_name (str): имя колонки с текстом (по умолчанию 'chunk').
        n_sentences (int): количество предложений, которое нужно взять из каждой ячейки.

    Возвращает:
        list: список строк, где каждый элемент — первые N предложений из соответствующей ячейки.
              Если в ячейке меньше предложений, возвращается весь текст.
    """
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
    except FileNotFoundError:
        raise FileNotFoundError(f"Файл {file_path} не найден.")
    except Exception as e:
        raise Exception(f"Ошибка при чтении файла: {e}")

    if column_name not in df.columns:
        raise KeyError(f"В файле отсутствует колонка '{column_name}'.")

    result = []
    for cell in df[column_name][:n_sentences]:
        if pd.isna(cell) or not isinstance(cell, str):
            result.append("")
            continue

        sentences = re.split(r'(?<=[.!?])\s+', cell.strip())
        first_n = sentences[:n_sentences]
        result.append(' '.join(first_n))

    return result