import fitz
import re
from pathlib import Path


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

    # Разбиваем текст по точкам и точкам с запятой, сохраняя разделители
    parts = re.split(r'([.;])', text)
    sentences = []

    # Объединяем текст с его разделителем
    for i in range(0, len(parts) - 1, 2):
        sentence = (parts[i] + parts[i + 1]).strip()
        if sentence:
            sentences.append(sentence)

    # Добавляем последний кусок, если он не заканчивался на разделитель
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
        # Если само по себе предложение длиннее max_chars,
        # придется его обрезать или пропустить. В данном случае пропускаем
        # (или можно реализовать логику жесткой обрезки, если нужно).
        if len(sent) > max_chars:
            continue

        # Если добавление текущего предложения не превышает лимит
        if len(current_window) + len(sent) + (1 if current_window else 0) <= max_chars:
            if current_window:
                current_window += " " + sent
            else:
                current_window = sent
        else:
            # Если превышает, сохраняем текущее окно (если оно достаточно длинное)
            if len(current_window) >= min_chars:
                windows.append(current_window)
            # Начинаем новое окно с текущего предложения
            current_window = sent

    # Проверяем последний собранный кусок
    if current_window and len(current_window) >= min_chars:
        # Убедимся, что он заканчивается на нужный знак
        if current_window.endswith('.') or current_window.endswith(';'):
            windows.append(current_window)

    return windows


def extract_windows_from_pdf(
        pdf_path: str | Path,
        min_chars: int = 10,
        max_chars: int = 350,
) -> list[str]:
    """Извлекает текст из PDF и возвращает список окон по предложениям.

    Args:
        pdf_path (str | Path): Путь к целевому PDF-файлу для чтения.
        min_chars (int, optional): Минимально допустимая длина текстового окна в символах. По умолчанию 10.
        max_chars (int, optional): Максимально допустимая длина текстового окна в символах. По умолчанию 350.

    Returns:
        list[str]: Список текстовых окон, извлеченных и сформированных из содержимого PDF-документа.
    """

    doc = fitz.open(pdf_path)
    full_text = ""

    for page_num in range(len(doc)):
        page = doc.load_page(page_num)
        full_text += page.get_text("text") + " "

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
        print(f"[WARNING] PDF-файлы не найдены в папке: {folder}")
        return []

    all_windows: list[str] = []

    for pdf_path in pdf_files:
        try:
            windows = extract_windows_from_pdf(pdf_path, min_chars=min_chars, max_chars=max_chars)
            all_windows.extend(windows)
            print(f"[OK] {pdf_path.name} → {len(windows)} окно(н)")
        except Exception as e:
            print(f"[ERROR] {pdf_path.name}: {e}")

    return all_windows
