import requests
import time
import json

from src.extraction.pdf_parser import extract_fixed_from_excel

BASE_URL = "http://127.0.0.1:8000"
DOC_ID = "integration_test_doc.txt"
POLL_INTERVAL = 3
POLL_TIMEOUT = 600

TEST_TEXTS = extract_fixed_from_excel("data/02_interim/sent_abbr_term_old.xlsx", "chunk", 50)


def print_step(step_name: str) -> None:
    print(f"\n{'=' * 50}\n▶ {step_name}\n{'=' * 50}")


def check_health() -> bool:
    print_step("Проверка состояния серверов")
    health = requests.get(f"{BASE_URL}/health").json()
    print(f"Health: {health['status']}")
    if health["status"] != "ok":
        print("ВНИМАНИЕ: LLM сервер недоступен. Пайплайн может не сработать!")
        return False
    return True


def clear_database() -> None:
    print_step("Очистка базы данных")
    code_res = requests.get(f"{BASE_URL}/danger/generate-reset-code").json()
    code = code_res["code"]
    print(f"Код сброса: {code}")

    clear_res = requests.delete(f"{BASE_URL}/danger/clear-database", json={"code": code})
    clear_res.raise_for_status()
    body = clear_res.json()
    print(f"Очистка: {body.get('status')} — {body.get('message', '')}")


def submit_document() -> None:
    print_step("Отправка текста в пайплайн")
    payload = {"document_id": DOC_ID, "texts": TEST_TEXTS}
    response = requests.post(f"{BASE_URL}/extract", json=payload)

    if response.status_code not in (200, 201):
        print(f"ОШИБКА СЕРВЕРА ({response.status_code}): {response.text}")
        response.raise_for_status()

    print(f"Ответ: {response.json().get('message', response.json())}")


def wait_for_pipeline() -> bool:
    """
    Опрашивает /status до тех пор, пока все 6 стадий не завершены,
    документ не переходит в статус 'error', или не истекает таймаут.
    Возвращает True при успехе.
    """
    print_step("Ожидание завершения пайплайна (стадии 1–6)")
    deadline = time.time() + POLL_TIMEOUT

    while time.time() < deadline:
        status_res = requests.get(f"{BASE_URL}/status/{DOC_ID}").json()
        doc_status = status_res.get("status", "unknown")
        stages = status_res.get("stages", {})
        chunks_info = status_res.get("chunks", {})

        ready = sum(1 for v in stages.values() if v is True)
        total = len(stages)

        print(
            f"[{doc_status}] Стадий: {ready}/{total} | "
            f"Сущностей: {chunks_info.get('entities_found', '?')} | "
            f"С определениями: {chunks_info.get('with_definitions', '?')}"
        )

        if doc_status == "error":
            print("\nОШИБКА: Документ перешёл в статус 'error'.")
            return False

        if ready == total and total > 0:
            print("\nВсе стадии завершены.")
            return True

        time.sleep(POLL_INTERVAL)

    print(f"\nТАЙМАУТ: пайплайн не завершился за {POLL_TIMEOUT} секунд.")
    return False


def build_dictionary() -> None:
    print_step("Запуск разрешения конфликтов и сборки словаря")
    build_res = requests.post(f"{BASE_URL}/dictionary/build/{DOC_ID}")
    build_res.raise_for_status()
    print(f"Ответ: {build_res.json().get('message')}")

    # Даём Celery время завершить задачи разрешения конфликтов
    print("Ожидаем завершения conflict resolution", end="", flush=True)
    for _ in range(10):
        time.sleep(2)
        status = requests.get(f"{BASE_URL}/status/{DOC_ID}").json()
        stages = status.get("stages", {})
        if stages.get("term_conflicts") and stages.get("abbr_conflicts"):
            print(" ✓")
            return
        print(".", end="", flush=True)
    print("\nВНИМАНИЕ: conflict resolution не подтверждён за отведённое время.")


def print_stats() -> None:
    print_step("Итоговая статистика базы данных")
    stats = requests.get(f"{BASE_URL}/stats").json()
    print(json.dumps(stats, indent=2, ensure_ascii=False))


def test_pipeline() -> None:
    try:
        check_health()
        clear_database()
        submit_document()

        success = wait_for_pipeline()
        if not success:
            print("\nПайплайн не завершился успешно. Прерываем тест.")
            return

        build_dictionary()
        print_stats()

    except requests.exceptions.ConnectionError:
        print("Ошибка: FastAPI сервер не запущен. Запустите его командой: uvicorn src.api:app --reload")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP ошибка: {e}")


if __name__ == "__main__":
    test_pipeline()
