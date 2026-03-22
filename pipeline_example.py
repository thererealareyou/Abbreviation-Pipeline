import requests
import time
import json


BASE_URL = "http://127.0.0.1:8000"
DOC_ID = "integration_test_doc.txt"
POLL_INTERVAL = 3
POLL_TIMEOUT = 600


TEST_TEXTS = ["Документ 1 1. Назначение Данная Методика оценки технологического долга ИТ (далее - Методика) регламентирует понятие «Технологический долг ИТ», а также определяет единый подход к оценке, расчёту стоимости и приоритизации инициатив, связанных с технологическим долгом.",
"Технологический долг по информационной безопасности (ИБ), а также по требованиям Регулятора в части импортозамещения (ИМЗ) и зоны ответственности критической информационной инфраструктуры (ЗО КИИ) не входят в периметр данной Методики кроме случаев, когда технологическая задолженность возникает по требованию ИБ и ИМЗ к компонентам ИТ- ландшафта. 2.",
"2. Нормативные ссылки 2. 3. Термины, определения и сокращения Для целей Методики в ней используются термины и сокращения, определенные в Глоссарии терминов и определений , а также следующие: Бизнес-владелец ТД ИТ – ответственное лицо со стороны бизнеса, которое подтверждает отклонение и согласовывает бюджет на его устранение.",
"Бизнес-сегмент – подразделение внутри организации, состоящее из сотрудников, которые работают с определенной категорией клиентов. К основным бизнес-сегментам относятся: В2С, В2В, B2O, БТИ.",
"Владелец ТД ИТ - лицо или подразделение, которое выделяет бюджет и необходимые ресурсы для устранения отклонения, согласовывает планы и сроки устранения, а также контролирует выполнение сроков устранения отклонения. Владелец процесса (архитектура) - лицо/подразделение, ответственное за сбор и актуализацию информации о технологической задолженности.",
"Инициатор устранения ТД ИТ - лицо или подразделение, которое инициирует процесс устранения Отклонения и формирует план мероприятий по его устранению.",
"Информационная система - совокупность взаимосвязанных аппаратных и программных средств, предназначенных для автоматизации сбора, хранения, обработки, передачи и поиска информации в рамках определённой предметной области или деятельности.",
"ИС реализует информационную модель этой области, обеспечивая пользователю доступ к необходимым данным и их обработку. ИТ-инфраструктура - совокупность оборудования, программного обеспечения, сетевых ресурсов и сервисов, обеспечивающих работу, хранение и передачу данных в организации.",
"ИТ-ландшафт - совокупность всех информационных систем, программных продуктов, аппаратных компонентов, сетевой инфраструктуры, хранилищ данных и интеграционных механизмов, используемых в организации, а также их взаимосвязей и архитектурных особенностей, обеспечивающих функционирование и развитие ИТ-среды в соответствии с бизнес-требованиями.",
"ИТ-лидер - должностное лицо, наделённое полномочиями принимать решения, направлять и контролировать процессы разработки, внедрения и эксплуатации информационных технологий в организации.",
"ИТ-среда - комплекс технических, программных, информационных и организационных ресурсов, а также инфраструктура, предназначенная для разработки, тестирования, эксплуатации, сопровождения и поддержки информационных систем и приложений.",
]

def print_step(step_name: str) -> None:
    print(f"\n{'=' * 50}\n▶ {step_name}\n{'=' * 50}")

def check_health() -> bool:
    print_step("Проверка состояния серверов")
    health = requests.get(f"{BASE_URL}/health").json()
    print(f"Статус:  {health['status']}")
    print(f"БД:      {health.get('db', '?')}")
    print(f"LLM:     {health.get('llm', '?')}")

    if health["status"] != "ok":
        print("ВНИМАНИЕ: один из сервисов недоступен.")
        return False
    if health.get("llm") == "unreachable":
        print("ОШИБКА: LLM сервер недоступен.")
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
    Опрашивает /status до завершения всех 6 стадий, статуса 'error' или таймаута.
    Отображает детальный прогресс по каждому этапу.
    Возвращает True при успехе.
    """
    print_step("Ожидание завершения пайплайна")
    deadline = time.time() + POLL_TIMEOUT

    while time.time() < deadline:
        status_res = requests.get(f"{BASE_URL}/status/{DOC_ID}").json()
        doc_status = status_res.get("status", "unknown")
        stages = status_res.get("stages", {})
        finding = status_res.get("finding", {})
        defining = status_res.get("defining", {})
        conflicts = status_res.get("conflicts", {})

        ready = sum(1 for v in stages.values() if v is True)
        total = len(stages)

        print(
            f"[{doc_status}] Стадий: {ready}/{total}\n"
            f"  Поиск:       abbr {finding.get('abbr_chunks_processed', '?')}/{finding.get('total_chunks', '?')} чанков, "
            f"найдено abbr={finding.get('found_abbrs', '?')} term={finding.get('found_terms', '?')}\n"
            f"  Определения: abbr {defining.get('abbrs_processed', '?')}/{defining.get('abbrs_total', '?')}, "
            f"term {defining.get('terms_processed', '?')}/{defining.get('terms_total', '?')}\n"
            f"  Конфликты:   abbr {conflicts.get('abbr_processed', '?')}/{conflicts.get('abbr_unique_words', '?')} "
            f"(конфликтов: {conflicts.get('abbr_actual_conflicts', '?')}), "
            f"term {conflicts.get('term_processed', '?')}/{conflicts.get('term_unique_words', '?')} "
            f"(конфликтов: {conflicts.get('term_actual_conflicts', '?')})"
        )

        if doc_status == "error":
            print("\nОШИБКА: Документ перешёл в статус 'error'.")
            return False

        if doc_status == "completed":
            print("\nПайплайн завершён успешно.")
            return True

        time.sleep(POLL_INTERVAL)

    print(f"\nТАЙМАУТ: пайплайн не завершился за {POLL_TIMEOUT} секунд.")
    return False


def print_stats() -> None:
    print_step("Итоговая статистика базы данных")
    stats = requests.get(f"{BASE_URL}/stats").json()
    print(json.dumps(stats, indent=2, ensure_ascii=False))


def print_result(target: str, limit: int = 10) -> None:
    print_step(f"Результат: {target} (первые {limit})")
    res = requests.get(f"{BASE_URL}/result/{DOC_ID}", params={"target": target, "limit": limit})
    if res.status_code != 200:
        print(f"ОШИБКА ({res.status_code}): {res.text}")
        return
    body = res.json()
    print(f"Всего записей в ответе: {body.get('count', '?')}")
    print(json.dumps(body.get("data", {}), indent=2, ensure_ascii=False))


def test_list_documents() -> None:
    print_step("Список документов")
    res = requests.get(f"{BASE_URL}/documents", params={"limit": 10}).json()
    print(f"Всего документов в БД: {res.get('total', '?')}")
    for d in res.get("documents", []):
        print(f"  [{d['status']}] {d['filename']} (id={d['id']}, создан: {d['created_at']})")


def test_delete_document() -> None:
    print_step(f"Удаление документа: {DOC_ID}")
    res = requests.delete(f"{BASE_URL}/document/{DOC_ID}")
    if res.status_code == 200:
        print(f"Успешно: {res.json().get('message')}")
    elif res.status_code == 409:
        print(f"Нельзя удалить: {res.json().get('detail')}")
    else:
        print(f"ОШИБКА ({res.status_code}): {res.text}")


def test_pipeline() -> None:
    try:
        if not check_health():
            return

        clear_database()
        submit_document()

        success = wait_for_pipeline()
        if not success:
            print("\nПайплайн не завершился успешно. Прерываем тест.")
            return

        print_stats()
        print_result("abbr", limit=10)
        print_result("term", limit=10)
        print_result("transliteration", limit=20)
        test_list_documents()

    except requests.exceptions.ConnectionError:
        print("Ошибка: FastAPI сервер не запущен. Запустите: uvicorn src.api:app --reload")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP ошибка: {e}")


if __name__ == "__main__":
    test_pipeline()
