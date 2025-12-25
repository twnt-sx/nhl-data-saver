import requests
from datetime import datetime, timezone

from airflow.hooks.base import BaseHook


def get_current_utc_ts():
    return datetime.now(timezone.utc)


def send_get_request(url: str) -> dict:
    """
    Отправляет GET-запрос на переданный url для получения данных. В случае отсутствия
    в ответе HTTP-ошибки возвращает тело ответа
    """

    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def telegram_conn_exists(conn_id: str) -> bool:
    """
    Проверяет, если в Airflow connection с указанным conn_id
    """

    try:
        BaseHook.get_connection(conn_id)
        return True
    except Exception:
        return False