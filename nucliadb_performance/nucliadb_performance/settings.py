from typing import Optional

from pydantic import BaseSettings


class Settings(BaseSettings):
    main_api: str = "http://localhost:8080/api"
    search_api: Optional[str] = None
    reader_api: Optional[str] = None
    predict_api: str = (
        "http://predict.learning.svc.cluster.local:8080/api/internal/predict"
    )
    benchmark_output: Optional[str] = None
    saved_requests_file: Optional[str] = None
    exports_folder: str = "exports"


def get_benchmark_output_file():
    return settings.benchmark_output or "benchmark.json"


def get_search_api_url():
    return settings.search_api or settings.main_api


def get_reader_api_url():
    return settings.reader_api or settings.main_api


def get_predict_api_url():
    return settings.predict_api


settings = Settings()
