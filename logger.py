"""
공통 로거 설정
모든 파서/엔진에서 import해서 사용.

출력:
  - 콘솔 (stdout)
  - 파일 (logs/app_YYYYMMDD.log)

사용:
  from logger import get_logger
  logger = get_logger("sism_parser")
"""

import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler

# 로그 디렉토리
LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, f"app_{datetime.now():%Y%m%d}.log")

# 포맷
FMT      = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"

_configured = False


def _setup():
    global _configured
    if _configured:
        return

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # 이미 핸들러가 있으면 중복 추가 방지
    if root.handlers:
        root.handlers.clear()

    formatter = logging.Formatter(FMT, DATE_FMT)

    # 콘솔 핸들러
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    root.addHandler(console)

    # 파일 핸들러 (10MB, 최대 7개 보관)
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=10 * 1024 * 1024,
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)   # 파일엔 DEBUG까지 기록
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    _configured = True


def get_logger(name: str) -> logging.Logger:
    """설정된 로거 반환"""
    _setup()
    return logging.getLogger(name)