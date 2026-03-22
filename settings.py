"""
수집기 설정 파일
환경변수 또는 직접 값 입력
"""

import os

# ── DB ──────────────────────────────────────
DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg://collector:collector1234@localhost:5432/freelancer_jobs"
)

# DB_URL = "postgresql+psycopg://collector:collector1234@localhost:5432/freelancer_jobs"

# ── Redis ───────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://:redis1234@localhost:6379/0")

# ── 크몽 계정 ───────────────────────────────
KMONG_EMAIL    = os.getenv("KMONG_EMAIL",    "")
KMONG_PASSWORD = os.getenv("KMONG_PASSWORD", "")

# ── 수집 설정 ───────────────────────────────
MAX_PAGES     = int(os.getenv("MAX_PAGES", "50"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1.2"))

# ── 스케줄 (cron) ───────────────────────────
SCHEDULE_HOUR   = os.getenv("SCHEDULE_HOUR",   "9,15,21")
SCHEDULE_MINUTE = os.getenv("SCHEDULE_MINUTE", "0")

# ── 알림 ────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SLACK_WEBHOOK    = os.getenv("SLACK_WEBHOOK",    "")
