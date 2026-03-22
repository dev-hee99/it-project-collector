"""
통합 수집 엔진
Playwright 기반 4개 파서를 하나로 묶어 실행.

구조:
  - Playwright Spider: SISM, OKKY, 프리모아, 크몽
  - Item Pipeline: 정규화 → 중복 제거 → 필터 → DB 저장
  - APScheduler: 주기 실행 (기본 6시간마다)

실행:
  python engine.py                     # 즉시 1회 실행
  python engine.py --schedule          # 스케줄러 모드
  python engine.py --source sism       # 특정 소스만
"""

import argparse
import logging
import signal
import sys
import time
from dataclasses import asdict
from datetime import datetime

# 파서 임포트 (parser/ 디렉토리)
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "parser"))

from sism_parser    import crawl_sism
from okky_parser    import crawl_okky
from freemoa_parser import crawl_freemoa
from kmong_parser   import crawl_kmong

from pipeline import Pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("engine")


# ──────────────────────────────────────────────
# 소스 설정
# ──────────────────────────────────────────────

SOURCES = {
    "sism": {
        "crawler":   lambda: crawl_sism(max_pages=50),
        "label":     "SISM",
        "max_pages": 50,
    },
    "okky": {
        "crawler":   lambda: crawl_okky(max_pages=50),
        "label":     "OKKY Jobs",
        "max_pages": 50,
    },
    "freemoa": {
        "crawler":   lambda: crawl_freemoa(max_pages=50),
        "label":     "프리모아",
        "max_pages": 50,
    },
    "kmong": {
        "crawler":   lambda: crawl_kmong(max_pages=50),
        "label":     "크몽",
        "max_pages": 50,
    },
}


# ──────────────────────────────────────────────
# 단일 소스 수집
# ──────────────────────────────────────────────

def run_source(source_key: str, pipeline: Pipeline) -> int:
    """특정 소스 수집 실행 → 저장된 건수 반환"""
    if source_key not in SOURCES:
        logger.error(f"알 수 없는 소스: {source_key}")
        return 0

    from cache import CollectLock, meta

    cfg   = SOURCES[source_key]
    label = cfg["label"]

    with CollectLock(source_key) as locked:
        if not locked:
            return 0

        meta.set_running(source_key)
        logger.info(f"[{label}] 수집 시작")

        count = 0
        try:
            for job in cfg["crawler"]():
                pipeline.process(job)
                count += 1
        except Exception as e:
            logger.error(f"[{label}] 수집 중 오류: {e}", exc_info=True)

        meta.set_done(source_key, count)
        logger.info(f"[{label}] 수집 완료 — {count}건")
        return count


# ──────────────────────────────────────────────
# 전체 수집 (순차 실행)
# ──────────────────────────────────────────────

def run_all(sources: list[str] | None = None) -> dict:
    """모든 소스 순차 수집"""
    pipeline = Pipeline()
    targets  = sources or list(SOURCES.keys())
    results  = {}

    start = datetime.now()
    logger.info(f"=== 전체 수집 시작: {start:%Y-%m-%d %H:%M} ===")

    for key in targets:
        results[key] = run_source(key, pipeline)
        time.sleep(2)   # 소스 간 딜레이

    pipeline.close()

    elapsed = (datetime.now() - start).seconds
    total   = sum(results.values())
    logger.info(f"=== 수집 완료: 총 {total}건, {elapsed}초 소요 ===")
    logger.info(f"소스별: {results}")
    return results


# ──────────────────────────────────────────────
# 스케줄러
# ──────────────────────────────────────────────

def start_scheduler(sources: list[str] | None = None):
    """
    APScheduler로 주기 실행.
    기본: 매일 오전 9시, 오후 3시, 오후 9시 실행
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        logger.error("apscheduler 미설치 — pip install apscheduler")
        sys.exit(1)

    scheduler = BlockingScheduler(timezone="Asia/Seoul")

    scheduler.add_job(
        lambda: run_all(sources),
        CronTrigger(hour="9,15,21", minute="0"),
        id="collect_all",
        name="전체 수집",
        misfire_grace_time=300,
        coalesce=True,
    )

    # Graceful shutdown
    def shutdown(signum, frame):
        logger.info("스케줄러 종료 중...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("스케줄러 시작 (매일 09:00, 15:00, 21:00 실행)")
    logger.info("종료: Ctrl+C")

    # 시작 즉시 1회 실행
    run_all(sources)

    scheduler.start()


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IT 프리랜서 공고 수집기")
    parser.add_argument(
        "--schedule", action="store_true",
        help="스케줄러 모드 (주기 실행)"
    )
    parser.add_argument(
        "--source", nargs="+",
        choices=list(SOURCES.keys()),
        help="수집할 소스 (미입력 시 전체)"
    )
    args = parser.parse_args()

    if args.schedule:
        start_scheduler(args.source)
    else:
        run_all(args.source)
