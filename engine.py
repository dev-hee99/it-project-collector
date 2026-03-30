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

# Lazy imports for parsers to avoid dependency issues when running specific sources
def get_crawl_sism():
    from sism_parser import crawl_sism
    return crawl_sism

def get_crawl_okky():
    from okky_parser import crawl_okky
    return crawl_okky

def get_crawl_freemoa():
    from freemoa_parser import crawl_freemoa
    return crawl_freemoa

def get_crawl_kmong():
    from kmong_parser import crawl_kmong
    return crawl_kmong

# 이랜서는 parser/ 디렉토리에 위치하지만 루트 logger 등을 임포트하므로 
# engine.py와 같은 레벨에서 임포트 가능하게 설정됨
def get_parse_elancer():
    from elancer_parser import parse
    return parse

from pipeline import Pipeline

from logger import get_logger
logger = get_logger("engine")


# ──────────────────────────────────────────────
# 소스 설정
# ──────────────────────────────────────────────

SOURCES = {
    "sism": {
        "crawler":   lambda: get_crawl_sism()(max_pages=50),
        "label":     "SISM",
        "max_pages": 50,
    },
    "okky": {
        "crawler":   lambda: get_crawl_okky()(max_pages=50),
        "label":     "OKKY Jobs",
        "max_pages": 50,
    },
    "freemoa": {
        "crawler":   lambda: get_crawl_freemoa()(max_pages=50),
        "label":     "프리모아",
        "max_pages": 50,
    },
    "kmong": {
        "crawler":   lambda: get_crawl_kmong()(max_pages=50),
        "label":     "크몽",
        "max_pages": 50,
    },
    "elancer": {
        "crawler":   lambda: get_parse_elancer()(),
        "label":     "이랜서",
        "max_pages": 0,   # API 기반, 페이지 개념 없음
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

def clear_stale_locks(sources: list[str]) -> None:
    """
    이전 비정상 종료로 남은 락 제거.
    실제 프로세스가 살아있는지 확인 후 죽은 락만 제거.
    """
    try:
        from cache import CollectLock, _make_client, KEY_LOCK
        client = _make_client()
        if not client:
            return
        for src in sources:
            key = KEY_LOCK.format(source=src)
            if client.exists(key):
                logger.warning(f"  [{src}] 이전 락 감지 — 강제 해제")
                client.delete(key)
    except Exception as e:
        logger.debug(f"락 정리 중 오류 (무시): {e}")


def run_all(sources: list[str] | None = None) -> dict:
    """모든 소스 순차 수집"""
    pipeline = Pipeline()
    targets  = sources or list(SOURCES.keys())
    results  = {}

    # 이전 비정상 종료로 남은 락 정리
    clear_stale_locks(targets)

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