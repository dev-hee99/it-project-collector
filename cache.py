"""
Redis 기반 URL 해시 캐시
pipeline.py의 DuplicateFilter를 대체.

역할:
  1. 중복 체크 — 수집한 url_hash를 Redis SET에 저장, 재실행 간 중복 방지
  2. 수집 상태 캐시 — 마지막 수집 시간, 페이지 진행 상황 저장
  3. 인메모리 fallback — Redis 미연결 시 자동으로 set() 사용

Redis 키 구조:
  jobs:hashes          → SET  { url_hash, ... }           (전체 수집 해시)
  jobs:hashes:{source} → SET  { url_hash, ... }           (소스별 해시)
  jobs:meta:{source}   → HASH { last_run, last_page, count }
  jobs:lock:{source}   → STRING (수집 중복 실행 방지 락)
"""

import logging
import time
from datetime import datetime

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from settings import REDIS_URL

logger = logging.getLogger("cache")

# Redis 키 상수
KEY_ALL_HASHES    = "jobs:hashes"
KEY_SOURCE_HASHES = "jobs:hashes:{source}"
KEY_META          = "jobs:meta:{source}"
KEY_LOCK          = "jobs:lock:{source}"

HASH_TTL  = 60 * 60 * 24 * 30   # 30일 (초)
LOCK_TTL  = 60 * 60 * 2          # 2시간 (수집 최대 소요 시간)


# ──────────────────────────────────────────────
# Redis 클라이언트
# ──────────────────────────────────────────────

def _make_client():
    if not REDIS_AVAILABLE:
        return None
    try:
        client = redis.from_url(
            REDIS_URL,
            decode_responses=True,
            socket_timeout=3,
            socket_connect_timeout=3,
            retry_on_timeout=True,
        )
        client.ping()
        logger.info("Redis 연결 성공")
        return client
    except Exception as e:
        logger.warning(f"Redis 연결 실패 — 인메모리 fallback 사용: {e}")
        return None


# ──────────────────────────────────────────────
# 중복 필터
# ──────────────────────────────────────────────

class RedisFilter:
    """
    Redis SET 기반 중복 필터.
    Redis 미연결 시 인메모리 set으로 자동 fallback.

    pipeline.py 사용 예:
        self.dup_filter = RedisFilter()
        if self.dup_filter.is_duplicate(url_hash):
            continue
    """

    def __init__(self):
        self._client  = _make_client()
        self._mem:set = set()          # fallback용 인메모리 set
        self._use_redis = self._client is not None

    def is_duplicate(self, url_hash: str, source: str = "") -> bool:
        """
        url_hash가 이미 수집된 항목이면 True 반환.
        신규 항목이면 캐시에 추가 후 False 반환.
        """
        if self._use_redis:
            return self._check_redis(url_hash, source)
        return self._check_mem(url_hash)

    def _check_redis(self, url_hash: str, source: str) -> bool:
        try:
            pipe = self._client.pipeline()
            # 전체 해시 SET에 추가 시도 (SADD: 이미 있으면 0, 신규면 1 반환)
            pipe.sadd(KEY_ALL_HASHES, url_hash)
            if source:
                pipe.sadd(KEY_SOURCE_HASHES.format(source=source), url_hash)
            results = pipe.execute()

            added = results[0]  # 1 = 신규, 0 = 중복
            return added == 0   # 중복이면 True

        except Exception as e:
            logger.warning(f"Redis 체크 실패 → 인메모리 fallback: {e}")
            self._use_redis = False
            return self._check_mem(url_hash)

    def _check_mem(self, url_hash: str) -> bool:
        if url_hash in self._mem:
            return True
        self._mem.add(url_hash)
        return False

    def bulk_load(self, source: str = "") -> int:
        """
        Redis에서 기존 해시를 인메모리로 프리로드.
        대량 수집 전 호출하면 네트워크 왕복을 줄일 수 있음.
        """
        if not self._use_redis:
            return 0
        try:
            key    = KEY_SOURCE_HASHES.format(source=source) if source else KEY_ALL_HASHES
            hashes = self._client.smembers(key)
            self._mem.update(hashes)
            logger.info(f"  Redis 프리로드: {len(hashes)}개 해시")
            return len(hashes)
        except Exception as e:
            logger.warning(f"Redis 프리로드 실패: {e}")
            return 0

    def clear_source(self, source: str):
        """특정 소스 해시 초기화 (재수집 시 사용)"""
        if not self._use_redis:
            self._mem.clear()
            return
        try:
            self._client.delete(KEY_SOURCE_HASHES.format(source=source))
            logger.info(f"  [{source}] 캐시 초기화 완료")
        except Exception as e:
            logger.warning(f"캐시 초기화 실패: {e}")

    def count(self, source: str = "") -> int:
        """캐시된 해시 수 반환"""
        if not self._use_redis:
            return len(self._mem)
        try:
            key = KEY_SOURCE_HASHES.format(source=source) if source else KEY_ALL_HASHES
            return self._client.scard(key)
        except Exception:
            return len(self._mem)


# ──────────────────────────────────────────────
# 수집 메타 캐시
# ──────────────────────────────────────────────

class CollectMeta:
    """
    소스별 수집 상태를 Redis HASH에 저장.
    engine.py에서 마지막 수집 시간 / 페이지 진행 상황 추적용.
    """

    def __init__(self):
        self._client = _make_client()

    def set_running(self, source: str, page: int = 1):
        """수집 시작 기록"""
        if not self._client:
            return
        try:
            key = KEY_META.format(source=source)
            self._client.hset(key, mapping={
                "status":     "running",
                "last_page":  page,
                "started_at": datetime.now().isoformat(),
            })
        except Exception:
            pass

    def set_done(self, source: str, count: int):
        """수집 완료 기록"""
        if not self._client:
            return
        try:
            key = KEY_META.format(source=source)
            self._client.hset(key, mapping={
                "status":    "done",
                "last_run":  datetime.now().isoformat(),
                "count":     count,
            })
        except Exception:
            pass

    def set_last_page(self, source: str, page: int):
        """현재 수집 중인 페이지 번호 업데이트"""
        if not self._client:
            return
        try:
            self._client.hset(
                KEY_META.format(source=source),
                "last_page", page,
            )
        except Exception:
            pass

    def get(self, source: str) -> dict:
        """소스 수집 메타 조회"""
        if not self._client:
            return {}
        try:
            return self._client.hgetall(KEY_META.format(source=source))
        except Exception:
            return {}

    def get_all(self) -> dict:
        """전체 소스 메타 조회"""
        sources = ["sism", "okky", "freemoa", "kmong"]
        return {s: self.get(s) for s in sources}


# ──────────────────────────────────────────────
# 수집 중복 실행 방지 락
# ──────────────────────────────────────────────

class CollectLock:
    """
    동일 소스의 수집이 중복 실행되지 않도록 Redis 락 사용.
    with 문으로 사용:
        with CollectLock("sism") as locked:
            if not locked:
                return  # 이미 실행 중
            ...
    """

    def __init__(self, source: str):
        self._source = source
        self._client = _make_client()
        self._key    = KEY_LOCK.format(source=source)
        self._locked = False

    def __enter__(self) -> bool:
        if not self._client:
            self._locked = True
            return True
        try:
            # SET NX EX: 키 없을 때만 설정 (원자적 락)
            result = self._client.set(
                self._key, "1",
                nx=True,           # 없을 때만
                ex=LOCK_TTL,       # 2시간 TTL
            )
            self._locked = result is True
            if not self._locked:
                logger.warning(f"  [{self._source}] 이미 수집 중 — 건너뜀")
            return self._locked
        except Exception:
            self._locked = True
            return True

    def __exit__(self, *args):
        if self._locked and self._client:
            try:
                self._client.delete(self._key)
            except Exception:
                pass


# ──────────────────────────────────────────────
# pipeline.py 교체용 단일 인터페이스
# ──────────────────────────────────────────────

# 싱글턴 — pipeline.py에서 import해서 바로 사용
filter   = RedisFilter()
meta     = CollectMeta()
