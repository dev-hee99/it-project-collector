"""
Item Pipeline
파싱 → 정규화 → 중복 제거 → 필터 → DB 저장
"""

import hashlib
from logger import get_logger
import logging
import re
from dataclasses import asdict
from datetime import date, datetime
from typing import Any

logger = get_logger("pipeline")


# ──────────────────────────────────────────────
# 공통 스키마 (모든 소스를 동일한 구조로 변환)
# ──────────────────────────────────────────────

COMMON_FIELDS = [
    "source",           # 출처 (sism/okky/freemoa/kmong)
    "source_id",        # 원본 ID (pno, request_id 등)
    "url",              # 상세 페이지 URL
    "url_hash",         # 중복 체크용 해시
    "title",            # 공고 제목
    "company",          # 회사/의뢰자
    "category",         # 카테고리
    "skills",           # 기술 스택 (list)
    "budget",           # 예산/단가
    "start_date",       # 공고 시작일
    "end_date",         # 공고 마감일
    "deadline",         # 마감 표시 (D-N 등)
    "project_duration", # 프로젝트 기간
    "location",         # 근무지
    "work_type",        # 근무 형태 (상주/원격 등)
    "status",           # 모집 상태
    "description",      # 공고 본문
    "collected_at",     # 수집 시간
]

# 소스별 필드 매핑 (source_field → common_field)
FIELD_MAP = {
    "sism": {
        "source_id":        "url_hash",
        "company":          "company",
        "project_duration": "project_duration",
    },
    "okky": {
        "source_id":  "job_id",
        "company":    "company",
        "work_type":  "employment_type",
    },
    "freemoa": {
        "source_id":  "pno",
        "company":    "",          # 프리모아는 회사명 없음
        "work_type":  "work_type",
    },
    "kmong": {
        "source_id":  "request_id",
        "company":    "",
        "work_type":  "project_type",
    },
}

# 기술 스택 정규화 테이블
SKILL_NORMALIZE = {
    "REACT":             "React",
    "REACTJS":           "React",
    "REACT.JS":          "React",
    "NODEJS":            "Node.js",
    "NODE.JS":           "Node.js",
    "NEXTJS":            "Next.js",
    "NEXT.JS":           "Next.js",
    "SPRINGBOOT":        "Spring Boot",
    "SPRING BOOT":       "Spring Boot",
    "SPRING FRAMEWORK":  "Spring",
    "POSTGRESQL":        "PostgreSQL",
    "POSTGRE SQL":       "PostgreSQL",
    "MSSQL":             "MSSQL",
    "MS-SQL":            "MSSQL",
    "REACTNATIVE":       "React Native",
    "REACT NATIVE":      "React Native",
    "GITHUB ACTIONS":    "GitHub Actions",
}


def normalize_skill(skill: str) -> str:
    key = re.sub(r"\s+", " ", skill.strip()).upper()
    return SKILL_NORMALIZE.get(key, skill.strip())


def normalize_skills(skills: list[str]) -> list[str]:
    """
    스킬 정규화 + 대소문자 통일.
    - SKILL_NORMALIZE 테이블로 표기 통일
    - 중복 제거 (대소문자 무시)
    - 빈 문자열 제거
    """
    seen: dict[str, str] = {}
    for s in skills:
        if not s or not s.strip():
            continue
        normalized = normalize_skill(s.strip())
        key = normalized.upper()
        if key not in seen:
            seen[key] = normalized
    return list(seen.values())


def normalize_budget(budget: str) -> str:
    """예산 문자열 정규화: 쉼표 제거, 단위 통일"""
    if not budget:
        return ""
    cleaned = re.sub(r"[,\s]", "", budget)
    # 만원 단위로 통일
    m = re.match(r"(\d+)만원?", cleaned)
    if m:
        return f"{int(m.group(1)):,}만원"
    return budget.strip()


# ──────────────────────────────────────────────
# 공통 스키마로 변환
# ──────────────────────────────────────────────

def to_common(job: Any) -> dict:
    """소스별 Job 객체 → 공통 스키마 dict 변환"""
    # dataclass 면 asdict, 이미 dict 면 그대로
    raw = asdict(job) if not isinstance(job, dict) else job

    # source 확인
    source = raw.get("source", "unknown")
    fmap   = FIELD_MAP.get(source, {})

    common = {f: "" for f in COMMON_FIELDS}
    common["skills"] = []

    # 공통 필드 직접 복사
    for field in COMMON_FIELDS:
        if field in raw:
            common[field] = raw[field]

    # 소스별 필드 매핑
    for common_key, source_key in fmap.items():
        if source_key and source_key in raw and raw[source_key]:
            common[common_key] = raw[source_key]

    # 스킬 정규화
    common["skills"] = normalize_skills(common.get("skills") or [])

    # 예산 정규화
    common["budget"] = normalize_budget(common.get("budget", ""))

    # description fallback (body 필드)
    if not common["description"] and "body" in raw:
        common["description"] = raw["body"]

    # collected_at 표준화
    common["collected_at"] = datetime.now().isoformat()

    return common


# ──────────────────────────────────────────────
# 중복 체크
# ──────────────────────────────────────────────

# Redis 기반 중복 필터 (cache.py)
# Redis 미연결 시 자동으로 인메모리 set fallback
from cache import RedisFilter as DuplicateFilter


# ──────────────────────────────────────────────
# DB 저장 (SQLAlchemy)
# ──────────────────────────────────────────────

class DBWriter:
    """
    PostgreSQL 저장.
    settings.py의 DB_URL을 참조.
    미설정 시 JSON 파일로 fallback.
    """
    def __init__(self):
        self._buffer: list[dict] = []
        self._fallback_path = f"jobs_{datetime.now():%Y%m%d_%H%M}.json"
        self._use_db = False
        try:
            from db import upsert_job  # noqa: F401 — import 가능 여부 체크
            self._use_db = True
            logger.info("DB 연결 준비 완료")
        except Exception as e:
            logger.warning(f"DB 연결 실패 — JSON fallback 사용: {e}")

    def write(self, common: dict):
        if self._use_db:
            self._write_db(common)
        else:
            self._buffer.append(common)

    def exists(self, url_hash: str) -> bool:
        """DB에 해당 해시가 존재하는지 확인 (cache.py에서 DB 존재 여부 체크용)"""
        if not self._use_db:
            return False
        from db import exists_job
        return exists_job(url_hash)

    def _write_db(self, common: dict):
        from db import upsert_job
        upsert_job(common)

    def close(self):
        if self._buffer:
            import json
            with open(self._fallback_path, "w", encoding="utf-8") as f:
                json.dump(self._buffer, f, ensure_ascii=False, indent=2)
            logger.info(f"JSON fallback 저장: {self._fallback_path} ({len(self._buffer)}건)")


# ──────────────────────────────────────────────
# Pipeline 메인
# ──────────────────────────────────────────────

class Pipeline:
    """
    수집된 Job 객체를 받아 처리하는 파이프라인.
    engine.py에서 pipeline.process(job) 호출.
    """

    def __init__(self):
        self.dup_filter = DuplicateFilter()
        self.db_writer  = DBWriter()
        from cache import meta as _meta
        self._meta = _meta
        self._stats     = {"total": 0, "saved": 0, "duplicate": 0, "filtered": 0}

    def process(self, job: Any) -> bool:
        """
        단일 Job 처리.
        반환: True = 저장됨, False = 건너뜀
        """
        self._stats["total"] += 1

        # dataclass 면 asdict, 이미 dict 면 그대로
        raw = asdict(job) if not isinstance(job, dict) else job

        # 1. 중복 체크
        url_hash = raw.get("url_hash", "")
        if not url_hash:
            url_hash = hashlib.sha256(raw.get("url", "").encode()).hexdigest()[:16]

        if self.dup_filter.is_duplicate(url_hash):
            # [추가] Redis에는 있는데 DB에는 없는 경우 캐시 삭제 후 재수집
            if not self.db_writer.exists(url_hash):
                logger.info(f"  Redis 캐시 존재하나 DB에 없음 -> 캐시 삭제 후 수집 진행: {url_hash}")
                self.dup_filter.remove_hash(url_hash, raw.get("source", ""))
            else:
                self._stats["duplicate"] += 1
                logger.info(f"  중복 건너뜀: {raw.get('title','')[:30]}")
                return False

        # 2. 공통 스키마 변환
        common = to_common(job)

        # 3. 필수 필드 검증
        if not common.get("title") or not common.get("url"):
            self._stats["filtered"] += 1
            logger.debug(f"  필수 필드 없음 — 건너뜀")
            return False

        # 4. DB 저장
        self.db_writer.write(common)
        self._stats["saved"] += 1

        logger.info(
            f"  저장: [{common['source']}] {common['title'][:40]} "
            f"| 기술: {','.join(common['skills'][:3])} "
            f"| 마감: {common['deadline'] or common['end_date']}"
        )
        return True

    def close(self):
        self.db_writer.close()
        logger.info(
            f"파이프라인 통계: "
            f"총 {self._stats['total']}건 / "
            f"저장 {self._stats['saved']}건 / "
            f"중복 {self._stats['duplicate']}건 / "
            f"필터 {self._stats['filtered']}건"
        )