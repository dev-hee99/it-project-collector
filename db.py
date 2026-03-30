"""
DB 연결 및 upsert 처리
pipeline.py의 DBWriter에서 호출
"""

import logging
import re
from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from settings import DB_URL

logger = logging.getLogger("db")

# ──────────────────────────────────────────────
# 엔진 / 세션 (lazy 초기화)
# ──────────────────────────────────────────────

_engine  = None
_Session = None


def _get_engine():
    global _engine, _Session
    if _engine is None:
        _engine  = create_engine(
            DB_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=2,
            pool_recycle=3600,
        )
        _Session = sessionmaker(bind=_engine)
    return _engine


@contextmanager
def get_session():
    _get_engine()   # lazy 초기화
    session = _Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ──────────────────────────────────────────────
# 상태 판별
# ──────────────────────────────────────────────

def infer_status(status_str: str) -> str:
    """
    소스별 상태 텍스트 → DB ENUM (active / closed / unknown)
    """
    s = (status_str or "").lower()
    if any(k in s for k in ["모집중", "active", "open", "진행"]):
        return "active"
    if any(k in s for k in ["마감", "종료", "완료", "closed", "expired", "ended"]):
        return "closed"
    return "unknown"


def _is_active_local(deadline_str: str, status_str: str = "") -> bool:
    """
    순환 임포트 방지를 위해 db.py 내부에 직접 구현.
    sism_parser.is_active 와 동일한 로직.
    """
    from datetime import date
    closed = ["마감", "종료", "완료", "closed", "expired", "ended"]
    if any(k in (status_str or "").lower() for k in closed):
        return False
    d_m = re.search(r"D-(\d+)", deadline_str or "")
    if d_m:
        return int(d_m.group(1)) >= 0
    date_m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", deadline_str or "")
    if date_m:
        try:
            dl = date(int(date_m.group(1)), int(date_m.group(2)), int(date_m.group(3)))
            return dl >= date.today()
        except ValueError:
            pass
    return True


def infer_is_active(common: dict) -> bool:
    """공통 스키마 dict에서 모집중 여부 판단."""
    if "is_active" in common:
        return bool(common["is_active"])
    return _is_active_local(
        common.get("deadline", "") or common.get("end_date", ""),
        common.get("status", ""),
    )


def exists_job(url_hash: str) -> bool:
    """DB에 해당 url_hash가 존재하는지 확인"""
    if not url_hash:
        return False
    try:
        with get_session() as session:
            res = session.execute(
                text("SELECT 1 FROM jobs WHERE url_hash = :h LIMIT 1"),
                {"h": url_hash}
            ).fetchone()
            return res is not None
    except Exception as e:
        logger.error(f"DB 존재 확인 실패 ({url_hash}): {e}")
        return False


# ──────────────────────────────────────────────
# upsert
# ──────────────────────────────────────────────

UPSERT_SQL = text("""
    CALL upsert_job(
        :source, :source_id, :url, :url_hash,
        :title, :company, :category, :sub_category, :work_type, :location,
        :skills, :budget, :project_duration,
        :start_date, :end_date, :deadline, :posted_at,
        :description,
        :status ::job_status,
        :is_active
    )
""")


def upsert_job(common: dict) -> bool:
    """
    공통 스키마 dict → jobs 테이블 upsert.
    반환: True = 성공, False = 실패
    """
    try:
        status    = infer_status(common.get("status", ""))
        is_active = infer_is_active(common)

        params = {
            "source":           common.get("source",           "unknown"),
            "source_id":        common.get("source_id",        ""),
            "url":              common.get("url",               ""),
            "url_hash":         common.get("url_hash",          ""),
            "title":            common.get("title",             ""),
            "company":          common.get("company",           ""),
            "category":         common.get("category",          ""),
            "sub_category":     common.get("sub_category",      ""),
            "work_type":        common.get("work_type",         ""),
            "location":         common.get("location",          ""),
            # skills: 리스트 → PostgreSQL 배열
            "skills":           common.get("skills") or [],
            "budget":           common.get("budget",            ""),
            "project_duration": common.get("project_duration",  ""),
            "start_date":       common.get("start_date",        ""),
            "end_date":         common.get("end_date",          ""),
            "deadline":         common.get("deadline",          ""),
            "posted_at":        common.get("posted_at",         ""),
            "description":      (common.get("description") or "")[:5000],
            "status":           status,
            "is_active":        is_active,
        }

        with get_session() as session:
            session.execute(UPSERT_SQL, params)

        return True

    except Exception as e:
        logger.error(f"upsert 실패 ({common.get('url_hash','')}): {e}")
        return False


# ──────────────────────────────────────────────
# 만료 공고 비활성화 (스케줄러에서 주기 호출)
# ──────────────────────────────────────────────

def deactivate_expired():
    """만료된 공고를 is_active=FALSE 로 업데이트"""
    try:
        with get_session() as session:
            session.execute(text("CALL deactivate_expired_jobs()"))
        logger.info("만료 공고 비활성화 완료")
    except Exception as e:
        logger.error(f"만료 공고 비활성화 실패: {e}")


# ──────────────────────────────────────────────
# 조회 헬퍼 (대시보드 / API 용)
# ──────────────────────────────────────────────

def get_active_jobs(
    source: str | None = None,
    skill:  str | None = None,
    limit:  int = 50,
    offset: int = 0,
) -> list[dict]:
    """
    모집중 공고 목록 조회.
    source, skill 필터 지원.
    """
    conditions = ["is_active = TRUE"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if source:
        conditions.append("source = :source ::job_source")
        params["source"] = source

    if skill:
        # 대소문자 무시 스킬 필터
        conditions.append("EXISTS (SELECT 1 FROM unnest(skills) s WHERE lower(s) = lower(:skill))")
        params["skill"] = skill

    where = " AND ".join(conditions)
    sql   = text(f"""
        SELECT id, source, title, company, category,
               skills, budget, project_duration,
               start_date, end_date, deadline,
               work_type, location, url, collected_at
        FROM jobs
        WHERE {where}
        ORDER BY collected_at DESC
        LIMIT :limit OFFSET :offset
    """)

    with get_session() as session:
        rows = session.execute(sql, params).mappings().all()
        return [dict(r) for r in rows]


def get_stats() -> dict:
    """소스별 / 전체 수집 통계"""
    sql = text("SELECT * FROM v_stats_by_source")
    with get_session() as session:
        rows = session.execute(sql).mappings().all()
        return {"by_source": [dict(r) for r in rows]}


def get_top_skills(limit: int = 20) -> list[dict]:
    """모집중 공고 기준 인기 스킬 순위"""
    sql = text(f"SELECT skill, job_count FROM v_top_skills LIMIT :limit")
    with get_session() as session:
        rows = session.execute(sql, {"limit": limit}).mappings().all()
        return [dict(r) for r in rows]