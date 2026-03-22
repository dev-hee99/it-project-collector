"""
IT 프로젝트 공고 수집기 — Streamlit 대시보드

실행:
    streamlit run dashboard.py

구성:
    1. 사이드바  — 필터 (소스 / 스킬 / 기간 / 검색어)
    2. 상단 지표 — 총 공고수 / 소스별 현황 / 마지막 수집
    3. 인기 스킬 — 수평 바차트
    4. 공고 목록 — 카드 형태 + 페이지네이션
    5. 수집 현황 — 소스별 통계 테이블
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, timedelta

import pandas as pd
import streamlit as st

# ──────────────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="IT 프로젝트 공고 수집기",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────
# DB / 캐시 연결 (없으면 더미 데이터)
# ──────────────────────────────────────────────

@st.cache_resource
def load_db():
    logs = []
    try:
        logs.append(("info", "⏳ DB 연결 시도 중..."))

        from settings import DB_URL
        logs.append(("info", f"📌 DB URL: `{DB_URL}`"))

        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL, pool_pre_ping=True, connect_args={"connect_timeout": 5})
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logs.append(("success", "✅ DB 연결 성공"))

        from db import get_active_jobs, get_stats, get_top_skills
        logs.append(("success", "✅ db.py 로드 성공"))

        from cache import meta as collect_meta
        logs.append(("success", "✅ cache.py 로드 성공"))

        return {
            "get_active_jobs": get_active_jobs,
            "get_stats":       get_stats,
            "get_top_skills":  get_top_skills,
            "collect_meta":    collect_meta,
            "mode":            "db",
            "logs":            logs,
        }
    except Exception as e:
        logs.append(("error", f"❌ DB 연결 실패: `{e}`"))
        logs.append(("warning", "⚠️ 더미 데이터 모드로 전환"))
        return {"mode": "demo", "logs": logs}


def get_demo_jobs(limit=20, offset=0, source=None, skill=None, keyword=None):
    """DB 미연결 시 더미 데이터"""
    rows = [
        {"id": 1,  "source": "sism",    "title": "[상주/서울] Spring Boot 백엔드 개발자",
         "company": "(주)테크컴퍼니", "category": "개발", "skills": ["Java", "Spring Boot", "MySQL"],
         "budget": "600만원", "project_duration": "6개월",
         "start_date": "2026-04-01", "end_date": "2026-05-01",
         "deadline": "D-12", "work_type": "기간제 상주", "location": "서울",
         "url": "https://sism.co.kr", "collected_at": datetime.now()},
        {"id": 2,  "source": "okky",    "title": "React + TypeScript 프론트엔드 개발",
         "company": "스타트업A", "category": "개발", "skills": ["React", "TypeScript", "Next.js"],
         "budget": "500만원", "project_duration": "3개월",
         "start_date": "2026-04-15", "end_date": "2026-04-30",
         "deadline": "D-5", "work_type": "계약직", "location": "재택",
         "url": "https://jobs.okky.kr", "collected_at": datetime.now()},
        {"id": 3,  "source": "freemoa", "title": "Python Django REST API 개발",
         "company": "", "category": "개발", "skills": ["Python", "Django", "PostgreSQL", "AWS"],
         "budget": "400~500만원", "project_duration": "4개월",
         "start_date": "2026-04-01", "end_date": "2026-04-20",
         "deadline": "D-8", "work_type": "도급", "location": "원격",
         "url": "https://freemoa.net", "collected_at": datetime.now()},
        {"id": 4,  "source": "kmong",   "title": "Flutter 앱 개발 의뢰",
         "company": "", "category": "개발", "skills": ["Flutter", "Dart", "Firebase"],
         "budget": "300만원", "project_duration": "2개월",
         "start_date": "2026-04-10", "end_date": "2026-04-25",
         "deadline": "D-3", "work_type": "프로젝트", "location": "원격",
         "url": "https://kmong.com", "collected_at": datetime.now()},
        {"id": 5,  "source": "sism",    "title": "[상주/경기] Node.js 백엔드 + AWS 인프라",
         "company": "IT솔루션(주)", "category": "개발", "skills": ["Node.js", "AWS", "Docker", "Redis"],
         "budget": "550만원", "project_duration": "12개월",
         "start_date": "2026-05-01", "end_date": "2026-05-10",
         "deadline": "D-18", "work_type": "기간제 상주", "location": "경기",
         "url": "https://sism.co.kr", "collected_at": datetime.now()},
    ]
    if source:
        rows = [r for r in rows if r["source"] == source]
    if skill:
        rows = [r for r in rows if skill in (r["skills"] or [])]
    if keyword:
        rows = [r for r in rows if keyword.lower() in r["title"].lower()]
    return rows[offset:offset + limit]


def get_demo_stats():
    return {"by_source": [
        {"source": "sism",    "total": 128, "active": 94,  "closed": 34, "last_collected": datetime.now() - timedelta(hours=2)},
        {"source": "okky",    "total": 87,  "active": 61,  "closed": 26, "last_collected": datetime.now() - timedelta(hours=2)},
        {"source": "freemoa", "total": 214, "active": 178, "closed": 36, "last_collected": datetime.now() - timedelta(hours=3)},
        {"source": "kmong",   "total": 56,  "active": 41,  "closed": 15, "last_collected": datetime.now() - timedelta(hours=3)},
    ]}


def get_demo_skills():
    return [
        {"skill": "Java",        "job_count": 112},
        {"skill": "React",       "job_count": 98},
        {"skill": "Spring Boot", "job_count": 87},
        {"skill": "Python",      "job_count": 76},
        {"skill": "TypeScript",  "job_count": 65},
        {"skill": "Node.js",     "job_count": 58},
        {"skill": "AWS",         "job_count": 54},
        {"skill": "Docker",      "job_count": 48},
        {"skill": "MySQL",       "job_count": 45},
        {"skill": "Vue",         "job_count": 41},
    ]


# ──────────────────────────────────────────────
# 연결 상태 체크
# ──────────────────────────────────────────────

@st.cache_data(ttl=30)  # 30초마다 갱신
def check_connections():
    result = {}

    # PostgreSQL 체크
    try:
        from settings import DB_URL
        from sqlalchemy import create_engine, text
        engine = create_engine(DB_URL, connect_args={"connect_timeout": 3})
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT COUNT(*) as total, "
                "COUNT(*) FILTER (WHERE is_active) as active "
                "FROM jobs"
            )).mappings().one()
            result["db"] = {
                "status":  "connected",
                "total":   row["total"],
                "active":  row["active"],
                "url":     DB_URL.split("@")[-1] if "@" in DB_URL else DB_URL,
            }
    except Exception as e:
        result["db"] = {"status": "error", "error": str(e)}

    # Redis 체크
    try:
        from settings import REDIS_URL
        import redis
        client = redis.from_url(REDIS_URL, socket_connect_timeout=3, decode_responses=True)
        client.ping()
        info        = client.info("memory")
        hash_count  = client.scard("jobs:hashes")
        result["redis"] = {
            "status":     "connected",
            "url":        REDIS_URL.split("@")[-1] if "@" in REDIS_URL else REDIS_URL,
            "hash_count": hash_count,
            "used_memory": info.get("used_memory_human", ""),
        }
    except Exception as e:
        result["redis"] = {"status": "error", "error": str(e)}

    return result


# ──────────────────────────────────────────────
# 소스 레이블 / 색상
# ──────────────────────────────────────────────

SOURCE_LABELS = {
    "sism":    "SISM",
    "okky":    "OKKY Jobs",
    "freemoa": "프리모아",
    "kmong":   "크몽",
    "all":     "전체",
}

SOURCE_COLORS = {
    "sism":    "#378ADD",
    "okky":    "#1D9E75",
    "freemoa": "#D85A30",
    "kmong":   "#BA7517",
}


def source_badge(source: str) -> str:
    color = SOURCE_COLORS.get(source, "#888")
    label = SOURCE_LABELS.get(source, source)
    return (
        f'<span style="background:{color}22;color:{color};'
        f'border:1px solid {color}66;border-radius:4px;'
        f'padding:2px 8px;font-size:11px;font-weight:500">'
        f'{label}</span>'
    )


# ──────────────────────────────────────────────
# CSS
# ──────────────────────────────────────────────

st.markdown("""
<style>
.job-card {
    border: 1px solid rgba(0,0,0,0.08);
    border-radius: 10px;
    padding: 16px 18px;
    margin-bottom: 10px;
    background: var(--background-color);
    transition: box-shadow .15s;
}
.job-card:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.08); }
.job-title { font-size: 15px; font-weight: 600; margin: 6px 0 4px; }
.job-meta  { font-size: 12px; color: #888; margin: 2px 0; }
.skill-tag {
    display: inline-block;
    background: #f0f0f0;
    border-radius: 4px;
    padding: 1px 7px;
    font-size: 11px;
    margin: 2px 2px 0 0;
    color: #444;
}
.metric-card {
    text-align: center;
    padding: 12px;
    border-radius: 8px;
    border: 1px solid rgba(0,0,0,0.07);
}
.metric-val  { font-size: 28px; font-weight: 700; }
.metric-label { font-size: 12px; color: #888; margin-top: 2px; }
.deadline-urgent { color: #E24B4A; font-weight: 600; }
.deadline-normal { color: #1D9E75; }
</style>
""", unsafe_allow_html=True)


# ──────────────────────────────────────────────
# 데이터 로드
# ──────────────────────────────────────────────

db = load_db()
is_demo = db["mode"] == "demo"

# 수집 프로세스 상태 초기화
if "collect_proc"    not in st.session_state: st.session_state["collect_proc"]    = None
if "collect_logs"    not in st.session_state: st.session_state["collect_logs"]    = []
if "collect_running" not in st.session_state: st.session_state["collect_running"] = False
if "collect_source"  not in st.session_state: st.session_state["collect_source"]  = "전체"
if "sort_option"     not in st.session_state: st.session_state["sort_option"]     = "posted_at_desc"

# DB 연결 로그 표시
with st.expander("🔌 DB 연결 로그", expanded=is_demo):
    for level, msg in db.get("logs", []):
        if level == "success":
            st.success(msg)
        elif level == "error":
            st.error(msg)
        elif level == "warning":
            st.warning(msg)
        else:
            st.info(msg)
    if not db.get("logs"):
        st.caption("로그 없음")

if is_demo:
    st.info("💡 DB 미연결 상태입니다. 더미 데이터로 표시 중이에요.", icon="ℹ️")


# ──────────────────────────────────────────────
# 사이드바 — 필터
# ──────────────────────────────────────────────

with st.sidebar:
    st.markdown("## 🔍 필터")

    selected_source = st.selectbox(
        "출처",
        options=["all", "sism", "okky", "freemoa", "kmong"],
        format_func=lambda x: SOURCE_LABELS[x],
        key="source_select",
    )
    st.session_state["source_filter"] = None if selected_source == "all" else selected_source

    keyword = st.text_input("제목 검색", placeholder="예: React, Spring Boot...", key="keyword_input")
    st.session_state["keyword_val"] = keyword

    top_skills = get_demo_skills() if is_demo else db["get_top_skills"](30)
    skill_options = [s["skill"] for s in top_skills]
    selected_skills = st.multiselect("기술 스택", options=skill_options, key="skills_select")
    st.session_state["selected_skills_val"] = selected_skills
    st.session_state["skill_filter"] = selected_skills[0] if selected_skills else None

    st.divider()
    st.markdown("## 📑 정렬")
    sort_option = st.selectbox(
        "정렬 기준",
        options=["posted_at_desc", "posted_at_asc", "deadline_asc", "collected_at_desc"],
        format_func=lambda x: {
            "posted_at_desc":    "등록일 최신순",
            "posted_at_asc":     "등록일 오래된순",
            "deadline_asc":      "마감 임박순",
            "collected_at_desc": "수집일 최신순",
        }[x],
        key="sort_select",
    )
    st.session_state["sort_option"] = sort_option

    st.divider()

    # 수집 실행 버튼
    st.markdown("## ⚙️ 수집 제어")

    source_opt = st.selectbox(
        "수집 소스",
        options=["전체", "sism", "okky", "freemoa", "kmong"],
        format_func=lambda x: SOURCE_LABELS.get(x, x),
        key="collect_source_select",
    )

    col_run, col_stop = st.columns(2)
    with col_run:
        run_clicked = st.button(
            "▶ 수집 시작",
            use_container_width=True,
            disabled=st.session_state["collect_running"],
        )
    with col_stop:
        stop_clicked = st.button(
            "■ 중지",
            use_container_width=True,
            disabled=not st.session_state["collect_running"],
        )

    if run_clicked:
        import subprocess, sys, os
        cmd = [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "engine.py")]
        if source_opt != "전체":
            cmd += ["--source", source_opt]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace",
        )
        st.session_state["collect_proc"]    = proc
        st.session_state["collect_logs"]    = []
        st.session_state["collect_running"] = True
        st.session_state["collect_source"]  = source_opt
        st.rerun()

    if stop_clicked:
        proc = st.session_state.get("collect_proc")
        if proc and proc.poll() is None:
            proc.terminate()
            st.session_state["collect_logs"].append("⛔ 수집 중지됨")
        st.session_state["collect_running"] = False
        st.rerun()

    st.divider()
    if st.button("🗑️ 캐시 초기화", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ──────────────────────────────────────────────
# 헤더
# ──────────────────────────────────────────────

st.markdown("# 💼 IT 프로젝트 공고 수집기")
st.caption(f"오늘 날짜 기준 모집중 공고 · {datetime.now():%Y년 %m월 %d일} 기준")
st.divider()


# ──────────────────────────────────────────────
# 수집 모니터링 fragment
# ──────────────────────────────────────────────

@st.fragment(run_every=2)
def monitor_panel():
    proc    = st.session_state.get("collect_proc")
    running = st.session_state.get("collect_running", False)
    logs    = st.session_state.get("collect_logs", [])
    source  = st.session_state.get("collect_source", "전체")

    # 프로세스 stdout 읽기 (non-blocking)
    if proc and proc.poll() is None:
        try:
            while True:
                line = proc.stdout.readline()
                if not line:
                    break
                line = line.rstrip()
                if line:
                    logs.append(line)
                    st.session_state["collect_logs"] = logs[-200:]  # 최대 200줄
        except Exception:
            pass
    elif proc and proc.poll() is not None and running:
        # 프로세스 종료 감지
        st.session_state["collect_running"] = False
        logs.append(f"✅ 수집 완료 (종료 코드: {proc.returncode})")
        st.session_state["collect_logs"] = logs

    if not logs and not running:
        return

    status_icon = "🔄" if running else "✅"
    status_text = f"수집 중... ({source})" if running else f"수집 완료 ({source})"

    with st.expander(f"{status_icon} 수집 모니터링 — {status_text}", expanded=running):
        # 진행 지표
        if running:
            st.markdown(
                f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">'
                f'<div style="width:10px;height:10px;border-radius:50%;background:#1D9E75;'
                f'animation:pulse 1s infinite"></div>'
                f'<span style="font-size:13px;color:#1D9E75">수집 진행 중</span>'
                f'</div>'
                f'<style>@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}</style>',
                unsafe_allow_html=True,
            )

        # 로그별 색상 처리
        if logs:
            colored_logs = []
            for line in logs[-100:]:  # 최근 100줄만 표시
                if any(k in line for k in ["ERROR", "오류", "실패", "❌"]):
                    colored_logs.append(f'<span style="color:#E24B4A">{line}</span>')
                elif any(k in line for k in ["WARNING", "경고", "⚠️"]):
                    colored_logs.append(f'<span style="color:#BA7517">{line}</span>')
                elif any(k in line for k in ["완료", "성공", "✅", "저장"]):
                    colored_logs.append(f'<span style="color:#1D9E75">{line}</span>')
                elif any(k in line for k in ["수집 중", "목록", "상세", "page="]):
                    colored_logs.append(f'<span style="color:#378ADD">{line}</span>')
                else:
                    colored_logs.append(f'<span style="color:var(--text-color)">{line}</span>')

            log_html = "<br>".join(colored_logs)
            st.markdown(
                f'<div style="'
                f'background:rgba(0,0,0,0.04);'
                f'border:1px solid rgba(0,0,0,0.08);'
                f'border-radius:8px;'
                f'padding:12px 14px;'
                f'font-family:monospace;'
                f'font-size:12px;'
                f'line-height:1.8;'
                f'max-height:400px;'
                f'overflow-y:auto;">'
                f'{log_html}'
                f'</div>',
                unsafe_allow_html=True,
            )

        # 통계 요약
        total_lines  = len(logs)
        saved_count  = sum(1 for l in logs if "저장:" in l or "저장 완료" in l)
        error_count  = sum(1 for l in logs if "ERROR" in l or "오류" in l)

        c1, c2, c3 = st.columns(3)
        c1.metric("로그 줄 수",  total_lines)
        c2.metric("저장 건수",   saved_count)
        c3.metric("오류 건수",   error_count)

        if st.button("🗑️ 로그 지우기", key="clear_logs"):
            st.session_state["collect_logs"] = []
            st.rerun()

monitor_panel()

# ──────────────────────────────────────────────
# 연결 상태 패널
# ──────────────────────────────────────────────

# ── 자동 갱신 주기 계산 ─────────────────────────
_auto    = st.session_state.get("auto_refresh_val", False)
_interval = st.session_state.get("refresh_interval_val", 30)

# session_state에 sidebar 값 저장 (fragment 밖에서 읽기 위해)
if "auto_refresh_val" not in st.session_state:
    st.session_state["auto_refresh_val"] = False

@st.fragment(run_every=(_interval if _auto else None))
def live_panel():
    conn = check_connections()
    db_info    = conn.get("db",    {})
    redis_info = conn.get("redis", {})

    db_ok    = db_info.get("status")    == "connected"
    redis_ok = redis_info.get("status") == "connected"

    with st.expander(
        f"🔌 연결 상태 — "
        f"DB {'🟢' if db_ok else '🔴'}  "
        f"Redis {'🟢' if redis_ok else '🔴'}",
        expanded=not (db_ok and redis_ok),
    ):
        col_db, col_redis = st.columns(2)

        with col_db:
            st.markdown("**PostgreSQL**")
            if db_ok:
                st.success("연결됨")
                st.caption(f"호스트: `{db_info.get('url','')}`")
                m1, m2 = st.columns(2)
                m1.metric("전체 공고", f"{db_info.get('total', 0):,}건")
                m2.metric("모집중",   f"{db_info.get('active', 0):,}건")
            else:
                st.error("연결 실패")
                st.code(db_info.get("error", ""), language="text")
            st.caption("settings.py의 `DB_URL`을 확인하세요.")

        with col_redis:
            st.markdown("**Redis**")
            if redis_ok:
                st.success("연결됨")
                st.caption(f"호스트: `{redis_info.get('url','')}`")
                m1, m2 = st.columns(2)
                m1.metric("캐시 해시 수", f"{redis_info.get('hash_count', 0):,}개")
                m2.metric("메모리 사용", redis_info.get("used_memory", "-"))
            else:
                st.warning("연결 실패 (선택 사항)")
                st.code(redis_info.get("error", ""), language="text")
                st.caption("Redis 없이도 인메모리 캐시로 동작해요.")

        if st.button("🔄 연결 상태 새로고침", key="refresh_conn"):
            st.cache_data.clear()
            st.rerun()

    # 마지막 갱신 시각 표시
    st.caption(f"마지막 갱신: {datetime.now():%H:%M:%S}")

    st.divider()

    # ── 데이터 조회 ──────────────────────────────
    source_filter = st.session_state.get("source_filter")
    skill_filter  = st.session_state.get("skill_filter")
    keyword_val   = st.session_state.get("keyword_val", "")

    if is_demo:
        jobs  = get_demo_jobs(limit=50, source=source_filter,
                              skill=skill_filter, keyword=keyword_val or None)
        stats = get_demo_stats()
        top_skills_data = get_demo_skills()
    else:
        jobs  = db["get_active_jobs"](source=source_filter, skill=skill_filter, limit=50)
        stats = db["get_stats"]()
        top_skills_data = db["get_top_skills"](15)

    if keyword_val and not is_demo:
        jobs = [j for j in jobs if keyword_val.lower() in j.get("title", "").lower()]

    selected_skills_val = st.session_state.get("selected_skills_val", [])
    if len(selected_skills_val) > 1:
        jobs = [j for j in jobs
                if all(s in (j.get("skills") or []) for s in selected_skills_val)]

    # ── 정렬 ──────────────────────────────────
    sort_opt = st.session_state.get("sort_option", "posted_at_desc")

    def sort_key_posted(j, reverse=True):
        """posted_at 기준 정렬 키 — 빈 값은 맨 뒤로"""
        val = j.get("posted_at") or j.get("collected_at") or ""
        return val if val else ("9" if not reverse else "")

    def sort_key_deadline(j):
        """마감 임박순 — D-N 숫자 작을수록 앞, 날짜형도 처리"""
        deadline = j.get("deadline", "") or j.get("end_date", "")
        import re as _re
        d_m = _re.search(r"D-(\d+)", deadline)
        if d_m:
            return int(d_m.group(1))
        date_m = _re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", deadline)
        if date_m:
            return int(date_m.group(1)) * 10000 + int(date_m.group(2)) * 100 + int(date_m.group(3))
        return 99999  # 마감일 없으면 맨 뒤

    if sort_opt == "posted_at_desc":
        jobs = sorted(jobs, key=lambda j: j.get("posted_at") or j.get("collected_at") or "", reverse=True)
    elif sort_opt == "posted_at_asc":
        jobs = sorted(jobs, key=lambda j: j.get("posted_at") or j.get("collected_at") or "")
    elif sort_opt == "deadline_asc":
        jobs = sorted(jobs, key=sort_key_deadline)
    elif sort_opt == "collected_at_desc":
        jobs = sorted(jobs, key=lambda j: str(j.get("collected_at") or ""), reverse=True)

    by_source = {r["source"]: r for r in stats.get("by_source", [])}
    total_active = sum(r.get("active", 0) for r in by_source.values())

    # ── 상단 지표 ─────────────────────────────
    cols = st.columns(5)
    metrics = [
        ("전체 모집중", total_active, "건"),
        ("SISM",    by_source.get("sism",    {}).get("active", 0), "건"),
        ("OKKY",    by_source.get("okky",    {}).get("active", 0), "건"),
        ("프리모아", by_source.get("freemoa", {}).get("active", 0), "건"),
        ("크몽",    by_source.get("kmong",   {}).get("active", 0), "건"),
    ]
    for col, (label, val, unit) in zip(cols, metrics):
        with col:
            st.markdown(
                f'<div class="metric-card">'
                f'<div class="metric-val">{val:,}<span style="font-size:14px;font-weight:400"> {unit}</span></div>'
                f'<div class="metric-label">{label}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── 인기 스킬 차트 ────────────────────────
    with st.expander("📊 인기 기술 스택 TOP 10", expanded=True):
        if top_skills_data:
            df_skills = pd.DataFrame(top_skills_data[:10])
            df_skills = df_skills.sort_values("job_count", ascending=True)
            chart_data = pd.DataFrame(
                {"공고 수": df_skills["job_count"].values},
                index=df_skills["skill"].values,
            )
            st.bar_chart(chart_data, horizontal=True, height=300)
        else:
            st.caption("스킬 데이터 없음")

    # ── 공고 목록 ─────────────────────────────
    sort_label = {
        "posted_at_desc":    "등록일 최신순",
        "posted_at_asc":     "등록일 오래된순",
        "deadline_asc":      "마감 임박순",
        "collected_at_desc": "수집일 최신순",
    }.get(sort_opt, "")
    st.markdown(f"### 📋 공고 목록 ({len(jobs)}건) <span style='font-size:13px;color:#888;font-weight:400'>· {sort_label}</span>", unsafe_allow_html=True)
    if not jobs:
        st.info("조건에 맞는 공고가 없어요.")
    else:
        PAGE_SIZE   = 10
        total_pages = max(1, (len(jobs) - 1) // PAGE_SIZE + 1)
        page_col, _ = st.columns([2, 8])
        with page_col:
            page = st.number_input(
                "페이지", min_value=1, max_value=total_pages,
                value=1, step=1, label_visibility="collapsed",
            )
        st.caption(f"{page} / {total_pages} 페이지")

        start     = (page - 1) * PAGE_SIZE
        page_jobs = jobs[start: start + PAGE_SIZE]

        for job in page_jobs:
            skills    = job.get("skills") or []
            budget    = job.get("budget", "")
            deadline  = job.get("deadline", "")
            duration  = job.get("project_duration", "")
            location  = job.get("location", "")
            work_type = job.get("work_type", "")
            end_date  = job.get("end_date", "")
            url       = job.get("url", "#")
            source    = job.get("source", "")

            is_urgent = False
            import re
            d_m = re.search(r"D-(\d+)", deadline)
            if d_m and int(d_m.group(1)) <= 7:
                is_urgent = True

            deadline_class = "deadline-urgent" if is_urgent else "deadline-normal"
            deadline_disp  = deadline or end_date or ""
            skill_tags = "".join(
                f'<span class="skill-tag">{s}</span>' for s in skills[:8]
            )
            meta_parts = [x for x in [location, work_type, duration, budget] if x]
            meta_str   = " · ".join(meta_parts)

            st.markdown(
                f'''<div class="job-card">
                    <div style="display:flex;justify-content:space-between;align-items:flex-start">
                        <div>
                            {source_badge(source)}
                            <div class="job-title">
                                <a href="{url}" target="_blank" style="text-decoration:none;color:inherit">
                                    {job.get("title", "")}
                                </a>
                            </div>
                            <div class="job-meta">{meta_str}</div>
                            <div style="margin-top:6px">{skill_tags}</div>
                        </div>
                        <div style="text-align:right;white-space:nowrap;margin-left:12px">
                            <div class="{deadline_class}" style="font-size:14px">{deadline_disp}</div>
                            <div class="job-meta" style="margin-top:4px">{job.get("start_date","") or ""}</div>
                        </div>
                    </div>
                </div>''',
                unsafe_allow_html=True,
            )

    # ── 수집 현황 테이블 ──────────────────────
    st.divider()
    st.markdown("### 🗂️ 소스별 수집 현황")
    if by_source:
        rows = []
        for source, data in by_source.items():
            last = data.get("last_collected", "")
            if hasattr(last, "strftime"):
                last = last.strftime("%Y-%m-%d %H:%M")
            rows.append({
                "출처":        SOURCE_LABELS.get(source, source),
                "전체":        data.get("total",  0),
                "모집중":      data.get("active", 0),
                "마감":        data.get("closed", 0),
                "마지막 수집": last,
            })
        st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)

        if not is_demo:
            try:
                all_meta = db["collect_meta"].get_all()
                meta_rows = []
                for src, m in all_meta.items():
                    if m:
                        meta_rows.append({
                            "소스":         SOURCE_LABELS.get(src, src),
                            "상태":         m.get("status", ""),
                            "마지막 페이지": m.get("last_page", ""),
                            "수집 건수":    m.get("count", ""),
                            "시작 시간":    m.get("started_at", ""),
                        })
                if meta_rows:
                    st.caption("Redis 수집 상태")
                    st.dataframe(pd.DataFrame(meta_rows), width="stretch", hide_index=True)
            except Exception:
                pass

# fragment 실행 (자동 갱신 주기 적용)
live_panel()


# ──────────────────────────────────────────────
# 실시간 신규 수집 피드 (3초마다 갱신)
# ──────────────────────────────────────────────

@st.fragment(run_every=3)
def realtime_feed():
    running  = st.session_state.get("collect_running", False)
    show     = st.session_state.get("show_feed", True)

    if not running and not show:
        return
    if is_demo:
        return

    try:
        from db import get_session
        from sqlalchemy import text as _text

        with get_session() as session:
            rows = session.execute(_text("""
                SELECT source, title, skills, budget, deadline,
                       work_type, location, url, collected_at
                FROM jobs
                WHERE collected_at >= NOW() - INTERVAL '3 minutes'
                ORDER BY collected_at DESC
                LIMIT 20
            """)).mappings().all()
            new_jobs = [dict(r) for r in rows]
    except Exception:
        return

    if not new_jobs:
        if running:
            st.info("⏳ 수집 중... 신규 데이터를 기다리고 있어요.")
        return

    with st.expander(
        f"🟢 실시간 수집 피드 — 최근 3분 내 {len(new_jobs)}건",
        expanded=running,
    ):
        st.caption(f"마지막 확인: {datetime.now():%H:%M:%S}")

        for job in new_jobs:
            source       = job.get("source", "")
            title        = job.get("title", "")
            url          = job.get("url", "#")
            skills       = job.get("skills") or []
            budget       = job.get("budget", "")
            deadline     = job.get("deadline", "")
            location     = job.get("location", "")
            collected_at = job.get("collected_at", "")

            if hasattr(collected_at, "strftime"):
                collected_str = collected_at.strftime("%H:%M:%S")
            else:
                collected_str = str(collected_at)[11:19] if collected_at else ""

            if isinstance(skills, str):
                skills = [s.strip() for s in skills.split(",") if s.strip()]

            skill_tags = "".join(
                f'<span class="skill-tag">{s}</span>'
                for s in skills[:5]
            )
            meta_parts = [x for x in [location, budget, deadline] if x]
            meta_str   = " · ".join(meta_parts)

            card_html = (
                '<div class="job-card" style="border-left:3px solid #1D9E75">'
                '<div style="display:flex;justify-content:space-between;align-items:flex-start">'
                '<div>'
                + source_badge(source)
                + f'<div class="job-title" style="margin-top:4px">'
                f'<a href="{url}" target="_blank" style="text-decoration:none;color:inherit">'
                f'{title}</a></div>'
                f'<div class="job-meta">{meta_str}</div>'
                f'<div style="margin-top:4px">{skill_tags}</div>'
                '</div>'
                '<div style="text-align:right;white-space:nowrap;margin-left:12px">'
                '<div style="font-size:11px;color:#1D9E75;font-weight:500">NEW</div>'
                f'<div class="job-meta">{collected_str}</div>'
                '</div></div></div>'
            )
            st.markdown(card_html, unsafe_allow_html=True)

realtime_feed()