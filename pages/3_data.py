"""
수집 데이터 조회 페이지
- 필터 / 정렬 / 검색
- 공고 카드 목록 + 페이지네이션
- 인기 스킬 차트
- 소스별 통계
"""

import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(
    page_title="수집 데이터",
    page_icon="📋",
    layout="wide",
)

SOURCE_LABELS = {
    "sism": "SISM", "okky": "OKKY Jobs",
    "freemoa": "프리모아", "kmong": "크몽", "all": "전체",
}
SOURCE_COLORS = {
    "sism": "#378ADD", "okky": "#1D9E75",
    "freemoa": "#D85A30", "kmong": "#BA7517",
}

# ── DB 연결 ───────────────────────────────────
@st.cache_resource
def load_db():
    try:
        from db import get_active_jobs, get_stats, get_top_skills
        return {"get_active_jobs": get_active_jobs,
                "get_stats": get_stats,
                "get_top_skills": get_top_skills,
                "mode": "db"}
    except Exception:
        return {"mode": "demo"}


def get_demo_jobs(source=None, skill=None, keyword=None, limit=50):
    rows = [
        {"id": 1,  "source": "sism",    "title": "[상주/서울] Spring Boot 백엔드 개발자",
         "company": "(주)테크", "category": "개발", "skills": ["Java", "Spring Boot", "MySQL"],
         "budget": "600만원", "project_duration": "6개월",
         "start_date": "2026-04-01", "end_date": "2026-05-01",
         "deadline": "D-12", "work_type": "기간제 상주", "location": "서울",
         "url": "https://sism.co.kr", "collected_at": datetime.now()},
        {"id": 2,  "source": "okky",    "title": "React + TypeScript 프론트엔드",
         "company": "스타트업A", "category": "개발", "skills": ["React", "TypeScript", "Next.js"],
         "budget": "500만원", "project_duration": "3개월",
         "start_date": "2026-04-15", "end_date": "2026-04-30",
         "deadline": "D-5", "work_type": "계약직", "location": "재택",
         "url": "https://jobs.okky.kr", "collected_at": datetime.now()},
        {"id": 3,  "source": "freemoa", "title": "Python Django REST API 개발",
         "company": "", "category": "개발", "skills": ["Python", "Django", "PostgreSQL"],
         "budget": "400만원", "project_duration": "4개월",
         "start_date": "2026-04-01", "end_date": "2026-04-20",
         "deadline": "D-8", "work_type": "도급", "location": "원격",
         "url": "https://freemoa.net", "collected_at": datetime.now()},
        {"id": 4,  "source": "kmong",   "title": "Flutter 앱 개발 의뢰",
         "company": "", "category": "개발", "skills": ["Flutter", "Dart", "Firebase"],
         "budget": "300만원", "project_duration": "2개월",
         "start_date": "2026-04-10", "end_date": "2026-04-25",
         "deadline": "D-3", "work_type": "프로젝트", "location": "원격",
         "url": "https://kmong.com", "collected_at": datetime.now()},
    ]
    if source: rows = [r for r in rows if r["source"] == source]
    if skill:  rows = [r for r in rows if skill.lower() in [s.lower() for s in r["skills"]]]
    if keyword:
        kw = keyword.lower()
        rows = [r for r in rows if kw in r["title"].lower()
                or any(kw in s.lower() for s in r["skills"])]
    return rows[:limit]


def get_demo_stats():
    return {"by_source": [
        {"source": "sism",    "total": 128, "active": 94,  "closed": 34},
        {"source": "okky",    "total": 87,  "active": 61,  "closed": 26},
        {"source": "freemoa", "total": 214, "active": 178, "closed": 36},
        {"source": "kmong",   "total": 56,  "active": 41,  "closed": 15},
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


def source_badge(source):
    color = SOURCE_COLORS.get(source, "#888")
    label = SOURCE_LABELS.get(source, source)
    return (
        f'<span style="background:{color}22;color:{color};'
        f'border:1px solid {color}66;border-radius:4px;'
        f'padding:2px 8px;font-size:11px;font-weight:500">{label}</span>'
    )


db      = load_db()
is_demo = db["mode"] == "demo"

# ── 헤더 ─────────────────────────────────────
st.markdown("# 📋 수집 데이터")
if is_demo:
    st.info("💡 DB 미연결 — 더미 데이터 표시 중", icon="ℹ️")
st.divider()

# ── 필터 바 (4열) ─────────────────────────────
f1, f2, f3, f4 = st.columns([2, 2, 3, 2])

with f1:
    sel_source = st.selectbox(
        "출처", ["all", "sism", "okky", "freemoa", "kmong"],
        format_func=lambda x: SOURCE_LABELS.get(x, x),
        key="data_source", label_visibility="collapsed",
    )
    source_filter = None if sel_source == "all" else sel_source

with f2:
    sort_opt = st.selectbox(
        "정렬",
        ["collected_at_desc", "posted_at_desc", "posted_at_asc", "deadline_asc"],
        format_func=lambda x: {
            "collected_at_desc": "🕐 수집일 최신순",
            "posted_at_desc":    "📅 등록일 최신순",
            "posted_at_asc":     "📅 등록일 오래된순",
            "deadline_asc":      "⏰ 마감 임박순",
        }[x],
        key="data_sort", label_visibility="collapsed",
    )

with f3:
    keyword = st.text_input(
        "검색", placeholder="🔎  제목 또는 기술스택 검색...",
        key="data_keyword", label_visibility="collapsed",
    )

with f4:
    skill_opts = get_demo_skills() if is_demo else db["get_top_skills"](50)
    sel_skills = st.multiselect(
        "기술 스택", [s["skill"] for s in skill_opts],
        placeholder="기술 스택 선택",
        key="data_skills", label_visibility="collapsed",
    )

st.divider()

# ── 데이터 조회 ───────────────────────────────
if is_demo:
    jobs   = get_demo_jobs(source=source_filter,
                           skill=sel_skills[0] if sel_skills else None,
                           keyword=keyword or None)
    stats  = get_demo_stats()
    skills = get_demo_skills()
else:
    jobs   = db["get_active_jobs"](source=source_filter, limit=200)
    stats  = db["get_stats"]()
    skills = db["get_top_skills"](15)

# 키워드 필터 (대소문자 무시)
if keyword:
    kw   = keyword.lower()
    jobs = [j for j in jobs
            if kw in j.get("title", "").lower()
            or any(kw in s.lower() for s in (j.get("skills") or []))]

# 스킬 필터 (대소문자 무시)
if sel_skills:
    sel_lower = [s.lower() for s in sel_skills]
    jobs = [j for j in jobs
            if all(any(sl == sk.lower() for sk in (j.get("skills") or []))
                   for sl in sel_lower)]

# 정렬
def _dl_key(j):
    dl = j.get("deadline", "") or j.get("end_date", "")
    m  = re.search(r"D-(\d+)", dl)
    if m: return int(m.group(1))
    dm = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", dl)
    if dm: return int(dm.group(1))*10000+int(dm.group(2))*100+int(dm.group(3))
    return 99999

if   sort_opt == "collected_at_desc": jobs = sorted(jobs, key=lambda j: str(j.get("collected_at") or ""), reverse=True)
elif sort_opt == "posted_at_desc":    jobs = sorted(jobs, key=lambda j: j.get("posted_at") or "", reverse=True)
elif sort_opt == "posted_at_asc":     jobs = sorted(jobs, key=lambda j: j.get("posted_at") or "")
elif sort_opt == "deadline_asc":      jobs = sorted(jobs, key=_dl_key)

by_src    = {r["source"]: r for r in stats.get("by_source", [])}
total_act = sum(r.get("active", 0) for r in by_src.values())

# ── 상단 지표 ─────────────────────────────────
cols = st.columns(5)
for col, (label, val) in zip(cols, [
    ("전체 모집중", total_act),
    ("SISM",    by_src.get("sism",    {}).get("active", 0)),
    ("OKKY",    by_src.get("okky",    {}).get("active", 0)),
    ("프리모아", by_src.get("freemoa", {}).get("active", 0)),
    ("크몽",    by_src.get("kmong",   {}).get("active", 0)),
]):
    with col:
        st.metric(label, f"{val:,}건")

st.divider()

# ── 인기 스킬 차트 ────────────────────────────
with st.expander("📊 인기 기술 스택 TOP 10", expanded=False):
    if skills:
        df_sk = pd.DataFrame(skills[:10]).sort_values("job_count", ascending=True)
        st.bar_chart(pd.DataFrame({"공고 수": df_sk["job_count"].values},
                                   index=df_sk["skill"].values),
                     horizontal=True, height=280)

# ── 공고 목록 ─────────────────────────────────
sort_lbl = {
    "collected_at_desc": "수집일 최신순",
    "posted_at_desc":    "등록일 최신순",
    "posted_at_asc":     "등록일 오래된순",
    "deadline_asc":      "마감 임박순",
}.get(sort_opt, "")

st.markdown(
    f"### 📋 공고 목록 ({len(jobs)}건) "
    f"<span style='font-size:13px;color:#888;font-weight:400'>· {sort_lbl}</span>",
    unsafe_allow_html=True,
)

if not jobs:
    st.info("조건에 맞는 공고가 없어요.")
else:
    PAGE_SIZE   = 10
    total_pages = max(1, (len(jobs) - 1) // PAGE_SIZE + 1)
    pg_col, _   = st.columns([2, 8])
    with pg_col:
        page = st.number_input(
            "페이지", min_value=1, max_value=total_pages,
            value=1, step=1, label_visibility="collapsed",
        )
    st.caption(f"{page} / {total_pages} 페이지")

    for job in jobs[(page-1)*PAGE_SIZE: page*PAGE_SIZE]:
        skills_  = job.get("skills") or []
        budget   = job.get("budget", "")
        deadline = job.get("deadline", "")
        duration = job.get("project_duration", "")
        location = job.get("location", "")
        work_type= job.get("work_type", "")
        end_date = job.get("end_date", "")
        url      = job.get("url", "#")
        source   = job.get("source", "")
        start_d  = job.get("start_date", "")

        d_m       = re.search(r"D-(\d+)", deadline)
        is_urgent = d_m and int(d_m.group(1)) <= 7
        dl_color  = "#E24B4A" if is_urgent else "#1D9E75"
        dl_disp   = deadline or end_date or ""

        if isinstance(skills_, str):
            skills_ = [s.strip() for s in skills_.split(",") if s.strip()]

        skill_tags = "".join(
            f'<span style="background:#f0f0f0;border-radius:4px;'
            f'padding:1px 7px;font-size:11px;margin:2px 2px 0 0;'
            f'display:inline-block">{s}</span>'
            for s in skills_[:8]
        )
        meta_str = " · ".join(x for x in [location, work_type, duration, budget] if x)

        st.markdown(
            f'<div style="border:1px solid rgba(0,0,0,0.08);border-radius:10px;'
            f'padding:16px 18px;margin-bottom:10px">'
            f'<div style="display:flex;justify-content:space-between;align-items:flex-start">'
            f'<div style="flex:1;min-width:0">'
            f'{source_badge(source)}'
            f'<div style="font-size:15px;font-weight:600;margin:6px 0 4px;'
            f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
            f'<a href="{url}" target="_blank" style="text-decoration:none;color:inherit">'
            f'{job.get("title","")}</a></div>'
            f'<div style="font-size:12px;color:#888;margin:2px 0">{meta_str}</div>'
            f'<div style="margin-top:6px">{skill_tags}</div>'
            f'</div>'
            f'<div style="text-align:right;white-space:nowrap;margin-left:12px">'
            f'<div style="font-size:14px;color:{dl_color};font-weight:500">{dl_disp}</div>'
            f'<div style="font-size:11px;color:#aaa;margin-top:4px">{start_d}</div>'
            f'</div></div></div>',
            unsafe_allow_html=True,
        )