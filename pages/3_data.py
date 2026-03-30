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
    "freemoa": "프리모아", "kmong": "크몽",
    "elancer": "이랜서", "all": "전체",
}
SOURCE_COLORS = {
    "sism": "#378ADD", "okky": "#1D9E75",
    "freemoa": "#D85A30", "kmong": "#BA7517",
    "elancer": "#0066CC",
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

# ── 기초 데이터 조회 (필터 UI 전에 스킬 목록 필요) ────────────
skill_opts = get_demo_skills() if is_demo else db["get_top_skills"](50)

# ── 상단 지표 & 인기 스킬 ─────────────────────────────────────
if is_demo:
    stats  = get_demo_stats()
    skills = get_demo_skills()
else:
    stats  = db["get_stats"]()
    skills = db["get_top_skills"](15)

by_src    = {r["source"]: r for r in stats.get("by_source", [])}
total_act = sum(r.get("active", 0) for r in by_src.values())

cols = st.columns(6)
for col, (label, val) in zip(cols, [
    ("전체 모집중", total_act),
    ("SISM",    by_src.get("sism",    {}).get("active", 0)),
    ("OKKY",    by_src.get("okky",    {}).get("active", 0)),
    ("프리모아", by_src.get("freemoa", {}).get("active", 0)),
    ("크몽",    by_src.get("kmong",   {}).get("active", 0)),
    ("이랜서",  by_src.get("elancer", {}).get("active", 0)),
]):
    with col:
        st.metric(label, f"{val:,}건")

with st.expander("📊 인기 기술 스택 TOP 10", expanded=False):
    if skills:
        df_sk = pd.DataFrame(skills[:10]).sort_values("job_count", ascending=True)
        st.bar_chart(pd.DataFrame({"공고 수": df_sk["job_count"].values},
                                   index=df_sk["skill"].values),
                     horizontal=True, height=280)

st.divider()

# ── 검색 조건 ─────────────────────────────────
f1, f2, f3, f4 = st.columns([2, 2, 3, 2])

with f1:
    sel_source = st.selectbox(
        "출처", ["all", "sism", "okky", "freemoa", "kmong", "elancer"],
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
    sel_skills = st.multiselect(
        "기술 스택", [s["skill"] for s in skill_opts],
        placeholder="기술 스택 선택",
        key="data_skills", label_visibility="collapsed",
    )

# ── 데이터 조회 & 필터 적용 ───────────────────
if is_demo:
    jobs = get_demo_jobs(source=source_filter,
                         skill=sel_skills[0] if sel_skills else None,
                         keyword=keyword or None)
else:
    jobs = db["get_active_jobs"](source=source_filter, limit=9999)

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

# 무한 스크롤(Load More)을 위한 세션 상태 초기화
if "items_to_show" not in st.session_state:
    st.session_state.items_to_show = 15

# 필터가 바뀌면 다시 처음부터 보여주기 위해 키 조합 생성
current_filter_hash = f"{sel_source}_{sort_opt}_{keyword}_{sel_skills}"
if "last_filter_hash" not in st.session_state or st.session_state.last_filter_hash != current_filter_hash:
    st.session_state.items_to_show = 15
    st.session_state.last_filter_hash = current_filter_hash

if not jobs:
    st.info("조건에 맞는 공고가 없어요.")
else:
    # 지정된 개수만큼만 슬라이싱하여 표시
    visible_jobs = jobs[:st.session_state.items_to_show]

    for job in visible_jobs:
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
            f'<span style="background:rgba(38, 39, 48, 0.2);border-radius:4px;'
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

    # 자동 무한 스크롤 트리거 (버튼 숨김 + 모던 로더)
    if len(jobs) > st.session_state.items_to_show:
        # 1. 최신 트렌드에 맞는 심플한 로더 CSS
        st.markdown(
            """
            <style>
                /* 전체 로더 컨테이너 */
                .modern-loader-container {
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    justify-content: center;
                    padding: 40px 0;
                    gap: 12px;
                }
                
                /* 심플한 진행바 스타일 로더 */
                .modern-progress-bar {
                    width: 120px;
                    height: 4px;
                    background-color: rgba(29, 158, 117, 0.1);
                    border-radius: 10px;
                    position: relative;
                    overflow: hidden;
                }
                
                .modern-progress-bar::after {
                    content: "";
                    position: absolute;
                    top: 0;
                    left: -50%;
                    width: 50%;
                    height: 100%;
                    background: linear-gradient(90deg, transparent, #1D9E75, transparent);
                    animation: loading-slide 1.5s infinite ease-in-out;
                    border-radius: 10px;
                }
                
                @keyframes loading-slide {
                    0% { left: -50%; }
                    100% { left: 100%; }
                }
                
                /* 은은한 텍스트 스타일 */
                .loader-text {
                    font-size: 13px;
                    color: #999;
                    letter-spacing: -0.01em;
                    font-weight: 400;
                }

                /* 숨겨진 버튼 영역 */
                .hidden-btn-area {
                    height: 0;
                    overflow: hidden;
                    opacity: 0;
                    pointer-events: none;
                }
            </style>
            """,
            unsafe_allow_html=True
        )

        # 2. 모던 로더 표시
        st.markdown(
            """
            <div class="modern-loader-container">
                <div class="modern-progress-bar"></div>
                <div class="loader-text">더 많은 프로젝트를 가져오고 있습니다</div>
            </div>
            """,
            unsafe_allow_html=True
        )

        # 3. 실제 동작을 위한 숨겨진 버튼 (영역 자체를 숨김)
        if st.button("TRIGGER_LOAD_MORE", key="hidden_load_more_btn"):
            st.session_state.items_to_show += 15
            st.rerun()

        # 4. JS IntersectionObserver 트리거 및 버튼 숨김 처리
        st.components.v1.html(
            """
            <div id="scroll-trigger" style="height: 20px; width: 100%;"></div>
            <script>
                let isTriggering = false;
                
                // Streamlit 부모 문서에 접근
                const parentDoc = window.parent.document;
                
                // 'TRIGGER_LOAD_MORE' 텍스트를 가진 버튼 찾기
                const buttons = Array.from(parentDoc.querySelectorAll('button'));
                const loadMoreBtn = buttons.find(btn => btn.textContent.includes('TRIGGER_LOAD_MORE'));
                
                // 화면에서 버튼 영역 자체를 완전히 숨김
                if (loadMoreBtn) {
                    const btnContainer = loadMoreBtn.closest('div[data-testid="stElementContainer"]');
                    if (btnContainer) {
                        btnContainer.style.display = 'none'; // 여백까지 깔끔하게 숨김
                    } else {
                        loadMoreBtn.style.display = 'none';
                    }
                }
                
                function triggerLoadMore() {
                    if (isTriggering || !loadMoreBtn) return;
                    
                    try {
                        isTriggering = true;
                        console.log("Auto-loading more items...");
                        loadMoreBtn.click(); // 숨겨진 버튼 클릭 이벤트 강제 발생
                        
                        // 리런(rerun) 대기 시간 동안 중복 방지
                        setTimeout(() => { isTriggering = false; }, 5000);
                    } catch (e) {
                        console.error("Infinite scroll error:", e);
                    }
                }

                const observer = new IntersectionObserver((entries) => {
                    if (entries[0].isIntersecting && !isTriggering) {
                        triggerLoadMore();
                    }
                }, { 
                    root: null,
                    rootMargin: '400px', // 사용자가 바닥에 도달하기 전 미리 로드
                    threshold: 0.1 
                });

                const trigger = document.getElementById('scroll-trigger');
                if (trigger) {
                    observer.observe(trigger);
                }
            </script>
            """,
            height=20,
        )
    elif len(jobs) > 0:
        st.divider()
        st.info("✅ 모든 공고를 다 불러왔습니다.")