"""
시스템 정보 & 연동 상태 페이지
- DB / Redis 연결 상태 및 상세 정보
- 소스별 수집 통계
- 수집 제어 (시작 / 중지 / 모니터링)
- 수집 로그 실시간 확인
"""

import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(
    page_title="시스템 상태",
    page_icon="🖥️",
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

# ── session_state 초기화 ──────────────────────
for k, v in [
    ("collect_proc", None), ("collect_logs", []),
    ("collect_running", False), ("collect_source", "전체"),
]:
    if k not in st.session_state:
        st.session_state[k] = v


# ── 연결 상태 체크 ────────────────────────────
@st.cache_data(ttl=10)
def check_connections() -> dict:
    result = {}

    # PostgreSQL
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
                "status": "connected",
                "total":  row["total"],
                "active": row["active"],
                "url":    DB_URL.split("@")[-1] if "@" in DB_URL else DB_URL,
            }
    except Exception as e:
        result["db"] = {"status": "error", "error": str(e)}

    # Redis
    try:
        from settings import REDIS_URL
        import redis as _redis
        client = _redis.from_url(REDIS_URL, socket_connect_timeout=3, decode_responses=True)
        client.ping()
        info = client.info("memory")
        result["redis"] = {
            "status":     "connected",
            "url":        REDIS_URL.split("@")[-1] if "@" in REDIS_URL else REDIS_URL,
            "hash_count": client.scard("jobs:hashes"),
            "used_memory": info.get("used_memory_human", ""),
        }
    except Exception as e:
        result["redis"] = {"status": "error", "error": str(e)}

    return result


@st.cache_data(ttl=10)
def get_stats() -> dict:
    try:
        from db import get_stats as _get_stats
        return _get_stats()
    except Exception:
        return {}


@st.cache_data(ttl=10)
def get_collect_meta() -> dict:
    try:
        from cache import meta
        return meta.get_all()
    except Exception:
        return {}


# ── 헤더 ─────────────────────────────────────
st.markdown("# 🖥️ 시스템 상태")
st.caption(f"{datetime.now():%Y-%m-%d %H:%M:%S} 기준")
st.divider()


# ── 1. 연결 상태 ──────────────────────────────
conn     = check_connections()
db_info  = conn.get("db",    {})
red_info = conn.get("redis", {})
db_ok    = db_info.get("status")  == "connected"
red_ok   = red_info.get("status") == "connected"

st.markdown("## 🔌 연결 상태")
col_db, col_redis = st.columns(2)

with col_db:
    st.markdown(
        f'<div style="border:1px solid {"rgba(29,158,117,0.4)" if db_ok else "rgba(226,75,74,0.4)"};'
        f'border-radius:10px;padding:16px 20px">'
        f'<div style="font-size:15px;font-weight:500;margin-bottom:8px">'
        f'{"🟢" if db_ok else "🔴"} PostgreSQL</div>',
        unsafe_allow_html=True,
    )
    if db_ok:
        st.success("연결됨")
        st.caption(f"호스트: `{db_info.get('url','')}`")
        m1, m2 = st.columns(2)
        m1.metric("전체 공고",  f"{db_info.get('total',  0):,}건")
        m2.metric("모집중 공고", f"{db_info.get('active', 0):,}건")
    else:
        st.error("연결 실패")
        st.code(db_info.get("error", ""), language="text")
        st.caption("settings.py의 `DB_URL` 확인")
    st.markdown("</div>", unsafe_allow_html=True)

with col_redis:
    st.markdown(
        f'<div style="border:1px solid {"rgba(29,158,117,0.4)" if red_ok else "rgba(186,117,23,0.4)"};'
        f'border-radius:10px;padding:16px 20px">'
        f'<div style="font-size:15px;font-weight:500;margin-bottom:8px">'
        f'{"🟢" if red_ok else "🟡"} Redis</div>',
        unsafe_allow_html=True,
    )
    if red_ok:
        st.success("연결됨")
        st.caption(f"호스트: `{red_info.get('url','')}`")
        m1, m2 = st.columns(2)
        m1.metric("캐시 해시",   f"{red_info.get('hash_count', 0):,}개")
        m2.metric("메모리 사용", red_info.get("used_memory", "-"))
        
        if st.button("🗑️ Redis 캐시 전체 삭제", help="중복 체크용 해시를 모두 삭제하여 다시 수집할 수 있게 합니다."):
            try:
                from cache import RedisFilter
                RedisFilter().clear_all()
                st.success("Redis 캐시가 초기화되었습니다.")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"캐시 삭제 중 오류 발생: {e}")
    else:
        st.warning("연결 실패 (선택 사항)")
        st.code(red_info.get("error", ""), language="text")
        st.caption("Redis 없이도 인메모리 캐시로 동작해요.")
    st.markdown("</div>", unsafe_allow_html=True)

if st.button("🔄 연결 상태 새로고침"):
    st.cache_data.clear()
    st.rerun()

st.divider()


# ── 2. 소스별 수집 통계 ───────────────────────
st.markdown("## 📊 소스별 수집 통계")

stats    = get_stats()
by_src   = {r["source"]: r for r in stats.get("by_source", [])}
col_meta = get_collect_meta()

if by_src:
    # 지표 카드
    srcs = ["sism", "okky", "freemoa", "kmong", "elancer"]
    cols = st.columns(len(srcs))
    for col, src in zip(cols, srcs):
        data  = by_src.get(src, {})
        color = SOURCE_COLORS.get(src, "#888")
        label = SOURCE_LABELS.get(src, src)
        total  = data.get("total",  0)
        active = data.get("active", 0)
        last   = data.get("last_collected", "")
        if hasattr(last, "strftime"):
            last = last.strftime("%m/%d %H:%M")
        with col:
            st.markdown(
                f'<div style="border:1px solid {color}44;border-top:3px solid {color};'
                f'border-radius:8px;padding:14px 16px;text-align:center">'
                f'<div style="font-size:13px;font-weight:500;color:{color}">{label}</div>'
                f'<div style="font-size:26px;font-weight:700;margin:6px 0">{active:,}</div>'
                f'<div style="font-size:11px;color:#888">모집중 / 전체 {total:,}건</div>'
                f'<div style="font-size:10px;color:#aaa;margin-top:4px">{last}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # 상세 테이블
    import pandas as pd
    rows = []
    for src in srcs:
        d = by_src.get(src, {})
        m = col_meta.get(src, {})
        last = d.get("last_collected", "")
        if hasattr(last, "strftime"):
            last = last.strftime("%Y-%m-%d %H:%M")
        rows.append({
            "출처":          SOURCE_LABELS.get(src, src),
            "전체":          d.get("total",  0),
            "모집중":        d.get("active", 0),
            "마감":          d.get("closed", 0),
            "마지막 수집":   last,
            "수집 상태":     m.get("status",    "-"),
            "마지막 페이지": m.get("last_page", "-"),
            "수집 건수":     m.get("count",     "-"),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("수집 데이터 없음 — 수집을 실행하면 통계가 표시돼요.")

st.divider()


# ── 3. 수집 제어 ──────────────────────────────
st.markdown("## ⚙️ 수집 제어")

ctrl_col1, ctrl_col2, ctrl_col3 = st.columns([2, 1, 1])

with ctrl_col1:
    source_opt = st.selectbox(
        "수집 소스",
        options=["전체", "sism", "okky", "freemoa", "kmong", "elancer"],
        format_func=lambda x: SOURCE_LABELS.get(x, x),
        key="sys_source_select",
    )

with ctrl_col2:
    run_clicked = st.button(
        "▶ 수집 시작",
        use_container_width=True,
        disabled=st.session_state["collect_running"],
        type="primary",
    )

with ctrl_col3:
    stop_clicked = st.button(
        "■ 중지",
        use_container_width=True,
        disabled=not st.session_state["collect_running"],
    )

if run_clicked:
    cmd = [
        sys.executable, "-u",
        str(Path(__file__).parent.parent / "engine.py"),
    ]
    if source_opt != "전체":
        cmd += ["--source", source_opt]
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=0,
        encoding="utf-8",
        errors="replace",
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    st.session_state["collect_proc"]    = proc
    st.session_state["collect_logs"]    = []
    st.session_state["collect_running"] = True
    st.session_state["collect_source"]  = source_opt
    st.toast(f"▶ [{source_opt}] 수집 시작!", icon="🚀")
    st.rerun()

if stop_clicked:
    proc = st.session_state.get("collect_proc")
    if proc and proc.poll() is None:
        proc.terminate()
        st.session_state["collect_logs"].append("⛔ 수집 중지됨")
    st.session_state["collect_running"] = False
    st.toast("수집이 중지됐어요.", icon="⛔")
    st.rerun()

st.divider()


# ── 4. 수집 모니터링 (2초 갱신) ───────────────
@st.fragment(run_every=2)
def monitor():
    proc    = st.session_state.get("collect_proc")
    running = st.session_state.get("collect_running", False)
    logs    = st.session_state.get("collect_logs", [])

    # stdout 읽기
    if proc and proc.poll() is None:
        try:
            import msvcrt
            buf = ""
            for _ in range(200):
                try:
                    chunk = os.read(proc.stdout.fileno(), 4096)
                    if not chunk:
                        break
                    buf += chunk.decode("utf-8", errors="replace")
                except (BlockingIOError, OSError):
                    break
            for line in buf.splitlines():
                line = line.rstrip()
                if line:
                    logs.append(line)
            if buf:
                st.session_state["collect_logs"] = logs[-300:]
        except (ImportError, Exception):
            try:
                import select
                while select.select([proc.stdout], [], [], 0)[0]:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    line = line.rstrip()
                    if line:
                        logs.append(line)
                st.session_state["collect_logs"] = logs[-300:]
            except Exception:
                pass

    elif proc and proc.poll() is not None and running:
        st.session_state["collect_running"] = False
        logs.append(f"✅ 수집 완료 (종료 코드: {proc.returncode})")
        st.session_state["collect_logs"] = logs
        st.cache_data.clear()

    if not logs and not running:
        st.info("수집 시작 버튼을 누르면 여기에 로그가 실시간으로 표시돼요.")
        return

    # 상태 배너
    saved = sum(1 for l in logs if "저장:" in l)
    errs  = sum(1 for l in logs if "ERROR" in l)
    color = "#1D9E75" if running else "#888"
    icon  = "🔄" if running else "✅"
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:10px;'
        f'padding:10px 16px;border-radius:8px;margin-bottom:12px;'
        f'background:rgba(0,0,0,0.03);border:1px solid rgba(0,0,0,0.07)">'
        f'<span style="font-size:18px">{icon}</span>'
        f'<span style="color:{color};font-weight:500">{"수집 진행 중" if running else "수집 완료"}</span>'
        f'<span style="color:#888;font-size:12px;margin-left:auto">'
        f'저장 {saved}건 · 오류 {errs}건 · {datetime.now():%H:%M:%S}</span>'
        f'</div>'
        f'<style>@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}</style>',
        unsafe_allow_html=True,
    )

    # 로그 박스 (좌) + 신규 데이터 (우)
    col_log, col_data = st.columns([1, 1], gap="medium")

    with col_log:
        st.markdown("**📋 수집 로그**")
        colored = []
        for line in reversed(logs[-80:]):
            esc = line.replace("<", "&lt;").replace(">", "&gt;")
            if "ERROR"   in line or "오류"   in line: c = "#E24B4A"
            elif "WARNING" in line or "경고" in line: c = "#BA7517"
            elif "✅" in line or "저장:" in line:      c = "#1D9E75"
            elif "page=" in line or "수집 중" in line: c = "#378ADD"
            else:                                       c = "var(--color-text-primary,#333)"
            colored.append(f'<span style="color:{c}">{esc}</span>')

        st.markdown(
            '<div style="height:420px;overflow-y:auto;'
            'background:rgba(0,0,0,0.03);border:1px solid rgba(0,0,0,0.08);'
            'border-radius:8px;padding:10px 14px;'
            'font-family:monospace;font-size:11px;line-height:1.9">'
            + "<br>".join(colored) +
            '</div>',
            unsafe_allow_html=True,
        )
        if st.button("🗑️ 로그 지우기", key="sys_clear_logs"):
            st.session_state["collect_logs"] = []
            st.rerun()

    with col_data:
        st.markdown("**🟢 실시간 수집 데이터**")
        new_jobs = []
        try:
            from db import get_session
            from sqlalchemy import text as _text
            with get_session() as session:
                rows = session.execute(_text("""
                    SELECT source, title, skills, budget,
                           deadline, work_type, location,
                           url, collected_at
                    FROM jobs
                    WHERE collected_at >= NOW() - INTERVAL '30 minutes'
                    ORDER BY collected_at DESC LIMIT 30
                """)).mappings().all()
                new_jobs = [dict(r) for r in rows]
        except Exception:
            pass

        if not new_jobs:
            st.caption("수집이 시작되면 여기에 데이터가 표시돼요.")
        else:
            st.caption(f"최근 30분 내 {len(new_jobs)}건 · {datetime.now():%H:%M:%S}")
            cards = ""
            for job in new_jobs:
                src    = job.get("source", "")
                title  = job.get("title",  "")
                url    = job.get("url",    "#")
                skills = job.get("skills") or []
                budget = job.get("budget", "")
                dl     = job.get("deadline", "")
                loc    = job.get("location", "")
                cat    = job.get("collected_at", "")
                color  = SOURCE_COLORS.get(src, "#888")
                label  = SOURCE_LABELS.get(src, src)
                cat_str = cat.strftime("%H:%M:%S") if hasattr(cat, "strftime") else str(cat)[11:19]
                if isinstance(skills, str):
                    skills = [s.strip() for s in skills.split(",") if s.strip()]
                skill_tags = "".join(
                    f'<span style="background:#f0f0f0;border-radius:3px;'
                    f'padding:1px 5px;font-size:10px;margin:1px">{s}</span>'
                    for s in skills[:4]
                )
                meta = " · ".join(x for x in [loc, budget, dl] if x)
                badge = (
                    f'<span style="background:{color}22;color:{color};'
                    f'border:1px solid {color}55;border-radius:3px;'
                    f'padding:1px 5px;font-size:10px">{label}</span>'
                )
                cards += (
                    f'<div style="border:1px solid rgba(0,0,0,0.07);'
                    f'border-left:3px solid {color};border-radius:7px;'
                    f'padding:9px 12px;margin-bottom:7px">'
                    f'<div style="display:flex;justify-content:space-between">'
                    f'<div style="flex:1;min-width:0">'
                    f'{badge}'
                    f'<div style="font-size:12px;font-weight:600;margin:3px 0 1px;'
                    f'overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'
                    f'<a href="{url}" target="_blank" style="text-decoration:none;color:inherit">'
                    f'{title}</a></div>'
                    f'<div style="font-size:11px;color:#888">{meta}</div>'
                    f'<div style="margin-top:3px">{skill_tags}</div>'
                    f'</div>'
                    f'<div style="font-size:10px;color:#1D9E75;margin-left:8px;flex-shrink:0">'
                    f'{cat_str}</div>'
                    f'</div></div>'
                )
            st.markdown(
                f'<div style="height:420px;overflow-y:auto">{cards}</div>',
                unsafe_allow_html=True,
            )

monitor()