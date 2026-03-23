"""
로그 뷰어 페이지
"""

import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="로그 뷰어",
    page_icon="📄",
    layout="wide",
)

ROOT_DIR = Path(__file__).parent.parent
LOG_DIR  = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_log_files() -> list[Path]:
    return sorted(LOG_DIR.glob("app_*.log"), reverse=True)


def read_log(path: Path, last_n: int = 500) -> list[str]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            return f.readlines()[-last_n:]
    except Exception as e:
        return [f"[오류] {e}"]


def colorize(line: str) -> tuple[str, str]:
    u = line.upper()
    if "[ERROR]"   in u: return "#E24B4A", "🔴"
    if "[WARNING]" in u: return "#BA7517", "🟡"
    if "[DEBUG]"   in u: return "#888888", "⚪"
    if "[INFO]"    in u: return "var(--color-text-primary)", "🔵"
    return "var(--color-text-secondary)", "⚪"


def parse_stats(lines: list[str]) -> dict:
    stats = {"total": 0, "info": 0, "warning": 0, "error": 0, "saved": 0}
    for l in lines:
        stats["total"] += 1
        u = l.upper()
        if   "[ERROR]"   in u: stats["error"]   += 1
        elif "[WARNING]" in u: stats["warning"] += 1
        elif "[INFO]"    in u: stats["info"]    += 1
        if "저장:" in l or "저장 완료" in l: stats["saved"] += 1
    return stats


# ── 헤더 ──────────────────────────────────────
st.markdown("# 📄 로그 뷰어")
st.caption("수집 엔진 실행 로그를 확인하세요.")
st.divider()

log_files = get_log_files()

if not log_files:
    st.info("아직 로그 파일이 없어요. 수집을 한 번 실행하면 `logs/` 폴더에 생성돼요.")
    st.stop()

# ── 사이드바 ──────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ 설정")

    selected_file = st.selectbox(
        "로그 파일",
        options=log_files,
        format_func=lambda f: f"{f.name} ({f.stat().st_size // 1024} KB)",
    )

    last_n = st.select_slider(
        "표시 줄 수",
        options=[100, 200, 500, 1000, 2000],
        value=500,
    )

    st.markdown("**레벨 필터**")
    show_info    = st.checkbox("INFO",    value=True)
    show_warning = st.checkbox("WARNING", value=True)
    show_error   = st.checkbox("ERROR",   value=True)
    show_debug   = st.checkbox("DEBUG",   value=False)

    keyword = st.text_input("키워드 검색", placeholder="예: sism, 저장, ERROR")

    st.divider()
    auto_refresh = st.toggle("자동 갱신 (3초)", value=False)

    st.divider()
    if st.button("🗑️ 현재 파일 삭제", use_container_width=True):
        os.remove(selected_file)
        st.toast(f"{selected_file.name} 삭제 완료", icon="🗑️")
        st.rerun()

    if st.button("🗑️ 전체 로그 삭제", use_container_width=True, type="secondary"):
        for f in log_files:
            os.remove(f)
        st.toast("전체 로그 삭제 완료", icon="🗑️")
        st.rerun()


# ── 로그 읽기 + 필터 ─────────────────────────
@st.cache_data(ttl=3 if auto_refresh else 60)
def load_lines(path: str, n: int) -> list[str]:
    return read_log(Path(path), n)

lines    = load_lines(str(selected_file), last_n)
filtered = []
for line in lines:
    u = line.upper()
    if not show_debug   and "[DEBUG]"   in u: continue
    if not show_info    and "[INFO]"    in u: continue
    if not show_warning and "[WARNING]" in u: continue
    if not show_error   and "[ERROR]"   in u: continue
    if keyword and keyword.lower() not in line.lower(): continue
    filtered.append(line)

# ── 통계 지표 ─────────────────────────────────
stats = parse_stats(filtered)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("전체",     stats["total"])
c2.metric("INFO",     stats["info"])
c3.metric("WARNING",  stats["warning"],
          delta=f"-{stats['warning']}" if stats["warning"] else None,
          delta_color="inverse")
c4.metric("ERROR",    stats["error"],
          delta=f"-{stats['error']}" if stats["error"] else None,
          delta_color="inverse")
c5.metric("저장 건수", stats["saved"])
st.divider()

# ── 탭 ───────────────────────────────────────
tab_color, tab_raw, tab_chart = st.tabs(["🎨 컬러 뷰", "📝 원본", "📊 통계 차트"])

with tab_color:
    if not filtered:
        st.info("조건에 맞는 로그가 없어요.")
    else:
        rows_html = ""
        for line in reversed(filtered):
            color, icon = colorize(line)
            escaped = line.rstrip().replace("<", "&lt;").replace(">", "&gt;")
            rows_html += (
                f'<div style="padding:2px 6px;'
                f'border-bottom:1px solid rgba(0,0,0,0.04);'
                f'font-family:monospace;font-size:12px;'
                f'line-height:1.7;color:{color}">'
                f'{icon} {escaped}</div>'
            )
        st.markdown(
            f'<div style="height:600px;overflow-y:auto;'
            f'border:1px solid rgba(0,0,0,0.08);border-radius:8px;'
            f'background:rgba(0,0,0,0.02)">'
            f'{rows_html}</div>',
            unsafe_allow_html=True,
        )
        st.caption(
            f"총 {len(filtered)}줄 · 최신 순 · {datetime.now():%H:%M:%S} 기준"
        )

with tab_raw:
    st.code("".join(filtered), language="text")

with tab_chart:
    time_data: dict[str, int] = {}
    level_data = {"INFO": 0, "WARNING": 0, "ERROR": 0, "DEBUG": 0}

    for line in filtered:
        t_m = re.search(r"(\d{2}:\d{2}):\d{2}", line)
        if t_m:
            key = t_m.group(1)
            time_data[key] = time_data.get(key, 0) + 1
        u = line.upper()
        for lv in level_data:
            if f"[{lv}]" in u:
                level_data[lv] += 1
                break

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**레벨별 분포**")
        if any(level_data.values()):
            st.bar_chart(pd.DataFrame(
                {"건수": list(level_data.values())},
                index=list(level_data.keys()),
            ))
    with col_b:
        st.markdown("**시간대별 건수**")
        if time_data:
            df_t = pd.DataFrame(
                {"건수": list(time_data.values())},
                index=list(time_data.keys()),
            ).sort_index()
            st.line_chart(df_t)

# ── 자동 갱신 ─────────────────────────────────
if auto_refresh:
    import time
    time.sleep(3)
    st.cache_data.clear()
    st.rerun()