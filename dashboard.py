"""
IT 프로젝트 공고 수집기 — 진입점
pages/ 폴더의 멀티페이지로 자동 이동
"""
import streamlit as st

st.set_page_config(
    page_title="IT 프로젝트 공고 수집기",
    page_icon="💼",
    layout="wide",
)

st.switch_page("pages/3_data.py")