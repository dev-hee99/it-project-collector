"""
이랜서(elancer) 파서
- 상주: POST api/pjt/get_list  prjctTrnkyYN=N
- 재택: POST api/pjt/get_list  prjctTrnkyYN=Y
- 페이지네이션: page / limit 파라미터
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, date, timedelta
from typing import Any

import requests
import urllib3

# 상위 디렉토리 임포트 허용 (logger.py 등)
import sys
import os
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from logger import get_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = get_logger(__name__)

# ──────────────────────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────────────────────
API_URL    = "https://api.elancer.co.kr/api/pjt/get_list"
DETAIL_URL = "https://www.elancer.co.kr/project_detail/{puno}"

PAGE_LIMIT = 10          # 한 번에 가져올 건수
MAX_PAGES  = 20          # 안전 상한

# PJTState 비트 중 '모집중' 여부 — 하위 비트 0 이면 모집 중
_STATE_CLOSED_BIT = 0b10  # 2번째 비트가 1이면 마감

# occupation 코드 목록 (개발자 + 디자이너 + 기획자)
OCCUPATIONS = [
    #  개발자만 수집
    ("E030001", "개발자"), 
    # ("E010001", "디자이너"),
    # ("E020001", "기획자"),
]

# 기본 헤더
HEADERS = {
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Content-Type": "application/json;charset=UTF-8",
    "Accept":       "application/json, text/plain, */*",
    "Origin":       "https://www.elancer.co.kr",
    "Referer":      "https://www.elancer.co.kr/",
}


# ──────────────────────────────────────────────────────────────
# 유틸
# ──────────────────────────────────────────────────────────────
def _today() -> date:
    return datetime.now().date()


def _parse_date(raw: str) -> str:
    """ISO datetime 또는 날짜 문자열 → YYYY-MM-DD"""
    if not raw:
        return ""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else ""


def _deadline_str(bidedate: str) -> str:
    """마감일 → 'D-N' 또는 '마감' 문자열"""
    dd = _parse_date(bidedate)
    if not dd:
        return ""
    try:
        diff = (date.fromisoformat(dd) - _today()).days
        if diff < 0:
            return "마감"
        if diff == 0:
            return "D-Day"
        return f"D-{diff}"
    except Exception:
        return dd


def _duration_str(worksdate: str, workedate: str) -> str:
    """시작일~종료일 → '약 N개월' 또는 'N일'"""
    s = _parse_date(worksdate)
    e = _parse_date(workedate)
    if not (s and e):
        return ""
    try:
        diff = (date.fromisoformat(e) - date.fromisoformat(s)).days
        if diff >= 28:
            months = round(diff / 30)
            return f"{months}개월"
        return f"{diff}일"
    except Exception:
        return ""


def _budget_str(item: dict) -> str:
    """minmoney/maxmoney + moneytype → 예산 문자열"""
    lo = int(item.get("minmoney") or 0)
    hi = int(item.get("maxmoney") or 0)
    mt = item.get("moneytype", "")      # B=가격제안 A=협의 C=확정
    mn = item.get("moneytype_name", "")

    if lo == 0 and hi == 0:
        return mn or "협의"
    if mt == "B":                        # 가격제안
        return mn or "가격제안"
    if lo and hi:
        return f"{lo:,}~{hi:,}만원/월"
    if hi:
        return f"{hi:,}만원/월"
    if lo:
        return f"{lo:,}만원/월~"
    return mn or ""


def _skills(item: dict) -> list[str]:
    """txt_keyword 콤마 구분 → 리스트"""
    raw = item.get("txt_keyword", "") or item.get("skill_add_description", "") or ""
    parts = [s.strip() for s in re.split(r"[,\n]", raw) if s.strip()]
    # 중복 제거 · 최대 15개
    seen, out = set(), []
    for p in parts:
        if p.lower() not in seen:
            seen.add(p.lower()); out.append(p)
    return out[:15]


def _location(item: dict) -> str:
    """jobplace 'A|B' → 'A B', 재택이면 '재택'"""
    code = item.get("code", "")           # '상주' | '재택' | '반상주'
    jp   = item.get("jobplace", "") or ""
    jp   = jp.replace("|", " ").strip()
    juso = item.get("juso", "") or ""
    loc  = jp or juso or ""
    if "재택" in code:
        return f"재택 {loc}".strip()
    if "반상주" in code:
        return f"반상주 {loc}".strip()
    return loc or code


def _is_open(state) -> bool:
    """PJTState로 모집 중 여부 판단"""
    # 1000000001 → 모집중, 1000000111 → 마감(지원완료)
    # 마지막 두 자리가 11 이면 마감으로 처리 (01 이면 모집중)
    try:
        s = str(int(state))
        if s.endswith("11") or s.endswith("111"):
            return False
        return s.endswith("01") or s.endswith("001")
    except Exception:
        return True


def _is_active_item(item: dict) -> bool:
    """모집중인 공고인지 종합 판단 (PJTState + 마감일)"""
    # 1. PJTState 로 마감 여부 확인
    if not _is_open(item.get("PJTState", 0)):
        return False
    
    # 2. 마감일(bidedate)로 마감 여부 한 번 더 확인
    bidedate = item.get("bidedate", "")
    if bidedate:
        dd = _parse_date(bidedate)
        if dd:
            try:
                # 오늘 이전이면 마감
                if (date.fromisoformat(dd) - _today()).days < 0:
                    return False
            except:
                pass
    
    return True


# ──────────────────────────────────────────────────────────────
# 단일 아이템 → 표준 공고 dict
# ──────────────────────────────────────────────────────────────
def _normalize(item: dict, work_type_label: str) -> dict:
    puno       = item.get("puno", "")
    projectkey = item.get("projectkey", "")
    bidedate   = _parse_date(item.get("bidedate", ""))
    worksdate  = _parse_date(item.get("worksdate", ""))
    workedate  = _parse_date(item.get("workedate", ""))
    wdate      = _parse_date(item.get("wdate", ""))

    status = "모집중" if _is_active_item(item) else "마감"

    return {
        "source":           "elancer",
        "source_id":        str(puno),
        "external_id":      str(puno),
        "url_hash":         hashlib.sha256(f"elancer:{puno}".encode()).hexdigest()[:16],
        "title":            item.get("name", "").strip(),
        "company":          "",                          # 비공개
        "category":         item.get("occupation_name", [{}])[0].get("name", "") if isinstance(item.get("occupation_name"), list) else "",
        "skills":           _skills(item),
        "budget":           _budget_str(item),
        "project_duration": _duration_str(worksdate, workedate),
        "start_date":       worksdate,
        "end_date":         workedate,
        "deadline":         _deadline_str(item.get("bidedate", "")),
        "deadline_date":    bidedate,
        "work_type":        item.get("code", work_type_label),   # 상주/재택/반상주
        "location":         _location(item),
        "grade":            item.get("pjt_grade_name", ""),       # 초급/중급/고급
        "career_min":       item.get("career1", 0),
        "career_max":       item.get("career2", 0),
        "field":            item.get("field", ""),
        "status":           status,
        "description":      (item.get("content", "") or "")[:500],
        "url":              DETAIL_URL.format(puno=puno),
        "posted_at":        wdate,
        "collected_at":     datetime.now(),
    }


# ──────────────────────────────────────────────────────────────
# API 호출
# ──────────────────────────────────────────────────────────────
def _fetch_page(
    session: requests.Session,
    trnky: str,          # "N"=상주, "Y"=재택
    occupation: str,
    page: int,
) -> dict[str, Any]:
    payload = {
        "page":         page,
        "limit":        PAGE_LIMIT,
        "occupation":   occupation,
        "skill":        "",
        "hopejobtype":  "",
        "grade":        "",
        "place":        "",
        "place2":       "",
        "searchTxt":    "",
        "orderBy":      "mdate",
        "PJTState":     0,
        "uformat":      "",
        "jobdate":      "",
        "prjctPrd":     0,
        "minmoney":     0,
        "maxmoney":     0,
        "moneychk":     "",
        "prjctTrnkyYN": trnky,
    }
    resp = session.post(
        API_URL, json=payload, headers=HEADERS,
        timeout=20, verify=False,
    )
    if resp.status_code != 200:
        log.error(f"[elancer] API error: {resp.status_code}, body: {resp.text[:500]}")
        resp.raise_for_status()

    try:
        return resp.json()
    except Exception as e:
        log.error(f"[elancer] JSON parsing error: {e}, body: {resp.text[:500]}")
        raise


def _fetch_all(trnky: str, label: str) -> list[dict]:
    session  = requests.Session()
    results  = []
    occ_code = OCCUPATIONS[0][0]          # 개발자만 (필요 시 루프 추가)

    total = 0;
    for occ_code, occ_name in OCCUPATIONS:
        page = 1
        while page <= MAX_PAGES:
            try:
                body  = _fetch_page(session, trnky, occ_code, page)
                data  = body.get("data", {})
                items = data.get("list", [])
                if(page == 1): # 전체 건수는 페이지 1에서 가져옴 그 이외 페이지에서는 0으로 줌.
                    total = int(data.get("total", 0))

            except Exception as e:
                log.error("[elancer] %s %s p%d 요청 실패: %s", label, occ_name, page, e)
                break

            if not items:
                break

            for item in items:
                # [수정] 모집 중이 아닌(마감된) 공고는 수집하지 않음 (PJTState + 마감일 종합 판단)
                if not _is_active_item(item):
                    continue
                results.append(_normalize(item, label))

            log.debug("[elancer] %s %s p:%d %d/%d → %d건",
                      label, occ_name, page, -(-total//PAGE_LIMIT), len(items))

            if page * PAGE_LIMIT >= total:
                break
            page += 1

    return results


# ──────────────────────────────────────────────────────────────
# 공개 진입점
# ──────────────────────────────────────────────────────────────
def parse() -> list[dict]:
    """상주 + 재택 전체 수집"""
    log.info("[elancer] 수집 시작")
    jobs: list[dict] = []

    jobs += _fetch_all("N", "상주")
    log.info("[elancer] 상주 %d건 수집", len(jobs))

    remote = _fetch_all("Y", "재택")
    jobs  += remote
    log.info("[elancer] 재택 %d건 수집", len(remote))

    # 중복 제거 (같은 puno가 occupation 여러 개에 나올 수 있음)
    seen, unique = set(), []
    for j in jobs:
        key = j["external_id"]
        if key not in seen:
            seen.add(key); unique.append(j)

    log.info("[elancer] 최종 %d건 (중복 제거 후)", len(unique))
    return unique


# ──────────────────────────────────────────────────────────────
# 단독 실행 테스트
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import logging as _log
    _log.basicConfig(level=_log.DEBUG,
                     format="%(levelname)s %(name)s: %(message)s")
    jobs = parse()
    print(f"\n총 {len(jobs)}건")
    for j in jobs[:3]:
        print("-" * 60)
        for k, v in j.items():
            if k not in ("description", "collected_at"):
                print(f"  {k:20s}: {v}")