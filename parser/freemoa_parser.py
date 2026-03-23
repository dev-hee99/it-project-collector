"""
프리모아 (freemoa.net) 구인공고 파서
카테고리: 개발만 필터링
특이사항:
  - pno: li 자식 div[data-pno] 에서 추출
  - 상세 URL: /m4/s41?page={page}&pno={pno}&first_pno={pno}
  - 상세 데이터: XHR /m4a/s41a?pno={pno} 직접 호출 (클릭 불필요)
  - 상세 필드: data-name 속성 (title, costView, during, txt 등)
  - 페이지네이션: JS 클릭 방식
"""

import hashlib
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Generator

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import Page, sync_playwright


from datetime import date, datetime as _dt

TODAY = date.today()


def is_active(deadline_str: str, status_str: str = "") -> bool:
    """
    오늘 날짜 기준 모집중 여부 판단.
    - status 텍스트에 마감/종료/완료 포함 시 False
    - D-숫자 형태: D-0 이상이면 True
    - YYYY-MM-DD 형태 마감일: 오늘 이후이면 True
    - 판단 불가 시 True (보수적으로 포함)
    """
    closed_keywords = ["마감", "종료", "완료", "closed", "expired", "ended"]
    if any(k in status_str.lower() for k in closed_keywords):
        return False

    # D-N 패턴
    d_m = re.search(r"D-(\d+)", deadline_str or "")
    if d_m:
        return int(d_m.group(1)) >= 0

    # YYYY-MM-DD 또는 YYYY.MM.DD 패턴
    date_m = re.search(r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", deadline_str or "")
    if date_m:
        try:
            dl = date(int(date_m.group(1)), int(date_m.group(2)), int(date_m.group(3)))
            return dl >= TODAY
        except ValueError:
            pass

    return True  # 마감일 정보 없으면 포함


def parse_date_range(text: str) -> tuple[str, str]:
    """
    본문에서 공고 시작일 / 마감일 추출.
    반환: (start_date, end_date) — 없으면 빈 문자열
    """
    dates = re.findall(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", text or "")
    start = dates[0] if len(dates) >= 1 else ""
    end   = dates[1] if len(dates) >= 2 else ""
    return start, end


def parse_project_duration(text: str) -> str:
    """
    본문에서 프로젝트 기간 추출.
    예: "6개월", "3M", "180일", "장기", "단기", "1년"
    """
    m = re.search(
        r"(\d+\s*년|\d+\s*개월|\d+\s*M|\d+\s*일|장기|단기|협의|상시)",
        text or "",
        re.IGNORECASE,
    )
    return m.group(0).strip() if m else ""


from logger import get_logger
logger = get_logger("freemoa_parser")


# ──────────────────────────────────────────────
# 데이터 모델
# ──────────────────────────────────────────────

@dataclass
class FreemoaJob:
    pno: str                # 프리모아 프로젝트 번호
    url: str                # 상세 페이지 URL
    url_hash: str           # 중복 체크용 해시
    title: str
    work_type: str          # 도급 / 기간제 상주 / 상주
    status: str             # 모집중 / 마감
    category: str           # 개발 / 디자인 등
    skills: list[str]       # 기술 스택
    budget: str             # 월 임금 (costView)
    duration: str           # 예상기간 (during)
    applicants: str         # 지원자수 (ALL_APPLY_COUNT)
    start_date: str         # 시작예정일 (BEGIN_EXPECT)
    end_date: str           # 공고 마감일 (날짜형)
    deadline: str           # 마감일정 (D-N)
    project_duration: str   # 프로젝트 기간
    plan: str               # 플랜명 (plan_nm)
    body: str               # 본문 (txt)
    location: str           # 지역 (제목에서 추출)
    source: str = "freemoa"
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

BASE_URL        = "https://www.freemoa.net"
LIST_URL        = f"{BASE_URL}/m4/s41"
DETAIL_API_URL  = f"{BASE_URL}/m4a/s41a"   # XHR 상세 API

DEV_KEYWORDS    = ["개발"]

MAX_PAGES       = 50
PAGE_TIMEOUT    = 40_000
PAGINATION_WAIT = 3_000
DETAIL_DELAY    = 0.8    # XHR 호출 간 대기(초)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": LIST_URL,
    "X-Requested-With": "XMLHttpRequest",
}


# ──────────────────────────────────────────────
# 기술 스택 추출
# ──────────────────────────────────────────────

SKILL_KEYWORDS = [
    "Java", "Python", "Kotlin", "Swift", "Go", "Rust", r"C\+\+", r"C#",
    "JavaScript", "TypeScript", "PHP", "Ruby", "Scala",
    "React", "Vue", "Angular", r"Next\.js", "Nuxt", "Svelte",
    "HTML", "CSS", "jQuery", "Tailwind",
    "Spring", "Django", "FastAPI", "Flask", r"Node\.js", "Express",
    "Laravel", "Rails", "NestJS",
    "MySQL", r"Postgre\s*SQL", "Oracle", "MariaDB", "MongoDB", "Redis",
    "Elasticsearch", "MSSQL", "SQLite",
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Jenkins",
    "Terraform", "Ansible", r"GitHub\s*Actions",
    "JSP", "Mybatis", "JPA", "Hibernate", "MSA", "REST", "GraphQL",
    "Git", "SVN", "Linux",
    "Flutter", r"React\s*Native", "Android", "iOS",
    "Spring Framework", "Spring Boot",
    "SAP", "ABAP", "ERP",
]

SKILL_RE    = re.compile(r"\b(" + "|".join(SKILL_KEYWORDS) + r")\b", re.IGNORECASE)
LOCATION_RE = re.compile(
    r"\[(상주|재택|원격|수원|서울|경기|인천|부산|대전|대구|광주|울산|세종|제주|신규|웹|앱)[^\]]*\]"
)


def extract_skills(text: str) -> list[str]:
    found = SKILL_RE.findall(text)
    seen: dict[str, str] = {}
    for s in found:
        if s.upper() not in seen:
            seen[s.upper()] = s
    return list(seen.values())


def make_hash(pno: str) -> str:
    return hashlib.sha256(pno.encode()).hexdigest()[:16]


def build_detail_url(pno: str, page_num: int = 1) -> str:
    return f"{LIST_URL}?page={page_num}&pno={pno}&first_pno={pno}"


# ──────────────────────────────────────────────
# pno 추출
# ──────────────────────────────────────────────

def extract_pno_from_li(li) -> str:
    """
    li 자식 div[data-pno] 에서 pno 추출.
    디버그 결과: li > div[data-pno='47996'] 구조 확인됨.
    """
    el = li.select_one("[data-pno]")
    if el:
        return str(el.get("data-pno", "")).strip()
    return ""


# ──────────────────────────────────────────────
# 목록 파서
# ──────────────────────────────────────────────

def parse_list_html(html: str, page_num: int = 1) -> list[dict]:
    """
    #projectListNew ul li 에서 개발 카테고리 항목만 추출.
    pno + 목록 메타만 반환 (상세는 XHR로 별도 수집).
    """
    soup  = BeautifulSoup(html, "lxml")
    ul    = soup.select_one("#projectListNew")
    if not ul:
        logger.warning("#projectListNew 없음")
        return []

    items = []
    for li in ul.find_all("li", recursive=False):

        # 제목
        title_el = li.select_one("p.title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        # 카테고리 (첫 번째 projectInfo div 첫 토큰)
        info_divs = li.select("div.projectInfo")
        category  = ""
        if info_divs:
            cat_text = info_divs[0].get_text(separator=",", strip=True)
            parts    = [p.strip() for p in cat_text.split(",") if p.strip()]
            category = parts[0] if parts else ""

        # ── 개발 카테고리 필터 ──
        if not any(kw in category for kw in DEV_KEYWORDS):
            continue

        # 근무 형태
        work_type = ""
        for cls in ["b", "d", "c"]:
            el = li.select_one(f"p.{cls}")
            if el and el.get_text(strip=True):
                work_type = el.get_text(strip=True)
                break

        # 모집 상태
        status_el = li.select_one("p.e")
        status    = status_el.get_text(strip=True) if status_el else ""

        # ── 모집중 필터 (상태 + 날짜) ──
        if status != "모집중":
            continue

        # 기술 스택 (목록에서 1차 추출)
        skills = []
        if info_divs:
            cat_text   = info_divs[0].get_text(separator=",", strip=True)
            parts      = [p.strip() for p in cat_text.split(",") if p.strip()]
            raw_skills = parts[1:] if len(parts) > 1 else []
            skills     = list(dict.fromkeys(
                raw_skills + extract_skills(",".join(raw_skills))
            ))

        # 마감일 (목록에서 파싱)
        deadline = ""
        if len(info_divs) > 1:
            meta_text = info_divs[1].get_text(separator="|", strip=True)
            dead_m    = re.search(r"(D-\d+|D-Day|마감)", meta_text)
            deadline  = dead_m.group(1) if dead_m else ""

        # 지역
        loc_m    = LOCATION_RE.search(title)
        location = loc_m.group(0) if loc_m else ""

        # pno
        pno = extract_pno_from_li(li)

        items.append({
            "pno":       pno,
            "title":     title,
            "work_type": work_type,
            "status":    status,
            "category":  category,
            "skills":    skills,
            "deadline":  deadline,
            "location":  location,
            "page_num":  page_num,
        })

    return items


# ──────────────────────────────────────────────
# XHR 상세 API 호출
# ──────────────────────────────────────────────

def fetch_detail_xhr(pno: str, session: requests.Session) -> dict:
    """
    /m4a/s41a?pno={pno} XHR 호출로 상세 HTML 수신.
    응답이 HTML 조각이면 data-name 속성으로 파싱.
    응답이 JSON이면 바로 반환.
    """
    try:
        resp = session.get(
            DETAIL_API_URL,
            params={"pno": pno},
            timeout=15,
            verify=False,   # SSL 인증서 오류 우회
        )
        resp.raise_for_status()
        ct = resp.headers.get("Content-Type", "")

        # JSON 응답
        if "json" in ct:
            return resp.json()

        # HTML 조각 응답 → data-name 속성으로 파싱
        return parse_detail_html_fragment(resp.text)

    except Exception as e:
        logger.warning(f"  XHR 상세 실패 (pno={pno}): {e}")
        return {}


def parse_detail_html_fragment(html: str) -> dict:
    """
    #projectViewWrap 안의 data-name 속성으로 상세 필드 추출.
    확인된 data-name: title, costView, during, ALL_APPLY_COUNT,
                      BEGIN_EXPECT, proj_filed_new, plan_nm, pvNmu, txt
    """
    soup   = BeautifulSoup(html, "lxml")
    result = {}

    # data-name 속성 기반 추출
    for el in soup.find_all(attrs={"data-name": True}):
        name = el.get("data-name", "").strip()
        text = el.get_text(strip=True)
        if name and text:
            result[name] = text

    # 본문 (pre 태그 전체 텍스트)
    txt_el = soup.find("pre", attrs={"data-name": "txt"})
    if txt_el:
        result["txt"] = txt_el.get_text(separator="\n", strip=True)

    return result


def build_job(meta: dict, detail: dict) -> FreemoaJob:
    """목록 메타 + 상세 XHR 데이터를 합쳐 FreemoaJob 생성"""
    pno      = meta["pno"]
    page_num = meta.get("page_num", 1)

    # 상세에서 덮어쓰기 (더 정확한 데이터 우선)
    title    = detail.get("title")    or meta["title"]
    budget   = detail.get("costView") or ""
    duration = detail.get("during")   or ""
    applicants = detail.get("ALL_APPLY_COUNT") or ""
    start_date = detail.get("BEGIN_EXPECT")    or ""
    plan       = detail.get("plan_nm")         or ""
    body       = detail.get("txt")             or ""

    # 스킬: 본문에서 추가 추출
    skills = meta["skills"]
    if body:
        extra  = extract_skills(body)
        skills = list(dict.fromkeys(skills + extra))

    # 공고 시작일 / 마감일 / 프로젝트 기간
    _start, end_date = parse_date_range(body)
    if not start_date:
        start_date = _start
    project_duration = parse_project_duration(body) or duration

    return FreemoaJob(
        pno        = pno,
        url        = build_detail_url(pno, page_num),
        url_hash   = make_hash(pno),
        title      = title,
        work_type  = meta["work_type"],
        status     = meta["status"],
        category   = meta["category"],
        skills     = skills,
        budget     = budget,
        duration   = duration,
        applicants = applicants,
        start_date = start_date,
        end_date   = end_date,
        deadline   = meta["deadline"],
        project_duration = project_duration,
        plan       = plan,
        body       = body[:2000],
        location   = meta["location"],
    )


# ──────────────────────────────────────────────
# 페이지네이션
# ──────────────────────────────────────────────

def get_total_pages(html: str) -> int:
    soup       = BeautifulSoup(html, "lxml")
    pagination = soup.select_one("#projectPagination")
    if not pagination:
        return 1
    nums = [int(a.get_text(strip=True))
            for a in pagination.find_all("a")
            if a.get_text(strip=True).isdigit()]
    return max(nums) if nums else 1


def click_page(pw_page: Page, page_num: int) -> bool:
    try:
        locator = pw_page.locator(
            "#projectPagination a, #projectPagination button"
        ).filter(has_text=re.compile(rf"^\s*{page_num}\s*$"))

        if locator.count() == 0:
            # 다음 블록으로 이동
            next_btn = pw_page.locator(
                "#projectPagination a, #projectPagination button"
            ).filter(has_text=re.compile(r"다음|next|>", re.IGNORECASE))
            if next_btn.count() > 0:
                next_btn.first.click()
                pw_page.wait_for_timeout(PAGINATION_WAIT)
                locator = pw_page.locator(
                    "#projectPagination a, #projectPagination button"
                ).filter(has_text=re.compile(rf"^\s*{page_num}\s*$"))

        if locator.count() > 0:
            locator.first.click()
            pw_page.wait_for_timeout(PAGINATION_WAIT)
            return True

        logger.warning(f"  페이지 {page_num} 버튼 없음")
        return False
    except Exception as e:
        logger.warning(f"  페이지 {page_num} 클릭 실패: {e}")
        return False


# ──────────────────────────────────────────────
# 크롤러 메인
# ──────────────────────────────────────────────

def crawl_freemoa(
    max_pages: int = MAX_PAGES,
    headless: bool = True,
) -> Generator[FreemoaJob, None, None]:
    """
    프리모아 프로젝트 목록에서 개발 카테고리 공고를 yield.

    흐름:
      1. Playwright로 목록 페이지 렌더링 → li 파싱 + pno 수집
      2. pno별로 requests XHR → /m4a/s41a 상세 데이터 수집
      3. 목록 메타 + 상세 데이터 합쳐서 FreemoaJob yield

    사용 예:
        for job in crawl_freemoa(max_pages=3):
            print(job.title, job.url)
    """
    seen_hashes: set[str] = set()

    # SSL 경고 메시지 억제 (verify=False 사용 시)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # requests 세션 (XHR 상세 호출용)
    session = requests.Session()
    session.headers.update(HEADERS)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="ko-KR",
        )
        pw_page = context.new_page()

        # 첫 페이지 로드
        logger.info(f"첫 페이지 로드 중: {LIST_URL}")
        try:
            pw_page.goto(LIST_URL, wait_until="load", timeout=PAGE_TIMEOUT)
            pw_page.wait_for_timeout(5000)
        except Exception as e:
            logger.error(f"첫 페이지 로드 실패: {e}")
            browser.close()
            return

        # Playwright 쿠키 → requests 세션에 공유 (로그인 세션 유지용)
        cookies = pw_page.context.cookies()
        for c in cookies:
            session.cookies.set(c["name"], c["value"], domain=c.get("domain", ""))

        html        = pw_page.content()
        total_pages = min(get_total_pages(html), max_pages)
        logger.info(f"총 {total_pages} 페이지 수집 예정")

        def process_page_html(html: str, page_num: int):
            metas = parse_list_html(html, page_num)
            logger.info(f"  → 개발 공고 {len(metas)}건 발견")

            for meta in metas:
                pno      = meta["pno"]
                url_hash = make_hash(pno) if pno else make_hash(meta["title"])

                if url_hash in seen_hashes:
                    logger.debug(f"  중복 건너뜀: {meta['title'][:30]}")
                    continue
                seen_hashes.add(url_hash)

                if not pno:
                    logger.warning(f"  pno 없음 — 건너뜀: {meta['title'][:40]}")
                    continue

                # XHR 상세 호출
                logger.info(f"  상세 수집 (pno={pno}): {meta['title'][:40]}")
                detail = fetch_detail_xhr(pno, session)
                time.sleep(DETAIL_DELAY)

                job = build_job(meta, detail)

                if not is_active(job.deadline, job.status):
                    logger.debug(f"마감 건너뜀: {job.title[:30]}")
                    continue

                yield job

        # 1페이지 처리
        yield from process_page_html(html, 1)

        # 2페이지~
        for page_num in range(2, total_pages + 1):
            logger.info(f"page={page_num} 이동 중...")
            if not click_page(pw_page, page_num):
                logger.warning(f"  page={page_num} 이동 실패 — 종료")
                break

            html = pw_page.content()
            if not html:
                break

            yield from process_page_html(html, page_num)

        browser.close()


# ──────────────────────────────────────────────
# CLI 실행
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import json

    results = []
    for job in crawl_freemoa(max_pages=3):
        results.append(asdict(job))
        print(f"\n{'='*60}")
        print(f"제목    : {job.title}")
        print(f"URL     : {job.url}")
        print(f"pno     : {job.pno}")
        print(f"지역    : {job.location}")
        print(f"형태    : {job.work_type}")
        print(f"상태    : {job.status}")
        print(f"기술    : {', '.join(job.skills)}")
        print(f"금액    : {job.budget}")
        print(f"기간    : {job.duration}")
        print(f"마감    : {job.deadline}")
        print(f"지원자  : {job.applicants}")
        print(f"시작예정: {job.start_date}")
        print(f"마감일  : {job.end_date}")
        print(f"프로젝트기간: {job.project_duration}")
        print(f"본문    : {job.body[:80]}")

    with open("freemoa_jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(results)}건 수집 완료 → freemoa_jobs.json")