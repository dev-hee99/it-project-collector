"""
SISM (sism.co.kr) 구인공고 파서 — Playwright 버전
카테고리: 개발 (sca=개발)
JS 렌더링 사이트이므로 requests 대신 Playwright 사용
"""

import hashlib
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Generator
from urllib.parse import urlencode

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


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("sism_parser")


# ──────────────────────────────────────────────
# 데이터 모델
# ──────────────────────────────────────────────

@dataclass
class SismJob:
    url: str
    url_hash: str
    title: str
    company: str
    category: str
    skills: list[str]
    location: str
    duration: str
    budget: str
    level: str
    posted_at: str
    start_date: str      # 공고 시작일
    end_date: str        # 공고 마감일 (날짜형)
    deadline: str
    project_duration: str  # 프로젝트 기간
    views: int
    body: str
    source: str = "sism"
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

BASE_URL     = "https://sism.co.kr"
LIST_URL     = f"{BASE_URL}/bbs/board.php"
BO_TABLE     = "guin"
CATEGORY     = "개발"

# JS 렌더링 완료 대기 셀렉터 — wr_id 링크가 나타날 때까지 대기
WAIT_SELECTOR = "a[href*='wr_id']"

PAGE_LOAD_TIMEOUT = 20_000   # ms
ELEMENT_TIMEOUT   = 10_000   # ms
DETAIL_DELAY      = 1.2      # 상세 페이지 간 대기(초)
MAX_PAGES         = 50


# ──────────────────────────────────────────────
# 기술 스택 추출
# ──────────────────────────────────────────────

SKILL_KEYWORDS = [
    "Java", "Python", "Kotlin", "Swift", "Go", "Rust", r"C\+\+", r"C#",
    "JavaScript", "TypeScript", "PHP", "Ruby", "Scala",
    "React", "Vue", "Angular", r"Next\.js", "Nuxt",
    "HTML", "CSS", "jQuery",
    "Spring", "Django", "FastAPI", "Flask", r"Node\.js", "Express",
    "Laravel", "Rails",
    "MySQL", "PostgreSQL", "Oracle", "MariaDB", "MongoDB", "Redis",
    "Elasticsearch", "MSSQL",
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Jenkins",
    "Terraform", "Ansible",
    "JSP", "Mybatis", "JPA", "Hibernate", "MSA", "REST", "GraphQL",
    "Git", "SVN", "Linux",
    "SAP", "ABAP", "ERP", "SI", "SM",
]

SKILL_RE    = re.compile(r"\b(" + "|".join(SKILL_KEYWORDS) + r")\b", re.IGNORECASE)
LEVEL_RE    = re.compile(r"(초급|중급|고급|시니어|주니어|리드|PM|PL)", re.IGNORECASE)
DURATION_RE = re.compile(r"\d+\s*개월|\d+\s*M|즉시|장기|단기", re.IGNORECASE)
BUDGET_RE   = re.compile(r"\d[\d,]*\s*(?:만원|원/월|원|만|MM)?", re.IGNORECASE)
LOCATION_RE = re.compile(
    r"(서울|경기|인천|부산|대전|대구|광주|울산|세종|제주|강원|충북|충남|전북|전남|경북|경남)\s*\S*"
)


def extract_skills(text: str) -> list[str]:
    found = SKILL_RE.findall(text)
    seen: dict[str, str] = {}
    for s in found:
        key = s.upper()
        if key not in seen:
            seen[key] = s
    return list(seen.values())


# ──────────────────────────────────────────────
# URL 빌더
# ──────────────────────────────────────────────

def list_url(page: int = 1) -> str:
    params = {"bo_table": BO_TABLE, "sca": CATEGORY, "page": page}
    return f"{LIST_URL}?{urlencode(params, encoding='utf-8')}"


# ──────────────────────────────────────────────
# 목록 파서
# ──────────────────────────────────────────────

def parse_list_html(html: str) -> list[dict]:
    """
    Playwright가 렌더링한 HTML에서 목록 추출.
    wr_id 포함 링크를 기준으로 파싱 — 테이블/div 구조에 무관.
    """
    soup = BeautifulSoup(html, "lxml")
    items = []
    seen_urls: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "wr_id=" not in href:
            continue

        full_url = href if href.startswith("http") else BASE_URL + href
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        title = a.get_text(strip=True)
        if not title:
            continue

        # 부모 행에서 메타 정보 추출
        row = a.find_parent(["tr", "li", "div"])
        company   = ""
        posted_at = ""
        views     = 0

        if row:
            text = row.get_text(separator=" ", strip=True)

            date_m = re.search(r"\d{2,4}[-./]\d{1,2}[-./]\d{1,2}", text)
            if date_m:
                posted_at = date_m.group(0)

            hit_m = re.search(r"조회\s*[:\s]*(\d+)", text)
            if hit_m:
                views = int(hit_m.group(1))

        items.append({
            "title":     title,
            "url":       full_url,
            "company":   company,
            "posted_at": posted_at,
            "views":     views,
        })

    return items


def has_next_page(html: str, current_page: int) -> bool:
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        m = re.search(r"page=(\d+)", a["href"])
        if m and int(m.group(1)) > current_page:
            return True
    return False


# ──────────────────────────────────────────────
# 상세 파서
# ──────────────────────────────────────────────

def parse_detail_html(html: str, url: str, meta: dict) -> SismJob:
    soup = BeautifulSoup(html, "lxml")

    # 제목
    title_el = soup.find(["h1", "h2", "h3"])
    title = title_el.get_text(strip=True) if title_el else meta.get("title", "")

    # 본문: 가장 긴 텍스트 블록 순서대로 시도
    body = ""
    for selector in ["article", "section", "main", "#bo_v_con", ".bo_v_con", ".view_content", ".content"]:
        el = soup.select_one(selector) if selector.startswith(("#", ".")) else soup.find(selector)
        if el:
            candidate = el.get_text(separator="\n", strip=True)
            if len(candidate) > 200:
                body = candidate
                break

    if not body:
        body = soup.get_text(separator="\n", strip=True)

    full_text = f"{title}\n{body}"

    skills   = extract_skills(full_text)
    level    = (LEVEL_RE.search(full_text) or type("", (), {"group": lambda s, i: ""})()).group(1)
    dur_m    = DURATION_RE.search(body)
    duration = dur_m.group(0) if dur_m else ""
    bud_list = BUDGET_RE.findall(body)
    budget   = bud_list[0] if bud_list else ""
    loc_m    = LOCATION_RE.search(body)
    location = loc_m.group(0)[:20] if loc_m else ""
    dead_m   = re.search(r"마감\s*[:\s]*(\d{4}[-./]\d{1,2}[-./]\d{1,2}|\S+)", body)
    deadline = dead_m.group(1) if dead_m else ""

    company  = meta.get("company", "")
    nick_el  = soup.select_one(".sv_member, .nick, .writer, .author")
    if nick_el:
        company = nick_el.get_text(strip=True)

    # 공고 시작일 / 마감일 / 프로젝트 기간
    start_date, end_date = parse_date_range(full_text)
    if not deadline and end_date:
        deadline = end_date
    project_duration = parse_project_duration(full_text) or duration

    return SismJob(
        url=url,
        url_hash=hashlib.sha256(url.encode()).hexdigest()[:16],
        title=title,
        company=company,
        category=CATEGORY,
        skills=skills,
        location=location,
        duration=duration,
        budget=budget,
        level=level,
        posted_at=meta.get("posted_at", ""),
        start_date=start_date,
        end_date=end_date,
        deadline=deadline,
        project_duration=project_duration,
        views=meta.get("views", 0),
        body=body[:2000],
    )


# ──────────────────────────────────────────────
# Playwright 헬퍼
# ──────────────────────────────────────────────

def wait_and_get_html(page: Page, url: str, wait_selector: str | None = None) -> str:
    """페이지 이동 후 JS 렌더링 완료까지 대기하고 HTML 반환"""
    page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
    if wait_selector:
        try:
            page.wait_for_selector(wait_selector, timeout=ELEMENT_TIMEOUT)
        except Exception:
            logger.warning(f"셀렉터 대기 타임아웃: {wait_selector!r}")
    page.wait_for_load_state("networkidle", timeout=PAGE_LOAD_TIMEOUT)
    return page.content()


# ──────────────────────────────────────────────
# 크롤러 메인
# ──────────────────────────────────────────────

def crawl_sism(
    max_pages: int = MAX_PAGES,
    delay: float = DETAIL_DELAY,
    headless: bool = True,
) -> Generator[SismJob, None, None]:
    """
    SISM 개발 카테고리를 순회하며 구인공고를 yield.

    사용 예:
        for job in crawl_sism(max_pages=3):
            print(job.title, job.skills)
    """
    seen_hashes: set[str] = set()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            locale="ko-KR",
        )
        page = context.new_page()

        for page_num in range(1, max_pages + 1):
            url = list_url(page_num)
            logger.info(f"목록 수집 중: page={page_num} — {url}")

            try:
                html = wait_and_get_html(page, url, wait_selector=WAIT_SELECTOR)
            except Exception as e:
                logger.error(f"목록 페이지 로드 실패 (page={page_num}): {e}")
                break

            items = parse_list_html(html)
            logger.info(f"  → {len(items)}건 발견")

            if not items:
                logger.info("항목 없음 — 수집 종료")
                break

            for meta in items:
                job_url  = meta["url"]
                url_hash = hashlib.sha256(job_url.encode()).hexdigest()[:16]

                if url_hash in seen_hashes:
                    logger.debug(f"중복 건너뜀: {job_url}")
                    continue
                seen_hashes.add(url_hash)

                logger.info(f"  상세 수집: {meta['title'][:45]}")
                try:
                    detail_html = wait_and_get_html(page, job_url)
                except Exception as e:
                    logger.warning(f"상세 페이지 로드 실패: {e}")
                    continue

                job = parse_detail_html(detail_html, job_url, meta)
                yield job
                time.sleep(delay)

            if not has_next_page(html, page_num):
                logger.info("마지막 페이지 — 수집 완료")
                break

        browser.close()


# ──────────────────────────────────────────────
# CLI 실행
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import json

    results = []
    for job in crawl_sism(max_pages=2):
        results.append(asdict(job))
        print(f"\n{'='*60}")
        print(f"제목    : {job.title}")
        print(f"회사    : {job.company}")
        print(f"기술    : {', '.join(job.skills)}")
        print(f"기간    : {job.duration}")
        print(f"단가    : {job.budget}")
        print(f"난이도  : {job.level}")
        print(f"근무지  : {job.location}")
        print(f"등록일  : {job.posted_at}")
        print(f"시작일  : {job.start_date}")
        print(f"마감일  : {job.end_date}")
        print(f"프로젝트기간: {job.project_duration}")
        print(f"URL     : {job.url}")

    with open("sism_jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(results)}건 수집 완료 → sism_jobs.json")
