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
    project_type: str   # SI / SM 등 프로젝트 유형
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
    "SAP", "ABAP", "ERP",
]

SKILL_RE    = re.compile(r"\b(" + "|".join(SKILL_KEYWORDS) + r")\b", re.IGNORECASE)
LEVEL_RE    = re.compile(r"(초급|중급|고급|시니어|주니어|리드|PM|PL)", re.IGNORECASE)
DURATION_RE = re.compile(r"\d+\s*개월|\d+\s*M|즉시|장기|단기", re.IGNORECASE)
BUDGET_RE   = re.compile(r"\d[\d,]*\s*(?:만원|원/월|원|만|MM)?", re.IGNORECASE)
LOCATION_RE = re.compile(
    r"(서울|경기|인천|부산|대전|대구|광주|울산|세종|제주|강원|충북|충남|전북|전남|경북|경남)\s*\S*"
)


# 프로젝트 유형 키워드 (스킬이 아닌 별도 필드로 분리)
PROJECT_TYPE_KEYWORDS = {
    "SI": "SI (시스템 구축)",
    "SM": "SM (시스템 유지보수)",
}
PROJECT_TYPE_RE = re.compile(
    r"(SI|SM)",
    re.IGNORECASE,
)


def extract_project_type(text: str) -> str:
    """본문에서 프로젝트 유형(SI/SM) 추출"""
    m = PROJECT_TYPE_RE.search(text or "")
    if m:
        key = m.group(1).upper()
        return PROJECT_TYPE_KEYWORDS.get(key, key)
    return ""


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

def _is_closed_badge(row) -> bool:
    """
    SISM 목록 li/div에서 마감 배지 존재 여부 확인.

    실제 HTML 구조:
      <span class="badge outline medium legend" style="color:#f91a19;...">마감</span>

    마감 배지가 있으면 True 반환 → 상세 페이지 요청 생략.
    """
    # 1순위: legend 클래스 배지에서 "마감" 텍스트 확인 (가장 정확)
    for span in row.select("span.legend"):
        if "마감" in span.get_text(strip=True):
            return True

    # 2순위: list-comment 영역 안의 마감 텍스트
    comment = row.select_one(".list-comment")
    if comment and "마감" in comment.get_text(strip=True):
        return True

    # 3순위: h3 태그 색상이 #aaa (마감 공고는 회색 처리)
    h3 = row.select_one("h3")
    if h3:
        style = h3.get("style", "")
        if "color:#aaa" in style.replace(" ", "") or "color: #aaa" in style:
            return True

    return False


def _parse_deadline_from_row(row) -> str:
    """
    SISM 목록 행에서 마감일/기간 추출.

    실제 HTML 구조:
      <div class="list-local">
        <span>서울 종로구</span>
        <span>2026-03-23</span>   ← 등록일 또는 마감일
        <span>5개월</span>        ← 프로젝트 기간
      </div>
    """
    local = row.select_one(".list-local")
    if not local:
        return ""

    spans = [s.get_text(strip=True) for s in local.find_all("span")]
    for span in spans:
        # 날짜 형식 (YYYY-MM-DD)
        m = re.search(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", span)
        if m:
            return m.group(0)

    return ""


def parse_list_html(html: str) -> list[dict]:
    """
    Playwright가 렌더링한 HTML에서 목록 추출.

    마감 공고 사전 필터링 (상세 요청 생략):
      1. <span class="badge ... legend">마감</span> 배지 존재
      2. <h3 style="color:#aaa"> — 회색 제목 (마감 공고 스타일)
      3. .list-comment 영역 내 "마감" 텍스트
    """
    soup     = BeautifulSoup(html, "lxml")
    items    = []
    seen_urls: set[str] = set()
    skipped  = 0

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

        # 부모 컨테이너 (.list-type3 또는 li/div)
        row = a.find_parent(["li", "div"])
        # 더 큰 컨테이너로 이동 (마감 배지가 다른 위치에 있을 수 있음)
        list_wrap = row
        for _ in range(4):
            parent = list_wrap.find_parent(["li", "div"]) if list_wrap else None
            if parent and "list" in " ".join(parent.get("class", [])):
                list_wrap = parent
                break

        container = list_wrap or row

        # ── 마감 배지 체크 (핵심 필터) ────────────
        if container and _is_closed_badge(container):
            skipped += 1
            logger.debug(f"마감 배지 감지, 제외: {title[:40]}")
            continue

        # 메타 정보 추출
        posted_at = ""
        deadline  = ""
        views     = 0
        company   = ""

        if container:
            # 마감일 추출 (.list-local 스팬)
            deadline = _parse_deadline_from_row(container)

            # 날짜형 마감일이 오늘 이전이면 제외
            if deadline:
                date_m = re.search(
                    r"(\d{4})[-./](\d{1,2})[-./](\d{1,2})", deadline
                )
                if date_m:
                    from datetime import date as _date
                    try:
                        dl = _date(
                            int(date_m.group(1)),
                            int(date_m.group(2)),
                            int(date_m.group(3)),
                        )
                        if dl < _date.today():
                            skipped += 1
                            logger.debug(f"날짜 마감 제외: {title[:40]} ({deadline})")
                            continue
                    except ValueError:
                        pass

            # 조회수
            text  = container.get_text(separator=" ", strip=True)
            hit_m = re.search(r"조회\s*[:\s]*(\d+)", text)
            if hit_m:
                views = int(hit_m.group(1))

            # 등록일
            local = container.select_one(".list-local")
            if local:
                spans = [s.get_text(strip=True) for s in local.find_all("span")]
                for span in spans:
                    dm = re.search(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", span)
                    if dm:
                        posted_at = dm.group(0)
                        break

            # 회사명
            comp_el = container.select_one(".company img")
            if comp_el:
                company = comp_el.get("alt", "")

        items.append({
            "title":     title,
            "url":       full_url,
            "company":   company,
            "posted_at": posted_at,
            "deadline":  deadline,
            "views":     views,
        })

    if skipped:
        logger.info(f"  목록 마감 제외: {skipped}건 (상세 요청 생략)")

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
    # 목록 단계에서 이미 추출된 deadline 우선 사용
    deadline = meta.get("deadline", "")
    if not deadline:
        # 날짜형/D-N 패턴만 허용 — \S+ 제거로 "된","됩니다" 오캡처 방지
        dead_m = re.search(
            r"마감(?:일자?|일정)?\s*[:\s]\s*(\d{4}[-./]\d{1,2}[-./]\d{1,2})"
            r"|(\d{4}[-./]\d{1,2}[-./]\d{1,2})\s*마감"
            r"|(D-\d+|D-Day)",
            body,
            re.IGNORECASE,
        )
        if dead_m:
            deadline = next(g for g in dead_m.groups() if g)
        else:
            deadline = ""

    company  = meta.get("company", "")
    nick_el  = soup.select_one(".sv_member, .nick, .writer, .author")
    if nick_el:
        company = nick_el.get_text(strip=True)

    # 프로젝트 유형 (SI/SM)
    project_type = extract_project_type(full_text)

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
        project_type=project_type,
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

                # 오늘 날짜 기준 마감 공고 제외
                if not is_active(job.deadline or job.end_date, ""):
                    logger.debug(f"마감 건너뜀: {job.title[:40]}")
                    continue

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
        print(f"유형    : {job.project_type}")
        print(f"근무지  : {job.location}")
        print(f"등록일  : {job.posted_at}")
        print(f"시작일  : {job.start_date}")
        print(f"마감일  : {job.end_date}")
        print(f"프로젝트기간: {job.project_duration}")
        print(f"URL     : {job.url}")

    with open("sism_jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(results)}건 수집 완료 → sism_jobs.json")