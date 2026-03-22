"""
OKKY Jobs (jobs.okky.kr) 구인공고 파서
URL    : https://jobs.okky.kr/contract
타입   : 계약직/프리랜서 공고
기반   : Next.js → __NEXT_DATA__ JSON 직접 파싱
fallback: DOM 파싱 (JSON 없을 때)
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Generator
from urllib.parse import urljoin

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
logger = logging.getLogger("okky_parser")


# ──────────────────────────────────────────────
# 데이터 모델
# ──────────────────────────────────────────────

@dataclass
class OkkyJob:
    url: str
    url_hash: str
    job_id: str           # OKKY 내부 ID
    title: str
    company: str
    category: str         # 직군 (예: 백엔드, 프론트엔드)
    employment_type: str  # 계약직 / 프리랜서 등
    skills: list[str]     # 기술 스택
    location: str
    duration: str         # 계약 기간
    budget: str           # 급여/단가
    career: str           # 경력 요건
    posted_at: str
    start_date: str      # 공고 시작일
    end_date: str        # 공고 마감일 (날짜형)
    deadline: str
    project_duration: str  # 프로젝트 기간
    body: str
    source: str = "okky"
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

BASE_URL          = "https://jobs.okky.kr"
LIST_PATH         = "/contract"

PAGE_LOAD_TIMEOUT = 20_000
ELEMENT_TIMEOUT   = 10_000
DETAIL_DELAY      = 1.0
MAX_PAGES         = 50

# Next.js 렌더링 완료 대기 셀렉터
WAIT_SELECTOR     = "article, .job-card, [class*='JobCard'], [class*='job_card'], a[href*='/contract/']"


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
    "MySQL", "PostgreSQL", "Oracle", "MariaDB", "MongoDB", "Redis",
    "Elasticsearch", "MSSQL", "DynamoDB",
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Jenkins",
    "Terraform", "Ansible", "GitHub Actions",
    "JSP", "Mybatis", "JPA", "Hibernate", "MSA", "REST", "GraphQL",
    "Git", "SVN", "Linux",
]

SKILL_RE    = re.compile(r"\b(" + "|".join(SKILL_KEYWORDS) + r")\b", re.IGNORECASE)
CAREER_RE   = re.compile(r"(\d+\s*년?\s*[-~]\s*\d*\s*년?|신입|경력\s*무관|\d+년\s*이상)", re.IGNORECASE)
DURATION_RE = re.compile(r"\d+\s*개월|\d+\s*M|즉시|장기|단기|협의", re.IGNORECASE)
BUDGET_RE   = re.compile(r"\d[\d,]*\s*(?:만원|원/월|원|만|MM)?", re.IGNORECASE)
LOCATION_RE = re.compile(
    r"(서울|경기|인천|부산|대전|대구|광주|울산|세종|제주|강원|충북|충남|전북|전남|경북|경남|재택|원격)\s*\S{0,6}"
)


def extract_skills(text: str) -> list[str]:
    found = SKILL_RE.findall(text)
    seen: dict[str, str] = {}
    for s in found:
        if s.upper() not in seen:
            seen[s.upper()] = s
    return list(seen.values())


# ──────────────────────────────────────────────
# URL 빌더
# ──────────────────────────────────────────────

def list_url(page: int = 1) -> str:
    return f"{BASE_URL}{LIST_PATH}?page={page}"


def detail_url(job_id: str) -> str:
    return f"{BASE_URL}/recruits/{job_id}"


# ──────────────────────────────────────────────
# Next.js __NEXT_DATA__ 파서 (1순위)
# ──────────────────────────────────────────────

def extract_next_data(html: str) -> dict | None:
    """
    Next.js 앱은 <script id="__NEXT_DATA__"> 태그에
    페이지 데이터를 JSON으로 심어둡니다.
    """
    soup = BeautifulSoup(html, "lxml")
    tag = soup.find("script", id="__NEXT_DATA__")
    if not tag or not tag.string:
        return None
    try:
        return json.loads(tag.string)
    except json.JSONDecodeError:
        logger.warning("__NEXT_DATA__ JSON 파싱 실패")
        return None


def _safe_get(obj: Any, *keys: str, default: Any = "") -> Any:
    """중첩 dict/list 안전 접근"""
    for key in keys:
        if isinstance(obj, dict):
            obj = obj.get(key, default)
        elif isinstance(obj, list) and isinstance(key, int):
            obj = obj[key] if key < len(obj) else default
        else:
            return default
    return obj if obj is not None else default


def parse_list_from_next_data(data: dict) -> list[dict]:
    """
    __NEXT_DATA__ → 목록 아이템 추출.
    OKKY Jobs 실제 경로: pageProps → result → content
    """
    page_props = _safe_get(data, "props", "pageProps", default={})

    # 1순위: 실제 확인된 경로
    content = _safe_get(page_props, "result", "content", default=None)
    if isinstance(content, list) and content:
        logger.info(f"  __NEXT_DATA__ result.content → {len(content)}건")
        return content

    # 2순위: 혹시 구조가 바뀔 경우를 대비한 fallback 후보
    candidates = [
        page_props.get("jobs"),
        page_props.get("items"),
        page_props.get("list"),
        _safe_get(page_props, "data", "jobs"),
        _safe_get(page_props, "data", "list"),
        _safe_get(page_props, "data", "content"),
    ]
    for items in candidates:
        if isinstance(items, list) and items:
            logger.info(f"  __NEXT_DATA__ fallback 경로 발견: {len(items)}건")
            return items

    logger.debug("__NEXT_DATA__에서 목록을 찾지 못했습니다 — DOM fallback 사용")
    return []


def normalize_next_item(item: dict) -> dict:
    """
    __NEXT_DATA__ 아이템을 공통 메타 dict로 정규화.
    실제 확인된 필드: id, title, dateCreated, category, recruitResponse
    """
    job_id = str(item.get("id") or "")
    title  = str(item.get("title") or "")

    # recruitResponse 안에 상세 채용 정보가 들어있을 수 있음
    recruit = item.get("recruitResponse") or {}

    company = (
        recruit.get("companyName") or recruit.get("company")
        or item.get("companyName") or item.get("company") or ""
    )
    posted_at = (
        item.get("dateCreated") or item.get("createdAt")
        or recruit.get("dateCreated") or ""
    )
    deadline = (
        recruit.get("deadline") or recruit.get("endDate")
        or item.get("deadline") or ""
    )
    location = (
        recruit.get("location") or recruit.get("workPlace")
        or item.get("location") or ""
    )
    employment_type = (
        recruit.get("employmentType") or item.get("employmentType") or "계약직"
    )

    # category: 문자열 or dict
    raw_cat = item.get("category") or recruit.get("category") or ""
    category = raw_cat if isinstance(raw_cat, str) else raw_cat.get("name", "")

    # skills: list 또는 없음
    raw_skills = (
        recruit.get("skills") or recruit.get("techStack")
        or item.get("skills") or item.get("tags") or []
    )
    skills = [s if isinstance(s, str) else s.get("name", "") for s in raw_skills]

    url = item.get("url") or (detail_url(job_id) if job_id else "")

    return {
        "job_id":          job_id,
        "title":           title,
        "company":         str(company),
        "employment_type": str(employment_type),
        "category":        str(category),
        "skills":          skills,
        "location":        str(location),
        "posted_at":       str(posted_at),
        "deadline":        str(deadline),
        "url":             url,
        "raw":             item,
    }


# ──────────────────────────────────────────────
# DOM fallback 파서 (2순위)
# ──────────────────────────────────────────────

def parse_list_from_dom(html: str) -> list[dict]:
    """
    __NEXT_DATA__에 목록이 없을 때 DOM에서 직접 파싱.
    /contract/ 포함 링크를 기준으로 카드 추출.
    """
    soup = BeautifulSoup(html, "lxml")
    items = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        # /contract/{id} 패턴만 처리 (목록 페이지 /contract?page= 제외)
        if not re.search(r"/contract/[\w-]+", href):
            continue

        full_url = href if href.startswith("http") else urljoin(BASE_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)

        # URL에서 job_id 추출
        id_m = re.search(r"/contract/([\w-]+)", href)
        job_id = id_m.group(1) if id_m else ""

        # 부모 카드 컨테이너
        card = a.find_parent(["article", "li", "div"])
        text = card.get_text(separator=" ", strip=True) if card else a.get_text(strip=True)

        title = a.get_text(strip=True) or text[:60]
        date_m = re.search(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", text)

        items.append({
            "job_id":          job_id,
            "title":           title,
            "company":         "",
            "employment_type": "계약직",
            "category":        "",
            "skills":          [],
            "location":        "",
            "posted_at":       date_m.group(0) if date_m else "",
            "deadline":        "",
            "url":             full_url,
            "raw":             {},
        })

    return items


def has_next_page(html: str, current_page: int) -> bool:
    """다음 페이지 존재 여부"""
    data = extract_next_data(html)
    if data:
        result = _safe_get(data, "props", "pageProps", "result", default={})
        if isinstance(result, dict):
            # Spring Pageable 응답: last=True 이면 마지막 페이지
            if result.get("last") is True:
                return False
            if result.get("last") is False:
                return True
            # totalPages 방식
            total_pages = result.get("totalPages")
            if total_pages is not None:
                return current_page < int(total_pages)

    # fallback: DOM 링크에서 다음 페이지 확인
    soup = BeautifulSoup(html, "lxml")
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m and int(m.group(1)) > current_page:
            return True
    return False


# ──────────────────────────────────────────────
# 상세 페이지 파서
# ──────────────────────────────────────────────

def parse_detail(html: str, url: str, meta: dict) -> OkkyJob:
    """상세 페이지 HTML → OkkyJob"""

    # __NEXT_DATA__에서 상세 데이터 우선 추출
    data = extract_next_data(html)
    detail_raw: dict = {}
    if data:
        page_props = _safe_get(data, "props", "pageProps", default={})
        detail_raw = (
            page_props.get("job") or page_props.get("item")
            or page_props.get("data") or {}
        )

    # 본문 텍스트
    soup = BeautifulSoup(html, "lxml")
    body = ""
    for selector in [".job-description", ".description", "[class*='Description']",
                     "[class*='content']", "article", "main"]:
        el = soup.select_one(selector)
        if el:
            candidate = el.get_text(separator="\n", strip=True)
            if len(candidate) > 100:
                body = candidate
                break
    if not body:
        body = soup.get_text(separator="\n", strip=True)

    full_text = f"{meta['title']}\n{body}"

    # __NEXT_DATA__ 우선, 없으면 정규식 fallback
    title    = detail_raw.get("title")    or meta.get("title", "")
    company  = (detail_raw.get("company") or detail_raw.get("companyName")
                or meta.get("company", ""))
    location = (detail_raw.get("location") or detail_raw.get("workPlace")
                or meta.get("location", ""))
    deadline = (detail_raw.get("deadline") or detail_raw.get("endDate")
                or meta.get("deadline", ""))
    posted_at = (detail_raw.get("createdAt") or detail_raw.get("registeredAt")
                 or meta.get("posted_at", ""))
    employment_type = (detail_raw.get("employmentType") or meta.get("employment_type", "계약직"))
    category        = (detail_raw.get("category") or meta.get("category", ""))

    # 스킬: __NEXT_DATA__ 리스트 우선, 없으면 본문 정규식
    raw_skills = (detail_raw.get("skills") or detail_raw.get("techStack")
                  or detail_raw.get("tags") or meta.get("skills") or [])
    if raw_skills:
        skills = [s if isinstance(s, str) else s.get("name", "") for s in raw_skills]
    else:
        skills = extract_skills(full_text)

    # 정규식 보완 필드
    if not location:
        loc_m = LOCATION_RE.search(body)
        location = loc_m.group(0)[:20] if loc_m else ""

    dur_m    = DURATION_RE.search(body)
    duration = dur_m.group(0) if dur_m else ""

    bud_list = BUDGET_RE.findall(body)
    budget   = bud_list[0] if bud_list else ""

    car_m  = CAREER_RE.search(body)
    career = car_m.group(0) if car_m else ""

    job_id = meta.get("job_id", "")

    # 공고 시작일 / 마감일 / 프로젝트 기간
    start_date, end_date = parse_date_range(f"{str(posted_at)} {body}")
    if not deadline and end_date:
        deadline = end_date
    project_duration = parse_project_duration(body) or duration

    return OkkyJob(
        url=url,
        url_hash=hashlib.sha256(url.encode()).hexdigest()[:16],
        job_id=job_id,
        title=str(title),
        company=str(company),
        category=str(category),
        employment_type=str(employment_type),
        skills=skills,
        location=str(location),
        duration=duration,
        budget=budget,
        career=career,
        posted_at=str(posted_at),
        start_date=start_date,
        end_date=end_date,
        deadline=str(deadline),
        project_duration=project_duration,
        body=body[:2000],
    )


# ──────────────────────────────────────────────
# Playwright 헬퍼
# ──────────────────────────────────────────────

def wait_and_get_html(page: Page, url: str, wait_selector: str | None = None) -> str:
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

def crawl_okky(
    max_pages: int = MAX_PAGES,
    delay: float = DETAIL_DELAY,
    headless: bool = True,
) -> Generator[OkkyJob, None, None]:
    """
    OKKY Jobs 계약직 전체 페이지를 순회하며 공고를 yield.

    사용 예:
        for job in crawl_okky(max_pages=3):
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
                logger.error(f"목록 로드 실패 (page={page_num}): {e}")
                break

            # 1순위: __NEXT_DATA__ JSON
            next_data = extract_next_data(html)
            if next_data:
                raw_items = parse_list_from_next_data(next_data)
                items = [normalize_next_item(i) for i in raw_items]
            else:
                items = []

            # 2순위: DOM fallback
            if not items:
                logger.info("  __NEXT_DATA__ 없음 → DOM fallback")
                items = parse_list_from_dom(html)

            logger.info(f"  → {len(items)}건 발견")

            if not items:
                logger.info("항목 없음 — 수집 종료")
                break

            for meta in items:
                job_url  = meta["url"]
                if not job_url:
                    continue

                url_hash = hashlib.sha256(job_url.encode()).hexdigest()[:16]
                if url_hash in seen_hashes:
                    logger.debug(f"중복 건너뜀: {job_url}")
                    continue
                seen_hashes.add(url_hash)

                logger.info(f"  상세 수집: {meta['title'][:45]}")
                try:
                    detail_html = wait_and_get_html(page, job_url)
                except Exception as e:
                    logger.warning(f"상세 로드 실패: {e}")
                    continue

                job = parse_detail(detail_html, job_url, meta)

                if not is_active(job.deadline or job.end_date, ""):
                    logger.debug(f"마감 건너뜀: {job.title[:30]}")
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
    import json as _json

    results = []
    for job in crawl_okky(max_pages=2):
        results.append(asdict(job))
        print(f"\n{'='*60}")
        print(f"제목    : {job.title}")
        print(f"회사    : {job.company}")
        print(f"직군    : {job.category}")
        print(f"기술    : {', '.join(job.skills)}")
        print(f"경력    : {job.career}")
        print(f"기간    : {job.duration}")
        print(f"단가    : {job.budget}")
        print(f"근무지  : {job.location}")
        print(f"등록일  : {job.posted_at}")
        print(f"마감일  : {job.deadline}")
        print(f"시작일  : {job.start_date}")
        print(f"마감일(날짜): {job.end_date}")
        print(f"프로젝트기간: {job.project_duration}")
        print(f"URL     : {job.url}")

    with open("okky_jobs.json", "w", encoding="utf-8") as f:
        _json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(results)}건 수집 완료 → okky_jobs.json")
