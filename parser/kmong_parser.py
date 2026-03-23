"""
크몽 (kmong.com) 프로젝트 의뢰 파서
대상: 발주자가 올린 프로젝트 의뢰
카테고리: IT·프로그래밍 (category_list=6)

핵심 구조:
  - REST API: GET /api/custom-project/v1/requests?category_list=6&page=N
  - 로그인 필요 → Playwright로 로그인 후 쿠키를 requests 세션에 공유
  - 상세: GET /api/custom-project/v1/requests/{id}  또는  상세 페이지 파싱
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Generator
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright


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
logger = get_logger("kmong_parser")


# ──────────────────────────────────────────────
# 데이터 모델
# ──────────────────────────────────────────────

@dataclass
class KmongJob:
    request_id: str
    url: str
    url_hash: str
    title: str
    category: str
    sub_category: str
    project_type: str
    budget: str
    start_date: str      # 공고 시작일
    end_date: str        # 공고 마감일 (날짜형)
    deadline: str
    project_duration: str  # 프로젝트 기간
    skills: list[str]
    description: str
    posted_at: str
    status: str
    views: int
    proposals: int
    source: str = "kmong"
    collected_at: str = field(default_factory=lambda: datetime.now().isoformat())


# ──────────────────────────────────────────────
# 설정
# ──────────────────────────────────────────────

BASE_URL    = "https://kmong.com"
API_BASE    = f"{BASE_URL}/api/custom-project/v1"
LIST_API    = f"{API_BASE}/requests"
DETAIL_PAGE = f"{BASE_URL}/enterprise/requests"

CATEGORY_ID = "6"   # IT·프로그래밍
PER_PAGE    = 10
MAX_PAGES   = 50

PAGE_TIMEOUT = 40_000
DETAIL_DELAY = 1.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer":         f"{BASE_URL}/enterprise/requests",
}

# 크몽 계정 정보 (로그인 필요 시 입력)
KMONG_EMAIL    = ""   # 예: "your@email.com"
KMONG_PASSWORD = ""   # 예: "yourpassword"


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
    "Elasticsearch", "MSSQL",
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Jenkins",
    "Terraform", "Ansible", r"GitHub\s*Actions",
    "JSP", "Mybatis", "JPA", "Hibernate", "MSA", "REST", "GraphQL",
    "Git", "SVN", "Linux",
    "Flutter", r"React\s*Native", "Android", "iOS",
    "Spring Boot", "Spring Framework",
]

SKILL_RE = re.compile(r"\b(" + "|".join(SKILL_KEYWORDS) + r")\b", re.IGNORECASE)


def extract_skills(text: str) -> list[str]:
    found = SKILL_RE.findall(text)
    seen: dict[str, str] = {}
    for s in found:
        if s.upper() not in seen:
            seen[s.upper()] = s
    return list(seen.values())


def make_hash(request_id: str) -> str:
    return hashlib.sha256(str(request_id).encode()).hexdigest()[:16]


def build_detail_url(request_id: str) -> str:
    return f"{DETAIL_PAGE}/{request_id}"


def build_list_api_url(page: int = 1) -> str:
    params = {
        "q":                  "",
        "sort":               "CREATED_AT",
        "category_list":      CATEGORY_ID,
        "sub_category_list":  "",
        "project_type":       "",
        "page":               page,
        "per_page":           PER_PAGE,
    }
    return f"{LIST_API}?{urlencode(params)}"


# ──────────────────────────────────────────────
# 로그인 & 쿠키 획득
# ──────────────────────────────────────────────

def get_session_with_cookies(email: str = "", password: str = "") -> requests.Session:
    """
    Playwright로 크몽 로그인 후 쿠키를 requests 세션에 공유.
    email/password 미입력 시 비로그인 세션 반환 (일부 데이터만 접근 가능).
    """
    session = requests.Session()
    session.headers.update(HEADERS)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="ko-KR",
        )
        page = context.new_page()

        if email and password:
            logger.info("크몽 로그인 시도 중...")
            try:
                # 로그인 페이지 이동
                page.goto(f"{BASE_URL}/login", wait_until="load", timeout=PAGE_TIMEOUT)
                page.wait_for_timeout(2000)

                # 이메일/비밀번호 입력
                page.fill("input[type='email'], input[name='email'], #email", email)
                page.fill("input[type='password'], input[name='password'], #password", password)
                page.click("button[type='submit'], .login-btn, [class*='login']")
                page.wait_for_timeout(3000)

                logger.info("  로그인 완료")
            except Exception as e:
                logger.warning(f"  로그인 실패: {e} — 비로그인으로 진행")
        else:
            # 비로그인: 목록 페이지만 방문해서 기본 쿠키 수집
            logger.info("비로그인 세션으로 쿠키 수집 중...")
            page.goto(f"{BASE_URL}/enterprise/requests", wait_until="load", timeout=PAGE_TIMEOUT)
            page.wait_for_timeout(3000)

        # Playwright 쿠키 → requests 세션 이전
        for c in context.cookies():
            session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain", ".kmong.com"),
            )

        browser.close()

    logger.info(f"세션 쿠키 {len(session.cookies)}개 설정 완료")
    return session


# ──────────────────────────────────────────────
# API 호출
# ──────────────────────────────────────────────

def fetch_list_api(page_num: int, session: requests.Session) -> dict:
    """
    GET /api/custom-project/v1/requests?category_list=6&page=N
    JSON 응답 반환.
    """
    url = build_list_api_url(page_num)
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"  목록 API 실패 (page={page_num}): {e}")
        return {}


def fetch_detail_api(request_id: str, session: requests.Session) -> dict:
    """
    GET /api/custom-project/v1/requests/{id}
    없으면 상세 페이지 HTML fallback.
    """
    url = f"{LIST_API}/{request_id}"
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        logger.debug(f"  상세 API 실패 (id={request_id}): {e}")
    return {}


# ──────────────────────────────────────────────
# 데이터 정규화
# ──────────────────────────────────────────────

def _safe(obj, *keys, default=""):
    for k in keys:
        if isinstance(obj, dict):
            obj = obj.get(k, default)
        else:
            return default
    return obj if obj is not None else default


def _format_budget(amount) -> str:
    """
    크몽 amount 필드 (원 단위 정수) → 읽기 쉬운 문자열.
    예: 22000000 → "2,200만원"
         500000  → "50만원"
    """
    try:
        amt = int(amount)
        if amt >= 10000:
            man = amt // 10000
            rem = (amt % 10000) // 1000
            if rem:
                return f"{man:,}만 {rem}천원"
            return f"{man:,}만원"
        return f"{amt:,}원"
    except (ValueError, TypeError):
        return str(amount)


def _format_deadline(deadline_val) -> tuple[str, str]:
    """
    크몽 deadline 필드 (마감까지 남은 일수 정수) → (deadline_str, end_date_str).

    실제 API 응답: deadline = 8  (오늘로부터 8일 후 마감)
    반환:
      deadline_str = "D-8"
      end_date_str = "2026-04-01"  (오늘 + N일)
    """
    from datetime import date, timedelta
    try:
        days = int(deadline_val)
        end  = date.today() + timedelta(days=days)
        return f"D-{days}", end.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        # 이미 문자열인 경우 그대로 반환
        s = str(deadline_val)
        return s, ""


def normalize_item(item: dict) -> KmongJob | None:
    """
    크몽 API 응답 아이템 → KmongJob.

    실제 확인된 API 필드 (2026-03 기준):
      id            : 공고 ID
      title         : 제목
      content       : 본문 (목록), description (상세)
      amount        : 예산 (원 단위 정수)
      deadline      : 마감까지 남은 일수 (정수)
      days          : 프로젝트 기간 (일수 정수)
      status        : APPROVAL / CLOSED 등
      project_type  : OUTSOURCING / SI 등
      proposal_count: 제안 수
      breadcrumb    : 카테고리 경로 (예: "IT·프로그래밍 / 웹사이트 개발")
      category.cat1_name / cat2_name : 카테고리명
      answers       : Q&A 리스트

    등록일(createdAt) API 미제공 → 수집 시각(collected_at)으로 대체
    """
    request_id = str(item.get("id") or item.get("requestId") or "")
    if not request_id:
        return None

    title = str(item.get("title") or item.get("name") or "")

    # ── 카테고리 ──────────────────────────────
    # breadcrumb: "IT·프로그래밍 / 웹사이트 개발"
    breadcrumb   = str(item.get("breadcrumb") or "")
    crumb_parts  = [p.strip() for p in breadcrumb.split("/")]
    category     = crumb_parts[0] if crumb_parts else ""
    sub_category = crumb_parts[1] if len(crumb_parts) > 1 else ""

    # category 딕셔너리 방식 fallback
    cat_dict = item.get("category") or {}
    if isinstance(cat_dict, dict):
        category     = category     or cat_dict.get("cat1_name") or ""
        sub_category = sub_category or cat_dict.get("cat2_name") or ""

    # ── 예산: amount (원 단위 정수) ────────────
    amount = item.get("amount") or item.get("budget") or 0
    budget = _format_budget(amount) if amount else ""

    # ── 본문 ──────────────────────────────────
    description = str(
        item.get("description") or item.get("content") or item.get("body") or ""
    )

    # ── 날짜 처리 ──────────────────────────────
    # deadline: 마감까지 남은 일수 (정수)
    # → "D-N" 문자열과 실제 날짜 계산
    raw_deadline = item.get("deadline")
    if raw_deadline is not None:
        deadline, end_date = _format_deadline(raw_deadline)
    else:
        deadline, end_date = "", ""

    # 등록일: API 미제공 → 빈 값 (collected_at으로 대체)
    posted_at  = ""
    start_date = ""

    # ── 프로젝트 기간: days (일수 정수) ─────────
    raw_days = item.get("days")
    if raw_days:
        try:
            d = int(raw_days)
            # 30일 단위로 개월 환산
            if d >= 30:
                months = round(d / 30)
                project_duration = f"{months}개월" if months < 12 else f"{months // 12}년"
            else:
                project_duration = f"{d}일"
        except (ValueError, TypeError):
            project_duration = str(raw_days)
    else:
        project_duration = parse_project_duration(description)

    # ── 상태 ──────────────────────────────────
    raw_status = str(item.get("status") or "")
    STATUS_MAP = {
        "APPROVAL": "모집중",
        "CLOSED":   "마감",
        "COMPLETE": "완료",
        "CANCEL":   "취소",
    }
    status = STATUS_MAP.get(raw_status.upper(), raw_status)

    # ── 유형 ──────────────────────────────────
    TYPE_MAP = {
        "OUTSOURCING": "외주",
        "SI":          "SI",
        "SM":          "SM",
    }
    raw_type     = str(item.get("project_type") or item.get("projectType") or "")
    project_type = TYPE_MAP.get(raw_type.upper(), raw_type)

    # ── 스킬: 본문에서 추출 (API 미제공) ────────
    raw_skills = item.get("skills") or item.get("tags") or item.get("techStack") or []
    skills = [s if isinstance(s, str) else _safe(s, "name") for s in raw_skills]
    if not skills and description:
        skills = extract_skills(description)

    # ── 통계 ──────────────────────────────────
    proposals = int(item.get("proposal_count") or item.get("proposalCount") or 0)
    views     = int(item.get("viewCount")       or item.get("views")         or 0)

    return KmongJob(
        request_id       = request_id,
        url              = build_detail_url(request_id),
        url_hash         = make_hash(request_id),
        title            = title,
        category         = category,
        sub_category     = sub_category,
        project_type     = project_type,
        budget           = budget,
        start_date       = start_date,
        end_date         = end_date,
        deadline         = deadline,
        project_duration = project_duration,
        skills           = skills,
        description      = description[:2000],
        posted_at        = posted_at,
        status           = status,
        views            = views,
        proposals        = proposals,
    )


def has_next_page(api_resp: dict, current_page: int) -> bool:
    """
    크몽 API 페이지네이션 판단.
    실제 응답 키: total, last_page, next_page_link, previous_page_link
    """
    # next_page_link 있으면 다음 페이지 존재
    next_link = api_resp.get("next_page_link")
    if next_link:
        return True

    # last_page 플래그
    last_page = api_resp.get("last_page")
    if last_page is not None:
        return not bool(last_page)

    # total 개수로 판단
    total = api_resp.get("total") or 0
    if total:
        return current_page * PER_PAGE < int(total)

    # 아이템 수로 fallback
    return len(_get_items(api_resp)) >= PER_PAGE


def _get_items(api_resp: dict) -> list:
    """API 응답에서 아이템 리스트 추출 — 경로 후보 순서대로 시도"""
    candidates = [
        api_resp.get("requests"),
        api_resp.get("items"),
        api_resp.get("list"),
        api_resp.get("content"),
        api_resp.get("data"),
    ]
    for c in candidates:
        if isinstance(c, list):
            return c
        if isinstance(c, dict):
            for v in c.values():
                if isinstance(v, list) and v:
                    return v
    return []


# ──────────────────────────────────────────────
# 크롤러 메인
# ──────────────────────────────────────────────

def crawl_kmong(
    max_pages: int  = MAX_PAGES,
    delay: float    = DETAIL_DELAY,
    email: str      = KMONG_EMAIL,
    password: str   = KMONG_PASSWORD,
) -> Generator[KmongJob, None, None]:
    """
    크몽 IT·프로그래밍 프로젝트 의뢰를 순회하며 yield.

    로그인 없이도 일부 공고 수집 가능.
    전체 수집이 필요하면 email/password 입력.

    사용 예:
        for job in crawl_kmong(max_pages=3, email="id@email.com", password="pw"):
            print(job.title, job.budget)
    """
    seen_hashes: set[str] = set()

    # 세션 (쿠키 포함)
    session = get_session_with_cookies(email, password)

    for page_num in range(1, max_pages + 1):
        logger.info(f"목록 수집 중: page={page_num}")

        api_resp = fetch_list_api(page_num, session)
        if not api_resp:
            logger.warning("  API 응답 없음 — 종료")
            break

        # 첫 페이지에서 실제 키 구조 로깅 (디버그용)
        if page_num == 1:
            logger.info(f"  API 응답 최상위 키: {list(api_resp.keys())}")

        items = _get_items(api_resp)
        logger.info(f"  → {len(items)}건 발견")

        if not items:
            logger.info("  항목 없음 — 수집 종료")
            break

        for item in items:
            # 첫 아이템 키 로깅 (디버그용)
            if page_num == 1 and items.index(item) == 0:
                logger.info(f"  첫 아이템 키: {list(item.keys()) if isinstance(item, dict) else item}")

            job = normalize_item(item)
            if not job:
                continue

            if job.url_hash in seen_hashes:
                logger.debug(f"  중복: {job.title[:30]}")
                continue
            seen_hashes.add(job.url_hash)

            # 오늘 날짜 기준 모집중 필터
            if not is_active(job.end_date or job.deadline, job.status):
                logger.debug(f"  마감 건너뜀: {job.title[:30]}")
                continue

            # 상세 API로 description 보완 (목록에 없을 경우)
            if not job.description:
                logger.info(f"  상세 보완: {job.title[:40]}")
                detail = fetch_detail_api(job.request_id, session)
                if detail:
                    raw = detail if isinstance(detail, dict) else {}
                    desc = str(
                        raw.get("description") or raw.get("content") or
                        raw.get("body") or ""
                    )
                    if desc:
                        job.description = desc[:2000]
                        if not job.skills:
                            job.skills = extract_skills(desc)
                time.sleep(delay)

            yield job

        if not has_next_page(api_resp, page_num):
            logger.info("  마지막 페이지 — 수집 완료")
            break

        time.sleep(0.5)


# ──────────────────────────────────────────────
# CLI 실행
# ──────────────────────────────────────────────

if __name__ == "__main__":
    # 로그인이 필요하면 아래에 입력
    EMAIL    = ""
    PASSWORD = ""

    results = []
    for job in crawl_kmong(max_pages=2, email=EMAIL, password=PASSWORD):
        results.append(asdict(job))
        print(f"\n{'='*60}")
        print(f"제목      : {job.title}")
        print(f"URL       : {job.url}")
        print(f"ID        : {job.request_id}")
        print(f"카테고리  : {job.category} > {job.sub_category}")
        print(f"유형      : {job.project_type}")
        print(f"예산      : {job.budget}")
        print(f"시작일    : {job.start_date}")
        print(f"마감일    : {job.end_date}")
        print(f"마감(원본): {job.deadline}")
        print(f"프로젝트기간: {job.project_duration}")
        print(f"기술      : {', '.join(job.skills)}")
        print(f"등록일    : {job.posted_at}")
        print(f"상태      : {job.status}")
        print(f"조회/제안 : {job.views} / {job.proposals}")
        print(f"설명      : {job.description[:80]}")

    with open("kmong_jobs.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"\n총 {len(results)}건 수집 완료 → kmong_jobs.json")