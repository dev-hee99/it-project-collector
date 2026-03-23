"""
위시켓 구조 디버거 v2 — 네트워크 요청 전체 캡처 + 대기 강화
"""
import json, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

LIST_URL = "https://www.wishket.com/project/?category=dev&is_open=true&ordering=-updated"

xhr_requests  = []
xhr_responses = []

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
        locale="ko-KR",
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()

    # 모든 요청 캡처
    def on_req(req):
        if req.resource_type in ("fetch", "xhr", "document"):
            xhr_requests.append(f"[{req.method}] {req.url[:200]}")

    # 모든 응답 캡처
    def on_resp(resp):
        ct = resp.headers.get("content-type","")
        if "json" in ct and "wishket" in resp.url:
            try:
                body = resp.json()
                xhr_responses.append({"url": resp.url[:200], "body": body})
            except Exception:
                pass

    page.on("request",  on_req)
    page.on("response", on_resp)

    print("=== 위시켓 로드 중 ===")
    page.goto(LIST_URL, wait_until="networkidle", timeout=60000)
    page.wait_for_timeout(4000)

    # 스크롤해서 lazy load 트리거
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    page.wait_for_timeout(2000)

    html = page.content()
    soup = BeautifulSoup(html, "lxml")
    print(f"HTML 길이: {len(html)}")

    # 1. __NEXT_DATA__
    print("\n▶ 1. __NEXT_DATA__")
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        data = json.loads(tag.string)
        pp = data.get("props",{}).get("pageProps",{})
        print(f"pageProps 최상위 키: {list(pp.keys())}")
        def scan(d, prefix="", depth=0):
            if depth > 4: return
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, list) and v:
                        print(f"  [LIST] {prefix}.{k} → {len(v)}건")
                        if isinstance(v[0], dict):
                            print(f"    키: {list(v[0].keys())[:12]}")
                            # 날짜/마감 관련 필드
                            for fk, fv in v[0].items():
                                if any(x in fk.lower() for x in ["date","time","dead","close","end","start","create","regist","expire"]):
                                    print(f"    날짜필드 {fk}: {str(fv)[:60]}")
                    elif isinstance(v, dict):
                        scan(v, f"{prefix}.{k}", depth+1)
        scan(pp, "pageProps")
    else:
        print("  없음")

    # 2. 캡처된 JSON API 응답
    print(f"\n▶ 2. wishket JSON API 응답 ({len(xhr_responses)}개)")
    for r in xhr_responses[:5]:
        print(f"\n  URL: {r['url']}")
        body = r["body"]
        if isinstance(body, dict):
            print(f"  최상위 키: {list(body.keys())[:8]}")
            for k, v in body.items():
                if isinstance(v, list) and v:
                    print(f"  [LIST] {k} → {len(v)}건")
                    if isinstance(v[0], dict):
                        print(f"    첫 아이템 키: {list(v[0].keys())[:12]}")
                        for fk, fv in v[0].items():
                            if any(x in fk.lower() for x in ["date","time","dead","close","end","start","create","regist","expire","status"]):
                                print(f"    {fk}: {str(fv)[:60]}")
        elif isinstance(body, list) and body:
            print(f"  리스트 {len(body)}건")
            if isinstance(body[0], dict):
                print(f"  첫 아이템 키: {list(body[0].keys())[:12]}")

    # 3. 전체 XHR 요청
    print(f"\n▶ 3. 전체 요청 ({len(xhr_requests)}개)")
    for u in xhr_requests:
        if "wishket" in u.lower(): print(f"  {u}")

    # 4. 공고 링크
    print("\n▶ 4. 공고 링크")
    links = set()
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/project/" in h and re.search(r"\d{3,}", h):
            links.add(h)
    for l in sorted(links)[:8]: print(f"  {l}")

    # 5. 공고 카드 셀렉터 후보
    print("\n▶ 5. 공고 카드 후보")
    for el in soup.find_all(attrs={"class": True}):
        cls = " ".join(el.get("class",[]))
        if any(k in cls for k in ["ProjectCard","project-card","ProjectList","project-item","ProjectItem"]):
            txt = el.get_text(separator=" ", strip=True)[:80]
            print(f"  class='{cls[:60]}' → {txt}")

    # 6. 상태/마감 텍스트
    print("\n▶ 6. 마감/모집 텍스트")
    for el in soup.find_all(string=re.compile("모집중|마감|D-\d+|진행중|오픈")):
        t = el.strip()
        if 1 < len(t) < 30: print(f"  '{t}'")

    with open("wishket_debug.html","w",encoding="utf-8") as f: f.write(html)
    print("\nwishket_debug.html 저장 완료")
    browser.close()