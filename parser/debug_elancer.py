"""
이랜서 상주/재택 페이지 구조 디버거
"""
import json, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

URLS = {
    "상주": "https://www.elancer.co.kr/list-partner",
    "재택": "https://www.elancer.co.kr/list-partner?pf=%ED%84%B4%ED%82%A4",
}

xhr_responses = []

def on_resp(resp):
    ct = resp.headers.get("content-type","")
    if "json" in ct and "elancer" in resp.url:
        try:
            body = resp.json()
            xhr_responses.append({"url": resp.url[:200], "body": body})
        except Exception:
            pass

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
        locale="ko-KR",
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()
    page.on("response", on_resp)

    for label, url in URLS.items():
        xhr_responses.clear()
        print(f"\n{'='*60}")
        print(f"▶ {label} 페이지: {url}")
        print('='*60)
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(4000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        html = page.content()
        soup = BeautifulSoup(html, "lxml")
        print(f"HTML 길이: {len(html)}")
        print(f"현재 URL: {page.url}")

        # __NEXT_DATA__
        tag = soup.find("script", id="__NEXT_DATA__")
        if tag and tag.string:
            data = json.loads(tag.string)
            pp = data.get("props",{}).get("pageProps",{})
            print(f"\n[__NEXT_DATA__] pageProps 키: {list(pp.keys())}")

            def scan(d, prefix="", depth=0):
                if depth > 5 or not isinstance(d, dict): return
                for k, v in d.items():
                    full = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, list) and v:
                        print(f"  LIST {full} → {len(v)}건")
                        if isinstance(v[0], dict):
                            print(f"    아이템 키: {list(v[0].keys())}")
                            # 첫 아이템 전체 출력
                            for fk, fv in v[0].items():
                                print(f"    {fk}: {str(fv)[:80]}")
                    elif isinstance(v, dict):
                        scan(v, full, depth+1)
                    elif v and not isinstance(v, (list, dict)):
                        if any(x in k.lower() for x in ["total","count","page"]):
                            print(f"  {full}: {v}")
            scan(pp)
        else:
            print("\n[__NEXT_DATA__] 없음")

        # XHR API 응답
        print(f"\n[XHR JSON] {len(xhr_responses)}개")
        for r in xhr_responses:
            print(f"\n  URL: {r['url']}")
            body = r["body"]
            if isinstance(body, dict):
                print(f"  키: {list(body.keys())[:10]}")
                for k, v in body.items():
                    if isinstance(v, list) and v:
                        print(f"  LIST {k} → {len(v)}건")
                        if isinstance(v[0], dict):
                            print(f"    아이템 키: {list(v[0].keys())}")
                            for fk, fv in v[0].items():
                                print(f"    {fk}: {str(fv)[:80]}")

        # 공고 링크
        links = {a["href"] for a in soup.find_all("a", href=True)
                 if re.search(r"\d{4,}", a["href"])}
        print(f"\n[공고 링크 샘플] {len(links)}개")
        for l in sorted(links)[:5]: print(f"  {l}")

        # 텍스트 샘플 — 마감일/모집/예산 등
        print("\n[텍스트 샘플]")
        for el in soup.find_all(string=re.compile("마감|모집|예산|단가|만원|원/월|기간|시작|상주|재택|D-")):
            t = el.strip()
            if 2 < len(t) < 60: print(f"  '{t}'")

        # 첫 공고 카드 전체 텍스트 추출
        print("\n[첫 카드 텍스트 덤프]")
        for card in soup.find_all(attrs={"class": True})[:200]:
            cls = " ".join(card.get("class",[]))
            txt = card.get_text(separator="|", strip=True)
            if 50 < len(txt) < 400 and any(k in txt for k in ["만원","원/월","개월","마감","D-"]):
                print(f"  class='{cls[:50]}'\n  {txt[:300]}")
                print()
                break

        with open(f"elancer_{label}.html","w",encoding="utf-8") as f: f.write(html)
        print(f"elancer_{label}.html 저장 완료")

    browser.close()