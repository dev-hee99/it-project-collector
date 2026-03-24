"""
이랜서 API 파라미터 & 응답 구조 완전 분석
"""
import json, re, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from playwright.sync_api import sync_playwright

URLS = {
    "상주": "https://www.elancer.co.kr/list-partner",
    "재택": "https://www.elancer.co.kr/list-partner?pf=%ED%84%B4%ED%82%A4",
}

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/123.0 Safari/537.36",
        locale="ko-KR",
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()

    # 요청 파라미터도 캡처
    api_calls = []

    def on_req(req):
        if "api.elancer.co.kr/api/pjt" in req.url:
            entry = {
                "method": req.method,
                "url": req.url,
                "post_data": req.post_data,
                "headers": dict(req.headers),
            }
            api_calls.append(entry)
            print(f"\n[REQ] {req.method} {req.url}")
            if req.post_data:
                print(f"  POST: {req.post_data[:500]}")

    def on_resp(resp):
        if "api.elancer.co.kr/api/pjt" in resp.url:
            try:
                body = resp.json()
                print(f"\n[RESP] {resp.url[:120]}")
                print(f"  status: {resp.status}")
                print(f"  최상위 키: {list(body.keys()) if isinstance(body,dict) else type(body)}")
                data = body.get("data", {}) if isinstance(body, dict) else {}
                if isinstance(data, dict):
                    print(f"  data 키: {list(data.keys())}")
                    for k, v in data.items():
                        if isinstance(v, list) and v:
                            print(f"  LIST data.{k} → {len(v)}건")
                            if isinstance(v[0], dict):
                                print(f"    아이템 키: {list(v[0].keys())}")
                                # 첫 아이템 전체 필드 출력
                                for fk, fv in v[0].items():
                                    print(f"    {fk}: {str(fv)[:100]}")
                        elif not isinstance(v, (list, dict)):
                            print(f"  data.{k}: {v}")
                elif isinstance(data, list) and data:
                    print(f"  data 리스트 → {len(data)}건")
                    if isinstance(data[0], dict):
                        print(f"  첫 아이템 키: {list(data[0].keys())}")
                        for fk, fv in data[0].items():
                            print(f"    {fk}: {str(fv)[:100]}")
            except Exception as e:
                print(f"  JSON 파싱 실패: {e}")

    page.on("request", on_req)
    page.on("response", on_resp)

    for label, url in URLS.items():
        print(f"\n{'='*60}")
        print(f"▶ {label}: {url}")
        print('='*60)
        api_calls.clear()
        page.goto(url, wait_until="networkidle", timeout=60000)
        page.wait_for_timeout(4000)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)

        # 2페이지 클릭/스크롤로 pagination 파라미터 확인
        print("\n  → 페이지네이션 파라미터 탐색 중...")
        try:
            # 다음 페이지 버튼 클릭 시도
            next_btn = page.locator("button:has-text('다음'), [aria-label='다음'], .next, [class*='next']").first
            if next_btn.is_visible(timeout=2000):
                next_btn.click()
                page.wait_for_timeout(2000)
        except Exception:
            pass

        print(f"\n  총 API 호출: {len(api_calls)}개")

    browser.close()