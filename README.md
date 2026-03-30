# IT 프로젝트 공고 수집기

SISM, OKKY Jobs, 프리모아, 크몽에서 IT 개발 프로젝트 공고를 자동 수집하는 도구입니다.

## 프로젝트 구조

```
it-project-collector/
├── parser/
│   ├── sism_parser.py
│   ├── okky_parser.py
│   ├── freemoa_parser.py
│   └── kmong_parser.py
├── engine.py       # 수집 실행 진입점
├── pipeline.py     # 데이터 처리 파이프라인
├── cache.py        # Redis 캐시
├── db.py           # DB 연결 및 upsert
├── settings.py     # 환경변수 설정
├── dashboard.py    # Streamlit 대시보드
├── schema.sql      # PostgreSQL 스키마
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## 시작하기

### 1. 환경변수 설정

```bash
cp .env.example .env
# .env 파일을 열어 실제 값으로 수정
```

### 2. Docker로 DB/Redis 실행

```bash
docker compose up -d
```

### 3. 가상환경 및 패키지 설치

```bash
py -3.14 -m venv .venv
.venv\Scripts\activate        # Windows
source .venv/bin/activate     # Mac/Linux

pip install -r requirements.txt
playwright install chromium
```

### 4. 실행

```bash
# 즉시 1회 수집
python engine.py

# 스케줄 모드 (09:00, 15:00, 21:00 자동 실행)
python engine.py --schedule

# 특정 소스만 수집
python engine.py --source sism okky

# 대시보드 실행
streamlit run dashboard.py
```

## 수집 대상

| 사이트 | 카테고리 | 방식 |
|---|---|---|
| SISM | 개발 | Playwright |
| OKKY Jobs | 계약직 | Playwright + __NEXT_DATA__ |
| 프리모아 | 개발 · 모집중 | Playwright + XHR API |
| 크몽 | IT·프로그래밍 | REST API |

## 요구사항

- Python 3.14 이상
- Docker Desktop


## 트러블 슈팅

### Redis Lock 걸렸을 때 조치 방법
```
# Redis 접속
docker exec -it it_collector_redis redis-cli -a redis1234

# 락 키 확인
keys jobs:lock:*

# SISM 락 삭제
del jobs:lock:sism

# 전체 락 삭제
del jobs:lock:sism jobs:lock:okky jobs:lock:freemoa jobs:lock:kmong
```