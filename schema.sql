-- ============================================================
-- IT 프리랜서 공고 수집기 DB 스키마
-- PostgreSQL 14+
-- ============================================================


-- ── 확장 ──────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";   -- 제목 유사도 검색용


-- ── ENUM 타입 ─────────────────────────────────────────────────
CREATE TYPE job_source AS ENUM ('sism', 'okky', 'freemoa', 'kmong');

CREATE TYPE job_status AS ENUM ('active', 'closed', 'unknown');


-- ============================================================
-- 메인 테이블: jobs
-- ============================================================
CREATE TABLE IF NOT EXISTS jobs (
    -- PK
    id                BIGSERIAL       PRIMARY KEY,

    -- 출처 식별
    source            job_source      NOT NULL,
    source_id         VARCHAR(100)    NOT NULL,          -- 원본 사이트 ID (pno, request_id 등)
    url               TEXT            NOT NULL,
    url_hash          CHAR(16)        NOT NULL,          -- SHA-256 앞 16자 (중복 체크 키)

    -- 공고 기본 정보
    title             TEXT            NOT NULL,
    company           VARCHAR(200)    DEFAULT '',
    category          VARCHAR(100)    DEFAULT '',
    sub_category      VARCHAR(100)    DEFAULT '',
    work_type         VARCHAR(100)    DEFAULT '',        -- 상주/원격/도급/기간제 등
    location          VARCHAR(200)    DEFAULT '',

    -- 기술 스택 (정규화된 스킬명 배열)
    skills            TEXT[]          DEFAULT '{}',

    -- 금액 / 기간
    budget            VARCHAR(200)    DEFAULT '',        -- 예: "500만원", "300~500만원"
    project_duration  VARCHAR(100)    DEFAULT '',        -- 예: "6개월", "180일"

    -- 날짜
    start_date        VARCHAR(30)     DEFAULT '',        -- 공고 시작일
    end_date          VARCHAR(30)     DEFAULT '',        -- 공고 마감일 (날짜형)
    deadline          VARCHAR(30)     DEFAULT '',        -- 마감 표시 (D-N 등)
    posted_at         VARCHAR(50)     DEFAULT '',        -- 등록일 (원본 텍스트)

    -- 본문
    description       TEXT            DEFAULT '',

    -- 상태
    status            job_status      NOT NULL DEFAULT 'unknown',
    is_active         BOOLEAN         NOT NULL DEFAULT TRUE,  -- 오늘 기준 모집중 여부

    -- 수집 메타
    collected_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- 제약
    CONSTRAINT uq_jobs_url_hash UNIQUE (url_hash)
);

COMMENT ON TABLE  jobs                  IS 'IT 프리랜서 공고 수집 테이블';
COMMENT ON COLUMN jobs.url_hash         IS 'SHA-256(url) 앞 16자, 중복 방지 키';
COMMENT ON COLUMN jobs.skills           IS '정규화된 기술 스택 배열 (예: {React, Spring, AWS})';
COMMENT ON COLUMN jobs.is_active        IS '수집 시점 기준 모집중 여부';
COMMENT ON COLUMN jobs.source_id        IS '각 사이트 고유 ID (pno / job_id / request_id 등)';


-- ============================================================
-- 스킬 집계 테이블: skills
-- 빠른 스킬별 통계 조회용 역정규화 테이블
-- ============================================================
CREATE TABLE IF NOT EXISTS job_skills (
    job_id   BIGINT       NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill    VARCHAR(100) NOT NULL,
    PRIMARY KEY (job_id, skill)
);

COMMENT ON TABLE job_skills IS 'jobs.skills 배열을 행으로 풀어낸 역정규화 테이블 (집계 최적화)';


-- ============================================================
-- 인덱스
-- ============================================================

-- 중복 체크 (이미 UNIQUE 인덱스 생성됨)
-- CREATE UNIQUE INDEX ON jobs(url_hash);

-- 소스별 조회
CREATE INDEX IF NOT EXISTS idx_jobs_source
    ON jobs(source);

-- 모집중 필터링 (가장 빈번한 조회 조건)
CREATE INDEX IF NOT EXISTS idx_jobs_is_active
    ON jobs(is_active)
    WHERE is_active = TRUE;

-- 수집일 내림차순 (최신 공고 조회)
CREATE INDEX IF NOT EXISTS idx_jobs_collected_at
    ON jobs(collected_at DESC);

-- 마감일 조회
CREATE INDEX IF NOT EXISTS idx_jobs_end_date
    ON jobs(end_date)
    WHERE end_date != '';

-- 스킬 배열 GIN 인덱스 (skills @> '{React}' 쿼리 최적화)
CREATE INDEX IF NOT EXISTS idx_jobs_skills_gin
    ON jobs USING GIN(skills);

-- 제목 trigram 인덱스 (LIKE '%React%' 최적화)
CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm
    ON jobs USING GIN(title gin_trgm_ops);

-- 스킬 집계 테이블
CREATE INDEX IF NOT EXISTS idx_job_skills_skill
    ON job_skills(skill);


-- ============================================================
-- updated_at 자동 갱신 트리거
-- ============================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_jobs_updated_at ON jobs;
CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();


-- ============================================================
-- job_skills 자동 동기화 트리거
-- jobs.skills 배열 변경 시 job_skills 테이블 자동 갱신
-- ============================================================
CREATE OR REPLACE FUNCTION sync_job_skills()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    -- 기존 스킬 삭제
    DELETE FROM job_skills WHERE job_id = NEW.id;
    -- 새 스킬 삽입
    INSERT INTO job_skills (job_id, skill)
    SELECT NEW.id, unnest(NEW.skills)
    ON CONFLICT DO NOTHING;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_job_skills ON jobs;
CREATE TRIGGER trg_sync_job_skills
    AFTER INSERT OR UPDATE OF skills ON jobs
    FOR EACH ROW
    EXECUTE FUNCTION sync_job_skills();


-- ============================================================
-- UPSERT 프로시저
-- 동일 url_hash 존재 시 주요 필드만 UPDATE (불필요한 쓰기 최소화)
-- ============================================================
CREATE OR REPLACE PROCEDURE upsert_job(
    p_source            job_source,
    p_source_id         VARCHAR,
    p_url               TEXT,
    p_url_hash          CHAR(16),
    p_title             TEXT,
    p_company           VARCHAR,
    p_category          VARCHAR,
    p_sub_category      VARCHAR,
    p_work_type         VARCHAR,
    p_location          VARCHAR,
    p_skills            TEXT[],
    p_budget            VARCHAR,
    p_project_duration  VARCHAR,
    p_start_date        VARCHAR,
    p_end_date          VARCHAR,
    p_deadline          VARCHAR,
    p_posted_at         VARCHAR,
    p_description       TEXT,
    p_status            job_status,
    p_is_active         BOOLEAN
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO jobs (
        source, source_id, url, url_hash,
        title, company, category, sub_category, work_type, location,
        skills, budget, project_duration,
        start_date, end_date, deadline, posted_at,
        description, status, is_active
    ) VALUES (
        p_source, p_source_id, p_url, p_url_hash,
        p_title, p_company, p_category, p_sub_category, p_work_type, p_location,
        p_skills, p_budget, p_project_duration,
        p_start_date, p_end_date, p_deadline, p_posted_at,
        p_description, p_status, p_is_active
    )
    ON CONFLICT (url_hash) DO UPDATE SET
        -- 변동 가능한 필드만 업데이트
        title            = EXCLUDED.title,
        status           = EXCLUDED.status,
        is_active        = EXCLUDED.is_active,
        deadline         = EXCLUDED.deadline,
        end_date         = EXCLUDED.end_date,
        skills           = EXCLUDED.skills,
        budget           = EXCLUDED.budget,
        description      = CASE
                               WHEN LENGTH(EXCLUDED.description) > LENGTH(jobs.description)
                               THEN EXCLUDED.description
                               ELSE jobs.description
                           END,
        updated_at       = NOW()
    -- 변경이 없으면 아무것도 하지 않음 (쓰기 최소화)
    WHERE
        jobs.status    IS DISTINCT FROM EXCLUDED.status    OR
        jobs.is_active IS DISTINCT FROM EXCLUDED.is_active OR
        jobs.deadline  IS DISTINCT FROM EXCLUDED.deadline  OR
        jobs.skills    IS DISTINCT FROM EXCLUDED.skills;
END;
$$;


-- ============================================================
-- 만료 공고 자동 비활성화 (매일 자정 실행 권장)
-- ============================================================
CREATE OR REPLACE PROCEDURE deactivate_expired_jobs()
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE jobs
    SET
        is_active  = FALSE,
        status     = 'closed',
        updated_at = NOW()
    WHERE
        is_active = TRUE
        AND end_date != ''
        AND TO_DATE(
            REGEXP_REPLACE(end_date, '[./]', '-', 'g'),
            'YYYY-MM-DD'
        ) < CURRENT_DATE;

    RAISE NOTICE '만료 공고 비활성화 완료: % 건', ROW_COUNT;
END;
$$;


-- ============================================================
-- 유용한 뷰
-- ============================================================

-- 오늘 기준 모집중 공고 (가장 자주 쓰는 뷰)
CREATE OR REPLACE VIEW v_active_jobs AS
SELECT
    id, source, title, company, category,
    skills, budget, project_duration,
    start_date, end_date, deadline,
    work_type, location, url, collected_at
FROM jobs
WHERE is_active = TRUE
ORDER BY collected_at DESC;

-- 소스별 수집 통계
CREATE OR REPLACE VIEW v_stats_by_source AS
SELECT
    source,
    COUNT(*)                                    AS total,
    COUNT(*) FILTER (WHERE is_active = TRUE)    AS active,
    COUNT(*) FILTER (WHERE is_active = FALSE)   AS closed,
    MAX(collected_at)                           AS last_collected
FROM jobs
GROUP BY source
ORDER BY source;

-- 스킬별 공고 수 (상위 20개)
CREATE OR REPLACE VIEW v_top_skills AS
SELECT
    skill,
    COUNT(*)  AS job_count
FROM job_skills
JOIN jobs ON jobs.id = job_skills.job_id
WHERE jobs.is_active = TRUE
GROUP BY skill
ORDER BY job_count DESC
LIMIT 20;

