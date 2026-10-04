-- ============================================================
-- 모멘텀 스크리닝 스키마 (02_market_screening.md 명세 기준, 무료 플랜 적용 버전)
-- v2: 데이터 소스를 네이버 스크래핑 → 공공데이터포털(data.go.kr) "금융위원회_주식시세정보"
--     (getStockPriceInfo_V2) API로 교체. 이 API는 basDt+beginTrPrc로 "그날 유동성 기준을
--     통과한 전체 종목"을, likeSrtnCd+beginBasDt~endBasDt로 "한 종목의 가격 이력(기간)"을
--     한 번에 조회할 수 있어 전체 종목을 하나씩 조회할 필요가 없다.
-- ============================================================

-- 1. 백필 대기열: "유동성 기준은 통과했지만 아직 전체 가격 이력을 못 받은" 종목만 여기 쌓인다.
--    (과거 버전처럼 corp_master 전체를 넣는 게 아니라, 매일 유니버스 갱신 때 새로 통과한 종목만 들어옴)
CREATE TABLE IF NOT EXISTS market_fetch_queue (
  stock_code TEXT PRIMARY KEY,
  corp_name  TEXT,
  queued_at  TEXT
);

-- 2. 유동성 통과 여부 + 백필 상태 기록
--    - passed: 가장 최근 유니버스 갱신 시점에 거래대금 기준을 통과했는지
--    - backfilled: data.go.kr로 전체 가격 이력(약 250일치)을 받아온 적이 있는지
--      (한 번 받으면 그 뒤로는 매일 유니버스 갱신 호출이 "오늘 하루치"를 자동으로 덧붙여주므로 다시 반복할 필요 없음)
CREATE TABLE IF NOT EXISTS market_universe_status (
  stock_code        TEXT PRIMARY KEY,
  corp_name         TEXT,
  passed            INTEGER NOT NULL,  -- 1=유동성 기준 통과, 0=미달
  avg_trading_value REAL,              -- 가장 최근 조회일의 거래대금(data.go.kr trPrc)
  days_fetched      INTEGER,           -- (레거시 컬럼, 더 이상 채우지 않음 — 네이버 스크래핑 시절 평균거래대금 계산용)
  checked_at        TEXT,              -- 가장 최근에 유니버스 갱신이 일어난 시각
  backfilled        INTEGER NOT NULL DEFAULT 0, -- 1=전체 가격이력 백필 완료
  backfilled_at     TEXT               -- 백필 완료 시각(하루 처리 한도 집계 기준)
);
-- 의도적으로 passed/backfilled에 인덱스를 두지 않았다: D1은 인덱스가 걸린 컬럼에 쓸 때마다
-- "테이블 행 + 인덱스 행"으로 쓰기가 중복 집계되어(공식 문서: "two rows written: one to the
-- table itself, and one to the index") 하루 쓰기 한도(10만행/일)를 예상보다 훨씬 빨리 소진시킨다.
-- 이 테이블은 최대 수천 행 수준이라 인덱스 없이 WHERE로 걸러도 속도에 문제가 없다.

-- 3. 일별 가격/거래량 원본 (유동성 통과 종목만 저장 — §10 원칙: 원천은 저장, 파생지표는 그때그때 계산)
CREATE TABLE IF NOT EXISTS market_raw_daily (
  market_date   TEXT NOT NULL,
  stock_code    TEXT NOT NULL,
  close_price   REAL,
  volume        INTEGER,
  trading_value REAL,
  PRIMARY KEY (market_date, stock_code)
);
-- 여기도 같은 이유로 추가 인덱스를 두지 않았다 — 이 테이블이 쓰기가 가장 잦은 테이블이라 영향이 가장 크다.
-- (조회는 /api/market/top30에서 가끔 전체 스캔하는 정도라 인덱스 없이도 D1의 읽기 한도(500만행/일)로 충분히 감당된다)

-- 참고: market(KOSPI/KOSDAQ 구분)은 data.go.kr 응답의 mrktCtg 필드로 이제 얻을 수 있지만,
-- 1차 버전에서는 저장하지 않았다(모멘텀 스코어 계산에 필수가 아님). 필요해지면 market_universe_status에
-- mrktCtg 컬럼을 추가해 같이 저장하면 된다. is_trading_halt(거래정지)는 여전히 별도 소스가 필요하다.

-- ※ 이 스크립트를 이전에 한 번 실행해서 market_universe_status 테이블이 이미 존재한다면
--   (위 CREATE TABLE IF NOT EXISTS는 무시되어) backfilled/backfilled_at 컬럼이 추가되지 않는다.
--   그럴 때는 아래 두 줄만 D1 콘솔에서 추가로 실행해주면 된다.
-- ALTER TABLE market_universe_status ADD COLUMN backfilled INTEGER NOT NULL DEFAULT 0;
-- ALTER TABLE market_universe_status ADD COLUMN backfilled_at TEXT;

-- ※ 이전 버전의 이 스크립트로 이미 인덱스 3개(idx_universe_status_passed, idx_universe_status_backfilled,
--   idx_market_raw_stock_date)를 만들어두셨다면, 쓰기 비용을 줄이기 위해 아래 3줄로 지워주세요
--   (데이터는 전혀 건드리지 않고 인덱스만 제거합니다).
-- DROP INDEX IF EXISTS idx_universe_status_passed;
-- DROP INDEX IF EXISTS idx_universe_status_backfilled;
-- DROP INDEX IF EXISTS idx_market_raw_stock_date;
