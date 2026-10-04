CREATE TABLE IF NOT EXISTS financial_raw (
  corp_code           TEXT NOT NULL,
  bsns_year           TEXT NOT NULL,
  reprt_code          TEXT NOT NULL,
  period_label        TEXT,
  period_order        INTEGER,   -- year*10 + (1분기=1,반기=2,3분기=3,사업보고서=4), 정렬용
  fs_div              TEXT,
  revenue             REAL,
  cogs                REAL,
  operating_income    REAL,
  net_income          REAL,
  total_equity        REAL,
  total_liabilities   REAL,
  cash                REAL,
  st_financial_assets REAL,
  ocf                 REAL,
  capex               REAL,
  fcf                 REAL,
  receivables         REAL,
  inventory           REAL,
  payables            REAL,
  updated_at          TEXT,
  PRIMARY KEY (corp_code, bsns_year, reprt_code)
);
CREATE INDEX IF NOT EXISTS idx_financial_raw_corp ON financial_raw (corp_code, period_order);

-- 4단계 추가분: 총주식수/자기주식수/주당배당금
ALTER TABLE financial_raw ADD COLUMN total_shares REAL;
ALTER TABLE financial_raw ADD COLUMN treasury_shares REAL;
ALTER TABLE financial_raw ADD COLUMN dividend_per_share REAL;

-- 5단계 추가분: ROIC 계산용 (투하자본 차감 항목)
ALTER TABLE financial_raw ADD COLUMN short_term_trading_securities REAL;
ALTER TABLE financial_raw ADD COLUMN fvpl_financial_assets REAL;
ALTER TABLE financial_raw ADD COLUMN fvoci_financial_assets REAL;
ALTER TABLE financial_raw ADD COLUMN investment_property REAL;

-- 5단계 추가분 v2: 새 IC 공식(영업 관점) + 5단계 ROE 분석 준비용
ALTER TABLE financial_raw ADD COLUMN other_receivables REAL;
ALTER TABLE financial_raw ADD COLUMN short_term_loans REAL;
ALTER TABLE financial_raw ADD COLUMN other_payables REAL;
ALTER TABLE financial_raw ADD COLUMN short_term_borrowings REAL;
ALTER TABLE financial_raw ADD COLUMN current_portion_lt_debt REAL;
ALTER TABLE financial_raw ADD COLUMN current_lease_liabilities REAL;
ALTER TABLE financial_raw ADD COLUMN tangible_assets REAL;
ALTER TABLE financial_raw ADD COLUMN intangible_assets REAL;
ALTER TABLE financial_raw ADD COLUMN right_of_use_assets REAL;
ALTER TABLE financial_raw ADD COLUMN parent_net_income REAL;
ALTER TABLE financial_raw ADD COLUMN pretax_income REAL;
ALTER TABLE financial_raw ADD COLUMN interest_expense REAL;
ALTER TABLE financial_raw ADD COLUMN parent_equity REAL;

-- 5단계 추가분 v3: 공시일자 기준 주가/PER/PBR/FCF Yield
ALTER TABLE financial_raw ADD COLUMN filing_date TEXT;
ALTER TABLE financial_raw ADD COLUMN price_at_filing REAL;
ALTER TABLE financial_raw ADD COLUMN per_at_filing REAL;
ALTER TABLE financial_raw ADD COLUMN pbr_at_filing REAL;
ALTER TABLE financial_raw ADD COLUMN fcf_yield_at_filing REAL;

-- 5단계 추가분 v4: 주식수 미공시 분기에 전기값 이월 시 출처 표시
ALTER TABLE financial_raw ADD COLUMN shares_source TEXT;

-- 5단계 추가분 v5: 공시시점 ROA(TTM), PEG(TTM)
ALTER TABLE financial_raw ADD COLUMN roa_at_filing REAL;
ALTER TABLE financial_raw ADD COLUMN peg_at_filing REAL;

-- 5단계 추가분 v6: 회계기간 말일(분기말/연말) 종가 — 공시일 주가와 비교해 상승률 계산용
ALTER TABLE financial_raw ADD COLUMN price_at_period_end REAL;

-- 5단계 추가분 v7: 공시시점 EPS(TTM), EPS 성장률(YoY) — PER/PEG 계산의 분자/분모 확인용
ALTER TABLE financial_raw ADD COLUMN eps_at_filing REAL;
ALTER TABLE financial_raw ADD COLUMN eps_growth_at_filing REAL;
