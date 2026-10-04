-- ============================================================
-- 거시경제 지표(환율·금리·WTI) 저장용 스키마
-- 소스: 원/달러 환율 + 금리(통안증권1년/국고채3년/국고채10년) = 한국은행 ECOS OpenAPI
--       WTI(서부텍사스유) = 미국 연준 FRED API
-- series 값: 'usdkrw'(원/달러), 'msb1y'(통안증권1년), 'ktb3y'(국고채3년),
--            'ktb10y'(국고채10년), 'wti'(WTI 현물가)
-- 날짜별 전체 시계열을 저장해둔다(분기말/공시일만 골라 받는 게 아니라) — ECOS/FRED 둘 다
-- 기간을 지정하면 한 번의 호출로 그 기간 전체를 돌려주기 때문에, 특정 날짜만 따로 여러 번
-- 조회하는 것보다 전체를 한 번에 받아 저장해두는 쪽이 호출 횟수도 적고 이후 "그 날짜 또는 그 이전
-- 가장 최근 값"을 조회(공시일 기준 point-in-time 조회, 미래 데이터 참조 방지)하기도 쉽다.
CREATE TABLE IF NOT EXISTS macro_data (
  series TEXT NOT NULL,
  date   TEXT NOT NULL, -- YYYYMMDD
  value  REAL,
  PRIMARY KEY (series, date)
);
