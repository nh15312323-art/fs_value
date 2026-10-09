// ============================================================
// Phase 5: ROIC/밸류에이션 계산 + 분기·연간 토글 + 헤더 고정
// ============================================================

const REPRT_CODES = [
  { code: "11013", label: "1분기", order: 1 },
  { code: "11012", label: "반기", order: 2 },
  { code: "11014", label: "3분기", order: 3 },
  { code: "11011", label: "사업보고서", order: 4 },
];

// Cron Trigger가 한 번 깨어날 때마다(짝수 분) 재무데이터 큐에서 처리하는 기간 수.
// 기간 1개당 DART 조회(CFS+실패시 OFS, 재시도 포함)+네이버 2회+D1 조회(TTM 계산용 여러 번)·저장을 합치면
// 최악의 경우 기간당 15~20개 정도 subrequest를 쓸 수 있어 보수적으로 2로 설정
// (무료 플랜 1회 호출당 50 subrequest 제한. 아래 모멘텀 큐는 홀수 분에 따로 처리해 예산을 분리했다).
const CRON_BATCH_SIZE = 2;

function buildPeriodList(startYear, endYear) {
  const list = [];
  for (let y = startYear; y <= endYear; y++) {
    for (const r of REPRT_CODES) list.push({ year: y, code: r.code, order: r.order, period_order: y * 10 + r.order });
  }
  return list;
}

// ============================================================
// 모멘텀 스크리닝 (02_market_screening.md 명세, 무료 플랜 적용)
// ============================================================
const MARKET_MIN_TRADING_VALUE = 1_500_000_000; // 거래대금 15억원 미만 종목은 유니버스에서 제외(유동성 필터, data.go.kr beginTrPrc로 서버에서 바로 필터링)
const MARKET_MIN_MCAP = 50_000_000_000;         // 시가총액 500억원 미만은 유니버스에서 제외(스크리닝 기본값 600억보다 조금 낮게 두어 경계 종목 이력을 유지)
const MARKET_APPEND_PAGE = 250;                 // 일일 시세 일괄 추가: 한 번의 틱(1분)에 처리하는 행 수(INSERT 약 16건 + 큐 등록 약 10건 → subrequest 50건 한도 안)
const MARKET_BACKFILL_CALENDAR_DAYS = 250;      // 120거래일 이상 확보 목적으로 달력일 기준 여유있게 요청(휴일/주말 포함)
// 한 틱(홀수 분)에 처리하는 종목 수. Worker 호출 1번에는 무료 플랜 기준 subrequest(= fetch + D1 쿼리 합산) 50건 한도가 있다.
//  - 오늘치 추가: 종목당 약 6~9건 → 3종목(≈27건)
//  - 신규 백필: 종목당 약 14건(KRX 1~2 + 멀티로우 INSERT 9 + 조회/upsert/삭제) → 2종목(≈28건)
const MARKET_CRON_BATCH_SIZE = 3;
const MARKET_NEW_BATCH_SIZE = 2;
// 하루 최대 "신규 종목 백필" 수 — 쓰기 한도 계산(중요):
// D1은 "테이블 행 + 그 행이 만드는 인덱스 항목"을 각각 쓰기 1건으로 센다. market_raw_daily의 PRIMARY KEY
// (market_date TEXT, stock_code TEXT)는 SQLite가 자동으로 별도 인덱스(sqlite_autoindex)를 만들기 때문에
// 인덱스를 따로 만들지 않아도 행 1개 = 쓰기 2건이다. 그래서 종목 1개 백필(약 170행) ≈ 340건 + 상태·큐 ≈ 350건.
// 예전 값 300종목 × 350 ≈ 105,000건은 그것만으로 무료 한도(하루 100,000건)를 넘겼다(유니버스 갱신 후 절반쯤
// 진행되다 한도가 소진된 원인). 이제 150종목 × 350 ≈ 52,500건으로 두고, 나머지(매일 오늘치 추가 약 1.4만,
// 재무 큐, 주간 스냅샷 등)에 약 3만 건 이상의 여유를 남긴다. 유동성 큰 종목부터 처리되므로 Top50 후보가
// 먼저 채워지고, 전체 유니버스 백필은 며칠에 걸쳐 끝난다.
const MARKET_DAILY_WRITE_CAP = 150;
let marketCapExhaustedDay = null; // 같은 isolate에서 오늘 한도 소진을 기억(매 분 COUNT 조회 방지)
let marketQueueReady = false;
async function ensureMarketQueueColumns(db) {
  if (marketQueueReady) return;
  try {
    await db.prepare("ALTER TABLE market_fetch_queue ADD COLUMN is_new INTEGER NOT NULL DEFAULT 0").run();
  } catch (e) {
    // 이미 컬럼이 있으면 오류가 나는 게 정상이다(무시).
  }
  try {
    await db.prepare("ALTER TABLE market_fetch_queue ADD COLUMN trv REAL").run(); // 거래대금(큰 종목부터 백필하려고 저장)
  } catch (e) { /* 이미 있으면 정상 */ }
  try {
    // 시가총액: 컬럼 추가만으로는 쓰기 건수가 늘지 않는다(행 수가 그대로이므로).
    await db.prepare("ALTER TABLE market_raw_daily ADD COLUMN market_cap REAL").run();
  } catch (e) { /* 이미 있으면 정상 */ }
  await db.prepare("CREATE INDEX IF NOT EXISTS idx_mfq_new ON market_fetch_queue (is_new)").run();
  // 작업 상태 저장용 아주 작은 표(일괄 시세 추가의 진행 쪽수, 영업이익 조회 대기 목록 등)
  await db.prepare("CREATE TABLE IF NOT EXISTS market_job (k TEXT PRIMARY KEY, v TEXT)").run();
  marketQueueReady = true;
}
// Momentum Score 가중치. §21: "역사적 연구가 이 가중치를 직접 입증한 것은 아니므로 하드코딩하지 말 것" → 설정값으로 분리
const MOMENTUM_WEIGHTS = { rs20: 0.20, rs60: 0.35, rs120: 0.45 };

// 분기 누적치 차감이 필요한 흐름(flow) 항목. 그 외는 시점(stock) 항목이라 그대로 둠.
const FLOW_KEYS = ["revenue", "cogs", "operating_income", "net_income", "ocf", "capex", "fcf", "parent_net_income", "pretax_income", "interest_expense", "depreciation", "amortization", "dividends_paid", "buyback", "intangible_capex"];

// 현금흐름표 항목: 반기·3분기 보고서의 값은 "연초부터의 누적(YTD)"이다(손익 항목은 분기 단독값). 분기별/TTM 계산 때 차분이 필요.
const CF_CUMULATIVE_KEYS = ["ocf", "capex", "fcf", "depreciation", "amortization", "dividends_paid", "buyback", "intangible_capex"];

const ACCOUNT_ITEMS = [
  // 금융업(은행/보험/증권/지주 등)은 "매출액/매출원가" 대신 "영업수익/영업비용"으로 공시하는 경우가 많아 이름 폴백에 추가
  { key: "revenue", ids: ["ifrs-full_Revenue", "ifrs_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"], names: ["매출액", "수익(매출액)", "영업수익"] },
  // 계정ID(CostOfSales)는 "영업비용"만 공시하는 회사에서 영업비용 전체에 붙는 경우가 있어(더블유게임즈) 이름이 "매출원가"일 때만 인정한다
  { key: "cogs", ids: [], names: ["매출원가"] },
  { key: "operating_income", ids: ["dart_OperatingIncomeLoss"], names: ["영업이익"] },
  { key: "net_income", ids: ["ifrs-full_ProfitLoss", "ifrs_ProfitLoss"], names: ["당기순이익", "반기순이익", "분기순이익", "순이익"] },
  { key: "total_equity", ids: ["ifrs-full_Equity", "ifrs_Equity"], names: ["자본총계"] },
  { key: "total_liabilities", ids: ["ifrs-full_Liabilities", "ifrs_Liabilities"], names: ["부채총계"] },
  { key: "cash", ids: ["ifrs-full_CashAndCashEquivalents", "ifrs_CashAndCashEquivalents"], names: ["현금및현금성자산"] },
  { key: "st_financial_assets", ids: [], names: ["단기금융상품", "단기금융자산"] },
  { key: "ocf", ids: ["ifrs-full_CashFlowsFromUsedInOperatingActivities", "ifrs_CashFlowsFromUsedInOperatingActivities"], names: ["영업활동현금흐름", "영업활동 현금흐름", "영업활동으로 인한 현금흐름", "영업활동으로인한현금흐름"] },
  { key: "capex_ppe", ids: ["ifrs-full_PurchaseOfPropertyPlantAndEquipment", "ifrs_PurchaseOfPropertyPlantAndEquipment", "ifrs-full_PaymentsToAcquirePropertyPlantAndEquipment"], names: ["유형자산의 취득", "유형자산 취득", "유형자산의취득", "유형자산취득"] },
  { key: "capex_intangible", ids: ["ifrs-full_PurchaseOfIntangibleAssetsOtherThanGoodwill", "ifrs_PurchaseOfIntangibleAssetsOtherThanGoodwill", "ifrs-full_PurchaseOfIntangibleAssets", "ifrs-full_PaymentsToAcquireIntangibleAssets"], names: ["무형자산의 취득", "무형자산 취득", "무형자산의취득", "무형자산취득"] },
  { key: "receivables", ids: ["ifrs-full_TradeAndOtherCurrentReceivables", "ifrs_TradeAndOtherCurrentReceivables"], names: ["매출채권"] },
  { key: "short_term_trading_securities", ids: [], names: ["단기매매증권"] },
  { key: "fvpl_financial_assets", sumContains: true, ids: ["ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss"], names: ["당기손익-공정가치측정금융자산", "당기손익공정가치측정금융자산"] },
  { key: "fvoci_financial_assets", sumContains: true, ids: ["ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome"], names: ["기타포괄손익-공정가치측정금융자산", "기타포괄손익공정가치측정금융자산"] },
  { key: "investment_property", ids: ["ifrs-full_InvestmentProperty"], names: ["투자부동산"] },
  { key: "inventory", ids: ["ifrs-full_Inventories", "ifrs_Inventories"], names: ["재고자산"] },
  { key: "payables", ids: ["ifrs-full_TradeAndOtherCurrentPayables", "ifrs_TradeAndOtherCurrentPayables"], names: ["매입채무"] },
  // ROIC의 투하자본(IC) 계산용으로 추가된 항목들
  // 신규 IC(영업 관점: 순운전자본+고정자산) 계산용 항목
  // 기타채권·기타채무는 "매출채권 및 기타채권"처럼 합쳐서 공시하는 회사가 많아, 이름이 "포함"만 돼도 잡히면 같은 줄이 매출채권과 이중으로 합산된다 → 정확히 일치할 때만 사용
  { key: "other_receivables", exact: true, ids: ["ifrs-full_OtherReceivables"], names: ["기타채권"] },
  { key: "other_payables", exact: true, ids: ["ifrs-full_OtherPayables"], names: ["기타채무"] },
  { key: "short_term_borrowings", ids: ["ifrs-full_ShorttermBorrowings", "ifrs-full_ShortTermBorrowings"], names: ["단기차입금"] },
  { key: "current_portion_lt_debt", sumNames: true, ids: ["ifrs-full_CurrentPortionOfLongtermBorrowings"], names: ["유동성장기부채", "유동성 장기차입금", "유동성장기차입금", "유동성사채"] },
  { key: "current_lease_liabilities", ids: ["ifrs-full_CurrentLeaseLiabilities"], names: ["유동리스부채"] },
  { key: "lease_liabilities_nc", exact: true, ids: [], names: ["비유동리스부채", "장기리스부채"] },
  { key: "tangible_assets", ids: ["ifrs-full_PropertyPlantAndEquipment"], names: ["유형자산"] },
  { key: "intangible_assets", ids: ["ifrs-full_IntangibleAssetsOtherThanGoodwill"], names: ["무형자산"] },
  { key: "right_of_use_assets", ids: ["ifrs-full_RightofuseAssets"], names: ["사용권자산"] },
  { key: "goodwill", exact: true, ids: ["ifrs-full_Goodwill"], names: ["영업권"] }, // 투하자본(IC)에 포함(인수 대가도 투자한 자본)
  // 5단계 ROE 분석 준비용 항목 (계산은 나중에, 지금은 원천만 저장)
  { key: "parent_net_income", ids: ["ifrs-full_ProfitLossAttributableToOwnersOfParent"], names: ["지배기업의 소유주에게 귀속되는 당기순이익", "지배기업소유주지분순이익", "지배주주순이익"] },
  { key: "pretax_income", ids: ["ifrs-full_ProfitLossBeforeTax"], names: ["법인세비용차감전순이익", "법인세비용차감전순손익", "세전이익"] },
  { key: "interest_expense", exact: true, ids: ["ifrs-full_InterestExpense"], names: ["이자비용"] },
  { key: "parent_equity", ids: ["ifrs-full_EquityAttributableToOwnersOfParent"], names: ["지배기업의 소유주에게 귀속되는 자본", "지배기업소유주지분", "지배주주지분"] },
  // ---- 재무분석 확장용(exact: 계정명을 정확히 일치할 때만 사용 — "유동자산"이 "비유동자산"에 잘못 걸리는 것 방지) ----
  { key: "current_assets", exact: true, ids: ["ifrs-full_CurrentAssets"], names: ["유동자산"] },
  { key: "current_liabilities", exact: true, ids: ["ifrs-full_CurrentLiabilities"], names: ["유동부채"] },
  { key: "long_term_borrowings", exact: true, ids: ["ifrs-full_LongtermBorrowings"], names: ["장기차입금"] },
  { key: "bonds", exact: true, ids: ["ifrs-full_BondsIssued"], names: ["사채"] },
  { key: "depreciation", exact: true, ids: ["ifrs-full_AdjustmentsForDepreciationExpense", "ifrs-full_DepreciationPropertyPlantAndEquipment"], names: ["감가상각비"] },
  { key: "amortization", exact: true, ids: ["ifrs-full_AdjustmentsForAmortisationExpense", "ifrs-full_AmortisationIntangibleAssetsOtherThanGoodwill"], names: ["무형자산상각비"] },
  { key: "dividends_paid", exact: true, ids: ["ifrs-full_DividendsPaidClassifiedAsFinancingActivities"], names: ["배당금의 지급", "배당금지급", "배당금 지급"] },
  { key: "buyback", exact: true, ids: ["dart_AcquisitionOfTreasuryShares"], names: ["자기주식의 취득", "자기주식취득", "자기주식 취득"] },
];

const DB_COLUMNS = [
  "corp_code", "bsns_year", "reprt_code", "period_label", "period_order", "fs_div",
  "revenue", "cogs", "operating_income", "net_income",
  "total_equity", "total_liabilities", "cash", "st_financial_assets",
  "ocf", "capex", "fcf", "receivables", "inventory", "payables",
  "total_shares", "treasury_shares", "dividend_per_share",
  "short_term_trading_securities", "fvpl_financial_assets", "fvoci_financial_assets", "investment_property",
  "other_receivables", "other_payables",
  "short_term_borrowings", "current_portion_lt_debt", "current_lease_liabilities", "lease_liabilities_nc",
  "tangible_assets", "intangible_assets", "right_of_use_assets",
  "parent_net_income", "pretax_income", "interest_expense", "parent_equity",
  "current_assets", "current_liabilities", "long_term_borrowings", "bonds",
  "depreciation", "amortization", "dividends_paid", "buyback", "intangible_capex",
  "goodwill", "preferred_shares",
  "filing_date", "price_at_filing", "price_at_period_end", "per_at_filing", "pbr_at_filing", "fcf_yield_at_filing",
  "roa_at_filing", "peg_at_filing", "eps_at_filing", "eps_growth_at_filing", "shares_source",
  "updated_at",
];

const HTML_PAGE = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DART 재무데이터</title>
  <style>
    :root {
      --primary: #2563eb;
      --primary-dark: #1d4ed8;
      --border: #e2e8f0;
      --bg: #f8fafc;
      --bg-card: #ffffff;
      --text: #0f172a;
      --text-muted: #64748b;
      --danger: #dc2626;
      --good: #16a34a;
      --radius: 10px;
    }
    * { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      max-width: 1040px; margin: 0 auto; padding: 12px 12px 60px;
      background: var(--bg); color: var(--text);
    }
    h3 { margin: 4px 0 16px; }
    .topbar { margin: 2px 0 10px; }
    .topbar .brand { font-size: 19px; font-weight: 800; letter-spacing: -.01em; }
    .topbar .tagline { font-size: 12px; color: var(--text-muted); margin-top: 2px; }
    .nav { display: flex; gap: 6px; position: sticky; top: 0; z-index: 20; background: var(--bg); padding: 6px 0 8px; margin-bottom: 8px; border-bottom: 1px solid var(--border); }
    .nav button { flex: 1; padding: 10px 6px; font-size: 14px; font-weight: 600; }
    .subnav { display: flex; gap: 6px; flex-wrap: wrap; margin-bottom: 10px; }
    .subnav button { padding: 7px 12px; font-size: 13.5px; border-radius: 999px; }
    .meta-line { font-size: 12.5px; color: var(--text-muted); line-height: 1.55; margin-top: 8px; }
    details.adv { margin-top: 8px; }
    details.adv > summary { font-size: 13px; color: var(--primary-dark); }
    /* 모든 결과 표 공용: 세로 스크롤 영역을 주어 머리글 행(th)이 위에 고정되게 한다(position: sticky는 스크롤되는 부모가 있어야 동작). */
    .tscroll { overflow: auto; max-height: 70vh; border: 1px solid var(--border); border-radius: var(--radius); background: #fff; margin-top: 8px; }
    .tscroll table { margin: 0; }
    @media (max-width: 560px) { .nav button { font-size: 12.5px; padding: 9px 2px; } }
    .card {
      background: var(--bg-card); border: 1px solid var(--border); border-radius: var(--radius);
      padding: 14px; margin-bottom: 14px;
    }
    .card-title { font-weight: 600; font-size: 13px; color: var(--text-muted); margin-bottom: 8px; text-transform: uppercase; letter-spacing: .03em; }
    .row { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
    input, select {
      font-size: 15px; padding: 9px 10px; border: 1px solid var(--border); border-radius: 8px;
      background: #fff; color: var(--text);
    }
    button {
      font-size: 15px; padding: 9px 14px; border-radius: 8px; border: 1px solid var(--border);
      background: #fff; color: var(--text); cursor: pointer;
    }
    button.primary { background: var(--primary); color: #fff; border-color: var(--primary); }
    button.primary:hover { background: var(--primary-dark); }
    button.toggle-active { background: var(--primary); color: #fff; border-color: var(--primary); }
    .yr-input { width: 80px; }
    .an-tbl { width: auto; min-width: 100%; font-size: 12px; }
    .an-tbl th, .an-tbl td { padding: 5px 8px; text-align: right; border-bottom: 1px solid var(--border); }
    .an-tbl th { background: #f1f5f9; font-weight: 600; }
    .an-tbl .an-first { text-align: left; position: sticky; left: 0; background: #fff; min-width: 150px; max-width: 220px; white-space: normal; z-index: 1; }
    .an-tbl th.an-first { background: #f1f5f9; z-index: 5; }
    details.card > summary { list-style: none; } details.card > summary::-webkit-details-marker { display: none; }
    details.card > summary::before { content: '▸ '; } details.card[open] > summary::before { content: '▾ '; }
    .sc-good { color:#047857; font-weight:600; }
    .sc-key { background: var(--bg); font-weight:700; }
    /* 스크리닝 표: 순위~영업이익 열은 왼쪽에 고정하고 나머지(20일 수익률~)만 좌우로 스크롤된다. */
    .scr th, .scr td { padding: 6px 7px; }
    .scr .f { position: sticky; background: #fff; z-index: 1; overflow: hidden; text-overflow: ellipsis; }
    .scr th.f { background: #f1f5f9; z-index: 6; }
    .scr .f1 { left: 0; min-width: 42px; max-width: 42px; }
    .scr .f2 { left: 42px; min-width: 104px; max-width: 104px; text-align: left; }
    .scr .f3 { left: 146px; min-width: 80px; max-width: 80px; }
    .scr .f4 { left: 226px; min-width: 80px; max-width: 80px; }
    .scr .f5 { left: 306px; min-width: 96px; max-width: 96px; }
    .scr .f6 { left: 402px; min-width: 104px; max-width: 104px; border-right: 2px solid #94a3b8; box-shadow: 3px 0 4px -2px rgba(15,23,42,.18); }
    /* 좁은 화면(폰)에서는 고정 영역이 화면을 다 차지하지 않도록 순위·종목만 고정한다. */
    @media (max-width: 720px) {
      .scr .f3, .scr .f4, .scr .f5, .scr .f6 { position: static; box-shadow: none; }
      .scr .f2 { border-right: 2px solid #94a3b8; }
    }
    .sc-bad { color:#b91c1c; }
    .sc-warn { color:#b45309; }
    .chart-chips { display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin-bottom:6px; }
    .chart-chip { display:inline-flex; align-items:center; gap:6px; border:1.5px solid var(--border); border-radius:999px; padding:3px 10px; font-size:13px; background:#fff; }
    .chart-chip i { width:8px; height:8px; border-radius:50%; display:inline-block; }
    .chart-chip b { cursor:pointer; color:var(--text-muted); font-size:15px; line-height:1; }
    .chart-chip b:hover { color:#dc2626; }
    .db-chip { display:flex; flex-direction:column; align-items:flex-start; gap:1px; padding:5px 10px; font-size:13px; background:#fff; }
    .db-chip small { font-size:10px; color:var(--text-muted); }
    .db-chip:hover { border-color:var(--primary); }
    #wrap { overflow: auto; margin-top: 4px; max-height: 65vh; border-radius: var(--radius); border: 1px solid var(--border); }
    table { border-collapse: separate; border-spacing: 0; white-space: nowrap; font-size: 13px; width: 100%; }
    td, th { border-bottom: 1px solid var(--border); border-right: 1px solid var(--border); padding: 7px 10px; text-align: right; }
    th { background: #f1f5f9; position: sticky; top: 0; z-index: 2; font-weight: 600; }
    tbody tr:nth-child(even) { background: #f8fafc; }
    th:first-child, td:first-child { position: sticky; left: 0; background: #fff; text-align: left; z-index: 1; }
    tbody tr:nth-child(even) td:first-child { background: #f8fafc; }
    th:first-child { z-index: 3; background: #f1f5f9; }
    /* 열이 많은 표의 "그룹명 + 개별 열" 2단 헤더(buildGroupedThead)용: 1행은 top:0, 2행은 1행
       높이(약 31px)만큼 내려서 고정 — 가로/세로 스크롤 모두에서 두 헤더 줄이 겹치지 않고 함께 고정된다. */
    thead tr:first-child th { top: 0; z-index: 4; }
    thead tr:first-child + tr th { top: 31px; z-index: 3; }
    thead tr:first-child th:first-child { z-index: 6; }
    thead tr:first-child + tr th:first-child { z-index: 6; background: #fff; }
    #status { color: var(--text-muted); margin: 8px 0; white-space: pre-line; font-size: 14px; }
    #status.error { color: var(--danger); }
    #summary div { margin: 5px 0; line-height: 1.5; }
    #summary b { color: var(--text); }
    #summary .stat-tile { background: #f1f5f9; border-radius: 8px; padding: 8px 10px; margin: 0; }
    .stat-tile .stat-label { font-size: 11px; color: var(--text-muted); margin-bottom: 3px; line-height: 1.3; }
    .stat-tile .stat-value { font-size: 15px; font-weight: 700; color: var(--text); }
    details { margin-top: 10px; }
    summary { cursor: pointer; font-size: 13px; color: var(--text-muted); padding: 6px 0; }
    #raw { white-space: pre-wrap; background: #f1f5f9; padding: 10px; font-size: 11px; border-radius: 8px; max-height: 300px; overflow: auto; }
    #globalError {
      display: none; position: sticky; top: 8px; z-index: 10; background: #fef2f2; border: 1px solid #fecaca;
      color: var(--danger); border-radius: 8px; padding: 10px 12px; margin-bottom: 12px; font-size: 13px;
      white-space: pre-line;
    }
    #globalError button { margin-left: 8px; font-size: 12px; padding: 3px 8px; }
    .watchlist-chip {
      display: inline-flex; align-items: center; gap: 4px; background: #eff6ff; border: 1px solid #bfdbfe;
      color: var(--primary-dark); border-radius: 999px; padding: 3px 10px; font-size: 12px; cursor: pointer;
    }
    .watchlist-chip:hover { background: #dbeafe; }
    .watchlist-chip .x { color: var(--text-muted); font-weight: 700; }
  </style>
</head>
<body>
  <div id="globalError"></div>
  <div class="topbar">
    <div class="brand">📊 한국주식 스크리닝 · 재무분석</div>
    <div class="tagline">DART 공시 · 공공데이터포털 시세 · 한국은행(ECOS)/FRED 거시지표</div>
  </div>

  <div class="nav" id="mainNav">
    <button id="tabBtnMomentum" onclick="switchTab('momentum')">① 스크리닝</button>
    <button id="tabBtnFinancial" class="toggle-active" onclick="switchTab('financial')">② 종목분석</button>
    <button id="tabBtnMacro" onclick="switchTab('macro')">③ 거시·상관</button>
    <button id="tabBtnData" onclick="switchTab('data')">④ 데이터 관리</button>
    <button id="tabBtnBt" onclick="switchTab('bt')">⑤ 백테스트</button>
  </div>

  <!-- ===================== ② 종목분석 ===================== -->
  <div id="tabFinancial">
    <div class="card">
      <div class="row">
        <input id="corpName" list="dbCompanyList" onfocus="loadDbCompanies().catch(function(){})" onkeydown="if(event.key==='Enter'){analyzeStock();}" placeholder="종목명을 입력하고 Enter (예: 삼성전자)" style="flex:1; min-width:170px;" autocomplete="off" />
        <datalist id="dbCompanyList"></datalist>
        <button class="primary" onclick="analyzeStock()">분석하기</button>
        <button id="btnWatch" onclick="toggleWatchlist()" title="관심종목에 추가/제거">☆ 관심</button>
      </div>
      <div id="watchlistWrap" class="row" style="margin-top:8px; gap:6px;"></div>
      <div id="stockMeta" class="meta-line"></div>
      <details class="adv">
        <summary>고급 설정 — 조회 기간 · DART 데이터 직접 받기</summary>
        <div class="row" style="margin-top:8px;">
          <label style="font-size:13px; color:var(--text-muted);">DART 조회 기간</label>
          <input id="startYear" type="number" class="yr-input" />
          <span>~</span>
          <input id="endYear" type="number" class="yr-input" />
        </div>
        <div class="row" style="margin-top:8px;">
          <button onclick="fullRefetch()" title="이 화면을 켜 둔 채 전체 기간을 DART에서 다시 받아 저장합니다(약 1~2분)">전체 기간 다시 받기(화면 켜둠)</button>
          <button onclick="queueFetch()" title="화면을 꺼도 서버가 1분에 1개 기간씩 받아 저장합니다">백그라운드로 받기(화면 꺼도 진행)</button>
          <button onclick="checkQueueStatus()">진행 상황 확인</button>
        </div>
        <div id="queueStatus" style="font-size:13px; color:var(--text-muted); margin-top:6px;"></div>
      </details>
    </div>

    <div id="status"></div>

    <div id="stockBody" style="display:none;">
      <div class="subnav">
        <button id="subBtn_summary" class="toggle-active" onclick="setSubTab('summary')">요약 · 분류 · 밸류에이션</button>
        <button id="subBtn_table" onclick="setSubTab('table')">재무제표 표</button>
        <button id="subBtn_five" onclick="setSubTab('five')">ROE 5단계 분해</button>
        <button id="subBtn_compare" onclick="setSubTab('compare')">2종목 비교</button>
      </div>

      <div id="sub_summary">
        <div class="card">
          <div class="row">
            <label style="font-size:14px;">현재 주가(원) <input id="currentPrice" type="number" style="width:130px" placeholder="자동 입력" /></label>
            <button class="primary" onclick="renderSummary()">이 가격으로 다시 계산</button>
            <button onclick="refreshPrice()">전일 종가 다시 가져오기</button>
          </div>
          <div id="priceNote" class="meta-line"></div>
        </div>
        <div id="summary" style="display:none;"></div>
      </div>

      <div id="sub_table" style="display:none;">
        <div class="card">
          <div class="row">
            <button id="btnQuarterly" class="toggle-active" onclick="setView('quarterly')">분기별 보기</button>
            <button id="btnAnnual" onclick="setView('annual')">연간 보기</button>
            <button onclick="downloadFinancialCsv()" title="지금 보이는 표를 엑셀에서 열 수 있는 CSV로 저장합니다">⬇ CSV (엑셀용)</button>
            <button onclick="downloadFinancialMarkdown()" title="LLM(ChatGPT·Claude 등)에 붙여넣기 좋은 마크다운으로 저장합니다">⬇ LLM용 마크다운</button>
          </div>
          <div class="meta-line">표의 첫 행(머리글)과 첫 열(기간)은 스크롤해도 고정됩니다. 머리글을 더블클릭하면 그래프가 열립니다.</div>
        </div>
        <div id="wrap"></div>
        <div id="chartWrap" style="display:none; margin-top:14px;" class="card">
          <div id="chartTitle" style="font-weight:600; margin-bottom:6px;"></div>
          <div id="chartChips" class="chart-chips"></div>
          <canvas id="chartCanvas" style="width:100%; height:220px; touch-action:pan-y;"></canvas>
          <p id="chartNote" style="font-size:12px; color:var(--text-muted); margin-top:6px;"></p>
        </div>
      </div>

      <div id="sub_five" style="display:none;">
        <div class="card">
          <div class="row">
            <button id="fsBtnAnnual" class="toggle-active" onclick="setFiveStepView('annual')">연도별</button>
            <button id="fsBtnQuarterly" onclick="setFiveStepView('quarterly')">분기별</button>
          </div>
        </div>
        <div id="fiveStepWrap" style="display:none;"></div>
      </div>

      <div id="sub_compare" style="display:none;">
        <div class="card">
          <div class="meta-line" style="margin:0 0 8px;">DB에 저장된 두 종목을 같은 기준으로 나란히 봅니다(현재가는 자동으로 불러옵니다).</div>
          <div class="row">
            <input id="cmpNameA" list="dbCompanyList" placeholder="종목 A (예: 삼성전자)" style="flex:1; min-width:120px;" />
            <input id="cmpPriceA" type="number" placeholder="A 현재가(비우면 자동)" style="width:150px" />
          </div>
          <div class="row" style="margin-top:6px;">
            <input id="cmpNameB" list="dbCompanyList" placeholder="종목 B (예: 현대자동차)" style="flex:1; min-width:120px;" />
            <input id="cmpPriceB" type="number" placeholder="B 현재가(비우면 자동)" style="width:150px" />
          </div>
          <div class="row" style="margin-top:8px;">
            <button class="primary" onclick="compareStocks()">비교하기</button>
          </div>
          <div id="cmpStatus" class="meta-line"></div>
        </div>
        <div id="cmpWrap" class="tscroll" style="display:none;"></div>
      </div>
    </div>
  </div><!-- /tabFinancial -->

  <!-- ===================== ① 스크리닝 ===================== -->
  <div id="tabMomentum" style="display:none;">
    <div class="card">
      <div class="card-title">조건 설정</div>
      <div class="row" style="margin-bottom:8px;">
        <span style="font-size:13px; color:var(--text-muted);">프리셋</span>
        <button onclick="setScreenPreset('std')" id="presetStd" class="toggle-active" style="padding:6px 10px; font-size:13px;">표준(권장)</button>
        <button onclick="setScreenPreset('large')" id="presetLarge" style="padding:6px 10px; font-size:13px;">대형·고유동</button>
        <button onclick="setScreenPreset('small')" id="presetSmall" style="padding:6px 10px; font-size:13px;">중소형 포함(위험↑)</button>
      </div>
      <div class="row" style="font-size:13px; color:var(--text-muted);">
        <label>시가총액 하위 <input id="inPcM" type="number" value="20" step="10" min="0" max="90" style="width:60px" /> % 제외</label>
        <label>20일 평균 거래대금 하위 <input id="inPcT" type="number" value="20" step="10" min="0" max="90" style="width:60px" /> % 제외</label>
        <label>주가 ≥ <input id="inPrice" type="number" value="1000" step="500" min="0" style="width:80px" /> 원</label>
      </div>
      <details class="adv">
        <summary>고급 — 복합 점수 가중치 · 결과 저장</summary>
        <div class="row" style="font-size:13px; color:var(--text-muted); margin-top:6px;">
          <span>복합 점수 가중치:</span>
          <label>2~3개월 수익률 <input id="inW3" type="number" step="0.1" min="0" value="0.5" style="width:70px" /></label>
          <label>2~6개월 수익률 <input id="inW6" type="number" step="0.1" min="0" value="0.5" style="width:70px" /></label>
        </div>
        <div class="row" style="font-size:13px; color:var(--text-muted); margin-top:6px;">
          <label><input id="inSave" type="checkbox" /> 이 결과를 주간 결과로 저장(쓰기 약 9건, 기본은 저장 안 함)</label>
        </div>
      </details>
      <div class="row" style="margin-top:10px;">
        <button class="primary" onclick="loadScreen('run')">스크리닝 실행</button>
        <button onclick="loadScreen('top')">저장된 주간 결과</button>
        <select id="screenDate" onchange="loadScreen('top')" style="min-width:110px;"><option value="">최신</option></select>
        <button onclick="downloadScreenCsv()">⬇ CSV</button>
      </div>
      <div id="screenStatus" class="meta-line"></div>
      <div id="screenOpStatus" class="meta-line" style="margin-top:2px;"></div>
    </div>

    <div class="card">
      <div id="screenRegime" style="font-size:13px; line-height:1.5; padding:8px 10px; border-radius:8px; background:var(--bg); border:1px solid var(--border); margin-bottom:8px;">저장된 결과를 불러오는 중...</div>
      <div id="screenTabs" style="display:none; gap:6px; flex-wrap:wrap;"></div>
      <div id="screenDesc" class="meta-line"></div>
      <div id="screenWrap" class="tscroll" style="display:none; margin-top:10px;"></div>
      <div class="row" style="margin-top:10px;">
        <button onclick="prepareFinancials()" title="지금 보이는 목록 상위 종목 중 재무데이터가 없는 종목을 서버가 백그라운드로 받아둡니다">📥 이 목록 상위 종목 재무데이터 미리 받아두기</button>
        <select id="prepN"><option value="10">상위 10개</option><option value="20" selected>상위 20개</option><option value="50">상위 50개</option></select>
        <span id="prepStatus" class="meta-line" style="margin:0;"></span>
      </div>
    </div>

    <details class="card">
      <summary style="font-weight:600; color:var(--text);">계산 방식 · 학술 근거 · 한계 (눌러서 보기)</summary>
      <div style="font-size:13px; color:var(--text-muted); line-height:1.7; margin-top:8px;">
        <b>대상 종목(유니버스)</b>: 코스피·코스닥 <b>보통주</b>. 우선주(종목코드 끝자리가 0이 아닌 종목), 스팩, 리츠, 동전주(최소 주가 미만)는 제외합니다.
        시가총액은 기준일 값, 거래대금은 <b>최근 20거래일 평균</b>(거래대금 합 ÷ 20, 저장되지 않은 날은 0으로 보아 보수적으로 계산)입니다. 하루치 거래대금은 일시적인 급증·급감에 흔들려 쓰지 않습니다.<br />
        <b>수익률</b>은 모두 거래일 기준입니다: 기준일 종가 ÷ N거래일 전 종가 − 1. 기준일은 종목 시세가 충분히 들어온 가장 최근 거래일입니다.<br />
        <b>복합 점수(권장 목록)</b>: 「2~3개월 수익률」과 「2~6개월 수익률」을 각각 전체 후보 안에서의 <b>퍼센타일 순위</b>(0~1)로 바꾼 뒤 가중평균(기본 0.5:0.5, ×100 해서 점수로 표시)합니다.
        여기서 "2~N개월"은 <b>최근 1개월(21거래일)을 뺀</b> 구간이고, 원수익률이 아니라 순위를 쓰므로 급등주 몇 개가 점수를 왜곡하지 않습니다.<br />
        <b>6-1 모멘텀</b>: 21거래일 전 종가 ÷ 126거래일 전 종가 − 1 (최근 1개월을 건너뛴 6개월 모멘텀).<br />
        <b>20·60·120일 수익률</b>: 단순 기간 수익률(최근 1개월 포함, 참고용). <b>RS</b>: (1+주식 수익률) ÷ (1+코스피 수익률). 1보다 크면 코스피를 이긴 것입니다.<br />
        <b>⚠ 단기 과열 표시</b>: 최근 1개월 수익률이 후보 종목 중 <b>상위 5%</b>이면 붙입니다(필터 아님). 고정 %가 아니라 순위라서 장세(강세·약세)에 따라 기준이 자동으로 달라집니다. 5%는 관행적 값이며 학술적으로 검증된 임계값이 아닙니다.<br />
        <b>왜 최근 1개월을 건너뛰나요?</b> 3~12개월 수익률이 높은 종목이 이후에도 초과수익을 낸다는 중기 모멘텀(Jegadeesh &amp; Titman, 1993)과 달리, 직전 1개월 수익률은 오히려 반대로 되돌려지는 경향(단기 반전, Jegadeesh 1990; Lehmann 1990)이 있어 순위에서 뺍니다.<br />
        <b>한계(꼭 읽어주세요)</b><br />
        ① 한국을 포함한 일부 아시아 시장은 중기 모멘텀 효과가 약하거나 불안정하다는 연구가 있습니다(예: Chui·Titman·Wei, 2010). 개인 투자자 비중이 커 단기 과열 후 반전도 흔합니다. 이 목록은 "후보를 좁히는 도구"이지 매수 신호가 아닙니다.<br />
        ② 저장된 가격 이력은 약 6~7개월(126거래일 이상)이라 학계 표준인 12-1 모멘텀과 52주 신고가 근접도(George &amp; Hwang, 2004)는 계산하지 않습니다.<br />
        ③ <b>시가총액·거래대금은 절대 금액이 아니라 "하위 20% 제외"</b>가 기본입니다. 국내 퀀트 투자 실무(예: 소형주 하위 20~30% 제외)에서 널리 쓰이는 방식이며, 시장 규모가 변해도 기준이 따라가는 장점이 있습니다. 순위는 이 앱이 저장한 종목(시총 500억·거래대금 15억 이상 보통주) 안에서 매기므로 전체 상장 종목 기준의 하위 20%보다 실제로는 더 엄격합니다. 어떤 비율이 "정답"이라는 학술적 증명은 없고(Hou·Xue·Zhang 2020: 이상현상 상당수가 초소형·저유동 종목에서 나와 이를 제외하면 재현성이 크게 떨어짐), 최소주가 1,000원은 호가단위 때문에 수익률이 왜곡되는 동전주를 거르는 실무 관행입니다. 프리셋: 표준 20% / 대형·고유동 50% / 중소형 포함 0%<br />
        ④ 공공데이터포털 시세는 기준일 다음 영업일 13시 이후에 올라오므로 기준일이 어제가 아닐 수 있습니다. 거래비용·증권거래세·배당은 반영하지 않으며, 이 순위의 과거 성과(백테스트)는 아직 검증하지 않았습니다.<br />
        ⑤ 영업이익은 공공데이터포털 기업재무정보(연결 우선)의 최근 사업연도 값이며, 없으면 DART로 저장한 값으로 대체합니다.
      </div>
    </details>
  </div><!-- /tabMomentum -->

  <!-- ===================== ③ 거시·상관 ===================== -->
  <div id="tabMacro" style="display:none;">
    <div class="card">
      <div class="card-title">거시경제 변화와 실적·주가수익률의 상관관계</div>
      <div class="row">
        <span id="macroCorrFor" style="font-weight:600;">종목분석 탭에서 종목을 먼저 분석해주세요.</span>
        <button class="primary" onclick="computeMacroCorrelation()">상관관계 계산</button>
      </div>
      <div id="macroCorrStatus" class="meta-line"></div>
    </div>
    <div id="macroCorrWrap" style="display:none;"></div>
  </div><!-- /tabMacro -->

  <!-- ===================== ⑤ 백테스트 ===================== -->
  <div id="tabBt" style="display:none;">
    <div class="card">
      <div class="card-title">🧪 백테스트 — 과거에 이 방법을 썼다면 어땠을까?</div>
      <div style="font-size:12.5px; line-height:1.65;">
        <b>어떻게 검증하나요?</b> 매 기간마다 종목을 점수(예: F-Score)로 줄 세워 5개 그룹으로 나누고, 그 뒤 실제 수익률이 점수 순서대로 높았는지 봅니다.
        상위 그룹이 하위 그룹보다 일관되게 높으면 그 방법에 정보가 있다는 뜻입니다. 이는 Fama-French 이후 학계가 쓰는 표준 방식(분위 포트폴리오 정렬)입니다.
        <ul style="margin:4px 0 0 18px; padding:0;">
          <li><b>롱숏</b> = 상위 그룹 수익 − 하위 그룹 수익, <b>IC</b> = 점수와 이후 수익의 순위상관(0이면 무관, 1이면 완벽).</li>
          <li><b>t 통계량(Newey-West)</b>: 우연이 아닐 가능성. 절댓값 2 미만은 우연과 구별 안 되고, 여러 방법을 시험했다면 3 이상이어야 믿을 만합니다(Harvey-Liu-Zhu 2016). 이 화면은 시도 횟수를 세어 자동 보정합니다.</li>
          <li>결과 아래의 <b>주의할 점</b>(표본 부족·생존편향·선택편향·배당·비용)을 꼭 읽으세요. 데이터가 적으면 판정이 「표본 부족」으로 나옵니다.</li>
        </ul>
      </div>
      <div id="btStatus" class="meta-line" style="margin-top:6px;"></div>
    </div>

    <div class="card">
      <div class="card-title">A. 재무분석 방법 백테스트 (DART 저장 데이터)</div>
      <div class="row"><button onclick="btLoadStocks()">① 저장된 종목 불러오기</button><span id="btFinInfo" style="font-size:12px; color:var(--text-muted);"></span></div>
      <div style="margin:8px 0; font-size:12.5px;">
        <label>방식 <select id="btFinMode" onchange="btFinModeChange()"><option value="annual">연간 재무 방법 (공시일 매수 → 다음 해 공시일 매도)</option><option value="momq">분기 모멘텀 (분기말 주가)</option></select></label>
      </div>
      <div id="btFinFactors" style="margin:6px 0;"></div>
      <div id="btMomBox" style="display:none; margin:6px 0; font-size:12.5px;">
        <label>형성기간 <select id="btMomK"><option value="1">1분기(3개월)</option><option value="2" selected>2분기(6개월)</option><option value="4">4분기(12개월)</option></select></label>
        <label>건너뛰기 <select id="btMomSkip"><option value="0" selected>0분기</option><option value="1">1분기</option></select></label>
      </div>
      <div class="row" style="font-size:12.5px; gap:10px; flex-wrap:wrap;">
        <label>그룹 수 <select id="btFinG"><option value="3">3</option><option value="5" selected>5</option><option value="10">10</option></select></label>
        <label>왕복 거래비용 <input id="btFinCost" type="number" value="0.5" step="0.1" min="0" style="width:60px;" /> %</label>
        <label><input id="btFinTrim" type="checkbox" checked /> 극단 수익률(−90% 이하·+500% 이상) 제외</label>
      </div>
      <div class="row" style="margin-top:8px; gap:8px; flex-wrap:wrap;">
        <button onclick="btRunFin()">② 백테스트 실행</button>
        <button onclick="btApplyFin()">이 방법을 종목분석 Quality 판정에 적용</button>
        <button onclick="btResetTrials()">시도 기록 초기화</button>
        <span id="btApplyMsg" style="font-size:12px; color:var(--text-muted);"></span>
      </div>
      <div id="btFinOut"></div>
    </div>

    <div class="card">
      <div class="card-title">B. 스크리닝 조건 백테스트 (일별 시세)</div>
      <div style="font-size:12px; color:var(--text-muted); line-height:1.55; margin-bottom:6px;">스크리닝과 같은 방식(최근 1개월을 뺀 수익률 순위 + 시총·거래대금 하위 제외)으로 과거 기준일마다 종목을 줄 세우고, 이후 보유기간 수익률을 비교합니다. 저장된 시세 이력이 짧아 기준일이 매우 적을 수 있습니다.</div>
      <div class="row" style="font-size:12.5px; gap:10px; flex-wrap:wrap;">
        <label>2~3개월 가중치 <input id="btScW3" type="number" value="0.5" step="0.1" min="0" style="width:60px;" /></label>
        <label>2~6개월 가중치 <input id="btScW6" type="number" value="0.5" step="0.1" min="0" style="width:60px;" /></label>
        <label>건너뛰기 <input id="btScS" type="number" value="21" min="0" max="42" style="width:55px;" /> 거래일</label>
        <label>3개월=<input id="btScL3" type="number" value="63" min="21" style="width:55px;" />일 6개월=<input id="btScL6" type="number" value="126" min="42" style="width:55px;" />일</label>
        <label>보유 <input id="btScH" type="number" value="21" min="5" max="126" style="width:55px;" /> 거래일</label>
      </div>
      <div class="row" style="font-size:12.5px; gap:10px; flex-wrap:wrap; margin-top:6px;">
        <label>시총 하위 <input id="btScPcm" type="number" value="20" min="0" max="90" step="10" style="width:55px;" />% 제외</label>
        <label>거래대금 하위 <input id="btScPct" type="number" value="20" min="0" max="90" step="10" style="width:55px;" />% 제외</label>
        <label>주가 ≥ <input id="btScPrice" type="number" value="1000" step="500" style="width:70px;" />원</label>
        <label>그룹 수 <select id="btScG"><option value="3">3</option><option value="5" selected>5</option><option value="10">10</option></select></label>
        <label>왕복 비용 <input id="btScCost" type="number" value="0.5" step="0.1" style="width:55px;" />%</label>
        <label>최대 기준일 수 <input id="btScMax" type="number" value="20" min="2" max="30" style="width:55px;" /></label>
      </div>
      <div class="row" style="margin-top:8px;"><button onclick="btRunScreen()">백테스트 실행</button><span style="font-size:11.5px; color:var(--text-muted);">기준일 하나당 DB 약 2.5만 행을 읽습니다(하루 한도 500만 행 → 20개 기준일이면 약 10%).</span></div>
      <div id="btScOut"></div>
    </div>

    <details class="card">
      <summary style="cursor:pointer; font-weight:700;">📖 이 백테스트가 따르는 방법론과 근거 (펼치기)</summary>
      <div class="tscroll"><table class="an-tbl" style="margin-top:6px;"><thead><tr><th class="an-first">방법</th><th>하는 일</th><th>근거</th><th>근거 수준</th></tr></thead><tbody>
        <tr><td class="an-first">분위 포트폴리오 정렬</td><td style="text-align:left;">점수 순위로 그룹을 나눠 그룹별 평균 수익률 비교</td><td style="text-align:left;">Fama-French(1992, 1993) 이래 학계 표준. Hou-Xue-Zhang(2020)의 이상현상 재현 연구도 이 방식 사용</td><td>학술 표준</td></tr>
        <tr><td class="an-first">정보계수(IC)</td><td style="text-align:left;">점수와 이후 수익의 Spearman 순위상관</td><td style="text-align:left;">Spearman(1904), 퀀트 운용의 표준 평가 지표(Grinold-Kahn)</td><td>학술·실무</td></tr>
        <tr><td class="an-first">Newey-West t</td><td style="text-align:left;">수익률이 시간적으로 상관돼 있어도 맞는 표준오차</td><td style="text-align:left;">Newey-West(1987), 지연 선택 Newey-West(1994)</td><td>학술 표준</td></tr>
        <tr><td class="an-first">다중검정 보정</td><td style="text-align:left;">여러 조합을 시험할수록 우연히 좋은 결과가 나오는 것을 보정(시도 횟수 기록)</td><td style="text-align:left;">Harvey-Liu-Zhu(2016: 새 요인은 t≥3), Bailey-López de Prado(2014: Deflated Sharpe, 여기선 근사), White(2000: Reality Check, 미구현)</td><td>학술 (근사 구현)</td></tr>
        <tr><td class="an-first">미래정보 편향 방지</td><td style="text-align:left;">공시일 이후 종가로만 매수. 공시일 이전 가격·재무로 판단하지 않음</td><td style="text-align:left;">백테스트의 기본 원칙(look-ahead bias)</td><td>원칙</td></tr>
        <tr><td class="an-first">표본 분할</td><td style="text-align:left;">전반·후반으로 나눠 부호가 같은지 점검</td><td style="text-align:left;">표본 내/외 검증, Walk-forward(Pardo 2008)의 간이판. 정식 표본 외 검증은 아님</td><td>실무 (간이)</td></tr>
        <tr><td class="an-first">거래비용</td><td style="text-align:left;">상위 그룹 교체율 × 왕복 비용을 차감</td><td style="text-align:left;">Novy-Marx &amp; Velikov(2016): 거래비용 후 많은 이상현상이 약해짐. 한국은 매도 시 증권거래세가 있어 비용 입력값은 직접 조정하세요</td><td>학술 + 가정</td></tr>
        <tr><td class="an-first">알려진 한계</td><td style="text-align:left;">생존편향·선택편향·배당 미반영·소형주 체결 불가·분기 자료의 한계</td><td style="text-align:left;">Hou-Xue-Zhang(2020): 소형주 제외와 엄격한 유의수준을 적용하면 많은 이상현상이 재현되지 않음</td><td>한계 고지</td></tr>
      </tbody></table></div>
      <div style="font-size:11.5px; color:var(--text-muted); margin-top:6px;">논문 인용은 이 앱에서 온라인으로 재확인하지 못했습니다. 중요한 판단 전에 원문을 확인하세요. 백테스트 결과는 미래 수익을 보장하지 않습니다.</div>
    </details>
  </div>

  <!-- ===================== ④ 데이터 관리 ===================== -->
  <div id="tabData" style="display:none;">
    <div class="card">
      <div class="card-title">데이터 현황</div>
      <div id="dataSummary" style="font-size:13px; line-height:1.7;">불러오는 중...</div>
      <div class="row" style="margin-top:8px;"><button onclick="loadDataSummary()">새로고침</button></div>
      <div class="meta-line">평소에는 서버가 자동으로 갱신합니다(시세: 매일 한국시간 18:01 시작, 스크리닝 결과: 매주 토요일 새벽 1시 저장, 재무 큐: 짝수 분마다 처리). 아래 버튼은 처음 한 번이나 문제가 있을 때만 누르면 됩니다.</div>
    </div>

    <div class="card">
      <div class="card-title">1. 시장 시세 데이터 (스크리닝용)</div>
      <div class="row">
        <button class="primary" onclick="refreshMarketUniverse()">오늘자 시세 받기(유니버스 갱신)</button>
        <button onclick="checkMarketStatus()">진행 상황 새로고침</button>
      </div>
      <div id="marketStatus" class="meta-line" style="white-space:pre-line;"></div>
    </div>

    <div class="card">
      <div class="card-title">2. 재무 데이터 (DART)</div>
      <div class="row">
        <button onclick="toggleDbList()">DB 저장 종목 보기</button>
        <span class="meta-line" style="margin:0;">개별 종목은 ② 종목분석에서 "분석하기"를 누르면 없을 때 자동으로 받습니다.</span>
      </div>
      <div id="dbListWrap" style="display:none; margin-top:10px; padding:10px; background:var(--bg); border:1px solid var(--border); border-radius:8px;">
        <div class="row" style="align-items:center;">
          <input id="dbListFilter" oninput="renderDbList()" placeholder="종목명/종목코드로 거르기" style="flex:1; min-width:140px;" />
          <button onclick="toggleDbList(true)" style="padding:6px 10px; font-size:13px;">새로고침</button>
        </div>
        <div id="dbListCount" style="font-size:12px; color:var(--text-muted); margin:6px 0;"></div>
        <div id="dbListBody" style="display:flex; flex-wrap:wrap; gap:6px; max-height:220px; overflow:auto;"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">3. 거시경제 데이터 (환율·금리·WTI)</div>
      <p class="meta-line" style="margin-top:0;">
        원/달러 환율·통안증권(1년)·국고채(3년)·국고채(10년)는 한국은행 ECOS, WTI는 미국 FRED에서 최근 9년치를 받아옵니다. 한 번 받아두면 ③ 거시·상관 탭이 계속 재사용합니다.
      </p>
      <div class="row"><button class="primary" onclick="refreshMacroData()">거시경제 데이터 갱신</button></div>
      <div id="macroStatus" class="meta-line" style="white-space:pre-line;"></div>
    </div>

    <details class="card">
      <summary style="font-weight:600; color:var(--text);">CB·BW 발행내역 (참고용 — 발행 시점 기준, 이후 상환·전환분은 반영 안 됨)</summary>
      <div class="row" style="margin-top:8px;">
        <label>시작일 <input id="cbwStart" type="text" placeholder="20150101" style="width:110px" /></label>
        <label>종료일 <input id="cbwEnd" type="text" placeholder="오늘(YYYYMMDD)" style="width:110px" /></label>
      </div>
      <div class="row" style="margin-top:8px;">
        <button onclick="rawCheckCbBw('cvbdIsDecsn')">전환사채(CB) 발행내역</button>
        <button onclick="rawCheckCbBw('bdwtIsDecsn')">신주인수권부사채(BW) 발행내역</button>
      </div>
      <pre id="cbwRaw" style="margin-top:8px; white-space:pre-wrap; background:#f1f5f9; padding:10px; font-size:11px; border-radius:8px; max-height:300px; overflow:auto;"></pre>
    </details>

    <details class="card">
      <summary style="font-weight:600; color:var(--text);">🔧 원본 데이터 확인 (디버깅용 — 위쪽 종목명 입력칸의 종목 기준)</summary>
      <div class="row" style="margin-top:8px;">
        <label>연도 <input id="rawYear" type="number" class="yr-input" value="2018" /></label>
        <select id="rawReprt">
          <option value="11013">1분기</option>
          <option value="11012">반기</option>
          <option value="11014">3분기</option>
          <option value="11011" selected>사업보고서</option>
        </select>
        <select id="rawFsDiv">
          <option value="CFS" selected>연결(CFS)</option>
          <option value="OFS">개별(OFS)</option>
        </select>
      </div>
      <div class="row" style="margin-top:8px;">
        <button onclick="rawCheck('stockTotqySttus')">주식총수 원본보기</button>
        <button onclick="rawCheck('alotMatter')">배당현황 원본보기</button>
        <button onclick="rawCheckFs()">재무제표 원본보기</button>
      </div>
      <pre id="raw" style="margin-top:8px;"></pre>
    </details>
  </div><!-- /tabData -->


  <script>
    // --- 전역 에러 배너: 개별 함수의 catch에서 놓친 에러나 예상치 못한 예외도 화면에 보이게 한다 ---
    function showGlobalError(msg) {
      const el = document.getElementById('globalError');
      el.style.display = 'block';
      el.innerHTML = (msg || '알 수 없는 오류가 발생했습니다.') + '<button onclick="this.parentElement.style.display=\\'none\\'">닫기</button>';
    }
    window.addEventListener('error', (e) => showGlobalError('스크립트 오류: ' + (e.message || e)));
    window.addEventListener('unhandledrejection', (e) => showGlobalError('처리되지 않은 오류: ' + (e.reason && e.reason.message ? e.reason.message : e.reason)));

    // --- 관심종목(워치리스트): 로그인 없이 이 브라우저에만 저장되는 반복조회용 즐겨찾기 ---
    const WATCHLIST_KEY = 'fs_watchlist';
    function loadWatchlist() {
      try { return JSON.parse(localStorage.getItem(WATCHLIST_KEY) || '[]'); } catch (e) { return []; }
    }
    function saveWatchlist(list) {
      try { localStorage.setItem(WATCHLIST_KEY, JSON.stringify(list)); } catch (e) { /* 저장 공간이 없어도 조용히 무시 */ }
    }
    function renderWatchlistChips() {
      const list = loadWatchlist();
      const wrap = document.getElementById('watchlistWrap');
      if (!wrap) return;
      wrap.innerHTML = list.length === 0
        ? '<span style="font-size:12px; color:var(--text-muted);">관심종목이 없습니다. 종목을 분석한 뒤 "☆ 관심"을 눌러 추가하면 여기에 바로가기가 생깁니다.</span>'
        : list.map((name) => \`<span class="watchlist-chip" onclick="loadWatchedStock('\${name.replace(/'/g, "\\\\'")}')">⭐ \${name} <span class="x" onclick="event.stopPropagation(); removeFromWatchlist('\${name.replace(/'/g, "\\\\'")}')">✕</span></span>\`).join('');
      updateWatchButtonState();
    }
    function updateWatchButtonState() {
      const name = document.getElementById('corpName').value.trim();
      const btn = document.getElementById('btnWatch');
      if (!btn) return;
      const inList = loadWatchlist().includes(name);
      btn.textContent = inList ? '★ 관심 해제' : '☆ 관심';
    }
    function toggleWatchlist() {
      const name = document.getElementById('corpName').value.trim();
      if (!name) { alert('종목명을 먼저 입력해주세요.'); return; }
      let list = loadWatchlist();
      if (list.includes(name)) list = list.filter((n) => n !== name);
      else list.push(name);
      saveWatchlist(list);
      renderWatchlistChips();
    }
    function removeFromWatchlist(name) {
      saveWatchlist(loadWatchlist().filter((n) => n !== name));
      renderWatchlistChips();
    }
    function loadWatchedStock(name) {
      document.getElementById('corpName').value = name;
      updateWatchButtonState();
      analyzeStock();
    }

    // 조회기간 입력 기본값: 최근 10년
    (function () {
      const thisYear = new Date().getFullYear();
      document.getElementById('startYear').value = thisYear - 9;
      document.getElementById('endYear').value = thisYear;
      renderWatchlistChips();
      document.getElementById('corpName').addEventListener('input', updateWatchButtonState);
    })();

    let rawRows = [];      // DB/DART에서 받아온, 가공 안 된 원본 기간별 데이터
    let currentRows = [];  // 현재 화면에 표시 중인 데이터 (분기별 변환 or 연간 그대로)
    let viewMode = 'quarterly';
    // 실제 데이터가 있는 행만 분석에 사용 (공시 전이라 비어있는 행/오류 행 제외)
    const isUsable = (r) => !r.error && r.fs_div != null;

    const FLOW_KEYS = ${JSON.stringify(FLOW_KEYS)};
    const CF_CUM_KEYS = ${JSON.stringify(CF_CUMULATIVE_KEYS)}; // 반기·3분기 값이 누적(YTD)인 현금흐름 항목

    function toQuarterlyRows(rows) {
      const byYear = {};
      for (const r of rows.filter(isUsable)) {
        if (!byYear[r.bsns_year]) byYear[r.bsns_year] = {};
        byYear[r.bsns_year][r.reprt_code] = r;
      }
      const out = [];
      for (const y of Object.keys(byYear).sort()) {
        const q1 = byYear[y]['11013'];
        const q2 = byYear[y]['11012']; // 손익: 반기 thstrm_amount = 2분기 단독값 / 현금흐름: 1~6월 누적
        const q3 = byYear[y]['11014']; // 손익: 3분기 단독값 / 현금흐름: 1~9월 누적
        const annual = byYear[y]['11011']; // 사업보고서 thstrm_amount = 연간 누적
        const diff = (a, b) => (a != null && b != null) ? a - b : null;
        const mix = (a, b) => !!(a && b && a.fs_div && b.fs_div && a.fs_div !== b.fs_div); // 연결/개별이 섞이면 차분하지 않음

        if (q1) out.push({ ...q1, period_label: \`\${y} 1분기\` });
        if (q2) {
          const row = { ...q2, period_label: \`\${y} 2분기\` };
          for (const key of CF_CUM_KEYS) row[key] = (q1 && !mix(q2, q1)) ? diff(q2[key], q1[key]) : null; // 누적 → 2분기 단독
          out.push(row);
        }
        if (q3) {
          const row = { ...q3, period_label: \`\${y} 3분기\` };
          for (const key of CF_CUM_KEYS) row[key] = (q2 && !mix(q3, q2)) ? diff(q3[key], q2[key]) : null; // 누적 → 3분기 단독
          out.push(row);
        }
        if (annual) {
          const row = { ...annual, period_label: \`\${y} 4분기\`, annual_net_income: annual.net_income, annual_parent_net_income: annual.parent_net_income };
          for (const key of FLOW_KEYS) {
            if (CF_CUM_KEYS.includes(key)) {
              row[key] = (q3 && !mix(annual, q3)) ? diff(annual[key], q3[key]) : null; // 연간 − 1~9월 누적 = 4분기 단독
            } else {
              const parts = [q1 && q1[key], q2 && q2[key], q3 && q3[key]];
              row[key] = (!mix(annual, q1) && !mix(annual, q2) && !mix(annual, q3) && annual[key] != null && parts.every((v) => v != null)) ? annual[key] - (parts[0] + parts[1] + parts[2]) : null;
            }
          }
          out.push(row);
        }
      }
      return out;
    }

    function toAnnualRows(rows) {
      return rows
        .filter((r) => isUsable(r) && r.reprt_code === '11011')
        .sort((a, b) => a.bsns_year.localeCompare(b.bsns_year))
        .map((r) => ({ ...r, period_label: \`\${r.bsns_year} 연간\` }));
    }

    function setView(mode) {
      viewMode = mode;
      document.getElementById('btnQuarterly').className = mode === 'quarterly' ? 'toggle-active' : '';
      document.getElementById('btnAnnual').className = mode === 'annual' ? 'toggle-active' : '';
      applyView();
    }

    function applyView() {
      const rows = viewMode === 'quarterly' ? toQuarterlyRows(rawRows) : toAnnualRows(rawRows);
      renderTable(rows);
      ensureMacroForTable();
      if (chartSel.length) renderChartPanel(false); // 모드/데이터가 바뀌면 열려 있는 차트도 다시 그림
    }

    // 표의 거시경제 열을 채우기 위해 macro_data를 (한 번만) 받아온다. 받자마자 표를 다시 그려 값이 채워지게 하고,
    // 실패(테이블 미생성·갱신 안 함 등)하면 조용히 N/A로 두며 같은 실패를 반복 재시도하지 않는다.
    // "거시경제 데이터 갱신"을 누르면 실패 표시를 풀어 다음 조회 때 다시 시도한다.
    let macroLoadPromise = null;
    let macroLoadFailed = false;
    function ensureMacroForTable() {
      if (macroSeriesCache || macroLoadFailed || macroLoadPromise) return;
      macroLoadPromise = loadMacroSeries()
        .then(() => { macroLoadPromise = null; if (rawRows.length > 0) applyView(); })
        .catch(() => { macroLoadPromise = null; macroLoadFailed = true; });
    }

    // 열이 많은 표를 "그룹명 + 개별 열" 2단 헤더로 그리는 공용 헬퍼. groups: [{name, color, cols:[{label,...}]}]
    // thRenderer(col)는 각 그룹 안의 개별 <th> 내용을 만들어주는 함수(표마다 다를 수 있어 주입받음).
    // 반환값의 groupStartKeys는 "이 열에서 새 그룹이 시작된다"는 표시로, 본문 행에도 같은 구분선을
    // 이어그려서(세로 경계선) 헤더뿐 아니라 데이터를 내려보며 읽을 때도 그룹 경계가 계속 보이게 한다.
    function buildGroupedThead(groups, thRenderer) {
      const groupRow = groups.map((g) =>
        \`<th colspan="\${g.cols.length}" style="background:\${g.color}1f; color:\${g.color}; border-bottom:2px solid \${g.color}; text-align:center;">\${g.name}</th>\`
      ).join('');
      const colRow = groups.map((g, gi) =>
        g.cols.map((col, ci) => {
          const isGroupStart = ci === 0 && gi > 0;
          const borderStyle = isGroupStart ? \`border-left:2px solid \${g.color}99;\` : '';
          return thRenderer(col, borderStyle);
        }).join('')
      ).join('');
      const groupStartKeys = new Set();
      groups.forEach((g, gi) => { if (gi > 0 && g.cols[0]) groupStartKeys.add(g.cols[0].key); });
      return { headHtml: \`<thead><tr>\${groupRow}</tr><tr>\${colRow}</tr></thead>\`, groupStartKeys };
    }
    function groupBorderStyle(key, groupStartKeys, groups) {
      if (!groupStartKeys.has(key)) return '';
      const g = groups.find((gr) => gr.cols[0] && gr.cols[0].key === key);
      return g ? \`border-left:2px solid \${g.color}99;\` : '';
    }

    // 열 정의: 관련 있는 항목끼리 그룹으로 묶는다. 그룹마다 색을 둬서 2단 헤더(그룹명 + 개별 열이름)로
    // 그리면, 45개 가까운 열이 쭉 나열될 때보다 "이 덩어리는 손익, 이 덩어리는 운전자본" 식으로 한눈에
    // 구역을 구분해 읽을 수 있다 — 넓은 재무데이터 표에서 흔히 쓰는 패턴(블룸버그·BI 툴 등).
    // type이 서식을 결정하고, key가 '_'로 시작하면 저장된 값이 아니라 그 자리에서 계산하는 파생값(매출총이익률 등).
    // 표 렌더링과 "LLM이 읽기 좋은 파일로 내보내기" 양쪽에서 같은 열 정의를 재사용하도록 모듈 스코프로 뺐다.
    // 거시지표 키→표시 이름. COLUMN_GROUPS가 스크립트 로드 시점에 바로 이 값을 쓰므로(const는 선언 전 참조 불가)
    // 반드시 COLUMN_GROUPS보다 위에 둔다.
    const MACRO_LABELS = { usdkrw: '원/달러 환율', msb1y: '통안증권(1년)', ktb3y: '국고채(3년)', ktb10y: '국고채(10년)', wti: 'WTI 현물가' };
    const MACRO_COL_LABELS = { usdkrw: '원/달러 환율(원)', msb1y: '통안채 1년(%)', ktb3y: '국고채 3년(%)', ktb10y: '국고채 10년(%)', wti: 'WTI 현물가($)' };
    const COLUMN_GROUPS = [
        { name: '기본', color: '#64748b', cols: [
          { label: '기간', key: 'period_label', type: 'text' },
          { label: 'fs_div', key: 'fs_div', type: 'text' },
        ] },
        { name: '손익', color: '#7c3aed', cols: [
          { label: '매출액', key: 'revenue', type: 'won' },
          { label: '매출원가', key: 'cogs', type: 'won' },
          { label: '매출총이익률', key: '_grossMargin', type: 'percent' },
          { label: '영업이익', key: 'operating_income', type: 'won' },
          { label: '영업이익률', key: '_opMargin', type: 'percent' },
          { label: '당기순이익', key: 'net_income', type: 'won' },
          { label: '지배주주순이익', key: 'parent_net_income', type: 'won' },
          { label: '세전이익', key: 'pretax_income', type: 'won' },
          { label: '이자비용', key: 'interest_expense', type: 'won' },
        ] },
        { name: '현금흐름', color: '#0891b2', cols: [
          { label: '영업활동현금흐름', key: 'ocf', type: 'won' },
          { label: 'CapEx', key: 'capex', type: 'won' },
          { label: '잉여현금흐름', key: 'fcf', type: 'won' },
        ] },
        { name: '자본/부채', color: '#0d9488', cols: [
          { label: '총자본', key: 'total_equity', type: 'won' },
          { label: '지배주주자기자본', key: 'parent_equity', type: 'won' },
          { label: '총부채', key: 'total_liabilities', type: 'won' },
        ] },
        { name: '현금성/금융자산', color: '#2563eb', cols: [
          { label: '현금및현금성자산', key: 'cash', type: 'won' },
          { label: '단기금융자산', key: 'st_financial_assets', type: 'won' },
          { label: '단기매매증권', key: 'short_term_trading_securities', type: 'won' },
          { label: '당기손익FV금융자산', key: 'fvpl_financial_assets', type: 'won' },
          { label: '기타포괄손익FV금융자산', key: 'fvoci_financial_assets', type: 'won' },
        ] },
        { name: '운전자본', color: '#ca8a04', cols: [
          { label: '매출채권', key: 'receivables', type: 'won' },
          { label: '기타채권', key: 'other_receivables', type: 'won' },
          { label: '재고자산', key: 'inventory', type: 'won' },
          { label: '매입채무', key: 'payables', type: 'won' },
          { label: '기타채무', key: 'other_payables', type: 'won' },
        ] },
        { name: '차입금/리스부채', color: '#dc2626', cols: [
          { label: '단기차입금', key: 'short_term_borrowings', type: 'won' },
          { label: '유동성장기부채', key: 'current_portion_lt_debt', type: 'won' },
          { label: '유동리스부채', key: 'current_lease_liabilities', type: 'won' },
          { label: '비유동리스부채', key: 'lease_liabilities_nc', type: 'won' },
        ] },
        { name: '확장 항목(재무분석용)', color: '#0f766e', cols: [
          { label: '유동자산', key: 'current_assets', type: 'won' },
          { label: '유동부채', key: 'current_liabilities', type: 'won' },
          { label: '장기차입금', key: 'long_term_borrowings', type: 'won' },
          { label: '사채', key: 'bonds', type: 'won' },
          { label: '감가상각비(CF)', key: 'depreciation', type: 'won' },
          { label: '무형자산상각비(CF)', key: 'amortization', type: 'won' },
          { label: '배당금 지급(CF)', key: 'dividends_paid', type: 'won' },
          { label: '자기주식 취득(CF)', key: 'buyback', type: 'won' },
          { label: '무형자산 취득(CF)', key: 'intangible_capex', type: 'won' },
          { label: '영업권', key: 'goodwill', type: 'won' },
          { label: '우선주 유통수', key: 'preferred_shares', type: 'won' },
        ] },
        { name: '고정자산', color: '#9333ea', cols: [
          { label: '유형자산', key: 'tangible_assets', type: 'won' },
          { label: '무형자산', key: 'intangible_assets', type: 'won' },
          { label: '사용권자산', key: 'right_of_use_assets', type: 'won' },
          { label: '투자부동산', key: 'investment_property', type: 'won' },
        ] },
        { name: '주식/배당', color: '#be185d', cols: [
          { label: '총주식수', key: 'total_shares', type: 'won' },
          { label: '자기주식수', key: 'treasury_shares', type: 'won' },
          { label: '주식수 출처', key: 'shares_source', type: 'text' },
          { label: '주당배당금', key: 'dividend_per_share', type: 'won' },
          { label: '배당성향(연간)', key: '_payoutRatio', type: 'percent' },
        ] },
        { name: '공시시점 밸류에이션·수익성(TTM)', color: '#ea580c', cols: [
          { label: '공시일자', key: 'filing_date', type: 'text' },
          { label: '기간말 주가', key: 'price_at_period_end', type: 'won' },
          { label: '공시시점 주가', key: 'price_at_filing', type: 'won' },
          { label: '기간말→공시일 상승률', key: '_priceReturn', type: 'percent' },
          { label: 'EPS(TTM)', key: 'eps_at_filing', type: 'won' },
          { label: 'EPS 성장률(YoY)', key: 'eps_growth_at_filing', type: 'percent' },
          { label: '공시시점 PER(TTM)', key: 'per_at_filing', type: 'ratio' },
          { label: '공시시점 PBR', key: 'pbr_at_filing', type: 'ratio' },
          { label: '공시시점 ROA(TTM)', key: 'roa_at_filing', type: 'percent' },
          { label: '공시시점 FCF Yield(TTM)', key: 'fcf_yield_at_filing', type: 'percent' },
          { label: '공시시점 PEG(TTM)', key: 'peg_at_filing', type: 'ratio' },
        ] },
        // 거시경제 지표: 회계기간 말일(3·6·9·12월말)과 공시일 각각의 값. 말일/공시일이 휴장일이면 그 이전 가장
        // 가까운 영업일 값(미래 값은 절대 쓰지 않음). 값은 macro_data 테이블에서 오므로 "거시경제 데이터 갱신"을
        // 한 번 해둬야 채워진다(안 했으면 N/A).
        { name: '거시경제 · 기간말(3·6·9·12월말)', color: '#4f46e5', cols:
          Object.keys(MACRO_LABELS).map((k) => ({ label: MACRO_COL_LABELS[k], key: '_macroPE_' + k, type: 'macro' })) },
        { name: '거시경제 · 공시일', color: '#c026d3', cols:
          Object.keys(MACRO_LABELS).map((k) => ({ label: MACRO_COL_LABELS[k], key: '_macroFL_' + k, type: 'macro' })) },
        { name: '비고', color: '#64748b', cols: [
          { label: '비고', key: 'error', type: 'text' },
        ] },
      ];
    const COLUMNS = COLUMN_GROUPS.flatMap((g) => g.cols);

    // 보고서 종류별 회계기간 말일(12월 결산 기준 — 서버의 PERIOD_END_SUFFIX와 같은 규칙). 4분기 행도 사업보고서(11011)라 1231.
    const PERIOD_END_SUFFIX_FE = { '11013': '0331', '11012': '0630', '11014': '0930', '11011': '1231' };
    function periodEndDateOf(r) {
      const suffix = PERIOD_END_SUFFIX_FE[r.reprt_code];
      return (suffix && r.bsns_year) ? r.bsns_year + suffix : null;
    }
    // 표/상관계수에서 쓰는 거시값 조회: 목표일 이전 가장 가까운 값이되, 10일(영업일 공백·연휴 감안) 넘게 오래된 값은
    // "그 시점 값"이 아니므로 N/A 처리한다(예: 거시데이터를 한참 갱신 안 했을 때 옛 값을 최신처럼 보여주는 것 방지).
    const MACRO_MAX_GAP_DAYS = 10;
    function macroAt(key, ymd) {
      if (!macroSeriesCache || !ymd) return null;
      return macroValueAsOf(macroSeriesCache[key], ymd, MACRO_MAX_GAP_DAYS);
    }

    function rawValue(r, col) {
      if (col.key.startsWith('_macroPE_')) return macroAt(col.key.slice('_macroPE_'.length), periodEndDateOf(r));
      if (col.key.startsWith('_macroFL_')) return macroAt(col.key.slice('_macroFL_'.length), r.filing_date);
      if (col.key === '_grossMargin') { const gpv = grossProfitOf(r); return gpv != null ? gpv / r.revenue : null; }
      if (col.key === '_opMargin') return (r.operating_income != null && r.revenue) ? r.operating_income / r.revenue : null;
      if (col.key === '_priceReturn') return priceReturnOf(r);
      if (col.key === '_payoutRatio') return payoutRatioOf(r);
      return r[col.key];
    }

    function formatCell(v, type, key) {
      if (type === 'text') return v ?? (key === 'error' ? '' : 'N/A');
      if (v == null) return 'N/A';
      if (type === 'percent') return (v * 100).toFixed(2) + '%';
      if (type === 'ratio') return v.toFixed(2);
      if (type === 'macro') return Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 });
      return Number(v).toLocaleString();
    }

    function renderTable(rows) {
      currentRows = rows;
      const { headHtml, groupStartKeys } = buildGroupedThead(COLUMN_GROUPS, (col, borderStyle) => {
        const chartable = col.type !== 'text';
        return chartable
          ? \`<th style="\${borderStyle}" ondblclick="showChart('\${col.key}','\${col.label}')" title="더블클릭하면 그래프">\${col.label}</th>\`
          : \`<th style="\${borderStyle}">\${col.label}</th>\`;
      });

      let bodyHtml = '';
      for (const r of rows) {
        bodyHtml += '<tr>' + COLUMNS.map((col) => {
          const style = groupBorderStyle(col.key, groupStartKeys, COLUMN_GROUPS);
          return \`<td style="\${style}">\${formatCell(rawValue(r, col), col.type, col.key)}</td>\`;
        }).join('') + '</tr>';
      }
      const html = \`<table>\${headHtml}<tbody>\${bodyHtml}</tbody></table>\`;
      document.getElementById('wrap').innerHTML = html;

      // showChart가 계산 파생값(_grossMargin 등)도 그릴 수 있도록 rawValue 함수를 전역에 노출
      window.__columnDefs = COLUMNS;
      window.__rawValue = rawValue;
    }

    // 지금 화면에 보이는 DB 조회 결과(currentRows)를 LLM이 가장 이해하기 좋은 형태로 내보낸다.
    // 마크다운을 고른 이유: (1) LLM이 학습 데이터에서 가장 많이 접하는 표 형식이라 열-값 대응을
    // 안정적으로 읽어낸다. (2) DB 원본 컬럼명(예: ifrs-full 계정코드 기반 영문 키) 대신 화면과 똑같은
    // 한글 라벨을 그대로 쓸 수 있어 "이 숫자가 뭘 의미하는지" 별도 설명 없이 바로 전달된다.
    // (3) 그룹(손익/현금흐름/...)별로 표를 나눠서, 폭이 넓은 표 하나보다 LLM이 행-열을 혼동할 여지가 적다.
    function buildMarkdownExport(rows, corpName, viewModeLabel) {
      const esc = (s) => String(s).replace(/\\|/g, '\\\\|').replace(/\\n/g, ' ');
      let md = \`# \${corpName || '종목'} 재무 데이터 (DART 공시 기반)\n\n\`;
      md += \`- 조회 기준: \${viewModeLabel}\n\`;
      md += \`- 내보낸 시각: \${new Date().toISOString()}\n\`;
      md += \`- 표기 단위: 금액=원(KRW), 비율=%, 배수='배'. "N/A"는 해당 기간 미공시 또는 계산 불가(분모 0 등)를 의미합니다.\n\`;
      md += \`- fs_div: CFS=연결재무제표, OFS=개별재무제표. '공시시점' 항목은 모두 TTM(직전 4개 분기 합산)·공시일 주가 기준입니다. '거시경제' 항목은 회계기간 말일/공시일 이전 가장 가까운 영업일 값(10일 넘게 오래된 값은 N/A)이며 금리 단위는 %입니다.\n\n\`;
      for (const g of COLUMN_GROUPS) {
        if (g.name === '기본' || g.name === '비고') continue; // 기간은 각 표의 첫 열로 이미 들어가고, 비고는 아래 오류 목록에서 별도 처리
        const headerCols = ['기간', ...g.cols.map((c) => esc(c.label.replace(/<br\\\/>/g, ' ')))];
        md += \`## \${g.name}\n\n\`;
        md += \`| \${headerCols.join(' | ')} |\n\`;
        md += \`|\${headerCols.map(() => '---').join('|')}|\n\`;
        for (const r of rows) {
          const cells = [esc(r.period_label), ...g.cols.map((c) => esc(formatCell(rawValue(r, c), c.type, c.key)))];
          md += \`| \${cells.join(' | ')} |\n\`;
        }
        md += \`\n\`;
      }
      const failed = rows.filter((r) => r.error);
      if (failed.length) {
        md += \`## 조회 실패/미공시 기간\n\n\`;
        for (const r of failed) md += \`- \${esc(r.period_label)}: \${esc(r.error)}\n\`;
        md += \`\n\`;
      }
      return md;
    }

    async function downloadFinancialMarkdown() {
      if (!currentRows || currentRows.length === 0) { alert('먼저 종목을 조회하세요.'); return; }
      // 거시경제 열까지 채워서 내보내도록, 아직 못 받았으면 잠깐 기다려 받아온다(실패해도 N/A로 계속 진행).
      if (!macroSeriesCache && !macroLoadFailed) {
        try { await loadMacroSeries(); } catch (e) { macroLoadFailed = true; }
      }
      const corpName = document.getElementById('corpName').value.trim();
      const viewModeLabel = viewMode === 'quarterly' ? '분기별' : '연간';
      const md = buildMarkdownExport(currentRows, corpName, viewModeLabel);
      // 한글이 깨져(엉뚱한 한자처럼) 보이는 건 Blob의 실제 바이트 인코딩이 아니라, BOM(파일 맨 앞의
      // "이건 UTF-8입니다" 표시)이 없어서 메모장 등 일부 프로그램이 윈도우 기본 인코딩(CP949)으로
      // 잘못 추측해 읽기 때문이다. UTF-8 BOM을 맨 앞에 붙여 그 추측 오류를 없앤다.
      const BOM = String.fromCharCode(0xFEFF);
      const blob = new Blob([BOM + md], { type: 'text/markdown;charset=utf-8' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = \`\${corpName || 'financial'}_dart_\${viewModeLabel}.md\`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    }

    // 우선주가 있으면 순이익·자본이 보통주+우선주 전체의 것이므로 주당 지표는 둘을 합친 유통주식수로 나눈다(우선주가 없으면 그대로 보통주 유통주식수).
    function shareOut(r) {
      if (r.total_shares == null || r.treasury_shares == null) return null;
      const o = r.total_shares - r.treasury_shares + (r.preferred_shares || 0);
      return o > 0 ? o : null;
    }

    // 영업 관점 투하자본(IC) = 영업운전자본 + 고정자산.
    //   영업운전자본 = (매출채권 + 기타채권 + 재고자산) − (매입채무 + 기타채무)
    //   고정자산     = 유형자산 + 무형자산 + 사용권자산 + 영업권
    // 차입금·리스부채·대여금·현금은 "자금 조달/금융" 항목이라 영업 투하자본에 넣지도 빼지도 않는다(McKinsey Valuation / Damodaran의 영업 방식 IC).
    // (이전 버전은 단기차입금 등을 영업부채에서 "빼서" 결과적으로 IC에 더하고 있었다 — 차입금이 있는 회사의 ROIC가 낮게 나오던 오류.)
    function computeIC(r) {
      if (r.receivables == null && r.payables == null && r.tangible_assets == null) return null;
      const v = (x) => x || 0;
      const operatingAssets = v(r.receivables) + v(r.other_receivables) + v(r.inventory);
      const operatingLiabilities = v(r.payables) + v(r.other_payables);
      const fixedAssets = v(r.tangible_assets) + v(r.intangible_assets) + v(r.right_of_use_assets) + v(r.goodwill);
      return (operatingAssets - operatingLiabilities) + fixedAssets;
    }

    // ROIC = 영업이익 × (1−세율) ÷ 평균 투하자본(기초·기말 평균; 전기 자료가 없으면 기말). 세율은 유효세율(effTaxRate), 범위 밖이면 AN_TH.taxRate.
    // ROE가 평균 자본을 쓰는 것과 같은 기준으로 맞춘다(기간 중 자본이 늘어난 회사의 ROIC가 과소 계산되는 것을 방지).
    // 매출총이익 = 매출액 − 매출원가. 단, "영업비용"만 공시하는 회사(예: 더블유게임즈)는 영업비용이 매출원가 태그로 잡혀
    // 매출총이익이 영업이익과 같아지므로(매출−원가 = 영업이익) 이 경우는 계산하지 않는다(N/A). 매출원가가 매출 이상인 이상값도 제외.
    function grossProfitOf(r) {
      if (!r || r.revenue == null || r.cogs == null || !(r.revenue > 0)) return null;
      if (r.cogs < 0 || r.cogs > r.revenue) return null;
      if (r.operating_income != null && Math.abs((r.revenue - r.cogs) - r.operating_income) <= 0.001 * r.revenue) return null;
      return r.revenue - r.cogs;
    }

    // 유효세율 = 1 − 순이익/세전이익 (세전이익>0, 0~40% 범위일 때만). 벗어나면 법정 근사 24% 사용 — 일회성 손익이 큰 해의 왜곡 방지
    function effTaxRate(r) {
      if (r && r.pretax_income > 0 && r.net_income != null) {
        const t = 1 - r.net_income / r.pretax_income;
        if (isFinite(t) && t >= 0 && t <= 0.4) return t;
      }
      return AN_TH.taxRate;
    }

    // 평균 투하자본: 전기(같은 재무제표 기준)가 있고 투하자본이 양수면 (기초+기말)/2, 아니면 기말. usedPrior=평균에 전기를 썼는지
    function computeIcAvg(r, prior) {
      const ic = computeIC(r);
      if (ic == null || ic <= 0) return null;
      const pr = prior ? comparablePrior(r, prior) : null;
      const icPrev = pr ? computeIC(pr) : null;
      const usedPrior = icPrev != null && icPrev > 0;
      return { icAvg: usedPrior ? (ic + icPrev) / 2 : ic, usedPrior: usedPrior, pr: pr };
    }

    function computeROIC(r, prior) {
      if (r.operating_income == null) return null;
      const ia = computeIcAvg(r, prior);
      if (!ia) return null;
      return (r.operating_income * (1 - effTaxRate(r))) / ia.icAvg;
    }

    // ROIC 4단계 분해 (Novy-Marx식 확장): ROIC = (매출총이익/매출) × (매출/총자산) × (NOPAT/매출총이익) × (총자산/투하자본)
    // 곱하면 NOPAT/투하자본이 되는 항등식. 모든 항에 같은 평균 기준(평균 총자산·평균 투하자본)을 써서 항등식이 정확히 성립하게 한다.
    // 매출총이익을 알 수 없는 회사(영업비용만 공시 등)나 매출총이익 ≤ 0이면 계산하지 않는다(N/A).
    function computeROIC4(r, prior) {
      if (!r || r.operating_income == null || r.revenue == null || !(r.revenue > 0)) return null;
      const gp = grossProfitOf(r);
      if (gp == null || !(gp > 0)) return null;
      const ia = computeIcAvg(r, prior);
      if (!ia || r.total_equity == null || r.total_liabilities == null) return null;
      const ta = r.total_equity + r.total_liabilities;
      let taAvg = ta;
      if (ia.usedPrior) {
        if (!ia.pr || ia.pr.total_equity == null || ia.pr.total_liabilities == null) return null; // IC 평균에 전기를 썼으면 총자산도 같은 기준이어야 함
        taAvg = (ta + ia.pr.total_equity + ia.pr.total_liabilities) / 2;
      }
      if (!(taAvg > 0)) return null;
      const nopat = r.operating_income * (1 - effTaxRate(r));
      const o = { gm: gp / r.revenue, turn: r.revenue / taAvg, opx: nopat / gp, lev: taAvg / ia.icAvg, roic: nopat / ia.icAvg, nopat: nopat, icAvg: ia.icAvg, taAvg: taAvg };
      o.gpa = o.gm * o.turn;
      o.check = o.gm * o.turn * o.opx * o.lev;
      return o;
    }

    function computeROE(r) {
      if (r.net_income == null || !r.total_equity) return null;
      return r.net_income / r.total_equity;
    }

    // 전기와 재무제표 기준(연결 CFS / 개별 OFS)이 다르면 평균에 쓰지 않음 (기준이 섞이면 왜곡됨)
    function comparablePrior(r, prior) {
      return (prior && prior.fs_div === r.fs_div) ? prior : null;
    }

    // 전기와 당기 값의 평균. 전기 자료가 없으면 당기(기말) 값을 그대로 사용
    function avgOf(cur, prev) {
      if (cur == null) return null;
      return prev != null ? (cur + prev) / 2 : cur;
    }

    // 지배주주 ROE = 지배주주순이익 / 평균 지배주주자본 (분자·분모를 같은 기준으로 맞춤)
    function computeParentROEAvg(r, prior) {
      const eq = avgOf(r.parent_equity, prior ? prior.parent_equity : null);
      if (r.parent_net_income == null || !eq) return null;
      return r.parent_net_income / eq;
    }

    // 밸류에이션용 ROE: 지배주주 기준 우선, 지배주주 자료가 없으면 연결(순이익 / 평균 총자본) 기준으로 대체
    function computeROEAvg(r, prior) {
      prior = comparablePrior(r, prior);
      const p = computeParentROEAvg(r, prior);
      if (p != null) return { value: p, basis: 'parent' };
      const eq = avgOf(r.total_equity, prior ? prior.total_equity : null);
      if (r.net_income == null || !eq) return null;
      return { value: r.net_income / eq, basis: 'consolidated' };
    }

    // 기간말(분기말/연말) 종가 → 공시일 종가까지의 등락률. 실적 발표 전후 주가 반응을 보기 위함.
    function priceReturnOf(r) {
      if (r.price_at_period_end == null || r.price_at_period_end <= 0 || r.price_at_filing == null) return null;
      return r.price_at_filing / r.price_at_period_end - 1;
    }

    // 배당성향 = 주당배당금 ÷ EPS(연간, 지배주주순이익÷유통주식수). 적자 연도나 배당 데이터가 없으면 null.
    // (적자 연도에 배당을 하면 배당성향이 음수/왜곡되어 의미가 없어 null 처리)
    function payoutRatioOf(r) {
      const outstanding = shareOut(r);
      if (!outstanding || outstanding <= 0) return null;
      // 분기별 보기의 4분기 행은 이익이 4분기 단독값이라, 배당성향은 "연간" 이익으로 계산한다
      const pn = r.annual_parent_net_income !== undefined ? r.annual_parent_net_income : r.parent_net_income;
      const nn = r.annual_net_income !== undefined ? r.annual_net_income : r.net_income;
      const earnings = pn != null ? pn : nn;
      if (earnings == null || earnings <= 0 || r.dividend_per_share == null) return null;
      const eps = earnings / outstanding;
      return r.dividend_per_share / eps;
    }

    function computeStepMetrics(r, prior) {
      prior = comparablePrior(r, prior);
      const totalAssets = (r.total_liabilities != null && r.total_equity != null) ? r.total_liabilities + r.total_equity : null;
      const priorAssets = (prior && prior.total_liabilities != null && prior.total_equity != null) ? prior.total_liabilities + prior.total_equity : null;
      const avgAssets = (totalAssets != null && priorAssets != null) ? (totalAssets + priorAssets) / 2 : totalAssets;
      const avgEquity = (r.total_equity != null && prior && prior.total_equity != null) ? (r.total_equity + prior.total_equity) / 2 : r.total_equity;

      const ebit = (r.pretax_income != null && r.interest_expense != null) ? r.pretax_income + r.interest_expense : null;

      const taxBurden = (r.net_income != null && r.pretax_income) ? r.net_income / r.pretax_income : null;
      const interestBurden = (r.pretax_income != null && ebit) ? r.pretax_income / ebit : null;
      const ebitMargin = (ebit != null && r.revenue) ? ebit / r.revenue : null;
      const assetTurnover = (r.revenue != null && avgAssets) ? r.revenue / avgAssets : null;
      const leverage = (avgAssets != null && avgEquity) ? avgAssets / avgEquity : null;

      const factors = [taxBurden, interestBurden, ebitMargin, assetTurnover, leverage];
      const roeCheck = factors.every((v) => v != null) ? factors.reduce((a, b) => a * b, 1) : null;
      const parentROE = computeParentROEAvg(r, prior);
      const roic = computeROIC(r, prior);

      return {
        label: r.period_label, isOFS: r.fs_div === 'OFS', taxBurden, interestBurden, ebitMargin, assetTurnover, leverage, roeCheck, parentROE, roic,
        periodEndPrice: r.price_at_period_end, filingPrice: r.price_at_filing, priceReturn: priceReturnOf(r),
      };
    }

    function compute5StepRows(annualRows) {
      const byYear = {};
      annualRows.forEach((r) => { byYear[r.bsns_year] = r; });
      return annualRows.map((r) => computeStepMetrics(r, byYear[String(Number(r.bsns_year) - 1)]));
    }

    // 특정 분기(r)를 끝점으로 하는 "최근 4개 분기 합산"(TTM, 연환산) 손익/현금흐름 값을 만든다.
    // quarterlyRows 안에서 r 자신 + 그 전 분기들을 찾아 합산하며, 재무상태표 항목(자본·부채 등)과
    // 주가·공시일 등 시점값은 r의 원래 값을 그대로 둔다(합산 대상은 FLOW_KEYS뿐).
    function buildTTMFlowForQuarter(quarterlyRows, r) {
      const Y = Number(r.bsns_year);
      const N = Number(r.period_order) % 10;
      if (!(N >= 1 && N <= 4)) return null;
      const find = (year, q) => quarterlyRows.find((x) => x.bsns_year === String(year) && x.period_label === \`\${year} \${q}분기\`);

      const needed = [];
      for (let k = 1; k <= N; k++) needed.push(find(Y, k));
      for (let k = N + 1; k <= 4; k++) needed.push(find(Y - 1, k));
      if (needed.some((x) => !x)) return null; // 4개 분기가 다 모여야 TTM 계산 가능

      const ttm = { ...r };
      const mixedBasis = new Set(needed.map((x) => x.fs_div).filter(Boolean)).size > 1; // 연결/개별 혼합 TTM 금지
      for (const key of FLOW_KEYS) {
        const vals = needed.map((x) => x[key]);
        ttm[key] = (!mixedBasis && vals.every((v) => v != null)) ? vals.reduce((a, b) => a + b, 0) : null;
      }
      return ttm;
    }

    // 사업보고서가 아직 없는 최신 연도를 위한 TTM(최근 4개 분기 합산) 행 생성
    function buildTTMRow(quarterlyRows) {
      if (quarterlyRows.length === 0) return null;
      const latest = quarterlyRows.reduce((a, b) => (b.period_order > a.period_order ? b : a));
      // period_order = 연도*10 + 분기번호 (예: 20262 → 2026년 2분기)
      const N = Number(latest.period_order) % 10;
      if (!(N >= 1 && N <= 4) || N === 4) return null; // 4분기(이미 사업보고서 있음)는 TTM 불필요

      const ttmFlow = buildTTMFlowForQuarter(quarterlyRows, latest);
      if (!ttmFlow) return null;

      const Y = Number(latest.bsns_year);
      const ttm = { ...ttmFlow, period_label: \`\${Y} TTM (\${N}분기 기준)\` };
      const priorSameQ = quarterlyRows.find((x) => x.bsns_year === String(Y - 1) && x.period_label === \`\${Y - 1} \${N}분기\`);
      return { row: ttm, prior: priorSameQ };
    }

    // 분기별 보기용 5단계 분석: 각 분기를 끝점으로 한 TTM(연환산) 손익 + 그 시점 재무상태표를 섞어서 계산.
    // (분기 단독 손익 그대로 쓰면 자산회전율·마진 등이 1/4 수준으로 왜곡되어 연도간 비교가 안 됨)
    function compute5StepRowsQuarterly(quarterlyRows) {
      return quarterlyRows.map((r) => {
        const N = Number(r.period_order) % 10;
        const Y = Number(r.bsns_year);
        const ttmFlow = buildTTMFlowForQuarter(quarterlyRows, r);
        const priorSameQ = quarterlyRows.find((x) => x.bsns_year === String(Y - 1) && x.period_label === \`\${Y - 1} \${N}분기\`);
        if (!ttmFlow) {
          // TTM 계산에 필요한 과거 분기가 부족 — 비율은 N/A 처리하되 주가 정보는 그대로 보여줌
          return { label: r.period_label + ' (TTM 자료 부족)', isOFS: r.fs_div === 'OFS', taxBurden: null, interestBurden: null, ebitMargin: null, assetTurnover: null, leverage: null, roeCheck: null, parentROE: null, roic: null, periodEndPrice: r.price_at_period_end, filingPrice: r.price_at_filing, priceReturn: priceReturnOf(r) };
        }
        return computeStepMetrics(ttmFlow, priorSameQ);
      });
    }

    let fiveStepMode = 'annual';
    function setFiveStepView(mode) {
      fiveStepMode = mode;
      document.getElementById('fsBtnAnnual').className = mode === 'annual' ? 'toggle-active' : '';
      document.getElementById('fsBtnQuarterly').className = mode === 'quarterly' ? 'toggle-active' : '';
      renderFiveStep();
    }

    function renderFiveStep() {
      const quarterlyRows = toQuarterlyRows(rawRows);
      let steps;
      let noteLabel;

      if (fiveStepMode === 'quarterly') {
        steps = compute5StepRowsQuarterly(quarterlyRows);
        noteLabel = '분기별 (손익·현금흐름은 해당 분기를 끝점으로 한 TTM 연환산값, 재무상태표는 해당 분기말 시점)';
      } else {
        const annualRows = toAnnualRows(rawRows);
        const ttm = buildTTMRow(quarterlyRows);
        steps = compute5StepRows(annualRows);
        if (ttm) steps.push(computeStepMetrics(ttm.row, ttm.prior));
        noteLabel = '연도별, 최신 연도는 사업보고서 없으면 TTM';
      }

      if (steps.length === 0) {
        const el0 = document.getElementById('fiveStepWrap');
        el0.style.display = 'block';
        el0.innerHTML = '<div class="card meta-line">분석할 데이터가 없습니다.</div>';
        return;
      }

      const pct = (v) => v != null ? (v * 100).toFixed(1) + '%' : 'N/A';
      const num = (v) => v != null ? v.toFixed(2) : 'N/A';
      const won = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';

      // 관련 열끼리 그룹 지어 2단 헤더로 — "듀폰 5단계 분해 요인"이 하나의 덩어리로 보이고,
      // 그 결과인 ROE/ROIC가 별도 덩어리로 분리되어 "무엇을 분해해서 무엇을 얻었는지"가 한눈에 보인다.
      const FIVE_STEP_GROUPS = [
        { name: '기간', color: '#64748b', cols: [{ label: '기간', key: 'label' }] },
        { name: '주가·수익률', color: '#2563eb', cols: [
          { label: '기간말 주가', key: 'periodEndPrice' },
          { label: '공시일 주가', key: 'filingPrice' },
          { label: '기간말→공시일<br/>상승률', key: 'priceReturn' },
        ] },
        { name: '듀폰 5단계 분해 요인 (곱하면 아래 ROE)', color: '#0d9488', cols: [
          { label: '세율부담<br/>(순이익/세전)', key: 'taxBurden' },
          { label: '이자부담<br/>(세전/EBIT)', key: 'interestBurden' },
          { label: 'EBIT마진', key: 'ebitMargin' },
          { label: '자산회전율', key: 'assetTurnover' },
          { label: '레버리지', key: 'leverage' },
        ] },
        { name: '결과: ROE·ROIC', color: '#7c3aed', cols: [
          { label: '계산된 ROE<br/>(연결·평균자본)', key: 'roeCheck' },
          { label: '지배주주 ROE<br/>(평균 지배자본)', key: 'parentROE' },
          { label: 'ROIC', key: 'roic' },
        ] },
      ];
      const fmtFns = {
        label: (s) => s.label, periodEndPrice: (s) => won(s.periodEndPrice), filingPrice: (s) => won(s.filingPrice),
        priceReturn: (s) => pct(s.priceReturn), taxBurden: (s) => pct(s.taxBurden), interestBurden: (s) => pct(s.interestBurden),
        ebitMargin: (s) => pct(s.ebitMargin), assetTurnover: (s) => num(s.assetTurnover), leverage: (s) => num(s.leverage),
        roeCheck: (s) => pct(s.roeCheck),
        parentROE: (s) => s.parentROE == null && s.isOFS ? '해당없음(개별)' : pct(s.parentROE),
        roic: (s) => pct(s.roic),
      };
      const { headHtml, groupStartKeys } = buildGroupedThead(FIVE_STEP_GROUPS, (col, borderStyle) => \`<th style="\${borderStyle}">\${col.label}</th>\`);
      let bodyHtml = '';
      for (const s of steps) {
        bodyHtml += '<tr>' + FIVE_STEP_GROUPS.flatMap((g) => g.cols).map((col) => {
          const style = groupBorderStyle(col.key, groupStartKeys, FIVE_STEP_GROUPS);
          return \`<td style="\${style}">\${fmtFns[col.key](s)}</td>\`;
        }).join('') + '</tr>';
      }
      const html = \`<table>\${headHtml}<tbody>\${bodyHtml}</tbody></table>\`;
      const el = document.getElementById('fiveStepWrap');
      el.style.display = 'block';
      el.innerHTML = \`<div class="card-title">5단계 ROE 분해 (\${noteLabel})</div><div style="font-size:11.5px; color:var(--text-muted); margin:2px 0 8px;">세율부담 × 이자부담 × EBIT마진 × 자산회전율 × 레버리지 = ROE (듀폰 분석) — 다섯 요인을 곱하면 오른쪽 "계산된 ROE"가 나오는지 직접 검산해볼 수 있습니다.</div><div class="tscroll">\` + html + '</div>';
    }

    function latestSnapshotRow(rows) {
      // 대차대조표 항목(자본총계/주식수)이 있는 기간 중 가장 최근 것 — 사업보고서가 아직 없으면 최신 분기라도 사용
      const withData = rows.filter((r) => !r.error && r.total_equity != null && r.total_shares != null && r.treasury_shares != null);
      if (withData.length === 0) return null;
      return withData.reduce((a, b) => (b.period_order > a.period_order ? b : a));
    }

    // rows: 한 종목의 원본 기간 데이터, priceInput: 현재 주가(없으면 null)
    // 반환값은 순수 데이터(숫자/문자열)만 담아서, 단일종목 요약/비교 화면 양쪽에서 재사용한다.
    function computeSummaryMetrics(rows, priceInput) {
      const annualRows = toAnnualRows(rows);
      if (annualRows.length === 0) return null;

      const byYearForROE = {};
      annualRows.forEach((r) => { byYearForROE[r.bsns_year] = r; });
      const roics = annualRows.map((r) => computeROIC(r, byYearForROE[String(Number(r.bsns_year) - 1)])).filter((v) => v != null);
      const roeResults = annualRows
        .map((r) => {
          const roe = computeROEAvg(r, byYearForROE[String(Number(r.bsns_year) - 1)]);
          if (!roe || !isFinite(roe.value)) return null;
          // 지속가능성장률(SGR) 모델: g = ROE × 유보율(1-배당성향). 배당으로 사외유출된 이익은 자기자본 재투자에
          // 쓰이지 않으므로, ROE를 그대로 복리로 쓰는 것보다 BPS 성장 추정에 더 맞다(Higgins, 1977 지속가능성장률).
          // 배당 데이터가 없는 연도(배당 미공시/무배당)는 유보율 100%(배당성향 0)로 간주.
          const payout = payoutRatioOf(r);
          const retention = payout != null ? 1 - payout : 1;
          return { ...roe, payout, retention, adjusted: roe.value * retention };
        })
        .filter((x) => x != null);
      const roes = roeResults.map((x) => x.value);
      const consolCnt = roeResults.filter((x) => x.basis === 'consolidated').length;
      const avgROIC = roics.length ? roics.reduce((a, b) => a + b, 0) / roics.length : null;
      const avgROE = roes.length ? roes.reduce((a, b) => a + b, 0) / roes.length : null;
      const payoutKnownResults = roeResults.filter((x) => x.payout != null);
      const avgPayout = payoutKnownResults.length ? payoutKnownResults.reduce((a, x) => a + x.payout, 0) / payoutKnownResults.length : null;
      const adjustedROEs = roeResults.map((x) => x.adjusted);
      const avgAdjustedROE = adjustedROEs.length ? adjustedROEs.reduce((a, b) => a + b, 0) / adjustedROEs.length : null;

      // BPS/유통주식수/EPS는 "가장 최근 사업보고서"가 아니라 실제로 가장 최근 조회된 시점(분기 포함) 기준
      const latest = latestSnapshotRow(rows) || annualRows[annualRows.length - 1];
      const outstandingShares = shareOut(latest);
      const equityForBps = latest.parent_equity != null ? latest.parent_equity : latest.total_equity;
      const bpsBasis = latest.parent_equity != null ? '지배주주지분' : (latest.fs_div === 'OFS' ? '총자본(개별재무제표)' : '총자본(지배주주지분 자료 없음)');
      const bps = (outstandingShares && equityForBps != null) ? equityForBps / outstandingShares : null;
      // 10년 후 예상 주가는 "배당으로 유출되지 않고 재투자된 몫"만 복리로 쌓인다고 보는 게 더 타당 → 조정 ROE 사용
      const projected = (bps != null && avgAdjustedROE != null) ? bps * Math.pow(1 + avgAdjustedROE, 10) : null;

      // 예상 상승배수 및 연환산 기대수익률(CAGR): (예상가/현재가)^(1/10) - 1
      const expectedMultiple = (projected != null && priceInput) ? projected / priceInput : null;
      const annualizedReturn = (expectedMultiple != null && expectedMultiple > 0) ? Math.pow(expectedMultiple, 1 / 10) - 1 : null;

      // EPS: 사업보고서가 아직 없는 최신연도는 TTM(최근 4개분기 합산) 이익을 사용
      const quarterlyRows = toQuarterlyRows(rows);
      const ttm = buildTTMRow(quarterlyRows);
      const earningsRow = ttm ? ttm.row : annualRows[annualRows.length - 1];
      const earningsBasis = ttm ? 'TTM' : '연간';
      const earnings = earningsRow.parent_net_income != null ? earningsRow.parent_net_income : earningsRow.net_income;
      const earningsSrcBasis = earningsRow.parent_net_income != null ? '지배주주순이익' : (earningsRow.fs_div === 'OFS' ? '순이익(별도재무제표)' : '연결순이익');
      const eps = (outstandingShares && earnings != null) ? earnings / outstandingShares : null;

      const marketCap = (priceInput && outstandingShares) ? priceInput * outstandingShares : null;
      const per = (priceInput && eps) ? priceInput / eps : null;
      const pbr = (priceInput && bps) ? priceInput / bps : null;
      const earningsYield = eps && priceInput ? eps / priceInput : null; // = 1/PER, 요구수익률 관점

      // --- Financial(재무안전성): 부채비율, 이자보상배율 ---
      // latest(가장 최근 조회 시점, 분기 포함)를 기준으로 계산 — 재무상태표 항목은 분기에도 찍히므로 바로 쓸 수 있다.
      const debtRatio = (latest.total_liabilities != null && latest.total_equity) ? latest.total_liabilities / latest.total_equity : null;
      // 이자보상배율은 "그 기간의" 영업이익/이자비용이 필요해 분기 단독값이 섞인 latest보다 TTM(또는 최신 연간) 쪽이 맞다.
      // 차입금이 전혀 없으면(이자 부담 자체가 없음) 이자보상배율은 의미가 없다 — 금융비용은 외환·평가손실 등이 섞여 있어 오히려 왜곡된다.
      const noDebt = (latest.current_assets != null && anBorrow(latest) === 0);
      const interestCoverage = (!noDebt && earningsRow.operating_income != null && earningsRow.interest_expense) ? earningsRow.operating_income / earningsRow.interest_expense : null;

      // --- Quality 보강: 매출총이익/총자산(Gross Profitability, Novy-Marx 2013) ---
      // "The Other Side of Value: The Gross Profitability Premium" 논문에서 제시된 지표로, ROE·ROIC처럼
      // 부채(레버리지)를 거치지 않고 "영업 자체가 자산을 얼마나 효율적으로 이익으로 바꾸는지"를 더 순수하게
      // 보여준다는 실증 연구 결과가 있다(분식/이익조정에도 ROE보다 덜 민감하다는 평가도 있음).
      // 매출총이익/총자산 = (매출총이익/매출액) × (매출액/총자산) = 매출총이익률 × 총자산회전율로 분해해서,
      // "마진이 좋아서"인지 "자산을 적게 쓰고도 매출을 많이 내서"인지 원인까지 바로 보이게 한다.
      // 총자산은 별도 컬럼이 없어 회계등식(자산=부채+자본)으로 역산한다.
      const totalAssets = (latest.total_equity != null && latest.total_liabilities != null)
        ? latest.total_equity + latest.total_liabilities
        : null;
      const grossProfit = grossProfitOf(earningsRow);
      const grossMargin = (grossProfit != null && earningsRow.revenue) ? grossProfit / earningsRow.revenue : null;
      const assetTurnover = (earningsRow.revenue != null && totalAssets) ? earningsRow.revenue / totalAssets : null;
      const grossProfitability = (grossProfit != null && totalAssets) ? grossProfit / totalAssets : null;

      // --- Growth(성장성): 매출/순이익 YoY — 연간 데이터 중 최근 2개 연도 비교 ---
      let revenueGrowth = null, netIncomeGrowth = null;
      if (annualRows.length >= 2) {
        const curr = annualRows[annualRows.length - 1];
        const prev = annualRows[annualRows.length - 2];
        // 전기 값이 0 이하이면 성장률이 정의되지 않는다(적자→흑자를 "+몇백 %"로 보여주면 오해) → N/A. 분기·CAGR 계산과 같은 기준.
        if (curr.revenue != null && prev.revenue > 0) revenueGrowth = (curr.revenue - prev.revenue) / prev.revenue;
        const currNi = curr.parent_net_income != null ? curr.parent_net_income : curr.net_income;
        const prevNi = prev.parent_net_income != null ? prev.parent_net_income : prev.net_income;
        if (currNi != null && prevNi > 0) netIncomeGrowth = (currNi - prevNi) / prevNi;
      }

      return {
        avgROIC, roicN: roics.length, avgROE, roeN: roes.length, consolCnt,
        avgPayout, payoutN: payoutKnownResults.length, avgAdjustedROE,
        latestLabel: latest.period_label, bpsBasis, bps, projected,
        priceInput, marketCap, expectedMultiple, annualizedReturn,
        eps, epsBasis: earningsBasis + '·' + earningsSrcBasis, per, pbr, earningsYield,
        debtRatio, interestCoverage, noDebt, revenueGrowth, netIncomeGrowth,
        totalAssets, grossProfit, grossMargin, assetTurnover, grossProfitability,
      };
    }

    // 팩터 카드의 "지금 이 순간 숫자"만으로는 좋아지는 중인지 나빠지는 중인지 알 수 없다 — 연도별 시계열을
    // 만들어 각 지표 옆에 작은 추이(스파크라인)를 붙여준다. PER/PBR은 그 연도 공시 시점의 실제 주가 기준으로
    // 이미 저장돼있는 per_at_filing/pbr_at_filing을 쓰고(그래야 "지금 주가로 과거를 평가"하는 왜곡이 없음),
    // ROE/ROIC는 5단계 분해와 같은 계산(연도별 평균자본 기준)을 재사용해 두 화면의 숫자가 항상 일치하게 한다.
    function computeFactorHistory(rows) {
      const annualRows = toAnnualRows(rows);
      if (annualRows.length === 0) return null;
      const steps = compute5StepRows(annualRows);
      const years = annualRows.map((r) => r.bsns_year);
      const per = annualRows.map((r) => r.per_at_filing);
      const pbr = annualRows.map((r) => r.pbr_at_filing);
      const earningsYield = annualRows.map((r) => (r.per_at_filing ? 1 / r.per_at_filing : null));
      const avgROE = steps.map((s) => (s.parentROE != null ? s.parentROE : s.roeCheck));
      const avgROIC = steps.map((s) => s.roic);
      const totalAssetsArr = annualRows.map((r) => (r.total_equity != null && r.total_liabilities != null) ? r.total_equity + r.total_liabilities : null);
      const grossProfitArr = annualRows.map((r) => grossProfitOf(r));
      const grossMargin = annualRows.map((r, i) => (grossProfitArr[i] != null && r.revenue) ? grossProfitArr[i] / r.revenue : null);
      const assetTurnover = annualRows.map((r, i) => (r.revenue != null && totalAssetsArr[i]) ? r.revenue / totalAssetsArr[i] : null);
      const grossProfitability = annualRows.map((r, i) => (grossProfitArr[i] != null && totalAssetsArr[i]) ? grossProfitArr[i] / totalAssetsArr[i] : null);
      const debtRatio = annualRows.map((r) => (r.total_liabilities != null && r.total_equity) ? r.total_liabilities / r.total_equity : null);
      // 차입금이 0인 해는 이자보상배율이 의미 없다(헤드라인 값과 같은 기준)
      const interestCoverage = annualRows.map((r) => (r.operating_income != null && r.interest_expense && !(anRefetched(r) && anBorrow(r) === 0)) ? r.operating_income / r.interest_expense : null);
      const revenueGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = annualRows[i - 1];
        return (r.revenue != null && prev.revenue > 0) ? (r.revenue - prev.revenue) / prev.revenue : null;
      });
      const netIncomeGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = annualRows[i - 1];
        const cur = r.parent_net_income != null ? r.parent_net_income : r.net_income;
        const pr = prev.parent_net_income != null ? prev.parent_net_income : prev.net_income;
        return (cur != null && pr > 0) ? (cur - pr) / pr : null;
      });
      return { years, per, pbr, earningsYield, avgROE, avgROIC, grossProfitability, grossMargin, assetTurnover, debtRatio, interestCoverage, revenueGrowth, netIncomeGrowth };
    }

    // 작은 추이선(스파크라인). 축·눈금 없이 "오르는 중/내리는 중"만 한눈에 보여주는 용도(Tufte의 sparkline
    // 개념) — 라벨 옆에 바로 붙여도 거슬리지 않을 만큼 작게. 값이 2개 미만이면 "추이 N/A"로 대체.
    function renderSparkline(values, color) {
      const w = 64, h = 22, pad = 3;
      const valid = values.map((v, i) => ({ v, i })).filter((p) => p.v != null && isFinite(p.v));
      if (valid.length < 2) return '<span style="color:var(--text-muted); font-size:10px;">추이 N/A</span>';
      const vs = valid.map((p) => p.v);
      const min = Math.min(...vs), max = Math.max(...vs);
      const range = (max - min) || Math.abs(max) || 1;
      const n = values.length;
      const pts = valid.map((p) => {
        const x = n > 1 ? pad + (p.i / (n - 1)) * (w - 2 * pad) : w / 2;
        const y = h - pad - ((p.v - min) / range) * (h - 2 * pad);
        return [x, y];
      });
      const path = pts.map((pt) => pt[0].toFixed(1) + ',' + pt[1].toFixed(1)).join(' ');
      const last = pts[pts.length - 1];
      return \`<svg width="\${w}" height="\${h}" style="vertical-align:middle; flex-shrink:0;"><polyline points="\${path}" fill="none" stroke="\${color}" stroke-width="1.6"/><circle cx="\${last[0].toFixed(1)}" cy="\${last[1].toFixed(1)}" r="2.2" fill="\${color}"/></svg>\`;
    }

    // 지표별 "참고용" 절대 기준(업종에 따라 예외가 흔하므로 단정적 판단이 아니라 참고용 배지임을 분명히 한다).
    // 방향: 'high'면 높을수록 좋음(good>=goodAt), 'low'면 낮을수록 좋음(good<=goodAt).
    const FACTOR_RULES = {
      per:        { dir: 'low',  good: 15,   caution: 25,   fmt: (v) => v.toFixed(2) + '배' },
      pbr:        { dir: 'low',  good: 1.5,  caution: 3,    fmt: (v) => v.toFixed(2) + '배' },
      earningsYield: { dir: 'high', good: 0.07, caution: 0.04, fmt: (v) => (v * 100).toFixed(2) + '%' },
      avgROE:     { dir: 'high', good: 0.15, caution: 0.08, fmt: (v) => (v * 100).toFixed(2) + '%' },
      avgROIC:    { dir: 'high', good: 0.10, caution: 0.05, fmt: (v) => (v * 100).toFixed(2) + '%' },
      debtRatio:  { dir: 'low',  good: 1.0,  caution: 2.0,  fmt: (v) => (v * 100).toFixed(1) + '%' },
      interestCoverage: { dir: 'high', good: 5, caution: 1.5, fmt: (v) => v.toFixed(2) + '배' },
      revenueGrowth:   { dir: 'high', good: 0.10, caution: 0.05, fmt: (v) => (v * 100).toFixed(2) + '%' },
      netIncomeGrowth: { dir: 'high', good: 0.10, caution: 0.05, fmt: (v) => (v * 100).toFixed(2) + '%' },
      // Novy-Marx(2013) 원 논문의 미국 대형주 상위 분위 기준(매출총이익/총자산 ≈ 0.33 이상)은 한국 시장·업종
      // 구성이 달라 그대로 쓰기 어려워, 보수적으로 눈금만 낮춰 참고용으로 둔다(동종업계 비교가 더 정확함).
      grossProfitability: { dir: 'high', good: 0.20, caution: 0.10, fmt: (v) => (v * 100).toFixed(2) + '%' },
      grossMargin:        { dir: 'high', good: 0.30, caution: 0.15, fmt: (v) => (v * 100).toFixed(2) + '%' },
      assetTurnover:      { dir: 'high', good: 1.0,  caution: 0.5,  fmt: (v) => v.toFixed(2) + '회' },
    };
    function judgeFactor(key, value) {
      if (value == null || !isFinite(value)) return { label: 'N/A', color: '#94a3b8', text: 'N/A' };
      const rule = FACTOR_RULES[key];
      if (!rule) return { label: '', color: '#94a3b8', text: rule ? rule.fmt(value) : String(value) };
      const isGood = rule.dir === 'high' ? value >= rule.good : value <= rule.good;
      const isCaution = rule.dir === 'high' ? value >= rule.caution : value <= rule.caution;
      const color = isGood ? '#16a34a' : (isCaution ? '#d97706' : '#dc2626');
      const label = isGood ? '양호' : (isCaution ? '보통' : '주의');
      return { label, color, text: rule.fmt(value) };
    }
    // 하나의 팩터 카드(Value/Quality/Financial/Growth)를 그려주는 공통 헬퍼.
    // items: [{ label, key, value, indent }] — key가 FACTOR_RULES에 있으면 배지가, 없으면 값만 표시된다.
    // indent:true면 "그 위 항목을 분해해서 보여주는 하위 항목"으로 살짝 들여써서 표시한다
    // (예: 매출총이익/총자산 = 매출총이익률 × 총자산회전율 분해).
    // subtitle: 이 팩터가 왜 투자지표로 쓰이는지 한 줄로 알려주는 설명(학술적 근거를 투자 초심자도
    // 바로 알 수 있게, UI에서 "이게 뭔데 좋은 거야?"라는 궁금증을 그 자리에서 풀어주기 위함).
    function renderFactorCard(title, color, items, subtitle) {
      const rows = items.map(({ label, key, value, indent, series }) => {
        const j = judgeFactor(key, value);
        const badge = j.label ? \`<span style="background:\${j.color}1a; color:\${j.color}; border:1px solid \${j.color}55; border-radius:999px; padding:2px 8px; font-size:11px; font-weight:600; margin-left:6px;">\${j.label}</span>\` : '';
        const labelStyle = indent ? 'color:var(--text-muted); font-size:12px; padding-left:14px;' : 'color:var(--text-muted); font-size:13px;';
        const spark = series ? renderSparkline(series, color) : '';
        return \`<div style="display:flex; justify-content:space-between; align-items:center; gap:8px; padding:5px 0; border-bottom:1px solid var(--border);">
          <span style="\${labelStyle}">\${indent ? '└ ' : ''}\${label}</span>
          <span style="display:flex; align-items:center; gap:8px;">\${spark}<span style="font-weight:\${indent ? '500' : '600'}; white-space:nowrap;">\${j.text}\${badge}</span></span>
        </div>\`;
      }).join('');
      const subtitleHtml = subtitle ? \`<div style="font-size:11.5px; color:var(--text-muted); margin:-2px 0 8px; line-height:1.4;">\${subtitle}</div>\` : '';
      return \`<div class="card" style="border-left:4px solid \${color};">
        <div class="card-title" style="color:\${color};">\${title}</div>
        \${subtitleHtml}
        \${rows}
      </div>\`;
    }

    // 밸류에이션 시뮬레이션 카드가 현재 어떤 종목 기준으로 그려졌는지(가정 입력값 리셋·재계산에 사용).
    let lastSummaryMetrics = null;

    function renderSummary() {
      const priceInput = Number(document.getElementById('currentPrice').value) || null;
      const m = computeSummaryMetrics(rawRows, priceInput);
      if (!m) {
        const el0 = document.getElementById('summary');
        el0.style.display = 'block';
        el0.innerHTML = '<div class="card meta-line">연간(사업보고서) 데이터가 없어 요약을 만들 수 없습니다. 분석하기를 다시 누르거나 고급 설정에서 DART 데이터를 받아주세요.</div>';
        return;
      }
      lastSummaryMetrics = m;
      const hist = computeFactorHistory(rawRows);

      const pctStr = (v) => v != null ? (v * 100).toFixed(2) + '%' : 'N/A';
      const wonStr = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';

      const el = document.getElementById('summary');
      el.style.display = 'block';

      const factorCards = [
        renderFactorCard('💰 Value (가치)', '#2563eb', [
          { label: 'PER', key: 'per', value: m.per, series: hist && hist.per },
          { label: 'PBR', key: 'pbr', value: m.pbr, series: hist && hist.pbr },
          { label: '이익수익률(1/PER)', key: 'earningsYield', value: m.earningsYield, series: hist && hist.earningsYield },
        ], '가격 대비 얼마나 싼가. Fama-French의 Value 팩터(HML) — 쌀수록 장기적으로 초과수익 경향이 있다는 실증 연구. 추이선은 연도별 공시시점 주가 기준.'),
        renderFactorCard('🏆 Quality (수익성)', '#7c3aed', [
          { label: 'ROE 평균 (' + m.roeN + '개년)', key: 'avgROE', value: m.avgROE, series: hist && hist.avgROE },
          { label: 'ROIC 평균 (' + m.roicN + '개년)', key: 'avgROIC', value: m.avgROIC, series: hist && hist.avgROIC },
          { label: '매출총이익/총자산', key: 'grossProfitability', value: m.grossProfitability, series: hist && hist.grossProfitability },
          { label: '매출총이익률', key: 'grossMargin', value: m.grossMargin, indent: true, series: hist && hist.grossMargin },
          { label: '총자산회전율', key: 'assetTurnover', value: m.assetTurnover, indent: true, series: hist && hist.assetTurnover },
        ], '번 돈의 질이 좋은가. ROE·ROIC는 레버리지가 섞일 수 있는 반면, 매출총이익/총자산(Novy-Marx, 2013)은 부채 효과 없이 "영업 자체"의 수익성을 보여줌 — 마진(이익률)이 좋아서인지, 자산 회전이 빨라서인지 아래 두 줄로 원인도 함께 확인.'),
        renderFactorCard('🛡 Financial (재무안전성)', '#0d9488', [
          { label: '부채비율(부채/자본)', key: 'debtRatio', value: m.debtRatio, series: hist && hist.debtRatio },
          { label: m.noDebt ? '이자보상배율 (차입금 없음 — 해당 없음)' : '이자보상배율 (영업이익/금융비용)', key: 'interestCoverage', value: m.interestCoverage, series: hist && hist.interestCoverage },
        ], '빚 때문에 무너질 위험이 낮은가. Altman Z-Score류 부실예측 연구에서 공통적으로 핵심 변수로 쓰이는 두 지표.'),
        renderFactorCard('🌱 Growth (성장성, YoY)', '#ea580c', [
          { label: '매출액 성장률', key: 'revenueGrowth', value: m.revenueGrowth, series: hist && hist.revenueGrowth },
          { label: '순이익 성장률', key: 'netIncomeGrowth', value: m.netIncomeGrowth, series: hist && hist.netIncomeGrowth },
        ], '사업이 커지고 있는가. 전년 대비 증가율로, 다른 세 팩터와 달리 "현재 수준"이 아니라 "방향"을 본다.'),
      ].join('');

      const statTile = (label, value) => \`<div class="stat-tile"><div class="stat-label">\${label}</div><div class="stat-value">\${value}</div></div>\`;

      el.innerHTML = \`
        <div style="font-size:12px; color:var(--text-muted); margin-bottom:8px;">배지는 참고용 절대 기준(업종별 예외가 흔하니 동종업계와 비교해 참고하세요): PER↓PBR↓ 좋음, ROE·ROIC·이자보상배율·성장률은 ↑ 좋음. 작은 꺾은선은 연도별 추이(점이 최신 연도).</div>
        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(250px, 1fr)); gap:10px;">\${factorCards}</div>

        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:8px; margin:6px 0 10px;">
          \${statTile('평균 배당성향', pctStr(m.avgPayout) + '<div style="font-size:10.5px; color:var(--text-muted); font-weight:400;">' + anPeriodTxt(anPeriod(anBuildSeries(rawRows), 'payout', 10)) + '</div>')}
          \${statTile('ROE 평균 / 기간', pctStr(m.avgROE) + '<div style="font-size:10.5px; color:var(--text-muted); font-weight:400;">' + anPeriodTxt(anPeriod(anBuildSeries(rawRows), 'roe', 10)) + '</div>')}
          \${statTile(\`최근 BPS (\${m.latestLabel})\`, wonStr(m.bps))}
          \${statTile(\`EPS (\${m.epsBasis})\`, m.eps != null ? Math.round(m.eps).toLocaleString() + '원' : 'N/A')}
          \${m.marketCap != null ? statTile('참고 시가총액', wonStr(m.marketCap)) : '<div class="stat-tile"><div class="stat-label">시가총액</div><div class="stat-value" style="font-size:12px; color:var(--danger);">현재 주가 입력 필요</div></div>'}
        </div>
        <div id="analysis"></div>
      \`;
      renderAnalysis();
    }

// ================= 재무분석 확장: 영역별 지표 · 시나리오 밸류에이션 · 분류 · 판단 요약 =================
// 원칙: 정의가 분명하고 학술적으로 쓰이는 지표만 넣는다. 임계값(기준선)은 "참고용 휴리스틱"이며 AN_TH에서 한 곳에 모아 두었다.
const AN_TH = { roicGood: 0.10, roicLow: 0.06, roeGood: 0.15, growthHigh: 0.10, growthVeryHigh: 0.15, growthLow: 0.05, fcfNiGood: 0.7, accrualMax: 0.05, perLow: 10, pbrLow: 1, taxRate: 0.24, defaultR: 0.09, omega: 0.62, minDIC: 0.10, perGraham: 15, pbrGraham: 1.5, grahamProd: 22.5, roicSpread: 0.03, pioLow: 3, shareTol: 0.01 };
const AN_STATE = { r: AN_TH.defaultR, payout: null, ov: {}, key: null, mom: null, momFor: null, momErr: null };
let AN_CTX = null;

const anFin = (v) => v != null && typeof v === 'number' && isFinite(v);
const anDiv = (a, b) => (anFin(a) && anFin(b) && b !== 0) ? a / b : null;
const anPct = (v, d) => anFin(v) ? (v * 100).toFixed(d == null ? 1 : d) + '%' : 'N/A';
const anX = (v, d) => anFin(v) ? v.toFixed(d == null ? 2 : d) + '배' : 'N/A';
const anEok = (v) => anFin(v) ? Math.round(v / 1e8).toLocaleString() + '억' : 'N/A';
const anWon = (v) => anFin(v) ? Math.round(v).toLocaleString() + '원' : 'N/A';
const anNum = (v, d) => anFin(v) ? v.toFixed(d == null ? 2 : d) : 'N/A';
const anFmt = (kind, v) => kind === 'eok' ? anEok(v) : kind === 'pct' ? anPct(v) : kind === 'x' ? anX(v) : kind === 'won' ? anWon(v) : anNum(v);
const anMean = (a) => { const x = a.filter(anFin); return x.length ? x.reduce((s, v) => s + v, 0) / x.length : null; };
const anStd = (a) => { const x = a.filter(anFin); if (x.length < 2) return null; const m = anMean(x); return Math.sqrt(x.reduce((s, v) => s + (v - m) * (v - m), 0) / (x.length - 1)); };
const anMedian = (a) => { const x = a.filter(anFin).sort((p, q) => p - q); if (!x.length) return null; const h = x.length >> 1; return x.length % 2 ? x[h] : (x[h - 1] + x[h]) / 2; };
const anClamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const anOutstanding = (r) => shareOut(r);
const anNI = (r) => r.parent_net_income != null ? r.parent_net_income : r.net_income;
const anAssets = (r) => (r.total_equity != null && r.total_liabilities != null) ? r.total_equity + r.total_liabilities : null;
// 새 항목(감가상각·배당지급 등)은 재조회 이후에만 채워진다. current_assets가 있으면 "신규 항목까지 수집된 행"으로 본다.
const anRefetched = (r) => r.current_assets != null;
const anFlow0 = (r, k) => r[k] != null ? r[k] : (anRefetched(r) ? 0 : null);
const anEsc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;');

function anBorrow(r) { return anRefetched(r) ? (r.short_term_borrowings || 0) + (r.current_portion_lt_debt || 0) + (r.long_term_borrowings || 0) + (r.bonds || 0) : null; }
function anLiquid(r) { return r.cash != null ? (r.cash || 0) + (r.st_financial_assets || 0) : null; }

// ---------- 연도별 시계열 ----------
function anBuildSeries(rows) {
  const annual = toAnnualRows(rows);
  const by = {}; annual.forEach((r) => { by[r.bsns_year] = r; });
  const list = annual.map((r) => {
    const pv = by[String(Number(r.bsns_year) - 1)];
    const prior = pv && pv.fs_div === r.fs_div ? pv : null;
    const d = { year: Number(r.bsns_year), r };
    d.revenue = r.revenue;
    d.gp = grossProfitOf(r);
    d.ebit = r.operating_income; d.ni = anNI(r); d.niCons = r.net_income;
    d.shares = anOutstanding(r); d.eps = anDiv(d.ni, d.shares);
    d.ocf = r.ocf; d.capex = r.capex; d.fcf = r.fcf;
    d.salesPrev = prior ? prior.revenue : null; d.ppePrev = prior ? prior.tangible_assets : null; // 오너어닝스(유지보수 CAPEX)용: 전기 매출·전기말 유형자산(연결/개별 기준이 같은 전기만)
    d.da = anRefetched(r) && (r.depreciation != null || r.amortization != null) ? (r.depreciation || 0) + (r.amortization || 0) : null;
    d.ebitda = anFin(d.ebit) && anFin(d.da) ? d.ebit + d.da : null;
    d.assets = anAssets(r); d.avgAssets = avgOf(d.assets, prior ? anAssets(prior) : null);
    d.equity = r.parent_equity != null ? r.parent_equity : r.total_equity;
    d.ic = computeIC(r); d.nopat = anFin(d.ebit) ? d.ebit * (1 - effTaxRate(r)) : null;
    const roe = computeROEAvg(r, pv); d.roe = roe && isFinite(roe.value) ? roe.value : null;
    d.roic = computeROIC(r, pv);
    { const q4 = computeROIC4(r, pv); d.r4 = q4; d.r4gm = q4 ? q4.gm : null; d.r4turn = q4 ? q4.turn : null; d.r4gpa = q4 ? q4.gpa : null; d.r4opx = q4 ? q4.opx : null; d.r4lev = q4 ? q4.lev : null; d.r4roic = q4 ? q4.roic : null; d.r4chk = q4 ? q4.check - q4.roic : null; }
    d.gm = anDiv(d.gp, d.revenue);
    d.gpa = anDiv(d.gp, d.assets); d.opm = anDiv(d.ebit, d.revenue); d.fcfm = anDiv(d.fcf, d.revenue);
    const niC = anFin(d.niCons) ? d.niCons : d.ni; // 영업CF·FCF는 연결 기준이므로 분모도 연결 순이익
    d.fcfni = niC > 0 ? anDiv(d.fcf, niC) : null; d.cfoni = niC > 0 ? anDiv(d.ocf, niC) : null;
    d.accrual = (anFin(d.niCons) && anFin(r.ocf)) ? anDiv(d.niCons - r.ocf, d.avgAssets) : null; // Sloan(1996) 계열 발생액, 현금흐름법(Hribar-Collins 2002) = (순이익-영업CF)/평균총자산
    d.at = anDiv(d.revenue, d.avgAssets);
    const wcA = (r.receivables != null || r.inventory != null) ? (r.receivables || 0) + (r.other_receivables || 0) + (r.inventory || 0) : null;
    const wcL = r.payables != null ? (r.payables || 0) + (r.other_payables || 0) : null;
    d.wc = (wcA != null && wcL != null) ? wcA - wcL : null; d.wcRev = anDiv(d.wc, d.revenue);
    d.borrow = anBorrow(r); d.liquid = anLiquid(r);
    d.netDebt = anFin(d.borrow) && anFin(d.liquid) ? d.borrow - d.liquid : null;
    d.de = anDiv(r.total_liabilities, r.total_equity); d.ibde = anDiv(d.borrow, r.total_equity);
    d.nde = (anFin(d.netDebt) && d.ebitda > 0) ? d.netDebt / d.ebitda : null;
    d.icov = (r.interest_expense && d.borrow !== 0) ? anDiv(d.ebit, r.interest_expense) : null; // 차입금 0이면 N/A
    d.cur = anDiv(r.current_assets, r.current_liabilities);
    d.cashDebt = d.borrow > 0 ? anDiv(d.liquid, d.borrow) : null;
    d.payout = payoutRatioOf(r);
    d.divPaid = anFlow0(r, 'dividends_paid'); d.buyback = anFlow0(r, 'buyback'); 
    d.rd = anFlow0(r, 'intangible_capex');
    d.shp = (d.ni > 0 && anFin(d.divPaid)) ? (d.divPaid + (d.buyback || 0)) / d.ni : null;
    d.capexRev = anDiv(d.capex, d.revenue); d.capexDa = d.da > 0 ? anDiv(d.capex, d.da) : null;
    d.per = r.per_at_filing; d.pbr = r.pbr_at_filing; d.fcfy = r.fcf_yield_at_filing;
    return d;
  });
  const idx = {}; list.forEach((d) => { idx[d.year] = d; });
  list.forEach((d) => {
    const p = idx[d.year - 1];
    d.shareChg = (p && anFin(d.shares) && anFin(p.shares) && p.shares > 0) ? d.shares / p.shares - 1 : null;
    const yoy = (k) => (p && anFin(d[k]) && anFin(p[k]) && p[k] > 0) ? d[k] / p[k] - 1 : null;
    ['revenue', 'gp', 'ebit', 'eps', 'fcf', 'ni'].forEach((k) => { d[k + 'Yoy'] = yoy(k); });
    if (anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3) d.epsYoy = null; // 액면분할·증자로 주식수가 30% 넘게 변한 해는 EPS 증가율이 왜곡되므로 N/A
    d.dIC = (p && anFin(d.ic) && anFin(p.ic)) ? d.ic - p.ic : null;
    d.reinv = (anFin(d.dIC) && d.nopat > 0) ? d.dIC / d.nopat : null;
    // 증분 ROIC(3년): ΔNOPAT(최근 3년) ÷ ΔInvested Capital(투자는 이익에 선행하므로 1년 앞선 시점부터 3년간)
    const a = idx[d.year - 3], b1 = idx[d.year - 1], b4 = idx[d.year - 4];
    d.incRoic = (a && b1 && b4 && anFin(d.nopat) && anFin(a.nopat) && anFin(b1.ic) && anFin(b4.ic) && (b1.ic - b4.ic) > AN_TH.minDIC * Math.abs(b4.ic)) ? (d.nopat - a.nopat) / (b1.ic - b4.ic) : null; // 투하자본이 10% 이상 늘지 않았으면 분모가 너무 작아 의미 없음
    d.incRoe = (a && b1 && b4 && anFin(d.ni) && anFin(a.ni) && anFin(b1.equity) && anFin(b4.equity) && (b1.equity - b4.equity) > AN_TH.minDIC * Math.abs(b4.equity)) ? (d.ni - a.ni) / (b1.equity - b4.equity) : null;
  });
  return list;
}
const anWin = (list, key, n) => { const ly = list.length ? list[list.length - 1].year : 0; return list.filter((d) => d.year > ly - n).map((d) => d[key]); }; // 연도 기준 창(빠진 해가 있어도 N년을 넘지 않음)
function anCagr(list, key, n) {
  const last = list[list.length - 1]; if (!last) return null;
  // EPS는 구간 중 주식수가 30% 넘게 변한 해가 있으면(분할·증자) 계산하지 않음 — 호출부가 순이익 기준으로 대체
  if (key === 'eps' && list.some((d) => d.year > last.year - n && anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3)) return null;
  const start = list.find((d) => d.year === last.year - n);
  if (!start || !anFin(last[key]) || !anFin(start[key]) || last[key] <= 0 || start[key] <= 0) return null;
  return Math.pow(last[key] / start[key], 1 / n) - 1;
}
// 값이 있는 연도들의 기간(시작~끝)과 유효연도 수
function anPeriod(list, key, n) {
  const ys = list.slice(-n).filter((d) => anFin(d[key])).map((d) => d.year);
  return ys.length ? { from: ys[0], to: ys[ys.length - 1], n: ys.length } : { from: null, to: null, n: 0 };
}
const anPeriodTxt = (p) => p.n ? ('기간: ' + p.from + '~' + p.to + ' / 유효연도: ' + p.n + '년') : '유효연도 없음';

// ---------- 최근 분기 YoY (최근 3개 분기) ----------
function anQuarterGrowth(rows) {
  const q = toQuarterlyRows(rows);
  const val = (r, k) => k === 'eps' ? anDiv(anNI(r), anOutstanding(r)) : k === 'gp' ? grossProfitOf(r) : k === 'ni' ? anNI(r) : r[k];
  const find = (y, n) => q.find((x) => x.period_label === (y + ' ' + n + '분기'));
  const last3 = q.slice(-3).reverse(); // 최신 → 과거
  if (!last3.length) return null;
  const keys = [['revenue', '매출액'], ['gp', '매출총이익'], ['ebit', '영업이익'], ['ni', '순이익'], ['eps', 'EPS'], ['fcf', 'FCF']];
  const out = { labels: last3.map((r) => r.period_label), rows: [] };
  for (const [k, name] of keys) {
    const kk = k === 'ebit' ? 'operating_income' : k;
    const g = last3.map((r) => {
      const y = Number(r.bsns_year), n = Number(r.period_label.split(' ')[1].replace('분기', ''));
      const pr = find(y - 1, n);
      const c = val(r, kk), p = pr ? val(pr, kk) : null;
      // 액면분할·증자 등으로 주식수가 30% 넘게 달라졌으면 EPS 비교가 왜곡되므로 N/A
      if (k === 'eps' && pr) { const s1 = anOutstanding(r), s0 = anOutstanding(pr); if (!(s1 > 0 && s0 > 0) || Math.abs(s1 / s0 - 1) > 0.3) return null; }
      return (anFin(c) && anFin(p) && p > 0) ? (c - p) / p : null;
    });
    let accel = null;
    if (anFin(g[0])) {
      const base = g.slice(1).filter(anFin);
      if (base.length) { const dlt = g[0] - anMean(base); accel = dlt > 0.03 ? '가속' : dlt < -0.03 ? '둔화' : '유지'; }
    }
    out.rows.push({ name, g, accel });
  }
  return out;
}

// ---------- 모멘텀(재무 탭에 있는 기간말 주가 기반의 약식) ----------
function anMomentum(rows, price) {
  if (!price) return null;
  const q = toQuarterlyRows(rows).filter((r) => r.price_at_period_end > 0);
  if (!q.length) return null;
  const endDate = (r) => { const n = Number(r.period_order) % 10; const md = n === 1 ? [2, 31] : n === 2 ? [5, 30] : n === 3 ? [8, 30] : [11, 31]; return Date.UTC(Number(r.bsns_year), md[0], md[1]); };
  const now = Date.now();
  const pick = (days) => {
    const target = now - days * 86400000; let best = null, bd = 1e18;
    for (const r of q) { const dd = Math.abs(endDate(r) - target); if (dd < bd) { bd = dd; best = r; } }
    return best && bd <= 50 * 86400000 ? price / best.price_at_period_end - 1 : null;
  };
  let ret6 = pick(182); const ret12 = pick(365);
  if (AN_STATE.mom && AN_STATE.mom.ret && anFin(AN_STATE.mom.ret.r6m)) ret6 = AN_STATE.mom.ret.r6m; // 일별 주가 기반 6개월 수익률이 있으면 우선 사용
  const react = anMean(q.slice(-4).map((r) => priceReturnOf(r)));
  // 스크리닝과 같은 기준: 최근 1개월(단기 반전 구간)을 뺀 6-1, 12-1 모멘텀으로 판정한다. 1개월 수익률이 없으면 기간 전체 수익률을 그대로 쓴다.
  const r1 = (AN_STATE.mom && AN_STATE.mom.ret && anFin(AN_STATE.mom.ret.r1m)) ? AN_STATE.mom.ret.r1m : null;
  const skip = (v) => (anFin(v) && r1 != null) ? (1 + v) / (1 + r1) - 1 : v;
  const ret6s = skip(ret6), ret12s = skip(ret12);
  let level = null, rel = null;
  const km = (AN_STATE.mom && AN_STATE.mom.kospi && anFin(AN_STATE.mom.kospi.r6m)) ? AN_STATE.mom.kospi.r6m : null;
  const a = ret6s, b = ret12s;
  if (anFin(a) || anFin(b)) {
    // 시계열 모멘텀(Moskowitz-Ooi-Pedersen 2012): 과거 수익률의 부호가 양이면 상승 추세. 강함 = 6M·12M 모두 양(+) 그리고 같은 기간 코스피를 이김(상대강도, Jegadeesh-Titman 1993)
    const pos = [a, b].filter(anFin).every((v) => v > 0), neg = [a, b].filter(anFin).every((v) => v < 0);
    rel = (km != null && anFin(ret6)) ? ret6 - km : null;
    level = (pos && rel != null && rel > 0) ? '강함' : pos ? '상승' : neg ? '약함' : '중립';
  }
  return { ret6, ret12, ret6s, ret12s, skipped: r1 != null, react, level, kospi6: km, rel };
}

// ---------- 밸류에이션 스냅샷 ----------
function anValSnapshot(rows, price, m, list) {
  const qrows = toQuarterlyRows(rows); const ttm = buildTTMRow(qrows);
  const annual = toAnnualRows(rows);
  const cur = ttm ? ttm.row : annual[annual.length - 1];
  const snap = latestSnapshotRow(rows);
  const out = { basis: ttm ? 'TTM' : '최근 연간', price, refetched: !!(snap && anRefetched(snap)) };
  out.ebit = cur ? cur.operating_income : null;
  out.da = (cur && anRefetched(cur) && (cur.depreciation != null || cur.amortization != null)) ? (cur.depreciation || 0) + (cur.amortization || 0) : null;
  out.ebitda = anFin(out.ebit) && anFin(out.da) ? out.ebit + out.da : null;
  out.fcf = cur ? cur.fcf : null;
  out.mcap = m.marketCap;
  const borrow = snap ? anBorrow(snap) : null, liquid = snap ? anLiquid(snap) : null;
  const minority = (snap && snap.total_equity != null && snap.parent_equity != null) ? snap.total_equity - snap.parent_equity : 0;
  out.netDebt = anFin(borrow) && anFin(liquid) ? borrow - liquid : null;
  // IFRS16: 리스료가 영업비용이 아니라 사용권자산 상각+이자로 처리되어 EBITDA에는 리스비용이 빠져 있으므로, EV에도 리스부채를 부채로 포함(유동 + 비유동; 비유동분은 재조회 후 반영)
  // 참고용: EV에서 차감하지 않은 금융자산(유동·비유동 합산, 비상장 지분 등 환금성이 낮은 것 포함 가능) — 어느 정도까지 현금으로 볼지는 판단이 필요해 자동 차감하지 않고 규모만 보여줌
  const fa = snap ? [snap.short_term_trading_securities, snap.fvpl_financial_assets, snap.fvoci_financial_assets].filter(anFin) : [];
  out.finAssets = fa.length ? fa.reduce(function (a, b) { return a + b; }, 0) : null;
  out.lease = snap ? (snap.current_lease_liabilities || 0) + (snap.lease_liabilities_nc || 0) : 0;
  out.leaseFull = !!(snap && snap.lease_liabilities_nc != null);
  out.ev = (anFin(out.mcap) && anFin(out.netDebt)) ? out.mcap + out.netDebt + out.lease + minority : null;
  out.evEbit = out.ebit > 0 ? anDiv(out.ev, out.ebit) : null;
  out.evEbitda = out.ebitda > 0 ? anDiv(out.ev, out.ebitda) : null;
  out.fcfYield = anDiv(out.fcf, out.mcap);
  out.per = m.per; out.pbr = m.pbr; out.earningsYield = m.earningsYield;
  // Normalized PER: 시가총액 ÷ 최근 최대 10개년 평균 순이익 (Graham-Dodd / Shiller CAPE 방식, 물가 미조정)
  const nis = list.slice(-10).map((d) => d.ni).filter(anFin);
  const avgNi = nis.length >= 3 ? anMean(nis) : null;
  out.normPer = (anFin(avgNi) && avgNi > 0) ? anDiv(out.mcap, avgNi) : null; out.normN = nis.length;
  const g3 = anCagr(list, 'eps', 3) != null ? anCagr(list, 'eps', 3) : anCagr(list, 'ni', 3);
  out.g3 = g3; out.peg = (anFin(out.per) && out.per > 0 && anFin(g3) && g3 > 0) ? out.per / (g3 * 100) : null;
  const rng = (key, cur0) => {
    const mk = (n) => { const v = list.slice(-n).map((d) => d[key]).filter((x) => anFin(x) && (key === 'fcfy' || x > 0)); if (v.length < 3) return null; const lo = Math.min(...v), hi = Math.max(...v); return { n: v.length, lo, med: anMedian(v), hi, pct: anFin(cur0) ? v.filter((x) => x <= cur0).length / v.length : null }; };
    return { y5: mk(5), y10: mk(10) };
  };
  out.rangePer = rng('per', out.per); out.rangePbr = rng('pbr', out.pbr); out.rangeFcfy = rng('fcfy', out.fcfYield);
  return out;
}

// ---------- 잔여이익모형(RIM) 시나리오 ----------
// V0 = B0 + Σ (ROE_t − r)·B_{t-1}/(1+r)^t + 종가치. Ohlson(1995), Edwards-Bell(1961), Penman 등 회계기반 가치평가의 표준 틀.
// ROE는 시나리오별 출발값에서 수렴값으로 선형 수렴(수익성의 평균회귀: Fama-French 2000), 종가치는 초과이익의 지속계수 ω(Dechow-Hutton-Sloan 1999, 약 0.62)로 감쇠.
function anRIM(B0, roe0, roeT, payout, r, N) {
  let B = B0, pv = 0, lastRI = 0;
  for (let t = 1; t <= N; t++) {
    const roe = roe0 + (roeT - roe0) * (t - 1) / (N - 1);
    const ni = roe * B; const ri = (roe - r) * B;
    pv += ri / Math.pow(1 + r, t); lastRI = ri;
    B = B + ni * (1 - payout);
  }
  pv += (lastRI * AN_TH.omega / (1 + r - AN_TH.omega)) / Math.pow(1 + r, N);
  return B0 + pv;
}
function anScenarioDefaults(m, list, r) {
  const hist = list.map((d) => d.roe).filter(anFin);
  if (hist.length < 3 || !anFin(m.bps) || m.bps <= 0) return null;
  const base0 = anMedian(hist.slice(-5)); const sd = anStd(hist.slice(-10));
  const bear0 = anClamp(base0 - sd, -0.1, 0.5), bull0 = anClamp(Math.max(base0 + sd, base0), -0.1, 0.5);
  const baseT = r + 0.5 * (base0 - r);
  return {
    base0, sd, n: hist.length,
    bear: { roe0: bear0, roeT: Math.min(bear0, r) },
    base: { roe0: base0, roeT: baseT },
    bull: { roe0: bull0, roeT: Math.max(base0, baseT) },
  };
}
function anScenarioCompute(m, def, r, payout, price) {
  const res = {};
  for (const k of ['bear', 'base', 'bull']) {
    const o = AN_STATE.ov;
    const roe0 = anFin(o[k + '0']) ? o[k + '0'] : def[k].roe0;
    const roeT = anFin(o[k + 'T']) ? o[k + 'T'] : def[k].roeT;
    const v = anRIM(m.bps, roe0, roeT, payout, r, 10);
    res[k] = { roe0, roeT, v, up: anFin(price) ? v / price - 1 : null, pbr: v / m.bps };
  }
  res.w = 0.25 * res.bear.v + 0.5 * res.base.v + 0.25 * res.bull.v;
  res.wUp = anFin(price) ? res.w / price - 1 : null;
  return res;
}

// ---------- 내재성장률(Reverse DCF) ----------
// (1) 이익 기준(가치동인 모형): P = E0(1+g)(1−g/ROE)/(r−g)  — Gordon + 재투자(g/ROE) 반영. g를 풀어서 "현재 주가가 요구하는 영구성장률".
function anImpliedGEarnings(price, eps0, roe, r) {
  if (!(price > 0) || !(eps0 > 0) || !(roe > 0)) return null;
  const f = (g) => eps0 * (1 + g) * (1 - g / roe) / (r - g) - price;
  let prev = -0.50, fp = f(prev);
  for (let g = -0.4995; g < r - 0.003; g += 0.0005) {
    const fg = f(g);
    if ((fp <= 0 && fg >= 0) || (fp >= 0 && fg <= 0)) {
      let lo = prev, hi = g;
      for (let i = 0; i < 60; i++) { const mid = (lo + hi) / 2; const fm = f(mid); if ((f(lo) <= 0) === (fm <= 0)) lo = mid; else hi = mid; }
      return (lo + hi) / 2;
    }
    prev = g; fp = fg;
  }
  return null;
}
// (2) 현금흐름 기준(Gordon): P = F0(1+g)/(r−g) → g = (P·r − F0)/(P + F0)
function anImpliedGFcf(price, f0, r) { return (price > 0 && f0 > 0) ? (price * r - f0) / (price + f0) : null; }

// ---------- 분류 / 판단 ----------
function anValuationLevel(vs, expG, impG, scen, price) {
  const sig = [];
  const pc = (rg) => { const x = rg && (rg.y10 || rg.y5); return x && anFin(x.pct) ? x.pct : null; };
  const pp = pc(vs.rangePer); if (pp != null) sig.push(pp <= 0.25 ? 1 : pp >= 0.75 ? -1 : 0);
  const pb = pc(vs.rangePbr); if (pb != null) sig.push(pb <= 0.25 ? 1 : pb >= 0.75 ? -1 : 0);
  if (anFin(expG) && anFin(impG)) sig.push(impG < expG - 0.02 ? 1 : impG > expG + 0.02 ? -1 : 0);
  if (scen && anFin(price)) sig.push(price < scen.bear.v ? 1 : price > scen.bull.v ? -1 : 0);
  if (sig.length < 2) return { level: null, n: sig.length };
  const a = anMean(sig);
  return { level: a >= 0.5 ? '매우 저평가' : a >= 0.2 ? '저평가' : a > -0.2 ? '적정' : a > -0.5 ? '높음' : '매우 높음', n: sig.length };
}
// 한 항목에는 한 번만 등급을 매기고(축 판정), 모든 라벨 조건은 그 같은 등급을 가져다 쓴다 → 같은 항목이 카드마다 다르게 표시되지 않는다.
const AN_RANK = {
  growth: ['낮음', '중간', '높음', '매우 높음'],
  roic: ['낮음', '중간', '높음'],
  quality: ['미흡', '보통', '양호'],
  val: ['매우 높음', '높음', '적정', '저평가', '매우 저평가'], // 오른쪽일수록 저렴
  mom: ['약함', '중립', '상승', '강함'],
};
// ---------- Piotroski(2000) F-Score: 9개 이진 신호 합계 ----------
function anPiotroski(list) {
  const d = list[list.length - 1]; if (!d) return null;
  const p = list.find((x) => x.year === d.year - 1); if (!p) return null;
  const roa = (x) => anDiv(x.ni, x.avgAssets);
  const lev = (x) => anDiv(x.borrow, x.assets);
  const sig = [];
  const add = (n, valTxt, rule, pass) => sig.push({ n, valTxt, rule, pass: pass == null ? null : !!pass });
  const r0 = roa(d), r1 = roa(p);
  add('1. ROA 흑자', 'ROA ' + anPct(r0), 'ROA 0 초과', anFin(r0) ? r0 > 0 : null);
  add('2. 영업현금흐름 흑자', '영업CF ' + anEok(d.ocf), '영업CF 0 초과', anFin(d.ocf) ? d.ocf > 0 : null);
  add('3. ROA 개선', 'ROA ' + anPct(r0) + ' vs 전년 ' + anPct(r1), '올해 ROA가 전년보다 큼', (anFin(r0) && anFin(r1)) ? r0 > r1 : null);
  add('4. 이익의 질(현금 기반)', '영업CF ' + anEok(d.ocf) + ' vs 순이익 ' + anEok(d.ni), '영업CF가 순이익보다 큼(발생액이 음수)', (anFin(d.ocf) && anFin(anFin(d.niCons) ? d.niCons : d.ni)) ? d.ocf > (anFin(d.niCons) ? d.niCons : d.ni) : null);
  const l0 = lev(d), l1 = lev(p);
  add('5. 레버리지 감소', '차입금/총자산 ' + anPct(l0) + ' vs 전년 ' + anPct(l1), '전년보다 늘지 않음', (anFin(l0) && anFin(l1)) ? l0 <= l1 : null);
  add('6. 유동비율 개선', '유동비율 ' + anX(d.cur) + ' vs 전년 ' + anX(p.cur), '올해가 전년보다 큼', (anFin(d.cur) && anFin(p.cur)) ? d.cur > p.cur : null);
  add('7. 신주 발행 없음', '주식수 변동 ' + anPct(d.shareChg), '증가율 ' + anPct(AN_TH.shareTol, 0) + ' 이하(기말 주식수 오차 허용)', anFin(d.shareChg) ? d.shareChg <= AN_TH.shareTol : null);
  add('8. 매출총이익률 개선', '매출총이익률 ' + anPct(d.gm) + ' vs 전년 ' + anPct(p.gm), '올해가 전년보다 큼', (anFin(d.gm) && anFin(p.gm)) ? d.gm > p.gm : null);
  add('9. 자산회전율 개선', '자산회전율 ' + anX(d.at) + ' vs 전년 ' + anX(p.at), '올해가 전년보다 큼', (anFin(d.at) && anFin(p.at)) ? d.at > p.at : null);
  const known = sig.filter((x) => x.pass != null).length, score = sig.filter((x) => x.pass === true).length;
  return { score, known, sig, year: d.year };
}
function anGrades(c) {
  const g = c.growth, r3 = c.roic3;
  const growth = g == null ? null : g >= AN_TH.growthVeryHigh ? '매우 높음' : g >= AN_TH.growthHigh ? '높음' : g >= AN_TH.growthLow ? '중간' : '낮음';
  const roic = r3 == null ? null : r3 >= c.r + AN_TH.roicSpread ? '높음' : r3 >= c.r ? '중간' : '낮음';
  const pio = c.pio;
  const qKnown = c.qPts.filter((x) => x != null), qGood = qKnown.filter(Boolean).length;
  const qLegacy = qKnown.length < 3 ? null : qGood >= 3 ? '양호' : qGood <= 1 ? '미흡' : '보통';
  const qFs = (!pio || pio.known < 8) ? null : pio.score >= pio.known - 1 ? '양호' : pio.score <= AN_TH.pioLow ? '미흡' : '보통';
  const quality = AN_CFG.qm === 'legacy4' ? qLegacy : qFs;
  const pctOf = (rg) => { const x = rg && (rg.y10 || rg.y5); return x && anFin(x.pct) ? x.pct : null; };
  const perPct = pctOf(c.vs.rangePer), pbrPct = pctOf(c.vs.rangePbr);
  const perCheap = anFin(c.vs.per) ? (c.vs.per > 0 && (c.vs.per <= AN_TH.perGraham || (perPct != null && perPct <= 0.25))) : null;
  const pbrCheap = anFin(c.vs.pbr) ? (c.vs.pbr > 0 && (c.vs.pbr <= AN_TH.pbrGraham || (pbrPct != null && pbrPct <= 0.25))) : null;
  return { growth, roic, quality, val: c.valLevel, mom: c.momLevel, perCheap, pbrCheap };
}
function anClassify(c) {
  const G = anGrades(c);
  c.grades = G;
  const rk = (axis, v) => v == null ? -1 : AN_RANK[axis].indexOf(v);
  // 조건 하나 = [라벨 텍스트, 충족 여부(true/false/null=미확인)]. 텍스트는 "현재 등급 / 필요 등급"을 항상 같은 형식으로 보여준다.
  const need = (axis, name, cur, curNote, needTxt, test) => [name + ': 현재 ' + (cur == null ? '미확인' : cur + (curNote ? '(' + curNote + ')' : '')) + ' / 필요 ' + needTxt, cur == null ? null : !!test(rk(axis, cur))];
  const gNote = anPct(c.growth), rNote = anPct(c.roic3);
  const legacyQ = AN_CFG.qm === 'legacy4';
  const qNote = legacyQ ? c.qPts.filter((x) => x === true).length + '/4 충족' : (c.pio ? c.pio.score + '/' + c.pio.known + '점' : null);
  const cond = {
    qualityOK: need('quality', 'Quality(F-Score)', G.quality, qNote, legacyQ ? '양호(4개 중 3개 이상)' : '양호(8~9점)', (i) => i >= 2),
    qualityLow: need('quality', 'Quality(F-Score)', G.quality, qNote, legacyQ ? '미흡(4개 중 1개 이하)' : '미흡(' + AN_TH.pioLow + '점 이하)', (i) => i === 0),
    growthHigh: need('growth', 'Growth', G.growth, gNote, '높음 이상', (i) => i >= 2),
    growthVery: need('growth', 'Growth', G.growth, gNote, '매우 높음', (i) => i >= 3),
    growthLow: need('growth', 'Growth', G.growth, gNote, '낮음', (i) => i === 0),
    roicHigh: need('roic', 'ROIC', G.roic, rNote, '높음', (i) => i >= 2),
    momUp: need('mom', 'Momentum', G.mom, null, '상승 이상', (i) => i >= 2),
    momWeakOrNeutral: need('mom', 'Momentum', G.mom, null, '중립 이하', (i) => i <= 1),
    momWeak: need('mom', 'Momentum', G.mom, null, '약함', (i) => i === 0),
    valNotRich: need('val', 'Valuation', G.val, null, '적정 이하(비싸지 않음)', (i) => i >= 2),
    valCheap: need('val', 'Valuation', G.val, null, '저평가 이상', (i) => i >= 3),
    valRich: need('val', 'Valuation', G.val, null, '높음 이상(비쌈)', (i) => i <= 1),
    epsVery: [ 'EPS(또는 순이익) 3년 CAGR: 현재 ' + (c.epsG == null ? '미확인' : anPct(c.epsG)) + ' / 필요 ' + anPct(AN_TH.growthVeryHigh, 0) + ' 이상', c.epsG == null ? null : c.epsG >= AN_TH.growthVeryHigh ],
    multiplesLow: [ 'PER·PBR 낮음: 현재 PER ' + anX(c.vs.per) + ' · PBR ' + anX(c.vs.pbr) + ' / 필요 둘 다 낮음(Graham 기준 PER≤' + AN_TH.perGraham + '·PBR≤' + AN_TH.pbrGraham + ' 또는 자기 과거 하위 25%)', (G.perCheap == null || G.pbrCheap == null) ? null : (G.perCheap && G.pbrCheap) ],
    roicDown: [ 'ROIC 하락: 3년 평균 ' + anPct(c.roic3) + ' vs 5년 평균 ' + anPct(c.roic5) + ' / 필요 3년<5년 또는 3년<자본비용 ' + anPct(c.r, 1), c.roicDown ],
    fcfDown: [ 'FCF 마진 하락: 3년 평균 ' + anPct(c.fcfm3) + ' vs 5년 평균 ' + anPct(c.fcfm5) + ' / 필요 3년<5년', c.fcfDown ],
  };
  const labels = [
    { name: '최우선선호주', desc: 'Quality↑ · Growth↑ · ROIC↑ · Momentum↑ · Valuation 적정', conds: [cond.qualityOK, cond.growthHigh, cond.roicHigh, cond.momUp, cond.valNotRich] },
    { name: '턴어라운드', desc: 'Quality 양호(F-Score 8~9) · Growth 낮음 · Valuation 저평가 · Momentum 아직 약함', conds: [cond.qualityOK, cond.growthLow, cond.valCheap, cond.momWeakOrNeutral] },
    { name: '성장주', desc: 'Growth↑↑ · EPS↑↑ · Momentum↑↑ · Valuation 높음', conds: [cond.growthVery, cond.epsVery, cond.momUp, cond.valRich] },
    { name: '밸류 Trap', desc: 'PER·PBR 낮음(Graham) · Quality 미흡(F-Score 낮음) · ROIC↓ · Growth↓ · Momentum↓', conds: [cond.multiplesLow, cond.qualityLow, cond.roicDown, cond.growthLow, cond.momWeak] },
  ];
  labels.forEach((L) => { L.pass = L.conds.filter((x) => x[1] === true).length; L.unk = L.conds.filter((x) => x[1] == null).length; L.total = L.conds.length; L.full = L.pass === L.total; });
  const matched = labels.filter((L) => L.full);
  let head;
  if (matched.length) head = { text: matched.map((L) => L.name).join(' / '), kind: 'match' };
  else {
    const best = labels.slice().sort((a, b) => (b.pass / b.total) - (a.pass / a.total))[0];
    head = (best && best.pass / best.total >= 0.6) ? { text: '가장 유사: ' + best.name + ' (' + best.pass + '/' + best.total + ' 충족)', kind: 'near' } : { text: '뚜렷한 유형 없음(혼합)', kind: 'none' };
  }
  let quad = null;
  if (G.growth && G.roic) {
    const hg = rk('growth', G.growth) >= 2, hr = G.roic === '높음';
    quad = hg && hr ? { n: '성장↑ · ROIC↑', t: '우량 성장 (성장이 가치를 만드는 구간 — 재투자할수록 유리)' }
      : (!hg && hr) ? { n: '성장↓ · ROIC↑', t: '현금창출형 (재투자처가 적음 — 배당·자사주 등 주주환원 정책이 중요)' }
      : (hg && !hr) ? { n: '성장↑ · ROIC↓', t: '성장하지만 자본효율 낮음 (ROIC가 자본비용보다 낮으면 성장이 오히려 가치를 깎을 수 있음)' }
      : { n: '성장↓ · ROIC↓', t: '구조적 부진 후보 (싸 보여도 가치함정 가능성 점검)' };
  }
  return { labels, head, quad, grades: G };
}

// ---------- 전체 컨텍스트 ----------
function anBuildContext(rows, price, m) {
  const list = anBuildSeries(rows);
  if (!list.length) return null;
  const r = AN_STATE.r;
  const last = list[list.length - 1];
  const vs = anValSnapshot(rows, price, m, list);
  const roe3 = anMean(anWin(list, 'roe', 3)), roe5 = anMean(anWin(list, 'roe', 5));
  const roic3 = anMean(anWin(list, 'roic', 3)), roic5 = anMean(anWin(list, 'roic', 5));
  const payoutAvg = anFin(AN_STATE.payout) ? AN_STATE.payout : (m.avgPayout != null ? m.avgPayout : 0);
  const reinv3 = anMean(anWin(list, 'reinv', 3));
  const gRoe = (roe3 != null) ? roe3 * (1 - anClamp(payoutAvg, 0, 1)) : null;               // 지속가능성장률(Higgins 1977) = ROE × 유보율
  const gRoic = (roic3 != null && reinv3 != null) ? roic3 * anClamp(reinv3, 0, 1.5) : null;  // 펀더멘털 성장률(Damodaran) = ROIC × 재투자율
  const def = anScenarioDefaults(m, list, r);
  const scen = def ? anScenarioCompute(m, def, r, anClamp(payoutAvg, 0, 0.95), price) : null;
  const roeBase = def ? def.base0 : roe5;
  const impE = anImpliedGEarnings(price, m.eps, roeBase, r);
  const f3 = anMean(anWin(list, 'fcf', 3)); const fps = anFin(f3) && anFin(last.shares) ? f3 / last.shares : null;
  const impF = (price && anFin(fps)) ? anImpliedGFcf(price, fps, r) : null;
  const expG = gRoe;
  const mom = anMomentum(rows, price);
  const pio = anPiotroski(list);
  const growthParts = [anCagr(list, 'revenue', 3), anCagr(list, 'ebit', 3), anCagr(list, 'eps', 3) != null ? anCagr(list, 'eps', 3) : anCagr(list, 'ni', 3)].filter(anFin);
  const growth = growthParts.length >= 2 ? anMean(growthParts) : (growthParts.length ? growthParts[0] : null);
  const epsG = anCagr(list, 'eps', 3) != null ? anCagr(list, 'eps', 3) : anCagr(list, 'ni', 3);
  const growthRev = anCagr(list, 'revenue', 3), growthEbit = anCagr(list, 'ebit', 3), growthEpsIsNi = anCagr(list, 'eps', 3) == null;
  const q = [roic3 != null ? roic3 >= AN_TH.roicGood : null, roe3 != null ? roe3 >= AN_TH.roeGood : null, anMean(anWin(list, 'fcfni', 3)) != null ? anMean(anWin(list, 'fcfni', 3)) >= AN_TH.fcfNiGood : null, anMean(anWin(list, 'accrual', 3)) != null ? anMean(anWin(list, 'accrual', 3)) <= AN_TH.accrualMax : null];
  const qKnown = q.filter((x) => x != null);
  const qualityOk = qKnown.length >= 3 ? qKnown.filter(Boolean).length >= 3 : null;
  const fm3 = anMean(anWin(list, 'fcfm', 3)), fm5 = anMean(anWin(list, 'fcfm', 5));
  const valL = anValuationLevel(vs, expG, impE, scen, price);
  const ctx = {
    list, pio, last, vs, r, roe3, roe5, roic3, roic5, payoutAvg, reinv3, gRoe, gRoic, def, scen, impE, impF, expG, mom, growth, epsG, growthRev, growthEbit, growthEpsIsNi, growthN: growthParts.length,
    qualityOk, qPts: q, valLevel: valL.level, valN: valL.n, momLevel: mom ? mom.level : null, base0: roeBase,
    perLow: anFin(vs.per) ? vs.per <= 10 : null, pbrLow: anFin(vs.pbr) ? vs.pbr <= 1 : null,
    roicDown: (roic3 != null && roic5 != null) ? (roic3 < roic5 || roic3 < r) : null,
    fcfDown: (fm3 != null && fm5 != null) ? fm3 < fm5 : null, fcfm3: fm3, fcfm5: fm5,
    qg: anQuarterGrowth(rows), price, m,
  };
  ctx.cls = anClassify(ctx);
  return ctx;
}

// ---------- 렌더링 부품 ----------
function anTable(list, defs, nYears) {
  const cols = list.slice(-nYears);
  const head = '<tr><th class="an-first">항목</th>' + cols.map((d) => '<th>' + d.year + '</th>').join('') + '</tr>';
  const body = defs.map((df) => '<tr><td class="an-first">' + df.label + '</td>' + cols.map((d) => '<td>' + anFmt(df.fmt, d[df.key]) + '</td>').join('') + '</tr>').join('');
  return '<div class="tscroll" style="max-height:60vh;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
}
function anTile(label, value, sub) {
  return '<div class="stat-tile"><div class="stat-label">' + label + '</div><div class="stat-value" style="font-size:15px;">' + value + '</div>' + (sub ? '<div style="font-size:10.5px; color:var(--text-muted); margin-top:2px; line-height:1.3;">' + sub + '</div>' : '') + '</div>';
}
function anGrid(tiles) { return '<div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:8px; margin:8px 0;">' + tiles.join('') + '</div>'; }
function anSection(title, color, desc, inner, open) {
  return '<details class="card" style="border-left:4px solid ' + color + ';"' + (open ? ' open' : '') + '><summary style="cursor:pointer; font-weight:700; color:' + color + '; font-size:14px;">' + title + '</summary>' +
    (desc ? '<div style="font-size:11.5px; color:var(--text-muted); margin:6px 0; line-height:1.45;">' + desc + '</div>' : '') + inner + '</details>';
}
const anMark = (v) => v === true ? '<span style="color:#16a34a; font-weight:700;">✓</span>' : v === false ? '<span style="color:#dc2626; font-weight:700;">✗</span>' : '<span style="color:#94a3b8;">?</span>';

function anRoeStats(c) {
  const l = c.list;
  const stat = (n) => { const v = anWin(l, 'roe', n).filter(anFin); return { avg: anMean(v), p: anPeriod(l, 'roe', n) }; };
  const s10 = stat(10), s5 = stat(5), s3 = stat(3);
  const all = l.slice(-10).filter((d) => anFin(d.roe));
  const mn = all.length ? all.reduce((a, b) => b.roe < a.roe ? b : a) : null, mx = all.length ? all.reduce((a, b) => b.roe > a.roe ? b : a) : null;
  let ttmRoe = null;
  try { const qr = toQuarterlyRows(rawRows); const t = buildTTMRow(qr); if (t) { const x = computeROEAvg(t.row, t.prior); ttmRoe = x ? x.value : null; } } catch (e) { /* TTM 자료 부족 */ }
  return anGrid([
    anTile('ROE 10Y 평균', anPct(s10.avg), anPeriodTxt(s10.p)), anTile('ROE 5Y 평균', anPct(s5.avg), anPeriodTxt(s5.p)), anTile('ROE 3Y 평균', anPct(s3.avg), anPeriodTxt(s3.p)),
    anTile('ROE TTM', anPct(ttmRoe), '최근 4개 분기 합산 순이익 ÷ 평균 자본'), anTile('ROE 표준편차(10Y)', anPct(anStd(all.map((d) => d.roe))), '클수록 수익성이 불안정'),
    anTile('ROE 최저', mn ? anPct(mn.roe) : 'N/A', mn ? mn.year + '년' : ''), anTile('ROE 최고', mx ? anPct(mx.roe) : 'N/A', mx ? mx.year + '년' : ''),
  ]);
}

const AN_BADGE = { proof: ['학술 검증', '#16a34a'], theory: ['이론 근거', '#2563eb'], practice: ['실무 관행', '#d97706'], assume: ['가정·휴리스틱', '#64748b'] };
function anBadge(k, label) { const b = AN_BADGE[k] || AN_BADGE.assume; return '<span style="display:inline-block; font-size:10px; font-weight:700; color:#fff; background:' + b[1] + '; border-radius:8px; padding:0 6px; line-height:16px; vertical-align:middle;">' + (label || b[0]) + '</span>'; }
function anItem(it) {
  const row = (k, v) => v ? '<div style="margin:4px 0;"><b>' + k + '</b> ' + v + '</div>' : '';
  return '<details style="border-top:1px solid var(--border); padding:6px 0;"><summary style="cursor:pointer; font-weight:600; font-size:13px;">' + it.name + ' ' + anBadge(it.badge, it.bl) + (it.now ? ' <span style="font-weight:400; color:var(--text-muted);">· 현재 ' + it.now + '</span>' : '') + '</summary><div style="font-size:12px; line-height:1.55; padding:4px 0 4px 10px;">' +
    row('개념', it.concept) + row('계산식', it.formula) + row('실제 입력값', it.inputs) + row('판정 기준', it.rule) + row('근거', it.evidence) + row('한계', it.limit) + '</div></details>';
}
function anRenderExplain(c) {
  const lv = (x) => x == null ? 'N/A' : x;
  const G = c.grades || c.cls.grades;
  const items = [];
  items.push({ name: 'Growth (성장)', badge: 'assume', now: lv(G.growth) + ' ' + anPct(c.growth),
    concept: '최근 3년 동안 매년 평균 몇 %씩 늘었는지(복리). 매출·영업이익·EPS 세 가지를 같은 비중으로 평균합니다.',
    formula: 'CAGR = (끝값 ÷ 시작값)^(1/3) − 1 , Growth = 평균(매출 CAGR, 영업이익 CAGR, EPS CAGR). EPS가 없으면 순이익.',
    inputs: '매출 ' + anPct(c.growthRev) + ' · 영업이익 ' + anPct(c.growthEbit) + ' · ' + (c.growthEpsIsNi ? '순이익' : 'EPS') + ' ' + anPct(c.epsG) + ' → 계산 가능한 ' + c.growthN + '개 평균 = ' + anPct(c.growth),
    rule: '낮음 ' + anPct(AN_TH.growthLow, 0) + ' 미만 · 중간 · 높음 ' + anPct(AN_TH.growthHigh, 0) + ' 이상 · 매우 높음 ' + anPct(AN_TH.growthVeryHigh, 0) + ' 이상',
    evidence: 'CAGR 계산식은 수학적 정의라 정확합니다. 그러나 5·10·15% 기준선은 학술 근거가 없는 임의 기준입니다. Chan·Karceski·Lakonishok(2003)은 과거 성장률이 미래 성장으로 잘 이어지지 않는다고 보고했습니다.',
    limit: '시작·끝 값이 0 이하면 계산 불가라 제외됩니다. 양 끝 두 해에 민감해 일회성 이익에 흔들립니다. 높은 Growth가 미래 성장을 보장하지 않습니다.' });
  items.push({ name: 'ROIC (투하자본이익률)', badge: 'theory', now: lv(G.roic) + ' ' + anPct(c.roic3),
    concept: '영업으로 번 세후이익이 사업에 묶인 자본 대비 몇 %인지. 자본비용(r)보다 높아야 성장할수록 기업가치가 늘어납니다.',
    formula: 'ROIC = 영업이익 × (1 − 세율 ' + anPct(AN_TH.taxRate, 0) + ') ÷ 평균 투하자본.  투하자본 = (매출채권+기타채권+재고 − 매입채무−기타채무) + (유형자산+무형자산+사용권자산+영업권)',
    inputs: '3년 평균 ' + anPct(c.roic3) + ' · 5년 평균 ' + anPct(c.roic5) + ' · 자본비용 r ' + anPct(c.r) + ' (화면 위쪽에서 바꿀 수 있음)',
    rule: '낮음 = ROIC가 r 미만(가치 파괴) · 중간 = r 이상 · 높음 = r + ' + anPct(AN_TH.roicSpread, 0) + 'p 이상',
    evidence: '「ROIC가 자본비용을 넘어야 성장이 가치를 만든다」는 Modigliani-Miller(1961) 가치평가 이론과 EVA(Stern Stewart)의 핵심입니다. 이론은 증명된 것이나 r=9%는 가정이고, 3%p 여유폭은 추정 오차를 감안한 임의 값입니다.',
    limit: '세율은 유효세율(1−순이익/세전이익, 0~40% 밖이면 24%), 투하자본에 리스·장기금융자산 일부 미반영, 3년 평균이라 최근 급변을 늦게 반영합니다.' });
  const pio = c.pio;
  const pioInputs = pio ? '<table class="an-tbl" style="margin-top:4px;"><thead><tr><th class="an-first">신호</th><th>값</th><th>기준</th><th>충족</th></tr></thead><tbody>' + pio.sig.map((x) => '<tr><td class="an-first">' + x.n + '</td><td>' + x.valTxt + '</td><td>' + x.rule + '</td><td>' + anMark(x.pass) + '</td></tr>').join('') + '</tbody></table>합계 ' + pio.score + '점 / 판정 가능 ' + pio.known + '개 (' + pio.year + '년 기준)' : '전년도 자료가 없어 계산 불가';
  items.push({ name: 'Quality (Piotroski F-Score)', badge: 'proof', now: lv(G.quality) + (pio ? ' ' + pio.score + '/' + pio.known + '점' : ''),
    concept: '재무제표 9가지 질문에 예/아니오로 답해 점수(0~9)를 매기는 재무 건전성 척도. 수익성 4개 · 재무구조 3개 · 효율성 2개로 구성됩니다.',
    formula: 'F = 9개 신호(각 1점)의 합. ROA는 순이익 ÷ 평균총자산, 레버리지는 차입금 ÷ 총자산(논문은 장기부채).',
    inputs: pioInputs,
    rule: '양호 = 8~9점(판정 불가 1개 있으면 전체 −1점 이상) · 미흡 = ' + AN_TH.pioLow + '점 이하 · 그 외 보통. 판정 가능한 신호가 8개 미만이면 N/A.',
    evidence: 'Piotroski(2000, Journal of Accounting Research): 미국 1976~96년 장부가치/시가가 높은(싼) 종목에서 F-Score 높은 종목이 낮은 종목보다 연 약 23%p 높은 수익을 냈고, 이는 가장 잘 알려진 재무제표 기반 검증 사례입니다. 단, 이 앱은 한국 시장에서의 재검증 결과를 확인하지 못했습니다.',
    limit: '논문의 극단 구간은 8~9점/0~1점인데 이 앱은 미흡을 ' + AN_TH.pioLow + '점 이하로 넓혔습니다(실무 완화). 논문은 싼 종목 안에서 검증했고 모든 종목에 적용한 근거는 아닙니다. 주식수 변동 1% 이내는 신주 발행 아님으로 처리합니다.' });
  const vs = c.vs;
  items.push({ name: 'Valuation (밸류에이션)', badge: 'practice', bl: '신호는 이론·종합은 휴리스틱', now: lv(G.val) + (c.valN ? ' (신호 ' + c.valN + '개)' : ''),
    concept: '지금 주가가 싼지 비싼지를 서로 다른 방법 4가지로 보고 평균 낸 등급입니다.',
    formula: '신호 4개(각 +1 싸다 / 0 / −1 비싸다): ① PER의 자기 과거 5~10년 분위(25% 이하 싸다, 75% 이상 비싸다) ② PBR 과거 분위 ③ 현재 주가가 내재하는 성장률 vs 기대성장률(차이 ±2%p) ④ 현재가 vs 잔여이익모형(RIM) 약세·강세 시나리오 값. 평균 ≥0.5 매우 저평가 · ≥0.2 저평가 · −0.2 초과 적정 · −0.5 초과 높음 · 그 이하 매우 높음.',
    inputs: 'PER ' + anX(vs.per) + ' · PBR ' + anX(vs.pbr) + ' · 내재성장률 ' + anPct(c.impE) + ' vs 기대성장률 ' + anPct(c.expG) + (c.scen ? ' · RIM Base ' + anWon(c.scen.base.v) + ' / 현재가 ' + anWon(c.price) : ''),
    rule: '위 평균값으로 5단계. 신호가 2개 미만이면 N/A.',
    evidence: 'Graham 기준(방어적 투자자: PER 15 이하, PBR 1.5 이하, PER×PBR 22.5 이하 — Graham 1949/1973)은 오랜 실무 고전입니다. RIM은 Ohlson(1995)·Edwards-Bell(1961)의 회계 기반 가치평가 이론입니다. 하지만 4개 신호를 같은 비중으로 평균하고 25/75% 분위·±2%p를 쓰는 것은 임의 선택입니다.',
    limit: '과거 분위는 자기 역사에 비교한 값이라 구조적으로 달라진 회사엔 부적절합니다. r(자본비용)과 시나리오 ROE가 가정이라 결과가 크게 달라집니다.' });
  items.push({ name: 'Momentum (모멘텀)', badge: 'proof', bl: '해외 검증·한국 약함', now: lv(G.mom),
    concept: '최근 주가가 오르는 추세인지 보는 지표. 오른 종목이 한동안 더 오르는 경향(관성)을 이용합니다.',
    formula: '6개월·12개월 수익률에서 최근 1개월을 뺀 값(6-1, 12-1)의 부호와 6개월 코스피 대비 초과 여부.',
    inputs: '6개월 ' + anPct(c.mom && c.mom.ret6) + ' · 12개월 ' + anPct(c.mom && c.mom.ret12) + ' · 최근 1개월 제외 후 6개월 ' + anPct(c.mom && c.mom.ret6s) + ' · 코스피 6개월 ' + anPct(c.mom && c.mom.kospi6) + (c.mom && c.mom.rel != null ? ' · 초과수익 ' + anPct(c.mom.rel) : ''),
    rule: '약함 = 6·12개월 모두 음수 · 중립 = 섞임 · 상승 = 모두 양수 · 강함 = 상승이면서 6개월 수익률이 코스피보다 높음',
    evidence: 'Jegadeesh-Titman(1993): 3~12개월 승자 매수·패자 매도 전략이 미국에서 월 약 1% 초과수익. Jegadeesh(1990)·Lehmann(1990): 1개월은 단기 반전이라 제외. Moskowitz-Ooi-Pedersen(2012): 자기 과거 수익률 부호만으로도 여러 자산에서 유효. 단, 한국 시장에서는 증거가 약하거나 불안정하다는 보고가 있어 참고 신호로만 쓰세요.',
    limit: '재무 탭의 주가는 분기말 가격이라 대략적이며, 일별 조회가 로드되면 일별 값으로 대체됩니다. 이 앱의 일별 이력은 약 126거래일이라 12-1 모멘텀을 정식으로 계산하지 못합니다.' });
  c.cls.labels.forEach((L) => {
    const note = L.name === '턴어라운드' ? 'Piotroski(2000)가 싼 종목 중 F-Score 높은 종목이 초과수익을 냈다고 보고한 방향과 일치하는 조합입니다. 단 Growth 낮음·Momentum 약함 조건은 임의 추가입니다.'
      : L.name === '밸류 Trap' ? '싼 종목 중 F-Score 낮은 종목이 부진했다는 Piotroski(2000)의 결과를 반영했습니다. 나머지 조건은 임의 조합입니다.'
      : '이 조합 자체를 검증한 논문은 이 앱에서 확인하지 못했습니다. 각 축의 근거 수준을 따릅니다.';
    items.push({ name: '라벨: ' + L.name, badge: (L.name === '턴어라운드' || L.name === '밸류 Trap') ? 'practice' : 'assume', bl: (L.name === '턴어라운드' || L.name === '밸류 Trap') ? '일부 학술 근거' : null, now: L.pass + '/' + L.total + ' 충족',
      concept: L.desc, formula: '모든 조건을 충족해야 확정(전부 AND). 60% 이상이면 「가장 유사」로 표시.',
      inputs: L.conds.map((x) => anMark(x[1]) + ' ' + anEsc(x[0])).join('<br>'), rule: null, evidence: note, limit: '라벨은 해석을 돕는 요약이며 매수·매도 신호가 아닙니다.' });
  });
  return anSection('📖 지표·계산식 설명서 (개념 · 계산식 · 실제 입력값 · 근거)', '#7c3aed',
    '항목을 눌러 펼치면 개념, 계산식, 이 종목에 실제로 들어간 값, 판정 기준, 학술 근거와 한계를 볼 수 있습니다. 논문 인용은 이 앱에서 온라인으로 재확인하지 못했으니 중요한 판단 전에는 원문을 확인하세요.',
    items.map(anItem).join(''), false) + anRenderGlossary(c);
}
function anRenderGlossary(c) {
  const vs = c.vs, l = c.last;
  const rows = [
    ['ROE', anPct(c.roe3) + ' (3Y)', '주주 자본으로 번 순이익 비율', '순이익 ÷ 평균 자본', '이론'],
    ['ROIC', anPct(c.roic3) + ' (3Y)', '사업에 투입된 자본의 수익률', '영업이익×(1−세율) ÷ 평균 투하자본', '이론'],
    ['FCF (잉여현금흐름)', anEok(l.fcf), '투자 후 실제로 남는 현금', '영업현금흐름 − CAPEX', '정의'],
    ['FCF/순이익', anNum(l.fcfni), '이익이 현금으로 뒷받침되는 정도', 'FCF ÷ 순이익 (순이익 양수일 때)', '가정(0.7 기준)'],
    ['발생액(Accrual)', anPct(l.accrual), '이익 중 현금이 아닌 회계상 부분의 비중', '(순이익 − 영업CF) ÷ 평균총자산', '학술(Sloan 1996: 발생액 높은 종목 부진)'],
    ['PER', anX(vs.per), '이익 1원당 주가', '주가 ÷ EPS', '정의'],
    ['PBR', anX(vs.pbr), '장부가치 1원당 주가', '주가 ÷ BPS', '정의'],
    ['PER × PBR', (anFin(vs.per) && anFin(vs.pbr)) ? anNum(vs.per * vs.pbr, 1) + ' (기준 ' + AN_TH.grahamProd + ' 이하)' : 'N/A', 'Graham의 복합 저평가 기준', 'PER × PBR ≤ 22.5', '실무 고전(Graham)'],
    ['Normalized PER', anX(vs.normPer), '경기 사이클을 평탄화한 PER', '시가총액 ÷ 최근 최대 10년 평균 순이익', '학술(Graham-Dodd·Shiller CAPE)'],
    ['PEG', anNum(vs.peg), '성장 대비 PER', 'PER ÷ (3년 EPS CAGR×100)', '실무 관행'],
    ['오너어닝스', '가치평가 방법 비교 참조', '주주가 실제로 가져갈 수 있는 이익', '영업CF − 유지보수 CAPEX (3년 평균)', 'Buffett 1986·Greenwald 2001'],
    ['린치 비율', '가치평가 방법 비교 참조', '성장+배당 대비 PER', '(EPS 성장률% + 배당수익률%) ÷ PER', '실무(Lynch 1989)'],
    ['EV/EBITDA', anX(vs.evEbitda), '기업 전체 값 대비 현금창출력', '(시총+순차입금+리스부채+비지배지분) ÷ EBITDA', '정의'],
    ['FCF Yield', anPct(vs.fcfYield), '시가총액 대비 FCF 수익률', 'FCF ÷ 시가총액', '정의'],
    ['지속가능성장률', anPct(c.gRoe), '외부 자금 없이 낼 수 있는 성장 한도', 'ROE × (1 − 배당성향)', '학술(Higgins 1977)'],
    ['내재성장률', anPct(c.impE), '현재 주가가 요구하는 영구 성장률', 'P = E0(1+g)(1−g/ROE)/(r−g) 를 g에 대해 풂', '이론(역 DCF)'],
    ['TTM', '최근 4개 분기 합', '최근 1년 실적(분기 누적 보정)', '손익은 4개 분기 합, 현금흐름은 누적값을 분기로 환산', '정의'],
  ];
  const body = rows.map((r) => '<tr><td class="an-first">' + r[0] + '</td><td>' + r[1] + '</td><td style="text-align:left;">' + r[2] + '</td><td style="text-align:left;">' + r[3] + '</td><td style="text-align:left;">' + r[4] + '</td></tr>').join('');
  return anSection('📚 용어 사전 (현재 값 포함)', '#475569', '화면에 나오는 핵심 용어의 뜻과 계산식, 이 종목의 현재 값입니다.',
    '<div class="tscroll" style="max-height:60vh;"><table class="an-tbl"><thead><tr><th class="an-first">용어</th><th>현재 값</th><th>뜻</th><th>계산식</th><th>근거 수준</th></tr></thead><tbody>' + body + '</tbody></table></div>', false);
}

function anRenderClassify(c) {
  const h = c.cls.head; const col = h.kind === 'match' ? '#16a34a' : h.kind === 'near' ? '#d97706' : '#64748b';
  const lv = (x) => x == null ? 'N/A' : x;
  const axes = anGrid([
    anTile('Growth', lv(c.grades.growth), anPct(c.growth) + ' = 3년 CAGR 단순평균 [매출 ' + anPct(c.growthRev) + ' · 영업이익 ' + anPct(c.growthEbit) + ' · ' + (c.growthEpsIsNi ? '순이익(EPS 없음) ' : 'EPS ') + anPct(c.epsG) + '], 계산 가능한 ' + c.growthN + '개만 평균(시작·끝 값이 0 이하면 N/A로 제외). 낮음<' + anPct(AN_TH.growthLow, 0) + ' · 높음≥' + anPct(AN_TH.growthHigh, 0) + ' · 매우 높음≥' + anPct(AN_TH.growthVeryHigh, 0) + ' ' + anBadge('assume')), anTile('ROIC', lv(c.grades.roic), '3Y 평균 ' + anPct(c.roic3) + ' (5Y ' + anPct(c.roic5) + ') vs 자본비용 r ' + anPct(c.r) + '. 낮음=r 미만(가치 파괴) · 중간=r 이상 · 높음=r+' + anPct(AN_TH.roicSpread, 0) + 'p 이상 ' + anBadge('theory')),
    anTile('Quality', lv(c.grades.quality), AN_CFG.qm === 'legacy4' ? ('기존 4점 방식: ROIC, ROE, FCF/순이익, 발생액 4개 중 ' + c.qPts.filter((x) => x === true).length + '개 충족(3개 이상 양호, 1개 이하 미흡) ' + anBadge('assume')) : c.pio ? ('Piotroski F-Score ' + c.pio.score + '/' + c.pio.known + '점(' + c.pio.year + '년). 양호 8~9 · 미흡 0~' + AN_TH.pioLow + ' · 그 외 보통 ' + anBadge('proof')) : '전년도 자료가 없어 계산 불가'),
    anTile('Valuation', lv(c.grades.val), (c.valN ? ('신호 ' + c.valN + '개 종합: PER·PBR 과거분위, 내재성장 vs 기대성장, 시나리오 대비 현재가 ') : '현재 주가 입력 필요 ') + anBadge('practice', '신호는 이론·종합은 휴리스틱')),
    anTile('Momentum', lv(c.grades.mom), c.mom ? ('6개월 ' + anPct(c.mom.ret6) + ' / 12개월 ' + anPct(c.mom.ret12) + (c.mom.kospi6 != null ? ' / 코스피 6M ' + anPct(c.mom.kospi6) : ' / 코스피 비교는 아래 일별 조회 후 반영') + '. 약함=둘 다 음 · 상승=둘 다 양 · 강함=상승+코스피 초과 ') + anBadge('proof', '해외 검증·한국 약함') : '현재 주가 입력 필요'),
  ]);
  const cards = c.cls.labels.map((L) => '<div style="border:1px solid var(--border); border-radius:8px; padding:8px 10px; margin-top:8px;"><div style="display:flex; justify-content:space-between; gap:8px;"><b>' + L.name + '</b><span style="font-size:12px; color:' + (L.full ? '#16a34a' : 'var(--text-muted)') + ';">' + L.pass + '/' + L.total + ' 충족' + (L.unk ? ' (미확인 ' + L.unk + ')' : '') + '</span></div><div style="font-size:11px; color:var(--text-muted); margin:2px 0 4px;">' + L.desc + '</div>' +
    L.conds.map((x) => '<div style="font-size:12px;">' + anMark(x[1]) + ' ' + anEsc(x[0]) + '</div>').join('') + '</div>').join('');
  const q = c.cls.quad ? '<div style="margin-top:8px; padding:8px 10px; background:#f1f5f9; border-radius:8px; font-size:12.5px;"><b>성장 × ROIC 4분면: ' + c.cls.quad.n + '</b> — ' + c.cls.quad.t + '</div>' : '';
  return anSection('🏷 종합 분류 (결과 시계열로 본 유형)', col,
    '각 항목 옆 배지는 근거 수준입니다 — 학술 검증(논문으로 성과가 확인된 방법) · 이론 근거(이론상 맞지만 입력값은 가정) · 실무 관행(널리 쓰이나 증명 안 됨) · 가정·휴리스틱(임의 기준선). 계산식·실제 입력값·한계는 바로 아래 「지표·계산식 설명서」에서 항목별로 펼쳐 볼 수 있습니다.',
    '<div style="font-size:16px; font-weight:800; color:' + col + '; margin:6px 0;">' + h.text + '</div>' + axes + q + cards, true);
}

function anRenderJudge(c) {
  const L = c.list, last = c.last, vs = c.vs, r = c.r, scen = c.scen;
  const items = [];
  // 1. PER
  {
    let t = '현재 PER ' + anX(vs.per);
    if (vs.rangePer && vs.rangePer.y5) t += ' · 최근 5년 공시시점 PER 중앙값 ' + anX(vs.rangePer.y5.med) + '(현재는 과거 분포의 ' + anPct(vs.rangePer.y5.pct, 0) + ' 분위, 0%=최저·100%=최고)';
    if (scen && anFin(c.m.eps) && c.m.eps > 0) t += ' · Base 시나리오 정당 PER ' + anX(scen.base.v / c.m.eps);
    t += '. PER은 "ROE(수익성)·성장·요구수익률"의 함수입니다 — ROE가 높고 오래 지속될수록, 요구수익률이 낮을수록 정당 PER이 높아집니다(잔여이익/고든 모형).';
    if (scen && anFin(c.m.eps) && c.m.eps > 0 && anFin(vs.per)) { const j = scen.base.v / c.m.eps; t += vs.per > j * 1.2 ? ' → 현재 PER이 정당 PER보다 높아, 시장이 Base보다 더 좋은 시나리오를 가격에 넣고 있습니다.' : vs.per < j * 0.8 ? ' → 현재 PER이 정당 PER보다 낮아, 시장이 Base보다 보수적으로 보고 있습니다.' : ' → 현재 PER은 Base 시나리오와 비슷한 수준입니다.'; }
    items.push(['PER이 왜 높거나 낮은가', t]);
  }
  // 2. ROE 분해
  {
    let t = 'N/A (5단계 분해에 필요한 자료 부족)';
    try {
      const steps = compute5StepRows(toAnnualRows(rawRows)); const s = steps[steps.length - 1], prev = steps.slice(0, -1).slice(-5);
      const keys = [['taxBurden', '세율부담'], ['interestBurden', '이자부담'], ['ebitMargin', 'EBIT마진'], ['assetTurnover', '자산회전율'], ['leverage', '레버리지']];
      const contrib = keys.map(([k, n]) => { const base = anMean(prev.map((x) => x[k]).filter((v) => anFin(v) && v > 0)); return (anFin(s[k]) && s[k] > 0 && anFin(base) && base > 0) ? { n, k, cur: s[k], base, lg: Math.log(s[k] / base) } : null; }).filter(Boolean);
      if (s && contrib.length >= 3) {
        contrib.sort((a, b) => Math.abs(b.lg) - Math.abs(a.lg));
        const top = contrib[0];
        t = '최근 ROE ' + anPct(c.last.roe) + ' = 세율부담 × 이자부담 × EBIT마진 × 자산회전율 × 레버리지(듀폰). 과거 평균 대비 가장 크게 달라진 요인은 <b>' + top.n + '</b>(' + anNum(top.base, 3) + ' → ' + anNum(top.cur, 3) + ', ' + (top.lg > 0 ? '상승' : '하락') + ')입니다. 레버리지 비중이 크면 ROE가 높아도 부채 덕일 수 있으니 ROIC·Gross Profitability와 함께 보세요.';
      }
    } catch (e) { /* 분해 실패 시 N/A 유지 */ }
    items.push(['ROE가 왜 높거나 낮은가', t]);
  }
  // 3. 지속가능성
  {
    const roes = L.slice(-10).map((d) => d.roe).filter(anFin);
    if (roes.length >= 3) {
      const hit = roes.filter((v) => v >= r).length / roes.length; const m0 = anMean(roes), sd = anStd(roes);
      const cv = (m0 > 0 && sd != null) ? sd / m0 : null;
      const lvl = hit >= 0.8 && cv != null && cv < 0.4 ? '높음' : hit >= 0.5 ? '보통' : '낮음';
      items.push(['ROE를 유지할 수 있는가', '최근 ' + roes.length + '개년 중 ROE가 요구수익률(' + anPct(r, 1) + ') 이상이었던 해 ' + Math.round(hit * roes.length) + '개(' + anPct(hit, 0) + '), 변동계수 ' + anNum(cv) + ' → 지속 가능성 <b>' + lvl + '</b>. 단, 높은 ROE도 시간이 지나면 평균으로 돌아가는 경향이 실증돼 있어(Fama-French 2000) Base 시나리오는 ROE가 10년에 걸쳐 요구수익률 쪽으로 절반 수렴한다고 가정합니다.']);
    } else items.push(['ROE를 유지할 수 있는가', 'ROE 자료 3개년 미만 — 판단 불가']);
  }
  // 4. 추가 1원
  {
    const inc = L.slice().reverse().find((d) => anFin(d.incRoic)), incE = L.slice().reverse().find((d) => anFin(d.incRoe));
    let t = '증분 ROIC(' + (inc ? inc.year + ' 기준 3년' : 'N/A') + '): ' + (inc ? anPct(inc.incRoic) : 'N/A') + ' / 증분 ROE: ' + (incE ? anPct(incE.incRoe) : 'N/A') + ' / 재투자율(최근 3Y 평균): ' + anPct(c.reinv3) + '. ';
    if (inc) t += inc.incRoic >= r ? '추가로 넣은 1원이 요구수익률 이상을 벌고 있어 재투자가 가치를 만들고 있습니다.' : '추가 투자의 수익이 요구수익률에 못 미쳐 성장이 가치를 만들지 못하고 있을 수 있습니다.';
    else t += '투하자본이 늘지 않았거나(정체·감소) 자료가 부족해 증분 수익률을 계산할 수 없습니다(이 경우 이익 증가는 신규 투자가 아니라 기존 자산의 효율 개선).';
    t += ' ※ "몇 년간 재투자가 가능한가"는 데이터로 측정할 수 없어(경쟁우위 지속기간은 가정의 영역) 시나리오의 ROE 수렴 속도로만 반영했습니다.';
    items.push(['추가로 1원을 투자하면 얼마를 버는가', t]);
  }
  // 5. 가격에 반영된 기대
  {
    let t = '현재 주가가 요구하는 내재성장률(이익 기준) ' + anPct(c.impE) + ' / (현금흐름 기준) ' + anPct(c.impF) + ' vs 펀더멘털 기대성장률(ROE×유보율) ' + anPct(c.gRoe) + ', (ROIC×재투자율) ' + anPct(c.gRoic) + ', 최근 5Y 순이익 CAGR ' + anPct(anCagr(L, 'ni', 5)) + '. ';
    if (anFin(c.impE) && anFin(c.expG)) t += c.impE > c.expG + 0.02 ? '시장은 기업이 낼 수 있는 성장보다 더 높은 성장을 이미 가격에 반영 중입니다.' : c.impE < c.expG - 0.02 ? '시장이 기대하는 성장이 펀더멘털이 뒷받침하는 성장보다 낮습니다(기대가 덜 반영됨).' : '시장 기대와 펀더멘털 성장이 비슷합니다.';
    items.push(['미래 기대가 주가에 얼마나 반영되었는가', t]);
  }
  // 6. 시장의 인식
  {
    const mo = c.mom; let t;
    if (mo) {
      t = '주가 6개월 ' + anPct(mo.ret6) + ' / 12개월 ' + anPct(mo.ret12) + '(분기말 주가 근사), 최근 4개 분기 실적공시 반응(기간말→공시일 평균) ' + anPct(mo.react) + '. ';
      const rr = vs.rangePer && vs.rangePer.y5; if (rr && anFin(rr.pct)) t += 'PER은 최근 5년 분포의 ' + anPct(rr.pct, 0) + ' 분위(0%=최저·100%=최고). ';
      t += (mo.level === '상승' || mo.level === '강함') ? '주가가 이미 개선을 반영하기 시작했습니다.' : mo.level === '약함' ? '아직 시장이 개선을 인정하지 않고 있습니다.' : '신호가 엇갈립니다.';
    } else t = '현재 주가를 입력하면 계산됩니다.';
    items.push(['시장이 이를 인식하기 시작했는가', t]);
  }
  return anSection('🧭 투자 판단 요약 (질문별 자동 정리)', '#0f766e', '아래 문장은 모두 이 화면의 계산 결과에서 자동으로 만들어진 것입니다. 인과를 증명하는 것이 아니라 숫자가 어떤 방향을 가리키는지 정리한 것입니다.',
    items.map((x, i) => '<div style="padding:7px 0; border-bottom:1px solid var(--border);"><div style="font-weight:700; font-size:13px;">' + (i + 1) + '. ' + x[0] + '</div><div style="font-size:12.5px; line-height:1.55; margin-top:2px;">' + x[1] + '</div></div>').join(''), true);
}

function anRenderMomentum() {
  const d = AN_STATE.mom;
  let inner;
  if (AN_STATE.momErr) inner = '<div style="font-size:12.5px; color:var(--danger);">' + anEsc(AN_STATE.momErr) + '</div>';
  else if (!d) inner = '<div style="font-size:12.5px; color:var(--text-muted);">일별 주가를 불러오는 중…</div>';
  else {
    const rsTxt = (v) => anFin(v) ? v.toFixed(3) + ' (' + (v >= 1 ? '+' : '') + ((v - 1) * 100).toFixed(1) + '%, ' + (v >= 1 ? '코스피 상회' : '코스피 하회') + ')' : 'N/A';
    const rt = (k) => anFin(d.ret[k]) ? (d.ret[k] >= 0 ? '+' : '') + anPct(d.ret[k]) : 'N/A';
    const kp = (k) => anFin(d.kospi[k]) ? '코스피 ' + (d.kospi[k] >= 0 ? '+' : '') + anPct(d.kospi[k]) : '';
    inner = '<div style="font-size:11.5px; color:var(--text-muted); margin-bottom:4px;">기준일 ' + d.base_date + ' 종가 ' + anWon(d.close) + ' (거래일 기준: 1M=21일, 3M=63일, 6M=126일)</div>' +
      anGrid([
        anTile('1M 수익률', rt('r1m'), kp('r1m')), anTile('3M 수익률', rt('r3m'), kp('r3m')), anTile('6M 수익률', rt('r6m'), kp('r6m')),
        anTile('6-1 모멘텀 (최근 1개월 제외)', (anFin(d.ret.r6m) && anFin(d.ret.r1m)) ? ((1 + d.ret.r6m) / (1 + d.ret.r1m) - 1 >= 0 ? '+' : '') + anPct((1 + d.ret.r6m) / (1 + d.ret.r1m) - 1) : 'N/A', '스크리닝 "6-1 모멘텀"과 같은 정의. 모멘텀 등급 판정에 사용'),
        anTile('20D RS (코스피 대비)', rsTxt(d.rs.rs20), kp('rs20')), anTile('60D RS (코스피 대비)', rsTxt(d.rs.rs60), kp('rs60')), anTile('120D RS (코스피 대비)', rsTxt(d.rs.rs120), kp('rs120')),
      ]) + (d.missing.length ? '<div style="font-size:11px; color:#b45309;">일부 구간은 시세가 부족해 N/A: ' + d.missing.join(', ') + '</div>' : '');
  }
  return anSection('📈 Momentum (일별 주가 · 코스피 대비)', '#0ea5e9',
    'RS = (1+종목 수익률) ÷ (1+코스피 수익률). 1보다 크면 같은 기간 코스피보다 많이 올랐다는 뜻입니다(상대강도 모멘텀: Jegadeesh-Titman 1993, Levy 1967). 스크리닝과 같은 거래일 기준·같은 공식입니다. 모멘텀 등급은 직전 1개월을 뺀 6-1·12-1 수익률로 판정합니다(1개월은 단기 반전 경향: Jegadeesh 1990). 시세는 data.go.kr에서 조회하며, 공표 지연으로 기준일은 보통 직전 거래일입니다.', inner, true);
}
// ---------- 미너비니 트렌드 템플릿 · 오닐 CAN SLIM(판정 가능한 항목만) ----------
async function anLoadTrend() {
  const name = (document.getElementById('corpName').value || '').trim();
  if (!name || AN_STATE.trendFor === name) return;
  AN_STATE.trendFor = name; AN_STATE.trend = null; AN_STATE.trendErr = null;
  try {
    const res = await fetch('/api/trend/stock?corp_name=' + encodeURIComponent(name));
    const data = await res.json();
    if (AN_STATE.trendFor !== name) return; // 그 사이 다른 종목으로 바뀌었으면 이 응답은 버림
    if (!res.ok) throw new Error(data.error || '조회 실패');
    AN_STATE.trend = data;
  } catch (e) { if (AN_STATE.trendFor !== name) return; AN_STATE.trendErr = '추세 조회 실패: ' + (e.message || e); }
  renderAnalysis();
}
function anRenderTrend(c) {
  const t = AN_STATE.trend;
  const mk = function (pass) { return pass === true ? '<span style="color:#16a34a; font-weight:700;">충족</span>' : pass === false ? '<span style="color:#dc2626; font-weight:700;">미충족</span>' : '<span style="color:#94a3b8;">판정 불가</span>'; };
  let mv = '';
  if (AN_STATE.trendErr) mv = '<div style="font-size:12.5px; color:var(--danger);">' + anEsc(AN_STATE.trendErr) + '</div>';
  else if (!t) mv = '<div style="font-size:12.5px; color:var(--text-muted);">일별 주가를 불러오는 중…</div>';
  else if (!t.ok) mv = '<div style="font-size:12.5px; padding:8px 10px; background:#fffbeb; border:1px solid #fcd34d; border-radius:8px;">트렌드 템플릿 판정 안 함: ' + anEsc(t.reason || '') + '</div>';
  else {
    mv = '<div style="font-size:11.5px; color:var(--text-muted); margin-bottom:4px;">기준일 ' + t.date + ' 종가 ' + anWon(t.close) + ' · 일별 종가 ' + t.n + '개 사용</div>' +
      '<div style="font-size:15px; font-weight:800; margin:4px 0;">' + t.passed + ' / ' + t.total + ' 충족' + (t.passed === t.total ? ' <span style="color:#16a34a;">— 1단계 조건을 모두 통과(스테이지 2 상승 추세)</span>' : '') + '</div>' +
      '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th style="text-align:left;">조건</th><th>결과</th><th style="text-align:left;">값</th></tr></thead><tbody>' +
      t.checks.map(function (x) { return '<tr><td class="an-first" style="text-align:left;">' + x.k + '. ' + x.label + '</td><td>' + mk(x.pass) + '</td><td style="text-align:left;">' + x.detail + '</td></tr>'; }).join('') + '</tbody></table></div>' +
      '<div style="font-size:12px; margin-top:6px; padding:6px 10px; background:#f8fafc; border:1px solid var(--border); border-radius:8px;"><b>8. RS Rating ≥ 70 (참고·대용, 위 개수에 미포함)</b> ' + (t.rs6 ? mk(t.rs6.pct >= 70) + ' — 최근 6개월(' + t.rs6.from + '~' + t.rs6.to + ') 수익률 ' + anPct(t.rs6.ret) + ', 저장된 ' + t.rs6.n + '개 종목 중 상위 ' + (100 - t.rs6.pct).toFixed(0) + '% (백분위 ' + t.rs6.pct.toFixed(0) + ')' : '<span style="color:#94a3b8;">판정 불가</span> — 저장된 전 종목 시세가 부족') + '<div style="font-size:11px; color:var(--text-muted);">원전은 12개월 가중 성과를 전 종목과 비교합니다. 여기서는 저장된 6개월(126거래일) 시세와 거래대금 상위 종목군만 쓰므로 근사치입니다.</div></div>' +
      '<div style="font-size:11px; color:var(--text-muted); margin-top:4px;">고가·저가는 종가 기준이며 원전은 장중 고저가입니다.</div>';
  }
  // CAN SLIM — 숫자로 명확히 판정 가능한 C, A, N만
  const L = c.list; const last = L[L.length - 1];
  const epsRow = c.qg ? c.qg.rows.find(function (x) { return x.name === 'EPS'; }) : null;
  const cg = epsRow ? epsRow.g[0] : null;
  const e3 = L.slice(-3).map(function (d) { return d.epsYoy; });
  const a1 = (e3.length === 3 && e3.every(anFin)) ? e3.every(function (v) { return v >= 0.25; }) : null;
  const roeL = last ? last.roe : null;
  const nPass = (t && t.ok) ? t.belowHi <= 0.15 : null;
  const cs = [
    ['C 당기 분기 이익 +25% 이상', cg == null ? null : cg >= 0.25, '최근 분기 EPS YoY ' + anPct(cg) + ' (분기 단독 지배주주순이익 ÷ 유통주식수. 분기 주식수는 직전 공시값을 이월한 경우가 있고, 주식수가 30% 넘게 달라졌으면 N/A)'],
    ['A 최근 3년 연간 EPS 증가율 각각 +25% 이상', a1, '최근 3년 EPS YoY: ' + e3.map(function (v) { return anPct(v, 0); }).join(' / ')],
    ['A ROE 17% 이상', anFin(roeL) ? roeL >= 0.17 : null, '최근 연간 ROE ' + anPct(roeL)],
    ['S 유통주식수 감소 또는 유지 (공급 축소)', anFin(last.shareChg) ? last.shareChg <= 0.005 : null, '유통주식수 전년 대비 ' + anPct(last.shareChg) + ((t && t.ok && anFin(t.volRatio)) ? ' · 최근 거래량은 50일 평균의 ' + t.volRatio.toFixed(2) + '배(오닐: 돌파 시 +40~50% 이상)' : '')],
    ['L 주도주 (6개월 상대강도 상위 20%)', (t && t.ok && t.rs6) ? t.rs6.pct >= 80 : null, (t && t.ok && t.rs6) ? '백분위 ' + t.rs6.pct.toFixed(0) + ' (대용: 오닐 RS Rating ≥ 80, 6개월·저장 종목군 기준)' : '전 종목 시세 부족'],
    ['M 시장 방향 (코스피 > 50일선, 50일선 상승)', (t && t.ok && t.market) ? (t.market.up && t.market.rising) : null, (t && t.ok && t.market) ? '코스피 ' + Math.round(t.market.close).toLocaleString() + ' vs 50일선 ' + Math.round(t.market.sma50).toLocaleString() + (t.market.rising ? ' (상승 중)' : ' (하락 중)') + ' · 오닐은 신규 고점 분산일 등으로 판단하므로 대용' : '코스피 지수 이력 부족'],
    ['N 신고가 근처 (52주 고가 대비 -15% 이내)', nPass, (t && t.ok) ? '고가 대비 -' + (t.belowHi * 100).toFixed(1) + '% (오닐은 차트 패턴의 피벗 돌파로 판단 — 15%는 대용 기준)' : '일별 시세 필요'],
  ];
  const ctab = '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th style="text-align:left;">항목</th><th>결과</th><th style="text-align:left;">값</th></tr></thead><tbody>' +
    cs.map(function (x) { return '<tr><td class="an-first" style="text-align:left;">' + x[0] + '</td><td>' + mk(x[1]) + '</td><td style="text-align:left;">' + x[2] + '</td></tr>'; }).join('') + '</tbody></table></div>' +
    '<div style="font-size:11px; color:var(--text-muted); margin-top:4px;"><b>판정하지 않은 항목:</b> I(기관 매수 추이) — 기관 수급 자료가 DB에 없습니다. S·L·M은 가진 자료로 만든 <b>대용 판정</b>이라 원전 기준과 다릅니다.</div>';
  const desc = '두 방식 모두 <b>상승 추세에 있는 성장주</b>를 고르는 실무 기법입니다. ' + anBadge('practice') + ' 미너비니 「Trade Like a Stock Market Wizard」(2013) 트렌드 템플릿, 오닐 「How to Make Money in Stocks」 CAN SLIM. 학술 검증은 아니지만 52주 고가 근접(George-Hwang 2004)·중기 모멘텀(Jegadeesh-Titman 1993)과 방향이 같습니다. 매수 신호가 아니라 <b>조건 점검표</b>입니다.';
  return anSection('🏅 미너비니 트렌드 템플릿 · 오닐 CAN SLIM (판정 가능한 항목만)', '#be123c', desc,
    '<div style="font-weight:700; font-size:13px; margin:4px 0;">미너비니 트렌드 템플릿</div>' + mv + '<div style="font-weight:700; font-size:13px; margin:12px 0 4px;">오닐 CAN SLIM</div>' + ctab, true);
}

async function anLoadMomentum() {
  const name = (document.getElementById('corpName').value || '').trim();
  if (!name || AN_STATE.momFor === name) return;
  AN_STATE.momFor = name; AN_STATE.mom = null; AN_STATE.momErr = null;
  try {
    const res = await fetch('/api/momentum/stock?corp_name=' + encodeURIComponent(name));
    const data = await res.json();
    if (AN_STATE.momFor !== name) return;
    if (!res.ok) throw new Error(data.error || '조회 실패');
    if (!data || !data.ret || !data.rs) throw new Error('응답 형식이 올바르지 않습니다.');
    AN_STATE.mom = data;
  } catch (e) { if (AN_STATE.momFor !== name) return; AN_STATE.momErr = '일별 모멘텀 조회 실패: ' + (e.message || e); }
  renderAnalysis();
}

function anRenderRD(c) {
  const L = c.list; const last = L[L.length - 1]; const a = L.find((d) => d.year === last.year - 3);
  const chk = [];
  const rg = anCagr(L, 'revenue', 3); chk.push(['매출 ↑ (3년 CAGR)', rg == null ? null : rg > 0, anPct(rg)]);
  chk.push(['매출총이익률 ↑ (3년 전 대비)', a && anFin(a.gm) && anFin(last.gm) ? last.gm > a.gm : null, a ? anPct(a.gm) + ' → ' + anPct(last.gm) : 'N/A']);
  chk.push(['ROIC ↑ (3년 전 대비)', a && anFin(a.roic) && anFin(last.roic) ? last.roic > a.roic : null, a ? anPct(a.roic) + ' → ' + anPct(last.roic) : 'N/A']);
  chk.push(['R&D 투자 ↑ (무형자산 취득 대용치, 3년 전 대비)', a && anFin(a.rd) && anFin(last.rd) ? last.rd > a.rd : null, a ? anEok(a.rd) + ' → ' + anEok(last.rd) : 'N/A(재조회 필요)']);
  const pass = chk.filter((x) => x[1] === true).length, known = chk.filter((x) => x[1] != null).length;
  const verdict = known < 3 ? '자료 부족' : pass === chk.length ? 'R&D(투자)가 매출·마진·ROIC 개선으로 이어지는 패턴' : pass >= 3 ? '대체로 성과로 이어지는 중(일부 항목 미충족)' : '투자 대비 성과가 아직 확인되지 않음';
  const body = '<div style="font-size:15px; font-weight:800; margin:4px 0;">' + verdict + ' <span style="font-size:12px; color:var(--text-muted); font-weight:500;">(' + pass + '/' + chk.length + ' 충족)</span></div>' +
    chk.map((x) => '<div style="font-size:12.5px; padding:3px 0;">' + anMark(x[1]) + ' ' + x[0] + ' — <span style="color:var(--text-muted);">' + x[2] + '</span></div>').join('') +
    anTable(L, [{ label: '매출액', key: 'revenue', fmt: 'eok' }, { label: '매출총이익률', key: 'gm', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }, { label: '무형자산 취득(R&D 대용)', key: 'rd', fmt: 'eok' }, { label: '무형자산취득/매출', key: 'rdRev', fmt: 'pct' }], 8);
  return anSection('🔬 R&D의 경제적 성과 점검', '#7c3aed',
    '재무제표 본문(DART 계정 API)에는 <b>연구개발비 지출액이 없어</b> 직접 측정은 불가능합니다(주석 공시). 그래서 "R&D의 결과로 기대되는 3가지(매출↑·매출총이익률↑·ROIC↑)가 함께 개선되는가"를 보고, R&D 투자는 개발비 자본화가 잡히는 <b>무형자산 취득액</b>으로 대용했습니다. 동행 여부만 보여 줄 뿐 인과를 증명하지는 않습니다.', body, false);
}

function anRenderScenario(c) {
  if (!c.scen) return anSection('🎯 시나리오 밸류에이션 (Bear / Base / Bull)', '#2563eb', '', '<div style="font-size:12.5px;">ROE가 3개 연도 이상 있고 BPS를 계산할 수 있어야 합니다(조회·저장 후 다시 계산).</div>', true);
  const d = c.def; const p = c.price;
  const inp = (id, v) => '<input id="' + id + '" type="number" step="0.5" value="' + (v * 100).toFixed(1) + '" oninput="anScenarioInput()" style="width:68px; padding:4px 6px; font-size:13px;">';
  const nm = { bear: ['Bear (비관)', '#dc2626'], base: ['Base (기본)', '#2563eb'], bull: ['Bull (낙관)', '#16a34a'] };
  const head = '<tr><th>시나리오</th><th>출발 ROE(%)</th><th>10년 후 수렴 ROE(%)</th><th>주당가치</th><th>현재가 대비</th><th>내재 PBR</th></tr>';
  const body = ['bear', 'base', 'bull'].map((k) => '<tr><td style="color:' + nm[k][1] + '; font-weight:700;">' + nm[k][0] + '</td><td>' + inp('anS_' + k + '0', c.scen[k].roe0) + '</td><td>' + inp('anS_' + k + 'T', c.scen[k].roeT) + '</td><td id="anV_' + k + '"></td><td id="anU_' + k + '"></td><td id="anB_' + k + '"></td></tr>').join('');
  const payWarn = c.m.avgPayout == null ? '<div style="font-size:12px; color:#b45309; margin:6px 0;">⚠ 배당 자료가 없어 배당성향 0%(이익 전액 유보)로 가정했습니다. 이 가정은 가치를 높게 만들 수 있으니 아래 배당성향 칸에 직접 입력하세요.</div>' : '';
  const html = payWarn + '<div style="display:flex; flex-wrap:wrap; gap:14px; align-items:end; padding:8px 10px; background:#f8fafc; border:1px solid var(--border); border-radius:8px; margin:8px 0;">' +
    '<label style="font-size:12px; color:var(--text-muted);">요구수익률 r(%)<br/><input id="anR" type="number" step="0.5" value="' + (c.r * 100).toFixed(1) + '" onchange="anSetR()" style="width:70px; margin-top:3px;"></label>' +
    '<label style="font-size:12px; color:var(--text-muted);">배당성향(%)<br/><input id="anPayout" type="number" step="5" value="' + (c.payoutAvg * 100).toFixed(0) + '" onchange="anSetPayout()" style="width:70px; margin-top:3px;"></label>' +
    '<button onclick="anResetScenario()" style="height:30px; font-size:12px; padding:4px 10px;">기본값으로 초기화</button></div>' +
    '<div style="overflow-x:auto;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>' +
    '<div id="anW" style="margin-top:8px; font-size:13px;"></div>';
  const desc = '<b>잔여이익모형(Residual Income Model)</b> — Ohlson(1995) 등 회계 기반 가치평가의 표준 틀입니다. 주당가치 = BPS + 향후 10년 초과이익(ROE−r)×장부가치의 현재가치 + 종가치. ROE는 출발값에서 수렴값으로 선형 수렴(수익성은 평균으로 돌아가는 경향: Fama-French 2000), 종가치는 초과이익이 해마다 ' + AN_TH.omega + ' 비율로 감쇠한다고 둡니다(Dechow-Hutton-Sloan 1999 등 실증연구가 약 0.6~0.7대로 추정한 범위의 가정값 — 정확한 수치는 표본마다 다르며 이 기본값은 원문으로 확인하지 못했습니다). ' +
    '<br/>시나리오는 이 회사 <b>자신의 ROE 이력</b>에서 만듭니다: 출발 ROE = Base 최근 5년 중앙값(' + anPct(d.base0) + '), Bear/Bull은 ±1표준편차(' + anPct(d.sd) + '). 수렴 ROE = Bear: 요구수익률로 완전 수렴, Base: 요구수익률 쪽으로 절반 수렴, Bull: 중앙값 유지. 모두 직접 고쳐 볼 수 있습니다. 확률가중치 25/50/25는 임의 가정입니다. (배당성향은 순이익 대비 배당만 반영 — 자사주 매입은 별도 반영하지 않음)';
  return anSection('🎯 시나리오 밸류에이션 (Bear / Base / Bull) — 기존 BPS×(1+ROE)^10 대체', '#2563eb', desc, html, true);
}
function anScenarioInput() {
  const c = AN_CTX; if (!c || !c.def) return;
  const rd = (id) => { const e = document.getElementById(id); const v = e ? Number(e.value) : NaN; return isFinite(v) && e && e.value !== '' ? v / 100 : null; };
  for (const k of ['bear', 'base', 'bull']) { const a = rd('anS_' + k + '0'), b = rd('anS_' + k + 'T'); if (a != null) AN_STATE.ov[k + '0'] = a; if (b != null) AN_STATE.ov[k + 'T'] = b; }
  anUpdateScenarioView();
}
function anUpdateScenarioView() {
  const c = AN_CTX; if (!c || !c.def) return;
  const sc = anScenarioCompute(c.m, c.def, c.r, anClamp(c.payoutAvg, 0, 0.95), c.price); c.scen = sc;
  for (const k of ['bear', 'base', 'bull']) {
    const set = (id, t, col) => { const e = document.getElementById(id); if (e) { e.textContent = t; if (col) e.style.color = col; } };
    set('anV_' + k, anWon(sc[k].v)); set('anU_' + k, anFin(sc[k].up) ? (sc[k].up >= 0 ? '+' : '') + anPct(sc[k].up) : '현재가 입력 필요', anFin(sc[k].up) ? (sc[k].up >= 0 ? '#16a34a' : '#dc2626') : null); set('anB_' + k, anX(sc[k].pbr));
  }
  const w = document.getElementById('anW');
  if (w) {
    let pos = '';
    if (anFin(c.price)) pos = c.price < sc.bear.v ? ' → 현재가가 <b>Bear보다도 낮아</b> 비관 시나리오까지 반영된 가격입니다.' : c.price > sc.bull.v ? ' → 현재가가 <b>Bull보다도 높아</b> 낙관 시나리오 이상이 이미 반영된 가격입니다.' : c.price < sc.base.v ? ' → 현재가가 Bear~Base 사이(Base보다 저렴).' : ' → 현재가가 Base~Bull 사이(Base보다 비쌈).';
    w.innerHTML = '확률가중 가치(25/50/25): <b>' + anWon(sc.w) + '</b>' + (anFin(sc.wUp) ? ' (현재가 대비 ' + (sc.wUp >= 0 ? '+' : '') + anPct(sc.wUp) + ')' : '') + pos;
  }
}
function anSetR() { const v = Number(document.getElementById('anR').value); if (isFinite(v) && v > 1 && v < 30) { AN_STATE.r = v / 100; AN_STATE.ov = {}; renderAnalysis(); } }
function anSetPayout() { const v = Number(document.getElementById('anPayout').value); if (isFinite(v) && v >= 0 && v <= 95) { AN_STATE.payout = v / 100; renderAnalysis(); } }
function anResetScenario() { AN_STATE.ov = {}; AN_STATE.payout = null; AN_STATE.r = AN_TH.defaultR; renderAnalysis(); }

// ---------- 가치평가 방법 비교: RIM+고든 / 오너어닝스+국채금리 / 린치 6분류·PEG ----------
const AN_M = { g: 0, oeG: 0, erp: 0.05, rfOv: null };
// RIM + 고든 종가치: 10년 명시기간 뒤 초과이익이 매년 g로 영구 성장 → TV = RI_N(1+g)/(r−g). g=0이면 초과이익이 영구 유지(ω=1)
function anRIMGordon(B0, roe0, roeT, payout, r, N, g) {
  let B = B0, pv = 0, lastRI = 0;
  for (let t = 1; t <= N; t++) {
    const roe = roe0 + (roeT - roe0) * (t - 1) / (N - 1);
    const ni = roe * B; const ri = (roe - r) * B;
    pv += ri / Math.pow(1 + r, t); lastRI = ri;
    B = B + ni * (1 - payout);
  }
  pv += (lastRI * (1 + g) / (r - g)) / Math.pow(1 + r, N);
  return B0 + pv;
}
function anLatestRf() {
  if (anFin(AN_M.rfOv)) return { v: AN_M.rfOv, src: '직접 입력', date: null };
  let s = null;
  try { s = (typeof macroSeriesCache !== 'undefined' && macroSeriesCache) ? macroSeriesCache.ktb10y : null; } catch (e) { s = null; }
  if (!s || !s.length) return null;
  let b = s[0]; for (const p of s) { if (p.date > b.date) b = p; }
  return { v: b.value / 100, src: 'ECOS 국고채 10년', date: b.date };
}
// 오너어닝스 = 영업활동현금흐름 − 유지보수 CAPEX (Greenwald et al. 2001 『Value Investing』의 유지보수 CAPEX 추정 방식; 버핏 1986의 "유지에 필요한 설비투자만 차감" 개념)
//   비율 = 전기말 유형자산 ÷ 전기 매출,  성장 CAPEX = 비율 × 매출 증가분,  유지보수 CAPEX = 총 CAPEX − 성장 CAPEX,  오너어닝스 = 영업CF − 유지보수 CAPEX
//   (= FCF + 성장 CAPEX). 영업CF가 이미 감가상각 가산과 운전자본 변동을 반영하므로 감가상각비 항목이 따로 필요 없다.
//   가드레일: A) 매출이 줄었으면 성장 CAPEX=0(CAPEX 전액을 유지보수로 봄)  B) 유지보수 CAPEX가 음수면 0  C) 전기 매출이 없거나 0 이하·전기 유형자산/CAPEX 누락이면 그 연도는 계산하지 않음
function anOwnerEarn(c) {
  const ly0 = c.list[c.list.length - 1].year;
  const L = c.list.filter(function (d) { return d.year > ly0 - 3; });
  const det = L.map(function (d) {
    const o = { year: d.year, oe: null, why: null };
    if (!anFin(d.ocf)) { o.why = '영업CF 없음'; return o; }
    if (!anFin(d.capex) || d.capex < 0) { o.why = 'CAPEX(유형자산 취득) 없음'; return o; }
    if (!anFin(d.revenue)) { o.why = '당기 매출 없음'; return o; }
    if (!anFin(d.salesPrev) || !(d.salesPrev > 0)) { o.why = '전기 매출 없음/0 이하 또는 전기 기준(연결·개별) 불일치'; return o; }
    if (!anFin(d.ppePrev) || d.ppePrev < 0) { o.why = '전기말 유형자산 없음'; return o; }
    const ratio = d.ppePrev / d.salesPrev, dS = d.revenue - d.salesPrev;
    const growth = dS > 0 ? ratio * dS : 0; // 규칙 A
    let maint = d.capex - growth; let ruleB = false;
    if (maint < 0) { maint = 0; ruleB = true; } // 규칙 B
    const nc = anFin(d.niCons) ? d.niCons : d.ni; const sh = (anFin(nc) && anFin(d.ni) && nc > 0 && d.ni > 0) ? anClamp(d.ni / nc, 0, 1) : 1;
    o.ratio = ratio; o.dS = dS; o.growth = growth; o.maint = maint; o.capex = d.capex; o.ocf = d.ocf; o.ruleA = dS <= 0; o.ruleB = ruleB; o.sh = sh;
    o.oeCons = d.ocf - maint; o.oe = o.oeCons * sh; // 영업CF·CAPEX는 연결 전체 → 지배주주 지분율(지배주주순이익÷연결순이익)로 지배주주 몫 환산
    return o;
  });
  if (L.length < 3 || det.filter(function (o) { return anFin(o.oe); }).length < 3) return { fail: true, det: det, n: L.length };
  const arr = det.map(function (o) { return o.oe; });
  const oe = anMean(arr); const sh = c.last.shares;
  return { oe: oe, arr: arr, det: det, years: L.map(function (d) { return d.year; }), oeps: anFin(sh) && sh > 0 ? oe / sh : null };
}
// 린치 6분류. 숫자 기준을 린치가 명시한 것(성장률: 저성장 2~4%, 우량 10~12%, 고성장 20~25%)만 사용하고, 애매한 구간은 '경계'로 남김
function anLynch(c) {
  const L = c.list.slice(-8); const out = { key: null, name: null, conf: '분류 불가', why: [], g: null, gBasis: null, notes: [] };
  const known = L.filter(function (d) { return anFin(d.ni); });
  const cur = c.last.ni;
  if (known.length < 4 || !anFin(cur)) { out.why.push('순이익 자료가 4개 연도 미만이라 분류하지 않습니다.'); return out; }
  const g5 = anCagr(c.list, 'eps', 5), g3 = anCagr(c.list, 'eps', 3);
  const gg = anFin(g5) ? g5 : g3; out.g = gg; out.gBasis = anFin(g5) ? '5년 EPS CAGR' : (anFin(g3) ? '3년 EPS CAGR' : null);
  const prev3 = L.slice(-4, -1).map(function (d) { return d.ni; }).filter(anFin);
  if (cur < 0) { out.key = 'loss'; out.name = '적자 (회생주 후보 — 확정 불가)'; out.conf = '분류 보류'; out.why.push('최근 연간 순이익이 적자입니다. 회생(Turnaround)인지 쇠퇴인지는 숫자만으로 구분할 수 없어 보류합니다.'); return out; }
  let cdown = 0, cup = 0;
  for (let i = 1; i < L.length; i++) { const a0 = L[i - 1].ni, b0 = L[i].ni; if (anFin(a0) && anFin(b0) && a0 > 0) { const y0 = b0 / a0 - 1; if (y0 <= -0.15) cdown++; else if (y0 >= 0.15) cup++; } }
  const cycLike = known.length >= 6 && cdown >= 2 && cup >= 2;
  if (prev3.some(function (v) { return v < 0; }) && cycLike) { out.key = 'turn'; out.name = '회생주 또는 순환주 (구분 불가)'; out.conf = '경계'; out.why.push('최근 적자가 있었지만 이익이 오르내림을 반복하는 패턴이라 회생주인지 경기순환주인지 숫자만으로 가를 수 없습니다.'); return out; }
  if (prev3.some(function (v) { return v < 0; })) { out.key = 'turn'; out.name = '회생주 (Turnaround)'; out.conf = '명확'; out.why.push('최근 3개 연도 안에 적자가 있었고 최근 연도는 흑자입니다.'); return out; }
  const vs = c.vs; const nc = (anFin(vs.netDebt) && anFin(vs.mcap) && vs.mcap > 0) ? -vs.netDebt / vs.mcap : null;
  if (anFin(nc) && nc >= 0.5) { out.key = 'asset'; out.name = '자산주 (Asset Play)'; out.conf = '명확'; out.why.push('순현금(현금+단기금융상품−차입금)이 시가총액의 ' + anPct(nc, 0) + '로 50% 이상입니다.'); return out; }
  if (anFin(vs.pbr) && vs.pbr <= 0.5) out.notes.push('PBR ' + anX(vs.pbr) + '(0.5 이하): 자산주 성격이 있을 수 있으나 자산의 실제 가치를 알 수 없어 분류에는 쓰지 않았습니다.');
  let downs = 0, ups = 0;
  for (let i = 1; i < L.length; i++) { const a = L[i - 1].ni, b = L[i].ni; if (anFin(a) && anFin(b) && a > 0) { const y = b / a - 1; if (y <= -0.15) downs++; else if (y >= 0.15) ups++; } }
  if (known.length >= 6 && downs >= 2 && ups >= 2) { out.key = 'cyc'; out.name = '경기순환주 (Cyclical) 의심'; out.conf = '경계(휴리스틱)'; out.why.push('최근 ' + L.length + '년 중 순이익이 15% 이상 줄어든 해 ' + downs + '번, 15% 이상 늘어난 해 ' + ups + '번. 린치는 업종(자동차·철강·항공 등)으로 구분하므로 이 기준은 대용치입니다.'); return out; }
  if (!anFin(gg)) { out.why.push('EPS 성장률(3·5년 CAGR)을 구할 수 없어(시작·끝 연도가 모두 흑자여야 함) 성장 기준 분류를 하지 않습니다.'); return out; }
  const gs = anPct(gg) + '(' + out.gBasis + ')';
  if (gg < 0.06) { out.key = 'slow'; out.name = '저성장주 (Slow Grower)'; out.conf = '명확'; out.why.push('EPS 성장률 ' + gs + ' < 6% (린치: GDP 성장률 안팎, 2~4%).'); }
  else if (gg < 0.15) { out.key = 'stal'; out.name = '우량 대형주 (Stalwart)'; out.conf = '명확'; out.why.push('EPS 성장률 ' + gs + ' (6~15%, 린치: 약 10~12%).'); }
  else if (gg >= 0.20) { out.key = 'fast'; out.name = '고성장주 (Fast Grower)'; out.conf = '명확'; out.why.push('EPS 성장률 ' + gs + ' ≥ 20% (린치: 20~25%).'); }
  else { out.key = 'edge'; out.name = '우량주~고성장주 경계'; out.conf = '경계'; out.why.push('EPS 성장률 ' + gs + '가 15~20% 구간이라 린치 기준(10~12% vs 20~25%)으로 어느 쪽인지 가르지 않았습니다.'); }
  if (anFin(gg) && gg > 0.5) out.notes.push('성장률이 50%를 넘습니다. 지속 불가능한 단기 급성장일 수 있어 PEG 신뢰도가 낮습니다(린치: 25% 넘는 성장이 오래가는 경우는 드묾).');
  return out;
}
function anLynchPeg(c, ly, price) {
  const per = c.vs.per, g = ly.g;
  const dps = c.last.r ? c.last.r.dividend_per_share : null;
  const dy = (anFin(dps) && anFin(price) && price > 0) ? dps / price : 0;
  const dyKnown = anFin(dps);
  const res = { applicable: false, reason: '', peg: null, adj: null, ratio: null, dy: dy, dyKnown: dyKnown, verdict: '' };
  if (ly.key === 'cyc') { res.reason = '경기순환주는 이익이 정점일 때 PER이 가장 낮아 보이므로 PEG를 쓰지 않습니다(린치: 순환주는 PER이 낮을 때가 오히려 위험).'; return res; }
  if (ly.key === 'turn' || ly.key === 'loss') { res.reason = '회생주는 과거 성장률이 의미 없어 PEG를 쓰지 않습니다(린치: 부채·현금 소진 속도를 봄).'; return res; }
  if (ly.key === 'asset') { res.reason = '자산주는 이익 성장이 아니라 자산 가치가 핵심이라 PEG를 쓰지 않습니다.'; return res; }
  if (!ly.key || !anFin(g) || g <= 0 || !anFin(per) || per <= 0) { res.reason = 'PER 또는 양(+)의 EPS 성장률이 없어 계산할 수 없습니다.'; return res; }
  res.applicable = true;
  res.peg = per / (g * 100);
  res.adj = per / (g * 100 + dy * 100);
  res.ratio = (g * 100 + dy * 100) / per;
  const r = res.ratio;
  res.verdict = r >= 2 ? '린치 기준 매력적(비율 2 이상)' : r >= 1.5 ? '무난(1.5~2)' : r >= 1 ? '보통(1~1.5)' : '불리(1 미만)';
  return res;
}
function anRenderMethods(c) {
  const html = '<div id="anMethBody"></div>';
  const desc = '같은 종목을 <b>서로 다른 가치평가 방법</b>으로 계산해 나란히 보여줍니다. 방법마다 가정이 달라 숫자가 다르며, 평균 내지 않고 <b>어떤 가정에서 얼마가 나오는지</b>를 보는 용도입니다. 명확히 계산할 수 있는 것만 넣었고 자료가 부족하면 N/A로 둡니다.';
  return anSection('🧭 가치평가 방법 비교 · 린치 6분류 (RIM+고든 / 오너어닝스+국채금리 / PEG)', '#0f766e', desc, html, true);
}
function anMethSet(k, id) {
  const e = document.getElementById(id); const v = e ? Number(e.value) : NaN;
  if (e && e.value === '') { if (k === 'rfOv') AN_M.rfOv = null; }
  else if (isFinite(v)) { if (k === 'g') AN_M.g = anClamp(v / 100, -0.05, 0.05); else if (k === 'oeG') AN_M.oeG = anClamp(v / 100, -0.05, 0.08); else if (k === 'erp') AN_M.erp = anClamp(v / 100, 0, 0.12); else if (k === 'rfOv') AN_M.rfOv = anClamp(v / 100, 0.001, 0.2); }
  anUpdateMethods();
}
function anUpdateMethods() {
  const c = AN_CTX; const el = document.getElementById('anMethBody'); if (!c || !el) return;
  const p = c.price, r = c.r, rf = anLatestRf();
  const up = function (v) { return anFin(v) && anFin(p) && p > 0 ? '<span style="color:' + (v >= p ? '#16a34a' : '#dc2626') + '; font-weight:700;">' + (v >= p ? '+' : '') + anPct(v / p - 1) + '</span>' : '현재가 필요'; };
  const inpS = 'width:70px; margin-top:3px;';
  const rows = [];
  // 1) RIM
  if (c.scen && c.def) {
    const pay = anClamp(c.payoutAvg, 0, 0.95); const g = AN_M.g;
    const okG = g < r - 0.02;
    const gv = function (k) { return okG ? anRIMGordon(c.m.bps, c.scen[k].roe0, c.scen[k].roeT, pay, r, 10, g) : null; };
    rows.push(['① RIM + 감쇠 종가치(ω=' + AN_TH.omega + ') — 현행', 'Base ' + anWon(c.scen.base.v), '범위 ' + anWon(c.scen.bear.v) + ' ~ ' + anWon(c.scen.bull.v), up(c.scen.base.v), '초과이익이 해마다 ' + AN_TH.omega + '배로 줄어듦(DHS 1999 등 실증연구의 0.6~0.7대 범위 안의 가정값)']);
    const b = gv('base'), lo = gv('bear'), hi = gv('bull');
    rows.push(['② RIM + 고든 종가치 (g=' + anPct(g) + ')', anFin(b) ? 'Base ' + anWon(b) : 'N/A', anFin(lo) ? '범위 ' + anWon(lo) + ' ~ ' + anWon(hi) : 'g는 r−2%p보다 작아야 함', up(b), '10년 뒤 초과이익이 g로 영구 성장(고든). g=0%면 초과이익이 영원히 유지된다는 뜻이라 ①보다 낙관적일 수 있음']);
  } else rows.push(['① ② RIM', 'N/A', '', '', 'ROE 3개 연도 이상과 BPS가 필요']);
  // 2) 오너어닝스
  let oe = anOwnerEarn(c);
  let oeNote = '';
  if (!oe || oe.fail) {
    const miss = (oe && oe.det ? oe.det : []).filter(function (o) { return o.why; }).map(function (o) { return o.year + ': ' + o.why; });
    oeNote = '영업CF·CAPEX·전기 매출·전기말 유형자산이 최근 3개 연도 모두 있어야 계산합니다.' + (miss.length ? ' 비어 있는 항목 — ' + miss.join(' / ') : ' (연도 수 부족)') + ' (이전에 저장된 행은 재조회가 필요할 수 있음)';
    oe = null;
  }
  else if (!anFin(oe.oeps)) oeNote = '오너어닝스 총액은 ' + anEok(oe.oe) + '(3년 평균)이나, 최근 연도 주식수가 없어 주당 값을 계산할 수 없습니다(주식수 출처가 "미확인"이면 재조회 필요).';
  else if (!rf) oeNote = '국고채 10년 금리가 없습니다: 거시경제 데이터를 갱신하거나 아래에 직접 입력하세요.';
  else if (oe.oeps <= 0) oeNote = '3년 평균 오너어닝스가 0 이하(' + anEok(oe.oe) + ')라 가치를 계산하지 않습니다.';
  if (!oeNote) {
    const gg = AN_M.oeG; const d1 = rf.v - gg, d2 = rf.v + AN_M.erp - gg;
    if (d1 > 0.005) rows.push(['③ 오너어닝스 ÷ 국채금리 (g=' + anPct(gg) + ')', anWon(oe.oeps * (1 + gg) / d1), '할인율 ' + anPct(rf.v, 2), up(oe.oeps * (1 + gg) / d1), '버핏 방식: 국채금리로 할인(위험프리미엄 없음) — 낙관적 상한에 가까움']);
    if (d1 <= 0.005 && d2 <= 0.005) rows.push(['③ ④ 오너어닝스', 'N/A', '', '', '성장률이 할인율(국채금리 − 0.5%p) 이상이라 고든 공식을 쓸 수 없음']);
    if (d2 > 0.005) rows.push(['④ 오너어닝스 ÷ (국채금리+위험프리미엄 ' + anPct(AN_M.erp) + ')', anWon(oe.oeps * (1 + gg) / d2), '할인율 ' + anPct(rf.v + AN_M.erp, 2), up(oe.oeps * (1 + gg) / d2), '위험프리미엄은 사용자 가정(기본 5%)']);
  } else rows.push(['③ ④ 오너어닝스', 'N/A', '', '', oeNote]);
  const head = '<tr><th style="text-align:left;">방법</th><th>주당가치</th><th>범위/조건</th><th>현재가 대비</th><th style="text-align:left;">핵심 가정</th></tr>';
  const tbl = '<div style="overflow-x:auto;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + rows.map(function (x) { return '<tr><td class="an-first" style="text-align:left;">' + x[0] + '</td><td>' + x[1] + '</td><td>' + x[2] + '</td><td>' + x[3] + '</td><td style="text-align:left; font-size:11.5px;">' + x[4] + '</td></tr>'; }).join('') + '</tbody></table></div>';
  const ctl = '<div style="display:flex; flex-wrap:wrap; gap:14px; align-items:end; padding:8px 10px; background:#f8fafc; border:1px solid var(--border); border-radius:8px; margin:8px 0;">' +
    '<label style="font-size:12px; color:var(--text-muted);">RIM 종가치 성장률 g(%)<br/><input id="anMg" type="number" step="0.5" value="' + (AN_M.g * 100).toFixed(1) + '" onchange="anMethSet(' + "'g'" + ',' + "'anMg'" + ')" style="' + inpS + '"></label>' +
    '<label style="font-size:12px; color:var(--text-muted);">오너어닝스 성장률(%)<br/><input id="anMoeg" type="number" step="0.5" value="' + (AN_M.oeG * 100).toFixed(1) + '" onchange="anMethSet(' + "'oeG'" + ',' + "'anMoeg'" + ')" style="' + inpS + '"></label>' +
    '<label style="font-size:12px; color:var(--text-muted);">위험프리미엄(%)<br/><input id="anMerp" type="number" step="0.5" value="' + (AN_M.erp * 100).toFixed(1) + '" onchange="anMethSet(' + "'erp'" + ',' + "'anMerp'" + ')" style="' + inpS + '"></label>' +
    '<label style="font-size:12px; color:var(--text-muted);">국채 10년(%) 직접 입력<br/><input id="anMrf" type="number" step="0.1" placeholder="' + (rf && !anFin(AN_M.rfOv) ? (rf.v * 100).toFixed(2) : '') + '" value="' + (anFin(AN_M.rfOv) ? (AN_M.rfOv * 100).toFixed(2) : '') + '" onchange="anMethSet(' + "'rfOv'" + ',' + "'anMrf'" + ')" style="' + inpS + '"></label>' +
    '<div style="font-size:11.5px; color:var(--text-muted);">현재 국채금리: <b>' + (rf ? anPct(rf.v, 2) : 'N/A') + '</b>' + (rf ? ' (' + rf.src + (rf.date ? ', ' + rf.date : '') + ')' : '') + ' · 요구수익률 r(RIM) ' + anPct(r) + '</div></div>';
  let oeBox = '';
  if (oe) {
    const detTxt = oe.det.map(function (o) {
      return o.year + ': 영업CF ' + anEok(o.ocf) + ' − 유지보수CAPEX ' + anEok(o.maint) + ' (총CAPEX ' + anEok(o.capex) + ' − 성장 ' + anEok(o.growth) + (o.ruleA ? ', 매출 감소→성장 0' : '') + (o.ruleB ? ', 음수→0' : '') + ')' + (o.sh < 0.9999 ? ' × 지배지분율 ' + (o.sh * 100).toFixed(0) + '%' : '') + ' = ' + anEok(o.oe);
    }).join('<br/>');
    oeBox = '<div style="font-size:12px; margin:6px 0; line-height:1.5;"><b>오너어닝스 구성</b> (영업CF − 유지보수 CAPEX, 3년 평균 <b>' + anEok(oe.oe) + '</b>)<br/>' + detTxt +
      (anFin(c.vs.mcap) && c.vs.mcap > 0 ? '<br/><b>오너어닝스 수익률 ' + anPct(oe.oe / c.vs.mcap) + '</b>' + (rf ? ' vs 국채 ' + anPct(rf.v, 2) + ' → 스프레드 ' + ((oe.oe / c.vs.mcap - rf.v) * 100).toFixed(1) + '%p' : '') : '') +
      '<div style="color:var(--text-muted); font-size:11px;">방법: 유지보수 CAPEX = 총 CAPEX − (전기말 유형자산÷전기 매출)×매출 증가분 (Greenwald 등 2001의 추정). 매출 감소 시 성장 CAPEX 0, 유지보수가 음수면 0, 전기 매출이 없으면 계산 제외. 한계: 총 CAPEX는 유형+무형자산 취득이나 비율은 유형자산만 사용(무형 비중이 큰 회사는 유지보수 CAPEX가 다소 크게 추정 = 보수적), 영업CF에는 운전자본 변동·이자·법인세 지급이 그대로 포함되어 해마다 출렁일 수 있음(3년 평균으로 완화).</div></div>';
  }
  const ly = anLynch(c); const pg = anLynchPeg(c, ly, p);
  let lyBox = '<div style="margin-top:12px; font-weight:700; font-size:13px; color:#0f766e;">피터 린치 6분류 · PEG</div>' +
    '<div style="font-size:13px; margin:6px 0;"><b>' + (ly.name || '분류 불가') + '</b> <span style="font-size:11px; color:#fff; background:' + (ly.conf === '명확' ? '#16a34a' : ly.conf.indexOf('경계') === 0 ? '#d97706' : '#64748b') + '; border-radius:8px; padding:0 6px;">' + ly.conf + '</span></div>' +
    '<ul style="font-size:12px; margin:4px 0 4px 18px; line-height:1.5;">' + ly.why.concat(ly.notes).map(function (t) { return '<li>' + t + '</li>'; }).join('') + '</ul>';
  if (pg.applicable) {
    lyBox += anGrid([anTile('PER', anX(c.vs.per)), anTile('성장률', anPct(ly.g), ly.gBasis + ' (과거 실적, 린치는 예상치 사용)'), anTile('PEG', anNum(pg.peg), 'PER ÷ 성장률(%) · 1 이하면 성장에 비해 싸다, 0.5 이하 매우 유리, 2 이상 불리(린치)'),
      anTile('배당수익률', pg.dyKnown ? anPct(pg.dy) : '자료 없음(0 가정)', '최근 연간 DPS ÷ 현재가'), anTile('배당조정 PEG', anNum(pg.adj), 'PER ÷ (성장률+배당수익률)'), anTile('린치 비율', anNum(pg.ratio), '(성장률+배당수익률) ÷ PER · 2 이상 좋음, 1 미만 나쁨 → ' + pg.verdict)]);
  } else lyBox += '<div style="font-size:12px; padding:6px 10px; background:#f8fafc; border:1px solid var(--border); border-radius:8px;">PEG 미산출: ' + pg.reason + '</div>';
  const ev = '<div style="font-size:11px; color:var(--text-muted); margin-top:8px; line-height:1.5;">근거: ' + anBadge('theory') + ' RIM(Ohlson 1995, Edwards-Bell 1961)·고든 성장모형 / ' + anBadge('practice') + ' 오너어닝스(Buffett, 1986 버크셔 주주서한) · 린치 6분류·PEG(Lynch, <i>One Up on Wall Street</i>, 1989) — 모두 투자자 실무 틀이며 초과수익을 학술적으로 입증한 것은 아닙니다. 린치 분류 중 순환주 판정은 휴리스틱이라 "경계"로 표시합니다. 위 숫자는 모형 출력이지 목표주가가 아닙니다.</div>';
  el.innerHTML = tbl + ctl + oeBox + lyBox + ev;
}

// 재무분석 화면 상단 이동 막대: 섹션 바로가기 + 전체 펼치기/접기
function anBuildNav() {
  const el = document.getElementById('analysis'); if (!el) return;
  const ds = Array.prototype.slice.call(el.children).filter(function (x) { return x.tagName === 'DETAILS'; });
  if (ds.length < 3) return;
  const nav = document.createElement('div');
  nav.style.cssText = 'position:sticky; top:0; z-index:5; display:flex; flex-wrap:wrap; gap:6px; align-items:center; padding:6px 8px; margin:0 0 8px; background:var(--card, #fff); border:1px solid var(--border); border-radius:8px; font-size:12px;';
  const mk = function (txt, fn, strong) { const b = document.createElement('button'); b.type = 'button'; b.textContent = txt; b.style.cssText = 'font-size:12px; padding:3px 8px; height:auto; ' + (strong ? 'font-weight:700;' : ''); b.onclick = fn; nav.appendChild(b); };
  mk('전체 펼치기', function () { ds.forEach(function (d) { d.open = true; }); }, true);
  mk('전체 접기', function () { ds.forEach(function (d) { d.open = false; }); }, true);
  ds.forEach(function (d) {
    const sm = d.querySelector('summary'); if (!sm) return;
    const t = sm.textContent.split(' — ')[0].split(' (')[0].trim();
    if (!t) return;
    mk(t.length > 16 ? t.slice(0, 16) + '…' : t, function () { d.open = true; d.scrollIntoView({ behavior: 'smooth', block: 'start' }); });
  });
  el.insertBefore(nav, el.firstChild);
}
// ---------- ROIC 4단계 분해 (시계열 표 + 행을 클릭하면 차트) ----------
let AN_R4SEL = ['r4roic'];
const AN_R4ROWS = [
  { key: 'r4gm', label: '① 매출총이익률 (GP/매출)', fmt: 'pct', col: '#7c3aed', tip: '제품 경쟁력·가격결정력' },
  { key: 'r4turn', label: '② 총자산회전율 (매출/총자산)', fmt: 'x', col: '#0d9488', tip: '자산을 얼마나 굴려 매출을 만드는가' },
  { key: 'r4gpa', label: '  └ ①×② = GPA (GP/총자산, Novy-Marx)', fmt: 'pct', col: '#2563eb', tip: '참고: 위 두 항의 곱' },
  { key: 'r4opx', label: '③ 영업비용 통제력 (NOPAT/GP)', fmt: 'pct', col: '#d97706', tip: '매출총이익 중 세후영업이익으로 남는 비율' },
  { key: 'r4lev', label: '④ 투하자본 배수 (총자산/투하자본)', fmt: 'x', col: '#dc2626', tip: '총자산 대비 영업투하자본이 작을수록 ↑' },
  { key: 'r4roic', label: '= ROIC (NOPAT/평균 투하자본)', fmt: 'pct', col: '#111827', tip: '①×②×③×④' },
];
function anRenderRoic4(c) {
  const L = c.list.slice(-10);
  const have = L.filter(function (d) { return d.r4; });
  const desc = 'ROIC = <b>매출총이익률 × 총자산회전율 × 영업비용 통제력(NOPAT/매출총이익) × 투하자본 배수(총자산/투하자본)</b>. 네 항을 곱하면 NOPAT÷투하자본(ROIC)이 되는 항등식이며, 모든 항에 같은 평균(기초·기말) 기준을 써서 오차 없이 맞습니다(아래 검산 행). ①×②는 Novy-Marx(2013)의 GPA입니다. 행 이름을 <b>클릭하면 아래에 추이 차트</b>가 추가/제거됩니다.';
  if (!have.length) return anSection('🧩 ROIC 4단계 분해 (시계열)', '#0f766e', desc, '<div style="font-size:12.5px;">계산할 수 있는 연도가 없습니다. 매출총이익(매출원가)이 공시되지 않는 회사(영업비용만 공시하는 회사·금융업)이거나 투하자본 항목이 저장되지 않은 경우입니다(재조회 필요할 수 있음).</div>', true);
  const head = '<tr><th class="an-first">항목 (클릭 → 차트)</th>' + L.map(function (d) { return '<th>' + d.year + '</th>'; }).join('') + '</tr>';
  const body = AN_R4ROWS.map(function (rw) {
    return '<tr data-r4="' + rw.key + '" onclick="anR4Toggle(' + "'" + rw.key + "'" + ')" style="cursor:pointer;" title="' + rw.tip + ' — 클릭하면 차트"><td class="an-first"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:' + rw.col + ';margin-right:5px;"></span>' + rw.label + '</td>' + L.map(function (d) { return '<td>' + anFmt(rw.fmt, d[rw.key]) + '</td>'; }).join('') + '</tr>';
  }).join('') + '<tr style="color:var(--text-muted);"><td class="an-first">검산: ①×②×③×④ − ROIC (0이어야 함)</td>' + L.map(function (d) { return '<td>' + (anFin(d.r4chk) ? (Math.abs(d.r4chk) < 1e-9 ? '0' : d.r4chk.toExponential(1)) : 'N/A') + '</td>'; }).join('') + '</tr>';
  const table = '<div class="tscroll" style="max-height:60vh;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
  const note = '<div style="font-size:11.5px; color:var(--text-muted); margin-top:6px; line-height:1.5;">해석 시 주의: ③은 (영업이익/매출총이익)×(1−유효세율)이라 세금 영향이 섞입니다. 성장 투자(판관비·R&D)가 큰 회사는 ①이 높고 ③이 낮게 나올 수 있으나 <b>이것만으로 우량 성장주라고 단정할 수는 없고</b>, 투자 성과(매출 성장·시간에 따른 ③의 회복)를 함께 봐야 합니다. ④는 재무 레버리지가 아니라 총자산 중 영업에 투입된 자본의 비중이며, 현금·금융자산이 많거나 매입채무 등 무이자 영업부채가 클수록 커집니다. 매출총이익을 알 수 없는 회사는 N/A입니다.</div>';
  return anSection('🧩 ROIC 4단계 분해 (시계열)', '#0f766e', desc, table + '<div id="anR4Chart"></div>' + note, true);
}
function anR4Toggle(key) {
  const i = AN_R4SEL.indexOf(key);
  if (i >= 0) AN_R4SEL.splice(i, 1); else { if (AN_R4SEL.length >= 4) AN_R4SEL.shift(); AN_R4SEL.push(key); }
  anR4Draw();
}
function anR4Draw() {
  const c = AN_CTX; const box = document.getElementById('anR4Chart'); if (!box || !c) return;
  const L = c.list.slice(-10);
  document.querySelectorAll('tr[data-r4]').forEach(function (tr) { tr.style.background = AN_R4SEL.indexOf(tr.getAttribute('data-r4')) >= 0 ? 'rgba(15,118,110,0.10)' : ''; });
  if (!AN_R4SEL.length) { box.innerHTML = '<div style="font-size:12px; color:var(--text-muted); margin:8px 0;">위 표에서 행 이름을 클릭하면 추이 차트가 여기에 표시됩니다(최대 4개).</div>'; return; }
  const W = 640, H = 150, pl = 48, pr = 14, pt = 14, pb = 24;
  box.innerHTML = AN_R4SEL.map(function (key) {
    const rw = AN_R4ROWS.filter(function (x) { return x.key === key; })[0];
    const vals = L.map(function (d) { return anFin(d[key]) ? d[key] : null; });
    const fin = vals.filter(function (v) { return v != null; });
    if (!fin.length) return '<div style="font-size:12px; margin:8px 0;"><b>' + rw.label + '</b>: 표시할 값이 없습니다.</div>';
    let mn = Math.min.apply(null, fin), mx = Math.max.apply(null, fin); if (mn === mx) { mn -= Math.abs(mn) * 0.1 + 0.01; mx += Math.abs(mx) * 0.1 + 0.01; }
    const padv = (mx - mn) * 0.12; mn -= padv; mx += padv;
    const X = function (i) { return pl + (L.length > 1 ? i * (W - pl - pr) / (L.length - 1) : (W - pl - pr) / 2); };
    const Y = function (v) { return pt + (mx - v) * (H - pt - pb) / (mx - mn); };
    let svg = '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%; max-width:680px; background:var(--card,#fff); border:1px solid var(--border); border-radius:8px;">';
    [mn + padv, (mn + mx) / 2, mx - padv].forEach(function (g) { svg += '<line x1="' + pl + '" x2="' + (W - pr) + '" y1="' + Y(g).toFixed(1) + '" y2="' + Y(g).toFixed(1) + '" stroke="#e5e7eb" stroke-width="1"/><text x="' + (pl - 5) + '" y="' + (Y(g) + 4).toFixed(1) + '" font-size="10" text-anchor="end" fill="#6b7280">' + anFmt(rw.fmt, g) + '</text>'; });
    if (mn < 0 && mx > 0) svg += '<line x1="' + pl + '" x2="' + (W - pr) + '" y1="' + Y(0).toFixed(1) + '" y2="' + Y(0).toFixed(1) + '" stroke="#9ca3af" stroke-dasharray="3 3"/>';
    let seg = [];
    const flush = function () { if (seg.length > 1) svg += '<polyline fill="none" stroke="' + rw.col + '" stroke-width="2" points="' + seg.join(' ') + '"/>'; seg = []; };
    L.forEach(function (d, i) { if (vals[i] == null) flush(); else seg.push(X(i).toFixed(1) + ',' + Y(vals[i]).toFixed(1)); }); flush();
    L.forEach(function (d, i) {
      svg += '<text x="' + X(i).toFixed(1) + '" y="' + (H - 7) + '" font-size="10" text-anchor="middle" fill="#6b7280">' + String(d.year).slice(2) + '</text>';
      if (vals[i] != null) svg += '<circle cx="' + X(i).toFixed(1) + '" cy="' + Y(vals[i]).toFixed(1) + '" r="3.2" fill="' + rw.col + '"><title>' + d.year + ': ' + anFmt(rw.fmt, vals[i]) + '</title></circle><text x="' + X(i).toFixed(1) + '" y="' + (Y(vals[i]) - 7).toFixed(1) + '" font-size="9.5" text-anchor="middle" fill="' + rw.col + '">' + anFmt(rw.fmt, vals[i]) + '</text>';
    });
    svg += '</svg>';
    return '<div style="margin:10px 0 2px; font-size:12.5px; font-weight:600; color:' + rw.col + ';">' + rw.label + ' <span style="font-weight:400; color:var(--text-muted);">— ' + rw.tip + '</span> <button type="button" onclick="anR4Toggle(' + "'" + key + "'" + ')" style="font-size:11px; padding:0 6px; height:auto;">닫기 ×</button></div>' + svg;
  }).join('');
}

function anRenderValuation(c) {
  const v = c.vs, rg = (x) => x ? ('최저 ' + anFmt(x.k, x.lo) + ' / 중앙 ' + anFmt(x.k, x.med) + ' / 최고 ' + anFmt(x.k, x.hi)) : 'N/A';
  const rows = [['PER', 'x', v.rangePer, v.per], ['PBR', 'x', v.rangePbr, v.pbr], ['FCF Yield', 'pct', v.rangeFcfy, v.fcfYield]];
  const rtab = '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th>지표</th><th>현재</th><th>5Y 범위(공시시점)</th><th>5Y 분위</th><th>10Y 범위(공시시점)</th><th>10Y 분위</th></tr></thead><tbody>' +
    rows.map(([n, k, r5, cv]) => { const f = (x) => x ? ('최저 ' + anFmt(k, x.lo) + ' · 중앙 ' + anFmt(k, x.med) + ' · 최고 ' + anFmt(k, x.hi) + ' (n=' + x.n + ')') : 'N/A'; const pos = (x) => x && anFin(x.pct) ? anPct(x.pct, 0) : 'N/A'; return '<tr><td class="an-first">' + n + '</td><td>' + anFmt(k, cv) + '</td><td>' + f(r5.y5) + '</td><td>' + pos(r5.y5) + '</td><td>' + f(r5.y10) + '</td><td>' + pos(r5.y10) + '</td></tr>'; }).join('') + '</tbody></table></div>';
  const tiles = anGrid([
    anTile('PER', anX(v.per), 'EPS(' + c.m.epsBasis + ') 기준'), anTile('PBR', anX(v.pbr)), anTile('Earnings Yield', anPct(v.earningsYield, 2), '= EPS ÷ 주가 (1/PER)'),
    anTile('Normalized PER', anX(v.normPer), '시총 ÷ 최근 ' + v.normN + '개년 평균 순이익 (Graham-Dodd/Shiller 방식, 물가 미조정)'),
    anTile('EV/EBIT', anX(v.evEbit), v.ev == null ? '재조회 필요(차입금·현금 항목)' : 'EV ' + anEok(v.ev) + ' (' + v.basis + ', 순차입금 ' + anEok(v.netDebt) + ' = 차입금 − 현금·단기금융상품, 장기 금융자산은 차감 안 함; 리스부채 ' + anEok(v.lease) + ' 포함' + (v.leaseFull ? '' : ' — 비유동리스부채는 재조회 후 반영') + ')' + (anFin(v.finAssets) && v.finAssets > 0 ? ' · 참고: EV에서 차감하지 않은 금융자산(단기매매·당기손익FV·기타포괄FV) ' + anEok(v.finAssets) + (anFin(v.mcap) && v.mcap > 0 ? ' = 시총의 ' + anPct(v.finAssets / v.mcap, 0) : '') : '')), anTile('EV/EBITDA', anX(v.evEbitda), v.ebitda == null ? (v.refetched ? 'DART 현금흐름표에 감가상각 줄이 없는 회사라 계산 불가(주석 공시)' : '재조회 필요(감가상각 항목)') : 'EBITDA ' + anEok(v.ebitda) + ' = 영업이익+감가상각+무형자산상각'),
    anTile('FCF Yield', anPct(v.fcfYield, 2), 'FCF(' + v.basis + ') ÷ 시가총액'),
    anTile('PEG', anNum(v.peg), v.g3 != null ? 'PER ÷ (3년 EPS CAGR ' + anPct(v.g3) + ')' : '3년 성장률 산출 불가(적자/음수)'),
  ]);
  const imp = anGrid([
    anTile('Implied Growth (이익 기준)', anPct(c.impE), '현재 주가가 요구하는 영구성장률. 가치동인 모형 P=E₀(1+g)(1−g/ROE)/(r−g), ROE=' + anPct(c.base0) + ', r=' + anPct(c.r)),
    anTile('Implied Growth (FCF 기준)', anPct(c.impF), '고든모형 g=(P·r−F₀)/(P+F₀), F₀=최근 3년 평균 FCF/주'),
    anTile('Expected Growth (ROE×유보율)', anPct(c.gRoe), '지속가능성장률(Higgins 1977), ROE 3Y 평균 × (1−배당성향 ' + anPct(c.payoutAvg, 0) + ')'),
    anTile('Expected Growth (ROIC×재투자율)', anPct(c.gRoic), 'Damodaran 펀더멘털 성장률, 재투자율=ΔIC/NOPAT 3Y 평균 ' + anPct(c.reinv3, 0)),
  ]);
  return anSection('💹 Valuation (배수 · 과거 범위 · 내재/기대 성장)', '#2563eb',
    '※ Industry valuation(업종 비교)은 업종 분류·동종 비교 데이터가 DB에 없어 <b>제외</b>했습니다. 5Y/10Y 범위는 저장된 "연간 공시시점 PER·PBR·FCF Yield"의 분포이며 "분위"는 과거 값 중 현재 값 이하인 비율입니다(0%=역대 최저, 100%=역대 최고 — PER·PBR은 낮을수록, FCF Yield는 높을수록 저렴). Implied &gt; Expected이면 시장이 펀더멘털보다 큰 성장을 가격에 넣은 것입니다. <br>※ 정의 차이 안내: 이 앱의 EPS·BPS·PER은 <b>지배주주 기준 · 자사주 차감 유통주식수(우선주 포함)</b>이며, 포털(발행주식수 기준 EPS 등)·DART(가중평균주식수 EPS)와 값이 다를 수 있습니다(오류가 아닌 정의 차이). CAPEX는 유형자산(+무형자산) 취득액이라 포털의 CAPEX 정의와 다를 수 있습니다. 주식수 출처가 &quot;이월&quot;인 분기는 직전 공시값이라 다소 부정확하며, 연간은 이월하지 않고 &quot;미확인&quot;으로 표시합니다.',
    tiles + imp + rtab, true);
}

function anRenderAreas(c) {
  const L = c.list; const out = [];
  // Growth
  {
    const kinds = [['revenue', '매출액'], ['gp', '매출총이익'], ['ebit', '영업이익'], ['eps', 'EPS'], ['fcf', 'FCF']];
    const cagrRows = kinds.map(([k, n]) => '<tr><td class="an-first">' + n + '</td><td>' + anPct(anCagr(L, k, 3)) + '</td><td>' + anPct(anCagr(L, k, 5)) + '</td><td>' + anPct(anCagr(L, k, 10)) + '</td><td>' + anPct(L[L.length - 1][k + 'Yoy']) + '</td></tr>').join('');
    const splitWarn = L.some((d) => anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3) ? '<div style="font-size:11.5px; color:#b45309; margin-top:4px;">⚠ 유통주식수가 한 해에 30% 넘게 변한 해가 있습니다(액면분할·증자 등). EPS 성장률이 왜곡될 수 있으니 순이익 기준도 함께 보세요.</div>' : '';
    const qg = c.qg;
    const qtab = qg ? '<div style="font-weight:600; font-size:12.5px; margin-top:10px;">최근 3개 분기 YoY와 가속 여부</div><div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">항목</th>' + qg.labels.map((x) => '<th>' + x + '</th>').join('') + '<th>가속도</th></tr></thead><tbody>' +
      qg.rows.map((x) => '<tr><td class="an-first">' + x.name + '</td>' + x.g.map((v) => '<td>' + anPct(v) + '</td>').join('') + '<td style="font-weight:700; color:' + (x.accel === '가속' ? '#16a34a' : x.accel === '둔화' ? '#dc2626' : 'inherit') + ';">' + (x.accel || 'N/A') + '</td></tr>').join('') + '</tbody></table></div><div style="font-size:11px; color:var(--text-muted);">가속도: 최신 분기 YoY가 직전 두 분기 평균보다 3%p 넘게 높으면 가속, 낮으면 둔화, 아니면 유지. 전년 동기가 0 이하인 경우 N/A.</div>' : '';
    out.push(anSection('🌱 Growth (성장)', '#ea580c', 'CAGR은 최근 연간 값과 N년 전 연간 값이 모두 양수일 때만 계산합니다(적자 구간 N/A). 성장 가속은 이익의 모멘텀(Chan·Jegadeesh·Lakonishok 1996 earnings momentum)과 연결됩니다.',
      '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">항목</th><th>3Y CAGR</th><th>5Y CAGR</th><th>10Y CAGR</th><th>최근 YoY</th></tr></thead><tbody>' + cagrRows + '</tbody></table></div>' + splitWarn + qtab +
      '<div style="font-weight:600; font-size:12.5px; margin-top:10px;">연도별 YoY</div>' + anTable(L, kinds.map(([k, n]) => ({ label: n + ' YoY', key: k + 'Yoy', fmt: 'pct' })), 10), false));
  }
  // Quality
  out.push(anSection('🏆 Quality (수익성·이익의 질)', '#7c3aed', 'Gross Profitability(Novy-Marx 2013), 발생액(Sloan 1996: 현금 뒷받침 없는 이익은 지속성이 낮음), ROE/ROIC 평균·변동. ROIC = 영업이익×(1−유효세율; 범위 밖이면 24%) ÷ 평균 투하자본(영업운전자본 + 유형·무형·사용권자산 + 영업권, 차입금·현금 제외).',
    anRoeStats(c) + anGrid([
      anTile('ROIC 10Y 평균', anPct(anMean(anWin(L, 'roic', 10))), anPeriodTxt(anPeriod(L, 'roic', 10))), anTile('ROIC 5Y 평균', anPct(c.roic5), anPeriodTxt(anPeriod(L, 'roic', 5))), anTile('ROIC 3Y 평균', anPct(c.roic3), anPeriodTxt(anPeriod(L, 'roic', 3))),
    ]) + anTable(L, [
      { label: 'ROE', key: 'roe', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }, { label: 'Gross Profitability (매출총이익/총자산)', key: 'gpa', fmt: 'pct' },
      { label: '영업이익률', key: 'opm', fmt: 'pct' }, { label: 'FCF 마진', key: 'fcfm', fmt: 'pct' }, { label: 'FCF/순이익', key: 'fcfni', fmt: 'x' }, { label: 'CFO/순이익', key: 'cfoni', fmt: 'x' },
      { label: '발생액(Accrual, 낮을수록 좋음)', key: 'accrual', fmt: 'pct' }, { label: '자산회전율', key: 'at', fmt: 'x' }], 10), false));
  // Capital efficiency
  const lastD = c.last; const ipR = (lastD && lastD.r && anFin(lastD.r.investment_property) && anFin(lastD.ic) && lastD.ic > 0) ? lastD.r.investment_property / lastD.ic : null;
  const ipWarn = (ipR != null && ipR >= 0.1) ? ' <b style="color:#b45309;">⚠ 투자부동산이 투하자본의 ' + anPct(ipR, 0) + '에 해당합니다. 투하자본(ROIC 분모)에서는 영업 외 자산으로 보아 제외했는데, 임대수익이 영업이익에 들어 있는 회사라면 ROIC가 과대 계산됩니다.</b>' : '';
  out.push(anSection('⚙ Capital Efficiency (자본 효율 · 증분 수익)', '#0d9488', '"추가로 넣은 1원이 얼마를 버는가". 증분 ROIC = 최근 3년 ΔNOPAT ÷ 같은 기간(1년 선행) ΔInvested Capital (Mauboussin·Koller 방식). 투하자본이 늘지 않은 해는 N/A. 재투자율 = ΔIC ÷ NOPAT, 성장 = 재투자율 × ROIC (Damodaran). 투하자본은 영업 관점(영업운전자본 + 유형·무형·사용권자산 + 영업권)이며 현금·금융자산·투자부동산은 영업 외 자산으로 보아 제외합니다(Koller et al., Valuation).' + ipWarn,
    anTable(L, [{ label: 'NOPAT', key: 'nopat', fmt: 'eok' }, { label: '투하자본(IC)', key: 'ic', fmt: 'eok' }, { label: 'ΔIC (전년 대비)', key: 'dIC', fmt: 'eok' }, { label: '재투자율 (ΔIC/NOPAT)', key: 'reinv', fmt: 'pct' },
      { label: '증분 ROIC (3Y)', key: 'incRoic', fmt: 'pct' }, { label: '증분 ROE (3Y, ΔNI/Δ자본)', key: 'incRoe', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }], 10), false));
  // Cash quality
  out.push(anSection('💵 Cash Quality (현금흐름의 질)', '#0891b2', 'FCF = 영업CF − CAPEX(유형+무형 취득). 운전자본 = (매출채권+기타채권+재고) − (매입채무+기타채무).',
    anTable(L, [{ label: 'CFO (영업CF)', key: 'ocf', fmt: 'eok' }, { label: 'FCF', key: 'fcf', fmt: 'eok' }, { label: 'FCF 마진', key: 'fcfm', fmt: 'pct' }, { label: 'FCF/순이익', key: 'fcfni', fmt: 'x' }, { label: 'CFO/순이익', key: 'cfoni', fmt: 'x' },
      { label: '발생액(Accrual)', key: 'accrual', fmt: 'pct' }, { label: 'CAPEX', key: 'capex', fmt: 'eok' }, { label: 'CAPEX/매출', key: 'capexRev', fmt: 'pct' }, { label: '운전자본', key: 'wc', fmt: 'eok' }, { label: '운전자본/매출', key: 'wcRev', fmt: 'pct' }], 10), false));
  // BS
  out.push(anSection('🛡 Balance Sheet (재무 안정성)', '#475569', '차입금 = 단기차입금 + 유동성장기부채 + 장기차입금 + 사채(리스부채 제외). 순차입금 = 차입금 − (현금 + 단기금융상품). 차입금·EBITDA 항목은 <b>재조회 후</b>에 채워집니다.',
    anTable(L, [{ label: '부채비율 (부채/자본)', key: 'de', fmt: 'pct' }, { label: '차입금/자본', key: 'ibde', fmt: 'pct' }, { label: '차입금', key: 'borrow', fmt: 'eok' }, { label: '순차입금', key: 'netDebt', fmt: 'eok' },
      { label: '순차입금/EBITDA', key: 'nde', fmt: 'x' }, { label: '이자보상배율', key: 'icov', fmt: 'x' }, { label: '유동비율', key: 'cur', fmt: 'x' }, { label: '현금/차입금', key: 'cashDebt', fmt: 'x' }], 10), false));
  // Capital allocation
  out.push(anSection('🏦 Capital Allocation (자본 배분)', '#b45309', '이익을 배당·자사주·투자(CAPEX·R&D 대용) 중 어디에 쓰는가. (M&A·차입금 상환은 공시 계정명이 회사마다 달라 정확히 구분할 수 없어 뺐습니다.) 금액은 현금흐름표 기준(재조회 후 채워짐). 주식수 변화가 음수면 자사주 소각/매입으로 주식수가 줄어든 것(주주환원 효과, Ikenberry et al. 1995).',
    anGrid([anTile('평균 배당성향', anPct(c.m.avgPayout), '기간: ' + anPeriodTxt(anPeriod(L, 'payout', 10)).replace('기간: ', ''))]) +
    anTable(L, [{ label: '배당성향 (DPS/EPS)', key: 'payout', fmt: 'pct' }, { label: '배당금 지급', key: 'divPaid', fmt: 'eok' }, { label: '자사주 매입', key: 'buyback', fmt: 'eok' }, { label: '총주주환원성향 (배당+자사주)/순이익', key: 'shp', fmt: 'pct' },
      { label: '유통주식수 변화(YoY)', key: 'shareChg', fmt: 'pct' }, { label: 'CAPEX', key: 'capex', fmt: 'eok' }, { label: 'CAPEX/감가상각', key: 'capexDa', fmt: 'x' },
      { label: 'R&D 대용(무형자산 취득)', key: 'rd', fmt: 'eok' }], 10), false));
  return out.join('');
}

// ================= 백테스트 엔진 (순수 함수) =================
// 방법론: 횡단면 분위(quantile) 포트폴리오 정렬(Fama-French 방식) + 정보계수(IC, Spearman) + Newey-West t통계량
//        + 다중검정 보정(Harvey-Liu-Zhu 2016, Bailey-Lopez de Prado 2014 Deflated Sharpe 근사) + 표본 분할(전반/후반) + 거래비용 반영.
function btMean(a) { const x = a.filter((v) => typeof v === 'number' && isFinite(v)); return x.length ? x.reduce((s, v) => s + v, 0) / x.length : null; }
function btSd(a) { const x = a.filter((v) => typeof v === 'number' && isFinite(v)); if (x.length < 2) return null; const m = btMean(x); return Math.sqrt(x.reduce((s, v) => s + (v - m) * (v - m), 0) / (x.length - 1)); }
function btRankAvg(vals) {
  const idx = vals.map((v, i) => [v, i]).sort((a, b) => a[0] - b[0]);
  const r = new Array(vals.length);
  let i = 0;
  while (i < idx.length) { let j = i; while (j + 1 < idx.length && idx[j + 1][0] === idx[i][0]) j++; const avg = (i + j) / 2 + 1; for (let k = i; k <= j; k++) r[idx[k][1]] = avg; i = j + 1; }
  return r;
}
function btPearson(x, y) {
  const n = x.length; if (n < 3) return null;
  const mx = btMean(x), my = btMean(y); let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < n; i++) { sxy += (x[i] - mx) * (y[i] - my); sxx += (x[i] - mx) * (x[i] - mx); syy += (y[i] - my) * (y[i] - my); }
  return (sxx > 0 && syy > 0) ? sxy / Math.sqrt(sxx * syy) : null;
}
function btSpearman(x, y) { return btPearson(btRankAvg(x), btRankAvg(y)); }
function btNormCdf(x) { // Abramowitz-Stegun 7.1.26
  const t = 1 / (1 + 0.3275911 * Math.abs(x) / Math.SQRT2);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x / 2);
  return x >= 0 ? 0.5 * (1 + y) : 0.5 * (1 - y);
}
function btNormInv(p) { // Acklam 근사
  if (!(p > 0 && p < 1)) return p <= 0 ? -Infinity : Infinity;
  const a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
  const b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01];
  const c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
  const d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00];
  const pl = 0.02425; let q, r;
  if (p < pl) { q = Math.sqrt(-2 * Math.log(p)); return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  if (p > 1 - pl) { q = Math.sqrt(-2 * Math.log(1 - p)); return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1); }
  q = p - 0.5; r = q * q;
  return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
}
// Newey-West(1987) 보정 평균의 t통계량. lag는 Newey-West(1994) 규칙 floor(4(T/100)^(2/9)).
function btNeweyWest(x) {
  const T = x.length; if (T < 4) return null;
  const m = btMean(x); const e = x.map((v) => v - m);
  const lag = Math.min(T - 2, Math.max(0, Math.floor(4 * Math.pow(T / 100, 2 / 9))));
  const g = (l) => { let s = 0; for (let t = l; t < T; t++) s += e[t] * e[t - l]; return s / T; };
  let om = g(0); for (let l = 1; l <= lag; l++) om += 2 * (1 - l / (lag + 1)) * g(l);
  if (!(om > 0)) om = g(0);
  const se = Math.sqrt(om / T);
  return se > 0 ? { t: m / se, lag } : null;
}
function btMoments(x) {
  const n = x.length; if (n < 4) return { skew: 0, kurt: 3 };
  const m = btMean(x); let m2 = 0, m3 = 0, m4 = 0;
  x.forEach((v) => { const d = v - m; m2 += d * d; m3 += d * d * d; m4 += d * d * d * d; });
  m2 /= n; m3 /= n; m4 /= n;
  return m2 > 0 ? { skew: m3 / Math.pow(m2, 1.5), kurt: m4 / (m2 * m2) } : { skew: 0, kurt: 3 };
}
// Deflated Sharpe Ratio 근사(Bailey-Lopez de Prado 2014). sr = 기간당(비연율) 샤프, T = 기간 수, nTrials = 지금까지 시험한 조합 수.
function btDeflated(sr, T, x, nTrials) {
  if (!(T > 3) || sr == null) return null;
  const mo = btMoments(x);
  const den = Math.sqrt(Math.max(1e-12, 1 - mo.skew * sr + (mo.kurt - 1) / 4 * sr * sr));
  const se = den / Math.sqrt(T - 1);
  const gam = 0.5772156649;
  const sr0 = nTrials > 1 ? se * ((1 - gam) * btNormInv(1 - 1 / nTrials) + gam * btNormInv(1 - 1 / (nTrials * Math.E))) : 0;
  return { psr: btNormCdf((sr - sr0) / se), sr0, nTrials };
}
function btStat(series, ppy) {
  const x = series.filter((v) => typeof v === 'number' && isFinite(v));
  const n = x.length; if (!n) return null;
  const m = btMean(x), sd = btSd(x);
  const nw = btNeweyWest(x);
  let cum = 1, peak = 1, mdd = 0;
  x.forEach((v) => { cum *= (1 + v); if (cum > peak) peak = cum; const dd = cum / peak - 1; if (dd < mdd) mdd = dd; });
  return { n, mean: m, sd, t: (sd && n > 1) ? m / (sd / Math.sqrt(n)) : null, tNW: nw ? nw.t : null, lag: nw ? nw.lag : null,
    sharpe: sd ? (m / sd) * Math.sqrt(ppy) : null, srPeriod: sd ? m / sd : null, hit: x.filter((v) => v > 0).length / n, cum: cum - 1, mdd, ann: Math.pow(Math.max(1e-9, cum), ppy / n) - 1 };
}
// periods: [{label, items:[{id, s, f}], bench}] (오래된 순). 높은 점수 = 높은 그룹.
function btRun(periods, o) {
  const G = o.G || 5, minN = o.minN || (3 * G), cost = o.cost || 0, ppy = o.ppy || 1;
  const rows = [], skipped = [];
  let prevTop = null;
  periods.forEach((p) => {
    let it = p.items.filter((x) => typeof x.s === 'number' && isFinite(x.s) && typeof x.f === 'number' && isFinite(x.f));
    const before = it.length;
    if (o.trimLo != null) it = it.filter((x) => x.f > o.trimLo);
    if (o.trimHi != null) it = it.filter((x) => x.f < o.trimHi);
    const trimmed = before - it.length;
    if (it.length < minN) { skipped.push({ label: p.label, n: it.length }); return; }
    it.sort((a, b) => (a.s - b.s) || (a.id < b.id ? -1 : 1));
    const n = it.length, groups = []; for (let g = 0; g < G; g++) groups.push([]);
    it.forEach((x, i) => { groups[Math.min(G - 1, Math.floor(i * G / n))].push(x); });
    const gm = groups.map((gr) => btMean(gr.map((x) => x.f)));
    const uni = btMean(it.map((x) => x.f));
    const ic = btSpearman(it.map((x) => x.s), it.map((x) => x.f));
    const topIds = new Set(groups[G - 1].map((x) => x.id));
    let turn = 1;
    if (prevTop) { let keep = 0; topIds.forEach((id) => { if (prevTop.has(id)) keep++; }); turn = 1 - keep / topIds.size; }
    prevTop = topIds;
    const topNet = gm[G - 1] - turn * cost;
    rows.push({ label: p.label, n, trimmed, gm, uni, ic, spread: gm[G - 1] - gm[0], excess: gm[G - 1] - uni, excessNet: topNet - uni, topNet, turn, bench: p.bench });
  });
  const T = rows.length;
  const res = { rows, skipped, T, G, ppy, cost };
  if (!T) return res;
  res.groupStats = []; for (let g = 0; g < G; g++) res.groupStats.push(btStat(rows.map((r) => r.gm[g]), ppy));
  res.uniStat = btStat(rows.map((r) => r.uni), ppy);
  res.spreadStat = btStat(rows.map((r) => r.spread), ppy);
  res.excessStat = btStat(rows.map((r) => r.excess), ppy);
  res.excessNetStat = btStat(rows.map((r) => r.excessNet), ppy);
  res.topNetStat = btStat(rows.map((r) => r.topNet), ppy);
  res.icStat = btStat(rows.map((r) => r.ic), ppy);
  res.avgN = btMean(rows.map((r) => r.n)); res.avgTurn = btMean(rows.slice(1).map((r) => r.turn));
  res.mono = btSpearman(res.groupStats.map((_, i) => i), res.groupStats.map((s) => s ? s.mean : 0));
  const half = Math.floor(T / 2);
  res.split = T >= 6 ? { first: btMean(rows.slice(0, half).map((r) => r.spread)), second: btMean(rows.slice(half).map((r) => r.spread)), nFirst: half, nSecond: T - half } : null;
  const benchRows = rows.filter((r) => typeof r.bench === 'number');
  res.benchMean = benchRows.length ? btMean(benchRows.map((r) => r.bench)) : null;
  return res;
}

// ================= 종목분석 기준 설정 (재무분석 방법 변경) =================
const AN_CFG = { qm: 'fscore' };
const AN_CFG_DEF = { roicSpread: 0.03, pioLow: 3, growthLow: 0.05, growthHigh: 0.10, growthVeryHigh: 0.15, perGraham: 15, pbrGraham: 1.5 };
function anCfgSave() {
  const th = {}; Object.keys(AN_CFG_DEF).forEach((k) => { th[k] = AN_TH[k]; });
  try { localStorage.setItem('fsv_an_cfg', JSON.stringify({ qm: AN_CFG.qm, th })); } catch (e) { /* 저장 불가 환경 */ }
}
function anCfgLoad() {
  try {
    const o = JSON.parse(localStorage.getItem('fsv_an_cfg') || 'null');
    if (!o) return;
    if (o.qm === 'legacy4' || o.qm === 'fscore') AN_CFG.qm = o.qm;
    if (o.th) Object.keys(AN_CFG_DEF).forEach((k) => { if (typeof o.th[k] === 'number' && isFinite(o.th[k])) AN_TH[k] = o.th[k]; });
  } catch (e) { /* 무시 */ }
}
function anCfgSet(k, v) {
  if (k === 'qm') AN_CFG.qm = v === 'legacy4' ? 'legacy4' : 'fscore';
  else {
    let x = Number(v); if (!isFinite(x) || x < 0) return;
    if (['roicSpread', 'growthLow', 'growthHigh', 'growthVeryHigh'].indexOf(k) >= 0) x = x / 100;
    const old = AN_TH[k]; AN_TH[k] = x;
    if (!(AN_TH.growthLow <= AN_TH.growthHigh && AN_TH.growthHigh <= AN_TH.growthVeryHigh)) { AN_TH[k] = old; alert('Growth 기준은 낮음 ≤ 높음 ≤ 매우 높음 순서여야 합니다.'); }
  }
  anCfgSave(); if (rawRows.length) renderAnalysis();
}
function anCfgReset() { AN_CFG.qm = 'fscore'; Object.keys(AN_CFG_DEF).forEach((k) => { AN_TH[k] = AN_CFG_DEF[k]; }); anCfgSave(); if (rawRows.length) renderAnalysis(); }
function anRenderSettings() {
  const changed = AN_CFG.qm !== 'fscore' || Object.keys(AN_CFG_DEF).some((k) => AN_TH[k] !== AN_CFG_DEF[k]);
  const num = (k, label, pct, hint) => '<label style="display:block; margin:4px 0; font-size:12.5px;">' + label + ' <input type="number" step="' + (pct ? 1 : 0.5) + '" value="' + (pct ? +(AN_TH[k] * 100).toFixed(2) : AN_TH[k]) + '" onchange="anCfgSet(' + "'" + k + "'" + ', this.value)" style="width:70px;" /> ' + (pct ? '%' : '') + ' <span style="color:var(--text-muted); font-size:11px;">' + hint + '</span></label>';
  const inner = '<div style="font-size:12px; line-height:1.55; margin:4px 0;">종목분석의 등급 기준을 바꿀 수 있습니다. 바꾼 값은 이 브라우저에 저장되고 즉시 모든 분류에 반영됩니다. 어떤 값이 더 나은지는 「⑤ 백테스트」로 확인하세요(기준을 바꿔 가며 고르면 과최적화 위험이 있어 백테스트가 시도 횟수를 세어 보정합니다).</div>' +
    '<label style="display:block; margin:4px 0; font-size:12.5px;">Quality 판정 방식 <select onchange="anCfgSet(' + "'qm'" + ', this.value)"><option value="fscore"' + (AN_CFG.qm === 'fscore' ? ' selected' : '') + '>Piotroski F-Score (학술 검증)</option><option value="legacy4"' + (AN_CFG.qm === 'legacy4' ? ' selected' : '') + '>기존 4점(ROIC·ROE·FCF/NI·발생액, 임의 기준)</option></select></label>' +
    num('pioLow', 'F-Score 미흡 상한', false, '이 점수 이하면 미흡 (논문의 극단 구간은 0~1)') +
    num('roicSpread', 'ROIC 높음 여유폭', true, 'ROIC ≥ 자본비용 + 이 값이면 높음') +
    num('growthLow', 'Growth 낮음 미만', true, '임의 기준') + num('growthHigh', 'Growth 높음 이상', true, '임의 기준') + num('growthVeryHigh', 'Growth 매우 높음 이상', true, '임의 기준') +
    num('perGraham', 'PER 저평가 상한', false, 'Graham 15') + num('pbrGraham', 'PBR 저평가 상한', false, 'Graham 1.5') +
    '<div class="row" style="margin-top:6px;"><button onclick="anCfgReset()">기본값으로 되돌리기</button></div>';
  return anSection('⚙ 분석 기준 설정 (재무분석 방법 변경)' + (changed ? ' — 기본값에서 변경됨' : ''), '#0369a1', '', inner, false);
}
anCfgLoad();

// ================= 백테스트: 데이터 준비 · 화면 =================
const BT_FACTORS = {
  fscore: { name: 'Piotroski F-Score (재무 건전성)', badge: 'proof', ev: 'Piotroski(2000)', fn: (L, d) => { const p = anPiotroski(L); return (p && p.known >= 8) ? p.score : null; } },
  gpa: { name: '매출총이익/총자산 (Gross Profitability)', badge: 'proof', ev: 'Novy-Marx(2013)', fn: (L, d) => d.gpa },
  lowaccr: { name: '낮은 발생액 (이익의 질)', badge: 'proof', ev: 'Sloan(1996)', fn: (L, d) => anFin(d.accrual) ? -d.accrual : null },
  ep: { name: '이익수익률 EP = EPS/주가 (가치)', badge: 'proof', ev: 'Basu(1977), Fama-French(1992)', fn: (L, d, P) => (anFin(d.eps) && P > 0) ? d.eps / P : null },
  bp: { name: '장부가/주가 BP (가치)', badge: 'proof', ev: 'Fama-French(1992·1993)', fn: (L, d, P) => (anFin(d.equity) && anFin(d.shares) && d.shares > 0 && P > 0) ? d.equity / d.shares / P : null },
  fcfy: { name: 'FCF수익률 = 주당FCF/주가', badge: 'proof', ev: 'Lakonishok-Shleifer-Vishny(1994)', fn: (L, d, P) => (anFin(d.fcf) && anFin(d.shares) && d.shares > 0 && P > 0) ? d.fcf / d.shares / P : null },
  graham: { name: 'Graham 복합 (낮은 PER×PBR)', badge: 'practice', ev: 'Graham(1949)', fn: (L, d, P) => { const e = d.eps, b = (anFin(d.equity) && anFin(d.shares) && d.shares > 0) ? d.equity / d.shares : null; return (e > 0 && b > 0 && P > 0) ? -((P / e) * (P / b)) : null; } },
  roic: { name: 'ROIC 3년 평균', badge: 'practice', ev: 'Greenblatt(2005) 등 실무', fn: (L) => anMean(anWin(L, 'roic', 3)) },
  growth: { name: 'Growth (3년 CAGR 평균, 이 앱 정의)', badge: 'assume', ev: '성장 지속성은 약함(Chan-Karceski-Lakonishok 2003)', fn: (L) => { const e = anCagr(L, 'eps', 3) != null ? anCagr(L, 'eps', 3) : anCagr(L, 'ni', 3); const g = [anCagr(L, 'revenue', 3), anCagr(L, 'ebit', 3), e].filter(anFin); return g.length >= 2 ? anMean(g) : null; } },
  legacy4: { name: '기존 Quality 4점(ROIC·ROE·FCF/NI·발생액)', badge: 'assume', ev: '이 앱의 이전 방식(임의 기준선)', fn: (L) => { const q = [anMean(anWin(L, 'roic', 3)) >= AN_TH.roicGood, anMean(anWin(L, 'roe', 3)) >= AN_TH.roeGood, anMean(anWin(L, 'fcfni', 3)) >= AN_TH.fcfNiGood, anMean(anWin(L, 'accrual', 3)) <= AN_TH.accrualMax]; const k = [anMean(anWin(L, 'roic', 3)), anMean(anWin(L, 'roe', 3)), anMean(anWin(L, 'fcfni', 3)), anMean(anWin(L, 'accrual', 3))].filter((v) => v != null).length; return k >= 3 ? q.filter(Boolean).length : null; } },
};
const BT = { stocks: null, loading: false, trialsKey: 'fsv_bt_trials', last: null };
function btTrials(sig) {
  let arr = [];
  try { arr = JSON.parse(localStorage.getItem(BT.trialsKey) || '[]'); } catch (e) { arr = []; }
  if (sig && arr.indexOf(sig) < 0) { arr.push(sig); try { localStorage.setItem(BT.trialsKey, JSON.stringify(arr)); } catch (e) { /* 저장 불가 */ } }
  return Math.max(1, arr.length);
}
function btResetTrials() { try { localStorage.removeItem(BT.trialsKey); } catch (e) { /* 무시 */ } btMsg('시도 기록을 지웠습니다.'); }
function btMsg(t) { const el = document.getElementById('btStatus'); if (el) el.textContent = t; }

async function btLoadStocks() {
  if (BT.loading) return;
  BT.loading = true;
  try {
    btMsg('저장된 종목 목록을 확인하는 중...');
    const r0 = await fetch('/api/db/companies'); const d0 = await r0.json();
    const comps = d0.companies || [];
    if (!comps.length) { btMsg('저장된 재무 데이터가 없습니다. 먼저 종목을 조회·저장해 주세요.'); return; }
    const by = {};
    for (let i = 0; i < comps.length; i += 8) {
      const part = comps.slice(i, i + 8);
      btMsg('재무 데이터 불러오는 중... ' + Math.min(i + 8, comps.length) + ' / ' + comps.length + '개 종목');
      const res = await fetch('/api/backtest/fin-batch?codes=' + encodeURIComponent(part.map((c) => c.corp_code).join(',')));
      const d = await res.json();
      if (!res.ok) throw new Error(d.error || '불러오기 실패');
      (d.rows || []).forEach((row) => { (by[row.corp_code] = by[row.corp_code] || []).push(row); });
    }
    BT.stocks = comps.map((c) => ({ code: c.corp_code, name: c.corp_name, rows: by[c.corp_code] || [] })).filter((s) => s.rows.length);
    btMsg('불러오기 완료: ' + BT.stocks.length + '개 종목 (' + BT.stocks.reduce((a, s) => a + s.rows.length, 0) + '개 기간). 아래에서 방법을 고르고 실행하세요.');
    document.getElementById('btFinInfo').textContent = '불러온 종목 ' + BT.stocks.length + '개';
  } catch (e) { btMsg('오류: ' + (e.message || e)); } finally { BT.loading = false; }
}

// 연간(재무) 백테스트: FY Y 사업보고서 공시일 종가에 매수 → 다음 해 사업보고서 공시일 종가에 매도(약 12개월). 공시일 이후 가격만 쓰므로 미래정보 편향이 없다.
function btBuildAnnual(keys, o) {
  const perYear = {};
  BT.stocks.forEach((s) => {
    const ann = {};
    s.rows.forEach((r) => { if (r.reprt_code === '11011' && isUsable(r)) ann[Number(r.bsns_year)] = r; });
    Object.keys(ann).forEach((ys) => {
      const Y = Number(ys), a = ann[Y], nx = ann[Y + 1];
      if (!nx) return;
      const P0 = a.price_at_filing, P1 = nx.price_at_filing;
      if (!(P0 > 0 && P1 > 0)) return;
      if (a.filing_date && nx.filing_date && !(nx.filing_date > a.filing_date)) return;
      let L;
      try { L = anBuildSeries(s.rows.filter((r) => Number(r.bsns_year) <= Y)); } catch (e) { return; }
      const d = L.length ? L[L.length - 1] : null;
      if (!d || d.year !== Y) return;
      const vals = {};
      for (const k of keys) { let v = null; try { v = BT_FACTORS[k].fn(L, d, P0); } catch (e) { v = null; } if (!anFin(v)) return; vals[k] = v; }
      (perYear[Y] = perYear[Y] || []).push({ id: s.code, vals, f: P1 / P0 - 1 });
    });
  });
  return Object.keys(perYear).map(Number).sort((a, b) => a - b).map((Y) => {
    const arr = perYear[Y];
    // 방법이 여러 개면 각 방법의 횡단면 순위(퍼센타일)를 같은 비중으로 평균한다.
    const sc = arr.map(() => 0);
    keys.forEach((k) => { const rk = btRankAvg(arr.map((x) => x.vals[k])); rk.forEach((v, i) => { sc[i] += v / arr.length / keys.length; }); });
    return { label: 'FY' + Y + ' → ' + (Y + 1), items: arr.map((x, i) => ({ id: x.id, s: keys.length === 1 ? x.vals[keys[0]] : sc[i], f: x.f })), bench: null };
  });
}
// 분기 모멘텀: 분기말 주가만 사용. 형성기간 K분기, 건너뛰기 skip분기, 보유 1분기. (분기 자료라 "최근 1개월 제외"는 불가 — 0 또는 1분기 건너뛰기만 가능)
function btBuildMomQ(K, skip) {
  const per = {};
  BT.stocks.forEach((s) => {
    const px = {};
    s.rows.forEach((r) => {
      if (!(r.price_at_period_end > 0)) return;
      const q = { '11013': 0, '11012': 1, '11014': 2, '11011': 3 }[r.reprt_code]; if (q == null) return;
      px[Number(r.bsns_year) * 4 + q] = r.price_at_period_end;
    });
    Object.keys(px).map(Number).forEach((qi) => {
      const p1 = px[qi + 1], p0 = px[qi - skip], pk = px[qi - skip - K], pc = px[qi];
      if (!(p1 > 0 && pc > 0 && p0 > 0 && pk > 0)) return;
      (per[qi] = per[qi] || []).push({ id: s.code, s: p0 / pk - 1, f: p1 / pc - 1 });
    });
  });
  return Object.keys(per).map(Number).sort((a, b) => a - b).map((qi) => ({ label: Math.floor(qi / 4) + ' ' + ((qi % 4) + 1) + 'Q', items: per[qi], bench: null }));
}

async function btRunFin() {
  if (!BT.stocks) { btMsg('먼저 「저장된 종목 불러오기」를 눌러 주세요.'); return; }
  const mode = document.getElementById('btFinMode').value;
  const G = Number(document.getElementById('btFinG').value) || 5;
  const cost = (Number(document.getElementById('btFinCost').value) || 0) / 100;
  const trim = document.getElementById('btFinTrim').checked;
  const keys = [].slice.call(document.querySelectorAll('.btFac')).filter((c) => c.checked).map((c) => c.value);
  let periods, ppy, sig, title;
  if (mode === 'momq') {
    const K = Number(document.getElementById('btMomK').value) || 2, skip = Number(document.getElementById('btMomSkip').value) || 0;
    periods = btBuildMomQ(K, skip); ppy = 4; title = '분기 모멘텀 (형성 ' + K + '분기, 건너뛰기 ' + skip + '분기, 보유 1분기)';
    sig = 'momq|' + K + '|' + skip + '|' + G;
  } else {
    if (!keys.length) { btMsg('재무 방법을 하나 이상 고르세요.'); return; }
    periods = btBuildAnnual(keys, {}); ppy = 1; title = '연간 재무: ' + keys.map((k) => BT_FACTORS[k].name.split(' (')[0]).join(' + ') + (keys.length > 1 ? ' (순위 평균 복합)' : '');
    sig = 'fin|' + keys.slice().sort().join('+') + '|' + G;
  }
  const res = btRun(periods, { G, cost, ppy, trimLo: trim ? -0.9 : null, trimHi: trim ? 5 : null, minN: Math.max(3 * G, 10) });
  const nTr = btTrials(sig);
  BT.last = { kind: 'fin', mode, keys, G };
  const keysShown = mode === 'momq' ? [] : keys;
  document.getElementById('btFinOut').innerHTML = btRenderResult(res, { title, ppy, nTr, kind: mode === 'momq' ? 'momq' : 'fin', keys: keysShown, stocks: BT.stocks.length });
}

// ---------- 스크리닝 백테스트(서버 SQL이 기준일별 점수·미래수익을 계산해 주고, 분위 집계는 여기서) ----------
async function btRunScreen() {
  const g = (id) => document.getElementById(id).value;
  const H = Number(g('btScH')) || 21, S = Number(g('btScS')), L3 = Number(g('btScL3')) || 63, L6 = Number(g('btScL6')) || 126;
  const w3 = Number(g('btScW3')), w6 = Number(g('btScW6'));
  const G = Number(g('btScG')) || 5, cost = (Number(g('btScCost')) || 0) / 100;
  const maxD = Math.min(30, Number(g('btScMax')) || 20);
  const q = 'S=' + (S || 0) + '&L3=' + L3 + '&L6=' + L6 + '&H=' + H + '&w3=' + w3 + '&w6=' + w6 + '&pcm=' + g('btScPcm') + '&pct=' + g('btScPct') + '&price=' + g('btScPrice');
  const out = document.getElementById('btScOut'); out.innerHTML = '';
  try {
    btMsg('이력 범위 확인 중...');
    const pr = await (await fetch('/api/backtest/screen-plan')).json();
    const cal = pr.cal || [];
    const need = Math.max(w6 > 0 ? L6 : 0, w3 > 0 ? L3 : 0, S || 0);
    const bases = [];
    for (let i = H; i + need < cal.length; i += H) bases.push(i);
    if (!bases.length) {
      out.innerHTML = '<div class="card" style="border-left:4px solid #dc2626;"><b>이 조건으로는 백테스트할 수 있는 기준일이 없습니다.</b><div style="font-size:12.5px; margin-top:6px; line-height:1.6;">저장된 거래일 달력은 ' + cal.length + '일입니다. 형성기간 ' + need + '거래일 + 보유 ' + H + '거래일 = ' + (need + H) + '거래일이 필요한데 이력이 부족합니다. 형성기간을 줄이거나(예: 2~3개월 신호만 사용 = 2~6개월 가중치 0), 보유일을 줄이거나, 이력이 더 쌓일 때까지 기다려야 합니다. 이력은 매일 18:01 자동 갱신으로 하루씩 늘어납니다.</div></div>';
      btMsg('기준일 없음'); return;
    }
    const use = bases.slice(0, maxD);
    const periods = [];
    for (let k = 0; k < use.length; k++) {
      btMsg('기준일 계산 중... ' + (k + 1) + ' / ' + use.length + ' (기준일마다 약 2.5만 행 읽음)');
      const res = await fetch('/api/backtest/screen-date?i=' + use[k] + '&' + q);
      const d = await res.json();
      if (!res.ok) { periods.push(null); continue; }
      periods.push({ label: d.base + ' → ' + d.fwdDate, items: d.rows.map((r) => ({ id: r[0], s: r[1], f: r[2] })), bench: d.kospiFwd, note: d.note });
    }
    const okP = periods.filter(Boolean).reverse();
    const res = btRun(okP, { G, cost, ppy: 252 / H, trimLo: -0.45, trimHi: 1.0, minN: Math.max(3 * G, 20) });
    const sig = 'scr|' + [H, S, L3, L6, w3, w6, G, g('btScPcm'), g('btScPct'), g('btScPrice')].join('|');
    const nTr = btTrials(sig);
    const notes = okP.filter((p) => p.note).map((p) => p.note);
    BT.last = { kind: 'scr', pcm: g('btScPcm'), pct: g('btScPct'), price: g('btScPrice'), w3, w6 };
    out.innerHTML = btRenderResult(res, { title: '스크리닝: 신호 가중치 2~3M ' + w3 + ' / 2~6M ' + w6 + ', 건너뛰기 ' + S + '일, 보유 ' + H + '거래일', ppy: 252 / H, nTr, kind: 'scr', extra: notes.length ? ['기준일 중 ' + notes.length + '개는 시가총액 이력이 없어 시가총액 제외 필터를 적용하지 못했습니다(거래대금·주가 필터만 적용).'] : [], bases: bases.length, used: use.length, calLen: cal.length })
      + '<div class="row" style="margin:8px 0;"><button onclick="btApplyScreen()">이 필터·가중치를 ① 스크리닝에 적용</button><span style="font-size:11.5px; color:var(--text-muted);">적용되는 것: 시총·거래대금 하위 제외 %, 최소 주가, 신호 가중치. (건너뛰기·형성·보유 기간은 스크리닝이 21/63/126거래일로 고정이라 옮겨지지 않습니다.)</span></div>';
    btMsg('완료: ' + okP.length + '개 기준일');
  } catch (e) { btMsg('오류: ' + (e.message || e)); }
}
function btApplyScreen() {
  const b = BT.last; if (!b || b.kind !== 'scr') return;
  document.getElementById('inPcM').value = b.pcm; document.getElementById('inPcT').value = b.pct; document.getElementById('inPrice').value = b.price;
  document.getElementById('inW3').value = b.w3; document.getElementById('inW6').value = b.w6;
  switchTab('momentum');
}
function btApplyFin() {
  const keys = [].slice.call(document.querySelectorAll('.btFac')).filter((c) => c.checked).map((c) => c.value);
  const el = document.getElementById('btApplyMsg');
  if (keys.length === 1 && keys[0] === 'fscore') { AN_CFG.qm = 'fscore'; anCfgSave(); el.textContent = '종목분석의 Quality 판정을 Piotroski F-Score 방식으로 설정했습니다.'; }
  else if (keys.length === 1 && keys[0] === 'legacy4') { AN_CFG.qm = 'legacy4'; anCfgSave(); el.textContent = '종목분석의 Quality 판정을 기존 4점 방식으로 설정했습니다.'; }
  else el.textContent = '종목분석 등급에 바꿔 쓸 수 있는 방식은 Quality의 F-Score / 기존 4점 두 가지입니다. 한 가지만 고르세요. 그 밖의 기준은 종목분석의 「분석 기준 설정」에서 직접 바꿀 수 있습니다.';
  if (typeof renderAnalysis === 'function' && rawRows.length) renderAnalysis();
}

// ---------- 결과 화면 ----------
function btBadgeFor(k) { const f = BT_FACTORS[k]; return f ? anBadge(f.badge) : ''; }
function btChart(res) {
  const W = 640, H = 190, pad = 34, G = res.G;
  const ser = (fn) => { let c = 1; const a = [1]; res.rows.forEach((r) => { c *= (1 + fn(r)); a.push(c); }); return a; };
  const lines = [['상위 그룹', '#16a34a', ser((r) => r.gm[G - 1])], ['하위 그룹', '#dc2626', ser((r) => r.gm[0])], ['전체 평균', '#64748b', ser((r) => r.uni)]];
  const all = [].concat.apply([], lines.map((l) => l[2])); const lo = Math.min.apply(null, all), hi = Math.max.apply(null, all);
  const n = res.rows.length; const X = (i) => pad + (W - 2 * pad) * (n ? i / n : 0); const Y = (v) => H - pad - (H - 2 * pad) * ((v - lo) / ((hi - lo) || 1));
  let s = '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%; max-width:640px; background:#fff; border:1px solid var(--border); border-radius:8px;">';
  s += '<line x1="' + pad + '" y1="' + Y(1) + '" x2="' + (W - pad) + '" y2="' + Y(1) + '" stroke="#cbd5e1" stroke-dasharray="3 3"/>';
  lines.forEach((l, k) => { s += '<polyline fill="none" stroke="' + l[1] + '" stroke-width="2" points="' + l[2].map((v, i) => X(i).toFixed(1) + ',' + Y(v).toFixed(1)).join(' ') + '"/>'; s += '<text x="' + (pad + 6 + k * 90) + '" y="14" font-size="11" fill="' + l[1] + '">' + l[0] + ' ' + ((l[2][l[2].length - 1] - 1) * 100).toFixed(0) + '%</text>'; });
  s += '<text x="' + pad + '" y="' + (H - 8) + '" font-size="10" fill="#64748b">' + escHtml(res.rows[0].label) + '</text><text x="' + (W - pad) + '" y="' + (H - 8) + '" font-size="10" fill="#64748b" text-anchor="end">' + escHtml(res.rows[n - 1].label) + '</text></svg>';
  return s;
}
function btRenderResult(res, m) {
  const f1 = (v) => v == null ? 'N/A' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%';
  const f2 = (v) => v == null ? 'N/A' : v.toFixed(2);
  if (!res.T) return '<div class="card" style="border-left:4px solid #dc2626;"><b>' + escHtml(m.title) + '</b><div style="font-size:12.5px; margin-top:6px;">유효한 기간이 없습니다 (기간별 종목 수가 최소 기준 미만이거나 가격·재무 자료가 부족). 건너뛴 기간 ' + res.skipped.length + '개' + (res.skipped.length ? ': ' + res.skipped.slice(0, 6).map((x) => x.label + '(' + x.n + '종목)').join(', ') : '') + '</div></div>';
  const sp = res.spreadStat, ic = res.icStat, G = res.G;
  const dsr = sp && sp.srPeriod != null ? btDeflated(sp.srPeriod, res.T, res.rows.map((r) => r.spread), m.nTr) : null;
  const warns = [];
  if (res.T < 8) warns.push('기간 수가 ' + res.T + '개뿐입니다. 통계적 결론을 내기엔 표본이 매우 부족합니다(일반적으로 수십 개 이상 필요).');
  if (res.avgN < 30) warns.push('기간당 평균 종목 수가 ' + res.avgN.toFixed(0) + '개로 적습니다. 그룹당 ' + (res.avgN / G).toFixed(0) + '개 정도라 한두 종목이 결과를 좌우합니다.');
  if (m.kind === 'fin' || m.kind === 'momq') warns.push('표본은 「이 앱에 저장해 둔 종목」(' + m.stocks + '개)뿐입니다. 사용자가 관심 있어 고른 종목이라 전체 시장을 대표하지 않으며(선택 편향), 상장폐지·거래정지 종목이 빠진 생존편향이 있습니다. 두 편향 모두 보통 성과를 실제보다 좋게 보이게 합니다.');
  if (m.kind === 'scr') warns.push('시세 이력이 짧아(달력 ' + m.calLen + '거래일) 겹치지 않는 기준일이 ' + m.bases + '개뿐입니다. 사용 ' + m.used + '개. 상장폐지 종목은 시세 DB에 없어 생존편향이 있고, 액면분할이 수정되지 않은 원시 종가를 쓰므로 수익률 −45% 이하·+100% 이상 종목은 오염 가능성 때문에 제외했습니다.');
  if (m.kind === 'momq') warns.push('분기말 주가만 있어 「최근 1개월 제외」를 못 합니다. 단기 반전 효과(Jegadeesh 1990)를 완전히 피하지 못합니다.');
  warns.push('가격 수익률만 계산했고 배당은 넣지 않았습니다. 거래비용은 입력한 왕복 비용 × 상위 그룹 교체 비율만 반영했고 시장충격·체결 불가(상·하한가, 소형주 유동성)는 반영하지 못합니다.');
  (m.extra || []).forEach((x) => warns.push(x));
  if (res.skipped.length) warns.push('종목 수 부족으로 제외된 기간 ' + res.skipped.length + '개: ' + res.skipped.slice(0, 5).map((x) => x.label).join(', '));
  const tAbs = sp && sp.tNW != null ? Math.abs(sp.tNW) : null;
  let verdict, vcol;
  if (res.T < 8) { verdict = '표본 부족 — 결과를 참고용으로만 보세요'; vcol = '#dc2626'; }
  else if (tAbs != null && tAbs >= 3) { verdict = '통계적으로 의미 있음 (|t|≥3, Harvey-Liu-Zhu 기준 충족)'; vcol = '#16a34a'; }
  else if (tAbs != null && tAbs >= 2) { verdict = '약한 증거 (|t|가 2~3: 여러 조합을 시험했다면 우연일 가능성이 큼)'; vcol = '#d97706'; }
  else { verdict = '통계적으로 구분되지 않음 (|t|<2: 우연과 구별 안 됨)'; vcol = '#64748b'; }
  const dir = sp && sp.mean < 0 ? ' · 상위 그룹이 오히려 부진(역방향)' : '';
  const cards = anGrid([
    anTile('분석 기간 수 T', String(res.T), '평균 종목 ' + res.avgN.toFixed(0) + '개 / 기간'),
    anTile('상위 그룹 평균', f1(res.groupStats[G - 1].mean), '기간당 · 전체평균 ' + f1(res.uniStat.mean)),
    anTile('상위−하위 (롱숏)', f1(sp.mean), 'NW t = ' + f2(sp.tNW) + ' · 적중 ' + (sp.hit * 100).toFixed(0) + '%'),
    anTile('상위−전체 (초과)', f1(res.excessStat.mean), '비용 반영 후 ' + f1(res.excessNetStat.mean) + ' · 교체율 ' + (res.avgTurn == null ? 'N/A' : (res.avgTurn * 100).toFixed(0) + '%')),
    anTile('IC (Spearman)', f2(ic.mean), 'IC t = ' + f2(ic.t) + ' · 양수 ' + (ic.hit * 100).toFixed(0) + '%'),
    anTile('연환산 샤프(롱숏)', f2(sp.sharpe), '최대낙폭 ' + f1(sp.mdd)),
    anTile('다중검정 보정', dsr ? (dsr.psr * 100).toFixed(0) + '%' : 'N/A', '지금까지 시도한 조합 ' + m.nTr + '개 기준 Deflated Sharpe 확률(근사). 90% 미만이면 우연 가능성'),
    anTile('단조성', f2(res.mono), '그룹 번호와 평균수익의 순위상관(1에 가까울수록 일관)'),
  ]);
  const grow = res.groupStats.map((st, g) => '<tr' + (g === G - 1 ? ' style="background:#f0fdf4;"' : g === 0 ? ' style="background:#fef2f2;"' : '') + '><td class="an-first">' + (g === G - 1 ? '상위 ' : g === 0 ? '하위 ' : '') + '그룹 ' + (g + 1) + '/' + G + '</td><td>' + f1(st.mean) + '</td><td>' + f1(st.mean - res.uniStat.mean) + '</td><td>' + f2(st.sharpe) + '</td><td>' + f1(st.cum) + '</td><td>' + f1(st.mdd) + '</td><td>' + (st.hit * 100).toFixed(0) + '%</td></tr>').join('');
  const prow = res.rows.map((r) => '<tr><td class="an-first">' + escHtml(r.label) + '</td><td>' + r.n + '</td><td>' + f1(r.gm[G - 1]) + '</td><td>' + f1(r.gm[0]) + '</td><td>' + f1(r.spread) + '</td><td>' + f2(r.ic) + '</td><td>' + (m.kind === 'scr' && typeof r.bench === 'number' ? f1(r.bench) : '-') + '</td></tr>').join('');
  const splitTxt = res.split ? ('전반 ' + res.split.nFirst + '개 기간 롱숏 평균 ' + f1(res.split.first) + ' / 후반 ' + res.split.nSecond + '개 기간 ' + f1(res.split.second) + (Math.sign(res.split.first) !== Math.sign(res.split.second) ? ' → 부호가 달라 결과가 불안정합니다.' : ' → 부호가 같아 시기별로 일관됩니다.')) : '기간이 6개 미만이라 전반/후반 비교 불가';
  const evid = m.keys ? '<div style="font-size:12px; margin:6px 0;">' + m.keys.map((k) => '<div>' + btBadgeFor(k) + ' <b>' + escHtml(BT_FACTORS[k].name) + '</b> — ' + escHtml(BT_FACTORS[k].ev) + '</div>').join('') + '</div>' : '';
  return '<div class="card" style="border-left:4px solid ' + vcol + ';"><div style="font-weight:800; font-size:14px;">' + escHtml(m.title) + '</div>' +
    '<div style="margin:6px 0; font-weight:700; color:' + vcol + ';">판정: ' + verdict + dir + '</div>' + evid + cards +
    '<div style="margin:8px 0; font-size:12px; line-height:1.55;"><b>표본 분할 점검</b> ' + splitTxt + '</div>' +
    btChart(res) +
    '<div class="tscroll" style="max-height:50vh; margin-top:8px;"><table class="an-tbl"><thead><tr><th class="an-first">그룹(낮은 점수 → 높은 점수)</th><th>기간당 평균</th><th>전체 대비</th><th>연환산 샤프</th><th>누적</th><th>최대낙폭</th><th>적중</th></tr></thead><tbody>' + grow + '</tbody></table></div>' +
    '<details style="margin-top:8px;"><summary style="cursor:pointer; font-weight:600;">기간별 결과 (' + res.T + '개)</summary><div class="tscroll" style="max-height:50vh;"><table class="an-tbl"><thead><tr><th class="an-first">기간</th><th>종목수</th><th>상위</th><th>하위</th><th>롱숏</th><th>IC</th><th>코스피</th></tr></thead><tbody>' + prow + '</tbody></table></div></details>' +
    '<div style="margin-top:8px; padding:8px 10px; background:#fffbeb; border:1px solid #fde68a; border-radius:8px; font-size:12px; line-height:1.55;"><b>주의할 점</b><ul style="margin:4px 0 0 18px; padding:0;">' + warns.map((w) => '<li>' + w + '</li>').join('') + '</ul></div></div>';
}

function btFinModeChange() {
  const m = document.getElementById('btFinMode').value;
  document.getElementById('btFinFactors').style.display = m === 'annual' ? '' : 'none';
  document.getElementById('btMomBox').style.display = m === 'momq' ? '' : 'none';
}
let btUiReady = false;
function btInitUi() {
  if (btUiReady) return; btUiReady = true;
  const keys = Object.keys(BT_FACTORS);
  document.getElementById('btFinFactors').innerHTML = '<div style="font-size:12.5px; font-weight:600; margin-bottom:4px;">검증할 방법 (여러 개 고르면 순위를 평균한 복합 점수)</div>' +
    keys.map((k) => '<label style="display:block; font-size:12.5px; margin:2px 0;"><input type="checkbox" class="btFac" value="' + k + '"' + (k === 'fscore' ? ' checked' : '') + ' /> ' + BT_FACTORS[k].name + ' ' + anBadge(BT_FACTORS[k].badge) + ' <span style="color:var(--text-muted); font-size:11px;">' + BT_FACTORS[k].ev + '</span></label>').join('');
}

function renderAnalysis() {
  const el = document.getElementById('analysis'); if (!el) return;
  const m = lastSummaryMetrics; if (!m) { el.innerHTML = ''; return; }
  const key = rawRows.length ? rawRows[0].corp_code : '';
  if (AN_STATE.key !== key) { AN_STATE.key = key; AN_STATE.ov = {}; AN_STATE.payout = null; AN_STATE.mom = null; AN_STATE.momFor = null; AN_STATE.momErr = null; }
  const price = m.priceInput;
  let c;
  try { c = anBuildContext(rawRows, price, m); } catch (e) { el.innerHTML = '<div class="card" style="color:var(--danger);">확장 분석 계산 오류: ' + anEsc(e.message || e) + '</div>'; return; }
  if (!c) { el.innerHTML = ''; return; }
  AN_CTX = c;
  const needRefetch = !c.last.r || !anRefetched(c.last.r);
  const notice = needRefetch ? '<div class="card" style="background:#fffbeb; border-color:#fcd34d; font-size:12.5px;">⚠ 이 종목은 <b>감가상각·배당지급·자사주·차입금 구성 항목이 아직 저장되어 있지 않습니다</b>. EBITDA, 순차입금, 총주주환원성향 등은 N/A로 표시되며, 위쪽에서 이 종목을 <b>다시 조회/저장</b>하면 채워집니다(DART 호출량은 기존과 같음).</div>' : '';
  el.innerHTML = notice + anRenderSettings() + anRenderClassify(c) + anRenderExplain(c) + anRenderMomentum() + anRenderTrend(c) + anRenderJudge(c) + anRenderScenario(c) + anRenderMethods(c) + anRenderRoic4(c) + anRenderValuation(c) + anRenderRD(c) + anRenderAreas(c);
  anBuildNav();
  anUpdateScenarioView();
  anUpdateMethods();
  anR4Draw();
  if (typeof macroSeriesCache !== 'undefined' && !macroSeriesCache && !macroLoadFailed && typeof loadMacroSeries === 'function') { loadMacroSeries().then(function () { anUpdateMethods(); }).catch(function () { macroLoadFailed = true; }); }
  anLoadMomentum();
  anLoadTrend();
}

    async function fetchLatestPrice() {
      const corpName = document.getElementById('corpName').value.trim();
      const noteEl = document.getElementById('priceNote');
      if (!corpName) return false;
      noteEl.textContent = '전일 종가 조회 중...';
      try {
        const res = await fetch('/api/latest-price?corp_name=' + encodeURIComponent(corpName));
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');
        document.getElementById('currentPrice').value = data.close;
        noteEl.textContent = data.corp_name + ' ' + data.date + ' 종가 ' + data.close.toLocaleString() + '원을 현재 주가로 사용했습니다(직접 고쳐서 다시 계산할 수 있습니다).';
        return true;
      } catch (e) {
        noteEl.textContent = '현재가를 자동으로 가져오지 못했습니다(' + e.message + '). 직접 입력한 뒤 "이 가격으로 다시 계산"을 눌러주세요.';
        return false;
      }
    }
    async function refreshPrice() { await fetchLatestPrice(); renderSummary(); }

    // 2종목 비교용: 입력칸이 비어 있으면 최신 종가를 자동으로 가져온다(실패하면 null).
    async function autoPriceFor(corpName) {
      try {
        const res = await fetch('/api/latest-price?corp_name=' + encodeURIComponent(corpName));
        const data = await res.json();
        return res.ok && data.close > 0 ? data.close : null;
      } catch (e) { return null; }
    }

    async function fetchRowsFromDb(corpName) {
      const res = await fetch(\`/api/financial-history-db?corp_name=\${encodeURIComponent(corpName)}\`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'DB 조회 실패');
      return data;
    }

    async function compareStocks() {
      const nameA = document.getElementById('cmpNameA').value.trim();
      const nameB = document.getElementById('cmpNameB').value.trim();
      let priceA = Number(document.getElementById('cmpPriceA').value) || null;
      let priceB = Number(document.getElementById('cmpPriceB').value) || null;
      const statusEl = document.getElementById('cmpStatus');
      const wrapEl = document.getElementById('cmpWrap');
      wrapEl.style.display = 'none';

      if (!nameA || !nameB) { statusEl.textContent = '두 종목명을 모두 입력해주세요.'; return; }
      statusEl.textContent = '조회 중...';

      let dataA, dataB;
      try {
        [dataA, dataB] = await Promise.all([fetchRowsFromDb(nameA), fetchRowsFromDb(nameB)]);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
        return;
      }

      if (!priceA) priceA = await autoPriceFor(nameA);
      if (!priceB) priceB = await autoPriceFor(nameB);
      const mA = computeSummaryMetrics(dataA.rows, priceA);
      const mB = computeSummaryMetrics(dataB.rows, priceB);
      if (!mA || !mB) {
        statusEl.textContent = '한쪽 이상 DB에 연간 데이터가 없습니다. 먼저 "DART에서 조회 + 저장"으로 데이터를 모아주세요.';
        return;
      }

      const pctStr = (v) => v != null ? (v * 100).toFixed(2) + '%' : 'N/A';
      const wonStr = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';
      const numStr = (v) => v != null ? v.toFixed(2) : 'N/A';

      let cA = null, cB = null;
      try { cA = anBuildContext(dataA.rows, priceA, mA); } catch (e) { cA = null; }
      try { cB = anBuildContext(dataB.rows, priceB, mB); } catch (e) { cB = null; }
      const gr = (c, k) => (c && c.cls && c.cls.grades && c.cls.grades[k]) ? c.cls.grades[k] : 'N/A';
      const rows = [
        ['Growth 등급', gr(cA, 'growth'), gr(cB, 'growth')],
        ['Quality 등급', gr(cA, 'quality'), gr(cB, 'quality')],
        ['ROIC 등급', gr(cA, 'roic'), gr(cB, 'roic')],
        ['Valuation 등급', gr(cA, 'val'), gr(cB, 'val')],
        ['ROE 평균', pctStr(mA.avgROE), pctStr(mB.avgROE)],
        ['ROIC 평균', pctStr(mA.avgROIC), pctStr(mB.avgROIC)],
        ['평균 배당성향', pctStr(mA.avgPayout), pctStr(mB.avgPayout)],
        ['BPS 기준시점', mA.latestLabel, mB.latestLabel],
        ['BPS', wonStr(mA.bps), wonStr(mB.bps)],
        ['현재주가', mA.priceInput ? mA.priceInput.toLocaleString() + '원' : 'N/A', mB.priceInput ? mB.priceInput.toLocaleString() + '원' : 'N/A'],
        ['EPS', mA.eps != null ? Math.round(mA.eps).toLocaleString() + '원' : 'N/A', mB.eps != null ? Math.round(mB.eps).toLocaleString() + '원' : 'N/A'],
        ['PER', numStr(mA.per), numStr(mB.per)],
        ['PBR', numStr(mA.pbr), numStr(mB.pbr)],
        ['이익수익률(1/PER)', pctStr(mA.earningsYield), pctStr(mB.earningsYield)],
        ['참고 시가총액', wonStr(mA.marketCap), wonStr(mB.marketCap)],
      ];

      let html = \`<table><tr><th>지표</th><th>\${dataA.corp_name}</th><th>\${dataB.corp_name}</th></tr>\`;
      for (const [label, a, b] of rows) {
        html += \`<tr><td>\${label}</td><td>\${a}</td><td>\${b}</td></tr>\`;
      }
      html += '</table>';
      statusEl.textContent = '';
      wrapEl.style.display = 'block';
      wrapEl.innerHTML = html;
    }

    // opts.fromYear: 그 연도부터 올해까지만 받아 기존 rawRows에 합친다(최신 공시 업데이트). 없으면 고급 설정의 조회 기간 전체.
    async function fetchAndSave(opts) {
      opts = opts || {};
      dbCompanies = null; // 새로 저장되면 DB 종목 목록 캐시 무효화
      const corpName = document.getElementById('corpName').value.trim();
      const startYear = opts.fromYear ? Number(opts.fromYear) : Number(document.getElementById('startYear').value);
      const endYear = opts.fromYear ? new Date().getFullYear() : Number(document.getElementById('endYear').value);
      const statusEl = document.getElementById('status');
      const baseRows = opts.fromYear ? rawRows.slice() : [];

      if (!startYear || !endYear || startYear > endYear) {
        statusEl.textContent = '조회 기간을 올바르게 입력해주세요 (시작연도 ≤ 종료연도).';
        statusEl.className = 'error';
        return;
      }
      statusEl.className = '';

      const reprtCodes = [
        { code: '11013', label: '1분기' },
        { code: '11012', label: '반기' },
        { code: '11014', label: '3분기' },
        { code: '11011', label: '사업보고서' },
      ];
      const periods = [];
      for (let y = startYear; y <= endYear; y++) {
        for (const r of reprtCodes) periods.push({ year: y, ...r });
      }

      const rows = [];
      for (let i = 0; i < periods.length; i++) {
        const p = periods[i];
        statusEl.textContent = '"' + corpName + '" DART에서 받는 중... (' + (i + 1) + '/' + periods.length + ': ' + p.year + ' ' + p.label + ')';
        try {
          const res = await fetch(\`/api/fetch-and-save?corp_name=\${encodeURIComponent(corpName)}&year=\${p.year}&reprt_code=\${p.code}\`);
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '저장 실패');
          rows.push(data.row);
        } catch (e) {
          rows.push({ period_label: \`\${p.year} \${p.label}\`, bsns_year: String(p.year), reprt_code: p.code, error: e.message });
        }
      }
      if (opts.fromYear) {
        // 기존 행 위에 새로 받은 행을 덮어쓰되, 새로 받은 것이 비었거나 오류면 기존 데이터를 지키지 않고 버리는 일이 없게 한다.
        const keyOf = (r) => String(r.bsns_year) + '|' + r.reprt_code;
        const merged = new Map(baseRows.map((r) => [keyOf(r), r]));
        for (const r of rows) { if (isUsable(r) || !merged.has(keyOf(r))) merged.set(keyOf(r), r); }
        const repOrder = { '11013': 1, '11012': 2, '11014': 3, '11011': 4 };
        rawRows = Array.from(merged.values()).sort((x, y) => (Number(x.bsns_year) - Number(y.bsns_year)) || ((repOrder[x.reprt_code] || 0) - (repOrder[y.reprt_code] || 0)));
      } else {
        rawRows = rows;
      }
      const savedCnt = rows.filter(isUsable).length;
      const failed = rows.filter((r) => r.error);
      const pendingCnt = rows.length - savedCnt - failed.length;
      statusEl.textContent = \`저장 완료: \${savedCnt}개 기간 저장, \${pendingCnt}개는 아직 공시 전이거나 데이터 없음\` + (failed.length ? \`, \${failed.length}개 오류 (\${failed[0].period_label}: \${failed[0].error})\` : '');
    }

    // 화면을 띄워둘 필요 없이, 서버(Worker)의 Cron Trigger가 1분마다 깨어나서 큐에 쌓인 기간을 몇 개씩
    // 조금씩 받아 D1에 저장한다. 화면을 닫거나 태블릿을 다른 앱으로 전환해도 계속 진행된다.
    async function queueFetch() {
      const corpName = document.getElementById('corpName').value.trim();
      const startYear = Number(document.getElementById('startYear').value);
      const endYear = Number(document.getElementById('endYear').value);
      const statusEl = document.getElementById('status');
      const qEl = document.getElementById('queueStatus');
      if (!corpName) { statusEl.textContent = '종목명을 입력해주세요.'; return; }
      if (!startYear || !endYear || startYear > endYear) {
        statusEl.textContent = '조회 기간을 올바르게 입력해주세요 (시작연도 ≤ 종료연도).';
        return;
      }
      try {
        const res = await fetch(\`/api/queue-fetch?corp_name=\${encodeURIComponent(corpName)}&start_year=\${startYear}&end_year=\${endYear}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '등록 실패');
        statusEl.textContent = \`\${data.corp_name}: \${data.queued}개 기간을 백그라운드 큐에 등록했습니다. 1분마다 몇 개씩 자동으로 받아집니다. (화면을 닫아도 계속 진행됩니다)\`;
        qEl.textContent = '잠시 후 "진행 상황 확인"을 눌러 남은 개수를 확인하세요.';
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }

    async function checkQueueStatus() {
      const corpName = document.getElementById('corpName').value.trim();
      const qEl = document.getElementById('queueStatus');
      if (!corpName) { qEl.textContent = '종목명을 입력해주세요.'; return; }
      qEl.textContent = '확인 중...';
      try {
        const res = await fetch(\`/api/queue-status?corp_name=\${encodeURIComponent(corpName)}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');
        qEl.textContent = data.remaining === 0
          ? '큐가 비었습니다 — 모두 처리 완료되었습니다. "DB에서 조회"로 결과를 확인하세요.'
          : \`아직 \${data.remaining}개 기간이 남아있습니다 (다음: \${data.next.map((p) => \`\${p.bsns_year} \${p.reprt_code}\`).join(', ')}).\`;
      } catch (e) {
        qEl.textContent = '오류: ' + e.message;
      }
    }

    function switchTab(tab) {
      const tabs = { financial: 'tabFinancial', momentum: 'tabMomentum', macro: 'tabMacro', data: 'tabData', bt: 'tabBt' };
      const btns = { financial: 'tabBtnFinancial', momentum: 'tabBtnMomentum', macro: 'tabBtnMacro', data: 'tabBtnData', bt: 'tabBtnBt' };
      Object.keys(tabs).forEach(function (k) {
        document.getElementById(tabs[k]).style.display = k === tab ? '' : 'none';
        document.getElementById(btns[k]).className = k === tab ? 'toggle-active' : '';
      });
      if (tab === 'momentum' && !screenAutoLoaded) { screenAutoLoaded = true; loadScreen('top', true); }
      if (tab === 'data') { loadDataSummary(); checkMarketStatus(); }
      if (tab === 'bt') { btInitUi(); }
      if (tab === 'macro' && macroCorrDirty && rawRows.length) computeMacroCorrelation();
      if (tab === 'financial' && subTab === 'table' && chartSel.length) renderChartPanel(false);
      window.scrollTo(0, 0);
    }

    async function loadDataSummary() {
      const el = document.getElementById('dataSummary');
      try {
        const res = await fetch('/api/data/summary', { cache: 'no-store' });
        const d = await res.json();
        if (!res.ok) throw new Error(d.error || '조회 실패');
        const ml = { usdkrw: '환율', msb1y: '통안1Y', ktb3y: '국고3Y', ktb10y: '국고10Y', wti: 'WTI' };
        const macroTxt = d.macro && d.macro.length ? d.macro.map(function (m) { return (ml[m.series] || m.series) + ' ~' + m.last; }).join(' · ') : '없음(③ 거시·상관을 쓰려면 아래에서 갱신)';
        el.innerHTML =
          '• <b>시세(스크리닝)</b>: 최근 거래일 ' + (d.market_last || '없음') + ' · 가격이력 확보 종목 ' + sN(d.backfilled) + '개 / 조건 통과 ' + sN(d.passed) + '개 (대기 ' + sN(d.market_queue) + '개)<br />' +
          '• <b>스크리닝 결과</b>: 저장된 주간 결과 ' + sN(d.screen_snaps) + '회, 최신 ' + (d.screen_last || '없음') + '<br />' +
          '• <b>재무데이터(DART)</b>: ' + sN(d.fin_companies) + '개 종목 · ' + sN(d.fin_rows) + '개 기간 저장 · 서버 대기열 ' + sN(d.fin_queue_rows) + '개 기간(' + sN(d.fin_queue_companies) + '개 종목)' + (d.fin_queue_rows ? ' — 1분에 약 1개 기간씩 처리, 약 ' + Math.ceil(d.fin_queue_rows / 60) + '시간 남음' : '') + '<br />' +
          '• <b>거시경제</b>: ' + macroTxt;
      } catch (e) {
        el.textContent = '현황을 불러오지 못했습니다: ' + e.message;
      }
    }

    async function refreshMarketUniverse() {
      const el = document.getElementById('marketStatus');
      el.textContent = '공공데이터포털에서 오늘자 통과 종목 받는 중...';
      try {
        const res = await fetch('/api/market/refresh-universe?force=1');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '갱신 실패');
        el.textContent = data.bas_dt + ' 기준 ' + data.universe_size + '개 종목(거래대금 15억 이상)을 확인했습니다. 시세 추가 작업을 시작했고, 약 ' + (data.pages || 1) + '번의 1분 주기(홀수 분)에 나눠 서버가 알아서 처리합니다. 처음 보는 종목은 백필 큐에 등록됩니다(화면을 닫아도 계속 진행됩니다).';
        checkMarketStatus();
      } catch (e) {
        el.textContent = '오류: ' + e.message;
      }
    }

    async function checkMarketStatus() {
      const el = document.getElementById('marketStatus');
      el.textContent = '확인 중...';
      try {
        const res = await fetch('/api/market/status');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');
        const aj = data.append_job ? ('  |  일괄 시세 추가 진행: ' + (data.append_job.nextPage - 1) + '/' + Math.ceil(data.append_job.total / data.append_job.pageSize) + '쪽') : '';
        el.textContent = '유동성 통과: ' + data.passed_total + '개 (확인한 종목 총 ' + data.checked_total + '개)  |  백필 완료: ' + data.backfilled_total + '개  |  백필 대기: ' + data.queue_remaining + '개  |  오늘 백필: ' + data.backfilled_today + '/' + data.daily_cap + aj;
      } catch (e) {
        el.textContent = '오류: ' + e.message;
      }
    }

    // ===== 모멘텀 스크리닝 화면 (8가지 Top 50) =====
    const SCREEN_LISTS = [
      { key: 'rw', label: '복합 점수(권장)', desc: '「2~3개월」과 「2~6개월」 수익률(최근 1개월 제외)을 후보 안에서의 퍼센타일 순위로 바꿔 가중평균한 점수(0~100)입니다. 극단적인 급등주에 덜 흔들리고 1개월 단기반전을 피합니다.' },
      { key: 'm16', label: '6-1 모멘텀', desc: '21거래일 전 종가 ÷ 126거래일 전 종가 − 1. 최근 1개월을 건너뛴 6개월 모멘텀(Jegadeesh & Titman 1993의 형태).' },
      { key: 'rs6', label: 'RS 6개월', desc: '(1+주식 6개월 수익률) ÷ (1+코스피 6개월 수익률). 1보다 크면 코스피보다 강했습니다. 최근 1개월 포함.' },
      { key: 'rs3', label: 'RS 3개월', desc: '(1+주식 3개월 수익률) ÷ (1+코스피 3개월 수익률). 최근 1개월 포함.' },
      { key: 'rs1', label: 'RS 1개월(반전 주의)', desc: '1개월 수익률은 단기 반전(되돌림) 경향이 있어 단독으로 쓰기엔 위험합니다. 과열 여부를 보는 참고용입니다.' },
      { key: 'r120', label: '120일 수익률', desc: '기준일 종가 ÷ 120거래일 전 종가 − 1. 최근 1개월 포함(참고용).' },
      { key: 'r60', label: '60일 수익률', desc: '기준일 종가 ÷ 60거래일 전 종가 − 1. 최근 1개월 포함(참고용).' },
      { key: 'r20', label: '20일 수익률', desc: '기준일 종가 ÷ 20거래일 전 종가 − 1. 단기 급등 종목이 많이 섞이며 반전 위험이 가장 큽니다(참고용).' }
    ];
    const SCREEN_PRESETS = { std: [20, 20, 1000], large: [50, 50, 1000], small: [0, 0, 1000] };
    let screenData = null;
    let screenTab = 'rw';
    let screenAutoLoaded = false;
    let opFillRunning = false;
    function sP(v, d) { return v == null ? 'N/A' : (v * 100).toFixed(d == null ? 1 : d) + '%'; }
    function sX(v, d) { return v == null ? 'N/A' : v.toFixed(d == null ? 2 : d); }
    function sN(v) { return v == null ? 'N/A' : Math.round(v).toLocaleString('ko-KR'); }
    function sCell(text, cls) { return '<td' + (cls ? ' class="' + cls + '"' : '') + '>' + text + '</td>'; }

    function setScreenTab(k) { screenTab = k; renderScreen(); }
    function setScreenPreset(k) {
      const v = SCREEN_PRESETS[k];
      if (!v) return;
      document.getElementById('inPcM').value = v[0];
      document.getElementById('inPcT').value = v[1];
      document.getElementById('inPrice').value = v[2];
      [['std', 'presetStd'], ['large', 'presetLarge'], ['small', 'presetSmall']].forEach(function (x) { document.getElementById(x[1]).className = x[0] === k ? 'toggle-active' : ''; });
    }
    function goAnalyzeCode(code) {
      let name = null;
      const lists = (screenData && screenData.lists) || {};
      SCREEN_LISTS.forEach(function (L) { (lists[L.key] || []).forEach(function (r) { if (r.stock_code === code && r.corp_name !== code) name = r.corp_name; }); });
      if (!name) { alert('종목명을 찾지 못했습니다.'); return; }
      document.getElementById('corpName').value = name;
      updateWatchButtonState();
      switchTab('financial');
      analyzeStock();
    }

    async function loadScreen(mode, auto) {
      const st = document.getElementById('screenStatus');
      const g = (id) => document.getElementById(id).value;
      let url;
      if (mode === 'run') {
        url = '/api/screen/run?pcm=' + encodeURIComponent(g('inPcM')) + '&pct=' + encodeURIComponent(g('inPcT')) + '&price=' + encodeURIComponent(g('inPrice')) +
          '&w3=' + encodeURIComponent(g('inW3')) + '&w6=' + encodeURIComponent(g('inW6')) +
          '&save=' + (document.getElementById('inSave').checked ? 1 : 0);
        st.textContent = '계산 중... (필요한 날짜의 가격만 읽어 순위를 매깁니다. 보통 몇 초 걸립니다)';
      } else {
        const d = document.getElementById('screenDate').value;
        url = '/api/screen/top' + (d ? '?date=' + d : '');
        st.textContent = '불러오는 중...';
      }
      try {
        const res = await fetch(url);
        const data = await res.json();
        if (!res.ok) {
          if (mode === 'top' && auto && res.status === 404) { st.textContent = '저장된 주간 결과가 없어 현재 조건으로 바로 계산합니다...'; return loadScreen('run'); }
          throw new Error(data.error || '실패');
        }
        screenData = data;
        renderScreen();
        fillOpIncomeLoop();
      } catch (e) {
        st.textContent = '오류: ' + e.message;
        document.getElementById('screenRegime').textContent = '표시할 스크리닝 결과가 없습니다.';
      }
    }

    function renderScreen() {
      if (!screenData) return;
      const meta = screenData.meta || {};
      const k = meta.kospi || {};
      const newFmt = !!(meta.weights && meta.weights.w3 != null);
      if (screenData.dates && screenData.dates.length) {
        const sel = document.getElementById('screenDate');
        sel.innerHTML = screenData.dates.map((d) => '<option value="' + d + '"' + (d === screenData.date ? ' selected' : '') + '>' + d + '</option>').join('');
      }
      const rg = document.getElementById('screenRegime');
      rg.style.background = '#f1f5f9';
      rg.innerHTML = '<b>기준일 ' + escHtml(meta.asof) + '</b> · 코스피 ' + Math.round(k.close || 0).toLocaleString() +
        ' · 코스피 수익률: 1개월 ' + sP(k.r21) + ' / 3개월 ' + sP(k.r63) + ' / 6개월 ' + sP(k.r126) + ' (20일 ' + sP(k.r20) + ' / 60일 ' + sP(k.r60) + ' / 120일 ' + sP(k.r120) + ')' +
        (meta.kospiError ? '<br /><span class="sc-warn">코스피 갱신 경고: ' + escHtml(meta.kospiError) + '</span>' : '') +
        (newFmt ? '' : '<br /><span class="sc-warn">이 저장 결과는 이전 방식(구버전 조건·복합 계산)으로 만든 것입니다. "스크리닝 실행"으로 새 기준 결과를 만드세요.</span>');

      const w = meta.weights || {};
      let stTxt;
      if (newFmt) {
        stTxt = '기준일 시세 ' + sN(meta.asof_rows) + '종목 중 보통주 · ' + (meta.pcMcap != null ? '시총 하위 ' + meta.pcMcap + '% 제외 · 20일 평균 거래대금 하위 ' + meta.pcTv + '% 제외' : '시총 ≥ ' + sN(meta.minMcapEok) + '억 · 20일 평균 거래대금 ≥ ' + sN(meta.minTvEok) + '억') + (meta.cutMcEok != null ? ' (이번 컷: 시총 ' + sN(meta.cutMcEok) + '억 · 20일 평균 거래대금 ' + sX(meta.cutTvEok, 1) + '억 이상만 통과)' : '') + ' · 주가 ≥ ' + sN(meta.minPrice) + '원 통과 ' + sN(meta.passed_floor) + '종목 · 각 목록 상위 50개' +
          ' · 복합 가중 2~3개월 ' + sX(w.w3) + ' / 2~6개월 ' + sX(w.w6) + (screenData.saved ? ' · 주간 결과로 저장됨' : '');
      } else {
        stTxt = '구버전 결과: 시총 ≥ ' + sN(meta.minMcapEok) + '억 · 하루 거래대금 ≥ ' + sN(meta.minTvEok) + '억';
      }
      if (meta.no_mcap > 0 && meta.asof_rows > 0 && meta.no_mcap / meta.asof_rows > 0.2) {
        stTxt += ' · 주의: 기준일 행 중 시가총액이 아직 저장되지 않은 종목이 ' + sN(meta.no_mcap) + '개 있어 결과에서 빠졌습니다(④ 데이터 관리에서 "오늘자 시세 받기"를 한 번 누르고 몇 분 뒤 다시 실행하세요).';
      }
      if (meta.clamped && meta.clamped.length) stTxt += ' · ' + meta.clamped.join(' ');
      if (((screenData.lists || {}).rw || []).length < 50) {
        stTxt += ' · 안내: 목록이 50개보다 적은 이유 — 조건 통과 종목 중 가격 이력(약 6개월)이 모두 저장된 종목만 계산됩니다. 현재 이력 확보 종목은 ' + sN(meta.backfilled) + '개이고 하루 150종목씩 늘어납니다.';
      }
      document.getElementById('screenStatus').textContent = stTxt;

      const lists = screenData.lists || {};
      const tabs = document.getElementById('screenTabs');
      tabs.style.display = 'flex';
      tabs.innerHTML = SCREEN_LISTS.map((L) => '<button class="' + (L.key === screenTab ? 'toggle-active' : '') + '" onclick="setScreenTab(&quot;' + L.key + '&quot;)" style="padding:6px 10px; font-size:13px;">' + L.label + '</button>').join('');
      const curL = SCREEN_LISTS.filter((L) => L.key === screenTab)[0];
      document.getElementById('screenDesc').textContent = curL ? curL.desc : '';

      const overlap = {};
      SCREEN_LISTS.forEach((L) => { (lists[L.key] || []).forEach((r) => { overlap[r.stock_code] = (overlap[r.stock_code] || 0) + 1; }); });
      const kc = (col, extra) => ((screenTab === col ? 'sc-key ' : '') + (extra || '')).trim();
      const fh = (n, t) => '<th class="f f' + n + '">' + t + '</th>';
      const head = '<tr>' + fh(1, '순위') + fh(2, '종목') + fh(3, '현재가') + fh(4, '시총(억)') + fh(5, '20일평균<br />거래대금(억)') + fh(6, '영업이익(억)') +
        '<th>1개월</th><th class="' + kc('r20') + '">20일</th><th class="' + kc('r60') + '">60일</th><th class="' + kc('r120') + '">120일</th><th class="' + kc('rw') + '">복합점수</th>' +
        '<th class="' + kc('m16') + '">6-1</th><th class="' + kc('rs1') + '">RS 1M</th><th class="' + kc('rs3') + '">RS 3M</th><th class="' + kc('rs6') + '">RS 6M</th><th>겹친 목록</th></tr>';
      const rows = lists[screenTab] || [];
      const body = rows.map((r, i) => {
        let opCell;
        if (r.op) {
          opCell = '<td class="f f6 ' + (r.op.v < 0 ? 'sc-bad' : '') + '">' + sN(r.op.v) + ' <span style="font-size:10px; color:var(--text-muted);">' + (r.op.year || '') + (r.op.src === 'dart' ? ' DART' : '') + '</span></td>';
        } else {
          opCell = '<td class="f f6">' + (opFillRunning ? '조회 중…' : 'N/A') + '</td>';
        }
        const ret = (v, col) => sCell(sP(v), kc(col, v != null && v > 0 ? 'sc-good' : (v != null && v < 0 ? 'sc-bad' : '')));
        const rs = (v, col) => sCell(sX(v, 3), kc(col, v != null && v > 1 ? 'sc-good' : (v != null && v < 1 ? 'sc-bad' : '')));
        const hot = r.hot ? ' <span class="sc-warn" title="최근 1개월 수익률이 후보 중 상위 5%: 단기 과열·반전 위험">⚠</span>' : '';
        const comp = newFmt ? sCell(r.rw == null ? 'N/A' : (r.rw * 100).toFixed(1), kc('rw')) : ret(r.rw, 'rw');
        return '<tr><td class="f f1">' + (i + 1) + '</td>' +
          '<td class="f f2"><a href="javascript:void(0)" onclick="goAnalyzeCode(&quot;' + r.stock_code + '&quot;)" title="클릭하면 종목분석으로 이동" style="color:var(--primary-dark); font-weight:700; text-decoration:none;">' + escHtml(r.corp_name) + '</a>' + hot + '<br /><span style="color:var(--text-muted); font-size:11px;">' + r.stock_code + '</span></td>' +
          '<td class="f f3">' + sN(r.c0) + '</td><td class="f f4">' + sN(r.mcapEok) + '</td><td class="f f5">' + sN(r.tvEok) + '</td>' + opCell +
          ret(r.r21, 'r21') + ret(r.r20, 'r20') + ret(r.r60, 'r60') + ret(r.r120, 'r120') + comp + ret(r.m16, 'm16') + rs(r.rs1, 'rs1') + rs(r.rs3, 'rs3') + rs(r.rs6, 'rs6') +
          sCell(overlap[r.stock_code] + '/' + SCREEN_LISTS.length, overlap[r.stock_code] >= 4 ? 'sc-good' : '') + '</tr>';
      }).join('');
      const wrap = document.getElementById('screenWrap');
      wrap.innerHTML = '<table class="scr" style="width:max-content; min-width:100%;"><thead>' + head + '</thead><tbody>' + (body || '<tr><td colspan="16">조건에 맞는 종목이 없습니다(가격 이력이 부족하거나 조건이 너무 엄격할 수 있습니다).</td></tr>') + '</tbody></table>';
      wrap.style.display = '';
    }

    // 지금 보이는 목록의 상위 종목 중 재무데이터가 없는 종목을 서버 대기열에 등록한다(서버가 1분에 약 1개 기간씩 DART에서 받아 저장).
    async function prepareFinancials() {
      const st = document.getElementById('prepStatus');
      if (!screenData) { st.textContent = '먼저 스크리닝 결과를 불러오세요.'; return; }
      const n = Number(document.getElementById('prepN').value) || 20;
      const rows = ((screenData.lists || {})[screenTab] || []).slice(0, n);
      let have;
      try { have = await loadDbCompanies(true); } catch (e) { st.textContent = '오류: ' + e.message; return; }
      const haveNames = {};
      have.forEach(function (c) { haveNames[c.corp_name] = 1; });
      const need = rows.filter(function (r) { return !haveNames[r.corp_name] && r.corp_name !== r.stock_code; });
      if (!need.length) { st.textContent = '상위 ' + rows.length + '개 종목 모두 재무데이터가 이미 있습니다.'; return; }
      const y1 = new Date().getFullYear(), y0 = y1 - 5;
      const periods = need.length * (y1 - y0 + 1) * 4;
      if (!confirm(need.length + '개 종목(' + y0 + '~' + y1 + '년, 약 ' + periods + '개 기간)을 서버 대기열에 등록합니다. 서버가 1분에 약 1개 기간씩 받으므로 완료까지 약 ' + Math.ceil(periods / 60) + '시간 걸립니다(화면을 꺼도 계속 진행됩니다). 진행할까요?')) return;
      let ok = 0, fail = 0;
      for (let i = 0; i < need.length; i++) {
        st.textContent = '대기열 등록 중... ' + (i + 1) + '/' + need.length;
        try {
          const res = await fetch('/api/queue-fetch?corp_name=' + encodeURIComponent(need[i].corp_name) + '&start_year=' + y0 + '&end_year=' + y1);
          if (res.ok) ok++; else fail++;
        } catch (e) { fail++; }
      }
      st.textContent = ok + '개 종목을 대기열에 등록했습니다' + (fail ? ' (' + fail + '개는 등록 실패)' : '') + '. ④ 데이터 관리에서 진행 상황을 볼 수 있습니다.';
    }

    // 영업이익이 없는 종목을 서버가 12개씩 data.go.kr에서 받아 오도록 반복 호출한다(받은 값은 서버 DB에 저장되어 다음부터는 바로 나옴).
    async function fillOpIncomeLoop() {
      if (opFillRunning || !screenData) return;
      const lists = screenData.lists || {};
      const need = {};
      SCREEN_LISTS.forEach((L) => { (lists[L.key] || []).forEach((r) => { if (!(r.op && r.op.src === 'datago')) need[r.stock_code] = 1; }); });
      const codes = Object.keys(need);
      const st = document.getElementById('screenOpStatus');
      if (!codes.length) { st.textContent = ''; return; }
      opFillRunning = true;
      renderScreen();
      try {
        for (let round = 0; round < 40; round++) {
          const res = await fetch('/api/screen/fill-opincome?codes=' + codes.join(','));
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '영업이익 조회 실패');
          SCREEN_LISTS.forEach((L) => { (lists[L.key] || []).forEach((r) => {
            const v = data.values ? data.values[r.stock_code] : undefined;
            if (v) r.op = v;
          }); });
          st.textContent = '영업이익(data.go.kr) 조회 중... 남은 종목 ' + data.remaining + '개';
          renderScreen();
          if (!data.remaining) { st.textContent = '영업이익 조회 완료(공공데이터에 없는 종목은 DART 값 또는 N/A).'; break; }
          if (!data.processed) { st.textContent = '영업이익 조회가 진행되지 않습니다: ' + ((data.errors && data.errors[0]) || '원인 불명'); break; }
        }
      } catch (e) {
        st.textContent = '영업이익 조회 오류: ' + e.message;
      } finally {
        opFillRunning = false;
        renderScreen();
      }
    }

    function downloadScreenCsv() {
      if (!screenData) { alert('먼저 결과를 불러와주세요.'); return; }
      const cols = ['목록', '순위', '종목명', '종목코드', '현재가', '시가총액(억원)', '20일평균거래대금(억원)', '영업이익(억원)', '영업이익연도', '영업이익출처', '1개월', '20일', '60일', '120일', '복합점수(0~100)', '6-1', 'RS1개월', 'RS3개월', 'RS6개월'];
      const n = (v, d) => (v == null ? '' : Number(v).toFixed(d == null ? 4 : d));
      const newFmt = !!((screenData.meta || {}).weights && screenData.meta.weights.w3 != null);
      const lines = [cols.join(',')];
      const lists = screenData.lists || {};
      SCREEN_LISTS.forEach((L) => {
        (lists[L.key] || []).forEach((r, i) => {
          const o = r.op || {};
          lines.push(['"' + L.label + '"', i + 1, '"' + String(r.corp_name).split('"').join('""') + '"', r.stock_code, n(r.c0, 0), n(r.mcapEok, 0), n(r.tvEok, 1), n(o.v, 1), o.year || '', o.src || '',
            n(r.r21), n(r.r20), n(r.r60), n(r.r120), newFmt ? n(r.rw == null ? null : r.rw * 100, 1) : n(r.rw), n(r.m16), n(r.rs1), n(r.rs3), n(r.rs6)].join(','));
        });
      });
      const bom = String.fromCharCode(0xFEFF);
      const blob = new Blob([bom + lines.join(String.fromCharCode(10))], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = 'screen_' + (screenData.date || 'result') + '.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }

    // 지금 보이는 재무제표 표(분기별/연간)를 엑셀에서 바로 열 수 있는 CSV(UTF-8 BOM)로 저장한다.
    function downloadFinancialCsv() {
      if (!currentRows || !currentRows.length) { alert('먼저 종목을 분석해주세요.'); return; }
      const NL = String.fromCharCode(10);
      const q = (t) => '"' + String(t == null ? '' : t).split('"').join('""') + '"';
      const head = COLUMN_GROUPS.flatMap((g) => g.cols.map((c) => q(g.name + ' / ' + String(c.label).split('<br/>').join(' ').split('<br />').join(' '))));
      const lines = [head.join(',')];
      currentRows.forEach((r) => {
        lines.push(COLUMNS.map((col) => {
          const v = rawValue(r, col);
          return col.type === 'text' ? q(v == null ? '' : v) : (v == null ? '' : String(v));
        }).join(','));
      });
      const blob = new Blob([String.fromCharCode(0xFEFF) + lines.join(NL)], { type: 'text/csv;charset=utf-8' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = (document.getElementById('corpName').value.trim() || 'financial') + '_' + (viewMode === 'quarterly' ? '분기' : '연간') + '.csv';
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }

    async function loadFromDb() { return analyzeStock(); }

    // ===== 종목분석: "분석하기" 한 번으로 조회 → (없으면 DART 자동 수집) → 현재가 → 요약·분류·밸류에이션 → ROE 분해까지 =====
    let subTab = 'summary';
    let analyzeSeq = 0;
    function setSubTab(k) {
      subTab = k;
      ['summary', 'table', 'five', 'compare'].forEach(function (x) {
        document.getElementById('sub_' + x).style.display = x === k ? '' : 'none';
        document.getElementById('subBtn_' + x).className = x === k ? 'toggle-active' : '';
      });
      if (k === 'table' && chartSel.length) renderChartPanel(false);
    }
    async function analyzeStock() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      statusEl.className = '';
      if (!corpName) { statusEl.textContent = '종목명을 입력해주세요.'; return; }
      const seq = ++analyzeSeq;
      document.getElementById('stockBody').style.display = 'none';
      document.getElementById('stockMeta').innerHTML = '';
      document.getElementById('currentPrice').value = '';
      document.getElementById('priceNote').textContent = '';
      statusEl.textContent = '"' + corpName + '" 조회 중...';
      let data;
      try { data = await fetchRowsFromDb(corpName); }
      catch (e) { statusEl.textContent = '오류: ' + e.message; statusEl.className = 'error'; return; }
      if (seq !== analyzeSeq) return;
      rawRows = data.rows;
      if (rawRows.filter(isUsable).length === 0) {
        statusEl.textContent = '"' + data.corp_name + '"은(는) 저장된 재무데이터가 없어 DART에서 새로 받아옵니다(약 1~2분, 화면을 켜두세요).';
        await fetchAndSave();
        if (seq !== analyzeSeq) return;
        if (rawRows.filter(isUsable).length === 0) return; // fetchAndSave가 남긴 오류 문구를 그대로 둔다
      }
      await showStockResult(seq);
    }
    async function showStockResult(seq) {
      document.getElementById('stockBody').style.display = '';
      renderStockMeta();
      applyView();
      const stEl = document.getElementById('status');
      const stTxt = stEl.textContent || '';
      if (stTxt.indexOf('조회 중') >= 0 || stTxt.indexOf('받는 중') >= 0 || stTxt.indexOf('받아옵니다') >= 0) stEl.textContent = '';
      await fetchLatestPrice();
      if (seq != null && seq !== analyzeSeq) return;
      renderSummary();
      renderFiveStep();
      macroCorrDirty = true;
      document.getElementById('macroCorrFor').textContent = '대상 종목: ' + document.getElementById('corpName').value.trim();
      setSubTab(subTab);
    }
    function renderStockMeta() {
      const usable = rawRows.filter(isUsable).sort((x, y) => Number(x.period_order) - Number(y.period_order));
      const el = document.getElementById('stockMeta');
      if (!usable.length) { el.innerHTML = ''; return; }
      const first = usable[0], last = usable[usable.length - 1];
      el.innerHTML = '저장된 재무데이터: <b>' + escHtml(first.period_label) + ' ~ ' + escHtml(last.period_label) + '</b> (' + usable.length + '개 기간) ' +
        '<button onclick="updateLatest()" style="padding:4px 10px; font-size:12.5px;" title="최근 2년 치를 DART에서 다시 받아 최신 공시를 반영합니다(약 10~20초)">최신 공시 업데이트</button>';
    }
    async function updateLatest() {
      const usable = rawRows.filter(isUsable);
      if (!usable.length) return;
      const lastYear = Math.max.apply(null, usable.map((r) => Number(r.bsns_year)));
      const seq = ++analyzeSeq;
      await fetchAndSave({ fromYear: lastYear - 1 });
      await showStockResult(seq);
    }
    async function fullRefetch() {
      const seq = ++analyzeSeq;
      await fetchAndSave();
      if (rawRows.filter(isUsable).length) await showStockResult(seq);
    }

    async function rawCheck(kind) {
      const corpName = document.getElementById('corpName').value.trim();
      const year = document.getElementById('rawYear').value;
      const reprtCode = document.getElementById('rawReprt').value;
      const statusEl = document.getElementById('status');
      const rawEl = document.getElementById('raw');
      statusEl.textContent = '원본 조회 중...';
      rawEl.textContent = '';
      try {
        const res = await fetch(\`/api/raw?kind=\${kind}&corp_name=\${encodeURIComponent(corpName)}&bsns_year=\${year}&reprt_code=\${reprtCode}\`);
        const data = await res.json();
        statusEl.textContent = \`\${kind} 원본 (\${year}년, reprt_code=\${reprtCode})\`;
        rawEl.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }

    async function rawCheckFs() {
      const corpName = document.getElementById('corpName').value.trim();
      const year = document.getElementById('rawYear').value;
      const reprtCode = document.getElementById('rawReprt').value;
      const fsDiv = document.getElementById('rawFsDiv').value;
      const statusEl = document.getElementById('status');
      const rawEl = document.getElementById('raw');
      statusEl.textContent = '원본 조회 중...';
      rawEl.textContent = '';
      try {
        const res = await fetch(\`/api/raw?kind=fnlttSinglAcntAll&corp_name=\${encodeURIComponent(corpName)}&bsns_year=\${year}&reprt_code=\${reprtCode}&fs_div=\${fsDiv}\`);
        const data = await res.json();
        statusEl.textContent = \`재무제표 원본 (\${year}년, reprt_code=\${reprtCode}, \${fsDiv})\`;
        const cfRows = (data.list || []).filter((r) => ['CF', 'IS', 'CIS'].includes(r.sj_div));
        rawEl.textContent = JSON.stringify(cfRows.length ? cfRows : data, null, 2);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }

    async function rawCheckCbBw(kind) {
      const corpName = document.getElementById('corpName').value.trim();
      const today = new Date();
      const todayStr = today.getFullYear() + String(today.getMonth() + 1).padStart(2, '0') + String(today.getDate()).padStart(2, '0');
      const start = document.getElementById('cbwStart').value.trim() || '20150101';
      const end = document.getElementById('cbwEnd').value.trim() || todayStr;
      const statusEl = document.getElementById('status');
      const el = document.getElementById('cbwRaw');
      statusEl.textContent = '조회 중...';
      el.textContent = '';
      try {
        const res = await fetch(\`/api/raw?kind=\${kind}&corp_name=\${encodeURIComponent(corpName)}&bgn_de=\${start}&end_de=\${end}\`);
        const data = await res.json();
        statusEl.textContent = \`\${kind} 조회 완료 (\${start}~\${end})\`;
        el.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }

    // ===== DB에 저장된 종목 목록 =====
    // 무료 한도 절약: 서버는 PK 인덱스만 훑는 집계 1회(+ corp_master 조인), 결과는 브라우저에 캐시하고 새 저장이 있을 때만 다시 받는다.
    let dbCompanies = null;
    let dbListShown = [];
    function escHtml(t) { return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); }
    async function loadDbCompanies(force) {
      if (dbCompanies && !force) return dbCompanies;
      const res = await fetch('/api/db/companies', force ? { cache: 'no-store' } : undefined); // 새로고침 버튼은 브라우저 30초 캐시를 건너뜀
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'DB 종목 목록 조회 실패');
      dbCompanies = data.companies;
      document.getElementById('dbCompanyList').innerHTML = dbCompanies.map((c) => '<option value="' + escHtml(c.corp_name) + '"></option>').join('');
      return dbCompanies;
    }
    async function toggleDbList(force) {
      const box = document.getElementById('dbListWrap');
      if (box.style.display === 'block' && !force) { box.style.display = 'none'; return; }
      box.style.display = 'block';
      document.getElementById('dbListBody').textContent = '불러오는 중...';
      try {
        await loadDbCompanies(!!force);
        renderDbList();
      } catch (e) {
        document.getElementById('dbListBody').textContent = '오류: ' + e.message;
      }
    }
    function renderDbList() {
      const q = document.getElementById('dbListFilter').value.trim().toLowerCase();
      const all = dbCompanies || [];
      dbListShown = all.filter((c) => !q || c.corp_name.toLowerCase().indexOf(q) >= 0 || String(c.stock_code || '').indexOf(q) >= 0);
      document.getElementById('dbListCount').textContent = '총 ' + all.length + '개 종목' + (q ? ' 중 ' + dbListShown.length + '개 일치' : '');
      document.getElementById('dbListBody').innerHTML = dbListShown.length
        ? dbListShown.map((c, i) =>
          '<button class="db-chip" onclick="pickDbCompany(' + i + ')" title="클릭하면 DB에서 바로 조회">' + escHtml(c.corp_name) +
          '<small>' + escHtml(c.stock_code || '') + ' · ' + c.first_year + '~' + c.last_year + ' · ' + c.n + '건</small></button>').join('')
        : '<span style="color:var(--text-muted); font-size:13px;">일치하는 종목이 없습니다.</span>';
    }
    function pickDbCompany(i) {
      const c = dbListShown[i];
      if (!c) return;
      document.getElementById('corpName').value = c.corp_name;
      updateWatchButtonState();
      switchTab('financial');
      analyzeStock();
    }

    // ===== 항목 추이 차트 (최대 3개, 위아래 분리 패널 + 겹쳐보기) =====
    // 설계 근거: 사람이 동시에 비교할 수 있는 항목 수는 대략 3~4개(Cowan 2001 작업기억 용량)라 상한을 3으로 둔다.
    // 단위가 다른 지표(예: 환율과 ROE)를 한 축에 겹치면 이중축 착시가 생기므로(Few·Tufte의 small multiples 권고),
    // 기본은 "같은 x축을 공유하는 위아래 분리 패널"이고, 모양(동행 여부)만 보고 싶을 때 "겹쳐보기(0~100 환산)"를 쓴다.
    const CHART_MAX = 3;
    const CHART_COLORS = ['#2563eb', '#ea580c', '#059669'];
    let chartSel = [];        // [{ key, label }]
    let chartOverlay = false;
    let chartHoverIdx = null;
    let chartGeom = null;     // 마우스 위치 -> 기간 인덱스 변환용 (그릴 때마다 갱신)
    let chartListenersReady = false;

    function showChart(key, label) {
      const i = chartSel.findIndex((s) => s.key === key);
      if (i >= 0) {
        chartSel.splice(i, 1); // 이미 있는 항목을 다시 더블클릭하면 제거
      } else {
        if (chartSel.length >= CHART_MAX) chartSel.shift(); // 4번째를 추가하면 가장 오래된 것부터 교체
        chartSel.push({ key: key, label: label });
      }
      chartHoverIdx = null;
      renderChartPanel(true);
    }
    function removeChartItem(key) {
      chartSel = chartSel.filter((s) => s.key !== key);
      chartHoverIdx = null;
      renderChartPanel(false);
    }
    function clearCharts() { chartSel = []; renderChartPanel(false); }
    function toggleChartOverlay() { chartOverlay = !chartOverlay; chartHoverIdx = null; renderChartPanel(false); }

    function shortPeriodLabel(lb) {
      const m = String(lb).match(/^(\d{4})\s*(.*)$/);
      if (!m) return String(lb);
      const yy = "'" + m[1].slice(2);
      const rest = m[2];
      if (rest.indexOf('1분기') >= 0) return yy + ' 1Q';
      if (rest.indexOf('2분기') >= 0 || rest.indexOf('반기') >= 0) return yy + ' 2Q';
      if (rest.indexOf('3분기') >= 0) return yy + ' 3Q';
      if (rest.indexOf('4분기') >= 0) return yy + ' 4Q';
      if (rest.indexOf('연간') >= 0) return yy;
      if (rest.indexOf('사업보고서') >= 0) return yy + ' FY';
      return yy + (rest ? ' ' + rest : '');
    }
    function fmtChartVal(v, type) {
      if (v == null) return 'N/A';
      if (type === 'percent') return (v * 100).toFixed(1) + '%';
      if (type === 'ratio') return v.toFixed(2);
      if (type === 'macro') return Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 });
      const a = Math.abs(v);
      if (a >= 1e12) return (v / 1e12).toFixed(1) + '조';
      if (a >= 1e8) return Math.round(v / 1e8).toLocaleString() + '억';
      if (a >= 1e4) return Math.round(v / 1e4).toLocaleString() + '만';
      return Number(v).toLocaleString(undefined, { maximumFractionDigits: 2 });
    }
    function fmtChartDelta(cur, prev, type) {
      if (cur == null || prev == null) return '';
      if (type === 'percent') {
        const d = (cur - prev) * 100;
        return (d >= 0 ? '▲' : '▼') + Math.abs(d).toFixed(1) + '%p';
      }
      if (!prev) return '';
      const d = (cur - prev) / Math.abs(prev) * 100;
      return (d >= 0 ? '▲' : '▼') + Math.abs(d).toFixed(1) + '%';
    }

    function collectChartSeries() {
      const rows = currentRows.filter((r) => !r.error);
      const getVal = window.__rawValue || ((r, c) => r[c.key]);
      const xs = rows.map((r) => r.period_label);
      const series = chartSel.map((s, idx) => {
        const col = (window.__columnDefs || []).find((c) => c.key === s.key) || { key: s.key };
        const vals = rows.map((r) => { const v = getVal(r, col); return (v == null || isNaN(v)) ? null : Number(v); });
        return { key: s.key, label: s.label, type: col.type, vals: vals, color: CHART_COLORS[idx % CHART_COLORS.length] };
      });
      return { xs: xs, series: series };
    }

    function renderChartPanel(scroll) {
      const wrap = document.getElementById('chartWrap');
      if (chartSel.length === 0) { wrap.style.display = 'none'; return; }
      wrap.style.display = 'block';
      document.getElementById('chartTitle').textContent = '항목 추이 비교 (' + chartSel.length + '/' + CHART_MAX + ')';
      let chips = chartSel.map((s, i) =>
        '<span class="chart-chip" style="border-color:' + CHART_COLORS[i] + ';"><i style="background:' + CHART_COLORS[i] + ';"></i>' + s.label +
        ' <b onclick="removeChartItem(\\'' + s.key + '\\')" title="제거">×</b></span>').join('');
      const overlayBtn = chartSel.length >= 2
        ? '<button class="' + (chartOverlay ? 'toggle-active' : '') + '" style="padding:4px 10px; font-size:13px;" onclick="toggleChartOverlay()">' + (chartOverlay ? '분리해서 보기' : '겹쳐보기(0~100 환산)') + '</button>'
        : '';
      chips += overlayBtn + '<button style="padding:4px 10px; font-size:13px;" onclick="clearCharts()">모두 지우기</button>';
      document.getElementById('chartChips').innerHTML = chips;
      const data = collectChartSeries();
      const empty = data.series.filter((s) => s.vals.every((v) => v == null));
      document.getElementById('chartNote').textContent = empty.length
        ? '데이터가 모두 N/A인 항목: ' + empty.map((s) => s.label).join(', ') + ' (거시지표는 "거시경제 데이터 갱신" 후 표시됩니다)'
        : (chartOverlay ? '겹쳐보기: 각 항목의 최소=0 ~ 최대=100으로 환산한 모양 비교용입니다. 마우스를 올리면 실제 값이 나옵니다.' : '열 제목을 더블클릭하면 항목이 추가(최대 3개)되고, 다시 더블클릭하면 제거됩니다. 마우스/터치로 같은 시점의 값을 한번에 볼 수 있습니다.');
      const canvas = document.getElementById('chartCanvas');
      const n = chartOverlay ? 1 : data.series.length;
      canvas.style.height = (chartOverlay ? 300 : n * 140 + 44) + 'px';
      if (!chartListenersReady) {
        const onMove = (ev) => {
          if (!chartGeom) return;
          const rect = canvas.getBoundingClientRect();
          const t = ev.touches && ev.touches.length ? ev.touches[0] : ev;
          const x = t.clientX - rect.left;
          let idx = Math.round((x - chartGeom.padL) / (chartGeom.xStep || 1));
          idx = Math.max(0, Math.min(chartGeom.count - 1, idx));
          if (idx !== chartHoverIdx) { chartHoverIdx = idx; drawCharts(); }
        };
        canvas.addEventListener('mousemove', onMove);
        canvas.addEventListener('touchmove', onMove, { passive: true });
        canvas.addEventListener('touchstart', onMove, { passive: true });
        canvas.addEventListener('mouseleave', () => { chartHoverIdx = null; drawCharts(); });
        window.addEventListener('resize', () => { if (chartSel.length) drawCharts(); });
        chartListenersReady = true;
      }
      drawCharts();
      if (scroll) wrap.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    }

    function drawCharts() {
      const canvas = document.getElementById('chartCanvas');
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const W = canvas.clientWidth, H = canvas.clientHeight;
      canvas.width = W * dpr; canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);
      const data = collectChartSeries();
      const count = data.xs.length;
      if (!count || !data.series.length) return;

      const padL = 58, padR = 26, padT = 8, padB = 30;
      const plotW = W - padL - padR;
      const xStep = count > 1 ? plotW / (count - 1) : 0;
      const xFor = (i) => count > 1 ? padL + i * xStep : padL + plotW / 2;
      chartGeom = { padL: padL, xStep: xStep, count: count };
      const FONT = '-apple-system, "Segoe UI", "Noto Sans KR", sans-serif';

      const panels = chartOverlay ? [{ overlay: true }] : data.series.map((s) => ({ s: s }));
      const gap = 14;
      const panelH = chartOverlay ? (H - padT - padB) : ((H - padT - padB - gap * (panels.length - 1)) / panels.length);

      panels.forEach((pn, pi) => {
        const top = padT + pi * (panelH + gap);
        const list = pn.overlay ? data.series : [pn.s];
        // y 범위
        let min = Infinity, max = -Infinity;
        const plotted = list.map((s) => {
          const nums = s.vals.filter((v) => v != null);
          if (!nums.length) return { s: s, vals: s.vals.map(() => null) };
          if (pn.overlay) {
            const lo = Math.min.apply(null, nums), hi = Math.max.apply(null, nums);
            return { s: s, vals: s.vals.map((v) => v == null ? null : (hi === lo ? 50 : (v - lo) / (hi - lo) * 100)) };
          }
          return { s: s, vals: s.vals };
        });
        plotted.forEach((p) => p.vals.forEach((v) => { if (v != null) { if (v < min) min = v; if (v > max) max = v; } }));
        if (!isFinite(min)) { min = 0; max = 1; }
        if (min === max) { min -= 1; max += 1; }
        const padY = (max - min) * 0.12;
        const lo = pn.overlay ? -4 : min - padY, hi = pn.overlay ? 104 : max + padY;
        const innerTop = top + (pn.overlay ? 6 : 22), innerH = panelH - (pn.overlay ? 6 : 22) - 4;
        const yFor = (v) => innerTop + innerH - ((v - lo) / (hi - lo)) * innerH;

        // 패널 배경/격자
        ctx.strokeStyle = '#eef2f7'; ctx.lineWidth = 1; ctx.fillStyle = '#94a3b8'; ctx.font = '10px ' + FONT; ctx.textAlign = 'right';
        for (let g = 0; g <= 2; g++) {
          const gv = pn.overlay ? g * 50 : (min + (max - min) * g / 2);
          const gy = yFor(gv);
          ctx.beginPath(); ctx.moveTo(padL, gy); ctx.lineTo(padL + plotW, gy); ctx.stroke();
          const t = pn.overlay ? String(Math.round(gv)) : fmtChartVal(gv, pn.s.type);
          ctx.fillText(t, padL - 6, gy + 3);
        }
        ctx.textAlign = 'left';
        if (!pn.overlay && min < 0 && max > 0) {
          ctx.strokeStyle = '#94a3b8'; ctx.setLineDash([4, 3]);
          ctx.beginPath(); ctx.moveTo(padL, yFor(0)); ctx.lineTo(padL + plotW, yFor(0)); ctx.stroke(); ctx.setLineDash([]);
        }
        // 패널 제목 + 최근값
        if (!pn.overlay) {
          const s = pn.s;
          ctx.fillStyle = s.color; ctx.font = '600 12px ' + FONT; ctx.fillText(s.label, padL, top + 12);
          let li = -1, pi2 = -1;
          for (let i = s.vals.length - 1; i >= 0; i--) { if (s.vals[i] != null) { if (li < 0) li = i; else { pi2 = i; break; } } }
          if (li >= 0) {
            const shown = chartHoverIdx != null ? chartHoverIdx : li;
            const cur = s.vals[shown];
            let prev = null;
            for (let i = shown - 1; i >= 0; i--) { if (s.vals[i] != null) { prev = s.vals[i]; break; } }
            const txt = (chartHoverIdx != null ? shortPeriodLabel(data.xs[shown]) + '  ' : '최근  ') + fmtChartVal(cur, s.type) + (cur != null ? '  ' + fmtChartDelta(cur, prev, s.type) : '');
            ctx.textAlign = 'right'; ctx.fillStyle = '#334155'; ctx.font = '12px ' + FONT;
            ctx.fillText(txt, padL + plotW, top + 12); ctx.textAlign = 'left';
          }
        }
        // 선/면
        plotted.forEach((p) => {
          const color = p.s.color;
          ctx.strokeStyle = color; ctx.lineWidth = 2; ctx.lineJoin = 'round';
          let started = false;
          ctx.beginPath();
          p.vals.forEach((v, i) => {
            if (v == null) { started = false; return; }
            const x = xFor(i), y = yFor(v);
            if (!started) { ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
          });
          ctx.stroke();
          if (!pn.overlay) { // 연속 구간 아래 옅은 면
            const grad = ctx.createLinearGradient(0, innerTop, 0, innerTop + innerH);
            grad.addColorStop(0, color + '33'); grad.addColorStop(1, color + '00');
            ctx.fillStyle = grad;
            let seg = [];
            const flush = () => {
              if (seg.length > 1) {
                ctx.beginPath(); ctx.moveTo(xFor(seg[0]), yFor(p.vals[seg[0]]));
                seg.forEach((i) => ctx.lineTo(xFor(i), yFor(p.vals[i])));
                ctx.lineTo(xFor(seg[seg.length - 1]), innerTop + innerH); ctx.lineTo(xFor(seg[0]), innerTop + innerH);
                ctx.closePath(); ctx.fill();
              }
              seg = [];
            };
            p.vals.forEach((v, i) => { if (v == null) flush(); else seg.push(i); });
            flush();
          }
          // 점: 기간이 적으면 모두, 많으면 최대/최소/최근만
          const idxs = [];
          p.vals.forEach((v, i) => { if (v != null) idxs.push(i); });
          const showAll = idxs.length <= 24;
          let iMax = -1, iMin = -1;
          idxs.forEach((i) => { if (iMax < 0 || p.vals[i] > p.vals[iMax]) iMax = i; if (iMin < 0 || p.vals[i] < p.vals[iMin]) iMin = i; });
          const last = idxs.length ? idxs[idxs.length - 1] : -1;
          idxs.forEach((i) => {
            const key = (i === last || i === iMax || i === iMin);
            if (!showAll && !key) return;
            ctx.beginPath(); ctx.arc(xFor(i), yFor(p.vals[i]), i === last ? 4 : 2.5, 0, Math.PI * 2);
            ctx.fillStyle = '#fff'; ctx.fill();
            ctx.lineWidth = 2; ctx.strokeStyle = color; ctx.stroke();
          });
          // 최대/최소 값 표기(분리 모드)
          if (!pn.overlay && idxs.length > 2) {
            ctx.font = '10px ' + FONT; ctx.fillStyle = color; ctx.textAlign = 'center';
            [[iMax, -7], [iMin, 14]].forEach((pair) => {
              if (pair[0] < 0 || iMax === iMin) return;
              const x = Math.max(padL + 16, Math.min(padL + plotW - 16, xFor(pair[0])));
              ctx.fillText(fmtChartVal(p.s.vals[pair[0]], p.s.type), x, yFor(p.vals[pair[0]]) + pair[1]);
            });
            ctx.textAlign = 'left';
          }
        });
        // 겹쳐보기 범례
        if (pn.overlay) {
          let lx = padL;
          ctx.font = '600 11px ' + FONT;
          data.series.forEach((s) => {
            ctx.fillStyle = s.color; ctx.fillRect(lx, top + 4, 10, 3);
            ctx.fillText(s.label, lx + 14, top + 9);
            lx += 14 + ctx.measureText(s.label).width + 14;
          });
        }
        // 호버 가이드선/점
        if (chartHoverIdx != null) {
          const hx = xFor(chartHoverIdx);
          ctx.strokeStyle = '#64748b'; ctx.lineWidth = 1; ctx.setLineDash([3, 3]);
          ctx.beginPath(); ctx.moveTo(hx, top); ctx.lineTo(hx, top + panelH); ctx.stroke(); ctx.setLineDash([]);
          plotted.forEach((p) => {
            const v = p.vals[chartHoverIdx];
            if (v == null) return;
            ctx.beginPath(); ctx.arc(hx, yFor(v), 5, 0, Math.PI * 2); ctx.fillStyle = p.s.color; ctx.fill();
            ctx.lineWidth = 2; ctx.strokeStyle = '#fff'; ctx.stroke();
          });
          if (pn.overlay) { // 겹쳐보기는 실제값을 선 위 툴팁으로
            const lines = data.series.map((s) => s.label + ': ' + fmtChartVal(s.vals[chartHoverIdx], s.type));
            ctx.font = '11px ' + FONT;
            const tw = Math.max.apply(null, lines.map((t) => ctx.measureText(t).width)) + 16;
            const th = lines.length * 15 + 8;
            let tx = hx + 10; if (tx + tw > padL + plotW) tx = hx - tw - 10;
            ctx.fillStyle = 'rgba(15,23,42,0.92)'; ctx.fillRect(tx, top + 22, tw, th);
            ctx.textAlign = 'left';
            lines.forEach((t, k) => { ctx.fillStyle = data.series[k].color === '#2563eb' ? '#93c5fd' : (data.series[k].color === '#ea580c' ? '#fdba74' : '#6ee7b7'); ctx.fillText(t, tx + 8, top + 22 + 15 * (k + 1)); });
          }
        }
      });

      // x축 라벨(공유)
      const axisY = H - padB + 16;
      ctx.fillStyle = '#64748b'; ctx.font = '10px ' + FONT; ctx.textAlign = 'center';
      const step = Math.max(1, Math.ceil(count * 54 / Math.max(plotW, 1)));
      for (let i = count - 1; i >= 0; i -= step) ctx.fillText(shortPeriodLabel(data.xs[i]), xFor(i), axisY);
      if (chartHoverIdx != null) {
        const t = shortPeriodLabel(data.xs[chartHoverIdx]);
        const w = ctx.measureText(t).width + 10;
        const hx = Math.max(padL + w / 2, Math.min(padL + plotW - w / 2, xFor(chartHoverIdx)));
        ctx.fillStyle = '#0f172a'; ctx.fillRect(hx - w / 2, axisY - 11, w, 15);
        ctx.fillStyle = '#fff'; ctx.fillText(t, hx, axisY);
      }
      ctx.textAlign = 'left';
    }

    // --- 거시경제 지표(환율·금리·WTI) ---
    let macroSeriesCache = null; // { usdkrw: [{date,value}, ...], ... } — /api/macro/series 결과를 한 번만 받아 재사용

    async function refreshMacroData() {
      const statusEl = document.getElementById('macroStatus');
      statusEl.textContent = '거시경제 데이터를 받아오는 중입니다(최근 9년치, 지표 5개 — 처음엔 여러 번에 나눠 자동으로 이어받습니다)...';
      try {
        let total = 0, last = null;
        for (let round = 1; round <= 30; round++) {
          const res = await fetch('/api/macro/refresh');
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '갱신 실패');
          last = data;
          total += Object.values(data.summary).reduce((a, s) => a + (s.points || 0), 0);
          statusEl.textContent = '받는 중... ' + round + '회차, 누적 ' + total.toLocaleString() + '개 저장';
          if (!data.partial) break;
        }
        macroSeriesCache = null; // 캐시 무효화 — 다음 조회 때 새로 받아오게
        macroLoadFailed = false;
        if (rawRows.length > 0) applyView(); // 표의 거시경제 열도 새 데이터로 다시 채움
        const lines = Object.entries(last.summary).map(([key, s]) =>
          s.error ? key + ': 실패 — ' + s.error : s.label + ': ' + (s.partial ? '일부 저장(다시 눌러 이어받기)' : (s.points + '개 저장(마지막 회차)')));
        statusEl.textContent = '완료 — 이번에 총 ' + total.toLocaleString() + '개 저장' + String.fromCharCode(10) + lines.join(String.fromCharCode(10));
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }

    async function loadMacroSeries() {
      if (macroSeriesCache) return macroSeriesCache;
      const res = await fetch('/api/macro/series');
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || '거시경제 데이터 조회 실패');
      const grouped = {};
      for (const r of data.rows) {
        if (!grouped[r.series]) grouped[r.series] = [];
        grouped[r.series].push({ date: r.date, value: r.value });
      }
      macroSeriesCache = grouped;
      return grouped;
    }

    // YYYYMMDD 두 날짜의 일수 차이(b - a)
    function daysBetweenYmd(a, b) {
      const toMs = (s) => Date.UTC(Number(s.slice(0, 4)), Number(s.slice(4, 6)) - 1, Number(s.slice(6, 8)));
      return Math.round((toMs(b) - toMs(a)) / 86400000);
    }

    // targetDate(YYYYMMDD) 이전(포함) 중 가장 최근 값을 찾는다 — 미래 데이터를 끌어다 쓰지 않도록(point-in-time).
    // maxGapDays를 주면, 찾은 값이 목표일보다 그 일수보다 더 오래 전 값일 때는 null(그 시점 값이 아니므로).
    function macroValueAsOf(series, targetDate, maxGapDays) {
      if (!series || series.length === 0) return null;
      let best = null;
      for (const p of series) {
        if (p.date <= targetDate && (!best || p.date > best.date)) best = p;
      }
      if (!best) return null;
      if (maxGapDays != null && daysBetweenYmd(best.date, targetDate) > maxGapDays) return null;
      return best.value;
    }

    function pearsonCorrelation(xs, ys) {
      const n = xs.length;
      if (n < 3) return null; // 표본 2개 이하면 상관계수가 무의미
      const mean = (arr) => arr.reduce((a, b) => a + b, 0) / arr.length;
      const mx = mean(xs), my = mean(ys);
      let num = 0, dx2 = 0, dy2 = 0;
      for (let i = 0; i < n; i++) {
        const dx = xs[i] - mx, dy = ys[i] - my;
        num += dx * dy; dx2 += dx * dx; dy2 += dy * dy;
      }
      if (dx2 === 0 || dy2 === 0) return null;
      return num / Math.sqrt(dx2 * dy2);
    }

    // ===== 거시경제 "변화"와 실적·주가수익률의 상관관계 =====
    // 설계 원칙(학술적 이유):
    //  1) 수준(level)이 아니라 변화로 비교한다. 환율·금리·유가의 "수준"과 성장률을 바로 상관시키면 시간 추세 때문에 실제로는 관계가 없어도
    //     높은 상관이 나오는 가짜 회귀(spurious regression, Granger & Newbold 1974)가 생긴다. 그래서 같은 기간의 변화율(환율·유가) 또는 변화폭(금리, %p)을 쓴다.
    //  2) 실적 성장률·주가수익률·거시 변화는 모두 "같은 1년 창"(연간: 전년 말→올해 말, 분기: 1년 전 같은 분기 말→이번 분기 말)에서 계산해 시점을 맞춘다. 공시일이 아니라 회계기간 말 기준이다.
    //  3) 상관계수는 극단값(예: 순이익이 몇 배 급증)에 덜 민감한 스피어만 순위상관(ρ)을 쓴다.
    //  4) 표본이 매우 적어 유의성(양측 5%)을 표시한다. 분기 YoY 창은 인접 분기끼리 3/4가 겹쳐 독립이 아니므로 유효 표본을 n/4로 보고 판정한다.
    //  5) 표의 칸이 많아 우연히 유의하게 나오는 칸이 섞인다(다중검정). 인과가 아니라 "참고용 동행성"이다.
    const MACRO_KIND = { usdkrw: 'pct', msb1y: 'diff', ktb3y: 'diff', ktb10y: 'diff', wti: 'pct' };
    const MACRO_CHG_LABELS = { usdkrw: '원/달러 환율 변화율', msb1y: '통안증권(1년) 변화(%p)', ktb3y: '국고채(3년) 변화(%p)', ktb10y: '국고채(10년) 변화(%p)', wti: 'WTI 변화율' };
    let macroCorrDirty = true;

    function rankAvg(arr) {
      const idx = arr.map(function (v, i) { return i; }).sort(function (a, b) { return arr[a] - arr[b]; });
      const r = new Array(arr.length);
      let i = 0;
      while (i < idx.length) {
        let j = i;
        while (j + 1 < idx.length && arr[idx[j + 1]] === arr[idx[i]]) j++;
        const avg = (i + j) / 2 + 1;
        for (let k = i; k <= j; k++) r[idx[k]] = avg;
        i = j + 1;
      }
      return r;
    }
    function spearmanCorrelation(xs, ys) {
      if (xs.length < 3) return null;
      return pearsonCorrelation(rankAvg(xs), rankAvg(ys));
    }
    const T_CRIT_95 = [null, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042];
    function tCrit95(df) { return df < 1 ? Infinity : df <= 30 ? T_CRIT_95[df] : df <= 60 ? 2.0 : df <= 120 ? 1.98 : 1.96; }
    function corrSignificant(r, nEff) {
      const df = nEff - 2;
      if (df < 1) return false;
      if (Math.abs(r) >= 0.9999) return true;
      return Math.abs(r) * Math.sqrt(df / (1 - r * r)) > tCrit95(df);
    }
    function macroChange(macro, key, startYmd, endYmd) {
      const a = macroValueAsOf(macro[key], startYmd, MACRO_MAX_GAP_DAYS);
      const b = macroValueAsOf(macro[key], endYmd, MACRO_MAX_GAP_DAYS);
      if (a == null || b == null) return null;
      if (MACRO_KIND[key] === 'diff') return b - a;
      return a > 0 ? b / a - 1 : null;
    }
    function macroChanges(macro, startYmd, endYmd) {
      const out = {};
      Object.keys(MACRO_LABELS).forEach(function (k) { out[k] = macroChange(macro, k, startYmd, endYmd); });
      return out;
    }
    // 전기 대비 성장률. 분모(전기값)가 0 이하이면 정의되지 않으므로 null(부호가 바뀌는 "성장률"은 해석이 왜곡됨).
    function growthOf(cur, prev) { return (cur != null && prev != null && prev > 0) ? cur / prev - 1 : null; }
    function priceRetOf(cur, prev) { return (cur != null && prev != null && cur > 0 && prev > 0) ? cur / prev - 1 : null; }
    const niOf = function (x) { return x.parent_net_income != null ? x.parent_net_income : x.net_income; };

    function buildAnnualCorrPoints(rowsIn, macro) {
      const A = toAnnualRows(rowsIn);
      const byYear = {};
      A.forEach(function (r) { byYear[Number(r.bsns_year)] = r; });
      const points = [];
      A.forEach(function (r) {
        const Y = Number(r.bsns_year);
        const prev = byYear[Y - 1];
        if (!prev) return;
        const next = byYear[Y + 1];
        const prev2 = byYear[Y - 2];
        const roeCur = computeROEAvg(r, prev), roePrev = prev2 ? computeROEAvg(prev, prev2) : null;
        points.push({
          label: String(Y) + ' 연간',
          macroValues: macroChanges(macro, String(Y - 1) + '1231', String(Y) + '1231'),
          revenue: growthOf(r.revenue, prev.revenue),
          op: growthOf(r.operating_income, prev.operating_income),
          ni: growthOf(niOf(r), niOf(prev)),
          droe: (roeCur && roePrev && roeCur.value != null && roePrev.value != null) ? roeCur.value - roePrev.value : null,
          ret: priceRetOf(r.price_at_period_end, prev.price_at_period_end),
          fwd: next ? priceRetOf(next.price_at_period_end, r.price_at_period_end) : null,
        });
      });
      return points;
    }
    function buildQuarterlyCorrPoints(rowsIn, macro) {
      const Q = toQuarterlyRows(rowsIn);
      const find = function (Y, tag) { return Q.find(function (x) { return Number(x.bsns_year) === Y && x.period_label.split(' ')[1] === tag; }); };
      const points = [];
      Q.forEach(function (r) {
        const Y = Number(r.bsns_year), tag = r.period_label.split(' ')[1];
        const prev = find(Y - 1, tag);
        if (!prev) return;
        const next = find(Y + 1, tag);
        const e1 = periodEndDateOf(r), e0 = periodEndDateOf(prev);
        if (!e1 || !e0) return;
        points.push({
          label: r.period_label,
          macroValues: macroChanges(macro, e0, e1),
          revenue: growthOf(r.revenue, prev.revenue),
          op: growthOf(r.operating_income, prev.operating_income),
          ni: growthOf(niOf(r), niOf(prev)),
          ret: priceRetOf(r.price_at_period_end, prev.price_at_period_end),
          fwd: next ? priceRetOf(next.price_at_period_end, r.price_at_period_end) : null,
        });
      });
      return points;
    }

    function fmtChg(key, v) {
      if (v == null) return 'N/A';
      return MACRO_KIND[key] === 'diff' ? (v >= 0 ? '+' : '') + v.toFixed(2) + '%p' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%';
    }
    const fmtPctSigned = function (v) { return v == null ? 'N/A' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%'; };
    const fmtPp = function (v) { return v == null ? 'N/A' : (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%p'; };

    // points: [{macroValues:{key:Δ}, <target key>: value}] → 거시 변화(행) × 실적/주가수익률(열) 스피어만 순위상관 표
    function renderCorrTable(points, targets, overlap) {
      const nCols = function (grp) { return targets.filter(function (t) { return t.group === grp; }).length; };
      let html = '<div class="tscroll"><table><thead><tr><th rowspan="2">거시 변화 (같은 기간)</th>' +
        '<th colspan="' + nCols('earn') + '" style="text-align:center; background:#ede9fe; color:#6d28d9;">실적 변화</th>' +
        '<th colspan="' + nCols('price') + '" style="text-align:center; background:#dbeafe; color:#1d4ed8;">주가 수익률</th></tr><tr>' +
        targets.map(function (t) { return '<th style="background:' + (t.group === 'price' ? '#eff6ff' : '#f5f3ff') + ';">' + t.label + '</th>'; }).join('') + '</tr></thead><tbody>';
      Object.keys(MACRO_LABELS).forEach(function (mk) {
        html += '<tr><td>' + MACRO_CHG_LABELS[mk] + '</td>';
        targets.forEach(function (t) {
          const pairs = points.filter(function (p) { return p.macroValues[mk] != null && p[t.key] != null; });
          const n = pairs.length;
          const rho = n >= 5 ? spearmanCorrelation(pairs.map(function (p) { return p.macroValues[mk]; }), pairs.map(function (p) { return p[t.key]; })) : null;
          if (rho == null) { html += '<td style="color:#94a3b8;">N/A (n=' + n + ')</td>'; return; }
          const nEff = overlap ? Math.max(3, Math.floor(n / 4)) : n;
          const sig = corrSignificant(rho, nEff);
          const color = sig ? (rho > 0 ? '#15803d' : '#b91c1c') : '#64748b';
          html += '<td style="color:' + color + '; font-weight:' + (sig ? 700 : 400) + ';">' + (rho >= 0 ? '+' : '') + rho.toFixed(2) + (sig ? ' *' : '') + ' <span style="color:#94a3b8; font-weight:400;">(n=' + n + ')</span></td>';
        });
        html += '</tr>';
      });
      return html + '</tbody></table></div>';
    }
    function renderCorrData(points, targets) {
      const keys = Object.keys(MACRO_LABELS);
      let html = '<div class="tscroll" style="max-height:50vh;"><table><thead><tr><th>기간</th>' +
        keys.map(function (k) { return '<th>' + MACRO_CHG_LABELS[k] + '</th>'; }).join('') +
        targets.map(function (t) { return '<th>' + t.label + '</th>'; }).join('') + '</tr></thead><tbody>';
      points.forEach(function (p) {
        html += '<tr><td>' + escHtml(p.label) + '</td>' + keys.map(function (k) { return '<td>' + fmtChg(k, p.macroValues[k]) + '</td>'; }).join('') +
          targets.map(function (t) { return '<td>' + (t.key === 'droe' ? fmtPp(p[t.key]) : fmtPctSigned(p[t.key])) + '</td>'; }).join('') + '</tr>';
      });
      return html + '</tbody></table></div>';
    }

    async function computeMacroCorrelation() {
      const statusEl = document.getElementById('macroCorrStatus');
      const wrapEl = document.getElementById('macroCorrWrap');
      wrapEl.style.display = 'none';
      if (rawRows.length === 0) { statusEl.textContent = '먼저 ② 종목분석 탭에서 종목을 분석해주세요.'; return; }
      statusEl.textContent = '계산 중...';
      let macro;
      try {
        macro = await loadMacroSeries();
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message + ' (④ 데이터 관리 탭에서 "거시경제 데이터 갱신"을 먼저 눌러주세요)';
        return;
      }
      if (Object.keys(macro).length === 0) {
        statusEl.textContent = '거시경제 데이터가 비어 있습니다. ④ 데이터 관리 탭에서 "거시경제 데이터 갱신"을 먼저 눌러주세요.';
        return;
      }
      const annualPts = buildAnnualCorrPoints(rawRows, macro);
      const quarterPts = buildQuarterlyCorrPoints(rawRows, macro);
      const annualTargets = [
        { key: 'revenue', label: '매출 성장률', group: 'earn' },
        { key: 'op', label: '영업이익 성장률', group: 'earn' },
        { key: 'ni', label: '순이익 성장률', group: 'earn' },
        { key: 'droe', label: 'ROE 변화(%p)', group: 'earn' },
        { key: 'ret', label: '같은 해 주가수익률', group: 'price' },
        { key: 'fwd', label: '다음 해 주가수익률(선행)', group: 'price' },
      ];
      const quarterTargets = [
        { key: 'revenue', label: '매출 성장률(YoY)', group: 'earn' },
        { key: 'op', label: '영업이익 성장률(YoY)', group: 'earn' },
        { key: 'ni', label: '순이익 성장률(YoY)', group: 'earn' },
        { key: 'ret', label: '1년 주가수익률', group: 'price' },
        { key: 'fwd', label: '다음 1년 주가수익률(선행)', group: 'price' },
      ];
      const section = function (title, note, pts, targets, overlap) {
        if (pts.length < 5) return '<div class="card"><b>' + title + '</b> <span class="meta-line">— 비교 가능한 기간이 ' + pts.length + '개뿐이라 계산하기엔 부족합니다(최소 5개). 조회 기간을 늘려 더 받아주세요.</span></div>';
        return '<div class="card"><b>' + title + '</b> <span class="meta-line">' + note + '</span>' + renderCorrTable(pts, targets, overlap) +
          '<details class="adv"><summary>계산에 사용한 값 보기(검증용)</summary>' + renderCorrData(pts, targets) + '</details></div>';
      };
      wrapEl.innerHTML =
        section('연간 (전년 말 → 올해 말)', 'n=' + annualPts.length + '개 연도. 서로 겹치지 않는 독립 표본입니다.', annualPts, annualTargets, false) +
        section('분기 (1년 전 같은 분기 → 이번 분기, YoY)', 'n=' + quarterPts.length + '개 분기. 인접 분기의 1년 창이 3/4 겹쳐 유효 표본은 약 n/4이며, 유의성(*)은 이를 반영해 판정했습니다.', quarterPts, quarterTargets, true) +
        '<div class="card meta-line" style="margin-top:0;"><b>읽는 법</b><br />' +
        '· 값은 스피어만 순위상관 ρ(−1~+1)입니다. +이면 같은 방향으로, −이면 반대 방향으로 움직였다는 뜻이며, * 는 양측 5% 수준에서 우연으로 보기 어려운 칸(색 강조)입니다.<br />' +
        '· 수준(환율 1,300원 등)이 아니라 같은 기간의 <b>변화</b>를 비교합니다. 수준끼리 비교하면 시간 추세 때문에 가짜 상관이 생기기 쉽습니다(Granger &amp; Newbold, 1974).<br />' +
        '· "같은 해 주가수익률"은 동행성, "다음 해 주가수익률(선행)"은 거시 변화가 이후 주가를 예측했는지를 봅니다. 주가는 배당을 뺀 종가 수익률이고 시장(코스피) 효과를 제거하지 않았습니다.<br />' +
        '· 표의 칸이 많아(5×6) 5% 수준에서도 우연히 * 가 1~2개는 나올 수 있고, 표본이 수년뿐이라 <b>인과나 예측 근거가 아닌 참고용</b>입니다.<br />' +
        '· 회계기간은 12월 결산 기준(분기말 3·6·9·12월)으로 가정합니다. 결산월이 다른 회사는 기간이 어긋납니다.</div>';
      statusEl.textContent = '';
      wrapEl.style.display = 'block';
      macroCorrDirty = false;
    }
  </script>
</body>
</html>`;

async function fetchDartGeneric(endpoint, corpCode, bsnsYear, reprtCode, proxyUrl, timeoutMs = 15000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", endpoint);
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bsns_year", bsnsYear);
  url.searchParams.set("reprt_code", reprtCode);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchDartByDateRange(endpoint, corpCode, bgnDe, endDe, proxyUrl, timeoutMs = 15000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", endpoint);
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bgn_de", bgnDe);
  url.searchParams.set("end_de", endDe);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchDart(corpCode, bsnsYear, reprtCode, fsDiv, proxyUrl, timeoutMs = 15000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", "fnlttSinglAcntAll");
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bsns_year", bsnsYear);
  url.searchParams.set("reprt_code", reprtCode);
  url.searchParams.set("fs_div", fsDiv);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}

async function fetchDartWithRetry(corpCode, bsnsYear, reprtCode, fsDiv, proxyUrl, retries = 2) {
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetchDart(corpCode, bsnsYear, reprtCode, fsDiv, proxyUrl);
    } catch (e) {
      if (attempt === retries) throw e;
      await new Promise((r) => setTimeout(r, 600 * (attempt + 1)));
    }
  }
}

function parseAmount(v) {
  if (v == null) return null;
  const raw = String(v).replace(/,/g, "").trim();
  if (raw === "" || raw === "-") return null;
  const n = Number(raw);
  return Number.isNaN(n) ? null : n;
}

// 계정ID(우선) 또는 계정명(폴백)으로 매칭되는 모든 행을 합산.
// 유동/비유동으로 나뉜 계정(예: 당기손익공정가치측정금융자산)을 자동으로 합쳐줌.
// 항목별로 찾을 재무제표 종류(sj_div). 같은 계정ID가 자본변동표(SCE)·현금흐름표(CF)·포괄손익(CIS) 등에
// 중복 등장하므로, 전체를 합산하면 값이 부풀어 오른다. 그래서 해당 재무제표 안에서만 찾는다.
// 손익 항목은 IS(손익계산서) → CIS(포괄손익계산서) 순서로, 먼저 값이 나오는 쪽 하나만 사용.
const SJ_BY_KEY = {
  revenue: ["IS", "CIS"], cogs: ["IS", "CIS"], operating_income: ["IS", "CIS"], net_income: ["IS", "CIS"],
  parent_net_income: ["IS", "CIS"], pretax_income: ["IS", "CIS"], interest_expense: ["IS", "CIS"], // CF(누적값) 폴백은 분기 환산이 어긋나 제외
  ocf: ["CF"], capex_ppe: ["CF"], capex_intangible: ["CF"],
  current_assets: ["BS"], current_liabilities: ["BS"], long_term_borrowings: ["BS"], bonds: ["BS"],
  depreciation: ["CF"], amortization: ["CF"], dividends_paid: ["CF"], buyback: ["CF"],
  total_equity: ["BS"], total_liabilities: ["BS"], cash: ["BS"], st_financial_assets: ["BS"],
  receivables: ["BS"], inventory: ["BS"], payables: ["BS"],
  short_term_trading_securities: ["BS"], fvpl_financial_assets: ["BS"], fvoci_financial_assets: ["BS"], investment_property: ["BS"],
  other_receivables: ["BS"], other_payables: ["BS"],
  short_term_borrowings: ["BS"], current_portion_lt_debt: ["BS"], current_lease_liabilities: ["BS"], lease_liabilities_nc: ["BS"],
  tangible_assets: ["BS"], intangible_assets: ["BS"], right_of_use_assets: ["BS"], parent_equity: ["BS"],
};

function sumAccount(list, ids, names, sjOrder, exactOnly, sumNames, sumContains) {
  const norm = (s) => (s || "").replace(/\s/g, "");
  const sumRows = (rows) => {
    let total = 0;
    let found = false;
    for (const m of rows) {
      const v = parseAmount(m.thstrm_amount);
      if (v != null) { total += v; found = true; }
    }
    return found ? total : null;
  };

  // sumNames: 같은 성격의 항목이 이름만 달리 나뉘어 공시되는 경우(유동성장기차입금 + 유동성사채 등) 정확히 일치하는 행을 모두 합산
  // sumContains: 유동/비유동으로 나뉜 금융자산(예: 유동 당기손익FV금융자산 + 비유동 당기손익FV금융자산)을 한 계정으로 합산 — 이름이 포함되거나 계정ID가 같은 본계정 행 전부
  if (sumContains) {
    for (const sj of sjOrder) {
      const rows = list.filter((row) => row.sj_div === sj && (!row.account_detail || row.account_detail === "-") && (ids.includes(row.account_id) || names.some((n) => norm(row.account_nm).includes(norm(n)))));
      const v = sumRows(rows);
      if (v != null) return v;
    }
  }
  if (sumNames) {
    for (const sj of sjOrder) {
      const rows = list.filter((row) => row.sj_div === sj && (!row.account_detail || row.account_detail === "-") && names.some((n) => norm(row.account_nm) === norm(n)));
      const v = sumRows(rows);
      if (v != null) return v;
    }
  }
  for (const sj of sjOrder) {
    const inSj = list.filter((row) => row.sj_div === sj);
    // account_detail이 "-"인 행이 본 계정. 값이 없으면 세부구분(member) 행까지 넓혀서 다시 찾음
    const plain = inSj.filter((row) => !row.account_detail || row.account_detail === "-");
    for (const pool of [plain, inSj]) {
      // 1) 계정ID: 우선순위대로 첫 번째로 값이 있는 ID의 행만 사용 (대체 ID를 중복 합산하지 않음)
      for (const id of ids) {
        const v = sumRows(pool.filter((row) => row.account_id === id));
        if (v != null) return v;
      }
      // 2) 계정명: 띄어쓰기 무시하고 정확히 일치하는 행 우선, 없으면 포함하는 첫 행 하나만
      for (const n of names) {
        const exact = pool.filter((row) => norm(row.account_nm) === norm(n));
        if (exact.length) {
          const v = sumRows(exact);
          if (v != null) return v;
        }
        if (exactOnly) continue;
        const fuzzy = pool.find((row) => row.account_nm?.includes(n) && parseAmount(row.thstrm_amount) != null);
        if (fuzzy) return parseAmount(fuzzy.thstrm_amount);
      }
    }
  }
  return null;
}

function pickStockCounts(dart) {
  const none = (reason) => ({ total_shares: null, treasury_shares: null, preferred_shares: null, reason });
  if (!dart) return none("응답 없음");
  if (dart.status !== "000") return none("DART " + dart.status + (dart.message ? " " + String(dart.message).slice(0, 30) : ""));
  const norm = (s) => (s || "").replace(/\s/g, "");
  const list = dart.list || [];
  // 구분명이 정확히 "보통주"가 아니어도("보통주식", "보통주(의결권…)" 등) '보통'이 들어가고 합계·우선이 아닌 첫 행을 보통주로 본다
  const row = list.find((r) => norm(r.se) === "보통주") || list.find((r) => /보통/.test(norm(r.se)) && !/합계|우선/.test(norm(r.se)));
  if (!row) return none("보통주 행 없음(구분: " + list.map((r) => norm(r.se)).filter(Boolean).slice(0, 5).join("/") + ")");
  // 이 기간에 "보통주" 공시 행 자체는 존재함 → 그 안의 개별 값이 "-"/빈칸이면
  // "미공시"가 아니라 "0주"라는 뜻(DART 표기 관행, 특히 자기주식이 없는 대다수 기업).
  // 행 자체가 없는 경우(1·3분기 미공시 등)만 위에서 null,null로 빠져 이월 로직을 타도록 둔다.
  const toCount = (v) => {
    const n = parseAmount(v);
    return n == null ? 0 : n;
  };
  // 우선주: 순이익·자본은 보통주+우선주 전체의 것이라, 주당 지표(EPS·BPS)를 보통주 수만으로 나누면 과대 계산된다 → 유통 우선주 수(발행−자기)를 따로 저장해 합산에 쓴다.
  const pref = list.find((r) => norm(r.se) === "우선주") || list.find((r) => /우선/.test(norm(r.se)) && !/합계/.test(norm(r.se)));
  const prefNet = pref ? Math.max(0, toCount(pref.istc_totqy) - toCount(pref.tesstk_co)) : 0;
  return { total_shares: toCount(row.istc_totqy), treasury_shares: toCount(row.tesstk_co), preferred_shares: prefNet };
}

function pickDividendPerShare(dart) {
  if (!dart || dart.status !== "000") return null;
  const row = dart.list.find((r) => r.se === "주당 현금배당금(원)" && r.stock_knd === "보통주");
  if (!row) return null; // 배당 항목 자체가 없으면 알 수 없음
  const v = parseAmount(row.thstrm);
  return v != null ? v : 0; // 항목은 있는데 값이 "-"/빈칸이면 무배당(0원)으로 저장
}

// 보고서 종류별 "회계기간 말일" (12월 결산 법인 기준 — 대다수가 해당. 변경결산기 법인은 다를 수 있음)
const PERIOD_END_SUFFIX = { "11013": "0331", "11012": "0630", "11014": "0930", "11011": "1231" };
function periodEndDateStr(year, reprtCode) {
  const suffix = PERIOD_END_SUFFIX[reprtCode];
  return suffix ? `${year}${suffix}` : null;
}

function addDaysStr(yyyymmdd, days) {
  const y = Number(yyyymmdd.slice(0, 4)), m = Number(yyyymmdd.slice(4, 6)), d = Number(yyyymmdd.slice(6, 8));
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return `${dt.getUTCFullYear()}${String(dt.getUTCMonth() + 1).padStart(2, "0")}${String(dt.getUTCDate()).padStart(2, "0")}`;
}

async function fetchNaverPrices(symbol, startDate, endDate, proxyUrl, timeoutMs = 15000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("source", "naver_price");
  url.searchParams.set("symbol", symbol);
  url.searchParams.set("start", startDate);
  url.searchParams.set("end", endDate);

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let text;
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    text = await resp.text();
  } finally {
    clearTimeout(timer);
  }

  // 네이버 siseJson 응답: [['날짜','시가','고가','저가','종가','거래량', ...], ["20240102", ...], ...]
  // 정식 JSON이 아니라 작은따옴표를 쓰므로 치환 후 파싱
  let arr;
  try {
    arr = JSON.parse(text.trim().replace(/'/g, '"'));
  } catch (e) {
    return [];
  }
  if (!Array.isArray(arr) || arr.length < 2) return [];
  return arr.slice(1)
    .map((row) => ({ date: String(row[0]), close: Number(row[4]), volume: row[5] != null ? Number(row[5]) : null }))
    .filter((r) => /^\d{8}$/.test(r.date) && !Number.isNaN(r.close));
}

// targetDate(YYYYMMDD) 이전(포함) 중 가장 최근 거래일의 종가를 찾는다 (공시일이 휴일/주말일 수 있으므로)
// 공시일 상한: 정정공시 등으로 rcept_no가 뒤늦은 날짜가 되는 것을 막기 위해 법정 제출기한 이후의 달 말일로 제한한다.
// 1분기=5/31, 반기=8/31, 3분기=11/30, 사업보고서=다음 해 3/31 (사업보고서 법정기한이 결산 후 90일이라 2월 말이면 실제 공시일보다 빨라져 미래 주가를 쓰지 않게 3월 말로 둠)
function filingCapDate(year, reprtCode) {
  const y = Number(year);
  if (reprtCode === "11013") return `${y}0531`;
  if (reprtCode === "11012") return `${y}0831`;
  if (reprtCode === "11014") return `${y}1130`;
  if (reprtCode === "11011") return `${y + 1}0331`;
  return null;
}
function effectiveFilingDate(rceptDate, year, reprtCode) {
  const cap = filingCapDate(year, reprtCode);
  return (cap && rceptDate > cap) ? cap : rceptDate;
}
// 기준일이 휴장일이면 그 다음 영업일(기준일 이후 첫 거래일) 종가
function closeOnOrAfter(prices, targetDate) {
  const c = prices.filter((p) => p.date >= targetDate).sort((a, b) => a.date.localeCompare(b.date));
  return c.length ? c[0].close : null;
}
function closeAsOf(prices, targetDate) {
  const candidates = prices.filter((p) => p.date <= targetDate).sort((a, b) => b.date.localeCompare(a.date));
  return candidates.length ? candidates[0].close : null;
}

// ============================================================
// 거시경제 지표(환율·금리·WTI) — ECOS(한국은행) + FRED(미 연준)
// ============================================================
// series별 소스/코드 정의. ECOS는 (통계표코드, 항목코드), FRED는 series_id만 있으면 된다.
// 금리·환율 통계표/항목코드는 ECOS 공식 사이트(https://ecos.bok.or.kr → 통계표코드 검색)에서
// 확인한 값을 그대로 쓴다 — 통계표가 개편되면 코드가 바뀔 수 있어 한 곳에 모아뒀다.
const MACRO_SERIES = {
  usdkrw: { source: "ecos", statCode: "731Y001", itemCode: "0000001", label: "원/달러 환율" },
  msb1y:  { source: "ecos", statCode: "817Y002", itemCode: "010150000", label: "통안증권(1년)" },
  ktb3y:  { source: "ecos", statCode: "817Y002", itemCode: "010200000", label: "국고채(3년)", expectName: "3년" },
  ktb10y: { source: "ecos", statCode: "817Y002", itemCode: "010210000", label: "국고채(10년)", expectName: "10년" },
  wti:    { source: "fred", seriesId: "DCOILWTICO", label: "WTI 현물가" },
};

// ECOS StatisticSearch API: 한 번 호출로 기간 전체(start~end)를 받아온다.
// https://ecos.bok.or.kr/api/StatisticSearch/{키}/json/kr/{시작행}/{끝행}/{통계표코드}/D/{시작일}/{종료일}/{항목코드}
async function fetchEcosSeries(statCode, itemCode, startDate, endDate, apiKey, timeoutMs = 15000, expectName = null) {
  const url = `https://ecos.bok.or.kr/api/StatisticSearch/${apiKey}/json/kr/1/10000/${statCode}/D/${startDate}/${endDate}/${itemCode}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let text;
  try {
    const resp = await fetch(url, { signal: controller.signal });
    text = await resp.text();
  } finally {
    clearTimeout(timer);
  }
  let json;
  try { json = JSON.parse(text); } catch (e) { throw new Error(`ECOS 응답 파싱 실패(${statCode}/${itemCode}): ${text.slice(0, 200)}`); }
  if (json.RESULT && json.RESULT.CODE && json.RESULT.CODE !== "INFO-000") {
    throw new Error(`ECOS 오류(${statCode}/${itemCode}): ${json.RESULT.CODE} ${json.RESULT.MESSAGE || ""}`);
  }
  const rows = json?.StatisticSearch?.row || [];
  // 항목코드가 다른 계열(예: 5년물)을 가리키는 사고를 막기 위한 이름 확인
  if (expectName && rows.length && !String(rows[0].ITEM_NAME1 || "").includes(expectName)) {
    throw new Error(`ECOS 항목 이름 불일치(${statCode}/${itemCode}): 기대 "${expectName}", 실제 "${rows[0].ITEM_NAME1}" — 저장하지 않음`);
  }
  return rows
    .map((r) => ({ date: r.TIME, value: Number(r.DATA_VALUE) }))
    .filter((r) => /^\d{8}$/.test(r.date) && !Number.isNaN(r.value));
}

// FRED(Federal Reserve Economic Data) API: 한 번 호출로 기간 전체를 받아온다.
async function fetchFredSeries(seriesId, startDate, endDate, apiKey, timeoutMs = 15000) {
  const fmt = (ymd) => `${ymd.slice(0, 4)}-${ymd.slice(4, 6)}-${ymd.slice(6, 8)}`;
  const url = `https://api.stlouisfed.org/fred/series/observations?series_id=${seriesId}&api_key=${apiKey}&file_type=json&observation_start=${fmt(startDate)}&observation_end=${fmt(endDate)}`;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let text;
  try {
    const resp = await fetch(url, { signal: controller.signal });
    text = await resp.text();
  } finally {
    clearTimeout(timer);
  }
  let json;
  try { json = JSON.parse(text); } catch (e) { throw new Error(`FRED 응답 파싱 실패(${seriesId}): ${text.slice(0, 200)}`); }
  if (json.error_message) throw new Error(`FRED 오류(${seriesId}): ${json.error_message}`);
  const obs = json.observations || [];
  return obs
    .map((o) => ({ date: o.date.replace(/-/g, ""), value: Number(o.value) }))
    .filter((r) => /^\d{8}$/.test(r.date) && !Number.isNaN(r.value)); // FRED는 휴장일에 value="."로 와서 NaN으로 자동 제외됨
}

// 거시지표 5개를 받아 macro_data에 저장한다. D1 쓰기는 하루 10만행이 "계정 전체 테이블 합산"이라서,
// 이 함수를 누를 때마다 수년치 전체를 다시 받아 쓰면(특히 여러 번 누르면) 그것만으로 하루 쓰기 한도를
// 다 써버릴 수 있다 — 실제로 발생한 D1 무료한도 초과 사고의 유력한 원인 중 하나였다.
// 그래서 explicitStart가 없으면(=사용자가 날짜를 직접 지정하지 않은 평소 "갱신" 클릭이면) 지표별로
// 이미 저장된 가장 최근 날짜 다음부터만 증분으로 받아온다 — 두 번째 클릭부터는 며칠치만 받아 거의
// 공짜에 가깝다. 한 번도 받은 적 없는 지표만 defaultLookbackStart(최초 백필 범위)부터 받는다.
async function runMacroRefresh(env, explicitStart, endDate, defaultLookbackStart) {
  if (!env.ECOS_API_KEY) throw new Error("ECOS_API_KEY 환경변수(시크릿)가 설정되지 않았습니다.");
  if (!env.FRED_API_KEY) throw new Error("FRED_API_KEY 환경변수(시크릿)가 설정되지 않았습니다.");

  const summary = {};
  // Worker 호출 1번에는 무료 플랜 기준 subrequest(외부 fetch + D1 쿼리) 50건 한도가 있다. 한 번에 저장하는 점 수를
  // 1,000개(INSERT 약 31건)로 제한하고, 남은 건 "partial"로 알려 화면이 이어서 다시 호출하게 한다.
  let budget = explicitStart ? Infinity : 1000;
  for (const [seriesKey, def] of Object.entries(MACRO_SERIES)) {
    let effectiveStart = explicitStart;
    if (!effectiveStart) {
      const lastRow = await env.DB
        .prepare("SELECT MAX(date) AS d FROM macro_data WHERE series = ?")
        .bind(seriesKey)
        .first();
      effectiveStart = lastRow && lastRow.d ? addDaysStr(lastRow.d, 1) : defaultLookbackStart;
    }
    if (effectiveStart > endDate) {
      summary[seriesKey] = { label: def.label, points: 0, note: "이미 최신 상태" };
      continue;
    }
    let points;
    try {
      points = def.source === "ecos"
        ? await fetchEcosSeries(def.statCode, def.itemCode, effectiveStart, endDate, env.ECOS_API_KEY, 15000, def.expectName || null)
        : await fetchFredSeries(def.seriesId, effectiveStart, endDate, env.FRED_API_KEY);
    } catch (e) {
      summary[seriesKey] = { error: e.message || String(e) };
      continue;
    }
    let partial = false;
    if (points.length > budget) {
      points = points.slice(0, Math.max(0, budget)); // 오래된 날짜부터 저장(다음 호출이 마지막 저장일 다음날부터 이어받음)
      partial = true;
    }
    budget -= points.length;
    if (points.length > 0) {
      await multiRowInsert(
        env.DB,
        "INSERT OR REPLACE INTO macro_data (series, date, value)",
        3,
        points,
        (p) => [seriesKey, p.date, p.value]
      );
    }
    summary[seriesKey] = { label: def.label, points: points.length, from: effectiveStart, partial };
  }
  return summary;
}

// ============================================================
// 모멘텀 스크리닝 - 핵심 함수 (02_market_screening.md 명세, 무료 플랜 적용)
// ============================================================

function todayStr() {
  const d = new Date();
  return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`;
}

// 오늘 00:00 UTC의 ISO 문자열. market_universe_status.checked_at(ISO)과 비교해 "오늘 처리한 개수"를 센다.
function todayUtcMidnightIso() {
  const d = new Date();
  return new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate())).toISOString();
}

// ------------------------------------------------------------
// 공공데이터포털(data.go.kr) "금융위원회_주식시세정보" API (getStockPriceInfo_V2)
// - basDt(기준일) + beginTrPrc(거래대금 이상)로 "그날 유동성 기준을 통과한 전체 종목"을 한 번에 조회 가능
// - likeSrtnCd(종목코드) + beginBasDt~endBasDt(기간)로 "한 종목의 가격 이력"을 날짜범위로 한 번에 조회 가능
// → 네이버 스크래핑(HTML 파싱, 중계기 경유) 없이 이 공식 API 하나로 유동성 필터 + 가격 백필을 모두 해결한다.
// ------------------------------------------------------------
const KRX_API_BASE = "https://apis.data.go.kr/1160100/GetStockSecuritiesInfoService_V2/getStockPriceInfo_V2";
const KRX_PAGE_SIZE = 500; // 한 페이지에 받아올 행 수(여유있게 설정; 필요시 줄여도 됨)
const KRX_MAX_PAGES = 20;  // 혹시 totalCount가 비정상적으로 커도 무한 루프에 빠지지 않도록 하는 안전장치

// 응답 XML에서 <item>...</item> 블록들을 regex로 파싱한다(Workers 런타임엔 DOM 파서가 없고,
// 필드가 단순 평면 구조라 regex로도 충분히 안전하게 뽑아낼 수 있다).
function parseKrxXml(xmlText) {
  const items = [];
  const itemRe = /<item>([\s\S]*?)<\/item>/g;
  const field = (block, tag) => {
    const m = block.match(new RegExp(`<${tag}>([^<]*)<\\/${tag}>`));
    return m ? m[1] : null;
  };
  const num = (v) => (v == null || v === "" ? null : Number(v));
  let m;
  while ((m = itemRe.exec(xmlText)) !== null) {
    const block = m[1];
    items.push({
      basDt: field(block, "basDt"),
      srtnCd: field(block, "srtnCd"),
      isinCd: field(block, "isinCd"),
      itmsNm: field(block, "itmsNm"),
      mrktCtg: field(block, "mrktCtg"),
      clpr: num(field(block, "clpr")),
      trqu: num(field(block, "trqu")),
      trPrc: num(field(block, "trPrc")),
      mrktTotAmt: num(field(block, "mrktTotAmt")),
    });
  }
  const totalMatch = xmlText.match(/<totalCount>(\d+)<\/totalCount>/);
  const codeMatch = xmlText.match(/<resultCode>(\d+)<\/resultCode>/);
  return {
    items,
    totalCount: totalMatch ? Number(totalMatch[1]) : items.length,
    resultCode: codeMatch ? codeMatch[1] : null,
  };
}

async function fetchKrxPage(params, apiKey, timeoutMs = 15000) {
  const url = new URL(KRX_API_BASE);
  url.searchParams.set("serviceKey", apiKey);
  url.searchParams.set("numOfRows", String(params.numOfRows || KRX_PAGE_SIZE));
  url.searchParams.set("pageNo", String(params.pageNo || 1));
  if (params.basDt) url.searchParams.set("basDt", params.basDt);
  if (params.beginBasDt) url.searchParams.set("beginBasDt", params.beginBasDt);
  if (params.endBasDt) url.searchParams.set("endBasDt", params.endBasDt);
  if (params.likeSrtnCd) url.searchParams.set("likeSrtnCd", params.likeSrtnCd);
  if (params.beginTrPrc != null) url.searchParams.set("beginTrPrc", String(params.beginTrPrc));

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  let text;
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    text = await resp.text();
  } finally {
    clearTimeout(timer);
  }
  return parseKrxXml(text);
}

// 한 종목의 날짜범위 가격 이력 전체를 페이지를 넘기며 받아온다 (모멘텀 계산용 백필).
// ============================================================
// 미너비니 트렌드 템플릿(Minervini, "Trade Like a Stock Market Wizard", 2013)의 가격 조건 7개를 일별 종가로 판정한다.
// 8번째 조건(RS Rating ≥ 70)은 전 종목 12개월 시세가 없어 6개월 백분위(저장 종목군)로 대용해 "참고"로만 표시한다(개수에 미포함).
// 데이터 정합성 가드: 시세가 부족하거나, 액면분할 등으로 수정되지 않은 흔적(하루 ±31% 초과 변동 — 국내 가격제한폭 30%)이
// 있거나, 누락 구간이 있으면 값을 만들지 않고 이유를 반환한다. 고가/저가는 종가 기준(원전은 장중 고저가).
// ============================================================
function computeTrendTemplate(hist, todayYmd) {
  const out = { ok: false, reason: null };
  const h = (hist || []).filter((x) => x && x.close > 0 && /^\d{8}$/.test(x.date)).map((x) => ({ date: x.date, close: x.close, volume: Number(x.volume) }));
  if (h.length < 252) { out.reason = "일별 시세가 252거래일 미만(" + h.length + "개)이라 판정하지 않습니다."; return out; }
  const w = h.slice(-252);
  const dayNum = (d) => Date.UTC(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6, 8)) / 86400000;
  for (let i = 1; i < w.length; i++) {
    const ret = w[i].close / w[i - 1].close - 1;
    if (Math.abs(ret) > 0.31) { out.reason = "하루 변동이 ±31%를 넘는 날(" + w[i].date + ")이 있어 액면분할·감자 등으로 수정되지 않은 시세일 수 있습니다. 잘못된 판정을 막기 위해 계산하지 않습니다."; return out; }
    if (dayNum(w[i].date) - dayNum(w[i - 1].date) > 12) { out.reason = "시세 누락 구간(" + w[i - 1].date + "~" + w[i].date + ")이 있어 판정하지 않습니다(거래정지 등)."; return out; }
  }
  const span = dayNum(w[w.length - 1].date) - dayNum(w[0].date);
  if (span < 340 || span > 420) { out.reason = "252거래일의 달력 기간이 " + span + "일로 비정상이라(시세 누락 가능) 판정하지 않습니다."; return out; }
  if (todayYmd && dayNum(todayYmd) - dayNum(w[w.length - 1].date) > 7) { out.reason = "최근 시세가 " + w[w.length - 1].date + "까지만 있어(7일 이상 지연) 판정하지 않습니다."; return out; }
  // 200일선의 21거래일 전 값이 필요하므로 전체 h에서 계산(최소 221개 보장: 252개 이상 확인됨)
  const sma = (n, back) => { const e = h.length - back; if (e - n < 0) return null; let s = 0; for (let i = e - n; i < e; i++) s += h[i].close; return s / n; };
  const close = w[w.length - 1].close;
  const s50 = sma(50, 0), s150 = sma(150, 0), s200 = sma(200, 0), s200p = sma(200, 21);
  let hi = -Infinity, lo = Infinity; for (const x of w) { if (x.close > hi) hi = x.close; if (x.close < lo) lo = x.close; }
  const aboveLo = close / lo - 1, belowHi = 1 - close / hi;
  // 거래량 비율(오닐 S): 최근 거래일 거래량 ÷ 직전 50거래일 평균. 거래량이 0이거나 없는 날이 있으면 계산하지 않음
  let volRatio = null;
  const vv = h.slice(-51).map((x) => x.volume);
  if (vv.length === 51 && vv.every((x) => x > 0)) { volRatio = vv[50] / (vv.slice(0, 50).reduce((a, b) => a + b, 0) / 50); }
  const f = (v) => Math.round(v).toLocaleString("en-US");
  const checks = [
    { k: 1, label: "주가 > 150일선 그리고 > 200일선", pass: close > s150 && close > s200, detail: f(close) + " vs 150일 " + f(s150) + " / 200일 " + f(s200) },
    { k: 2, label: "150일선 > 200일선", pass: s150 > s200, detail: f(s150) + " vs " + f(s200) },
    { k: 3, label: "200일선이 최소 1개월(21거래일) 상승 중", pass: s200 > s200p, detail: "현재 " + f(s200) + " vs 21거래일 전 " + f(s200p) },
    { k: 4, label: "50일선 > 150일선 그리고 > 200일선", pass: s50 > s150 && s50 > s200, detail: "50일 " + f(s50) },
    { k: 5, label: "주가 > 50일선", pass: close > s50, detail: f(close) + " vs " + f(s50) },
    { k: 6, label: "52주 저가보다 30% 이상 위", pass: aboveLo >= 0.30, detail: "저가 " + f(lo) + " 대비 +" + (aboveLo * 100).toFixed(1) + "%" },
    { k: 7, label: "52주 고가의 25% 이내(고가 대비 하락 ≤ 25%)", pass: belowHi <= 0.25, detail: "고가 " + f(hi) + " 대비 -" + (belowHi * 100).toFixed(1) + "%" },
  ];
  return { ok: true, volRatio, date: w[w.length - 1].date, n: h.length, close, sma50: s50, sma150: s150, sma200: s200, sma200prev: s200p, hi52: hi, lo52: lo, aboveLo, belowHi, checks, passed: checks.filter((c) => c.pass).length, total: checks.length };
}

async function fetchKrxStockHistory(stockCode, beginBasDt, endBasDt, apiKey) {
  const all = [];
  for (let pageNo = 1; pageNo <= KRX_MAX_PAGES; pageNo++) {
    const { items, totalCount, resultCode } = await fetchKrxPage(
      { likeSrtnCd: stockCode, beginBasDt, endBasDt, numOfRows: KRX_PAGE_SIZE, pageNo },
      apiKey
    );
    if (resultCode && resultCode !== "00") break;
    all.push(...items);
    if (items.length === 0 || all.length >= totalCount) break;
  }
  // likeSrtnCd는 부분일치이므로 정확히 같은 종목코드만 남긴다.
  return all
    .filter((it) => it.srtnCd === stockCode)
    .map((it) => ({ date: it.basDt, close: it.clpr, volume: it.trqu, tradingValue: it.trPrc, marketCap: it.mrktTotAmt }))
    .sort((a, b) => a.date.localeCompare(b.date));
}

// 특정 기준일(basDt)에 거래대금 조건(beginTrPrc 이상)을 만족하는 전체 종목을 페이지를 넘기며 받아온다.
async function fetchKrxDailyUniverse(basDt, minTradingValue, apiKey) {
  const all = [];
  for (let pageNo = 1; pageNo <= KRX_MAX_PAGES; pageNo++) {
    const { items, totalCount, resultCode } = await fetchKrxPage(
      { basDt, beginTrPrc: minTradingValue, numOfRows: KRX_PAGE_SIZE, pageNo },
      apiKey
    );
    if (resultCode && resultCode !== "00") break;
    all.push(...items);
    if (items.length === 0 || all.length >= totalCount) break;
  }
  return all;
}

// [일일 시세 추가 방식 — 무료 한도 설계]
// 예전에는 종목마다 API를 따로 불러(종목당 fetch 1~4 + D1 6~9건) 하루치를 붙였고, 1,300종목이면 15시간이 걸렸다.
// 이제는 "기준일 하나로 전체 종목을 한 번에 주는" 공공데이터 호출(basDt + beginTrPrc)을 쪽(page)별로 받아
// 이미 이력이 있는 종목은 그날 행을 멀티로우로 한꺼번에 넣고(쪽당 INSERT 약 16건), 처음 보는 종목은 백필 큐에 넣는다.
// 1,300종목이 약 6분(홀수 분 6번)에 끝나고, 종목별 큐 쓰기/삭제(종목당 4건)와 API 호출 수천 건이 사라진다.
// 이 함수는 "시작 표시"만 하고 실제 처리는 processAppendJobTick()이 cron에서 한 쪽씩 한다.
async function runUniverseRefresh(env, force) {
  if (!env.DATA_GO_KR_KEY) {
    throw new Error("DATA_GO_KR_KEY 환경변수(시크릿)가 설정되지 않았습니다. Cloudflare Workers 설정에서 추가해주세요.");
  }
  await ensureMarketQueueColumns(env.DB);
  let basDt = null;
  let total = 0;
  const today = new Date();
  for (let back = 0; back <= 7; back++) {
    const d = new Date(today);
    d.setUTCDate(d.getUTCDate() - back);
    const ds = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`;
    const probe = await fetchKrxPage({ basDt: ds, beginTrPrc: MARKET_MIN_TRADING_VALUE, numOfRows: 1, pageNo: 1 }, env.DATA_GO_KR_KEY);
    if (probe.items.length > 0) { basDt = ds; total = probe.totalCount; break; }
  }
  if (!basDt) {
    throw new Error("최근 7일 내 거래일 데이터를 찾지 못했습니다(주말/공휴일이 겹쳤을 수 있습니다).");
  }
  const lastRow = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'last'").first();
  if (!force && lastRow && lastRow.v === basDt) {
    return { bas_dt: basDt, universe_size: total, job_started: false, note: "이미 이 기준일의 시세를 추가했습니다(건너뜀)." };
  }
  const job = { basDt, nextPage: 1, pageSize: MARKET_APPEND_PAGE, total, startedAt: new Date().toISOString(), fails: 0, appended: 0, queued: 0 };
  await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('append', ?)").bind(JSON.stringify(job)).run();
  return { bas_dt: basDt, universe_size: total, job_started: true, pages: Math.ceil(total / MARKET_APPEND_PAGE) };
}

// cron(홀수 분)에서 호출: 진행 중인 일괄 추가 작업이 있으면 한 쪽을 처리한다. 작업이 있었으면 true.
async function processAppendJobTick(env) {
  const row = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'append'").first();
  if (!row) return false;
  let job;
  try { job = JSON.parse(row.v); } catch (e) { await env.DB.prepare("DELETE FROM market_job WHERE k = 'append'").run(); return false; }

  let page;
  try {
    page = await fetchKrxPage({ basDt: job.basDt, beginTrPrc: MARKET_MIN_TRADING_VALUE, numOfRows: job.pageSize, pageNo: job.nextPage }, env.DATA_GO_KR_KEY);
    if (page.resultCode && page.resultCode !== "00") throw new Error("API resultCode " + page.resultCode);
  } catch (e) {
    job.fails = (job.fails || 0) + 1;
    console.error("일괄 시세 추가 쪽 조회 실패:", job.nextPage, (e && e.message) || e);
    if (job.fails >= 4) await env.DB.prepare("DELETE FROM market_job WHERE k = 'append'").run(); // 계속 실패하면 포기(수동 갱신으로 재시작)
    else await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('append', ?)").bind(JSON.stringify(job)).run();
    return true;
  }

  const rows = page.items.filter((it) => it.srtnCd && it.clpr > 0 && it.mrktTotAmt != null && it.mrktTotAmt >= MARKET_MIN_MCAP);
  const { results: doneRows } = await env.DB.prepare("SELECT stock_code FROM market_universe_status WHERE backfilled = 1").all();
  const done = new Set(doneRows.map((r) => r.stock_code));
  const toAppend = rows.filter((it) => done.has(it.srtnCd));
  const toQueue = rows.filter((it) => !done.has(it.srtnCd));
  const nowIso = new Date().toISOString();

  // 같은 날을 다시 처리해도(수동 재실행) 행 수가 늘지 않도록 upsert(이미 있으면 갱신 = 쓰기 1건).
  await multiRowInsert(
    env.DB,
    "INSERT INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value, market_cap)",
    6,
    toAppend,
    (it) => [it.basDt, it.srtnCd, it.clpr, it.trqu, it.trPrc, it.mrktTotAmt],
    "ON CONFLICT(market_date, stock_code) DO UPDATE SET close_price = excluded.close_price, volume = excluded.volume, trading_value = excluded.trading_value, market_cap = excluded.market_cap"
  );
  await multiRowInsert(
    env.DB,
    "INSERT OR IGNORE INTO market_fetch_queue (stock_code, corp_name, queued_at, is_new, trv)",
    5,
    toQueue,
    (it) => [it.srtnCd, it.itmsNm, nowIso, 1, it.trPrc]
  );

  job.appended = (job.appended || 0) + toAppend.length;
  job.queued = (job.queued || 0) + toQueue.length;
  job.fails = 0;
  const finished = page.items.length < job.pageSize || job.nextPage * job.pageSize >= (page.totalCount || job.total);
  if (finished) {
    await env.DB.prepare("DELETE FROM market_job WHERE k = 'append'").run();
    await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('last', ?)").bind(job.basDt).run();
    console.log("일괄 시세 추가 완료:", job.basDt, "추가", job.appended, "신규 큐 등록", job.queued);
  } else {
    job.nextPage += 1;
    await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('append', ?)").bind(JSON.stringify(job)).run();
  }
  return true;
}

// D1은 쿼리(statement) 1건당 바인드 파라미터 최대 100개, Worker 호출당(무료 플랜) D1 쿼리 최대 50건 제한이 있다
// (batch() 안의 statement도 각각 1건으로 집계됨). 그래서 행마다 statement를 따로 만들어 batch()에 넘기면
// 수백 행만 돼도 금방 두 한도를 넘긴다. 대신 한 INSERT 문에 여러 행을 (?,?,...),(?,?,...) 형태로 묶어서
// statement 자체의 개수를 크게 줄인다(바인드 파라미터 100개 한도 안에서 한 statement에 최대한 많은 행을 담음).
// sqlPrefix는 "INSERT OR REPLACE INTO t (a,b,c)" 또는 "INSERT INTO t (...) ... ON CONFLICT ... DO UPDATE ..."처럼
// VALUES 바로 앞까지, sqlSuffix는 ON CONFLICT 절처럼 VALUES 뒤에 붙는 부분(없으면 빈 문자열).
async function multiRowInsert(db, sqlPrefix, columnsPerRow, rows, toParams, sqlSuffix = "") {
  if (rows.length === 0) return;
  const maxRowsPerStatement = Math.max(1, Math.floor(100 / columnsPerRow));
  for (let i = 0; i < rows.length; i += maxRowsPerStatement) {
    const chunk = rows.slice(i, i + maxRowsPerStatement);
    const valuesSql = chunk.map(() => `(${Array(columnsPerRow).fill("?").join(",")})`).join(",");
    const params = chunk.flatMap(toParams);
    await db.prepare(`${sqlPrefix} VALUES ${valuesSql} ${sqlSuffix}`).bind(...params).run();
  }
}

// 퍼센타일 랭크(0~1): 값이 작을수록 0에 가깝고 클수록 1에 가깝다. 동점은 평균 순위 사용.
// (명세: Momentum Score는 원시 수익률이 아니라 순위 기반으로 계산해 이상치 왜곡을 방지)
function percentileRanks(values) {
  const n = values.length;
  if (n === 0) return [];
  if (n === 1) return [0.5];
  const idx = values.map((v, i) => i).sort((a, b) => values[a] - values[b]);
  const ranks = new Array(n);
  let i = 0;
  while (i < n) {
    let j = i;
    while (j + 1 < n && values[idx[j + 1]] === values[idx[i]]) j++;
    const avgRank = (i + j) / 2; // 동점 구간의 평균 순위(0-based)
    const pct = avgRank / (n - 1);
    for (let k = i; k <= j; k++) ranks[idx[k]] = pct;
    i = j + 1;
  }
  return ranks;
}

// sortedRows: market_date 오름차순으로 정렬된 {market_date, close_price} 배열
// n거래일 수익률 = 최신종가/n거래일전종가 - 1. 데이터가 부족하면 null(0으로 채우지 않음 — 명세 §원칙).
function nDayReturn(sortedRows, n) {
  const len = sortedRows.length;
  if (len < n + 1) return null;
  const latest = sortedRows[len - 1].close_price;
  const past = sortedRows[len - 1 - n].close_price;
  if (latest == null || past == null || past === 0) return null;
  return latest / past - 1;
}

// stockRowsMap: Map<stock_code, 정렬된 일별 행 배열> → RS20/60/120 + 퍼센타일 랭크 + Momentum Score 계산.
// weights는 하드코딩하지 않고 인자로 받는다(명세: 20/35/45는 가설이며 설정 가능해야 함).
function computeMomentumTable(stockRowsMap, weights = MOMENTUM_WEIGHTS) {
  const rows = [];
  for (const [stockCode, sortedRows] of stockRowsMap.entries()) {
    const rs20 = nDayReturn(sortedRows, 20);
    const rs60 = nDayReturn(sortedRows, 60);
    const rs120 = nDayReturn(sortedRows, 120);
    // 셋 중 하나라도 계산 불가하면 랭킹에서 제외(0으로 채우지 않음)
    if (rs20 == null || rs60 == null || rs120 == null) continue;
    rows.push({ stock_code: stockCode, rs20, rs60, rs120 });
  }
  if (rows.length === 0) return [];

  const rank20 = percentileRanks(rows.map((r) => r.rs20));
  const rank60 = percentileRanks(rows.map((r) => r.rs60));
  const rank120 = percentileRanks(rows.map((r) => r.rs120));

  rows.forEach((r, i) => {
    r.rank20 = rank20[i];
    r.rank60 = rank60[i];
    r.rank120 = rank120[i];
    r.momentumScore = weights.rs20 * rank20[i] + weights.rs60 * rank60[i] + weights.rs120 * rank120[i];
  });

  rows.sort((a, b) => b.momentumScore - a.momentumScore);
  return rows;
}

async function getFallbackShareCounts(db, corpCode, beforeOrder) {
  if (!db) return null;
  const row = await db
    .prepare("SELECT total_shares, treasury_shares, preferred_shares, period_label FROM financial_raw WHERE corp_code = ? AND period_order < ? AND total_shares IS NOT NULL ORDER BY period_order DESC LIMIT 1")
    .bind(corpCode, beforeOrder)
    .first();
  return row || null;
}

const REPRT_CODE_BY_Q = { 1: "11013", 2: "11012", 3: "11014", 4: "11011" };

// ov: 아직 DB에 저장되지 않은 "지금 막 받은 행"(bsns_year·reprt_code 포함). 같은 기간을 찾으면 DB 대신 이 행을 쓴다
// (예전에는 저장 전이라 첫 조회 때 TTM·PER이 NULL이 되거나, 재조회 때 옛 값으로 계산되는 문제가 있었다)
async function getStoredPeriod(db, corpCode, year, reprtCode, keys, ov) {
  if (ov && String(ov.bsns_year) === String(year) && ov.reprt_code === reprtCode) {
    const o = { fs_div: ov.fs_div || null }; for (const k of keys) o[k] = ov[k] != null ? ov[k] : null; return o;
  }
  if (!db) return null;
  const row = await db
    .prepare(`SELECT fs_div, ${keys.join(",")} FROM financial_raw WHERE corp_code = ? AND bsns_year = ? AND reprt_code = ?`)
    .bind(corpCode, String(year), reprtCode)
    .first();
  return row || null;
}

// 사업보고서(연간)의 thstrm_amount는 "1년 누적"이라, 4분기만 떼어내려면
// 연간 - (1분기+2분기+3분기 단독값)을 계산해야 한다 (화면 쪽 toQuarterlyRows와 같은 원리, DB 조회 버전)
async function getIsolatedQ4(db, corpCode, year, keys, ov) {
  const annual = await getStoredPeriod(db, corpCode, year, "11011", keys, ov);
  if (!annual) return null;
  const q1 = await getStoredPeriod(db, corpCode, year, "11013", keys, ov);
  const q2 = await getStoredPeriod(db, corpCode, year, "11012", keys, ov);
  const q3 = await getStoredPeriod(db, corpCode, year, "11014", keys, ov);
  const result = { fs_div: annual.fs_div };
  // 연결(CFS)과 개별(OFS)이 섞인 기간끼리는 빼거나 더하지 않는다(기준이 다르면 값이 왜곡됨)
  if ([q1, q2, q3].some((q) => q && q.fs_div && annual.fs_div && q.fs_div !== annual.fs_div)) { for (const k of keys) result[k] = null; return result; }
  for (const k of keys) {
    if (annual[k] != null && q1 && q1[k] != null && q2 && q2[k] != null && q3 && q3[k] != null) {
      result[k] = annual[k] - (q1[k] + q2[k] + q3[k]);
    } else {
      result[k] = null;
    }
  }
  return result;
}

// 공시 시점(year, quarterNum) 기준 최근 4개 분기(TTM) 합산. quarterNum=4(사업보고서)면 그 해 자체가 이미 TTM.
// 현금흐름표 항목은 반기·3분기 보고서에도 "연초부터의 누적값"만 공시된다(손익계산서는 해당 분기 단독값이 따로 있음).
// 그래서 현금흐름 항목의 TTM = 전년 연간 + 올해 누적(YTD) − 전년 같은 시점 누적(YTD).
async function getTTMFlow(db, corpCode, year, quarterNum, keys, ov) {
  if (quarterNum === 4) return await getStoredPeriod(db, corpCode, year, "11011", keys, ov);
  const cfKeys = keys.filter((k) => CF_CUMULATIVE_KEYS.includes(k));
  const isKeys = keys.filter((k) => !CF_CUMULATIVE_KEYS.includes(k));
  if (cfKeys.length) {
    const result = {};
    if (isKeys.length) Object.assign(result, (await getTTMFlow(db, corpCode, year, quarterNum, isKeys, ov)) || Object.fromEntries(isKeys.map((k) => [k, null])));
    const code = REPRT_CODE_BY_Q[quarterNum];
    const prevAnnual = await getStoredPeriod(db, corpCode, year - 1, "11011", cfKeys, ov);
    const ytd = await getStoredPeriod(db, corpCode, year, code, cfKeys, ov);
    const prevYtd = await getStoredPeriod(db, corpCode, year - 1, code, cfKeys, ov);
    const cfBasis = [prevAnnual, ytd, prevYtd].filter((x) => x && x.fs_div).map((x) => x.fs_div);
    const cfMixed = new Set(cfBasis).size > 1;
    for (const k of cfKeys) {
      result[k] = (!cfMixed && prevAnnual && prevAnnual[k] != null && ytd && ytd[k] != null && prevYtd && prevYtd[k] != null) ? prevAnnual[k] + ytd[k] - prevYtd[k] : null;
    }
    return result;
  }

  const parts = [];
  for (let k = 1; k <= quarterNum; k++) parts.push(await getStoredPeriod(db, corpCode, year, REPRT_CODE_BY_Q[k], keys, ov));
  for (let k = quarterNum + 1; k <= 4; k++) {
    parts.push(k === 4 ? await getIsolatedQ4(db, corpCode, year - 1, keys, ov) : await getStoredPeriod(db, corpCode, year - 1, REPRT_CODE_BY_Q[k], keys, ov));
  }
  if (parts.some((p) => !p)) return null;
  if (new Set(parts.map((p) => p.fs_div).filter(Boolean)).size > 1) return null; // 연결/개별 혼합 금지

  const result = {};
  for (const k of keys) {
    const vals = parts.map((p) => p[k]);
    result[k] = vals.every((v) => v != null) ? vals.reduce((a, b) => a + b, 0) : null;
  }
  return result;
}

async function fetchPeriodRow(corpCode, stockCode, period, proxyUrl, db) {
  const emptyRow = (extra) => {
    const row = {
      corp_code: corpCode,
      bsns_year: String(period.year),
      reprt_code: period.code,
      period_label: `${period.year} ${period.label}`,
      period_order: period.year * 10 + period.order,
      fs_div: null,
      total_shares: null, treasury_shares: null, preferred_shares: null, dividend_per_share: null,
      filing_date: null, price_at_filing: null, price_at_period_end: null, per_at_filing: null, pbr_at_filing: null, fcf_yield_at_filing: null,
      roa_at_filing: null, peg_at_filing: null, eps_at_filing: null, eps_growth_at_filing: null,
      shares_source: null,
    };
    for (const item of ACCOUNT_ITEMS) row[item.key] = null;
    row.capex = null;
    row.fcf = null;
    row.intangible_capex = null;
    delete row.capex_ppe;
    delete row.capex_intangible;
    return { ...row, ...extra };
  };

  try {
    let fsDiv = "CFS";
    let dart = await fetchDartWithRetry(corpCode, period.year, period.code, fsDiv, proxyUrl);
    // 연결재무제표가 "자료 없음(013)"일 때만 개별(OFS)로 대체. 한도초과·점검 등 일시 오류는 개별로 잘못 대체하지 않고 빈 행으로 둠(재조회 대상)
    if (dart.status === "013") {
      fsDiv = "OFS";
      dart = await fetchDartWithRetry(corpCode, period.year, period.code, fsDiv, proxyUrl);
    }
    if (dart.status !== "000") return emptyRow();

    const vals = {};
    for (const item of ACCOUNT_ITEMS) {
      vals[item.key] = sumAccount(dart.list, item.ids, item.names, SJ_BY_KEY[item.key] || ["BS", "IS", "CIS", "CF"], item.exact, item.sumNames, item.sumContains);
      // 현금유출 항목은 공시 부호가 회사마다 다르므로 절댓값으로 통일(양수 = 유출 규모)
      if (item.exact && vals[item.key] != null && ["dividends_paid", "buyback"].includes(item.key)) vals[item.key] = Math.abs(vals[item.key]);
    }

    // 유형자산 취득이 없으면 CAPEX·FCF를 만들지 않음(무형자산만 잡히면 CAPEX가 과소 → FCF 과대평가)
    const capex = vals.capex_ppe != null
      ? Math.abs(vals.capex_ppe) + Math.abs(vals.capex_intangible || 0)
      : null;
    const fcf = vals.ocf != null && capex != null ? vals.ocf - capex : null;

    let stockCounts = { total_shares: null, treasury_shares: null, preferred_shares: null };
    let sharesSource = null;
    let stockFailReason = null; // 주식수 조회가 비었을 때 화면에서 원인을 볼 수 있도록 출처 칸에 남긴다
    let dividendPerShare = null;
    // 사업보고서(연간)는 재시도 1회 — 일시 실패 때문에 오래된 자기주식수가 연말 값으로 둔갑하는 것을 막는다
    for (let attempt = 0; attempt < (period.code === "11011" ? 2 : 1) && stockCounts.total_shares == null; attempt++) {
      try {
        const stockDart = await fetchDartGeneric("stockTotqySttus", corpCode, period.year, period.code, proxyUrl);
        stockCounts = pickStockCounts(stockDart);
        if (stockCounts.total_shares != null) sharesSource = "공시";
        else stockFailReason = stockCounts.reason || null;
      } catch (e) { stockFailReason = "조회 오류 " + String((e && e.message) || e).slice(0, 40); /* 실패해도 나머지는 살림 */ }
    }

    // 1·3분기 등은 주식총수현황이 공시되지 않는 경우가 많음 → 가장 최근 공시된 이전 기간 값을 이월
    // (자사주 매입/신주발행 등 중간 변동이 있었다면 다소 부정확할 수 있음 — 그래서 출처를 별도 표시)
    // 연간(사업보고서)은 이월하지 않는다: 연말 자기주식수는 분기 중 처분·소각으로 크게 달라질 수 있어 틀린 값이 '연말 값'처럼 보이기 때문(휴메딕스 2025 사례)
    if (stockCounts.total_shares == null && period.code === "11011") sharesSource = "미확인(연말 주식수 조회 실패" + (stockFailReason ? ": " + stockFailReason : "") + " — 재조회 필요)";
    if (stockCounts.total_shares == null && period.code !== "11011") {
      const fallback = await getFallbackShareCounts(db, corpCode, period.year * 10 + period.order);
      if (fallback) {
        stockCounts = { total_shares: fallback.total_shares, treasury_shares: fallback.treasury_shares, preferred_shares: fallback.preferred_shares != null ? fallback.preferred_shares : null };
        sharesSource = `이월(${fallback.period_label})`;
      } else {
        sharesSource = "미확인(" + (stockFailReason || "이월할 이전 공시 없음") + ")";
      }
    }

    if (period.code === "11011") {
      try {
        const divDart = await fetchDartGeneric("alotMatter", corpCode, period.year, period.code, proxyUrl);
        dividendPerShare = pickDividendPerShare(divDart);
      } catch (e) { /* 실패해도 나머지는 살림 */ }
    }

    const row = emptyRow({
      fs_div: fsDiv,
      total_shares: stockCounts.total_shares,
      treasury_shares: stockCounts.treasury_shares,
      preferred_shares: stockCounts.preferred_shares,
      dividend_per_share: dividendPerShare,
      shares_source: sharesSource,
    });
    for (const item of ACCOUNT_ITEMS) row[item.key] = vals[item.key];
    row.capex = capex;
    row.fcf = fcf;
    row.intangible_capex = vals.capex_intangible != null ? Math.abs(vals.capex_intangible) : null;
    delete row.capex_ppe;
    delete row.capex_intangible;

    // 회계기간 말일(분기말/반기말/연말) 종가 — 공시일 주가와 비교해 "실적 발표 전후 주가 상승률"을 보기 위함
    try {
      const periodEnd = periodEndDateStr(period.year, period.code);
      if (periodEnd && stockCode) {
        const pePrices = await fetchNaverPrices(stockCode, addDaysStr(periodEnd, -15), periodEnd, proxyUrl);
        row.price_at_period_end = closeAsOf(pePrices, periodEnd);
      }
    } catch (e) { /* 실패해도 나머지는 살림 */ }

    // 공시일자(rcept_no 앞 8자리) 기준 종가로 그 시점 PER/PBR/FCF Yield 계산
    const rceptNo = dart.list[0] && dart.list[0].rcept_no;
    if (rceptNo && stockCode) {
      const rceptDate = rceptNo.slice(0, 8);
      const filingDate = effectiveFilingDate(rceptDate, period.year, period.code);
      const filingCapped = filingDate !== rceptDate; // 상한 적용(정정공시 등)이면 월말이 휴장일일 수 있어 다음 영업일 종가를 쓴다
      row.filing_date = filingDate;
      try {
        const prices = await fetchNaverPrices(stockCode, addDaysStr(filingDate, -15), filingCapped ? addDaysStr(filingDate, 10) : filingDate, proxyUrl);
        const price = filingCapped ? closeOnOrAfter(prices, filingDate) : closeAsOf(prices, filingDate);
        row.price_at_filing = price;

        // 화면(재무분석)의 shareOut과 같은 기준: 발행주식수 − 자기주식 + 우선주 (순이익·자본이 모든 주식 종류의 몫이므로 같은 기준으로 나눔)
        const outstandingRaw = (stockCounts.total_shares != null && stockCounts.treasury_shares != null)
          ? stockCounts.total_shares - stockCounts.treasury_shares + (stockCounts.preferred_shares || 0)
          : null;
        const outstanding = outstandingRaw > 0 ? outstandingRaw : null; // 0/음수(데이터 이상)는 나눗셈 방지용으로 null 처리
        const equityForBps = vals.parent_equity != null ? vals.parent_equity : vals.total_equity;

        // PER·FCF Yield·ROA·PEG는 그 분기 하나만의 값이 아니라 TTM(최근 4개 분기 합산)을 씀 — 1·2·3분기도 "1년치" 기준이 되도록
        const quarterNum = period.order;
        const ttm = await getTTMFlow(db, corpCode, period.year, quarterNum, ["net_income", "parent_net_income", "fcf"], row);
        const ttmEarnings = ttm ? (ttm.parent_net_income != null ? ttm.parent_net_income : ttm.net_income) : null;
        const ttmFcf = ttm ? ttm.fcf : null;

        let eps = null, bps = null;
        if (price != null && outstanding) {
          eps = ttmEarnings != null ? ttmEarnings / outstanding : null;
          bps = equityForBps != null ? equityForBps / outstanding : null;
          const fcfPerShare = ttmFcf != null ? ttmFcf / outstanding : null;
          row.per_at_filing = (eps && eps > 0) ? price / eps : null;
          row.pbr_at_filing = (bps && bps > 0) ? price / bps : null;
          row.fcf_yield_at_filing = fcfPerShare != null ? fcfPerShare / price : null;
        }
        row.eps_at_filing = eps; // EPS(TTM) = TTM 순이익(지배주주 우선) ÷ 유통주식수

        // ROA(TTM) = TTM 연결순이익 ÷ 평균총자산 (평균: 이번 분기말 + 1년 전 같은 분기말)
        const totalAssetsNow = (vals.total_liabilities != null && vals.total_equity != null) ? vals.total_liabilities + vals.total_equity : null;
        const priorYearBS = await getStoredPeriod(db, corpCode, period.year - 1, period.code, ["total_liabilities", "total_equity"]);
        const totalAssetsPrior = (priorYearBS && priorYearBS.total_liabilities != null && priorYearBS.total_equity != null)
          ? priorYearBS.total_liabilities + priorYearBS.total_equity
          : null;
        const avgAssets = (totalAssetsNow != null && totalAssetsPrior != null) ? (totalAssetsNow + totalAssetsPrior) / 2 : totalAssetsNow;
        row.roa_at_filing = (avgAssets && ttm && ttm.net_income != null) ? ttm.net_income / avgAssets : null;

        // EPS 성장률(TTM, YoY) = (EPS(현재 시점) - EPS(1년 전 같은 시점)) ÷ EPS(1년 전 같은 시점)
        // — 1년 전 시점의 EPS도 "현재" 유통주식수로 나눠 근사(당시 실제 주식수는 안 씀 — 증자/감자가 있었으면 다소 부정확할 수 있음)
        // PEG(TTM) = PER(TTM) ÷ EPS 성장률(%)
        if (outstanding) {
          const ttmPrior = await getTTMFlow(db, corpCode, period.year - 1, quarterNum, ["net_income", "parent_net_income"], row);
          const ttmEarningsPrior = ttmPrior ? (ttmPrior.parent_net_income != null ? ttmPrior.parent_net_income : ttmPrior.net_income) : null;
          const epsPrior = ttmEarningsPrior != null ? ttmEarningsPrior / outstanding : null;
          if (eps != null && epsPrior != null && epsPrior > 0) {
            const growthRatio = (eps - epsPrior) / epsPrior; // 소수(예: 0.12 = 12%)
            row.eps_growth_at_filing = growthRatio;
            if (row.per_at_filing != null && growthRatio > 0) {
              row.peg_at_filing = row.per_at_filing / (growthRatio * 100); // 역성장 구간은 PEG가 의미 없어 null 유지
            }
          }
        }
      } catch (e) { /* 주가 조회 실패해도 나머지 재무데이터는 살림 */ }
    }

    return row;
  } catch (e) {
    return emptyRow({ error: String(e.message || e) });
  }
}

// 재무분석 확장으로 늘어난 컬럼을 (없을 때만) 한 번 추가한다. PRAGMA 1회로 확인하므로 평소엔 쿼리 1개만 쓴다.
let finColsReady = false;
const FIN_EXTRA_COLS = ["current_assets", "current_liabilities", "long_term_borrowings", "bonds", "depreciation", "amortization", "dividends_paid", "buyback", "intangible_capex", "goodwill", "preferred_shares", "lease_liabilities_nc"];
async function ensureFinancialColumns(db) {
  if (finColsReady) return;
  const info = await db.prepare("PRAGMA table_info(financial_raw)").all();
  const have = new Set((info.results || []).map((c) => c.name));
  for (const c of FIN_EXTRA_COLS) {
    if (!have.has(c)) {
      try { await db.prepare(`ALTER TABLE financial_raw ADD COLUMN ${c} REAL`).run(); } catch (e) { /* 동시 실행으로 이미 추가됨 */ }
    }
  }
  finColsReady = true;
}

async function saveRowsToDb(db, rows) {
  await ensureFinancialColumns(db);
  const now = new Date().toISOString();
  const colSql = DB_COLUMNS.join(", ");
  const ph = "(" + DB_COLUMNS.map(() => "?").join(", ") + ")";
  for (const r of rows) {
    // 오류가 났거나, DART에 데이터가 없는(아직 공시 전인) 빈 행은 저장하지 않음.
    // 빈 행을 저장하면 "완결된 연도"로 오인되거나 기존 정상 데이터를 null로 덮어쓸 수 있다.
    if (r.error || r.fs_div == null) continue;
    const values = DB_COLUMNS.map((c) => (c === "updated_at" ? now : r[c] ?? null));
    await db.prepare(`INSERT OR REPLACE INTO financial_raw (${colSql}) VALUES ${ph}`).bind(...values).run();
  }
}

export default {
  async fetch(request, env) {
    try {
      return await handleRequest(request, env);
    } catch (e) {
      // 라우트 하나하나에 try/catch를 다 붙이는 대신, 맨 바깥에서 한 번에 받아서 "화면에 원인이 보이는"
      // 일관된 JSON 에러로 바꿔준다. 콘솔에도 남겨서 Cloudflare Observability에서 스택을 확인할 수 있다.
      console.error("처리되지 않은 오류:", e && e.stack ? e.stack : e);
      return Response.json({ error: (e && e.message) || "알 수 없는 오류가 발생했습니다." }, { status: 500 });
    }
  },

  async scheduled(event, env, ctx) {
    try {
      return await handleScheduled(event, env, ctx);
    } catch (e) {
      console.error("scheduled 처리되지 않은 오류:", e && e.stack ? e.stack : e);
    }
  },
};

async function handleRequest(request, env) {
    const { pathname, searchParams } = new URL(request.url);

    if (pathname === "/") {
      return new Response(HTML_PAGE, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    if (pathname === "/api/raw") {
      const kind = searchParams.get("kind");
      const corpName = searchParams.get("corp_name");
      const bsnsYear = searchParams.get("bsns_year");
      const reprtCode = searchParams.get("reprt_code") || "11011";
      const fsDiv = searchParams.get("fs_div");
      const bgnDe = searchParams.get("bgn_de");
      const endDe = searchParams.get("end_de");

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      let raw;
      if (bgnDe && endDe) {
        raw = await fetchDartByDateRange(kind, corpRow.corp_code, bgnDe, endDe, env.DART_PROXY_URL);
      } else if (fsDiv) {
        raw = await fetchDart(corpRow.corp_code, bsnsYear, reprtCode, fsDiv, env.DART_PROXY_URL);
      } else {
        raw = await fetchDartGeneric(kind, corpRow.corp_code, bsnsYear, reprtCode, env.DART_PROXY_URL);
      }
      return Response.json(raw);
    }

    if (pathname === "/api/latest-price") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!corpRow.stock_code) return Response.json({ error: "종목코드가 없어 주가를 조회할 수 없습니다." }, { status: 400 });

      const today = new Date();
      const endStr = `${today.getFullYear()}${String(today.getMonth() + 1).padStart(2, "0")}${String(today.getDate()).padStart(2, "0")}`;
      const startStr = addDaysStr(endStr, -10);
      const prices = await fetchNaverPrices(corpRow.stock_code, startStr, endStr, env.DART_PROXY_URL);
      if (prices.length === 0) return Response.json({ error: "최근 시세를 가져오지 못했습니다." }, { status: 502 });
      const latest = prices.slice().sort((a, b) => b.date.localeCompare(a.date))[0];

      return Response.json({ corp_name: corpRow.corp_name, date: latest.date, close: latest.close });
    }

    // 재무분석 화면용: 한 종목의 1M/3M/6M 수익률과 20D/60D/120D RS(코스피 대비). D1 쓰기 없음(읽기 1회 + data.go.kr 1~2회).
    if (pathname === "/api/momentum/stock") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!corpRow.stock_code) return Response.json({ error: "종목코드가 없어 주가를 조회할 수 없습니다." }, { status: 400 });
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      const { results: calRows } = await env.DB
        .prepare("SELECT market_date, close_price FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 145")
        .bind(KOSPI_NAME).all();
      if (calRows.length < 135) {
        return Response.json({ error: "코스피 일별 지수 데이터가 부족합니다(" + calRows.length + "개). 모멘텀 탭에서 '② 지금 계산'을 한 번 실행하면 코스피를 받아옵니다." }, { status: 409 });
      }
      const kClose = new Map(calRows.map((r) => [r.market_date, r.close_price]));
      const cal = calRows.map((r) => r.market_date);
      const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
      const kstNow = new Date(Date.now() + 9 * 3600 * 1000);
      const hist = await fetchKrxStockHistory(corpRow.stock_code, ymd(new Date(kstNow.getTime() - 215 * 86400000)), ymd(kstNow), env.DATA_GO_KR_KEY);
      if (!hist.length) return Response.json({ error: "data.go.kr에서 이 종목의 최근 시세를 받지 못했습니다." }, { status: 502 });
      const sClose = new Map(hist.map((h) => [h.date, h.close]));
      // 기준일: 종목 시세와 코스피 지수가 모두 있는 가장 최근 거래일(두 데이터의 공표 시점이 달라도 같은 날짜로 맞춘다)
      const i0 = cal.findIndex((d) => sClose.get(d) > 0);
      if (i0 < 0 || i0 > 5) return Response.json({ error: "종목 시세와 코스피 지수의 최근 날짜가 맞지 않습니다." }, { status: 409 });
      const d0 = cal[i0];
      const at = (n) => cal[i0 + n];
      const c0 = sClose.get(d0), k0 = kClose.get(d0);
      const out = { corp_name: corpRow.corp_name, stock_code: corpRow.stock_code, base_date: d0, close: c0, ret: {}, kospi: {}, rs: {}, missing: [] };
      const pts = { r1m: 21, r3m: 63, r6m: 126, rs20: 20, rs60: 60, rs120: 120 };
      for (const [key, n] of Object.entries(pts)) {
        const d = at(n), cs = d ? sClose.get(d) : null, ks = d ? kClose.get(d) : null;
        if (!(cs > 0) || !(ks > 0)) { out.missing.push(key); continue; }
        const rs = c0 / cs - 1, rk = k0 / ks - 1;
        if (key.startsWith("rs")) out.rs[key] = (1 + rs) / (1 + rk);
        else out.ret[key] = rs;
        out.kospi[key] = rk;
      }
      return Response.json(out);
    }

    if (pathname === "/api/trend/stock") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!corpRow.stock_code) return Response.json({ error: "종목코드가 없어 주가를 조회할 수 없습니다." }, { status: 400 });
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
      const kstNow = new Date(Date.now() + 9 * 3600 * 1000);
      const hist = await fetchKrxStockHistory(corpRow.stock_code, ymd(new Date(kstNow.getTime() - 430 * 86400000)), ymd(kstNow), env.DATA_GO_KR_KEY);
      if (!hist.length) return Response.json({ error: "data.go.kr에서 이 종목의 시세를 받지 못했습니다." }, { status: 502 });
      const res = { corp_name: corpRow.corp_name, ...computeTrendTemplate(hist, ymd(kstNow)) };
      // 시장 방향(오닐 M)과 6개월 상대강도 백분위(RS Rating 대용) — 저장된 코스피 지수·전 종목 일별 종가 사용. 부족하면 null(만들지 않음)
      try {
        const { results: kRows } = await env.DB.prepare("SELECT market_date, close_price FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 140").bind(KOSPI_NAME).all();
        const dnum = (d) => Date.UTC(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6, 8)) / 86400000;
        res.market = null; res.rs6 = null;
        if (kRows.length >= 71 && dnum(ymd(kstNow)) - dnum(kRows[0].market_date) <= 7) {
          let gapOk = true;
          for (let i = 1; i < 71; i++) if (dnum(kRows[i - 1].market_date) - dnum(kRows[i].market_date) > 12) gapOk = false;
          const sma = (off) => { let t = 0; for (let i = off; i < off + 50; i++) t += kRows[i].close_price; return t / 50; };
          if (gapOk) res.market = { date: kRows[0].market_date, close: kRows[0].close_price, sma50: sma(0), sma50prev: sma(20), up: kRows[0].close_price > sma(0), rising: sma(0) > sma(20) };
        }
        const hs = hist.filter((x) => x.close > 0);
        if (res.ok && kRows.length >= 127) {
          const dStart = kRows[126].market_date, dEnd = hs[hs.length - 1].date;
          const own1 = hs.find((x) => x.date === dStart), own2 = hs[hs.length - 1];
          if (own1 && kRows[0].market_date === dEnd) {
            const { results: u } = await env.DB.prepare("SELECT stock_code, market_date, close_price FROM market_raw_daily WHERE market_date IN (?1, ?2)").bind(dStart, dEnd).all();
            const m0 = new Map(), m1 = new Map();
            for (const r of u) { if (r.close_price > 0) (r.market_date === dStart ? m0 : m1).set(r.stock_code, r.close_price); }
            const rets = []; // 액면분할·감자 미수정 시세가 만든 극단값(-70% 이하, +400% 이상)은 제외(근사)
            for (const [code, c0] of m0) { const c1 = m1.get(code); if (c1 && code !== corpRow.stock_code) { const rr = c1 / c0 - 1; if (rr > -0.7 && rr < 4) rets.push(rr); } }
            if (rets.length >= 300) {
              const own = own2.close / own1.close - 1;
              res.rs6 = { pct: rets.filter((x) => x < own).length / rets.length * 100, ret: own, n: rets.length, from: dStart, to: dEnd };
            }
          }
        }
      } catch (e) { res.market = res.market || null; res.rs6 = res.rs6 || null; }
      return Response.json(res);
    }

    if (pathname === "/api/fetch-and-save") {
      const corpName = searchParams.get("corp_name");
      const year = Number(searchParams.get("year"));
      const reprtCode = searchParams.get("reprt_code");

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const periodMeta = REPRT_CODES.find((r) => r.code === reprtCode);
      if (!periodMeta) return Response.json({ error: `알 수 없는 reprt_code: ${reprtCode}` }, { status: 400 });

      const row = await fetchPeriodRow(corpRow.corp_code, corpRow.stock_code, { year, ...periodMeta }, env.DART_PROXY_URL, env.DB);
      await saveRowsToDb(env.DB, [row]);

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, row });
    }

    if (pathname === "/api/queue-fetch") {
      const corpName = searchParams.get("corp_name");
      const startYear = Number(searchParams.get("start_year"));
      const endYear = Number(searchParams.get("end_year"));
      if (!corpName || !startYear || !endYear || startYear > endYear) {
        return Response.json({ error: "종목명과 조회 기간(시작연도 ≤ 종료연도)을 확인해주세요." }, { status: 400 });
      }
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const periods = buildPeriodList(startYear, endYear);
      const now = new Date().toISOString();
      // INSERT OR IGNORE: 이미 큐에 있는(아직 처리 안 된) 기간은 중복 등록하지 않음
      const stmts = periods.map((p) => env.DB.prepare(
        "INSERT OR IGNORE INTO fetch_queue (corp_code, corp_name, stock_code, bsns_year, reprt_code, period_order, queued_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(corpRow.corp_code, corpRow.corp_name, corpRow.stock_code, String(p.year), p.code, p.period_order, now));
      await env.DB.batch(stmts);

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, queued: periods.length });
    }

    if (pathname === "/api/queue-status") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const { results } = await env.DB
        .prepare("SELECT bsns_year, reprt_code FROM fetch_queue WHERE corp_code = ? ORDER BY period_order")
        .bind(corpRow.corp_code)
        .all();

      return Response.json({ corp_name: corpRow.corp_name, remaining: results.length, next: results.slice(0, 5) });
    }

    if (pathname === "/api/db/companies") {
      // financial_raw의 PK 인덱스(corp_code, bsns_year, reprt_code)만으로 집계되는 쿼리라 테이블 본문을 읽지 않는다.
      const { results } = await env.DB.prepare(
        "SELECT f.corp_code AS corp_code, m.corp_name AS corp_name, m.stock_code AS stock_code, COUNT(*) AS n, MIN(f.bsns_year) AS first_year, MAX(f.bsns_year) AS last_year " +
        "FROM financial_raw f LEFT JOIN corp_master m ON m.corp_code = f.corp_code GROUP BY f.corp_code ORDER BY m.corp_name"
      ).all();
      const companies = results.filter((r) => r.corp_name);
      return new Response(JSON.stringify({ companies }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=30" } });
    }

    if (pathname === "/api/financial-history-db") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const { results } = await env.DB
        .prepare("SELECT * FROM financial_raw WHERE corp_code = ? ORDER BY period_order")
        .bind(corpRow.corp_code)
        .all();

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, rows: results });
    }

    if (pathname === "/api/backtest/fin-batch") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter(Boolean).slice(0, 8);
      if (!codes.length) return Response.json({ error: "codes 필요" }, { status: 400 });
      const { results } = await env.DB.prepare("SELECT * FROM financial_raw WHERE corp_code IN (" + codes.map(() => "?").join(",") + ") ORDER BY corp_code, period_order").bind(...codes).all();
      return new Response(JSON.stringify({ rows: results }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=60" } });
    }
    if (pathname === "/api/backtest/screen-plan") {
      const { results } = await env.DB.prepare("SELECT market_date FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 400").bind(KOSPI_NAME).all();
      return Response.json({ cal: results.map((r) => r.market_date) });
    }
    if (pathname === "/api/backtest/screen-date") {
      const o = {}; for (const [k, v] of searchParams.entries()) o[k] = v;
      try { return Response.json(await backtestScreenDate(env, o)); }
      catch (e) { return Response.json({ error: (e && e.message) || String(e) }, { status: 400 }); }
    }

    if (pathname === "/api/market/refresh-universe") {
      // 실제 로직은 scheduled()의 일일 자동 갱신과 공유하는 runUniverseRefresh()에 있다.
      // 이 라우트는 일부러 가볍게 만들었다: KRX 조회 + market_fetch_queue에 등록만 하고, 실제 D1 쓰기(가격
      // 이력 백필/상태 upsert)는 전부 Cron(scheduled())에서 몇 개씩 나눠 처리한다.
      const result = await runUniverseRefresh(env, searchParams.get("force") === "1");
      return Response.json(result);
    }

    if (pathname === "/api/market/status") {
      const [queueRow, checkedRow, passedRow, backfilledRow, todayRow] = await Promise.all([
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_fetch_queue").first(),
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status").first(),
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE passed = 1").first(),
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled = 1").first(),
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled_at >= ?").bind(todayUtcMidnightIso()).first(),
      ]);
      return Response.json({
        queue_remaining: queueRow?.c ?? 0,
        checked_total: checkedRow?.c ?? 0,
        passed_total: passedRow?.c ?? 0,
        backfilled_total: backfilledRow?.c ?? 0,
        backfilled_today: todayRow?.c ?? 0,
        daily_cap: MARKET_DAILY_WRITE_CAP,
        min_trading_value: MARKET_MIN_TRADING_VALUE,
        min_mcap: MARKET_MIN_MCAP,
        append_job: await (async () => { try { const r = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'append'").first(); return r ? JSON.parse(r.v) : null; } catch (e) { return null; } })(),
      });
    }

    // ④ 데이터 관리 화면의 현황판: 가벼운 집계만(각 쿼리는 PK 인덱스로 처리되는 작은 표).
    if (pathname === "/api/data/summary") {
      await ensureScreenTables(env.DB);
      const one = async (sql) => { try { return await env.DB.prepare(sql).first(); } catch (e) { return null; } };
      const all = async (sql) => { try { return (await env.DB.prepare(sql).all()).results; } catch (e) { return []; } };
      const [mk, uni, snap, fin, fq, mq, macro] = await Promise.all([
        one("SELECT MAX(market_date) AS d FROM market_raw_daily"),
        one("SELECT SUM(CASE WHEN passed = 1 THEN 1 ELSE 0 END) AS passed, SUM(CASE WHEN backfilled = 1 THEN 1 ELSE 0 END) AS bf FROM market_universe_status"),
        one("SELECT MAX(snap_date) AS d, COUNT(DISTINCT snap_date) AS n FROM screen_lists"),
        one("SELECT COUNT(DISTINCT corp_code) AS c, COUNT(*) AS n FROM financial_raw"),
        one("SELECT COUNT(*) AS n, COUNT(DISTINCT corp_code) AS c FROM fetch_queue"),
        one("SELECT COUNT(*) AS n FROM market_fetch_queue"),
        all("SELECT series, MAX(date) AS last, COUNT(*) AS n FROM macro_data GROUP BY series"),
      ]);
      return Response.json({
        market_last: mk ? mk.d : null, passed: uni ? uni.passed : 0, backfilled: uni ? uni.bf : 0, market_queue: mq ? mq.n : 0,
        screen_last: snap ? snap.d : null, screen_snaps: snap ? snap.n : 0,
        fin_companies: fin ? fin.c : 0, fin_rows: fin ? fin.n : 0, fin_queue_rows: fq ? fq.n : 0, fin_queue_companies: fq ? fq.c : 0,
        macro,
      });
    }

    if (pathname === "/api/market/top30") {
      // 예전 Top30은 가격 원본 전체를 읽어(수십만 행) 무료 읽기 한도를 크게 쓰고 Worker CPU 한도에도 걸릴 수 있어
      // 중단했다. 같은 기능이 /api/screen/*(SQL 집계 + 저장된 스냅샷)으로 대체됐다.
      return Response.json({ error: "이 기능은 '주간 Top 50 스크리닝'으로 대체되었습니다(/api/screen/top)." }, { status: 410 });
    }

    if (pathname === "/api/screen/top") {
      const snap = await loadScreenSnapshot(env, searchParams.get("date"));
      if (!snap) return Response.json({ error: "저장된 스크리닝 결과가 없습니다. '스크리닝 실행'을 눌러 지금 계산하거나(고급에서 \"결과 저장\" 체크 시 저장), 토요일 새벽 1시 자동 저장을 기다려주세요." }, { status: 404 });
      return Response.json(snap);
    }

    // 사용자가 입력한 조건으로 지금 계산(기본은 저장하지 않음 = 쓰기 0건). save=1이면 주간 결과로 저장한다.
    if (pathname === "/api/screen/run" || pathname === "/api/screen/refresh") {
      const num = (k) => { const raw = searchParams.get(k); if (raw === null || raw === "") return null; const v = Number(raw); return Number.isFinite(v) ? v : null; };
      let kospiError = null;
      try { await refreshKospi(env); } catch (e) { kospiError = (e && e.message) || String(e); }
      const result = await computeScreenLists(env, { minMcapEok: num("mcap"), minTvEok: num("tv"), pcMcap: num("pcm"), pcTv: num("pct"), minPrice: num("price"), w3: num("w3"), w6: num("w6") });
      if (kospiError) result.meta.kospiError = kospiError;
      if (searchParams.get("save") === "1") await saveScreenLists(env, result);
      await attachOpIncome(env, result.lists);
      return Response.json({ date: result.meta.asof, dates: [], meta: result.meta, lists: result.lists, saved: searchParams.get("save") === "1" });
    }

    // 화면이 보여준 종목 중 영업이익이 아직 없는 종목을 12개씩 조회한다(화면이 남은 개수가 0이 될 때까지 반복 호출).
    // 진단용: 한 종목의 영업이익 조회 과정을 단계별 원문(앞부분)으로 보여준다. 예) /api/debug/opincome?code=005930
    if (pathname === "/api/debug/opincome") {
      const code = (searchParams.get("code") || "005930").trim();
      const out = { code, steps: [] };
      const raw = async (label, urls, params) => {
        for (const u of urls) {
          const url = new URL(u);
          url.searchParams.set("serviceKey", env.DATA_GO_KR_KEY || "");
          for (const [k, v] of Object.entries(params)) url.searchParams.set(k, String(v));
          try {
            const r = await fetch(url.toString());
            const t = await r.text();
            out.steps.push({ label, url: u, status: r.status, head: t.slice(0, 700) });
            if (r.status === 200 && /<resultCode>0?0<\/resultCode>/.test(t)) return t;
          } catch (e) { out.steps.push({ label, url: u, error: String((e && e.message) || e) }); }
        }
        return null;
      };
      const t1 = await raw("종목정보", KRX_LISTED_URLS, { numOfRows: 3, pageNo: 1, likeSrtnCd: code, resultType: "xml" });
      const crno = t1 && (t1.match(/<crno>([^<]+)<\/crno>/) || [])[1];
      out.crno = crno || null;
      if (crno) {
        const y = new Date().getUTCFullYear();
        await raw("재무(" + (y - 1) + ")", FINA_STAT_URLS, { crno, bizYear: y - 1, numOfRows: 3, pageNo: 1, resultType: "xml" });
        await raw("재무(" + (y - 2) + ")", FINA_STAT_URLS, { crno, bizYear: y - 2, numOfRows: 3, pageNo: 1, resultType: "xml" });
      }
      return Response.json(out);
    }

    if (pathname === "/api/screen/fill-opincome") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter((x) => /^[0-9A-Za-z]{6}$/.test(x)).slice(0, 400);
      if (!codes.length) return Response.json({ processed: 0, remaining: 0, values: {}, errors: [] });
      try {
        return Response.json(await fillOpIncome(env, codes, OPFILL_BATCH));
      } catch (e) {
        console.error("영업이익 조회 실패:", (e && e.message) || e); // 원인 문구를 로그에도 남긴다
        return Response.json({ error: (e && e.message) || String(e) }, { status: 502 });
      }
    }

    if (pathname === "/api/macro/refresh") {
      // start를 직접 지정하지 않으면(평소 "갱신" 버튼) 지표별로 "마지막 저장일 다음날"부터만 증분 수집한다.
      // 한 번도 받은 적 없는 지표만 최근 9년(분기말·공시일 상관관계 분석에 충분)부터 최초 백필한다.
      // start를 직접 지정하면(수동 재백필용) 그 날짜부터 전체를 다시 받는다 — D1 쓰기 한도를 많이
      // 쓰므로 꼭 필요할 때만 사용할 것.
      const end = searchParams.get("end") || todayStr();
      const explicitStart = searchParams.get("start") || null;
      const defaultLookbackStart = addDaysStr(end, -9 * 365); // macro_data도 행당 쓰기 2건(PK 인덱스)이라 최초 백필을 9년(≈2.3만 건)으로 제한
      const summary = await runMacroRefresh(env, explicitStart, end, defaultLookbackStart);
      return Response.json({ start: explicitStart || "(지표별 증분)", end, summary, partial: Object.values(summary).some((x) => x && x.partial) });
    }

    if (pathname === "/api/macro/series") {
      const { results } = await env.DB.prepare("SELECT series, date, value FROM macro_data ORDER BY series, date").all();
      return Response.json({ rows: results });
    }

    return new Response("Not Found", { status: 404 });
}

// Cron Trigger: 1분마다 깨어난다. 재무데이터 큐와 모멘텀 유니버스 큐가 subrequest 예산(요청당 50건)을
// 같이 나눠 쓰면 한도를 넘길 수 있어, 짝수 분/홀수 분으로 번갈아 처리해 완전히 분리한다.
// ============================================================
// 스크리닝 v3 — 한국 시장 단기·중기 모멘텀(주간 Top50) + 재무지표 병기
// 설계 근거(자세한 설명은 화면의 "방법론" 접이식 안내 참고):
//  - 중기 모멘텀(3·6개월): Jegadeesh & Titman(1993). 최근 1개월(21거래일)은 건너뜀(단기 반전: Jegadeesh 1990, Lehmann 1990).
//  - 종합점수: 3M·6M 건너뛴 수익률의 백분위 평균(가중치 기본 50:50). 유동성·시총·최저가 하한으로 소형 잡주를 제외.
//  - 한국 시장은 모멘텀 효과가 약하거나 불안정하다는 보고가 있어 결과는 참고용(투자 권유 아님).
//  - 보관 이력이 약 126거래일이라 12-1 모멘텀과 52주 신고가(George & Hwang 2004)는 계산하지 않음.
//  - 재무(밸류·퀄리티)는 Top50에 "함께 표시"해서 직접 판단(Asness·Moskowitz·Pedersen 2013, Novy-Marx 2013).
// 무료 한도 설계: 계산은 SQL 한 번(읽기 약 수십만 행)으로 끝내고 결과(51행)만 저장한다. 평소 조회는 저장분만 읽는다.
// ============================================================
// 주간 자동 스크리닝 시각: 토요일 01:00 한국시간 = 금요일 16:00 UTC (getUTCDay 5 = 금요일)
const WEEKLY_SCREEN_UTC_DOW = 5;
const WEEKLY_SCREEN_UTC_HOUR = 16;
const KOSPI_INDEX_API = "https://apis.data.go.kr/1160100/GetMarketIndexInfoService_V2/getStockMarketIndex_V2";
const KOSPI_NAME = "코스피";
const SCREEN_TOP_N = 50;
// 기본 필터(한국 시장 실무 관행 + 학계의 "초소형·저유동 종목 제외" 관행). 학술적으로 "정답"인 임계값은 없고, 값을 낮추면 거래비용·호가 문제로
// 실제로는 사기 어려운 종목이 늘어난다는 점이 핵심이다(Hou·Xue·Zhang 2020: 이상현상 상당수가 초소형주에서 나오며 이를 제외하면 재현성이 크게 떨어진다).
const SCREEN_DEFAULT_MCAP_EOK = 500;  // 시가총액 절대 하한(억원) = 저장 유니버스 하한. 실제 거름은 아래 "하위 N% 제외"가 한다.
const SCREEN_DEFAULT_TV_EOK = 15;     // "최근 20거래일 평균" 거래대금 절대 하한(억원) = 저장 유니버스 하한.
const SCREEN_DEFAULT_PCT_MCAP = 20;  // 시가총액 하위 20% 제외(한국 퀀트 실무: 소형주 하위 20~30% 제외가 일반적)
const SCREEN_DEFAULT_PCT_TV = 20;    // 20일 평균 거래대금 하위 20% 제외
const SCREEN_HOT_PCT = 0.95;         // ⚠ 과열 표시: 최근 1개월 수익률이 후보 중 상위 5%
const SCREEN_DEFAULT_MIN_PRICE = 1000; // 최소 주가(원). 동전주는 호가단위 때문에 수익률이 왜곡된다(호가 1틱이 수 %).
const SCREEN_DEFAULT_WEIGHTS = { w3: 0.5, w6: 0.5 }; // 복합 점수: (2~3개월 수익률 순위) / (2~6개월 수익률 순위) 가중
const SCREEN_AVG_DAYS = 20;
const SCREEN_LIST_KEYS = ["r20", "r60", "r120", "rw", "m16", "rs1", "rs3", "rs6"];
let screenTablesReady = false;

async function ensureScreenTables(db) {
  if (screenTablesReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS market_index_daily (market_date TEXT NOT NULL, idx_name TEXT NOT NULL, close_price REAL, PRIMARY KEY (market_date, idx_name))").run();
  // 주간 스냅샷: 목록 1개 = 1행(JSON). 한 번 저장에 9행(메타 1 + 목록 8)이라 쓰기가 매우 적다.
  await db.prepare("CREATE TABLE IF NOT EXISTS screen_lists (snap_date TEXT NOT NULL, list_key TEXT NOT NULL, payload TEXT, PRIMARY KEY (snap_date, list_key))").run();
  // data.go.kr(종목정보→법인등록번호→기업재무정보)로 받은 영업이익 캐시. 한 번 받으면 120일간 재사용한다.
  await db.prepare("CREATE TABLE IF NOT EXISTS op_income_cache (stock_code TEXT PRIMARY KEY, crno TEXT, biz_year INTEGER, op_profit REAL, fetched_at TEXT)").run();
  screenTablesReady = true;
}

function parseIndexXml(xml) {
  const items = [];
  const re = /<item>([\s\S]*?)<\/item>/g;
  const f = (b, t) => { const x = b.match(new RegExp(`<${t}>([^<]*)</${t}>`)); return x ? x[1] : null; };
  let m;
  while ((m = re.exec(xml)) !== null) {
    const clpr = f(m[1], "clpr");
    items.push({ basDt: f(m[1], "basDt"), idxNm: f(m[1], "idxNm"), clpr: clpr == null || clpr === "" ? null : Number(clpr) });
  }
  const tc = xml.match(/<totalCount>(\d+)<\/totalCount>/);
  const rc = xml.match(/<resultCode>([^<]*)<\/resultCode>/);
  const rm = xml.match(/<resultMsg>([^<]*)<\/resultMsg>/);
  return { items, totalCount: tc ? Number(tc[1]) : items.length, resultCode: rc ? rc[1] : null, resultMsg: rm ? rm[1] : null };
}

// 코스피 지수 일별 종가(공공데이터포털 "금융위원회_지수시세정보"). 이 API는 주식시세정보와 별도로 "활용신청"이 필요하다.
async function fetchIndexHistory(idxName, beginBasDt, endBasDt, apiKey) {
  const all = [];
  for (let pageNo = 1; pageNo <= 10; pageNo++) {
    const url = new URL(KOSPI_INDEX_API);
    url.searchParams.set("serviceKey", apiKey);
    url.searchParams.set("numOfRows", "500");
    url.searchParams.set("pageNo", String(pageNo));
    url.searchParams.set("idxNm", idxName);
    url.searchParams.set("beginBasDt", beginBasDt);
    url.searchParams.set("endBasDt", endBasDt);
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    let text;
    try {
      const resp = await fetch(url.toString(), { signal: controller.signal });
      text = await resp.text();
    } finally {
      clearTimeout(timer);
    }
    const { items, totalCount, resultCode, resultMsg } = parseIndexXml(text);
    if (resultCode !== "00") {
      throw new Error(`지수시세정보 API 응답 오류(${resultCode || "응답 형식 이상"} ${resultMsg || text.slice(0, 80)}). 공공데이터포털에서 '금융위원회_지수시세정보' 활용신청이 승인됐는지 확인해주세요.`);
    }
    all.push(...items);
    if (items.length === 0 || all.length >= totalCount) break;
  }
  return all.filter((it) => it.idxNm === idxName && it.clpr != null);
}

async function refreshKospi(env) {
  await ensureScreenTables(env.DB);
  if (!env.DATA_GO_KR_KEY) throw new Error("DATA_GO_KR_KEY 미설정");
  const last = await env.DB.prepare("SELECT MAX(market_date) AS d FROM market_index_daily WHERE idx_name = ?").bind(KOSPI_NAME).first();
  const end = todayStr();
  const begin = last && last.d ? addDaysStr(last.d, 1) : addDaysStr(end, -420);
  if (begin > end) return { added: 0 };
  const rows = await fetchIndexHistory(KOSPI_NAME, begin, end, env.DATA_GO_KR_KEY);
  if (rows.length) {
    await multiRowInsert(env.DB, "INSERT OR REPLACE INTO market_index_daily (market_date, idx_name, close_price)", 3, rows, (r) => [r.basDt, KOSPI_NAME, r.clpr]);
  }
  return { added: rows.length };
}

function chunkArr(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

// ------------------------------------------------------------
// 7개 Top50 목록 스크리닝 (무료 한도 설계)
//  - 계산에 필요한 건 "기준일, 20·21·60·63·120·126거래일 전" 7개 날짜의 종가뿐이다. market_raw_daily의 기본키가
//    (market_date, stock_code)라서 날짜로 찾으면 그 날짜의 행만 읽는다(≈종목 수 × 7 ≈ 9천 행).
//    예전처럼 250일치 전체(수십만 행)를 훑지 않는다 → 한 번 계산에 읽기 약 2만 행(하루 한도 500만 행의 0.4%).
//  - 거래일 달력은 코스피 지수 일별 데이터(약 140행)를 쓴다.
//  - 순위 정렬·상위 50 추리기는 전부 SQL 안에서 끝내서 Worker CPU(무료 10ms)를 거의 쓰지 않는다.
//  - 평소 조회는 주간 저장분(8행)만 읽는다. 사용자가 입력해서 계산한 결과는 기본적으로 저장하지 않는다(쓰기 0).
// ------------------------------------------------------------
// ------------------------------------------------------------
// 백테스트용 API
//  - /api/backtest/fin-batch : 저장된 재무 원자료를 8개 종목씩 묶어 내려준다(분위 집계는 브라우저가 한다).
//  - /api/backtest/screen-plan : 거래일 달력(코스피 지수 일별) 내려준다.
//  - /api/backtest/screen-date : 과거 기준일 하나에서 스크리닝과 같은 신호(점수)와 이후 H거래일 수익률을 SQL로 계산해 준다.
//    Worker CPU(무료 10ms)를 아끼려고 순위·필터는 전부 SQL에서 끝내고, 종목별 [코드, 점수, 미래수익률]만 돌려준다.
// ------------------------------------------------------------
async function backtestScreenDate(env, o) {
  await ensureMarketQueueColumns(env.DB);
  const { results: calRows } = await env.DB.prepare("SELECT market_date, close_price FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 400").bind(KOSPI_NAME).all();
  const cal = calRows.map((r) => r.market_date);
  const ci = (k, d, lo, hi) => { const v = o[k] == null || o[k] === "" ? NaN : Number(o[k]); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, Math.round(v))) : d; };
  const cf = (k, d, lo, hi) => { const v = o[k] == null || o[k] === "" ? NaN : Number(o[k]); return Number.isFinite(v) ? Math.min(hi, Math.max(lo, v)) : d; };
  const i = ci("i", 0, 0, 399), S = ci("S", 21, 0, 63), L3 = ci("L3", 63, 21, 252), L6 = ci("L6", 126, 42, 300), H = ci("H", 21, 1, 126);
  let w3 = cf("w3", 0.5, 0, 10), w6 = cf("w6", 0.5, 0, 10);
  if (w3 + w6 <= 0) { w3 = 0.5; w6 = 0.5; }
  const nw3 = w3 / (w3 + w6), nw6 = w6 / (w3 + w6);
  const pcm = cf("pcm", 20, 0, 90) / 100, pct = cf("pct", 20, 0, 90) / 100, minPrice = cf("price", 1000, 0, 1e7);
  const need = Math.max(w6 > 0 ? L6 : 0, w3 > 0 ? L3 : 0, S, SCREEN_AVG_DAYS - 1);
  if (i - H < 0 || i + need >= cal.length) throw new Error("이 기준일은 저장된 이력 범위 밖입니다(달력 " + cal.length + "거래일).");
  const d0 = cal[i], dF = cal[i - H], dS = cal[i + S], d3 = cal[i + L3 < cal.length ? i + L3 : i + need], d6 = cal[i + L6 < cal.length ? i + L6 : i + need], dA = cal[i + SCREEN_AVG_DAYS - 1];
  const k0 = calRows[i].close_price, kF = calRows[i - H].close_price;
  const cnt = await env.DB.prepare("SELECT COUNT(*) AS c, SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) AS m FROM market_raw_daily WHERE market_date = ?").bind(d0).first();
  const hasMc = !!(cnt && cnt.c > 0 && cnt.m >= cnt.c * 0.8);
  const sql =
    "WITH m AS MATERIALIZED (" +
    " SELECT stock_code AS sc," +
    "  MAX(CASE WHEN market_date = ?1 THEN close_price END) AS c0," +
    "  MAX(CASE WHEN market_date = ?1 THEN market_cap END) AS mc," +
    "  MAX(CASE WHEN market_date = ?2 THEN close_price END) AS cs," +
    "  MAX(CASE WHEN market_date = ?3 THEN close_price END) AS c3," +
    "  MAX(CASE WHEN market_date = ?4 THEN close_price END) AS c6," +
    "  MAX(CASE WHEN market_date = ?5 THEN close_price END) AS cf" +
    " FROM market_raw_daily WHERE market_date IN (?1,?2,?3,?4,?5)" +
    " GROUP BY stock_code HAVING c0 >= ?6 AND c0 > 0" +
    "), a AS MATERIALIZED (" +
    " SELECT stock_code AS sc, SUM(trading_value) / " + SCREEN_AVG_DAYS + ".0 AS atv FROM market_raw_daily WHERE market_date BETWEEN ?7 AND ?1 GROUP BY stock_code" +
    "), n AS MATERIALIZED (" +
    " SELECT m.*, a.atv AS atv, cs * 1.0 / NULLIF(c3, 0) - 1 AS q3, cs * 1.0 / NULLIF(c6, 0) - 1 AS m6" +
    " FROM m JOIN a ON a.sc = m.sc LEFT JOIN market_universe_status u ON u.stock_code = m.sc" +
    " WHERE a.atv >= ?8 AND substr(m.sc, -1) = '0'" +
    "  AND COALESCE(u.corp_name, '') NOT LIKE '%스팩%' AND COALESCE(u.corp_name, '') NOT LIKE '%기업인수목적%' AND COALESCE(u.corp_name, '') NOT LIKE '%리츠%'" +
    "), n2 AS MATERIALIZED (" +
    " SELECT n.*, " + (hasMc ? "PERCENT_RANK() OVER (ORDER BY mc)" : "1.0") + " AS pmc, PERCENT_RANK() OVER (ORDER BY atv) AS ptv FROM n" +
    "), nf AS MATERIALIZED (" +
    " SELECT * FROM n2 WHERE pmc >= ?9 AND ptv >= ?10 AND (?11 IS NULL OR mc >= ?11)" +
    "), p AS (" +
    " SELECT nf.*," +
    "  CASE WHEN q3 IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY (q3 IS NOT NULL) ORDER BY q3) END AS p3," +
    "  CASE WHEN m6 IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY (m6 IS NOT NULL) ORDER BY m6) END AS p6 FROM nf" +
    ") SELECT sc, c0, cf, CASE WHEN ?12 > 0 AND ?13 > 0 THEN (CASE WHEN p3 IS NOT NULL AND p6 IS NOT NULL THEN ?12 * p3 + ?13 * p6 END) WHEN ?12 > 0 THEN p3 ELSE p6 END AS rw FROM p";
  const binds = [d0, dS, d3, d6, dF, minPrice, dA, MARKET_MIN_TRADING_VALUE, pcm, pct];
  if (hasMc) binds.push(MARKET_MIN_MCAP);
  else binds.push(null);
  binds.push(nw3, nw6);
  const { results } = await env.DB.prepare(sql).bind(...binds).all();
  const rows = [];
  for (const r of results) {
    if (r.rw == null) continue;
    rows.push([r.sc, r.rw, (r.cf > 0 && r.c0 > 0) ? r.cf / r.c0 - 1 : null]);
  }
  return { base: d0, fwdDate: dF, n: rows.length, hasMc, rows, kospiFwd: (k0 > 0 && kF > 0) ? kF / k0 - 1 : null, note: hasMc ? null : d0 + ": 시가총액 이력 없음" };
}

async function computeScreenLists(env, opts) {
  opts = opts || {};
  await ensureScreenTables(env.DB);
  await ensureMarketQueueColumns(env.DB); // market_cap 컬럼 보장

  const clamped = [];
  let minMcap = Number(opts.minMcapEok);
  if (!Number.isFinite(minMcap) || minMcap <= 0) minMcap = SCREEN_DEFAULT_MCAP_EOK;
  let minTv = Number(opts.minTvEok);
  if (!Number.isFinite(minTv) || minTv <= 0) minTv = SCREEN_DEFAULT_TV_EOK;
  let minPrice = opts.minPrice == null || opts.minPrice === "" ? NaN : Number(opts.minPrice);
  if (!Number.isFinite(minPrice) || minPrice < 0) minPrice = SCREEN_DEFAULT_MIN_PRICE;
  const pctOf = (v, d) => { const x = v == null || v === "" ? NaN : Number(v); return Number.isFinite(x) && x >= 0 && x <= 90 ? x : d; };
  const pcMcap = pctOf(opts.pcMcap, SCREEN_DEFAULT_PCT_MCAP);
  const pcTv = pctOf(opts.pcTv, SCREEN_DEFAULT_PCT_TV);
  const floorMcapEok = MARKET_MIN_MCAP / 1e8;
  const floorTvEok = MARKET_MIN_TRADING_VALUE / 1e8;
  if (minMcap < floorMcapEok) { clamped.push("시가총액 하한을 " + floorMcapEok + "억원으로 올렸습니다(저장된 종목은 그 이상만 있음)."); minMcap = floorMcapEok; }
  if (minTv < floorTvEok) { clamped.push("거래대금 하한을 " + floorTvEok + "억원으로 올렸습니다(저장된 종목은 그 이상만 있음)."); minTv = floorTvEok; }

  let w3 = Number(opts.w3), w6 = Number(opts.w6);
  if (![w3, w6].every((x) => Number.isFinite(x) && x >= 0) || w3 + w6 <= 0) {
    ({ w3, w6 } = SCREEN_DEFAULT_WEIGHTS);
    if (opts.w3 != null || opts.w6 != null) clamped.push("가중치가 올바르지 않아 기본값(2~3개월 0.5 / 2~6개월 0.5)을 썼습니다.");
  }
  const nw = { w3: w3 / (w3 + w6), w6: w6 / (w3 + w6) };

  // 1) 거래일 달력(코스피 일별 지수)
  const { results: calRows } = await env.DB
    .prepare("SELECT market_date FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 145")
    .bind(KOSPI_NAME).all();
  const cal = calRows.map((r) => r.market_date);
  if (cal.length < 135) {
    throw new Error("코스피 지수 일별 데이터가 " + cal.length + "개뿐입니다(최소 135개 필요). '지수시세정보' 활용신청이 승인됐는지 확인하고 '② 지금 계산'을 다시 눌러주세요(처음엔 코스피를 자동으로 받아옵니다).");
  }

  // 2) 기준일: 시가총액이 저장된 행이 충분히 들어와 있는 가장 최근 거래일.
  //    (시가총액은 새 방식으로 저장된 행에만 있다. 처음 배포 직후에는 최신 거래일만 시총이 있고 이전 날짜는 거의 비어 있으므로
  //     "날짜별 전체 행 수"가 아니라 "시총이 있는 행 수"를 기준으로 고른다. 하루치 추가가 아직 진행 중인 날은 제외된다.)
  const stat = [];
  for (let i = 0; i < 5; i++) {
    const r = await env.DB.prepare("SELECT COUNT(*) AS c, SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) AS m FROM market_raw_daily WHERE market_date = ?").bind(cal[i]).first();
    stat.push({ c: r ? r.c : 0, m: r && r.m ? r.m : 0 });
  }
  const maxM = Math.max(...stat.map((x) => x.m));
  let i0 = -1;
  if (maxM > 0) {
    i0 = stat.findIndex((x) => x.m >= maxM * 0.8);
  } else {
    i0 = stat.findIndex((x) => x.c > 0); // 시총이 아직 어디에도 없음 → 결과는 비지만 원인을 화면에 안내한다
  }
  if (i0 < 0) throw new Error("최근 거래일의 종목 시세가 아직 없습니다. '유니버스 갱신'과 백필 진행 상황을 확인해주세요.");
  const pts = { d0: cal[i0], d20: cal[i0 + 20], d21: cal[i0 + 21], d60: cal[i0 + 60], d63: cal[i0 + 63], d120: cal[i0 + 120], d126: cal[i0 + 126] };

  // 3) 코스피 값(같은 날짜 지점)
  const dateList = Object.values(pts);
  const { results: kr } = await env.DB
    .prepare("SELECT market_date, close_price FROM market_index_daily WHERE idx_name = ? AND market_date IN (" + dateList.map(() => "?").join(",") + ")")
    .bind(KOSPI_NAME, ...dateList).all();
  const kAt = new Map(kr.map((r) => [r.market_date, r.close_price]));
  const k0 = kAt.get(pts.d0), k21 = kAt.get(pts.d21), k63 = kAt.get(pts.d63), k126 = kAt.get(pts.d126);
  if (!(k0 > 0 && k21 > 0 && k63 > 0 && k126 > 0)) throw new Error("코스피 기준 지점 값을 찾지 못했습니다.");
  const kospi = {
    close: k0,
    r20: k0 / kAt.get(pts.d20) - 1, r60: k0 / kAt.get(pts.d60) - 1, r120: k0 / kAt.get(pts.d120) - 1,
    r21: k0 / k21 - 1, r63: k0 / k63 - 1, r126: k0 / k126 - 1,
  };

  // 4) 종목 집계: 필요한 날짜 행만 읽어(기본키 앞부분 market_date로 바로 찾음) 한 번에 피벗·수익률·순위 상위 50을 만든다.
  //  - 필터: 기준일 시가총액 ≥ 하한, 기준일 종가 ≥ 최소주가, 최근 20거래일 평균 거래대금 ≥ 하한(하루치 아님),
  //          보통주만(종목코드 끝자리 0 — 우선주는 5·7·9·K 등), 스팩·리츠 제외.
  //  - 평균 거래대금 = 20거래일 거래대금 합 ÷ 20. 저장되지 않은 날(유동성 하한 미달로 저장 안 됨)은 0으로 보므로 보수적으로 계산된다.
  //  - 복합 점수 rw: (2~3개월 수익률 퍼센타일, 2~6개월 수익률 퍼센타일)의 가중평균. 최근 1개월(21거래일)을 뺀 수익률이라
  //    1개월 단기반전(Jegadeesh 1990)의 영향을 피한다. 원수익률이 아니라 순위(퍼센타일)로 합쳐 극단값에 덜 민감하다.
  const sql =
    "WITH m AS MATERIALIZED (" +
    " SELECT stock_code AS sc," +
    "  MAX(CASE WHEN market_date = ?1 THEN close_price END) AS c0," +
    "  MAX(CASE WHEN market_date = ?1 THEN market_cap END) AS mc," +
    "  MAX(CASE WHEN market_date = ?2 THEN close_price END) AS c20," +
    "  MAX(CASE WHEN market_date = ?3 THEN close_price END) AS c21," +
    "  MAX(CASE WHEN market_date = ?4 THEN close_price END) AS c60," +
    "  MAX(CASE WHEN market_date = ?5 THEN close_price END) AS c63," +
    "  MAX(CASE WHEN market_date = ?6 THEN close_price END) AS c120," +
    "  MAX(CASE WHEN market_date = ?7 THEN close_price END) AS c126" +
    " FROM market_raw_daily WHERE market_date IN (?1,?2,?3,?4,?5,?6,?7)" +
    " GROUP BY stock_code HAVING c0 >= ?15 AND c0 > 0 AND mc >= ?9" +
    "), a AS MATERIALIZED (" +
    " SELECT stock_code AS sc, SUM(trading_value) / " + SCREEN_AVG_DAYS + ".0 AS atv, MAX(CASE WHEN market_date = ?1 THEN trading_value END) AS tv1" +
    " FROM market_raw_daily WHERE market_date BETWEEN ?16 AND ?1 GROUP BY stock_code" +
    "), n AS MATERIALIZED (" +
    " SELECT m.*, a.atv AS atv, a.tv1 AS tv1, u.corp_name AS nm," +
    "  c0 * 1.0 / NULLIF(c20, 0) - 1 AS r20," +
    "  c0 * 1.0 / NULLIF(c60, 0) - 1 AS r60," +
    "  c0 * 1.0 / NULLIF(c120, 0) - 1 AS r120," +
    "  c0 * 1.0 / NULLIF(c21, 0) - 1 AS r21," +
    "  c21 * 1.0 / NULLIF(c63, 0) - 1 AS q3," +
    "  c21 * 1.0 / NULLIF(c126, 0) - 1 AS m16," +
    "  CASE WHEN c21 > 0 THEN (c0 * 1.0 / c21) / ?14 END AS rs1," +
    "  CASE WHEN c63 > 0 THEN (c0 * 1.0 / c63) / ?12 END AS rs3," +
    "  CASE WHEN c126 > 0 THEN (c0 * 1.0 / c126) / ?13 END AS rs6" +
    " FROM m JOIN a ON a.sc = m.sc LEFT JOIN market_universe_status u ON u.stock_code = m.sc" +
    " WHERE a.atv >= ?8 AND substr(m.sc, -1) = '0'" +
    "  AND COALESCE(u.corp_name, '') NOT LIKE '%스팩%' AND COALESCE(u.corp_name, '') NOT LIKE '%기업인수목적%' AND COALESCE(u.corp_name, '') NOT LIKE '%리츠%'" +
    "), n2 AS MATERIALIZED (" +
    " SELECT n.*, PERCENT_RANK() OVER (ORDER BY mc) AS pmc, PERCENT_RANK() OVER (ORDER BY atv) AS ptv FROM n" +
    "), nf AS MATERIALIZED (" +
    " SELECT * FROM n2 WHERE pmc >= ?17 AND ptv >= ?18" +
    "), p AS (" +
    " SELECT nf.*, PERCENT_RANK() OVER (ORDER BY r21) AS p21," +
    "  CASE WHEN q3 IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY (q3 IS NOT NULL) ORDER BY q3) END AS p3," +
    "  CASE WHEN m16 IS NOT NULL THEN PERCENT_RANK() OVER (PARTITION BY (m16 IS NOT NULL) ORDER BY m16) END AS p6," +
    "  COUNT(*) OVER () AS pass_n, MIN(mc) OVER () AS cut_mc, MIN(atv) OVER () AS cut_tv FROM nf" +
    "), q AS (" +
    " SELECT p.*, CASE WHEN p3 IS NOT NULL AND p6 IS NOT NULL THEN ?10 * p3 + ?11 * p6 END AS rw FROM p" +
    "), k AS (" +
    // D1은 UNION ALL(복합 SELECT) 항 수 제한이 작아 윈도 함수로 목록별 순위를 한 번에 매긴다.
    " SELECT q.*, " + SCREEN_LIST_KEYS.map((key) => "CASE WHEN " + key + " IS NOT NULL THEN ROW_NUMBER() OVER (ORDER BY " + key + " DESC, sc) END AS rk_" + key).join(", ") + " FROM q" +
    ") SELECT * FROM k WHERE " + SCREEN_LIST_KEYS.map((key) => "rk_" + key + " <= " + SCREEN_TOP_N).join(" OR ");
  const { results: agg } = await env.DB.prepare(sql)
    .bind(pts.d0, pts.d20, pts.d21, pts.d60, pts.d63, pts.d120, pts.d126, minTv * 1e8, minMcap * 1e8, nw.w3, nw.w6, k0 / k63, k0 / k126, k0 / k21, minPrice, cal[i0 + SCREEN_AVG_DAYS - 1], pcMcap / 100, pcTv / 100).all();
  const passedN = agg.length ? agg[0].pass_n : 0;
  const cutMcEok = agg.length ? agg[0].cut_mc / 1e8 : null; // 통과 종목 중 최소값 = 실제 컷 금액
  const cutTvEok = agg.length ? agg[0].cut_tv / 1e8 : null;

  const lists = {};
  for (const k of SCREEN_LIST_KEYS) lists[k] = [];
  for (const key of SCREEN_LIST_KEYS) {
    lists[key] = agg
      .filter((r) => r["rk_" + key] != null && r["rk_" + key] <= SCREEN_TOP_N)
      .sort((a, b) => a["rk_" + key] - b["rk_" + key])
      .map((r) => ({
        stock_code: r.sc, corp_name: r.nm || r.sc, c0: r.c0,
        tvEok: r.atv / 1e8, tv1Eok: r.tv1 != null ? r.tv1 / 1e8 : null, mcapEok: r.mc / 1e8,
        r20: r.r20, r60: r.r60, r120: r.r120, r21: r.r21, hot: r.p21 != null && r.p21 >= SCREEN_HOT_PCT ? 1 : 0, rw: r.rw, m16: r.m16, rs1: r.rs1, rs3: r.rs3, rs6: r.rs6,
      }));
  }

  // 5) 참고 집계: 기준일 행 수, 시가총액 미수집 수, 조건 통과 수(기준일 행만 읽음 ≈ 1,300행)
  const meta0 = await env.DB.prepare(
    "SELECT COUNT(*) AS total, SUM(CASE WHEN market_cap IS NULL THEN 1 ELSE 0 END) AS nomc FROM market_raw_daily WHERE market_date = ?1"
  ).bind(pts.d0).first();

  const bfRow = await env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled = 1").first();
  const meta = {
    backfilled: bfRow ? bfRow.c : null,
    asof: pts.d0, computed_at: new Date().toISOString(), points: pts, kospi,
    minMcapEok: minMcap, minTvEok: minTv, pcMcap, pcTv, hotPct: SCREEN_HOT_PCT, cutMcEok, cutTvEok, minPrice, weights: nw, clamped, avgDays: SCREEN_AVG_DAYS,
    asof_rows: meta0 ? meta0.total : null, no_mcap: meta0 ? meta0.nomc : null, passed_floor: passedN,
  };
  return { meta, lists };
}

async function saveScreenLists(env, result) {
  const date = result.meta.asof;
  await env.DB.prepare("DELETE FROM screen_lists WHERE snap_date = ?").bind(date).run();
  const rows = [[date, "meta", JSON.stringify(result.meta)]];
  for (const k of SCREEN_LIST_KEYS) rows.push([date, k, JSON.stringify(result.lists[k])]);
  await multiRowInsert(env.DB, "INSERT INTO screen_lists (snap_date, list_key, payload)", 3, rows, (r) => r);
}

async function runWeeklyScreening(env) {
  try { await refreshKospi(env); } catch (e) { console.error("코스피 갱신 실패:", (e && e.message) || e); }
  const result = await computeScreenLists(env, {}); // 기본값: 시총·거래대금 하위 20% 제외, 최소주가 1,000원, 복합 가중 0.5/0.5
  await saveScreenLists(env, result);
  // 영업이익 조회 대기열: 캐시에 없는 종목을 등록해 두면 짝수 분 cron이 12개씩 조금씩 채운다(화면을 열 필요 없음).
  try {
    const codes = [...new Set(SCREEN_LIST_KEYS.flatMap((k) => result.lists[k].map((r) => r.stock_code)))];
    const pending = await opPendingCodes(env, codes);
    if (pending.length) await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('opfill', ?)").bind(JSON.stringify(pending)).run();
  } catch (e) {
    console.error("영업이익 대기열 등록 실패:", (e && e.message) || e);
  }
  return result.meta;
}

// 저장된 주간 스냅샷 + 영업이익(캐시 → 없으면 DART 재무로 대체)
async function loadScreenSnapshot(env, date) {
  await ensureScreenTables(env.DB);
  const { results: dateRows } = await env.DB.prepare("SELECT DISTINCT snap_date FROM screen_lists ORDER BY snap_date DESC LIMIT 30").all();
  const dates = dateRows.map((r) => r.snap_date);
  if (!dates.length) return null;
  const useDate = date && dates.includes(date) ? date : dates[0];
  const { results } = await env.DB.prepare("SELECT list_key, payload FROM screen_lists WHERE snap_date = ?").bind(useDate).all();
  const lists = {};
  let meta = {};
  for (const r of results) {
    if (r.list_key === "meta") meta = JSON.parse(r.payload);
    else lists[r.list_key] = JSON.parse(r.payload);
  }
  for (const k of SCREEN_LIST_KEYS) if (!lists[k]) lists[k] = [];
  await attachOpIncome(env, lists);
  return { date: useDate, dates, meta, lists };
}

// ------------------------------------------------------------
// 영업이익: data.go.kr 종목정보(KRX상장종목정보) → 법인등록번호(crno) → 기업재무정보(요약재무제표) → 영업이익
//  - 종목당 호출 2~3회, 결과는 op_income_cache에 저장해 120일 재사용(영업이익은 연 1회 갱신).
//  - Worker 호출 1번에는 무료 플랜 기준 50건 한도가 있어 한 번에 12종목까지만 처리하고, 나머지는 "남은 개수"로 알려
//    화면(또는 짝수 분 cron)이 이어서 호출한다.
//  - data.go.kr에서 "금융위원회_KRX상장종목정보"와 "금융위원회_기업 재무정보" 각각 활용신청이 필요하다.
//  - 서비스 경로가 버전에 따라 다를 수 있어 후보 주소를 순서대로 시도하고, 성공한 주소를 기억한다.
// ------------------------------------------------------------
const KRX_LISTED_URLS = [
  "https://apis.data.go.kr/1160100/GetKrxListedInfoService_V2/getItemInfo_V2", // 확인된 실제 주소(상장종목정보조회 / 종목조회)
  "https://apis.data.go.kr/1160100/service/GetKrxListedInfoService/getItemInfo",
];
// 손익계산서 조회(getIncoStat_V2): 계정과목별 행(acitId, 당기금액 crtmAcitAmt 등)이 온다. 영업이익은 acitId "dart_OperatingIncomeLoss".
// (요약재무제표 getSummFinaStat_V2는 보조 후보로만 둔다.)
const FINA_STAT_URLS = [
  "https://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getIncoStat_V2",
  "https://apis.data.go.kr/1160100/GetFinaStatInfoService_V2/getIncoStat_V2",
  "https://apis.data.go.kr/1160100/service/GetFinaStatInfoService_V2/getSummFinaStat_V2",
];
const OPFILL_BATCH = 7; // 종목당 호출 최대 4회(종목정보 1 + 재무 최대 3) × 7 = 28, 나머지 D1 쿼리·콜드스타트 준비까지 합쳐도 50 미만
const OP_TTL_MS = 120 * 24 * 3600 * 1000;      // 값이 있는 캐시 유효기간
const OP_NULL_TTL_MS = 14 * 24 * 3600 * 1000;  // 값이 없던(조회 실패/미제공) 종목 재시도 간격
const goKrState = { listed: { idx: 0 }, fina: { idx: 0 } };

function parseGoKrXml(text) {
  const g = (t) => { const m = text.match(new RegExp("<" + t + ">([^<]*)</" + t + ">")); return m ? m[1] : null; };
  const items = [];
  const re = /<item>([\s\S]*?)<\/item>/g;
  let m;
  while ((m = re.exec(text)) !== null) {
    const o = {};
    const fr = /<(\w+)>([^<]*)<\/\1>/g;
    let f;
    while ((f = fr.exec(m[1])) !== null) o[f[1]] = f[2];
    items.push(o);
  }
  return { code: g("resultCode"), msg: g("resultMsg"), reason: g("returnReasonCode"), authMsg: g("returnAuthMsg") || g("errMsg"), items };
}

async function goKrFetch(urls, state, params, apiKey, label) {
  let lastErr = null;
  let authErr = null;
  for (let k = 0; k < urls.length; k++) {
    const i = (state.idx + k) % urls.length;
    const url = new URL(urls[i]);
    url.searchParams.set("serviceKey", apiKey);
    for (const [key, val] of Object.entries(params)) url.searchParams.set(key, String(val));
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 15000);
    let text = "", status = 0;
    try {
      const resp = await fetch(url.toString(), { signal: controller.signal });
      status = resp.status;
      text = await resp.text();
    } catch (e) {
      lastErr = label + " 호출 실패(" + ((e && e.message) || e) + ")";
      continue;
    } finally {
      clearTimeout(timer);
    }
    const p = parseGoKrXml(text);
    if (p.code === "00" || p.code === "0" || p.code === "03") { state.idx = i; return p; }
    lastErr = label + " 응답 오류(" + (p.code || p.reason || status) + " " + (p.msg || p.authMsg || text.slice(0, 60)) + ")";
    // 잘못된 서비스 주소도 "인증키/권한 오류"처럼 응답될 수 있어, 모든 후보 주소를 다 시도한 뒤에만 권한 오류로 확정한다.
    if (p.reason === "20" || /ACCESS DENIED/i.test(p.authMsg || "") || p.reason === "30" || p.reason === "31") authErr = (p.authMsg || p.reason);
  }
  if (authErr) {
    const err = new Error(label + " 사용 권한 오류: 공공데이터포털에서 해당 API '활용신청'이 승인됐는지, 인증키가 맞는지 확인해주세요. (" + authErr + ")");
    err.fatal = true;
    throw err;
  }
  const err = new Error((lastErr || (label + " 조회 실패")) + " — 해당 API 활용신청 여부와 서비스 주소를 확인해주세요.");
  throw err;
}

async function fetchOpIncomeFor(stockCode, cachedCrno, apiKey) {
  let crno = cachedCrno || null;
  if (!crno) {
    const p = await goKrFetch(KRX_LISTED_URLS, goKrState.listed, { numOfRows: 10, pageNo: 1, likeSrtnCd: stockCode, resultType: "xml" }, apiKey, "KRX상장종목정보");
    const norm = (v) => String(v || "").replace(/^A/, "");
    const hit = p.items.find((it) => norm(it.srtnCd) === stockCode && it.crno) || p.items.find((it) => it.crno);
    crno = hit ? hit.crno : null;
  }
  if (!crno) return { crno: null, year: null, op: null };
  const thisYear = new Date().getUTCFullYear();
  // 응답 행에서 영업이익(원)을 뽑는다. 손익계산서 형식: acitId=dart_OperatingIncomeLoss, 당기금액=crtmAcitAmt, 연결/별도는 fnclDcdNm.
  // 요약재무제표 형식(enpBzopPft)도 함께 인식한다. 연결재무제표가 있으면 연결을 우선한다.
  const pickOp = (items) => {
    const rows = [];
    for (const it of items) {
      let v = NaN;
      if (/OperatingIncome/i.test(it.acitId || "") || /^영업이익/.test(it.acitNm || "")) v = Number(it.crtmAcitAmt);
      else { const key = Object.keys(it).find((k) => /bzop/i.test(k)); if (key && it[key] !== "") v = Number(it[key]); }
      if (Number.isFinite(v)) rows.push({ v, cons: /연결/.test(it.fnclDcdNm || "") });
    }
    if (!rows.length) return null;
    return (rows.find((r) => r.cons) || rows[0]).v;
  };
  const attempts = [
    { year: thisYear - 1, params: { bizYear: thisYear - 1 } },
    { year: thisYear - 1, params: { basDt: String(thisYear - 1) + "1231" } },
    { year: thisYear - 2, params: { bizYear: thisYear - 2 } },
  ];
  for (const at of attempts) {
    const p = await goKrFetch(FINA_STAT_URLS, goKrState.fina, { crno, ...at.params, numOfRows: 100, pageNo: 1, resultType: "xml" }, apiKey, "기업재무정보");
    const op = pickOp(p.items);
    if (op != null) return { crno, year: at.year, op };
  }
  return { crno, year: null, op: null };
}

async function readOpCache(env, codes) {
  const out = new Map();
  for (const part of chunkArr(codes, 90)) {
    const { results } = await env.DB.prepare("SELECT stock_code, crno, biz_year, op_profit, fetched_at FROM op_income_cache WHERE stock_code IN (" + part.map(() => "?").join(",") + ")").bind(...part).all();
    for (const r of results) out.set(r.stock_code, r);
  }
  return out;
}

function opCacheFresh(r, nowMs) {
  if (!r) return false;
  const age = nowMs - Date.parse(r.fetched_at);
  return r.op_profit == null ? age < OP_NULL_TTL_MS : age < OP_TTL_MS;
}

async function opPendingCodes(env, codes) {
  await ensureScreenTables(env.DB);
  const cache = await readOpCache(env, codes);
  const now = Date.now();
  return codes.filter((c) => !opCacheFresh(cache.get(c), now));
}

// codes 중 캐시가 없거나 오래된 종목을 최대 limit개 조회해 저장한다.
async function fillOpIncome(env, codes, limit) {
  if (!env.DATA_GO_KR_KEY) throw new Error("DATA_GO_KR_KEY 미설정");
  await ensureScreenTables(env.DB);
  const cache = await readOpCache(env, codes);
  const now = Date.now();
  const pending = codes.filter((c) => !opCacheFresh(cache.get(c), now));
  const batch = pending.slice(0, limit || OPFILL_BATCH);
  const upserts = [];
  const values = {};
  const errors = [];
  const nowIso = new Date().toISOString();
  for (const code of batch) {
    try {
      const prev = cache.get(code);
      const r = await fetchOpIncomeFor(code, prev ? prev.crno : null, env.DATA_GO_KR_KEY);
      upserts.push([code, r.crno, r.year, r.op, nowIso]);
      values[code] = r.op == null ? null : { v: r.op / 1e8, year: r.year, src: "datago" };
    } catch (e) {
      if (e && e.fatal) throw e;
      errors.push(code + ": " + ((e && e.message) || e));
    }
  }
  await multiRowInsert(
    env.DB, "INSERT INTO op_income_cache (stock_code, crno, biz_year, op_profit, fetched_at)", 5, upserts, (r) => r,
    "ON CONFLICT(stock_code) DO UPDATE SET crno = excluded.crno, biz_year = excluded.biz_year, op_profit = excluded.op_profit, fetched_at = excluded.fetched_at"
  );
  return { processed: upserts.length, remaining: pending.length - upserts.length, values, errors: errors.slice(0, 3) };
}

// 짝수 분 cron(재무 큐가 빈 분)에서 호출: 주간 스크리닝이 등록해 둔 영업이익 조회 대기열을 12개씩 처리한다.
async function processOpFillTick(env) {
  if (!env.DATA_GO_KR_KEY) return;
  await ensureMarketQueueColumns(env.DB);
  const row = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'opfill'").first();
  if (!row) return;
  let list;
  try { list = JSON.parse(row.v); } catch (e) { list = []; }
  const batch = list.slice(0, OPFILL_BATCH);
  const rest = list.slice(OPFILL_BATCH);
  try {
    if (batch.length) await fillOpIncome(env, batch, OPFILL_BATCH);
  } catch (e) {
    console.error("영업이익 조회 중단:", (e && e.message) || e);
    await env.DB.prepare("DELETE FROM market_job WHERE k = 'opfill'").run(); // 권한 문제 등: 대기열을 버리고 화면에서 안내
    return;
  }
  if (rest.length) await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('opfill', ?)").bind(JSON.stringify(rest)).run();
  else await env.DB.prepare("DELETE FROM market_job WHERE k = 'opfill'").run();
}

// 각 행에 op = { v(억원), year, src } 를 붙인다. data.go.kr 캐시가 우선, 없으면 DART로 저장된 최근 사업연도 영업이익으로 대체.
async function attachOpIncome(env, lists) {
  const codes = [...new Set(SCREEN_LIST_KEYS.flatMap((k) => (lists[k] || []).map((r) => r.stock_code)))];
  if (!codes.length) return;
  const cache = await readOpCache(env, codes);
  const dart = new Map();
  const need = codes.filter((c) => { const r = cache.get(c); return !r || r.op_profit == null; });
  if (need.length) {
    try {
      const corpOf = new Map();
      for (const part of chunkArr(need, 90)) {
        const { results } = await env.DB.prepare("SELECT corp_code, stock_code FROM corp_master WHERE stock_code IN (" + part.map(() => "?").join(",") + ")").bind(...part).all();
        for (const r of results) corpOf.set(r.stock_code, r.corp_code);
      }
      const corps = [...new Set(corpOf.values())];
      const latest = new Map();
      for (const part of chunkArr(corps, 90)) {
        const { results } = await env.DB.prepare(
          "SELECT corp_code, bsns_year, operating_income FROM financial_raw WHERE corp_code IN (" + part.map(() => "?").join(",") + ") AND reprt_code = '11011' AND fs_div IS NOT NULL AND operating_income IS NOT NULL ORDER BY corp_code, bsns_year DESC"
        ).bind(...part).all();
        for (const r of results) if (!latest.has(r.corp_code)) latest.set(r.corp_code, r);
      }
      for (const [sc, cc] of corpOf) { const r = latest.get(cc); if (r) dart.set(sc, { v: r.operating_income / 1e8, year: Number(r.bsns_year), src: "dart" }); }
    } catch (e) {
      console.error("DART 영업이익 대체 조회 실패:", (e && e.message) || e);
    }
  }
  for (const k of SCREEN_LIST_KEYS) {
    for (const row of lists[k] || []) {
      const c = cache.get(row.stock_code);
      if (c && c.op_profit != null) row.op = { v: c.op_profit / 1e8, year: c.biz_year, src: "datago" };
      else row.op = dart.get(row.stock_code) || null;
    }
  }
}

async function handleScheduled(event, env, ctx) {
    const minute = new Date(event.scheduledTime).getUTCMinutes();
    const hour = new Date(event.scheduledTime).getUTCHours();
    const dow = new Date(event.scheduledTime).getUTCDay(); // 0=일 ... 5=금, 6=토

    // 매주 토요일 새벽 1시(한국) = 금요일 16:00(UTC): 기본 조건(시총·20일 평균 거래대금 하위 20% 제외·주가 1,000원)으로 스크리닝하고 저장한다.
    // ※ 공공데이터포털 시세는 "기준일 다음 영업일 13시 이후"에 올라오므로, 이 시각의 기준일은 보통 금요일이 아니라 목요일 종가다.
    if (dow === WEEKLY_SCREEN_UTC_DOW && hour === WEEKLY_SCREEN_UTC_HOUR && minute === 0) {
      try {
        await runWeeklyScreening(env);
      } catch (e) {
        console.error("주간 스크리닝 자동 저장 실패:", (e && e.message) || e);
      }
      return;
    }

    // 하루 한 번(UTC 9시 1분 = 한국시간 18:01) 최신 거래일 시세 일괄 추가를 "시작"한다(실제 처리는 이후 홀수 분마다 한 쪽씩).
    if (hour === 9 && minute === 1) {
      try {
        await runUniverseRefresh(env, false);
      } catch (e) {
        console.error("일일 자동 유니버스 갱신 실패:", (e && e.message) || e);
      }
      // 코스피 지수 증분 갱신(없는 날짜만, 보통 쓰기 1행). 실패해도 다른 작업에 영향 없도록 따로 감싼다.
      try {
        await refreshKospi(env);
      } catch (e) {
        console.error("코스피 지수 자동 갱신 실패:", (e && e.message) || e);
      }
      return; // 한 번의 호출에 일을 몰지 않는다(subrequest 50건 한도)
    }

    if (minute % 2 === 0) {
      // 짝수 분: 재무데이터(fetch_queue) 처리
      const { results } = await env.DB
        .prepare("SELECT corp_code, corp_name, stock_code, bsns_year, reprt_code FROM fetch_queue ORDER BY corp_code, period_order LIMIT ?")
        .bind(CRON_BATCH_SIZE)
        .all();

      if (results.length === 0) {
        // 재무 큐가 비어 있는 짝수 분에는 data.go.kr 영업이익 조회 대기열을 조금씩 처리한다.
        try { await processOpFillTick(env); } catch (e) { console.error("영업이익 조회 틱 실패:", (e && e.message) || e); }
        return;
      }
      for (const item of results) {
        const periodMeta = REPRT_CODES.find((r) => r.code === item.reprt_code);
        if (periodMeta) {
          try {
            const row = await fetchPeriodRow(item.corp_code, item.stock_code, { year: Number(item.bsns_year), ...periodMeta }, env.DART_PROXY_URL, env.DB);
            await saveRowsToDb(env.DB, [row]);
          } catch (e) {
            // 이 기간은 실패했지만 큐에서는 제거하고 다음 기간으로 넘어간다 (한 기간이 계속 실패해서 큐 전체가
            // 막히는 것을 방지). 재시도가 필요하면 "백그라운드로 받기"를 다시 눌러 재등록하면 된다.
            console.error("cron fetch-and-save 실패:", item.corp_code, item.bsns_year, item.reprt_code, e.message || e);
          }
        }
        await env.DB
          .prepare("DELETE FROM fetch_queue WHERE corp_code = ? AND bsns_year = ? AND reprt_code = ?")
          .bind(item.corp_code, item.bsns_year, item.reprt_code)
          .run();
      }
      return;
    }

    // 홀수 분: 모멘텀 유니버스(market_fetch_queue) 처리.
    // [읽기/쓰기 한도 보호 설계]
    //  - 큐에는 is_new 표시가 있다: 0 = 이미 이력이 있는 종목의 "오늘치 추가"(가벼움, 한도 제한 없음),
    //    1 = 처음 통과한 종목의 "전체 이력 백필"(무겁다 → 하루 MARKET_DAILY_WRITE_CAP개로 제한).
    //  - is_new에 인덱스를 걸어 두 종류를 각각 "앞에서 몇 개만" 바로 뽑는다. 예전처럼 큐 전체를 JOIN·정렬하면
    //    큐가 길 때 1분마다 수천 행을 읽어 하루 읽기 한도(500만 행)를 갉아먹었다.
    //  - 오늘 한도를 다 쓴 날은 "한도 소진" 표시를 메모리에 남겨 매 분 COUNT 조회를 반복하지 않는다.
    if (!env.DATA_GO_KR_KEY) return; // 키 미설정 시 조용히 건너뜀(재무데이터 큐는 짝수 분에 계속 처리됨)

    try {
      await ensureMarketQueueColumns(env.DB);
    } catch (e) {
      console.error("market 큐 준비 실패:", (e && e.message) || e);
      return;
    }

    // 진행 중인 "일일 시세 일괄 추가"가 있으면 이번 틱은 그것만 처리한다(한 호출에 일을 몰지 않기 위해).
    try {
      if (await processAppendJobTick(env)) return;
    } catch (e) {
      console.error("일괄 시세 추가 틱 실패:", (e && e.message) || e);
      return;
    }

    const today = todayStr();
    let capLeft = null; // 필요할 때만 계산(신규 종목을 처리할 때)
    const getCapLeft = async () => {
      if (capLeft != null) return capLeft;
      if (marketCapExhaustedDay === today) { capLeft = 0; return 0; }
      const row = await env.DB
        .prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled_at >= ?")
        .bind(todayUtcMidnightIso())
        .first();
      capLeft = Math.max(0, MARKET_DAILY_WRITE_CAP - (row?.c ?? 0));
      if (capLeft === 0) marketCapExhaustedDay = today;
      return capLeft;
    };

    // 1) 오늘치 추가 대상 먼저(가볍다)
    let { results: marketItems } = await env.DB
      .prepare("SELECT stock_code, corp_name, is_new FROM market_fetch_queue WHERE is_new = 0 LIMIT ?")
      .bind(MARKET_CRON_BATCH_SIZE)
      .all();
    // 2) 없으면 신규 백필 대상(한도가 남아있을 때만, 한 번에 적게)
    if (marketItems.length === 0) {
      const left = await getCapLeft();
      if (left <= 0) return;
      const r2 = await env.DB
        .prepare("SELECT stock_code, corp_name, is_new FROM market_fetch_queue WHERE is_new = 1 ORDER BY trv DESC LIMIT ?")
        .bind(Math.min(MARKET_NEW_BATCH_SIZE, left))
        .all();
      marketItems = r2.results;
    }
    if (marketItems.length === 0) return;

    const end = todayStr();
    const begin = addDaysStr(end, -MARKET_BACKFILL_CALENDAR_DAYS);
    const nowIso = new Date().toISOString();

    for (const item of marketItems) {
      let quotaExceeded = false;
      try {
        const st = await env.DB.prepare("SELECT backfilled FROM market_universe_status WHERE stock_code = ?").bind(item.stock_code).first();
        const alreadyBackfilled = !!(st && st.backfilled === 1);

        if (alreadyBackfilled) {
          // 이미 전체 이력이 있는 종목: 오늘(또는 가장 최근 영업일) 하루치만 가볍게 추가한다.
          let dayRow = null;
          for (let back = 0; back <= 3 && !dayRow; back++) {
            const ds = addDaysStr(end, -back);
            const { items } = await fetchKrxPage({ basDt: ds, likeSrtnCd: item.stock_code, numOfRows: 10, pageNo: 1 }, env.DATA_GO_KR_KEY);
            const exact = items.find((it) => it.srtnCd === item.stock_code);
            if (exact) dayRow = exact;
          }
          if (dayRow) {
            await env.DB.prepare(
              "INSERT OR REPLACE INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value, market_cap) VALUES (?, ?, ?, ?, ?, ?)"
            ).bind(dayRow.basDt, item.stock_code, dayRow.clpr, dayRow.trqu, dayRow.trPrc, dayRow.mrktTotAmt).run();
            await env.DB.prepare(
              "UPDATE market_universe_status SET passed = 1, avg_trading_value = ?, checked_at = ? WHERE stock_code = ?"
            ).bind(dayRow.trPrc, nowIso, item.stock_code).run();
          }
        } else {
          // 신규 통과 종목: 전체 이력을 백필한다(하루 백필 한도 안에서만).
          const left = await getCapLeft();
          if (left <= 0) {
            // 오늘 한도 소진 — 큐에서 지우지 않고 신규(is_new=1) 칸으로 옮겨 내일 처리한다.
            await env.DB.prepare("UPDATE market_fetch_queue SET is_new = 1 WHERE stock_code = ?").bind(item.stock_code).run();
            continue;
          }
          capLeft = left - 1;
          if (capLeft <= 0) marketCapExhaustedDay = today;
          const history = await fetchKrxStockHistory(item.stock_code, begin, end, env.DATA_GO_KR_KEY);
          if (history.length) {
            await multiRowInsert(
              env.DB,
              "INSERT OR REPLACE INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value, market_cap)",
              6,
              history,
              (p) => [p.date, item.stock_code, p.close, p.volume, p.tradingValue, p.marketCap]
            );
          }
          const latest = history.length ? history[history.length - 1] : null;
          await env.DB.prepare(
            `INSERT INTO market_universe_status (stock_code, corp_name, passed, avg_trading_value, days_fetched, checked_at, backfilled, backfilled_at)
             VALUES (?, ?, 1, ?, NULL, ?, 1, ?)
             ON CONFLICT(stock_code) DO UPDATE SET corp_name = excluded.corp_name, passed = 1, avg_trading_value = excluded.avg_trading_value, checked_at = excluded.checked_at, backfilled = 1, backfilled_at = excluded.backfilled_at`
          ).bind(item.stock_code, item.corp_name, latest ? latest.tradingValue : null, nowIso, nowIso).run();
        }
      } catch (e) {
        const msg = (e && e.message) || String(e);
        console.error("market 처리 실패:", item.stock_code, msg);
        // D1 하루 쓰기 한도 초과는 "이 종목만의 문제"가 아니라 "오늘은 더 이상 아무 것도 못 쓴다"는 뜻이다.
        // 이 경우 이번 종목의 DELETE를 건너뛰고(내일 다시 정상 처리되도록 큐에 남김) 이번 틱을 바로 끝낸다.
        if (/exceeded|row write limit|D1_ERROR/i.test(msg)) {
          quotaExceeded = true;
        }
      }
      if (quotaExceeded) break;
      try {
        await env.DB.prepare("DELETE FROM market_fetch_queue WHERE stock_code = ?").bind(item.stock_code).run();
      } catch (e) {
        console.error("market_fetch_queue 삭제 실패:", item.stock_code, e.message || e);
      }
    }
}
