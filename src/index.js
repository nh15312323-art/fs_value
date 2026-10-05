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
const FLOW_KEYS = ["revenue", "cogs", "operating_income", "net_income", "ocf", "capex", "fcf", "parent_net_income", "pretax_income", "interest_expense", "depreciation", "amortization", "dividends_paid", "buyback", "debt_repay", "debt_issue", "acquisitions", "intangible_capex"];

const ACCOUNT_ITEMS = [
  // 금융업(은행/보험/증권/지주 등)은 "매출액/매출원가" 대신 "영업수익/영업비용"으로 공시하는 경우가 많아 이름 폴백에 추가
  { key: "revenue", ids: ["ifrs-full_Revenue", "ifrs_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"], names: ["매출액", "수익(매출액)", "영업수익"] },
  { key: "cogs", ids: ["ifrs-full_CostOfSales", "ifrs_CostOfSales"], names: ["매출원가", "영업비용"] },
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
  { key: "inventory", ids: ["ifrs-full_Inventories", "ifrs_Inventories"], names: ["재고자산"] },
  { key: "payables", ids: ["ifrs-full_TradeAndOtherCurrentPayables", "ifrs_TradeAndOtherCurrentPayables"], names: ["매입채무"] },
  // ROIC의 투하자본(IC) 계산용으로 추가된 항목들
  { key: "short_term_trading_securities", ids: [], names: ["단기매매증권"] },
  { key: "fvpl_financial_assets", ids: ["ifrs-full_FinancialAssetsAtFairValueThroughProfitOrLoss"], names: ["당기손익-공정가치측정금융자산", "당기손익공정가치측정금융자산"] },
  { key: "fvoci_financial_assets", ids: ["ifrs-full_FinancialAssetsAtFairValueThroughOtherComprehensiveIncome"], names: ["기타포괄손익-공정가치측정금융자산", "기타포괄손익공정가치측정금융자산"] },
  { key: "investment_property", ids: ["ifrs-full_InvestmentProperty"], names: ["투자부동산"] },
  // 신규 IC(영업 관점: 순운전자본+고정자산) 계산용 항목
  { key: "other_receivables", ids: ["ifrs-full_OtherReceivables"], names: ["기타채권"] },
  { key: "short_term_loans", ids: [], names: ["단기대여금"] },
  { key: "other_payables", ids: ["ifrs-full_OtherPayables"], names: ["기타채무"] },
  { key: "short_term_borrowings", ids: ["ifrs-full_ShorttermBorrowings", "ifrs-full_ShortTermBorrowings"], names: ["단기차입금"] },
  { key: "current_portion_lt_debt", ids: ["ifrs-full_CurrentPortionOfLongtermBorrowings"], names: ["유동성장기부채", "유동성 장기차입금", "유동성장기차입금", "유동성사채"] },
  { key: "current_lease_liabilities", ids: ["ifrs-full_CurrentLeaseLiabilities"], names: ["유동리스부채"] },
  { key: "tangible_assets", ids: ["ifrs-full_PropertyPlantAndEquipment"], names: ["유형자산"] },
  { key: "intangible_assets", ids: ["ifrs-full_IntangibleAssetsOtherThanGoodwill", "ifrs-full_IntangibleAssetsAndGoodwill"], names: ["무형자산"] },
  { key: "right_of_use_assets", ids: ["ifrs-full_RightofuseAssets"], names: ["사용권자산"] },
  // 5단계 ROE 분석 준비용 항목 (계산은 나중에, 지금은 원천만 저장)
  { key: "parent_net_income", ids: ["ifrs-full_ProfitLossAttributableToOwnersOfParent"], names: ["지배기업의 소유주에게 귀속되는 당기순이익", "지배기업소유주지분순이익", "지배주주순이익"] },
  { key: "pretax_income", ids: ["ifrs-full_ProfitLossBeforeTax"], names: ["법인세비용차감전순이익", "법인세비용차감전순손익", "세전이익"] },
  { key: "interest_expense", ids: ["ifrs-full_FinanceCosts", "ifrs-full_InterestExpense"], names: ["이자비용", "금융비용"] },
  { key: "parent_equity", ids: ["ifrs-full_EquityAttributableToOwnersOfParent"], names: ["지배기업의 소유주에게 귀속되는 자본", "지배기업소유주지분", "지배주주지분"] },
  // ---- 재무분석 확장용(exact: 계정명을 정확히 일치할 때만 사용 — "유동자산"이 "비유동자산"에 잘못 걸리는 것 방지) ----
  { key: "current_assets", exact: true, ids: ["ifrs-full_CurrentAssets"], names: ["유동자산"] },
  { key: "current_liabilities", exact: true, ids: ["ifrs-full_CurrentLiabilities"], names: ["유동부채"] },
  { key: "long_term_borrowings", exact: true, ids: ["ifrs-full_LongtermBorrowings"], names: ["장기차입금"] },
  { key: "bonds", exact: true, ids: ["ifrs-full_BondsIssued"], names: ["사채"] },
  { key: "depreciation", exact: true, ids: ["ifrs-full_AdjustmentsForDepreciationExpense", "ifrs-full_DepreciationPropertyPlantAndEquipment"], names: ["감가상각비"] },
  { key: "amortization", exact: true, ids: ["ifrs-full_AdjustmentsForAmortisationExpense", "ifrs-full_AmortisationIntangibleAssetsOtherThanGoodwill"], names: ["무형자산상각비"] },
  { key: "dividends_paid", exact: true, ids: ["ifrs-full_DividendsPaidClassifiedAsFinancingActivities"], names: ["배당금의 지급", "배당금지급", "배당금 지급"] },
  { key: "buyback", exact: true, ids: ["ifrs-full_PaymentsToAcquireOrRedeemEntitysShares"], names: ["자기주식의 취득", "자기주식취득", "자기주식 취득"] },
  { key: "debt_repay", exact: true, ids: ["ifrs-full_RepaymentsOfBorrowings"], names: ["차입금의 상환", "장기차입금의 상환", "단기차입금의 상환", "사채의 상환"] },
  { key: "debt_issue", exact: true, ids: ["ifrs-full_ProceedsFromBorrowings"], names: ["차입금의 차입", "장기차입금의 차입", "단기차입금의 차입", "사채의 발행"] },
  { key: "acquisitions", exact: true, ids: ["ifrs-full_CashFlowsUsedInObtainingControlOfSubsidiariesOrOtherBusinessesClassifiedAsInvestingActivities"], names: ["종속기업의 취득", "사업결합으로 인한 현금유출", "관계기업투자의 취득", "종속기업에 대한 투자자산의 취득"] },
];

const DB_COLUMNS = [
  "corp_code", "bsns_year", "reprt_code", "period_label", "period_order", "fs_div",
  "revenue", "cogs", "operating_income", "net_income",
  "total_equity", "total_liabilities", "cash", "st_financial_assets",
  "ocf", "capex", "fcf", "receivables", "inventory", "payables",
  "total_shares", "treasury_shares", "dividend_per_share",
  "short_term_trading_securities", "fvpl_financial_assets", "fvoci_financial_assets", "investment_property",
  "other_receivables", "short_term_loans", "other_payables",
  "short_term_borrowings", "current_portion_lt_debt", "current_lease_liabilities",
  "tangible_assets", "intangible_assets", "right_of_use_assets",
  "parent_net_income", "pretax_income", "interest_expense", "parent_equity",
  "current_assets", "current_liabilities", "long_term_borrowings", "bonds",
  "depreciation", "amortization", "dividends_paid", "buyback", "debt_repay", "debt_issue", "acquisitions", "intangible_capex",
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
      max-width: 900px; margin: 0 auto; padding: 16px 12px 60px;
      background: var(--bg); color: var(--text);
    }
    h3 { margin: 4px 0 16px; }
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
    .an-tbl th.an-first { background: #f1f5f9; z-index: 2; }
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
  <h3>📊 DART 재무데이터</h3>

  <div class="row" style="margin-bottom:14px;">
    <button id="tabBtnFinancial" class="toggle-active" onclick="switchTab('financial')" style="flex:1;">재무분석</button>
    <button id="tabBtnMomentum" onclick="switchTab('momentum')" style="flex:1;">모멘텀 스크리닝</button>
  </div>

  <div id="tabFinancial">

  <div class="card">
    <div class="card-title">종목 조회</div>
    <div class="row">
      <input id="corpName" list="dbCompanyList" onfocus="loadDbCompanies().catch(function(){})" placeholder="종목명 (예: 삼성전자)" value="삼성전자" style="flex:1; min-width:160px;" autocomplete="off" />
      <datalist id="dbCompanyList"></datalist>
    </div>
    <div class="row" style="margin-top:8px;">
      <label style="font-size:14px; color:var(--text-muted);">조회 기간</label>
      <input id="startYear" type="number" class="yr-input" />
      <span>~</span>
      <input id="endYear" type="number" class="yr-input" />
      <span style="font-size:13px; color:var(--text-muted);">(DB에 없는 종목/기간만 새로 저장)</span>
    </div>
    <div class="row" style="margin-top:8px;">
      <button class="primary" onclick="fetchAndSave()">DART에서 조회 + 저장</button>
      <button onclick="loadFromDb()">DB에서 조회</button>
      <button onclick="toggleDbList()">DB 저장 종목 보기</button>
    </div>
    <div id="dbListWrap" style="display:none; margin-top:10px; padding:10px; background:var(--bg); border:1px solid var(--border); border-radius:8px;">
      <div class="row" style="align-items:center;">
        <input id="dbListFilter" oninput="renderDbList()" placeholder="종목명/종목코드로 거르기" style="flex:1; min-width:140px;" />
        <button onclick="toggleDbList(true)" style="padding:6px 10px; font-size:13px;">새로고침</button>
      </div>
      <div id="dbListCount" style="font-size:12px; color:var(--text-muted); margin:6px 0;"></div>
      <div id="dbListBody" style="display:flex; flex-wrap:wrap; gap:6px; max-height:220px; overflow:auto;"></div>
    </div>
    <div class="row" style="margin-top:8px;">
      <button onclick="queueFetch()">백그라운드로 받기 (화면 꺼도 진행)</button>
      <button onclick="checkQueueStatus()">진행 상황 확인</button>
      <button id="btnWatch" onclick="toggleWatchlist()">☆ 관심종목에 추가</button>
    </div>
    <div id="queueStatus" style="font-size:13px; color:var(--text-muted); margin-top:4px;"></div>
    <div id="watchlistWrap" class="row" style="margin-top:10px; gap:6px;"></div>
  </div>

  <div class="card">
    <div class="card-title">보기 방식</div>
    <div class="row">
      <button id="btnQuarterly" class="toggle-active" onclick="setView('quarterly')">분기별 보기</button>
      <button id="btnAnnual" onclick="setView('annual')">연간 보기</button>
      <button onclick="downloadFinancialMarkdown()" title="지금 표에 보이는 데이터를 LLM(ChatGPT·Claude 등)에 붙여넣거나 업로드하기 좋은 마크다운 파일로 저장합니다">⬇ LLM용 마크다운 다운로드</button>
    </div>
  </div>

  <div class="card">
    <div class="card-title">밸류에이션</div>
    <div class="row">
      <label>현재 주가 <input id="currentPrice" type="number" style="width:120px" placeholder="예: 88000" /></label>
      <button onclick="fetchLatestPrice()">전일 종가 가져오기</button>
      <button class="primary" onclick="renderSummary()">계산하기</button>
    </div>
    <div id="summary" style="display:none; margin-top:10px;"></div>
  </div>

  <div class="card">
    <div class="card-title">5단계 ROE 분해</div>
    <div class="row">
      <button id="fsBtnAnnual" class="toggle-active" onclick="setFiveStepView('annual')">연도별</button>
      <button id="fsBtnQuarterly" onclick="setFiveStepView('quarterly')">분기별</button>
      <button onclick="renderFiveStep()">분해해서 보기</button>
    </div>
    <div id="fiveStepWrap" style="display:none; margin-top:10px; overflow-x:auto;"></div>
  </div>

  <div class="card">
    <div class="card-title">2종목 비교 (DB에 저장된 데이터끼리, DART 재조회 없음)</div>
    <div class="row">
      <input id="cmpNameA" placeholder="종목 A (예: 삼성전자)" style="flex:1; min-width:120px;" />
      <input id="cmpPriceA" type="number" placeholder="A 현재주가" style="width:110px" />
    </div>
    <div class="row" style="margin-top:6px;">
      <input id="cmpNameB" placeholder="종목 B (예: 현대자동차)" style="flex:1; min-width:120px;" />
      <input id="cmpPriceB" type="number" placeholder="B 현재주가" style="width:110px" />
    </div>
    <div class="row" style="margin-top:8px;">
      <button class="primary" onclick="compareStocks()">비교하기</button>
    </div>
    <div id="cmpStatus" style="font-size:13px; color:var(--text-muted); margin-top:6px;"></div>
    <div id="cmpWrap" style="display:none; margin-top:10px; overflow-x:auto;"></div>
  </div>

  <div class="card">
    <div class="card-title">거시경제 지표 (환율·금리·WTI)</div>
    <p style="font-size:13px; color:var(--text-muted); line-height:1.5; margin-top:0;">
      원/달러 환율·통안증권(1년)·국고채(3년)·국고채(10년) 금리는 한국은행 ECOS에서, WTI 현물가는 미국 FRED에서 받아옵니다.
      한 번 갱신해두면(최근 9년치) 아래 "거시경제 상관관계"에서 계속 재사용합니다 — 보통 처음 한 번만 누르면 됩니다.
    </p>
    <div class="row">
      <button class="primary" onclick="refreshMacroData()">거시경제 데이터 갱신</button>
    </div>
    <div id="macroStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px; white-space:pre-line;"></div>
  </div>

  <div class="card">
    <div class="card-title">거시경제 상관관계 (현재 조회된 종목 기준)</div>
    <p style="font-size:13px; color:var(--text-muted); line-height:1.5; margin-top:0;">
      이 종목의 공시 시점마다, 그 시점의 거시지표 값과 "실적(매출·순이익 성장률, ROE)", "다음 공시까지의 주가 수익률"의
      상관계수를 계산합니다. <b>연간</b>(사업보고서 기준)과 <b>분기</b>(각 분기를 끝점으로 한 trailing 4분기 TTM 실적 기준)를
      함께 보여줍니다. 표본이 공시 횟수만큼이라 적을 수 있어 참고용입니다.
    </p>
    <div class="row">
      <button class="primary" onclick="computeMacroCorrelation()">상관관계 계산</button>
    </div>
    <div id="macroCorrStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px;"></div>
    <div id="macroCorrWrap" style="display:none; margin-top:10px; overflow-x:auto;"></div>
  </div>

  <div id="status"></div>
  <div id="wrap"></div>
  <div id="chartWrap" style="display:none; margin-top:14px;" class="card">
    <div id="chartTitle" style="font-weight:600; margin-bottom:6px;"></div>
    <div id="chartChips" class="chart-chips"></div>
    <canvas id="chartCanvas" style="width:100%; height:220px; touch-action:pan-y;"></canvas>
    <p id="chartNote" style="font-size:12px; color:var(--text-muted); margin-top:6px;"></p>
  </div>

  <div class="card">
    <div class="card-title">CB·BW 발행내역 (참고용 — 발행 시점 기준, 이후 상환·전환분은 반영 안 됨)</div>
    <div class="row">
      <label>시작일 <input id="cbwStart" type="text" placeholder="20150101" style="width:110px" /></label>
      <label>종료일 <input id="cbwEnd" type="text" placeholder="오늘(YYYYMMDD)" style="width:110px" /></label>
    </div>
    <div class="row" style="margin-top:8px;">
      <button onclick="rawCheckCbBw('cvbdIsDecsn')">전환사채(CB) 발행내역</button>
      <button onclick="rawCheckCbBw('bdwtIsDecsn')">신주인수권부사채(BW) 발행내역</button>
    </div>
    <pre id="cbwRaw" style="margin-top:8px; white-space:pre-wrap; background:#f1f5f9; padding:10px; font-size:11px; border-radius:8px; max-height:300px; overflow:auto;"></pre>
  </div>

  <details>
    <summary>🔧 원본 데이터 확인 (디버깅용)</summary>
    <div class="card" style="margin-top:8px;">
      <div class="row">
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
    </div>
  </details>

  </div><!-- /tabFinancial -->

  <div id="tabMomentum" style="display:none;">
    <div class="card">
      <div class="card-title">모멘텀 스크리닝 (실험적 기능)</div>
      <p style="font-size:13px; color:var(--text-muted); line-height:1.5; margin-top:0;">
        시가총액 500억원 이상 · 거래대금 15억원 이상 종목만 저장 대상으로 삼습니다. "유니버스 갱신"을 누르면 최신 거래일 시세를
        종목별이 아니라 날짜 기준으로 한꺼번에(약 6분 안에) 추가하고, 처음 보는 종목만 전체 가격이력을 백그라운드로 채웁니다
        (하루 150종목, 거래대금 큰 종목부터). 이 작업은 매일 한국시간 18:01에 자동으로 시작되므로, 버튼은 처음 한 번이나
        문제가 있을 때만 누르면 됩니다. 같은 날 여러 번 누르면 쓰기 한도를 낭비합니다.
      </p>
      <div class="row">
        <button class="primary" onclick="refreshMarketUniverse()">① 유니버스 갱신 (오늘자 통과 종목 받기)</button>
        <button onclick="checkMarketStatus()">진행 상황 새로고침</button>
      </div>
      <div id="marketStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px; white-space:pre-line;"></div>
    </div>

    <div class="card">
      <div class="card-title">모멘텀 스크리닝 — 8가지 Top 50 (시가총액·거래대금 직접 입력)</div>
      <div id="screenRegime" style="font-size:13px; line-height:1.5; padding:8px 10px; border-radius:8px; background:var(--bg); border:1px solid var(--border); margin-bottom:8px;">저장된 결과를 불러오는 중...</div>
      <details style="font-size:13px; color:var(--text-muted); line-height:1.6; margin-bottom:8px;">
        <summary style="cursor:pointer; color:var(--text);">8가지 목록은 어떻게 계산하나요? (눌러서 보기)</summary>
        <div style="margin-top:6px;">
          모든 수익률은 <b>거래일 기준</b>입니다(기준일 종가 ÷ N거래일 전 종가). 기준일은 종목 시세가 충분히 들어온 가장 최근 거래일입니다.<br />
          1) <b>20일 수익률</b> 상위 50 &nbsp; 2) <b>60일 수익률</b> 상위 50 &nbsp; 3) <b>120일 수익률</b> 상위 50<br />
          4) <b>20·60·120일 가중</b>: 입력한 가중치(합이 1이 아니어도 자동으로 합 1로 맞춤)로 세 수익률을 가중평균한 값 상위 50<br />
          5) <b>1개월 전 주가 ÷ 6개월 전 주가 − 1</b>: 21거래일 전 종가 ÷ 126거래일 전 종가 − 1. 최근 1개월을 일부러 빼는 중기 모멘텀(6-1)입니다. — Jegadeesh &amp; Titman (1993), 단기 반전은 Jegadeesh (1990)<br />
          6) <b>코스피 대비 1개월 RS</b>: (1 + 주식 1개월 수익률) ÷ (1 + 코스피 1개월 수익률), 21거래일 &nbsp; 7) <b>코스피 대비 3개월 RS</b>: 같은 식, 63거래일 &nbsp; 8) <b>코스피 대비 6개월 RS</b>: 같은 식, 126거래일. 1보다 크면 코스피를 이긴 것입니다.<br />
          <b>대상</b>: 기준일의 시가총액과 거래대금(그날 하루 값)이 입력한 하한 이상인 종목. 저장된 종목은 시총 500억·거래대금 15억 이상이라 그보다 낮게 입력해도 500억·15억으로 맞춰집니다.<br />
          <b>시가총액·영업이익(억원)</b>: 시가총액은 기준일 시세 API의 값입니다. 영업이익은 공공데이터포털 종목정보 → 법인등록번호 → 기업재무정보(요약재무제표)로 받은 가장 최근 사업연도 값이며(연결 우선), 못 받은 종목은 DART로 저장해 둔 값(표에 DART로 표시)으로 대체합니다.<br />
          <b>한계(꼭 읽어주세요)</b>: ① 공공데이터포털 시세는 기준일 다음 영업일 13시 이후에 올라오므로 기준일이 "어제"가 아니라 그 전 거래일일 수 있습니다(토요일 새벽 계산은 보통 목요일 종가). ② 거래대금은 기준일 하루 값이라 일시적으로 줄어든 종목이 빠질 수 있습니다. ③ 수익률 상위는 급등 종목이 많이 섞이고 변동성·위험은 반영하지 않습니다. ④ 학계의 모멘텀 효과는 "평균적으로 그랬다"이지 매번 통한다는 보증이 아니며, 한국 시장은 효과가 약하거나 불안정하다는 연구도 있습니다(Chui·Titman·Wei 2010). ⑤ 이 순위의 과거 성과(백테스트)는 아직 검증 전입니다. 참고용 도구로 쓰시고 투자 판단의 책임은 본인에게 있습니다.
        </div>
      </details>
      <div class="row" style="font-size:13px; color:var(--text-muted);">
        <label>시가총액 ≥ <input id="inMcap" type="number" value="600" step="50" min="500" style="width:80px" /> 억원</label>
        <label>거래대금 ≥ <input id="inTv" type="number" value="15" step="1" min="15" style="width:70px" /> 억원</label>
      </div>
      <div class="row" style="font-size:13px; color:var(--text-muted); margin-top:6px;">
        <span>가중 목록 가중치:</span>
        <label>20일 <input id="inW20" type="number" step="0.05" min="0" value="0.40" style="width:80px" /></label>
        <label>60일 <input id="inW60" type="number" step="0.05" min="0" value="0.30" style="width:80px" /></label>
        <label>120일 <input id="inW120" type="number" step="0.05" min="0" value="0.30" style="width:80px" /></label>
      </div>
      <div class="row" style="font-size:13px; color:var(--text-muted); margin-top:6px;">
        <label><input id="inSave" type="checkbox" /> 이 결과를 주간 결과로 저장(쓰기 약 16건, 기본은 저장 안 함)</label>
      </div>
      <div class="row" style="margin-top:8px;">
        <button class="primary" onclick="loadScreen('run')">② 입력한 조건으로 지금 계산</button>
        <button onclick="loadScreen('top')">① 저장된 주간 결과 보기</button>
        <select id="screenDate" onchange="loadScreen('top')" style="min-width:120px;"><option value="">최신</option></select>
        <button onclick="downloadScreenCsv()" style="padding:5px 10px; font-size:13px;">CSV 다운로드</button>
      </div>
      <div id="screenStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px; line-height:1.5;"></div>
      <div id="screenOpStatus" style="font-size:12px; color:var(--text-muted); margin-top:2px;"></div>
      <div id="screenTabs" style="display:none; margin-top:8px; gap:6px; flex-wrap:wrap;"></div>
      <div id="screenWrap" style="display:none; margin-top:10px; overflow:auto; max-height:70vh; border:1px solid var(--border); border-radius:8px;"></div>
    </div>
  </div><!-- /tabMomentum -->

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
        ? '<span style="font-size:12px; color:var(--text-muted);">관심종목이 없습니다. 종목명을 입력하고 "☆ 관심종목에 추가"를 눌러보세요.</span>'
        : list.map((name) => \`<span class="watchlist-chip" onclick="loadWatchedStock('\${name.replace(/'/g, "\\\\'")}')">⭐ \${name} <span class="x" onclick="event.stopPropagation(); removeFromWatchlist('\${name.replace(/'/g, "\\\\'")}')">✕</span></span>\`).join('');
      updateWatchButtonState();
    }
    function updateWatchButtonState() {
      const name = document.getElementById('corpName').value.trim();
      const btn = document.getElementById('btnWatch');
      if (!btn) return;
      const inList = loadWatchlist().includes(name);
      btn.textContent = inList ? '★ 관심종목에서 제거' : '☆ 관심종목에 추가';
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
      loadFromDb();
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

    function toQuarterlyRows(rows) {
      const byYear = {};
      for (const r of rows.filter(isUsable)) {
        if (!byYear[r.bsns_year]) byYear[r.bsns_year] = {};
        byYear[r.bsns_year][r.reprt_code] = r;
      }
      const out = [];
      for (const y of Object.keys(byYear).sort()) {
        const q1 = byYear[y]['11013'];
        const q2 = byYear[y]['11012']; // 반기 thstrm_amount = 2분기 단독값 (그대로 사용)
        const q3 = byYear[y]['11014']; // 3분기 thstrm_amount = 3분기 단독값 (그대로 사용)
        const annual = byYear[y]['11011']; // 사업보고서 thstrm_amount = 연간 누적

        if (q1) out.push({ ...q1, period_label: \`\${y} 1분기\` });
        if (q2) out.push({ ...q2, period_label: \`\${y} 2분기\` });
        if (q3) out.push({ ...q3, period_label: \`\${y} 3분기\` });
        if (annual) {
          const row = { ...annual, period_label: \`\${y} 4분기\` };
          for (const key of FLOW_KEYS) {
            const parts = [q1 && q1[key], q2 && q2[key], q3 && q3[key]];
            if (annual[key] != null && parts.every((v) => v != null)) {
              row[key] = annual[key] - (parts[0] + parts[1] + parts[2]);
            } else {
              row[key] = null;
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
          { label: '단기대여금', key: 'short_term_loans', type: 'won' },
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
      if (col.key === '_grossMargin') return (r.revenue != null && r.cogs != null && r.revenue) ? (r.revenue - r.cogs) / r.revenue : null;
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

    function computeIC(r) {
      // 영업 관점 IC = (매출채권+기타채권+재고자산-단기대여금) - (매입채무+기타채무-단기차입금-유동성장기부채-유동리스부채) + 유형자산+무형자산+사용권자산
      if (r.receivables == null && r.payables == null && r.tangible_assets == null) return null;
      const v = (x) => x || 0;
      const operatingReceivables = v(r.receivables) + v(r.other_receivables) + v(r.inventory) - v(r.short_term_loans);
      const operatingPayables = v(r.payables) + v(r.other_payables) - v(r.short_term_borrowings) - v(r.current_portion_lt_debt) - v(r.current_lease_liabilities);
      const fixedAssets = v(r.tangible_assets) + v(r.intangible_assets) + v(r.right_of_use_assets);
      return (operatingReceivables - operatingPayables) + fixedAssets;
    }

    function computeROIC(r) {
      const ic = computeIC(r);
      if (ic == null || ic <= 0 || r.operating_income == null) return null;
      return (r.operating_income * (1 - 0.24)) / ic;
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
      const outstanding = (r.total_shares != null && r.treasury_shares != null) ? r.total_shares - r.treasury_shares : null;
      if (!outstanding || outstanding <= 0) return null;
      const earnings = r.parent_net_income != null ? r.parent_net_income : r.net_income;
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
      const roic = computeROIC(r);

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
      for (const key of FLOW_KEYS) {
        const vals = needed.map((x) => x[key]);
        ttm[key] = vals.every((v) => v != null) ? vals.reduce((a, b) => a + b, 0) : null;
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

      if (steps.length === 0) { alert('분석할 데이터가 없습니다.'); return; }

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
      el.innerHTML = \`<div class="card-title">5단계 ROE 분해 (\${noteLabel})</div><div style="font-size:11.5px; color:var(--text-muted); margin:2px 0 8px;">세율부담 × 이자부담 × EBIT마진 × 자산회전율 × 레버리지 = ROE (듀폰 분석) — 다섯 요인을 곱하면 오른쪽 "계산된 ROE"가 나오는지 직접 검산해볼 수 있습니다.</div>\` + html;
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

      const roics = annualRows.map(computeROIC).filter((v) => v != null);
      const byYearForROE = {};
      annualRows.forEach((r) => { byYearForROE[r.bsns_year] = r; });
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
      const outstandingShares = (latest.total_shares != null && latest.treasury_shares != null)
        ? latest.total_shares - latest.treasury_shares
        : null;
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
      const earningsSrcBasis = earningsRow.parent_net_income != null ? '지배주주순이익' : '연결순이익';
      const eps = (outstandingShares && earnings != null) ? earnings / outstandingShares : null;

      const marketCap = (priceInput && outstandingShares) ? priceInput * outstandingShares : null;
      const per = (priceInput && eps) ? priceInput / eps : null;
      const pbr = (priceInput && bps) ? priceInput / bps : null;
      const earningsYield = eps && priceInput ? eps / priceInput : null; // = 1/PER, 요구수익률 관점

      // --- Financial(재무안전성): 부채비율, 이자보상배율 ---
      // latest(가장 최근 조회 시점, 분기 포함)를 기준으로 계산 — 재무상태표 항목은 분기에도 찍히므로 바로 쓸 수 있다.
      const debtRatio = (latest.total_liabilities != null && latest.total_equity) ? latest.total_liabilities / latest.total_equity : null;
      // 이자보상배율은 "그 기간의" 영업이익/이자비용이 필요해 분기 단독값이 섞인 latest보다 TTM(또는 최신 연간) 쪽이 맞다.
      const interestCoverage = (earningsRow.operating_income != null && earningsRow.interest_expense) ? earningsRow.operating_income / earningsRow.interest_expense : null;

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
      const grossProfit = (earningsRow.revenue != null && earningsRow.cogs != null)
        ? earningsRow.revenue - earningsRow.cogs
        : null;
      const grossMargin = (grossProfit != null && earningsRow.revenue) ? grossProfit / earningsRow.revenue : null;
      const assetTurnover = (earningsRow.revenue != null && totalAssets) ? earningsRow.revenue / totalAssets : null;
      const grossProfitability = (grossProfit != null && totalAssets) ? grossProfit / totalAssets : null;

      // --- Growth(성장성): 매출/순이익 YoY — 연간 데이터 중 최근 2개 연도 비교 ---
      let revenueGrowth = null, netIncomeGrowth = null;
      if (annualRows.length >= 2) {
        const curr = annualRows[annualRows.length - 1];
        const prev = annualRows[annualRows.length - 2];
        if (curr.revenue != null && prev.revenue) revenueGrowth = (curr.revenue - prev.revenue) / Math.abs(prev.revenue);
        const currNi = curr.parent_net_income != null ? curr.parent_net_income : curr.net_income;
        const prevNi = prev.parent_net_income != null ? prev.parent_net_income : prev.net_income;
        if (currNi != null && prevNi) netIncomeGrowth = (currNi - prevNi) / Math.abs(prevNi);
      }

      return {
        avgROIC, roicN: roics.length, avgROE, roeN: roes.length, consolCnt,
        avgPayout, payoutN: payoutKnownResults.length, avgAdjustedROE,
        latestLabel: latest.period_label, bpsBasis, bps, projected,
        priceInput, marketCap, expectedMultiple, annualizedReturn,
        eps, epsBasis: earningsBasis + '·' + earningsSrcBasis, per, pbr, earningsYield,
        debtRatio, interestCoverage, revenueGrowth, netIncomeGrowth,
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
      const grossProfitArr = annualRows.map((r) => (r.revenue != null && r.cogs != null) ? r.revenue - r.cogs : null);
      const grossMargin = annualRows.map((r, i) => (grossProfitArr[i] != null && r.revenue) ? grossProfitArr[i] / r.revenue : null);
      const assetTurnover = annualRows.map((r, i) => (r.revenue != null && totalAssetsArr[i]) ? r.revenue / totalAssetsArr[i] : null);
      const grossProfitability = annualRows.map((r, i) => (grossProfitArr[i] != null && totalAssetsArr[i]) ? grossProfitArr[i] / totalAssetsArr[i] : null);
      const debtRatio = annualRows.map((r) => (r.total_liabilities != null && r.total_equity) ? r.total_liabilities / r.total_equity : null);
      const interestCoverage = annualRows.map((r) => (r.operating_income != null && r.interest_expense) ? r.operating_income / r.interest_expense : null);
      const revenueGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = annualRows[i - 1];
        return (r.revenue != null && prev.revenue) ? (r.revenue - prev.revenue) / Math.abs(prev.revenue) : null;
      });
      const netIncomeGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = annualRows[i - 1];
        const cur = r.parent_net_income != null ? r.parent_net_income : r.net_income;
        const pr = prev.parent_net_income != null ? prev.parent_net_income : prev.net_income;
        return (cur != null && pr) ? (cur - pr) / Math.abs(pr) : null;
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
      revenueGrowth:   { dir: 'high', good: 0.10, caution: 0, fmt: (v) => (v * 100).toFixed(2) + '%' },
      netIncomeGrowth: { dir: 'high', good: 0.10, caution: 0, fmt: (v) => (v * 100).toFixed(2) + '%' },
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
      if (!m) { alert('연간(사업보고서) 데이터가 없습니다. 먼저 조회/저장하세요.'); return; }
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
          { label: '이자보상배율', key: 'interestCoverage', value: m.interestCoverage, series: hist && hist.interestCoverage },
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
const AN_TH = { roicGood: 0.10, growthHigh: 0.10, growthVeryHigh: 0.15, growthLow: 0.05, fcfNiGood: 0.7, accrualMax: 0.05, taxRate: 0.24, defaultR: 0.09, omega: 0.62 };
const AN_STATE = { r: AN_TH.defaultR, payout: null, ov: {}, key: null };
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
const anOutstanding = (r) => { if (r.total_shares == null || r.treasury_shares == null) return null; const o = r.total_shares - r.treasury_shares; return o > 0 ? o : null; };
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
    d.gp = (r.revenue != null && r.cogs != null) ? r.revenue - r.cogs : null;
    d.ebit = r.operating_income; d.ni = anNI(r); d.niCons = r.net_income;
    d.shares = anOutstanding(r); d.eps = anDiv(d.ni, d.shares);
    d.ocf = r.ocf; d.capex = r.capex; d.fcf = r.fcf;
    d.da = anRefetched(r) && (r.depreciation != null || r.amortization != null) ? (r.depreciation || 0) + (r.amortization || 0) : null;
    d.ebitda = anFin(d.ebit) && anFin(d.da) ? d.ebit + d.da : null;
    d.assets = anAssets(r); d.avgAssets = avgOf(d.assets, prior ? anAssets(prior) : null);
    d.equity = r.parent_equity != null ? r.parent_equity : r.total_equity;
    d.ic = computeIC(r); d.nopat = anFin(d.ebit) ? d.ebit * (1 - AN_TH.taxRate) : null;
    const roe = computeROEAvg(r, pv); d.roe = roe && isFinite(roe.value) ? roe.value : null;
    d.roic = computeROIC(r);
    d.gm = anDiv(d.gp, d.revenue);
    d.gpa = anDiv(d.gp, d.assets); d.opm = anDiv(d.ebit, d.revenue); d.fcfm = anDiv(d.fcf, d.revenue);
    d.fcfni = d.ni > 0 ? anDiv(d.fcf, d.ni) : null; d.cfoni = d.ni > 0 ? anDiv(d.ocf, d.ni) : null;
    d.accrual = (anFin(d.niCons) && anFin(r.ocf)) ? anDiv(d.niCons - r.ocf, d.avgAssets) : null; // Sloan(1996) 발생액 = (순이익-영업CF)/평균총자산
    d.at = anDiv(d.revenue, d.avgAssets);
    const wcA = (r.receivables != null || r.inventory != null) ? (r.receivables || 0) + (r.other_receivables || 0) + (r.inventory || 0) : null;
    const wcL = r.payables != null ? (r.payables || 0) + (r.other_payables || 0) : null;
    d.wc = (wcA != null && wcL != null) ? wcA - wcL : null; d.wcRev = anDiv(d.wc, d.revenue);
    d.borrow = anBorrow(r); d.liquid = anLiquid(r);
    d.netDebt = anFin(d.borrow) && anFin(d.liquid) ? d.borrow - d.liquid : null;
    d.de = anDiv(r.total_liabilities, r.total_equity); d.ibde = anDiv(d.borrow, r.total_equity);
    d.nde = (anFin(d.netDebt) && d.ebitda > 0) ? d.netDebt / d.ebitda : null;
    d.icov = r.interest_expense ? anDiv(d.ebit, r.interest_expense) : null;
    d.cur = anDiv(r.current_assets, r.current_liabilities);
    d.cashDebt = d.borrow > 0 ? anDiv(d.liquid, d.borrow) : null;
    d.payout = payoutRatioOf(r);
    d.divPaid = anFlow0(r, 'dividends_paid'); d.buyback = anFlow0(r, 'buyback'); d.acq = anFlow0(r, 'acquisitions');
    d.repay = anFlow0(r, 'debt_repay'); d.rd = anFlow0(r, 'intangible_capex');
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
    d.dIC = (p && anFin(d.ic) && anFin(p.ic)) ? d.ic - p.ic : null;
    d.reinv = (anFin(d.dIC) && d.nopat > 0) ? d.dIC / d.nopat : null;
    // 증분 ROIC(3년): ΔNOPAT(최근 3년) ÷ ΔInvested Capital(투자는 이익에 선행하므로 1년 앞선 시점부터 3년간)
    const a = idx[d.year - 3], b1 = idx[d.year - 1], b4 = idx[d.year - 4];
    d.incRoic = (a && b1 && b4 && anFin(d.nopat) && anFin(a.nopat) && anFin(b1.ic) && anFin(b4.ic) && (b1.ic - b4.ic) > 0) ? (d.nopat - a.nopat) / (b1.ic - b4.ic) : null;
    d.incRoe = (a && b1 && b4 && anFin(d.ni) && anFin(a.ni) && anFin(b1.equity) && anFin(b4.equity) && (b1.equity - b4.equity) > 0) ? (d.ni - a.ni) / (b1.equity - b4.equity) : null;
  });
  return list;
}
const anWin = (list, key, n) => list.slice(-n).map((d) => d[key]);
function anCagr(list, key, n) {
  const last = list[list.length - 1]; if (!last) return null;
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
  const val = (r, k) => k === 'gp' ? ((r.revenue != null && r.cogs != null) ? r.revenue - r.cogs : null) : k === 'ni' ? anNI(r) : r[k];
  const find = (y, n) => q.find((x) => x.period_label === (y + ' ' + n + '분기'));
  const last3 = q.slice(-3).reverse(); // 최신 → 과거
  if (!last3.length) return null;
  const keys = [['revenue', '매출액'], ['gp', '매출총이익'], ['ebit', '영업이익'], ['ni', '순이익'], ['fcf', 'FCF']];
  const out = { labels: last3.map((r) => r.period_label), rows: [] };
  for (const [k, name] of keys) {
    const kk = k === 'ebit' ? 'operating_income' : k;
    const g = last3.map((r) => {
      const y = Number(r.bsns_year), n = Number(r.period_label.split(' ')[1].replace('분기', ''));
      const pr = find(y - 1, n);
      const c = val(r, kk), p = pr ? val(pr, kk) : null;
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
  const ret6 = pick(182), ret12 = pick(365);
  const react = anMean(q.slice(-4).map((r) => priceReturnOf(r)));
  let level = null;
  const a = ret6, b = ret12;
  if (anFin(a) || anFin(b)) {
    const pos = [a, b].filter(anFin).every((v) => v > 0), neg = [a, b].filter(anFin).every((v) => v < 0);
    level = (pos && anFin(b) && b >= 0.3 && anFin(a) && a > 0.1) ? '강함' : pos ? '상승' : neg ? '약함' : '중립';
  }
  return { ret6, ret12, react, level };
}

// ---------- 밸류에이션 스냅샷 ----------
function anValSnapshot(rows, price, m, list) {
  const qrows = toQuarterlyRows(rows); const ttm = buildTTMRow(qrows);
  const annual = toAnnualRows(rows);
  const cur = ttm ? ttm.row : annual[annual.length - 1];
  const snap = latestSnapshotRow(rows);
  const out = { basis: ttm ? 'TTM' : '최근 연간', price };
  out.ebit = cur ? cur.operating_income : null;
  out.da = (cur && anRefetched(cur) && (cur.depreciation != null || cur.amortization != null)) ? (cur.depreciation || 0) + (cur.amortization || 0) : null;
  out.ebitda = anFin(out.ebit) && anFin(out.da) ? out.ebit + out.da : null;
  out.fcf = cur ? cur.fcf : null;
  out.mcap = m.marketCap;
  const borrow = snap ? anBorrow(snap) : null, liquid = snap ? anLiquid(snap) : null;
  const minority = (snap && snap.total_equity != null && snap.parent_equity != null) ? snap.total_equity - snap.parent_equity : 0;
  out.netDebt = anFin(borrow) && anFin(liquid) ? borrow - liquid : null;
  out.ev = (anFin(out.mcap) && anFin(out.netDebt)) ? out.mcap + out.netDebt + minority : null;
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
  let prev = -0.10, fp = f(prev);
  for (let g = -0.095; g < r - 0.003; g += 0.0005) {
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
function anClassify(c) {
  const ok = (cond) => cond == null ? null : !!cond;
  const g = c.growth, roic3 = c.roic3, val = c.valLevel, mom = c.momLevel;
  const cheap = val === '저평가' || val === '매우 저평가', rich = val === '높음' || val === '매우 높음';
  const labels = [
    { name: '최우선선호주', desc: 'Quality↑ · Growth↑ · ROIC↑ · Momentum↑ · Valuation 적정', conds: [
      ['Quality 양호 (4개 점검 중 3개 이상 충족)', ok(c.qualityOk)],
      ['Growth 높음 (성장률 ' + anPct(AN_TH.growthHigh, 0) + ' 이상)', ok(g != null ? g >= AN_TH.growthHigh : null)],
      ['ROIC(3년 평균) ' + anPct(AN_TH.roicGood, 0) + ' 이상', ok(roic3 != null ? roic3 >= AN_TH.roicGood : null)],
      ['Momentum 상승 이상', ok(mom != null ? (mom === '상승' || mom === '강함') : null)],
      ['Valuation 적정(또는 그 이하로 저렴)', ok(val != null ? (val === '적정' || cheap) : null)]] },
    { name: '턴어라운드', desc: 'Quality 양호 · Growth 낮음 · Valuation 저평가 · Momentum 아직 약함', conds: [
      ['Quality 양호', ok(c.qualityOk)],
      ['Growth 낮음 (' + anPct(AN_TH.growthLow, 0) + ' 미만)', ok(g != null ? g < AN_TH.growthLow : null)],
      ['Valuation 저평가 이상', ok(val != null ? cheap : null)],
      ['Momentum 약함 또는 중립', ok(mom != null ? (mom === '약함' || mom === '중립') : null)]] },
    { name: '성장주', desc: 'Growth↑↑ · EPS↑↑ · Momentum↑↑ · Valuation 높음', conds: [
      ['Growth 매우 높음 (' + anPct(AN_TH.growthVeryHigh, 0) + ' 이상)', ok(g != null ? g >= AN_TH.growthVeryHigh : null)],
      ['EPS(또는 순이익) 3년 CAGR ' + anPct(AN_TH.growthVeryHigh, 0) + ' 이상', ok(c.epsG != null ? c.epsG >= AN_TH.growthVeryHigh : null)],
      ['Momentum 강함/상승', ok(mom != null ? (mom === '상승' || mom === '강함') : null)],
      ['Valuation 높음 이상', ok(val != null ? rich : null)]] },
    { name: '밸류 Trap', desc: 'PER·PBR 낮음 · ROIC↓ · Growth↓ · FCF↓ · Momentum↓', conds: [
      ['PER 10배 이하 & PBR 1배 이하', ok(c.perLow != null && c.pbrLow != null ? (c.perLow && c.pbrLow) : null)],
      ['ROIC 하락 (3년 평균이 5년 평균보다 낮거나 6% 미만)', ok(c.roicDown)],
      ['Growth 낮음 (' + anPct(AN_TH.growthLow, 0) + ' 미만)', ok(g != null ? g < AN_TH.growthLow : null)],
      ['FCF 마진 하락 (3년 평균 < 5년 평균)', ok(c.fcfDown)],
      ['Momentum 약함', ok(mom != null ? mom === '약함' : null)]] },
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
  if (g != null && roic3 != null) {
    const hg = g >= AN_TH.growthHigh, hr = roic3 >= AN_TH.roicGood;
    quad = hg && hr ? { n: '성장↑ · ROIC↑', t: '우량 성장 (성장이 가치를 만드는 구간 — 재투자할수록 유리)' }
      : (!hg && hr) ? { n: '성장↓ · ROIC↑', t: '현금창출형 (재투자처가 적음 — 배당·자사주 등 주주환원 정책이 중요)' }
      : (hg && !hr) ? { n: '성장↑ · ROIC↓', t: '성장하지만 자본효율 낮음 (ROIC가 자본비용보다 낮으면 성장이 오히려 가치를 깎을 수 있음)' }
      : { n: '성장↓ · ROIC↓', t: '구조적 부진 후보 (싸 보여도 가치함정 가능성 점검)' };
  }
  return { labels, head, quad };
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
  const growthParts = [anCagr(list, 'revenue', 3), anCagr(list, 'ebit', 3), anCagr(list, 'eps', 3) != null ? anCagr(list, 'eps', 3) : anCagr(list, 'ni', 3)].filter(anFin);
  const growth = growthParts.length >= 2 ? anMean(growthParts) : (growthParts.length ? growthParts[0] : null);
  const epsG = anCagr(list, 'eps', 3) != null ? anCagr(list, 'eps', 3) : anCagr(list, 'ni', 3);
  const q = [roic3 != null ? roic3 >= AN_TH.roicGood : null, roe3 != null ? roe3 >= 0.10 : null, anMean(anWin(list, 'fcfni', 3)) != null ? anMean(anWin(list, 'fcfni', 3)) >= AN_TH.fcfNiGood : null, anMean(anWin(list, 'accrual', 3)) != null ? anMean(anWin(list, 'accrual', 3)) <= AN_TH.accrualMax : null];
  const qKnown = q.filter((x) => x != null);
  const qualityOk = qKnown.length >= 3 ? qKnown.filter(Boolean).length >= 3 : null;
  const fm3 = anMean(anWin(list, 'fcfm', 3)), fm5 = anMean(anWin(list, 'fcfm', 5));
  const valL = anValuationLevel(vs, expG, impE, scen, price);
  const ctx = {
    list, last, vs, r, roe3, roe5, roic3, roic5, payoutAvg, reinv3, gRoe, gRoic, def, scen, impE, impF, expG, mom, growth, epsG,
    qualityOk, qPts: q, valLevel: valL.level, valN: valL.n, momLevel: mom ? mom.level : null, base0: roeBase,
    perLow: anFin(vs.per) ? vs.per <= 10 : null, pbrLow: anFin(vs.pbr) ? vs.pbr <= 1 : null,
    roicDown: (roic3 != null && roic5 != null) ? (roic3 < roic5 || roic3 < 0.06) : null,
    fcfDown: (fm3 != null && fm5 != null) ? fm3 < fm5 : null,
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
  return '<div style="overflow-x:auto; margin-top:8px;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
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

function anRenderClassify(c) {
  const h = c.cls.head; const col = h.kind === 'match' ? '#16a34a' : h.kind === 'near' ? '#d97706' : '#64748b';
  const lv = (x) => x == null ? 'N/A' : x;
  const axes = anGrid([
    anTile('Growth(3Y CAGR 평균)', anPct(c.growth), '매출·영업이익·EPS(없으면 순이익) 3년 CAGR 평균'), anTile('ROIC (3Y 평균)', anPct(c.roic3), '5Y 평균 ' + anPct(c.roic5)),
    anTile('Quality', c.qualityOk == null ? 'N/A' : (c.qualityOk ? '양호' : '미흡'), 'ROIC≥10%, ROE≥10%, FCF/NI≥0.7, 발생액≤5% 중 ' + c.qPts.filter((x) => x === true).length + '개 충족'),
    anTile('Valuation', lv(c.valLevel), c.valN ? ('신호 ' + c.valN + '개 종합: PER·PBR 과거분위, 내재성장 vs 기대성장, 시나리오 대비 현재가') : '현재 주가 입력 필요'),
    anTile('Momentum(약식)', lv(c.momLevel), c.mom ? ('6개월 ' + anPct(c.mom.ret6) + ' / 12개월 ' + anPct(c.mom.ret12) + ' (분기말 주가 기준)') : '현재 주가 입력 필요'),
  ]);
  const cards = c.cls.labels.map((L) => '<div style="border:1px solid var(--border); border-radius:8px; padding:8px 10px; margin-top:8px;"><div style="display:flex; justify-content:space-between; gap:8px;"><b>' + L.name + '</b><span style="font-size:12px; color:' + (L.full ? '#16a34a' : 'var(--text-muted)') + ';">' + L.pass + '/' + L.total + ' 충족' + (L.unk ? ' (미확인 ' + L.unk + ')' : '') + '</span></div><div style="font-size:11px; color:var(--text-muted); margin:2px 0 4px;">' + L.desc + '</div>' +
    L.conds.map((x) => '<div style="font-size:12px;">' + anMark(x[1]) + ' ' + x[0] + '</div>').join('') + '</div>').join('');
  const q = c.cls.quad ? '<div style="margin-top:8px; padding:8px 10px; background:#f1f5f9; border-radius:8px; font-size:12.5px;"><b>성장 × ROIC 4분면: ' + c.cls.quad.n + '</b> — ' + c.cls.quad.t + '</div>' : '';
  return anSection('🏷 종합 분류 (결과 시계열로 본 유형)', col,
    '아래 기준선(성장 10%·ROIC 10% 등)은 학술 정설이 아니라 <b>참고용 휴리스틱</b>입니다. 각 지표는 학술적으로 쓰이는 정의를 따르되, 어떤 조건에서 어떤 라벨이 붙었는지 ✓/✗로 모두 공개합니다.',
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
  const html = '<div style="display:flex; flex-wrap:wrap; gap:14px; align-items:end; padding:8px 10px; background:#f8fafc; border:1px solid var(--border); border-radius:8px; margin:8px 0;">' +
    '<label style="font-size:12px; color:var(--text-muted);">요구수익률 r(%)<br/><input id="anR" type="number" step="0.5" value="' + (c.r * 100).toFixed(1) + '" onchange="anSetR()" style="width:70px; margin-top:3px;"></label>' +
    '<label style="font-size:12px; color:var(--text-muted);">배당성향(%)<br/><input id="anPayout" type="number" step="5" value="' + (c.payoutAvg * 100).toFixed(0) + '" onchange="anSetPayout()" style="width:70px; margin-top:3px;"></label>' +
    '<button onclick="anResetScenario()" style="height:30px; font-size:12px; padding:4px 10px;">기본값으로 초기화</button></div>' +
    '<div style="overflow-x:auto;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>' +
    '<div id="anW" style="margin-top:8px; font-size:13px;"></div>';
  const desc = '<b>잔여이익모형(Residual Income Model)</b> — Ohlson(1995) 등 회계 기반 가치평가의 표준 틀입니다. 주당가치 = BPS + 향후 10년 초과이익(ROE−r)×장부가치의 현재가치 + 종가치. ROE는 출발값에서 수렴값으로 선형 수렴(수익성은 평균으로 돌아가는 경향: Fama-French 2000), 종가치는 초과이익이 해마다 ' + AN_TH.omega + ' 비율로 감쇠한다고 둡니다(Dechow-Hutton-Sloan 1999 실증치). ' +
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

function anRenderValuation(c) {
  const v = c.vs, rg = (x) => x ? ('최저 ' + anFmt(x.k, x.lo) + ' / 중앙 ' + anFmt(x.k, x.med) + ' / 최고 ' + anFmt(x.k, x.hi)) : 'N/A';
  const rows = [['PER', 'x', v.rangePer, v.per], ['PBR', 'x', v.rangePbr, v.pbr], ['FCF Yield', 'pct', v.rangeFcfy, v.fcfYield]];
  const rtab = '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th>지표</th><th>현재</th><th>5Y 범위(공시시점)</th><th>5Y 분위</th><th>10Y 범위(공시시점)</th><th>10Y 분위</th></tr></thead><tbody>' +
    rows.map(([n, k, r5, cv]) => { const f = (x) => x ? ('최저 ' + anFmt(k, x.lo) + ' · 중앙 ' + anFmt(k, x.med) + ' · 최고 ' + anFmt(k, x.hi) + ' (n=' + x.n + ')') : 'N/A'; const pos = (x) => x && anFin(x.pct) ? anPct(x.pct, 0) : 'N/A'; return '<tr><td class="an-first">' + n + '</td><td>' + anFmt(k, cv) + '</td><td>' + f(r5.y5) + '</td><td>' + pos(r5.y5) + '</td><td>' + f(r5.y10) + '</td><td>' + pos(r5.y10) + '</td></tr>'; }).join('') + '</tbody></table></div>';
  const tiles = anGrid([
    anTile('PER', anX(v.per), 'EPS(' + c.m.epsBasis + ') 기준'), anTile('PBR', anX(v.pbr)), anTile('Earnings Yield', anPct(v.earningsYield, 2), '= EPS ÷ 주가 (1/PER)'),
    anTile('Normalized PER', anX(v.normPer), '시총 ÷ 최근 ' + v.normN + '개년 평균 순이익 (Graham-Dodd/Shiller 방식, 물가 미조정)'),
    anTile('EV/EBIT', anX(v.evEbit), v.ev == null ? '재조회 필요(차입금·현금 항목)' : 'EV ' + anEok(v.ev) + ' (' + v.basis + ')'), anTile('EV/EBITDA', anX(v.evEbitda), v.ebitda == null ? '재조회 필요(감가상각 항목)' : 'EBITDA ' + anEok(v.ebitda) + ' = 영업이익+감가상각+무형자산상각'),
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
    '※ Industry valuation(업종 비교)은 업종 분류·동종 비교 데이터가 DB에 없어 <b>제외</b>했습니다. 5Y/10Y 범위는 저장된 "연간 공시시점 PER·PBR·FCF Yield"의 분포이며 "분위"는 과거 값 중 현재 값 이하인 비율입니다(0%=역대 최저, 100%=역대 최고 — PER·PBR은 낮을수록, FCF Yield는 높을수록 저렴). Implied &gt; Expected이면 시장이 펀더멘털보다 큰 성장을 가격에 넣은 것입니다.',
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
  out.push(anSection('🏆 Quality (수익성·이익의 질)', '#7c3aed', 'Gross Profitability(Novy-Marx 2013), 발생액(Sloan 1996: 현금 뒷받침 없는 이익은 지속성이 낮음), ROE/ROIC 평균·변동. ROIC는 영업이익×(1-24%) ÷ 영업 관점 투하자본(기존 계산과 동일).',
    anRoeStats(c) + anGrid([
      anTile('ROIC 10Y 평균', anPct(anMean(anWin(L, 'roic', 10))), anPeriodTxt(anPeriod(L, 'roic', 10))), anTile('ROIC 5Y 평균', anPct(c.roic5), anPeriodTxt(anPeriod(L, 'roic', 5))), anTile('ROIC 3Y 평균', anPct(c.roic3), anPeriodTxt(anPeriod(L, 'roic', 3))),
    ]) + anTable(L, [
      { label: 'ROE', key: 'roe', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }, { label: 'Gross Profitability (매출총이익/총자산)', key: 'gpa', fmt: 'pct' },
      { label: '영업이익률', key: 'opm', fmt: 'pct' }, { label: 'FCF 마진', key: 'fcfm', fmt: 'pct' }, { label: 'FCF/순이익', key: 'fcfni', fmt: 'x' }, { label: 'CFO/순이익', key: 'cfoni', fmt: 'x' },
      { label: '발생액(Accrual, 낮을수록 좋음)', key: 'accrual', fmt: 'pct' }, { label: '자산회전율', key: 'at', fmt: 'x' }], 10), false));
  // Capital efficiency
  out.push(anSection('⚙ Capital Efficiency (자본 효율 · 증분 수익)', '#0d9488', '"추가로 넣은 1원이 얼마를 버는가". 증분 ROIC = 최근 3년 ΔNOPAT ÷ 같은 기간(1년 선행) ΔInvested Capital (Mauboussin·Koller 방식). 투하자본이 늘지 않은 해는 N/A. 재투자율 = ΔIC ÷ NOPAT, 성장 = 재투자율 × ROIC (Damodaran).',
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
  out.push(anSection('🏦 Capital Allocation (자본 배분)', '#b45309', '이익을 배당·자사주·투자·M&A·부채상환 중 어디에 쓰는가. 금액은 현금흐름표 기준(재조회 후 채워짐). 주식수 변화가 음수면 자사주 소각/매입으로 주식수가 줄어든 것(주주환원 효과, Ikenberry et al. 1995).',
    anGrid([anTile('평균 배당성향', anPct(c.m.avgPayout), '기간: ' + anPeriodTxt(anPeriod(L, 'payout', 10)).replace('기간: ', ''))]) +
    anTable(L, [{ label: '배당성향 (DPS/EPS)', key: 'payout', fmt: 'pct' }, { label: '배당금 지급', key: 'divPaid', fmt: 'eok' }, { label: '자사주 매입', key: 'buyback', fmt: 'eok' }, { label: '총주주환원성향 (배당+자사주)/순이익', key: 'shp', fmt: 'pct' },
      { label: '유통주식수 변화(YoY)', key: 'shareChg', fmt: 'pct' }, { label: 'M&A (종속·사업결합 취득)', key: 'acq', fmt: 'eok' }, { label: 'CAPEX', key: 'capex', fmt: 'eok' }, { label: 'CAPEX/감가상각', key: 'capexDa', fmt: 'x' },
      { label: 'R&D 대용(무형자산 취득)', key: 'rd', fmt: 'eok' }, { label: '차입금 상환', key: 'repay', fmt: 'eok' }], 10), false));
  return out.join('');
}

function renderAnalysis() {
  const el = document.getElementById('analysis'); if (!el) return;
  const m = lastSummaryMetrics; if (!m) { el.innerHTML = ''; return; }
  const key = rawRows.length ? rawRows[0].corp_code : '';
  if (AN_STATE.key !== key) { AN_STATE.key = key; AN_STATE.ov = {}; AN_STATE.payout = null; }
  const price = m.priceInput;
  let c;
  try { c = anBuildContext(rawRows, price, m); } catch (e) { el.innerHTML = '<div class="card" style="color:var(--danger);">확장 분석 계산 오류: ' + anEsc(e.message || e) + '</div>'; return; }
  if (!c) { el.innerHTML = ''; return; }
  AN_CTX = c;
  const needRefetch = !c.last.r || !anRefetched(c.last.r);
  const notice = needRefetch ? '<div class="card" style="background:#fffbeb; border-color:#fcd34d; font-size:12.5px;">⚠ 이 종목은 <b>감가상각·배당지급·자사주·차입금 구성 항목이 아직 저장되어 있지 않습니다</b>. EBITDA, 순차입금, 총주주환원성향 등은 N/A로 표시되며, 위쪽에서 이 종목을 <b>다시 조회/저장</b>하면 채워집니다(DART 호출량은 기존과 같음).</div>' : '';
  el.innerHTML = notice + anRenderClassify(c) + anRenderJudge(c) + anRenderScenario(c) + anRenderValuation(c) + anRenderRD(c) + anRenderAreas(c);
  anUpdateScenarioView();
}

    async function fetchLatestPrice() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      if (!corpName) { statusEl.textContent = '종목명을 먼저 입력해주세요.'; return; }
      statusEl.textContent = '전일 종가 조회 중...';
      try {
        const res = await fetch(\`/api/latest-price?corp_name=\${encodeURIComponent(corpName)}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');
        document.getElementById('currentPrice').value = data.close;
        statusEl.textContent = \`\${data.corp_name} \${data.date} 종가 \${data.close.toLocaleString()}원을 반영했습니다.\`;
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
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
      const priceA = Number(document.getElementById('cmpPriceA').value) || null;
      const priceB = Number(document.getElementById('cmpPriceB').value) || null;
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

      const mA = computeSummaryMetrics(dataA.rows, priceA);
      const mB = computeSummaryMetrics(dataB.rows, priceB);
      if (!mA || !mB) {
        statusEl.textContent = '한쪽 이상 DB에 연간 데이터가 없습니다. 먼저 "DART에서 조회 + 저장"으로 데이터를 모아주세요.';
        return;
      }

      const pctStr = (v) => v != null ? (v * 100).toFixed(2) + '%' : 'N/A';
      const wonStr = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';
      const numStr = (v) => v != null ? v.toFixed(2) : 'N/A';

      const rows = [
        ['10년 평균 ROIC', pctStr(mA.avgROIC), pctStr(mB.avgROIC)],
        ['10년 평균 ROE', pctStr(mA.avgROE), pctStr(mB.avgROE)],
        ['10년 평균 배당성향', pctStr(mA.avgPayout), pctStr(mB.avgPayout)],
        ['배당 조정 ROE(예측용)', pctStr(mA.avgAdjustedROE), pctStr(mB.avgAdjustedROE)],
        ['BPS 기준시점', mA.latestLabel, mB.latestLabel],
        ['BPS', wonStr(mA.bps), wonStr(mB.bps)],
        ['10년 후 예상주가', wonStr(mA.projected), wonStr(mB.projected)],
        ['연환산 기대수익률(CAGR)', pctStr(mA.annualizedReturn), pctStr(mB.annualizedReturn)],
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

    async function fetchAndSave() {
      dbCompanies = null; // 새로 저장되면 DB 종목 목록 캐시 무효화
      const corpName = document.getElementById('corpName').value.trim();
      const startYear = Number(document.getElementById('startYear').value);
      const endYear = Number(document.getElementById('endYear').value);
      const statusEl = document.getElementById('status');
      document.getElementById('wrap').innerHTML = '';
      document.getElementById('summary').style.display = 'none';

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
        statusEl.textContent = \`저장 중... (\${i + 1}/\${periods.length}: \${p.year} \${p.label})\`;
        try {
          const res = await fetch(\`/api/fetch-and-save?corp_name=\${encodeURIComponent(corpName)}&year=\${p.year}&reprt_code=\${p.code}\`);
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '저장 실패');
          rows.push(data.row);
        } catch (e) {
          rows.push({ period_label: \`\${p.year} \${p.label}\`, bsns_year: String(p.year), reprt_code: p.code, error: e.message });
        }
        rawRows = rows;
        applyView();
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
      document.getElementById('tabFinancial').style.display = tab === 'financial' ? '' : 'none';
      document.getElementById('tabMomentum').style.display = tab === 'momentum' ? '' : 'none';
      document.getElementById('tabBtnFinancial').className = tab === 'financial' ? 'toggle-active' : '';
      document.getElementById('tabBtnMomentum').className = tab === 'momentum' ? 'toggle-active' : '';
      if (tab === 'momentum') { checkMarketStatus(); if (!screenAutoLoaded) { screenAutoLoaded = true; loadScreen('top'); } }
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
      { key: 'r20', label: '20일 수익률' },
      { key: 'r60', label: '60일 수익률' },
      { key: 'r120', label: '120일 수익률' },
      { key: 'rw', label: '20·60·120일 가중' },
      { key: 'm16', label: '1개월전÷6개월전−1' },
      { key: 'rs1', label: '코스피 대비 1개월' },
      { key: 'rs3', label: '코스피 대비 3개월' },
      { key: 'rs6', label: '코스피 대비 6개월' }
    ];
    let screenData = null;
    let screenTab = 'r20';
    let screenAutoLoaded = false;
    let opFillRunning = false;
    function sP(v, d) { return v == null ? 'N/A' : (v * 100).toFixed(d == null ? 1 : d) + '%'; }
    function sX(v, d) { return v == null ? 'N/A' : v.toFixed(d == null ? 2 : d); }
    function sN(v) { return v == null ? 'N/A' : Math.round(v).toLocaleString('ko-KR'); }
    function sCell(text, cls) { return '<td' + (cls ? ' class="' + cls + '"' : '') + '>' + text + '</td>'; }

    function setScreenTab(k) { screenTab = k; renderScreen(); }

    async function loadScreen(mode) {
      const st = document.getElementById('screenStatus');
      const g = (id) => document.getElementById(id).value;
      let url;
      if (mode === 'run') {
        url = '/api/screen/run?mcap=' + encodeURIComponent(g('inMcap')) + '&tv=' + encodeURIComponent(g('inTv')) +
          '&w20=' + encodeURIComponent(g('inW20')) + '&w60=' + encodeURIComponent(g('inW60')) + '&w120=' + encodeURIComponent(g('inW120')) +
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
        if (!res.ok) throw new Error(data.error || '실패');
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
      if (screenData.dates && screenData.dates.length) {
        const sel = document.getElementById('screenDate');
        sel.innerHTML = screenData.dates.map((d) => '<option value="' + d + '"' + (d === screenData.date ? ' selected' : '') + '>' + d + '</option>').join('');
      }
      const rg = document.getElementById('screenRegime');
      rg.style.background = '#f1f5f9';
      rg.innerHTML = '<b>기준일 ' + escHtml(meta.asof) + '</b> · 코스피 ' + Math.round(k.close || 0).toLocaleString() +
        ' · 코스피 수익률: 20일 ' + sP(k.r20) + ' / 60일 ' + sP(k.r60) + ' / 120일 ' + sP(k.r120) + ' / 1개월 ' + sP(k.r21) + ' / 3개월 ' + sP(k.r63) + ' / 6개월 ' + sP(k.r126) +
        '<br /><span style="color:var(--text-muted)">RS 목록은 주식 수익률(1+r)을 코스피 수익률(1+r)로 나눈 값입니다. 1보다 크면 코스피보다 강했다는 뜻입니다.</span>' +
        (meta.kospiError ? '<br /><span class="sc-warn">코스피 갱신 경고: ' + escHtml(meta.kospiError) + '</span>' : '');

      const w = meta.weights || {};
      let stTxt = '기준일 시세 ' + sN(meta.asof_rows) + '종목 중 조건(시가총액 ≥ ' + sN(meta.minMcapEok) + '억, 거래대금 ≥ ' + sN(meta.minTvEok) + '억) 통과 ' + sN(meta.passed_floor) + '종목 · 각 목록 상위 50개' +
        ' · 가중치 20일 ' + sX(w.w20) + ' / 60일 ' + sX(w.w60) + ' / 120일 ' + sX(w.w120) + (screenData.saved ? ' · 주간 결과로 저장됨' : '');
      if (meta.no_mcap > 0 && meta.asof_rows > 0 && meta.no_mcap / meta.asof_rows > 0.2) {
        stTxt += ' · 주의: 기준일 행 중 시가총액이 아직 저장되지 않은 종목이 ' + sN(meta.no_mcap) + '개 있어 결과에서 빠졌습니다(상단 "유니버스 갱신"을 한 번 누르고 몇 분 뒤 다시 계산하세요).';
      }
      if (meta.clamped && meta.clamped.length) stTxt += ' · ' + meta.clamped.join(' ');
      if (((screenData.lists || {}).r20 || []).length < 50) {
        stTxt += ' · 안내: 목록이 50개보다 적은 이유 — 조건 통과 ' + sN(meta.passed_floor) + '종목 중 가격 이력(약 6개월)이 모두 저장된 종목만 계산됩니다. 현재 백필이 끝난 종목은 ' + sN(meta.backfilled) + '개이고, 하루 150종목씩 늘어납니다.';
      }
      document.getElementById('screenStatus').textContent = stTxt;

      const lists = screenData.lists || {};
      const tabs = document.getElementById('screenTabs');
      tabs.style.display = 'flex';
      tabs.innerHTML = SCREEN_LISTS.map((L) => '<button class="' + (L.key === screenTab ? 'toggle-active' : '') + '" onclick="setScreenTab(&quot;' + L.key + '&quot;)" style="padding:5px 10px; font-size:13px;">' + L.label + ' (' + ((lists[L.key] || []).length) + ')</button>').join('');

      const overlap = {};
      SCREEN_LISTS.forEach((L) => { (lists[L.key] || []).forEach((r) => { overlap[r.stock_code] = (overlap[r.stock_code] || 0) + 1; }); });
      const kc = (col, extra) => ((screenTab === col ? 'sc-key ' : '') + (extra || '')).trim();
      const fh = (n, t) => '<th class="f f' + n + '">' + t + '</th>';
      const head = '<tr>' + fh(1, '순위') + fh(2, '종목') + fh(3, '현재가') + fh(4, '시총(억)') + fh(5, '거래대금(억)') + fh(6, '영업이익(억)') +
        '<th class="' + kc('r20') + '">20일</th><th class="' + kc('r60') + '">60일</th><th class="' + kc('r120') + '">120일</th><th class="' + kc('rw') + '">가중</th>' +
        '<th class="' + kc('m16') + '">1M전÷6M전−1</th><th class="' + kc('rs1') + '">RS 1개월</th><th class="' + kc('rs3') + '">RS 3개월</th><th class="' + kc('rs6') + '">RS 6개월</th><th>겹친 목록</th></tr>';
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
        return '<tr><td class="f f1">' + (i + 1) + '</td>' +
          '<td class="f f2"><b>' + escHtml(r.corp_name) + '</b><br /><span style="color:var(--text-muted); font-size:11px;">' + r.stock_code + '</span></td>' +
          '<td class="f f3">' + sN(r.c0) + '</td><td class="f f4">' + sN(r.mcapEok) + '</td><td class="f f5">' + sN(r.tvEok) + '</td>' + opCell +
          ret(r.r20, 'r20') + ret(r.r60, 'r60') + ret(r.r120, 'r120') + ret(r.rw, 'rw') + ret(r.m16, 'm16') + rs(r.rs1, 'rs1') + rs(r.rs3, 'rs3') + rs(r.rs6, 'rs6') +
          sCell(overlap[r.stock_code] + '/' + SCREEN_LISTS.length, overlap[r.stock_code] >= 4 ? 'sc-good' : '') + '</tr>';
      }).join('');
      const wrap = document.getElementById('screenWrap');
      wrap.innerHTML = '<table class="scr" style="width:max-content; min-width:100%;"><thead>' + head + '</thead><tbody>' + (body || '<tr><td colspan="15">조건에 맞는 종목이 없습니다(가격 이력이 부족하거나 조건이 너무 엄격할 수 있습니다).</td></tr>') + '</tbody></table>';
      wrap.style.display = '';
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
      const cols = ['목록', '순위', '종목명', '종목코드', '현재가', '시가총액(억원)', '거래대금(억원)', '영업이익(억원)', '영업이익연도', '영업이익출처', '20일', '60일', '120일', '가중', '1M전÷6M전−1', 'RS1개월', 'RS3개월', 'RS6개월'];
      const n = (v, d) => (v == null ? '' : Number(v).toFixed(d == null ? 4 : d));
      const lines = [cols.join(',')];
      const lists = screenData.lists || {};
      SCREEN_LISTS.forEach((L) => {
        (lists[L.key] || []).forEach((r, i) => {
          const o = r.op || {};
          lines.push(['"' + L.label + '"', i + 1, '"' + String(r.corp_name).split('"').join('""') + '"', r.stock_code, n(r.c0, 0), n(r.mcapEok, 0), n(r.tvEok, 1), n(o.v, 1), o.year || '', o.src || '',
            n(r.r20), n(r.r60), n(r.r120), n(r.rw), n(r.m16), n(r.rs1), n(r.rs3), n(r.rs6)].join(','));
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

    async function loadFromDb() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      document.getElementById('wrap').innerHTML = '';
      document.getElementById('summary').style.display = 'none';
      statusEl.textContent = 'DB 조회 중...';
      try {
        const res = await fetch(\`/api/financial-history-db?corp_name=\${encodeURIComponent(corpName)}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'DB 조회 실패');
        statusEl.textContent = \`\${data.corp_name} — DB에 저장된 \${data.rows.length}개 기간 (DART 재조회 안 함)\`;
        rawRows = data.rows;
        applyView();
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
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
      loadFromDb();
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

    async function computeMacroCorrelation() {
      const statusEl = document.getElementById('macroCorrStatus');
      const wrapEl = document.getElementById('macroCorrWrap');
      wrapEl.style.display = 'none';
      if (rawRows.length === 0) { statusEl.textContent = '먼저 종목을 조회해주세요.'; return; }
      statusEl.textContent = '계산 중...';

      let macro;
      try {
        macro = await loadMacroSeries();
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message + ' (먼저 "거시경제 데이터 갱신"을 눌러주세요)';
        return;
      }
      if (Object.keys(macro).length === 0) {
        statusEl.textContent = '거시경제 데이터가 비어있습니다. 먼저 "거시경제 데이터 갱신"을 눌러주세요.';
        return;
      }

      const annualPts = buildAnnualCorrPoints(rawRows, macro);
      const quarterPts = buildQuarterlyCorrPoints(rawRows, macro);
      if (annualPts.length < 3 && quarterPts.length < 3) {
        statusEl.textContent = \`공시일·주가 데이터가 있는 기간이 연간 \${annualPts.length}개, 분기 \${quarterPts.length}개뿐이라 상관관계를 계산하기엔 부족합니다(각각 최소 3개 필요). "DART에서 조회 + 저장"으로 더 많은 기간을 받아주세요.\`;
        return;
      }

      const annualTargets = [
        { key: 'revenueGrowth', label: '매출액 성장률(YoY)' },
        { key: 'netIncomeGrowth', label: '순이익 성장률(YoY)' },
        { key: 'roe', label: 'ROE' },
        { key: 'forwardReturn', label: '다음 공시까지 주가수익률' },
      ];
      const quarterTargets = [
        { key: 'revenueGrowth', label: 'TTM 매출 성장률(YoY)' },
        { key: 'netIncomeGrowth', label: 'TTM 순이익 성장률(YoY)' },
        { key: 'roe', label: 'TTM ROE' },
        { key: 'forwardReturn', label: '다음 분기 공시까지 주가수익률' },
      ];
      const section = (title, note, points, targets) => points.length < 3
        ? \`<div style="margin:10px 0 4px;"><b>\${title}</b> <span style="color:var(--text-muted); font-size:12px;">— 표본이 \${points.length}개뿐이라 계산하기엔 부족합니다(최소 3개).</span></div>\`
        : \`<div style="margin:10px 0 4px;"><b>\${title}</b> <span style="color:var(--text-muted); font-size:12px;">\${note}</span></div>\` + renderCorrTable(points, targets);

      wrapEl.innerHTML =
        section('연간 (사업보고서 기준)', \`n=\${annualPts.length}개 연도\`, annualPts, annualTargets) +
        section('분기 (TTM·trailing 4분기 기준)', \`n=\${quarterPts.length}개 분기 — 매출·순이익·ROE는 각 분기를 끝점으로 한 최근 4개 분기 합산(TTM)값, 성장률은 1년 전 같은 분기 TTM 대비\`, quarterPts, quarterTargets);
      statusEl.textContent = \`연간 \${annualPts.length}개 · 분기 \${quarterPts.length}개 기준 (상관계수는 -1~1, |0.5| 이상만 색으로 강조). 분기 TTM은 인접 분기끼리 4개 중 3개 분기가 겹쳐서 실질 표본은 n보다 훨씬 적습니다 — 표본이 적어 참고용입니다.\`;
      wrapEl.style.display = 'block';
    }

    // 다음 공시가 "바로 다음 기간"인지(중간에 빠진 기간이 없는지) 확인해야 주가수익률이 정확히 한 기간치가 된다.
    // 연간: 연도 +1, 분기: 분기 인덱스(연도*4+분기) +1.
    function nextPeriodReturn(r, next, idxOf) {
      if (!next || next.price_at_filing == null || !r.price_at_filing) return null;
      if (idxOf(next) !== idxOf(r) + 1) return null;
      return next.price_at_filing / r.price_at_filing - 1;
    }

    function macroValuesAt(macro, ymd) {
      const out = {};
      for (const key of Object.keys(MACRO_LABELS)) out[key] = macroValueAsOf(macro[key], ymd, MACRO_MAX_GAP_DAYS);
      return out;
    }

    // 연간: 사업보고서 공시 시점마다 (그 시점 거시값, 그 해 실적, 다음 공시까지 주가수익률) 한 점.
    function buildAnnualCorrPoints(rowsIn, macro) {
      const annual = toAnnualRows(rowsIn).filter((r) => r.filing_date && r.price_at_filing != null);
      const ni = (x) => (x.parent_net_income != null ? x.parent_net_income : x.net_income);
      const points = [];
      for (let i = 0; i < annual.length; i++) {
        const r = annual[i];
        const prev = i > 0 && Number(annual[i - 1].bsns_year) === Number(r.bsns_year) - 1 ? annual[i - 1] : null;
        const next = i < annual.length - 1 ? annual[i + 1] : null;
        let revenueGrowth = null, netIncomeGrowth = null, roe = null;
        if (prev && r.revenue != null && prev.revenue) revenueGrowth = (r.revenue - prev.revenue) / Math.abs(prev.revenue);
        if (prev && ni(r) != null && ni(prev)) netIncomeGrowth = (ni(r) - ni(prev)) / Math.abs(ni(prev));
        if (prev) {
          const roeResult = computeROEAvg(r, prev);
          roe = roeResult ? roeResult.value : null;
        }
        const forwardReturn = nextPeriodReturn(r, next, (x) => Number(x.bsns_year));
        points.push({ label: r.period_label, filingDate: r.filing_date, revenueGrowth, netIncomeGrowth, roe, forwardReturn, macroValues: macroValuesAt(macro, r.filing_date) });
      }
      return points;
    }

    // 분기: 단독 분기값은 계절성·변동이 커서 거시지표와 비교하기 부적절하므로, 각 분기를 끝점으로 한
    // trailing 4분기 합산(TTM) 실적을 쓴다. 성장률은 1년 전 같은 분기의 TTM 대비(YoY), ROE는 5단계 분해(분기별)와 같은 계산.
    // TTM을 만들 4개 분기(또는 1년 전 비교분기)가 부족하면 그 항목만 비운다.
    function buildQuarterlyCorrPoints(rowsIn, macro) {
      const Q = toQuarterlyRows(rowsIn);
      const usable = Q.filter((r) => r.filing_date && r.price_at_filing != null);
      const ni = (x) => (x.parent_net_income != null ? x.parent_net_income : x.net_income);
      const qIdx = (x) => Number(x.bsns_year) * 4 + (Number(x.period_order) % 10);
      const points = [];
      for (let i = 0; i < usable.length; i++) {
        const r = usable[i];
        const next = i < usable.length - 1 ? usable[i + 1] : null;
        const N = Number(r.period_order) % 10;
        const Y = Number(r.bsns_year);
        const ttmCur = buildTTMFlowForQuarter(Q, r);
        const priorSameQ = Q.find((x) => x.bsns_year === String(Y - 1) && x.period_label === \`\${Y - 1} \${N}분기\`);
        const ttmPrev = priorSameQ ? buildTTMFlowForQuarter(Q, priorSameQ) : null;
        let revenueGrowth = null, netIncomeGrowth = null, roe = null;
        if (ttmCur && ttmPrev) {
          if (ttmCur.revenue != null && ttmPrev.revenue) revenueGrowth = (ttmCur.revenue - ttmPrev.revenue) / Math.abs(ttmPrev.revenue);
          if (ni(ttmCur) != null && ni(ttmPrev)) netIncomeGrowth = (ni(ttmCur) - ni(ttmPrev)) / Math.abs(ni(ttmPrev));
        }
        if (ttmCur && priorSameQ) {
          const s = computeStepMetrics(ttmCur, priorSameQ);
          roe = s.parentROE != null ? s.parentROE : s.roeCheck;
        }
        const forwardReturn = nextPeriodReturn(r, next, qIdx);
        points.push({ label: r.period_label, filingDate: r.filing_date, revenueGrowth, netIncomeGrowth, roe, forwardReturn, macroValues: macroValuesAt(macro, r.filing_date) });
      }
      return points;
    }

    // points: [{ macroValues:{key:value}, <target key>: value }] → 거시지표(행) × 실적/수익률(열) 상관계수 표
    function renderCorrTable(points, targets) {
      let html = '<table><tr><th>거시지표</th>' + targets.map((t) => \`<th>\${t.label}</th>\`).join('') + '</tr>';
      for (const macroKey of Object.keys(MACRO_LABELS)) {
        html += \`<tr><td>\${MACRO_LABELS[macroKey]}</td>\`;
        for (const t of targets) {
          const pairs = points.filter((p) => p.macroValues[macroKey] != null && p[t.key] != null);
          const corr = pearsonCorrelation(pairs.map((p) => p.macroValues[macroKey]), pairs.map((p) => p[t.key]));
          const text = corr == null ? \`N/A(n=\${pairs.length})\` : \`\${corr.toFixed(2)} (n=\${pairs.length})\`;
          const color = corr == null ? '#94a3b8' : (Math.abs(corr) >= 0.5 ? (corr > 0 ? '#16a34a' : '#dc2626') : '#64748b');
          html += \`<td style="color:\${color}; font-weight:600;">\${text}</td>\`;
        }
        html += '</tr>';
      }
      return html + '</table>';
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
  parent_net_income: ["IS", "CIS"], pretax_income: ["IS", "CIS"], interest_expense: ["IS", "CIS", "CF"],
  ocf: ["CF"], capex_ppe: ["CF"], capex_intangible: ["CF"],
  current_assets: ["BS"], current_liabilities: ["BS"], long_term_borrowings: ["BS"], bonds: ["BS"],
  depreciation: ["CF"], amortization: ["CF"], dividends_paid: ["CF"], buyback: ["CF"], debt_repay: ["CF"], debt_issue: ["CF"], acquisitions: ["CF"],
  total_equity: ["BS"], total_liabilities: ["BS"], cash: ["BS"], st_financial_assets: ["BS"],
  receivables: ["BS"], inventory: ["BS"], payables: ["BS"],
  short_term_trading_securities: ["BS"], fvpl_financial_assets: ["BS"], fvoci_financial_assets: ["BS"], investment_property: ["BS"],
  other_receivables: ["BS"], short_term_loans: ["BS"], other_payables: ["BS"],
  short_term_borrowings: ["BS"], current_portion_lt_debt: ["BS"], current_lease_liabilities: ["BS"],
  tangible_assets: ["BS"], intangible_assets: ["BS"], right_of_use_assets: ["BS"], parent_equity: ["BS"],
};

function sumAccount(list, ids, names, sjOrder, exactOnly) {
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
  if (!dart || dart.status !== "000") return { total_shares: null, treasury_shares: null };
  const norm = (s) => (s || "").replace(/\s/g, "");
  const row = dart.list.find((r) => norm(r.se) === "보통주");
  if (!row) return { total_shares: null, treasury_shares: null };
  // 이 기간에 "보통주" 공시 행 자체는 존재함 → 그 안의 개별 값이 "-"/빈칸이면
  // "미공시"가 아니라 "0주"라는 뜻(DART 표기 관행, 특히 자기주식이 없는 대다수 기업).
  // 행 자체가 없는 경우(1·3분기 미공시 등)만 위에서 null,null로 빠져 이월 로직을 타도록 둔다.
  const toCount = (v) => {
    const n = parseAmount(v);
    return n == null ? 0 : n;
  };
  return { total_shares: toCount(row.istc_totqy), treasury_shares: toCount(row.tesstk_co) };
}

function pickDividendPerShare(dart) {
  if (!dart || dart.status !== "000") return null;
  const row = dart.list.find((r) => r.se === "주당 현금배당금(원)" && r.stock_knd === "보통주");
  return row ? parseAmount(row.thstrm) : null;
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
  ktb3y:  { source: "ecos", statCode: "817Y002", itemCode: "010200000", label: "국고채(3년)" },
  ktb10y: { source: "ecos", statCode: "817Y002", itemCode: "010210000", label: "국고채(10년)" },
  wti:    { source: "fred", seriesId: "DCOILWTICO", label: "WTI 현물가" },
};

// ECOS StatisticSearch API: 한 번 호출로 기간 전체(start~end)를 받아온다.
// https://ecos.bok.or.kr/api/StatisticSearch/{키}/json/kr/{시작행}/{끝행}/{통계표코드}/D/{시작일}/{종료일}/{항목코드}
async function fetchEcosSeries(statCode, itemCode, startDate, endDate, apiKey, timeoutMs = 15000) {
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
        ? await fetchEcosSeries(def.statCode, def.itemCode, effectiveStart, endDate, env.ECOS_API_KEY)
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
    .prepare("SELECT total_shares, treasury_shares, period_label FROM financial_raw WHERE corp_code = ? AND period_order < ? AND total_shares IS NOT NULL ORDER BY period_order DESC LIMIT 1")
    .bind(corpCode, beforeOrder)
    .first();
  return row || null;
}

const REPRT_CODE_BY_Q = { 1: "11013", 2: "11012", 3: "11014", 4: "11011" };

async function getStoredPeriod(db, corpCode, year, reprtCode, keys) {
  if (!db) return null;
  const row = await db
    .prepare(`SELECT ${keys.join(",")} FROM financial_raw WHERE corp_code = ? AND bsns_year = ? AND reprt_code = ?`)
    .bind(corpCode, String(year), reprtCode)
    .first();
  return row || null;
}

// 사업보고서(연간)의 thstrm_amount는 "1년 누적"이라, 4분기만 떼어내려면
// 연간 - (1분기+2분기+3분기 단독값)을 계산해야 한다 (화면 쪽 toQuarterlyRows와 같은 원리, DB 조회 버전)
async function getIsolatedQ4(db, corpCode, year, keys) {
  const annual = await getStoredPeriod(db, corpCode, year, "11011", keys);
  if (!annual) return null;
  const q1 = await getStoredPeriod(db, corpCode, year, "11013", keys);
  const q2 = await getStoredPeriod(db, corpCode, year, "11012", keys);
  const q3 = await getStoredPeriod(db, corpCode, year, "11014", keys);
  const result = {};
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
async function getTTMFlow(db, corpCode, year, quarterNum, keys) {
  if (quarterNum === 4) return await getStoredPeriod(db, corpCode, year, "11011", keys);

  const parts = [];
  for (let k = 1; k <= quarterNum; k++) parts.push(await getStoredPeriod(db, corpCode, year, REPRT_CODE_BY_Q[k], keys));
  for (let k = quarterNum + 1; k <= 4; k++) {
    parts.push(k === 4 ? await getIsolatedQ4(db, corpCode, year - 1, keys) : await getStoredPeriod(db, corpCode, year - 1, REPRT_CODE_BY_Q[k], keys));
  }
  if (parts.some((p) => !p)) return null;

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
      total_shares: null, treasury_shares: null, dividend_per_share: null,
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
    if (dart.status !== "000") {
      fsDiv = "OFS";
      dart = await fetchDartWithRetry(corpCode, period.year, period.code, fsDiv, proxyUrl);
    }
    if (dart.status !== "000") return emptyRow();

    const vals = {};
    for (const item of ACCOUNT_ITEMS) {
      vals[item.key] = sumAccount(dart.list, item.ids, item.names, SJ_BY_KEY[item.key] || ["BS", "IS", "CIS", "CF"], item.exact);
      // 현금유출 항목은 공시 부호가 회사마다 다르므로 절댓값으로 통일(양수 = 유출 규모)
      if (item.exact && vals[item.key] != null && ["dividends_paid", "buyback", "debt_repay", "debt_issue", "acquisitions"].includes(item.key)) vals[item.key] = Math.abs(vals[item.key]);
    }

    const capex = (vals.capex_ppe != null || vals.capex_intangible != null)
      ? Math.abs(vals.capex_ppe || 0) + Math.abs(vals.capex_intangible || 0)
      : null;
    const fcf = vals.ocf != null && capex != null ? vals.ocf - capex : null;

    let stockCounts = { total_shares: null, treasury_shares: null };
    let sharesSource = null;
    let dividendPerShare = null;
    try {
      const stockDart = await fetchDartGeneric("stockTotqySttus", corpCode, period.year, period.code, proxyUrl);
      stockCounts = pickStockCounts(stockDart);
      if (stockCounts.total_shares != null) sharesSource = "공시";
    } catch (e) { /* 실패해도 나머지는 살림 */ }

    // 1·3분기 등은 주식총수현황이 공시되지 않는 경우가 많음 → 가장 최근 공시된 이전 기간 값을 이월
    // (자사주 매입/신주발행 등 중간 변동이 있었다면 다소 부정확할 수 있음 — 그래서 출처를 별도 표시)
    if (stockCounts.total_shares == null) {
      const fallback = await getFallbackShareCounts(db, corpCode, period.year * 10 + period.order);
      if (fallback) {
        stockCounts = { total_shares: fallback.total_shares, treasury_shares: fallback.treasury_shares };
        sharesSource = `이월(${fallback.period_label})`;
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
      const filingDate = rceptNo.slice(0, 8);
      row.filing_date = filingDate;
      try {
        const prices = await fetchNaverPrices(stockCode, addDaysStr(filingDate, -15), filingDate, proxyUrl);
        const price = closeAsOf(prices, filingDate);
        row.price_at_filing = price;

        const outstandingRaw = (stockCounts.total_shares != null && stockCounts.treasury_shares != null)
          ? stockCounts.total_shares - stockCounts.treasury_shares
          : null;
        const outstanding = outstandingRaw > 0 ? outstandingRaw : null; // 0/음수(데이터 이상)는 나눗셈 방지용으로 null 처리
        const equityForBps = vals.parent_equity != null ? vals.parent_equity : vals.total_equity;

        // PER·FCF Yield·ROA·PEG는 그 분기 하나만의 값이 아니라 TTM(최근 4개 분기 합산)을 씀 — 1·2·3분기도 "1년치" 기준이 되도록
        const quarterNum = period.order;
        const ttm = await getTTMFlow(db, corpCode, period.year, quarterNum, ["net_income", "parent_net_income", "fcf"]);
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
          const ttmPrior = await getTTMFlow(db, corpCode, period.year - 1, quarterNum, ["net_income", "parent_net_income"]);
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
const FIN_EXTRA_COLS = ["current_assets", "current_liabilities", "long_term_borrowings", "bonds", "depreciation", "amortization", "dividends_paid", "buyback", "debt_repay", "debt_issue", "acquisitions", "intangible_capex"];
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

    if (pathname === "/api/market/top30") {
      // 예전 Top30은 가격 원본 전체를 읽어(수십만 행) 무료 읽기 한도를 크게 쓰고 Worker CPU 한도에도 걸릴 수 있어
      // 중단했다. 같은 기능이 /api/screen/*(SQL 집계 + 저장된 스냅샷)으로 대체됐다.
      return Response.json({ error: "이 기능은 '주간 Top 50 스크리닝'으로 대체되었습니다(/api/screen/top)." }, { status: 410 });
    }

    if (pathname === "/api/screen/top") {
      const snap = await loadScreenSnapshot(env, searchParams.get("date"));
      if (!snap) return Response.json({ error: "저장된 스크리닝 결과가 없습니다. 아래에서 조건을 입력해 '② 지금 계산'을 누르거나(\"결과 저장\" 체크 시 저장), 토요일 새벽 1시 자동 저장을 기다려주세요." }, { status: 404 });
      return Response.json(snap);
    }

    // 사용자가 입력한 조건으로 지금 계산(기본은 저장하지 않음 = 쓰기 0건). save=1이면 주간 결과로 저장한다.
    if (pathname === "/api/screen/run" || pathname === "/api/screen/refresh") {
      const num = (k) => { const raw = searchParams.get(k); if (raw === null || raw === "") return null; const v = Number(raw); return Number.isFinite(v) ? v : null; };
      let kospiError = null;
      try { await refreshKospi(env); } catch (e) { kospiError = (e && e.message) || String(e); }
      const result = await computeScreenLists(env, { minMcapEok: num("mcap"), minTvEok: num("tv"), w20: num("w20"), w60: num("w60"), w120: num("w120") });
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
// 스크리닝 v2 — 학계에서 많이 검증된 요인(모멘텀 계열)으로 매주 Top50 + 재무지표 병기
// 설계 근거(자세한 설명은 화면의 "방법론" 접이식 안내 참고):
//  - 중기 모멘텀 6-1: Jegadeesh & Titman(1993). 최근 1개월은 건너뜀(단기 반전: Jegadeesh 1990).
//  - 고점 근접도: George & Hwang(2004).  - 모멘텀 연속성(Frog-in-the-pan): Da·Gurun·Warachka(2014).
//  - 변동성 조정 모멘텀: Barroso & Santa-Clara(2015).
//  - 절대/상대 모멘텀 필터와 시장 추세: Antonacci(2014 Dual Momentum), Faber(2007).
//  - 재무(밸류·퀄리티)는 Top50에 "함께 표시"해서 직접 판단(Asness·Moskowitz·Pedersen 2013, Novy-Marx 2013).
// 무료 한도 설계: 계산은 SQL 한 번(읽기 약 수십만 행)으로 끝내고 결과(51행)만 저장한다. 평소 조회는 저장분만 읽는다.
// ============================================================
// 주간 자동 스크리닝 시각: 토요일 01:00 한국시간 = 금요일 16:00 UTC (getUTCDay 5 = 금요일)
const WEEKLY_SCREEN_UTC_DOW = 5;
const WEEKLY_SCREEN_UTC_HOUR = 16;
const KOSPI_INDEX_API = "https://apis.data.go.kr/1160100/GetMarketIndexInfoService_V2/getStockMarketIndex_V2";
const KOSPI_NAME = "코스피";
const SCREEN_TOP_N = 50;
const SCREEN_DEFAULT_MCAP_EOK = 600;  // 시가총액 하한 기본값(억원)
const SCREEN_DEFAULT_TV_EOK = 15;     // 거래대금 하한 기본값(억원)
const SCREEN_DEFAULT_WEIGHTS = { w20: 0.4, w60: 0.3, w120: 0.3 };
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
async function computeScreenLists(env, opts) {
  opts = opts || {};
  await ensureScreenTables(env.DB);
  await ensureMarketQueueColumns(env.DB); // market_cap 컬럼 보장

  const clamped = [];
  let minMcap = Number(opts.minMcapEok);
  if (!Number.isFinite(minMcap) || minMcap <= 0) minMcap = SCREEN_DEFAULT_MCAP_EOK;
  let minTv = Number(opts.minTvEok);
  if (!Number.isFinite(minTv) || minTv <= 0) minTv = SCREEN_DEFAULT_TV_EOK;
  const floorMcapEok = MARKET_MIN_MCAP / 1e8;
  const floorTvEok = MARKET_MIN_TRADING_VALUE / 1e8;
  if (minMcap < floorMcapEok) { clamped.push("시가총액 하한을 " + floorMcapEok + "억원으로 올렸습니다(저장된 종목은 그 이상만 있음)."); minMcap = floorMcapEok; }
  if (minTv < floorTvEok) { clamped.push("거래대금 하한을 " + floorTvEok + "억원으로 올렸습니다(저장된 종목은 그 이상만 있음)."); minTv = floorTvEok; }

  let w20 = Number(opts.w20), w60 = Number(opts.w60), w120 = Number(opts.w120);
  if (![w20, w60, w120].every((x) => Number.isFinite(x) && x >= 0) || w20 + w60 + w120 <= 0) {
    ({ w20, w60, w120 } = SCREEN_DEFAULT_WEIGHTS);
    if (opts.w20 != null || opts.w60 != null || opts.w120 != null) clamped.push("가중치가 올바르지 않아 기본값(20일 0.4 / 60일 0.3 / 120일 0.3)을 썼습니다.");
  }
  const wSum = w20 + w60 + w120;
  const nw = { w20: w20 / wSum, w60: w60 / wSum, w120: w120 / wSum };

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

  // 4) 종목 집계: 필요한 7개 날짜 행만 읽어(기본키 앞부분 market_date로 바로 찾음) 한 번에 피벗·수익률·순위 상위 50을 만든다.
  const sql =
    "WITH m AS MATERIALIZED (" +
    " SELECT stock_code AS sc," +
    "  MAX(CASE WHEN market_date = ?1 THEN close_price END) AS c0," +
    "  MAX(CASE WHEN market_date = ?1 THEN trading_value END) AS tv," +
    "  MAX(CASE WHEN market_date = ?1 THEN market_cap END) AS mc," +
    "  MAX(CASE WHEN market_date = ?2 THEN close_price END) AS c20," +
    "  MAX(CASE WHEN market_date = ?3 THEN close_price END) AS c21," +
    "  MAX(CASE WHEN market_date = ?4 THEN close_price END) AS c60," +
    "  MAX(CASE WHEN market_date = ?5 THEN close_price END) AS c63," +
    "  MAX(CASE WHEN market_date = ?6 THEN close_price END) AS c120," +
    "  MAX(CASE WHEN market_date = ?7 THEN close_price END) AS c126" +
    " FROM market_raw_daily WHERE market_date IN (?1,?2,?3,?4,?5,?6,?7)" +
    " GROUP BY stock_code HAVING c0 > 0 AND tv >= ?8 AND mc >= ?9" +
    "), n AS MATERIALIZED (" +
    " SELECT m.*, u.corp_name AS nm," +
    "  c0 * 1.0 / NULLIF(c20, 0) - 1 AS r20," +
    "  c0 * 1.0 / NULLIF(c60, 0) - 1 AS r60," +
    "  c0 * 1.0 / NULLIF(c120, 0) - 1 AS r120," +
    "  CASE WHEN c20 > 0 AND c60 > 0 AND c120 > 0 THEN ?10 * (c0 * 1.0 / c20 - 1) + ?11 * (c0 * 1.0 / c60 - 1) + ?12 * (c0 * 1.0 / c120 - 1) END AS rw," +
    "  c21 * 1.0 / NULLIF(c126, 0) - 1 AS m16," +
    "  CASE WHEN c21 > 0 THEN (c0 * 1.0 / c21) / ?15 END AS rs1," +
    "  CASE WHEN c63 > 0 THEN (c0 * 1.0 / c63) / ?13 END AS rs3," +
    "  CASE WHEN c126 > 0 THEN (c0 * 1.0 / c126) / ?14 END AS rs6" +
    " FROM m LEFT JOIN market_universe_status u ON u.stock_code = m.sc" +
    "), k AS (" +
    // D1은 UNION ALL(복합 SELECT) 항 수 제한이 작아(약 5개) 7개를 이어 붙이면 오류가 난다 → 윈도 함수로 목록별 순위를 한 번에 매긴다.
    " SELECT n.*, " + SCREEN_LIST_KEYS.map((key) => "CASE WHEN " + key + " IS NOT NULL THEN ROW_NUMBER() OVER (ORDER BY " + key + " DESC) END AS rk_" + key).join(", ") + " FROM n" +
    ") SELECT * FROM k WHERE " + SCREEN_LIST_KEYS.map((key) => "rk_" + key + " <= " + SCREEN_TOP_N).join(" OR ");
  const { results: agg } = await env.DB.prepare(sql)
    .bind(pts.d0, pts.d20, pts.d21, pts.d60, pts.d63, pts.d120, pts.d126, minTv * 1e8, minMcap * 1e8, nw.w20, nw.w60, nw.w120, k0 / k63, k0 / k126, k0 / k21).all();

  const lists = {};
  for (const k of SCREEN_LIST_KEYS) lists[k] = [];
  for (const key of SCREEN_LIST_KEYS) {
    lists[key] = agg
      .filter((r) => r["rk_" + key] != null && r["rk_" + key] <= SCREEN_TOP_N)
      .sort((a, b) => a["rk_" + key] - b["rk_" + key])
      .map((r) => ({
        stock_code: r.sc, corp_name: r.nm || r.sc, c0: r.c0,
        tvEok: r.tv / 1e8, mcapEok: r.mc / 1e8,
        r20: r.r20, r60: r.r60, r120: r.r120, rw: r.rw, m16: r.m16, rs1: r.rs1, rs3: r.rs3, rs6: r.rs6,
      }));
  }

  // 5) 참고 집계: 기준일 행 수, 시가총액 미수집 수, 조건 통과 수(기준일 행만 읽음 ≈ 1,300행)
  const meta0 = await env.DB.prepare(
    "SELECT COUNT(*) AS total, SUM(CASE WHEN market_cap IS NULL THEN 1 ELSE 0 END) AS nomc, SUM(CASE WHEN market_cap >= ?2 AND trading_value >= ?3 THEN 1 ELSE 0 END) AS pass FROM market_raw_daily WHERE market_date = ?1"
  ).bind(pts.d0, minMcap * 1e8, minTv * 1e8).first();

  const bfRow = await env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled = 1").first();
  const meta = {
    backfilled: bfRow ? bfRow.c : null,
    asof: pts.d0, computed_at: new Date().toISOString(), points: pts, kospi,
    minMcapEok: minMcap, minTvEok: minTv, weights: nw, clamped,
    asof_rows: meta0 ? meta0.total : null, no_mcap: meta0 ? meta0.nomc : null, passed_floor: meta0 ? meta0.pass : null,
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
  const result = await computeScreenLists(env, {}); // 기본값: 시총 600억, 거래대금 15억, 가중치 0.4/0.3/0.3
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

    // 매주 토요일 새벽 1시(한국) = 금요일 16:00(UTC): 기본 조건(시총 600억·거래대금 15억)으로 스크리닝하고 저장한다.
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
