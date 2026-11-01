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
  "price_next_day", "price_next_day_date",
  "updated_at",
];

// 계산 엔진 함수의 원문을 화면 스크립트에 그대로 넣어 서버와 같은 코드를 쓴다(복붙으로 두 벌이 어긋나는 것을 막음).
const PF_CLIENT_FUNCS = [pfLedger, pfSummary, pfSizePosition, pfSplitTranches, pfTradeWarnings, pfBehavior, pfKelly].map((f) => f.toString()).join("\n");
const HTML_PAGE = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <meta name="color-scheme" content="light" />
  <meta name="theme-color" content="#2563eb" />
  <meta name="mobile-web-app-capable" content="yes" />
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <meta name="apple-mobile-web-app-title" content="VALUELAB" />
  <meta name="apple-mobile-web-app-status-bar-style" content="default" />
  <meta name="format-detection" content="telephone=no" />
  <link rel="manifest" href="/manifest.webmanifest" />
  <link rel="apple-touch-icon" href="/apple-touch-icon.png" />
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
    #freshBar { font-size: 12px; line-height: 1.5; padding: 5px 10px; margin: 0 0 8px; border-radius: 8px; border: 1px solid var(--border); background: #f8fafc; color: var(--text-muted); cursor: pointer; }
    #freshBar.warn { background: #fffbeb; border-color: #fcd34d; color: #92400e; }
    #freshBar.err { background: #f1f5f9; }
    .grpnav { display: flex; gap: 6px; position: sticky; top: var(--navh); z-index: 19; background: var(--bg); padding: 2px 0 8px; margin: -4px 0 8px; overflow-x: auto; flex-wrap: nowrap; border-bottom: 1px solid var(--border); }
    .grpnav button { flex: 0 0 auto; padding: 7px 14px; font-size: 13px; font-weight: 600; border-radius: 999px; white-space: nowrap; }
    @media (max-width: 760px) { .grpnav button { min-height: 40px; } }
    .nav button { flex: 1; padding: 10px 6px; font-size: 14px; font-weight: 600; white-space: nowrap; }
    .pfl { display: flex; flex-direction: column; font-size: 12px; color: var(--text-muted); gap: 3px; flex: 1; min-width: 150px; }
    .pfl input { width: 100%; }
    textarea { font-size: 14px; padding: 8px 10px; border: 1px solid var(--border); border-radius: 8px; font-family: inherit; }
    /* ---- 화면 이동 최소화(스크롤 줄이기) ---- */
    :root { --navh: 48px; --subh: 0px; }
    .subnav.sticky { position: sticky; top: var(--navh); z-index: 16; background: var(--bg); padding: 6px 0; margin: 0 0 8px; overflow-x: auto; flex-wrap: nowrap; border-bottom: 1px solid var(--border); }
    .subnav.sticky button { white-space: nowrap; flex: 0 0 auto; }
    #searchCard.compact #comboHint_corpName:empty, #searchCard.compact #watchlistWrap, #searchCard.compact #savedBox, #searchCard.compact .adv, #searchCard.compact #stockMeta { display: none; }
    #searchCard.compact { padding: 8px 10px; margin-bottom: 8px; }
    .an-sticky { position: sticky; top: calc(var(--navh) + var(--subh)); z-index: 12; display: flex; flex-wrap: nowrap; gap: 6px; align-items: center; overflow-x: auto; padding: 6px 8px; margin: 0 0 8px; background: var(--bg-card); border: 1px solid var(--border); border-radius: 8px; font-size: 12px; }
    .an-sticky button { white-space: nowrap; flex: 0 0 auto; }
    #chartDock.folded #priceBox, #chartDock.folded #chartWrap, #chartDock.folded #dockViewRow { display: none !important; }
    #analysis > * { scroll-margin-top: calc(var(--navh) + var(--subh) + 52px); }
    #fabTop { position: fixed; right: 12px; bottom: 16px; z-index: 40; width: 44px; height: 44px; border-radius: 50%; padding: 0; font-size: 18px; box-shadow: 0 4px 14px rgba(15,23,42,.25); display: none; background: #0f172a; color: #fff; border: 0; }
    @media (max-width: 560px) { #priceCanvas { height: 340px !important; } }
    /* ---- 내 계좌 ---- */
    .pf-kpi { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
    .pf-kpi > div { background: #f8fafc; border: 1px solid var(--border); border-radius: 10px; padding: 8px 10px; }
    .pf-kpi small { display: block; font-size: 11px; color: var(--text-muted); }
    .pf-kpi b { font-size: 17px; }
    .pf-pos { color: #16a34a; } .pf-neg { color: #dc2626; }
    .pf-chip { display: inline-block; font-size: 12.5px; padding: 5px 10px; border-radius: 999px; margin: 3px 4px 3px 0; background: #fffbeb; border: 1px solid #fcd34d; color: #92400e; }
    .pf-chip.bad { background: #fef2f2; border-color: #fca5a5; color: #991b1b; }
    .pf-chip.info { background: #eff6ff; border-color: #bfdbfe; color: #1e40af; }
    .pfc { border: 1px solid var(--border); border-radius: 12px; padding: 10px 12px; margin-bottom: 10px; background: #fff; }
    .pfc.flash { animation: pfflash 2.2s ease-out; }
    @keyframes pfflash { 0% { background: #dcfce7; } 100% { background: #fff; } }
    .pfc-h { display: flex; align-items: baseline; gap: 8px; flex-wrap: wrap; }
    .pfc-bar { position: relative; height: 8px; background: #e2e8f0; border-radius: 4px; margin: 8px 0 4px; }
    .pfc-bar i { position: absolute; left: 0; top: 0; bottom: 0; background: #2563eb; border-radius: 4px; }
    .pfc-bar i.over { background: #dc2626; }
    .pfc-bar u { position: absolute; top: -3px; bottom: -3px; width: 2px; background: #0f172a; }
    .pfc-m { font-size: 12.5px; color: var(--text-muted); line-height: 1.55; }
    .pfc .row button { padding: 6px 12px; font-size: 13px; }
    #pfuFab { position: fixed; right: 14px; bottom: 70px; z-index: 45; border-radius: 999px; padding: 12px 18px; font-size: 15px; font-weight: 700; background: var(--primary); color: #fff; border: 0; box-shadow: 0 6px 18px rgba(37,99,235,.4); display: none; }
    .pf-sheet { position: fixed; inset: 0; z-index: 60; display: none; }
    .pf-sheet-bg { position: absolute; inset: 0; background: rgba(15,23,42,.45); }
    .pf-sheet-body { position: absolute; left: 0; right: 0; bottom: 0; max-width: 640px; margin: 0 auto; max-height: 92vh; overflow-y: auto; background: #fff; border-radius: 16px 16px 0 0; padding: 14px 14px 22px; }
    .seg { display: flex; gap: 6px; margin: 8px 0; }
    .seg button { flex: 1; padding: 9px 4px; font-size: 14px; font-weight: 700; }
    .seg button.on.buy { background: #dc2626; color: #fff; border-color: #dc2626; }
    .seg button.on.sell { background: #2563eb; color: #fff; border-color: #2563eb; }
    .seg button.on.cash { background: #0f172a; color: #fff; border-color: #0f172a; }
    .chips { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 8px; }
    .chips button { padding: 5px 10px; font-size: 13px; border-radius: 999px; }
    .chips button.on { background: #0f172a; color: #fff; border-color: #0f172a; }
    .pf-prev { font-size: 13px; line-height: 1.6; background: #f8fafc; border: 1px solid var(--border); border-radius: 8px; padding: 8px 10px; margin: 8px 0; min-height: 20px; }
    #pfuToast { position: fixed; left: 12px; right: 12px; bottom: 24px; max-width: 520px; margin: 0 auto; z-index: 70; background: #0f172a; color: #fff; padding: 10px 14px; border-radius: 10px; font-size: 13px; line-height: 1.5; display: none; }
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
    .scr .f2 { left: 42px; min-width: 104px; max-width: 104px; text-align: left; white-space: normal; overflow: visible; }
    .scr .f3 { left: 146px; min-width: 80px; max-width: 80px; }
    .scr .f4 { left: 226px; min-width: 80px; max-width: 80px; }
    .scr .f5 { left: 306px; min-width: 96px; max-width: 96px; }
    .scr .f6 { left: 402px; min-width: 104px; max-width: 104px; border-right: 2px solid #94a3b8; box-shadow: 3px 0 4px -2px rgba(15,23,42,.18); }
    /* 좁은 화면(폰)에서는 고정 영역이 화면을 다 차지하지 않도록 순위·종목만 고정한다. */
    .scr .mini { display: none; }
    @media (max-width: 760px) {
      .scr th.f3, .scr th.f4, .scr th.f5, .scr th.f6, .scr td.f3, .scr td.f4, .scr td.f5, .scr td.f6 { display: none; }
      .scr .f1 { min-width: 34px; max-width: 34px; padding-left: 4px; padding-right: 4px; text-align: center; }
      .scr .f2 { left: 34px; min-width: 150px; max-width: 150px; white-space: normal; overflow: visible; border-right: 2px solid #94a3b8; box-shadow: 3px 0 4px -2px rgba(15,23,42,.25); }
      .scr th.f2 { box-shadow: 3px 0 4px -2px rgba(15,23,42,.25); }
      .scr .mini { display: block; font-size: 12px; line-height: 1.45; color: var(--text-muted); margin-top: 2px; font-weight: 400; }
      .scr .mini b { color: var(--text); font-weight: 600; }
      .tscroll, #wrap { overscroll-behavior: contain; -webkit-overflow-scrolling: touch; }
      .scr td { vertical-align: top; }
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
    .combo { position: relative; flex: 1; min-width: 170px; display: block; }
    .combo input { width: 100%; box-sizing: border-box; }
    .combo-list { position: absolute; left: 0; right: 0; top: 100%; z-index: 30; margin-top: 2px; background: var(--card-bg, #fff); border: 1px solid var(--border); border-radius: 10px; box-shadow: 0 8px 24px rgba(15,23,42,.14); max-height: 340px; overflow-y: auto; display: none; }
    .combo-item { display: flex; align-items: center; gap: 8px; padding: 9px 12px; cursor: pointer; font-size: 14px; border-bottom: 1px solid #f1f5f9; }
    .combo-item:last-child { border-bottom: 0; }
    .combo-item.act, .combo-item:hover { background: #eff6ff; }
    .combo-item .cc { color: var(--text-muted); font-size: 12px; }
    .combo-item .bd { margin-left: auto; font-size: 11px; padding: 2px 7px; border-radius: 999px; white-space: nowrap; }
    .combo-item .bd.ok { background: #dcfce7; color: #166534; }
    .combo-item .bd.no { background: #f1f5f9; color: #64748b; }
    .combo-hint { font-size: 12.5px; margin-top: 6px; min-height: 18px; color: var(--text-muted); line-height: 1.5; }
    .combo-hint .good { color: #166534; font-weight: 600; }
    .combo-hint .bad { color: #b45309; font-weight: 600; }
    .saved-chip { border: 1px solid var(--border); background: #fff; border-radius: 999px; padding: 4px 10px; font-size: 12.5px; cursor: pointer; margin: 0 4px 6px 0; }
    .saved-chip:hover { background: #eff6ff; }
    .saved-chip small { color: var(--text-muted); margin-left: 4px; }
    .saved-chip.recent { border-style: dashed; }
    .watchlist-chip .x { color: var(--text-muted); font-weight: 700; }
    /* ================= 아이폰·모바일 최적화 ================= */
    html { -webkit-text-size-adjust: 100%; text-size-adjust: 100%; color-scheme: light; }
    body { padding-left: max(12px, env(safe-area-inset-left)); padding-right: max(12px, env(safe-area-inset-right)); -webkit-tap-highlight-color: rgba(37,99,235,.15); }
    button, input, select, textarea, summary, .combo-item, label { touch-action: manipulation; }
    #bnav { display: none; position: fixed; left: 0; right: 0; bottom: 0; z-index: 50; background: rgba(255,255,255,.97); border-top: 1px solid var(--border); padding: 0 env(safe-area-inset-right) env(safe-area-inset-bottom) env(safe-area-inset-left); -webkit-backdrop-filter: blur(8px); backdrop-filter: blur(8px); }
    #bnav button { flex: 1; border: 0; background: transparent; border-radius: 0; min-height: 56px; padding: 6px 2px 4px; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 2px; font-size: 11.5px; font-weight: 700; color: var(--text-muted); }
    #bnav button .ic { font-size: 21px; line-height: 1; }
    #bnav button.on { color: var(--primary); }
    #moreSheet { position: fixed; inset: 0; z-index: 62; display: none; }
    #moreSheet .ms-bg { position: absolute; inset: 0; background: rgba(15,23,42,.45); }
    #moreSheet .ms-body { position: absolute; left: 0; right: 0; bottom: 0; max-width: 640px; margin: 0 auto; background: #fff; border-radius: 16px 16px 0 0; padding: 12px 14px calc(18px + env(safe-area-inset-bottom)); }
    #moreSheet .ms-body button { display: flex; align-items: center; gap: 12px; width: 100%; text-align: left; min-height: 54px; font-size: 16px; font-weight: 600; margin-top: 8px; padding: 10px 14px; }
    #moreSheet .ms-body button.on { background: var(--primary); color: #fff; border-color: var(--primary); }
    #lockScreen { position: fixed; inset: 0; z-index: 100; background: var(--bg); display: none; align-items: center; justify-content: center; padding: 20px; }
    #lockScreen .lk-box { width: 100%; max-width: 380px; background: #fff; border: 1px solid var(--border); border-radius: 16px; padding: 22px 18px; box-shadow: 0 10px 30px rgba(15,23,42,.12); }
    #lockScreen .lk-ico { font-size: 34px; text-align: center; }
    #lockScreen .lk-t { font-size: 18px; font-weight: 800; text-align: center; margin: 6px 0 4px; }
    #lockScreen .lk-s { font-size: 13px; color: var(--text-muted); line-height: 1.55; margin-bottom: 12px; text-align: center; }
    #lockScreen input[type=text] { width: 100%; -webkit-text-security: disc; }
    #lockScreen .lk-rem { display: flex; align-items: center; gap: 8px; font-size: 13.5px; margin: 10px 0; }
    #lockScreen .lk-rem input { width: 20px; height: 20px; }
    #lockScreen button.primary { width: 100%; min-height: 48px; font-size: 16px; font-weight: 700; }
    #lkMsg { font-size: 13px; color: var(--danger); min-height: 18px; margin-top: 8px; text-align: center; line-height: 1.5; }
    #privCover { position: fixed; inset: 0; z-index: 110; background: #0f172a; color: #fff; display: none; align-items: center; justify-content: center; font-size: 18px; font-weight: 800; }
    .stickybtn { position: sticky; bottom: 0; background: #fff; padding: 10px 0 2px; z-index: 2; }
    /* 태블릿 등 터치 기기(폭이 넓어 위 모바일 규칙이 안 걸리는 경우): 입력은 16px(자동 확대 방지)·터치 가능한 크기 */
    @media (pointer: coarse) and (min-width: 761px) {
      input:not([type=checkbox]):not([type=radio]):not([type=range]), select, textarea { font-size: 16px; min-height: 42px; }
      button { min-height: 40px; }
    }
    @media (max-width: 760px) {
      #bnav { display: flex; }
      #mainNav { display: none; }
      body { padding-bottom: calc(84px + env(safe-area-inset-bottom)); font-size: 15px; line-height: 1.55; }
      input:not([type=checkbox]):not([type=radio]):not([type=range]), select, textarea { font-size: 16px !important; min-height: 44px; }
      input[type=checkbox], input[type=radio] { width: 22px; height: 22px; }
      button { min-height: 42px; }
      button.primary, .seg button { min-height: 48px; }
      .chips button, .subnav button { min-height: 40px; }
      .combo-item { min-height: 46px; font-size: 15px; }
      .combo-item .cc { font-size: 13px; }
      .combo-hint, .meta-line, .pfc-m, .pfl { font-size: 13.5px; }
      .tscroll { -webkit-overflow-scrolling: touch; }
      [style*="font-size:11px"], [style*="font-size: 11px"] { font-size: 12.5px !important; }
      [style*="font-size:12px"], [style*="font-size: 12px"] { font-size: 13px !important; }
      [style*="font-size:12.5px"] { font-size: 13.5px !important; }
      [style*="font-size:13px"] { font-size: 14px !important; }
      #fabTop { bottom: calc(76px + env(safe-area-inset-bottom)); right: 12px; }
      #pfuFab { bottom: calc(128px + env(safe-area-inset-bottom)); right: 12px; padding: 14px 20px; font-size: 16px; }
      #pfuToast { bottom: calc(80px + env(safe-area-inset-bottom)); font-size: 14px; }
      .pf-sheet-body { max-height: calc(100vh - 12px); max-height: calc(100dvh - 12px); padding-bottom: calc(22px + env(safe-area-inset-bottom)); overscroll-behavior: contain; }
      #pfuSheet .row > input:not([type=checkbox]) { flex: 1 1 30%; min-width: 0; width: auto !important; }
      .pf-kpi b { font-size: 18px; }
      label { max-width: 100%; }
      details > summary { padding: 11px 0; }
      label select { display: block; width: 100%; max-width: 100%; margin-top: 4px; text-overflow: ellipsis; }
      select { max-width: 100%; }
    }
    @media (max-width: 380px) { #bnav button { font-size: 11px; } .pf-kpi { grid-template-columns: repeat(2, 1fr); } }
    /* ===== 표 도구 · 스크리닝 카드 · 종목 비교 ===== */
    .tblbar { display: none; align-items: center; gap: 6px; margin-top: 8px; flex-wrap: wrap; }
    .tblbar button { padding: 4px 12px; font-size: 14px; min-height: 40px; }
    .tblbar span { font-size: 12.5px; color: var(--text-muted); }
    .tfull { position: fixed !important; inset: 0; z-index: 95; max-height: none !important; margin: 0 !important; border-radius: 0 !important; border: 0 !important; background: #fff; padding-top: env(safe-area-inset-top); overflow: auto; }
    #tblClose { position: fixed; right: 12px; bottom: calc(16px + env(safe-area-inset-bottom)); z-index: 96; display: none; background: #0f172a; color: #fff; border: 0; border-radius: 999px; padding: 12px 20px; font-size: 15px; font-weight: 700; box-shadow: 0 6px 18px rgba(15,23,42,.35); }
    #dbBanner { display: none; background: #7f1d1d; color: #fff; padding: 10px 12px; border-radius: 10px; margin: 0 0 8px; font-size: 14px; line-height: 1.5; align-items: center; gap: 8px; flex-wrap: wrap; }
    #dbBanner button { min-height: 40px; padding: 2px 12px; font-size: 14px; border-radius: 999px; }
    .db-bar { height: 16px; background: #e2e8f0; border-radius: 8px; overflow: hidden; margin: 6px 0; }
    .db-bar i { display: block; height: 100%; background: #16a34a; border-radius: 8px; }
    .db-bar i.warn { background: #f59e0b; } .db-bar i.bad { background: #dc2626; }
    .db-big { font-size: 22px; font-weight: 800; }
    .sc-cards { display: none; }
    .sc-card { background: #fff; border: 1px solid var(--border); border-radius: 12px; padding: 10px 12px; margin-top: 8px; }
    .sc-h { display: flex; align-items: center; gap: 8px; }
    .sc-rk { min-width: 26px; height: 26px; padding: 0 6px; border-radius: 999px; background: #0f172a; color: #fff; font-size: 12px; font-weight: 800; display: flex; align-items: center; justify-content: center; }
    .sc-nm { font-weight: 800; font-size: 16px; color: var(--primary-dark); cursor: pointer; }
    .sc-cd { font-size: 12.5px; color: var(--text-muted); }
    .sc-big { margin-left: auto; text-align: right; line-height: 1.2; }
    .sc-big b { font-size: 19px; }
    .sc-big small { display: block; font-size: 11.5px; color: var(--text-muted); }
    .sc-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 6px; margin-top: 8px; }
    .sc-grid > div { background: #f8fafc; border-radius: 8px; padding: 5px 2px; text-align: center; font-size: 14px; font-weight: 700; }
    .sc-grid small { display: block; font-size: 11.5px; font-weight: 400; color: var(--text-muted); }
    .sc-sub { font-size: 13.5px; color: var(--text-muted); margin-top: 7px; line-height: 1.55; }
    .sc-act { display: flex; gap: 8px; margin-top: 8px; }
    .sc-act button { flex: 1; font-weight: 700; }
    .sc-fin { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; padding: 0 8px; white-space: nowrap; }
    .sc-fin input, .sc-fin-s input { width: 20px; height: 20px; margin: 0; }
    .sc-fin-s { display: inline-flex; align-items: center; gap: 3px; font-size: 12px; white-space: nowrap; }
    .sc-ctl { display: flex; align-items: center; gap: 6px; margin-top: 3px; white-space: nowrap; }
    .sc-cmp.on, .sc-cmp-s.on, #btnCmpAdd.on { background: #0f172a; color: #fff; border-color: #0f172a; }
    .sc-cmp-s { padding: 0 6px; min-height: 26px !important; font-size: 12px; margin-left: 4px; }
    .scr-view { display: flex; gap: 6px; margin-top: 8px; align-items: center; }
    .scr-view button { min-height: 40px; padding: 4px 12px; font-size: 14px; }
    #cmpTray { position: fixed; left: 8px; right: 8px; max-width: 640px; margin: 0 auto; bottom: calc(10px + env(safe-area-inset-bottom)); z-index: 48; display: none; align-items: center; gap: 6px; flex-wrap: wrap; background: #0f172a; color: #fff; border-radius: 14px; padding: 8px 10px; box-shadow: 0 8px 24px rgba(15,23,42,.35); }
    #cmpTray .ct-chip { display: inline-flex; align-items: center; gap: 2px; background: #1e293b; border-radius: 999px; padding: 2px 4px 2px 10px; font-size: 13.5px; }
    #cmpTray button { min-height: 36px; padding: 2px 10px; font-size: 14px; border-radius: 999px; }
    #cmpTray .ct-x { background: transparent; color: #cbd5e1; border: 0; padding: 0 8px; }
    #cmpTray .ct-go { background: var(--primary); color: #fff; border-color: var(--primary); font-weight: 700; margin-left: auto; }
    #cmpTray .ct-clr { background: transparent; color: #cbd5e1; border-color: #475569; }
    #cmpToast { position: fixed; left: 12px; right: 12px; max-width: 520px; margin: 0 auto; bottom: calc(90px + env(safe-area-inset-bottom)); z-index: 97; background: #7c2d12; color: #fff; padding: 10px 14px; border-radius: 10px; font-size: 14px; display: none; }
    .cmp-slot { margin-top: 6px; display: flex; gap: 6px; align-items: center; }
    .cmp-slot .combo { flex: 1; }
    .cmp-slot .cmp-x { min-width: 44px; }
    .cmp-best { font-weight: 800; background: #ecfdf5 !important; color: #047857; }
    #cmpWrap table { width: 100%; }
    #cmpWrap td, #cmpWrap th { white-space: normal; min-width: 68px; }
    #cmpWrap td:first-child, #cmpWrap th:first-child { min-width: 84px; }
    @media (max-width: 760px) {
      .tblbar { display: flex; }
      table { font-size: var(--tfs, 13px); }
      .an-tbl { font-size: var(--tfs, 13px); }
      #cmpWrap td:first-child, #cmpWrap th:first-child { max-width: 26vw; }
      th:first-child, td:first-child { max-width: 34vw; white-space: normal; box-shadow: 3px 0 4px -3px rgba(15,23,42,.35); }
      .an-tbl .an-first { min-width: 96px; max-width: 34vw; }
      td, th { padding: 8px 8px; }
      body.has-tray { padding-bottom: calc(150px + env(safe-area-inset-bottom)); }
      #cmpTray { bottom: calc(66px + env(safe-area-inset-bottom)); }
      #cmpToast { bottom: calc(150px + env(safe-area-inset-bottom)); }
      body.has-tray #fabTop { bottom: calc(138px + env(safe-area-inset-bottom)); }
      body.has-tray #pfuFab { bottom: calc(190px + env(safe-area-inset-bottom)); }
    }
  </style>
</head>
<body>
  <nav id="bnav" aria-label="주 메뉴">
    <button id="bn_find" onclick="switchGroup('find')"><span class="ic">🔎</span><span>발굴</span></button>
    <button id="bn_stock" onclick="switchGroup('stock')"><span class="ic">📈</span><span>종목</span></button>
    <button id="bn_my" onclick="switchGroup('my')"><span class="ic">💼</span><span>내 투자</span></button>
    <button id="bn_admin" onclick="switchGroup('admin')"><span class="ic">⚙</span><span>검증·관리</span></button>
    <button id="bn_more" onclick="toggleMore()"><span class="ic">☰</span><span>더보기</span></button>
  </nav>
  <div id="moreSheet">
    <div class="ms-bg" onclick="closeMore()"></div>
    <div class="ms-body">
      <b style="font-size:15px;">메뉴 더보기</b>
      <button onclick="closeMore(); cmpOpen()">⚖ 종목 비교 (최대 5)</button>
      <button onclick="closeMore(); appLock('잠겼습니다. 키를 입력하면 다시 열립니다.')">🔒 지금 잠그기</button>
    </div>
  </div>
  <div id="lockScreen" role="dialog" aria-modal="true" aria-label="접근 키 입력">
    <div class="lk-box">
      <div class="lk-ico">🔒</div>
      <div class="lk-t">접근 키 입력</div>
      <div class="lk-s">이 사이트는 본인만 쓰도록 잠겨 있습니다. Cloudflare에 등록한 <b>PF_KEY</b> 값을 입력하세요.</div>
      <input id="lkKey" type="text" lang="en" inputmode="text" name="pf-access-code" placeholder="접근 키" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false" enterkeyhint="go" data-lpignore="true" data-1p-ignore="true" onkeydown="if(event.key==='Enter')lockSubmit()" />
      <label class="lk-rem"><input type="checkbox" id="lkRem" /> 이 기기에서 30일간 기억 (분실·공용 기기에서는 끄세요)</label>
      <button class="primary" onclick="lockSubmit()">열기</button>
      <div id="lkMsg"></div>
    </div>
  </div>
  <div id="privCover" aria-hidden="true">🔒 VALUELAB</div>
  <div id="cmpTray"></div>
  <div id="cmpToast"></div>
  <button id="tblClose" onclick="tblFullClose()">✕ 전체화면 닫기</button>
  <div id="globalError"></div>
  <button id="fabTop" onclick="window.scrollTo({ top: 0, behavior: 'smooth' })" title="맨 위로">▲</button>
  <div class="topbar">
    <div class="brand">📊 한국주식 스크리닝 · 재무분석</div>
    <div class="tagline">DART 공시 · 공공데이터포털 시세 · 한국은행(ECOS)/FRED 거시지표</div>
  </div>

  <div id="freshBar" role="status" onclick="switchTab('data')" title="누르면 데이터 관리 화면이 열립니다" style="display:none;"></div>
  <div id="dbBanner" role="alert"><span id="dbBannerTxt"></span> <button onclick="switchTab('db')">DB 용량 열기</button> <button onclick="dbBannerHide()" title="이번 접속 동안 숨기기">✕</button></div>
  <div class="nav" id="mainNav">
    <button id="grpBtn_find" onclick="switchGroup('find')">🔎 발굴</button>
    <button id="grpBtn_stock" class="toggle-active" onclick="switchGroup('stock')">📈 종목</button>
    <button id="grpBtn_my" onclick="switchGroup('my')">💼 내 투자</button>
    <button id="grpBtn_admin" onclick="switchGroup('admin')">⚙ 검증·관리</button>
  </div>
  <div class="grpnav" id="grpNav" style="display:none;">
    <button id="tabBtnMomentum" data-grp="find" onclick="switchTab('momentum')">스크리닝</button>
    <button id="tabBtnFscr" data-grp="find" onclick="switchTab('fscr')">재무 스크리닝</button>
    <button id="tabBtnMacro" data-grp="find" onclick="switchTab('macro')">거시·상관</button>
    <button id="tabBtnFinancial" data-grp="stock" class="toggle-active" onclick="switchTab('financial')">종목분석</button>
    <button id="tabBtnPf" data-grp="my" onclick="switchTab('pf')">내 계좌</button>
    <button id="tabBtnBt" data-grp="admin" onclick="switchTab('bt')">백테스트</button>
    <button id="tabBtnData" data-grp="admin" onclick="switchTab('data')">데이터 관리</button>
    <button id="tabBtnDb" data-grp="admin" onclick="switchTab('db')">DB 용량</button>
    <button id="tabBtnGuide" data-grp="admin" onclick="switchTab('guide')">쉬운 설명</button>
  </div>

  <!-- ===================== 종목분석 ===================== -->
  <div id="tabFinancial">
    <div class="card" id="searchCard">
      <div class="row">
        <span class="combo"><input id="corpName" placeholder="종목명·종목코드·초성 (예: 삼성전자, 005930, ㅅㅅㅈㅈ)" autocomplete="off" /></span>
        <button class="primary" onclick="analyzeStock()">분석하기</button>
        <button id="btnWatch" onclick="toggleWatchlist()" title="관심종목에 추가/제거">☆ 관심</button>
        <button id="btnCmpAdd" onclick="cmpToggleCurrent()" title="비교할 종목 바구니에 담기(최대 5종목)">⚖ 비교 담기</button>
        <button id="btnSearchMore" onclick="toggleSearchCompact()" title="관심종목·저장 종목·고급 설정 펼치기/접기" style="display:none;">▾ 더보기</button>
      </div>
      <div id="comboHint_corpName" class="combo-hint"></div>
      <div id="watchlistWrap" class="row" style="margin-top:8px; gap:6px;"></div>
      <details id="savedBox" class="adv" style="margin-top:6px;">
        <summary id="savedSummary">📁 DB에 저장된 종목 (불러오는 중…)</summary>
        <div id="savedRecent" style="margin-top:8px;"></div>
        <input id="savedFilter" placeholder="저장된 종목 안에서 찾기" autocomplete="off" style="width:100%; box-sizing:border-box; margin:6px 0;" />
        <div class="row" style="gap:6px; align-items:center; margin-bottom:6px;"><select id="savedInd" onchange="renderSavedBox()"><option value="">업종: 전체</option></select><label style="font-size:12.5px;"><input type="checkbox" id="savedGrp" onchange="renderSavedBox()"> 업종별로 묶기</label><button type="button" onclick="indFillList('saved')" style="padding:4px 8px; font-size:12px;" title="업종을 아직 모르는 종목을 DART에서 받아 저장합니다(종목당 1회)">업종 받기</button><span id="savedIndMsg" class="meta-line"></span></div>
        <div id="savedChips"></div>
      </details>
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

    <div id="cmpPanel" style="display:none;">
      <div class="card">
        <div class="row" style="justify-content:space-between;"><b style="font-size:16px;">⚖ 종목 비교 (최대 5종목)</b><button onclick="cmpClose()">← 종목분석으로</button></div>
        <div class="meta-line" style="margin:6px 0 8px;">DB에 저장된 재무데이터를 같은 기준으로 나란히 봅니다(현재가는 자동). 종목분석·스크리닝에서 「⚖ 비교 담기」로 담아도 됩니다. 값이 좋아 보이는 쪽을 초록으로 표시하지만 투자 판단은 아닙니다.</div>
        <div class="cmp-slot"><span class="combo"><input id="cmpName0" placeholder="종목 1 (예: 삼성전자)" autocomplete="off" /></span><button class="cmp-x" onclick="cmpClearSlot(0)" title="비우기">✕</button></div>
        <div class="cmp-slot"><span class="combo"><input id="cmpName1" placeholder="종목 2 (예: 현대자동차)" autocomplete="off" /></span><button class="cmp-x" onclick="cmpClearSlot(1)" title="비우기">✕</button></div>
        <div class="cmp-slot"><span class="combo"><input id="cmpName2" placeholder="종목 3 (선택)" autocomplete="off" /></span><button class="cmp-x" onclick="cmpClearSlot(2)" title="비우기">✕</button></div>
        <div class="cmp-slot"><span class="combo"><input id="cmpName3" placeholder="종목 4 (선택)" autocomplete="off" /></span><button class="cmp-x" onclick="cmpClearSlot(3)" title="비우기">✕</button></div>
        <div class="cmp-slot"><span class="combo"><input id="cmpName4" placeholder="종목 5 (선택)" autocomplete="off" /></span><button class="cmp-x" onclick="cmpClearSlot(4)" title="비우기">✕</button></div>
        <details class="adv"><summary>현재가 직접 입력 (선택, 비우면 자동)</summary>
          <div class="row" style="margin-top:6px;"><input id="cmpPrice0" type="number" min="0" placeholder="종목1 현재가" style="flex:1 1 45%;" /><input id="cmpPrice1" type="number" min="0" placeholder="종목2 현재가" style="flex:1 1 45%;" /><input id="cmpPrice2" type="number" min="0" placeholder="종목3 현재가" style="flex:1 1 45%;" /><input id="cmpPrice3" type="number" min="0" placeholder="종목4 현재가" style="flex:1 1 45%;" /><input id="cmpPrice4" type="number" min="0" placeholder="종목5 현재가" style="flex:1 1 45%;" /></div>
        </details>
        <div class="row" style="margin-top:10px;"><button class="primary" onclick="compareStocks()" style="flex:1;">비교하기</button></div>
        <div id="cmpStatus" class="meta-line"></div>
      </div>
      <div class="tblbar" id="cmpBar"><button onclick="tblZoom(-1)">A−</button><button onclick="tblZoom(1)">A＋</button><button onclick="tblFull('cmpWrap')">⤢ 전체화면</button></div>
      <div id="cmpWrap" class="tscroll" style="display:none;"></div>
    </div>

    <div id="stockBody" style="display:none;">
      <div class="subnav sticky" id="stockSubnav">
        <button id="subBtn_summary" class="toggle-active" onclick="setSubTab('summary')">요약 · 분류 · 밸류에이션</button>
        <button id="subBtn_table" onclick="setSubTab('table')">재무제표 표</button>
        <button id="subBtn_five" onclick="setSubTab('five')">ROE 5단계 분해</button>
        <button id="subBtn_compare" onclick="cmpOpen()">⚖ 종목 비교(최대 5)</button>
      </div>

      <div id="chartDock" class="card" style="margin-bottom:12px;">
        <div class="row" id="dockBtns" style="gap:6px;">
          <button id="dockBtn_price" class="toggle-active" onclick="setDock('price')" style="padding:6px 11px; font-size:13px;">📈 주가</button>
          <button id="dockBtn_growth" onclick="setDock('growth')" style="padding:6px 11px; font-size:13px;">성장</button>
          <button id="dockBtn_margin" onclick="setDock('margin')" style="padding:6px 11px; font-size:13px;">마진</button>
          <button id="dockBtn_cash" onclick="setDock('cash')" style="padding:6px 11px; font-size:13px;">현금흐름</button>
          <button id="dockBtn_value" onclick="setDock('value')" style="padding:6px 11px; font-size:13px;">밸류에이션</button>
          <button id="dockBtn_safety" onclick="setDock('safety')" style="padding:6px 11px; font-size:13px;">재무안정</button>
          <button id="dockBtn_holder" onclick="setDock('holder')" style="padding:6px 11px; font-size:13px;">주주환원</button>
          <button id="dockFold" onclick="toggleDockFold()" style="padding:6px 11px; font-size:13px; margin-left:auto;" title="차트를 접어 아래 내용을 바로 봅니다">▴ 접기</button>
        </div>
        <div id="dockViewRow" class="row" style="margin-top:8px; display:none;">
          <span style="font-size:12.5px; color:var(--text-muted);">기간 단위</span>
          <button id="dockQ" class="toggle-active" onclick="setView('quarterly')" style="padding:4px 10px; font-size:12.5px;">분기</button>
          <button id="dockA" onclick="setView('annual')" style="padding:4px 10px; font-size:12.5px;">연간(+TTM)</button>
        </div>
        <div id="priceBox" style="margin-top:10px;">
          <div id="priceStats" style="font-size:12.5px; line-height:1.7;"></div>
          <div class="row" style="margin:6px 0; gap:6px;">
            <button id="rngBtn_126" onclick="setPriceRange(126)" style="padding:4px 10px; font-size:12.5px;">6개월</button>
            <button id="rngBtn_252" class="toggle-active" onclick="setPriceRange(252)" style="padding:4px 10px; font-size:12.5px;">1년</button>
            <button id="rngBtn_9999" onclick="setPriceRange(9999)" style="padding:4px 10px; font-size:12.5px;">전체(약 3년)</button>
            <label style="font-size:12.5px;"><input type="checkbox" id="chkMa50" checked onchange="drawPrice()" /> 50일선</label>
            <label style="font-size:12.5px;"><input type="checkbox" id="chkMa200" checked onchange="drawPrice()" /> 200일선</label>
          </div>
          <div class="row" style="margin:0 0 6px; gap:6px;"><span style="font-size:12.5px; color:var(--text-muted);">하단 막대</span>
            <button id="volBtn_tv" class="toggle-active" onclick="setVolMode('tv')" style="padding:4px 10px; font-size:12.5px;">거래대금</button>
            <button id="volBtn_vol" onclick="setVolMode('vol')" style="padding:4px 10px; font-size:12.5px;">거래량</button>
            <button id="volBtn_off" onclick="setVolMode('off')" style="padding:4px 10px; font-size:12.5px;">끄기</button></div>
          <canvas id="priceCanvas" style="width:100%; height:400px; touch-action:pan-y;"></canvas>
          <p id="pxNote" style="font-size:12px; color:var(--text-muted); margin-top:6px;"></p>
        </div>
        <div id="chartWrap" style="display:none; margin-top:10px;">
          <div id="chartTitle" style="font-weight:600; margin-bottom:6px;"></div>
          <div id="chartChips" class="chart-chips"></div>
          <canvas id="chartCanvas" style="width:100%; height:220px; touch-action:pan-y;"></canvas>
          <p id="chartNote" style="font-size:12px; color:var(--text-muted); margin-top:6px;"></p>
        </div>
      </div>

      <div id="matSlot"></div><!-- 📰 재료 점검: 주가 화면 바로 다음 -->

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
        <div class="tblbar"><button onclick="tblZoom(-1)">A−</button><button onclick="tblZoom(1)">A＋</button><button onclick="tblFull('wrap')">⤢ 전체화면</button><span>좌우로 밀어서 보기 →</span></div>
        <div id="wrap"></div>
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

    </div>
  </div><!-- /tabFinancial -->

  <!-- ===================== 스크리닝 ===================== -->
  <div id="tabMomentum" style="display:none;">
    <div class="card">
      <div class="card-title">조건 설정 <a href="#" onclick="guideOpen('rs'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
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
      <div class="scr-view" id="scrView"><button id="scrV_card" onclick="setScrView('card')">🗂 카드</button><button id="scrV_table" onclick="setScrView('table')">▦ 표</button></div>
      <div class="tblbar" id="scrBar"><button onclick="tblZoom(-1)">A−</button><button onclick="tblZoom(1)">A＋</button><button onclick="tblFull('screenWrap')">⤢ 전체화면</button><span>좌우로 밀어서 보기 →</span></div>
      <div id="screenCards" class="sc-cards"></div>
      <div id="screenWrap" class="tscroll" style="display:none; margin-top:10px;"></div>
      <div class="row" style="margin-top:10px;">
        <button id="btnFinPrep" onclick="prepareFinancials()" title="카드/표에서 고른 종목의 재무데이터를 서버가 백그라운드로 받아둡니다(이미 받은 기간은 건너뜀)">📥 선택한 종목 재무데이터 받기 (0/20)</button>
        <button onclick="finSelClear()">선택 해제</button>
        <span id="prepStatus" class="meta-line" style="margin:0;"></span>
      </div>
      <div class="meta-line" style="margin-top:4px;">카드의 「재무 받기」 칸이나 표의 ☑ 칸으로 종목을 고르세요(최대 20개, 목록을 바꿔도 선택은 유지됩니다). 올해부터 11년치(분기·반기·3분기·사업보고서)를 받습니다.</div>
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
        <b>12-1 모멘텀 · 52주 고가 대비</b>(참고 표시 전용, 점수·순위에는 반영하지 않음): 12-1 = 21거래일 전 종가 ÷ 252거래일 전 종가 − 1, 52주 고가 대비 = 기준일 종가 ÷ 최근 252거래일 최고 종가(1.00이면 신고가; George &amp; Hwang, 2004). 저장된 이력이 253거래일 미만이면 계산하지 않고 <b>"자료 부족"</b>으로 표시합니다(이력은 매일 하루씩 자동으로 늘어납니다). 하루 ±30%를 넘는 변동이 있었던 종목은 액면분할·병합 등으로 가격이 끊겼을 수 있어 두 값을 비우고 <b>"가격 불연속"</b>으로 표시합니다.<br />
        <b>거래대금 Z 10D · 20D (관심도 목록)</b>: Z = (기준일 거래대금 − 직전 N거래일 평균) ÷ 표준편차(모집단, 기준일 제외). 시장의 <b>관심이 평소보다 얼마나 몰렸는지</b>만 보여주는 발견용 신호로, 가격 모멘텀·사업의 질·저평가와는 별개이며 복합점수·"겹친 목록" 수에 넣지 않았습니다. 거래대금 0인 날과 저장되지 않은 날은 관측에서 제외하고(0으로 채우지 않음), 유효 관측이 10D 5개·20D 10개 미만이거나 표준편차가 0이면 N/A입니다. 거래대금 15억 미만인 날은 저장되지 않으므로 평소 거래대금이 그 근처인 종목은 Z가 낮게 나옵니다(보수적).<br />
        <b>20·60·120일 수익률</b>: 단순 기간 수익률(최근 1개월 포함, 참고용). <b>RS</b>: (1+주식 수익률) ÷ (1+코스피 수익률). 1보다 크면 코스피를 이긴 것입니다.<br />
        <b>⚠ 단기 과열 표시</b>: 최근 1개월 수익률이 후보 종목 중 <b>상위 5%</b>이면 붙입니다(필터 아님). 고정 %가 아니라 순위라서 장세(강세·약세)에 따라 기준이 자동으로 달라집니다. 5%는 관행적 값이며 학술적으로 검증된 임계값이 아닙니다.<br />
        <b>왜 최근 1개월을 건너뛰나요?</b> 3~12개월 수익률이 높은 종목이 이후에도 초과수익을 낸다는 중기 모멘텀(Jegadeesh &amp; Titman, 1993)과 달리, 직전 1개월 수익률은 오히려 반대로 되돌려지는 경향(단기 반전, Jegadeesh 1990; Lehmann 1990)이 있어 순위에서 뺍니다.<br />
        <b>한계(꼭 읽어주세요)</b><br />
        ① 한국을 포함한 일부 아시아 시장은 중기 모멘텀 효과가 약하거나 불안정하다는 연구가 있습니다(예: Chui·Titman·Wei, 2010). 개인 투자자 비중이 커 단기 과열 후 반전도 흔합니다. 이 목록은 "후보를 좁히는 도구"이지 매수 신호가 아닙니다.<br />
        ② 12-1 모멘텀·52주 고가 대비는 저장된 가격 이력이 253거래일 이상 쌓인 뒤부터 표시됩니다. 가격은 수정주가가 아니라 당시 종가라서 액면분할·유상증자 등이 있으면 왜곡될 수 있고(±30% 초과 변동 종목은 비움), 이 두 값이 한국 시장에서 초과수익을 낸다는 검증은 아직 하지 않았습니다(가설 단계).<br />
        ③ <b>시가총액·거래대금은 절대 금액이 아니라 "하위 20% 제외"</b>가 기본입니다. 국내 퀀트 투자 실무(예: 소형주 하위 20~30% 제외)에서 널리 쓰이는 방식이며, 시장 규모가 변해도 기준이 따라가는 장점이 있습니다. 순위는 이 앱이 저장한 종목(시총 500억·거래대금 15억 이상 보통주) 안에서 매기므로 전체 상장 종목 기준의 하위 20%보다 실제로는 더 엄격합니다. 어떤 비율이 "정답"이라는 학술적 증명은 없고(Hou·Xue·Zhang 2020: 이상현상 상당수가 초소형·저유동 종목에서 나와 이를 제외하면 재현성이 크게 떨어짐), 최소주가 1,000원은 호가단위 때문에 수익률이 왜곡되는 동전주를 거르는 실무 관행입니다. 프리셋: 표준 20% / 대형·고유동 50% / 중소형 포함 0%<br />
        ④ 공공데이터포털 시세는 기준일 다음 영업일 13시 이후에 올라오므로 기준일이 어제가 아닐 수 있습니다. 거래비용·증권거래세·배당은 반영하지 않으며, 이 순위의 과거 성과(백테스트)는 아직 검증하지 않았습니다.<br />
        ⑤ 영업이익은 공공데이터포털 기업재무정보(연결 우선)의 최근 사업연도 값이며, 없으면 DART로 저장한 값으로 대체합니다.
      </div>
    </details>
  </div><!-- /tabMomentum -->

  <!-- ===================== 거시·상관 ===================== -->
  <div id="tabFscr" style="display:none;">
    <div class="card">
      <div class="card-title">재무 스크리닝 — 재무가 저장된 종목 안에서 거르기 <a href="#" onclick="guideOpen('funnel'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
      <div style="font-size:12.5px; line-height:1.5;">ROIC·성장·FCF·가치 조건을 직접 켜서 거릅니다. 합산 점수는 만들지 않고, 조건마다 몇 종목이 남는지와 자료가 없어 빠진 수를 보여줍니다. 아래 기준값은 <b>참고용 출발값</b>이며 이 값으로 초과수익이 난다는 검증은 하지 않았습니다(백테스트 탭에서 방법별로 따로 확인).</div>
      <div class="row" style="margin-top:8px;">
        <button class="primary" onclick="fscLoad()">저장된 종목 불러오기·계산</button>
        <select id="fscInd" onchange="fscRender()"><option value="">업종: 전체</option></select> <select id="fscSec" onchange="fscRender()" title="금융업·지주회사는 ROIC·FCF가 맞지 않아 ROE·PBR로 따로 거릅니다"><option value="">유형: 전체</option><option value="gen">일반 기업(금융·지주 제외)</option><option value="fin">금융업 전체</option><option value="bank">은행·금융지주</option><option value="insurer">보험</option><option value="broker">증권</option><option value="credit">여신금융(카드·캐피탈)</option><option value="invest">투자·기타 금융</option><option value="holding">지주회사(비금융)</option></select> <a href="#" onclick="guideOpen('finsec'); return false;" style="font-size:11.5px;">ⓘ 유형 분류</a> <select id="fscBasis" onchange="fscRun()"><option value="ttm">최신값: TTM 우선(분기 포함)</option><option value="annual">최신값: 사업보고서(연간)만</option></select>
      </div>
      <div id="fscMsg" class="meta-line"></div>
    </div>
    <div class="card">
      <div class="card-title">조건 <span id="fscPresets" style="font-weight:400; font-size:12px;"></span></div>
      <div id="fscFilters"></div>
      <div class="meta-line">값이 없는(N/A) 종목은 그 조건을 켜면 제외됩니다. ROIC 3Y평균·성장률·F-Score는 연간(사업보고서) 기준, 나머지는 위에서 고른 최신값 기준입니다.</div>
    </div>
    <div class="card" id="fscOut"><div class="meta-line">「저장된 종목 불러오기·계산」을 누르면 결과가 여기에 표시됩니다.</div></div>
  </div><!-- /tabFscr -->

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

  <!-- ===================== 백테스트 ===================== -->
  <div id="tabBt" style="display:none;">
    <div class="card">
      <div class="card-title">🧪 백테스트 — 과거에 이 방법을 썼다면 어땠을까? <a href="#" onclick="guideOpen('bt-what'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
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
      <div class="card-title">A. 재무분석 방법 백테스트 (DART 저장 데이터) <a href="#" onclick="guideOpen('bt-groups'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
      <div class="row"><button onclick="btLoadStocks()">① 저장된 종목 불러오기</button><button onclick="btBackfillNextDay()" title="연간 백테스트가 매매가로 쓰는 '공시 다음 거래일 종가'를 기존 저장 행에 채웁니다(처음 한 번, 이후 새로 받는 행은 자동 저장)">공시 다음 거래일 가격 채우기</button><span id="btFinInfo" style="font-size:12px; color:var(--text-muted);"></span></div>
      <div style="margin:8px 0; font-size:12.5px;">
        <label>방식 <select id="btFinMode" onchange="btFinModeChange()"><option value="annual">연간 재무 방법 (공시 다음 거래일 종가 매수 → 다음 해 공시 다음 거래일 종가 매도)</option><option value="qttm">분기 TTM 재무 방법 (분기보고서 공시 다음 거래일 종가 매수 → 다음 분기보고서 공시 다음 거래일 종가 매도)</option><option value="momq">분기 모멘텀 (분기말 주가)</option></select></label>
      </div>
      <div style="margin:6px 0; font-size:12.5px;">
        <label>대상 종목 <select id="btUniv" onchange="btUnivChange()" title="Fama-French(1992) 등 많은 연구가 금융회사를 뺍니다(레버리지 의미가 달라서). 금융업은 F-Score·ROIC·FCF가 맞지 않아 이 지표들에서는 자동으로 값이 비어 빠집니다"><option value="all">전체 (기본)</option><option value="exfin">금융업 제외</option><option value="exboth">금융업·지주회사 제외</option><option value="finonly">금융업만</option></select></label>
        <span id="btUnivNote" style="color:var(--text-muted);"></span> <a href="#" onclick="guideOpen('btuniv'); return false;" style="font-size:11.5px;">ⓘ 쉬운 설명</a>
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
      <div class="card-title">A-2. 조합 탐색 — 어떤 재무·가치 조합이 우연이 아닌가 (역방향) <a href="#" onclick="guideOpen('bt-multi'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
      <div style="font-size:12.5px; line-height:1.5;">위 A는 내가 고른 방법이 유의한지 보지만, 여기서는 반대로 <b>요인 1~5개의 모든 조합을 한꺼번에 시험</b>해 어떤 조합이 유의한지 보여줍니다. 조합을 많이 뒤질수록 우연히 좋아 보이는 것이 생기므로, 수익률을 무작위로 섞은 가짜 세계를 여러 번 만들어 <b>우연으로 나올 수 있는 최고치</b>를 기준선으로 삼아 보정합니다. 또 기간을 나눠 앞 구간에서 고른 것이 뒤 구간에서도 같은 방향인지 확인합니다.</div>
      <div class="row" style="margin-top:8px; gap:10px; flex-wrap:wrap; font-size:12.5px;">
        <label>방식 <select id="cmbMode"><option value="annual">연간(기간 수 적음)</option><option value="qttm" selected>분기 TTM(기간 수 많음, 권장)</option></select></label>
        <label>최대 조합 크기 <select id="cmbK"><option>1</option><option>2</option><option>3</option><option>4</option><option selected>5</option></select></label>
        <label>그룹 수 <select id="cmbG"><option value="3">3</option><option value="5" selected>5</option><option value="10">10</option></select></label>
        <label>가짜 세계 <select id="cmbB"><option value="200">200회</option><option value="500" selected>500회</option><option value="1000">1000회</option></select></label>
        <label>씨앗 <input id="cmbSeed" type="number" value="1" style="width:60px;" /></label>
        <label><input id="cmbSplit" type="checkbox" checked /> 기간 나누기(앞 2/3 탐색 → 뒤 1/3 확인)</label>
      </div>
      <div id="cmbPool" style="margin:6px 0; font-size:12.5px;"></div>
      <div class="row"><button class="primary" onclick="cmbRun()">조합 탐색 실행</button> <span id="cmbMsg" class="meta-line"></span></div>
      <div id="cmbOut"></div>
    </div>

    <div class="card">
      <div class="card-title">A-3. 신규 지표 발굴 — 기존 방법 말고 어떤 지표가 유의한가 (역방향) <a href="#" onclick="guideOpen('bt-multi'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
      <div style="font-size:12.5px; line-height:1.5;">위 A·A-2는 내가 정한(또는 기존에 제시된 10개) 방법만 다룹니다. 여기서는 <b>저장된 재무로 만들 수 있는 후보 지표 40여 개</b>(수익성·안정성·효율·자본배분·가격결합·변화·지속성)를 <b>기존 방법과 함께 한꺼번에</b> 시험해, 어느 것이 <b>보정 후에도 우연이 아닌지</b>와 <b>높을수록/낮을수록 유리한지</b>를 알려줍니다. 지표가 많을수록 우연히 좋아 보이는 것이 늘어나므로 수익률을 섞은 가짜 세계로 보정하고, 앞 구간에서 찾은 것을 뒤 구간에서 다시 확인합니다. 결과는 <b>가설 후보</b>이며 투자 신호가 아닙니다.</div>
      <div class="row" style="margin-top:8px; gap:10px; flex-wrap:wrap; font-size:12.5px;">
        <label>방식 <select id="dscMode"><option value="annual">연간(기간 수 적음)</option><option value="qttm" selected>분기 TTM(기간 수 많음, 권장)</option></select></label>
        <label>범위 <select id="dscScope"><option value="all" selected>기존 10개 + 신규 후보</option><option value="cand">신규 후보만(시험 수 적음)</option></select></label>
        <label>그룹 수 <select id="dscG"><option value="3">3</option><option value="5" selected>5</option><option value="10">10</option></select></label>
        <label>가짜 세계 <select id="dscB"><option value="200">200회</option><option value="500" selected>500회</option><option value="1000">1000회</option></select></label>
        <label>씨앗 <input id="dscSeed" type="number" value="1" style="width:60px;" /></label>
        <label><input id="dscSplit" type="checkbox" checked /> 기간 나누기(앞 2/3 탐색 → 뒤 1/3 확인)</label>
      </div>
      <div class="row"><button class="primary" onclick="dscRun()">신규 지표 발굴 실행</button> <span id="dscMsg" class="meta-line"></span></div>
      <div id="dscOut"></div>
    </div>

    <div class="card">
      <div class="card-title">C. 종목별 보기 — 재무 값 · 주가 · 주가수익률 한 번에 <a href="#" onclick="guideOpen('bt-lookahead'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
      <div style="font-size:12px; color:var(--text-muted); line-height:1.55; margin-bottom:6px;">위 A의 연간 재무 백테스트가 종목마다 실제로 쓰는 값을 결산연도별로 모아 보여 줍니다. <b>매수가</b> = 사업보고서 공시 다음 거래일 종가, <b>매도가</b> = 다음 해 사업보고서 공시 다음 거래일 종가, <b>12개월 수익률</b> = 매도가 ÷ 매수가 − 1(배당 제외). 재무 팩터 값은 공시 시점에 알 수 있었던 자료와 매수가로만 계산합니다.</div>
      <div class="row" style="gap:8px; flex-wrap:wrap; align-items:center;">
        <input id="btStkIn" list="btStkList" placeholder="종목명 입력 또는 선택 (재무 데이터가 저장된 종목)" style="flex:1; min-width:200px;" onchange="btStkPick(this.value)" onkeydown="if(event.key==='Enter'){btStkPick(this.value);}" />
        <datalist id="btStkList"></datalist>
        <button onclick="btStkPick(document.getElementById('btStkIn').value)">보기</button>
        <button onclick="btStkDownload(false)" title="지금 보고 있는 종목을 엑셀에서 열 수 있는 CSV로 저장">이 종목 CSV</button>
        <button onclick="btStkDownload(true)" title="위 A의 ① 「저장된 종목 불러오기」를 누른 뒤 쓸 수 있습니다">전체 종목 CSV</button>
      </div>
      <div id="btStkMsg" class="meta-line"></div>
      <div id="btStkOut"></div>
    </div>

    <div class="card">
      <div class="card-title">B. 스크리닝 조건 백테스트 (일별 시세) <a href="#" onclick="guideOpen('bt-t'); return false;" style="font-size:12px; font-weight:400; margin-left:6px;">ⓘ 쉬운 설명</a></div>
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
        <tr><td class="an-first">미래정보 편향 방지</td><td style="text-align:left;">공시 다음 거래일 종가로만 매수·매도(공시 당일 종가는 공시 전 정보가 섞일 수 있어 쓰지 않음). 공시일은 법정 기한으로 상한을 둔 근사값이라 늦은 공시·정정공시는 완전히 통제하지 못함</td><td style="text-align:left;">백테스트의 기본 원칙(look-ahead bias)</td><td>원칙</td></tr>
        <tr><td class="an-first">표본 분할</td><td style="text-align:left;">전반·후반으로 나눠 부호가 같은지 점검</td><td style="text-align:left;">표본 내/외 검증, Walk-forward(Pardo 2008)의 간이판. 정식 표본 외 검증은 아님</td><td>실무 (간이)</td></tr>
        <tr><td class="an-first">거래비용</td><td style="text-align:left;">상위 그룹 교체율 × 왕복 비용을 차감</td><td style="text-align:left;">Novy-Marx &amp; Velikov(2016): 거래비용 후 많은 이상현상이 약해짐. 한국은 매도 시 증권거래세가 있어 비용 입력값은 직접 조정하세요</td><td>학술 + 가정</td></tr>
        <tr><td class="an-first">알려진 한계</td><td style="text-align:left;">생존편향·선택편향·배당 미반영·소형주 체결 불가·분기 자료의 한계</td><td style="text-align:left;">Hou-Xue-Zhang(2020): 소형주 제외와 엄격한 유의수준을 적용하면 많은 이상현상이 재현되지 않음</td><td>한계 고지</td></tr>
      </tbody></table></div>
      <div style="font-size:11.5px; color:var(--text-muted); margin-top:6px;">논문 인용은 이 앱에서 온라인으로 재확인하지 못했습니다. 중요한 판단 전에 원문을 확인하세요. 백테스트 결과는 미래 수익을 보장하지 않습니다.</div>
    </details>
  </div>


  <!-- ===================== DB 용량 ===================== -->
  <div id="tabGuide" style="display:none;">
    <div class="card">
      <div class="card-title">📖 쉬운 설명 — 백테스트·지표 용어집</div>
      <div style="font-size:12.5px; line-height:1.6;">화면에 나오는 말을 비전공자도 알 수 있게 풀어 쓴 설명집입니다. 항목마다 <b>한마디로 · 쉬운 예 · 이 앱에서 어디 · 조심할 점</b>을 적었습니다. 단어(예: 현금, 우연, ROIC)로 찾아보세요. 투자 권유가 아니라 계산과 용어의 설명입니다.</div>
      <div class="row" style="margin-top:8px;">
        <input id="guideQ" oninput="guideRender()" placeholder="찾을 말 (예: FCF, 우연, 업종)" style="flex:1; min-width:160px;" />
        <select id="guideCat" onchange="guideRender()"><option value="">전체 분야</option></select>
      </div>
      <div id="guideCount" class="meta-line"></div>
      <div id="guideOut"></div>
    </div>
  </div><!-- /tabGuide -->

  <div id="tabDb" style="display:none;">
    <div class="card">
      <div class="card-title">DB 용량 현황</div>
      <div id="dbGauge" class="meta-line">불러오는 중…</div>
      <div id="dbMsg" class="meta-line"></div>
      <div class="row" style="margin-top:8px;"><button onclick="dbLoad(true)">🔄 새로고침 (행 수 다시 계산)</button></div>
      <details class="adv"><summary>용량 한도 · 알림 기준 설정</summary>
        <div class="row" style="margin-top:8px;">
          <label style="font-size:13px;">DB 용량 한도(MB) <input id="dbLimitMb" type="number" inputmode="numeric" min="50" style="width:110px;" /></label>
          <label style="font-size:13px;">알림 기준(%) <input id="dbWarnPct" type="number" inputmode="numeric" min="50" max="99" style="width:90px;" /></label>
          <button onclick="dbSaveSettings()">저장</button>
        </div>
        <div class="meta-line">한도는 요금제마다 다릅니다(무료 플랜은 DB 1개당 500MB로 알고 있으나, 정확한 값은 Cloudflare 대시보드에서 확인하세요). 사용량이 알림 기준을 넘으면 모든 화면 위쪽에 경고가 뜹니다.</div>
      </details>
    </div>
    <div class="card">
      <div class="card-title">테이블별 사용량 <span id="dbMeasured" style="text-transform:none;"></span></div>
      <div class="tblbar"><button onclick="tblZoom(-1)">A−</button><button onclick="tblZoom(1)">A＋</button><button onclick="tblFull('dbTables')">⤢ 전체화면</button></div>
      <div id="dbTables" class="tscroll"></div>
    </div>
    <div class="card">
      <div class="card-title">용량 확보 — 조건을 정해 데이터 삭제</div>
      <div class="meta-line" style="margin:0 0 8px;">삭제도 D1 쓰기 한도를 쓰므로 <b id="dbCapTxt">하루 20,000행</b>까지만 지울 수 있고, 큰 삭제는 며칠에 나눠 진행됩니다. 삭제한 데이터는 되돌릴 수 없으니 먼저 <b>미리보기</b>로 개수를 확인하세요.</div>
      <select id="dbKind" onchange="dbKindChange()" style="width:100%;">
        <option value="price_old">일별 시세: 지정일 이전 삭제</option>
        <option value="price_inactive">일별 시세: 현재 저장 기준 미통과 종목 삭제</option>
        <option value="screen_old">주간 스크리닝 결과: 오래된 것 삭제</option>
        <option value="fin_company">재무데이터: 특정 종목 삭제</option>
        <option value="fin_old">재무데이터: 지정 연도 이전 삭제(전 종목)</option>
        <option value="opincome_cache">영업이익 조회 캐시 삭제</option>
      </select>
      <div id="dbKindHelp" class="meta-line"></div>
      <div id="dbParam" class="row" style="margin-top:6px;"></div>
      <div class="row" style="margin-top:10px;"><button onclick="dbPreview()" style="flex:1;">① 미리보기 (몇 행 지워지나)</button></div>
      <div id="dbPreviewBox" class="meta-line"></div>
      <div class="row" style="margin-top:6px;"><button id="dbRunBtn" class="primary" onclick="dbRun()" style="flex:1; display:none;">② 삭제 실행</button><button id="dbStopBtn" onclick="dbStop()" style="display:none;">중단</button></div>
      <div id="dbRunBox" class="meta-line"></div>
    </div>
  </div>

  <!-- ===================== 내 계좌 ===================== -->
  <div id="tabPf" style="display:none;">
    <div class="card" style="border-left:4px solid #f59e0b;">
      <div class="card-title">내 계좌·매매 관리 (기록과 점검 도구 — 주문은 내지 않습니다)</div>
      <div style="font-size:12.5px; line-height:1.6;">증권사에서 실제로 체결한 내용을 여기에 <b>기록</b>하면 평균단가·비중·현금·행동 통계를 계산합니다. 이 화면의 수량·경고는 <b>참고용 계산</b>이며 투자 권유나 최적 수익을 보장하지 않습니다.
        근거 수준: ①정의·산식(평균단가, 비중, 손실 예산 = 확립된 계산) ②반복 관찰된 행동 편향(처분효과 Shefrin&amp;Statman 1985·Odean 1998, 과잉거래 Barber&amp;Odean 2000, 손실회피 Kahneman&amp;Tversky 1979) ③실무 관행(체크리스트·숙려 시간·일지) ④정책값은 <b>본인 선택</b>이며 정답이 증명된 값이 아닙니다. 논문은 이 앱에서 온라인으로 재확인하지 못했습니다.</div>
    </div>
    <div class="card" id="pfuGate">
      <div class="card-title">접근 키</div>
      <div style="font-size:12.5px; color:var(--text-muted); margin-bottom:8px;">이 사이트 주소는 공개라서 보유 내역은 키가 있어야 저장·조회됩니다. Cloudflare → 이 Worker → Settings → Variables and Secrets에 <b>PF_KEY</b>(12자 이상 무작위 문자열, Secret 형식)를 등록한 뒤 같은 값을 아래에 입력하세요.</div>
      <div class="row"><input id="pfuKey" type="text" lang="en" inputmode="text" name="pf-access-code" placeholder="접근 키" autocomplete="off" autocapitalize="off" autocorrect="off" spellcheck="false" data-lpignore="true" data-1p-ignore="true" style="flex:1; min-width:180px; -webkit-text-security: disc;" /><button onclick="pfuUnlock()">열기</button></div>
      <div id="pfuGateMsg" class="meta-line"></div>
    </div>
    <div id="pfuMain" style="display:none;">
      <div class="subnav" id="pfuNav">
        <button onclick="pfuPanel('home')" id="pfuNb_home">현황</button>
        <button onclick="pfuPanel('plan')" id="pfuNb_plan">계획·수량</button>
        <button onclick="pfuPanel('ledger')" id="pfuNb_ledger">원장</button>
        <button onclick="pfuPanel('check')" id="pfuNb_check">행동 점검</button>
        <button onclick="pfuPanel('policy')" id="pfuNb_policy">정책</button>
        <button onclick="appLock('잠겼습니다. 키를 입력하면 다시 열립니다.')" id="pfuNb_lock" title="화면 잠금">🔒 잠금</button>
      </div>
      <div id="pfuMsg" class="meta-line"></div>

      <div id="pfuP_home">
        <div class="card" id="pfuSummary"></div>
        <div class="card" id="pfuAlerts" style="display:none;"></div>
        <div id="pfuHold"></div>
      </div>

      <div id="pfuP_plan" style="display:none;">
        <div class="card">
          <div class="card-title">투자 계획 (가설 · 반증 조건 · 위험 기준가)</div>
          <div class="row"><span class="combo"><input id="pfpName" placeholder="종목명·코드·초성" autocomplete="off" /></span>
            <select id="pfpStatus"><option>관찰</option><option>매수검토</option><option>보유</option><option>보류</option><option>축소검토</option><option>매도검토</option><option>종료</option></select>
            <input id="pfpSector" placeholder="업종(직접 입력)" style="width:120px;" /></div>
          <div id="comboHint_pfpName" class="combo-hint"></div>
          <textarea id="pfpThesis" placeholder="투자 논리: 왜 이 기업인가? (사업·경쟁우위·현금흐름 근거)" style="width:100%; margin-top:8px; min-height:60px;"></textarea>
          <textarea id="pfpDis" placeholder="반증 조건: 무엇이 관측되면 이 가설이 틀렸다고 인정할지 (매수검토·보유 상태는 필수)" style="width:100%; margin-top:8px; min-height:50px;"></textarea>
          <textarea id="pfpRisk" placeholder="핵심 위험 / 대안 종목과의 비교 / 촉매" style="width:100%; margin-top:8px; min-height:50px;"></textarea>
          <div class="row" style="margin-top:8px;">
            <input id="pfpBear" type="number" placeholder="Bear 가격" style="width:100px;" /><input id="pfpBase" type="number" placeholder="Base 가격" style="width:100px;" /><input id="pfpBull" type="number" placeholder="Bull 가격" style="width:100px;" />
          </div>
          <div class="row" style="margin-top:8px;">
            <input id="pfpEL" type="number" placeholder="매수구간 하단" style="width:110px;" /><input id="pfpEH" type="number" placeholder="매수구간 상단" style="width:110px;" /><input id="pfpRP" type="number" placeholder="위험관리 기준가" style="width:120px;" /><input id="pfpMaxQ" type="number" placeholder="최대 수량(주)" style="width:110px;" />
          </div>
          <textarea id="pfpSell" placeholder="매도·축소 규칙(사전에 정함): 예) 가치 도달 / 가설 훼손 / 비중 초과" style="width:100%; margin-top:8px; min-height:44px;"></textarea>
          <textarea id="pfpNoAvg" placeholder="물타기 금지/허용 규칙(사전에 정함)" style="width:100%; margin-top:8px; min-height:40px;"></textarea>
          <div class="row" style="margin-top:8px;"><label style="font-size:12.5px;">검토일 <input id="pfpReview" type="date" /></label></div>
          <div style="font-size:12.5px; margin-top:8px;"><b>분할 매수 계획</b> (조건·가격·비중%는 본인 선택. 일시 매수보다 분할이 낫다는 것은 이 앱에서 검증되지 않았습니다)</div>
          <div id="pfpTr"></div>
          <div class="row" style="margin-top:10px;"><button onclick="pfuSavePlan()">계획 저장(새 버전)</button><button onclick="pfuClearPlan()" style="background:#fff; color:var(--text);">새로 작성</button></div>
          <input type="hidden" id="pfpKey" />
        </div>
        <div class="card"><div class="card-title">저장된 계획</div><div id="pfuPlans"></div></div>
        <div class="card">
          <div class="card-title">매수 수량 계산기 (위험 예산 · 집중도 · 현금 · 유동성 중 가장 작은 값)</div>
          <div class="row"><span class="combo"><input id="pfcName" placeholder="종목명·코드·초성" autocomplete="off" /></span><button onclick="pfuQuote()">현재가·거래대금 불러오기</button></div><div id="comboHint_pfcName" class="combo-hint"></div>
          <div class="row" style="margin-top:8px;"><input id="pfcBuy" type="number" placeholder="매수 예정가" style="width:110px;" /><input id="pfcRisk" type="number" placeholder="위험관리 기준가" style="width:120px;" /><input id="pfcBear" type="number" placeholder="Bear 가격(선택)" style="width:120px;" /><input id="pfcAdv" type="number" placeholder="20일 평균 거래대금(원)" style="width:170px;" /></div>
          <div class="row" style="margin-top:8px;"><button onclick="pfuCalc()">계산</button></div>
          <div id="pfcOut" style="margin-top:10px;"></div>
        </div>
      </div>

      <div id="pfuP_ledger" style="display:none;">
        <div class="card"><div class="card-title">거래 원장 (삭제 없음 — 잘못된 기록은 무효 처리)</div>
          <input id="pflFilter" placeholder="종목명으로 거르기" oninput="pfuRenderLedger()" style="width:100%; margin-bottom:8px;" />
          <div id="pfuLedger"></div></div>
      </div>

      <div id="pfuP_check" style="display:none;">
        <div class="card" id="pfuBeh"></div>
        <div class="card">
          <div class="card-title">사후 검토 일지 (결과가 아니라 과정을 평가)</div>
          <div class="row"><span class="combo"><input id="pfjName" placeholder="종목명(선택)" autocomplete="off" /></span><input id="pfjOut" type="number" step="0.1" placeholder="결과 수익률 %" style="width:130px;" />
            <select id="pfjLS"><option value="">운/실력 판단</option><option>대부분 실력(과정)</option><option>반반</option><option>대부분 운</option><option>판단 불가</option></select>
            <select id="pfjEmo"><option value="">감정</option><option>평온</option><option>자신감</option><option>불안</option><option>공포</option><option>탐욕</option><option>후회</option></select></div>
          <div style="font-size:12.5px; margin-top:8px;">과정 점검 (해당하면 체크)</div>
          <div id="pfjProc"></div>
          <textarea id="pfjLesson" placeholder="배운 점 / 다음에 바꿀 규칙" style="width:100%; margin-top:8px; min-height:50px;"></textarea>
          <div class="row" style="margin-top:8px;"><button onclick="pfuSaveJournal()">일지 저장</button></div>
          <div id="pfuJournal" style="margin-top:10px;"></div>
        </div>
      </div>

      <div id="pfuP_policy" style="display:none;">
        <div class="card">
          <div class="card-title">나의 투자 정책 (본인이 정하는 값 — 정답이 증명된 숫자는 없습니다)</div>
          <div style="font-size:12.5px; color:var(--text-muted); margin-bottom:8px;">저장할 때마다 새 버전이 쌓입니다(과거 기록 보존). 입력 예시는 참고용 출발점일 뿐 추천값이 아닙니다.</div>
          <div class="row"><label class="pfl">1회 매수 허용 손실(총자산 대비 %)<input id="pfoRisk" type="number" step="0.1" placeholder="예: 0.5" /></label>
            <label class="pfl">단일 종목 최대 비중 %<input id="pfoMax" type="number" step="1" placeholder="예: 15" /></label>
            <label class="pfl">최소 현금 비중 %<input id="pfoCash" type="number" step="1" placeholder="예: 10" /></label></div>
          <div class="row" style="margin-top:8px;"><label class="pfl">1년 내 예정 현금 수요(원)<input id="pfoNeeds" type="number" placeholder="0" /></label>
            <label class="pfl">감내 가능한 최대 낙폭 %<input id="pfoDD" type="number" placeholder="예: 25" /></label>
            <label class="pfl">투자 기간(개월)<input id="pfoHz" type="number" placeholder="예: 60" /></label></div>
          <div class="row" style="margin-top:8px;"><label class="pfl">매수 수수료율 %<input id="pfoFee" type="number" step="0.001" placeholder="증권사 기준" /></label>
            <label class="pfl">매도 세율 %<input id="pfoTax" type="number" step="0.001" placeholder="현재 세법 확인" /></label>
            <label class="pfl">하루 거래대금 대비 최대 주문 %<input id="pfoAdv" type="number" step="0.1" placeholder="예: 1" /></label>
            <label class="pfl">계획 후 숙려 시간(시간)<input id="pfoCd" type="number" placeholder="예: 24" /></label></div>
          <input id="pfoNote" placeholder="메모" style="width:100%; margin-top:8px;" />
          <div class="row" style="margin-top:10px;"><button onclick="pfuSavePolicy()">정책 저장</button><button onclick="pfuExport()" style="background:#fff; color:var(--text);">전체 백업(JSON) 받기</button></div>
          <div style="font-size:12px; color:var(--text-muted); margin-top:8px;">수수료율·세율은 이 앱이 임의로 넣지 않습니다. 비워 두면 비용을 0으로 기록하고 그 사실을 알려줍니다.</div>
        </div>
      </div>
    </div>
  
  <button id="pfuFab" onclick="pfuOpenSheet('buy')">＋ 기록</button>
  <div id="pfuToast"></div>
  <div id="pfuSheet" class="pf-sheet">
    <div class="pf-sheet-bg" onclick="pfuCloseSheet()"></div>
    <div class="pf-sheet-body">
      <div class="row" style="justify-content:space-between;"><b style="font-size:16px;">거래 기록 <span style="font-size:12px; font-weight:400; color:var(--text-muted);">(증권사에서 이미 체결된 내용)</span></b><button onclick="pfuCloseSheet()" style="padding:4px 10px;">✕</button></div>
      <div class="seg" id="pftSeg">
        <button class="buy" data-side="buy" onclick="pfuSetSide('buy')">매수</button>
        <button class="sell" data-side="sell" onclick="pfuSetSide('sell')">매도</button>
        <button class="cash" data-side="deposit" onclick="pfuSetSide('deposit')">입금</button>
        <button class="cash" data-side="withdraw" onclick="pfuSetSide('withdraw')">출금</button>
      </div>
      <div id="pftStockRow">
        <span class="combo"><input id="pftName" placeholder="종목명·코드·초성 (예: ㅅㅅㅈㅈ)" autocomplete="off" /></span>
        <div id="comboHint_pftName" class="combo-hint"></div>
        <div id="pftCtx" class="meta-line" style="margin-top:2px;"></div>
      </div>
      <div class="row" style="margin-top:8px;">
        <input id="pftDate" type="date" style="width:150px;" oninput="pfuPreview()" />
        <input id="pftQty" type="number" inputmode="numeric" placeholder="수량(주)" style="width:110px;" oninput="pfuPreview()" />
        <input id="pftPrice" type="number" inputmode="decimal" placeholder="체결가" style="flex:1; min-width:110px;" oninput="pfuPreview()" />
      </div>
      <div class="row" id="pftAmtRow" style="margin-top:6px;"><input id="pftAmt" type="number" inputmode="numeric" min="0" placeholder="금액(원)으로 수량 계산 (선택)" style="flex:1;" oninput="pfuAmtToQty()" /><span id="pftAmtHint" class="meta-line" style="margin:0;"></span></div>
      <div id="pftQuick" class="chips"></div>
      <div id="pftPrev" class="pf-prev">종목을 고르면 현재가와 체결 후 비중이 여기에 미리 계산됩니다.</div>
      <div id="pftEmoWrap"><div style="font-size:12.5px; color:var(--text-muted);">지금 감정 (선택 — 기록해 두면 행동 점검에 쓰입니다)</div><div class="chips" id="pftEmo"></div></div>
      <div id="pftReasonWrap" style="display:none;"><div style="font-size:12.5px; color:var(--text-muted);">매도 사유 (사전 규칙 기준)</div><div class="chips" id="pftReason"></div></div>
      <details class="adv"><summary>비용 직접 입력 · 메모</summary>
        <div class="row" style="margin-top:6px;"><input id="pftFee" type="number" placeholder="수수료(비우면 정책 비율)" style="width:170px;" /><input id="pftTax" type="number" placeholder="세금(매도, 비우면 정책)" style="width:170px;" /></div>
        <input id="pftNote" placeholder="메모" style="width:100%; margin-top:6px;" />
      </details>
      <div id="pftWarn"></div>
      <div class="row stickybtn" style="margin-top:12px;"><button class="primary" id="pftGo" onclick="pfuSubmitTrade(false)" style="flex:1; padding:12px;">기록하기</button></div>
    </div>
  </div>
  </div><!-- /tabPf -->

  <!-- ===================== 데이터 관리 ===================== -->
  <div id="tabData" style="display:none;">
    <div class="card">
      <div class="card-title">데이터 현황</div>
      <div id="dataSummary" style="font-size:13px; line-height:1.7;">불러오는 중...</div>
      <div class="row" style="margin-top:8px;"><button onclick="loadDataSummary()">새로고침</button></div>
      <div class="meta-line">평소에는 서버가 자동으로 갱신합니다(시세: 매일 한국시간 18:01 시작, 스크리닝 결과: 매주 토요일 새벽 1시 저장, 재무 큐: 짝수 분마다 처리). 아래 버튼은 처음 한 번이나 문제가 있을 때만 누르면 됩니다.</div>
    </div>

    <div class="card">
      <div class="card-title">새 공시 자동 반영 (재무가 저장된 종목)</div>
      <div id="finAutoBox" style="font-size:13px; line-height:1.6;">불러오는 중...</div>
      <div class="row" style="margin-top:8px;">
        <button onclick="finAutoRun(true)">미리보기(등록 안 함)</button>
        <button class="primary" onclick="finAutoRun(false)">지금 확인해서 큐에 등록</button>
        <button onclick="finAutoLoad()">새로고침</button>
      </div>
      <div class="meta-line">매일 한국시간 20:31에 자동 실행됩니다. 보고기간이 끝난 지 30일이 지났고 값이 비어 있는 기간만 큐에 올리고, 같은 기간은 3일에 한 번·최대 10번만 다시 확인합니다. 12월 결산이 아닌 종목은 제외하며, 값이 이미 있는 기간은 덮어쓰지 않습니다. 실제 수집은 짝수 분마다 2건씩 처리되어 종목이 많으면 몇 시간 걸립니다.</div>
    </div>

    <div class="card">
      <div class="card-title">1. 시장 시세 데이터 (스크리닝용)</div>
      <div class="row">
        <button class="primary" onclick="refreshMarketUniverse()">오늘자 시세 받기(유니버스 갱신)</button>
        <button onclick="checkMarketStatus()">진행 상황 새로고침</button>
        <button onclick="showFunnel()">왜 이 종목 수인가? (유니버스 깔때기)</button>
      </div>
      <div id="funnelBox" class="meta-line" style="white-space:pre-line;"></div>
      <div id="marketStatus" class="meta-line" style="white-space:pre-line;"></div>
    </div>

    <div class="card">
      <div class="card-title">1-1. 저장 기준 설정 (바꾼 뒤부터 매일 이 기준으로 시세를 받습니다)</div>
      <div class="row">
        <label style="font-size:13px;">하루 거래대금 ≥ <input id="cfgTv" type="number" min="0" step="1" style="width:90px" /> 억원</label>
        <label style="font-size:13px;">시가총액 ≥ <input id="cfgMc" type="number" min="0" step="10" style="width:100px" /> 억원</label>
        <button class="primary" onclick="saveMarketCfg()">저장</button>
        <button onclick="previewCfg()">이 기준이면 몇 종목? (미리보기)</button>
      </div>
      <div id="cfgMsg" class="meta-line" style="white-space:pre-line;"></div>
      <details class="adv"><summary>어떻게 적용되나요? (꼭 읽어보세요)</summary>
        <div class="meta-line">• 저장 후 <b>다음 일일 수집부터</b>(한국시간 18:01 자동, 또는 위 「오늘자 시세 받기」) 이 기준이 적용됩니다. 스크리닝의 하한도 이 값을 따릅니다.<br />
        • <b>이미 저장된 과거 시세는 지우지 않습니다.</b> 기준을 올리면 새 기준 미만 종목은 더 이상 갱신되지 않을 뿐입니다.<br />
        • 기준을 <b>내리면</b> 새로 통과하는 종목은 약 250일치 이력을 받아야 하고, 무료 한도 때문에 <b>하루 약 150종목씩</b> 순서대로(거래대금 큰 종목부터) 채웁니다. 신규가 많으면 며칠 걸립니다.<br />
        • 거래대금을 0으로 두면 그날 거래가 적었던 종목도 빠지지 않아 이력에 구멍이 생기지 않습니다(저장량은 늘어납니다).</div>
      </details>
    </div>

    <div class="card">
      <div class="card-title">1-2. 기간 지정 시세 가져오기 (이력 늘리기 · 구멍 메우기)</div>
      <div class="row">
        <label style="font-size:13px;">시작일 <input id="rgFrom" type="date" /></label>
        <label style="font-size:13px;">종료일 <input id="rgTo" type="date" /></label>
        <button class="primary" onclick="rangeStart()">수집 시작</button>
        <button onclick="rangeStatusShow()">진행 확인</button>
        <button onclick="rangeCancel()">취소</button>
      </div>
      <div id="rgMsg" class="meta-line" style="white-space:pre-line;"></div>
      <details class="adv"><summary>설명</summary>
        <div class="meta-line">• 지정한 기간의 <b>날짜별 전체 시세</b>를 위 저장 기준(거래대금·시가총액)으로 걸러 받아 저장합니다. 이미 있는 날짜·종목은 건드리지 않고(쓰기 0), 시가총액이 비어 있던 행만 채웁니다.<br />
        • 12-1 모멘텀·52주 고가는 약 253거래일(1년+)의 이력이 필요한데 기본 수집은 약 250달력일(≈170거래일)이라 「자료 부족」일 수 있습니다. <b>과거 쪽으로 기간을 늘리면 이 항목이 채워집니다.</b><br />
        • 서버가 <b>4분에 1쪽(250행)</b>씩, 하루 최대 약 15,000행까지만 처리해 무료 쓰기 한도(하루 10만 건)를 지킵니다. 하루치 전 종목(약 1,000~1,500행)당 약 20분, 하루에 약 10거래일 속도라 긴 기간은 며칠 걸립니다. 화면을 꺼도 계속 진행되고, 한도에 닿으면 다음 날 이어서 합니다.<br />
        • 한 번에 최대 400일, 종료일은 오늘 이전. 주말은 건너뛰고 공휴일은 자동으로 감지해 건너뜁니다.</div>
      </details>
    </div>

    <div class="card">
      <div class="card-title">2. 재무 데이터 (DART)</div>
      <div class="row">
        <button onclick="toggleDbList()">DB 저장 종목 보기</button>
        <span class="meta-line" style="margin:0;">개별 종목은 종목분석에서 "분석하기"를 누르면 없을 때 자동으로 받습니다.</span>
      </div>
      <div class="row" style="margin-top:8px;">
        <button onclick="checkFiscalMonths()" title="저장된 종목의 결산월을 DART 회사개요에서 확인합니다">결산월 확인 (12월 결산이 아닌 종목 찾기)</button>
        <span class="meta-line" style="margin:0;">12월 결산이 아닌 회사는 공시시점 주가·PER 등 날짜 기반 값이 틀어져 백테스트에서 제외하고 경고합니다.</span>
      </div>
      <div id="fiscalStatus" class="meta-line" style="white-space:pre-line;"></div>
      <div id="dbListWrap" style="display:none; margin-top:10px; padding:10px; background:var(--bg); border:1px solid var(--border); border-radius:8px;">
        <div class="row" style="align-items:center;">
          <input id="dbListFilter" oninput="renderDbList()" placeholder="종목명/종목코드로 거르기" style="flex:1; min-width:140px;" />
          <button onclick="toggleDbList(true)" style="padding:6px 10px; font-size:13px;">새로고침</button>
        </div>
        <div class="row" style="gap:6px; align-items:center; margin-top:6px;"><select id="dbListInd" onchange="renderDbList()"><option value="">업종: 전체</option></select><label style="font-size:12.5px;"><input type="checkbox" id="dbListGrp" onchange="renderDbList()"> 업종별로 묶기</label><button type="button" onclick="indFillList('dbList')" style="padding:4px 8px; font-size:12px;" title="업종을 아직 모르는 종목을 DART에서 받아 저장합니다(종목당 1회)">업종 받기</button><span id="dbListIndMsg" class="meta-line"></span></div>
        <div id="dbListCount" style="font-size:12px; color:var(--text-muted); margin:6px 0;"></div>
        <div id="dbListBody" style="display:flex; flex-wrap:wrap; gap:6px; max-height:220px; overflow:auto;"></div>
      </div>
    </div>

    <div class="card">
      <div class="card-title">3. 거시경제 데이터 (환율·금리·WTI)</div>
      <p class="meta-line" style="margin-top:0;">
        원/달러 환율·통안증권(1년)·국고채(3년)·국고채(10년)는 한국은행 ECOS, WTI는 미국 FRED에서 최근 9년치를 받아옵니다. 한 번 받아두면 거시·상관 탭이 계속 재사용합니다.
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

    // ===== 접근 키 보관 · 잠금 화면 · 모든 /api 호출에 키 자동 첨부 =====
    // 키는 영문·숫자·기호(ASCII)만 가능하다. 태블릿 키보드가 한글/전각 문자·보이지 않는 문자를 섞으면 전송 자체가 실패하므로 입력 단계에서 걸러 안내한다.
    function keyClean(v) { return String(v == null ? '' : v).replace(/[\\u200B-\\u200D\\uFEFF\\s]/g, ''); }
    function keyBad(k) { return /[^\\x21-\\x7e]/.test(k) ? '키에 한글·전각 문자 등 영문 키보드로 칠 수 없는 글자가 섞여 있습니다. 키보드를 영어로 바꾸고 다시 입력하세요(복사해 붙여넣는 경우 앞뒤 공백·특수 따옴표 확인).' : ''; }
    function keyGet() {
      try { if (typeof PFU !== 'undefined' && PFU.key && !keyBad(PFU.key)) return PFU.key; } catch (e) {}
      try { const s = sessionStorage.getItem('pfKey'); if (s && !keyBad(s)) return s; } catch (e) {}
      try { const r = JSON.parse(localStorage.getItem('pfKeyR') || 'null'); if (r && r.k && !keyBad(r.k) && r.exp > Date.now()) return r.k; if (r) localStorage.removeItem('pfKeyR'); } catch (e) {}
      return '';
    }
    function keySave(k, remember) {
      try { sessionStorage.setItem('pfKey', k); } catch (e) {}
      try { if (remember) localStorage.setItem('pfKeyR', JSON.stringify({ k: k, exp: Date.now() + 30 * 86400000 })); else localStorage.removeItem('pfKeyR'); } catch (e) {}
    }
    function keyClear() {
      try { sessionStorage.removeItem('pfKey'); } catch (e) {}
      try { localStorage.removeItem('pfKeyR'); } catch (e) {}
      try { if (typeof PFU !== 'undefined') PFU.key = ''; } catch (e) {}
    }
    function keyRemembered() { try { return !!localStorage.getItem('pfKeyR'); } catch (e) { return false; } }
    const _fetch0 = window.fetch.bind(window);
    window.fetch = function (input, init) {
      let url = ''; try { url = typeof input === 'string' ? input : ((input && input.url) || ''); } catch (e) {}
      const own = url.indexOf('/api/') === 0 || url.indexOf(location.origin + '/api/') === 0;
      if (!own) return _fetch0(input, init);
      const k = keyGet();
      if (k) { init = Object.assign({}, init || {}); const h = new Headers(init.headers || {}); if (!h.has('x-pf-key')) h.set('x-pf-key', k); init.headers = h; }
      return _fetch0(input, init).then(function (res) {
        if (res.status === 401 && url.indexOf('/api/pf/') < 0 && url.indexOf('/api/auth/') < 0) appLock('접근 키가 필요하거나 맞지 않습니다. 다시 입력하세요.', true);
        return res;
      });
    };
    let appBooted = false, lockedAt = 0;
    function appLock(msg, keepNothing) {
      keyClear();
      try { if (typeof PFU !== 'undefined') { PFU.st = null; } } catch (e) {}
      ['pfuSummary', 'pfuAlerts', 'pfuHold'].forEach(function (i) { const e = document.getElementById(i); if (e) e.innerHTML = ''; });
      const m = document.getElementById('pfuMain'), g = document.getElementById('pfuGate'); if (m) m.style.display = 'none'; if (g) g.style.display = '';
      const f = document.getElementById('pfuFab'); if (f) f.style.display = 'none';
      const sh = document.getElementById('pfuSheet'); if (sh) sh.style.display = 'none';
      closeMore();
      document.getElementById('lkMsg').textContent = msg || ''; document.getElementById('lkKey').value = '';
      document.getElementById('lockScreen').style.display = 'flex';
      setTimeout(function () { try { document.getElementById('lkKey').focus(); } catch (e) {} }, 60);
    }
    function showLock(msg) { appLock(msg); }
    async function lockSubmit() {
      const msg = document.getElementById('lkMsg'); const k = keyClean(document.getElementById('lkKey').value);
      if (!k) { msg.textContent = '키를 입력하세요.'; return; }
      if (keyBad(k)) { msg.style.color = ''; msg.textContent = keyBad(k); return; }
      msg.style.color = '#64748b'; msg.textContent = '확인 중...';
      try {
        const res = await _fetch0('/api/auth/status', { cache: 'no-store', headers: { 'x-pf-key': k } });
        const d = await res.json().catch(function () { return {}; });
        msg.style.color = '';
        if (res.status === 429) { msg.textContent = d.error || '시도가 너무 많습니다. 잠시 후 다시 시도하세요.'; return; }
        if (!d.ok) { msg.textContent = '키가 맞지 않습니다.'; return; }
        keySave(k, document.getElementById('lkRem').checked);
        document.getElementById('lockScreen').style.display = 'none'; document.getElementById('lkKey').value = '';
        initLists();
        if (!appBooted) { appBooted = true; bootRoute(); }
        else {
          try { if (document.getElementById('tabPf').style.display !== 'none' && PFU.inited && !PFU.key) { document.getElementById('pfuKey').value = k; pfuUnlock(); } } catch (e) {}
          if (curTab === 'momentum' && !document.querySelector('#tabMomentum table')) { try { loadScreen('top', true); } catch (e) {} }
        }
      } catch (e) { msg.style.color = ''; msg.textContent = '확인하지 못했습니다: ' + e.message; }
    }
    async function bootAuth() {
      try {
        const k = keyGet();
        const res = await _fetch0('/api/auth/status', { cache: 'no-store', headers: k ? { 'x-pf-key': k } : {} });
        const d = await res.json().catch(function () { return null; });
        if (res.status === 429) { appLock((d && d.error) || '시도가 너무 많습니다.'); return false; }
        if (d && d.protected && !d.ok) { appLock(''); return false; }
      } catch (e) { /* 상태를 못 읽으면(오프라인 등) 기존처럼 계속 */ }
      return true;
    }
    // 개인정보 가림막: 앱 전환기(멀티태스킹) 미리보기에 계좌 내용이 보이지 않게 하고, 5분 이상 자리를 비우면 잠근다.
    document.addEventListener('visibilitychange', function () {
      const cv = document.getElementById('privCover'); if (!cv) return;
      if (document.visibilityState === 'hidden') { lockedAt = Date.now(); if (curTab === 'pf' && keyGet()) cv.style.display = 'flex'; }
      else {
        cv.style.display = 'none';
        if (lockedAt && Date.now() - lockedAt > 5 * 60000 && keyGet() && !keyRemembered()) appLock('오래 자리를 비워 잠겼습니다. 키를 다시 입력하세요.');
        lockedAt = 0;
      }
    });
    window.addEventListener('pagehide', function () { const cv = document.getElementById('privCover'); if (cv && curTab === 'pf' && keyGet()) cv.style.display = 'flex'; });
    window.addEventListener('pageshow', function () { const cv = document.getElementById('privCover'); if (cv) cv.style.display = 'none'; });

    // ===== 메뉴 이동: 하단 메뉴바 · 더보기 · 뒤로가기/링크(#탭) · 마지막 탭 기억 =====
    const TAB_IDS = ['momentum', 'fscr', 'financial', 'macro', 'data', 'bt', 'pf', 'db', 'guide'];
    let curTab = 'financial';
    function toggleMore() { const e = document.getElementById('moreSheet'); e.style.display = e.style.display === 'block' ? 'none' : 'block'; }
    function closeMore() { const e = document.getElementById('moreSheet'); if (e) e.style.display = 'none'; }
    // 메뉴는 4개 묶음(발굴·종목·내 투자·검증/관리). 묶음 안의 화면은 위쪽 보조 메뉴(#grpNav)로 옮긴다. 화면(탭) 자체와 switchTab은 그대로다.
    const TAB_GROUP = { momentum: 'find', fscr: 'find', macro: 'find', financial: 'stock', pf: 'my', bt: 'admin', data: 'admin', db: 'admin', guide: 'admin' };
    const GROUP_FIRST = { find: 'momentum', stock: 'financial', my: 'pf', admin: 'bt' };
    const grpLast = {};
    function switchGroup(g) { switchTab(grpLast[g] || GROUP_FIRST[g]); }
    function updateBnav(tab) {
      const g = TAB_GROUP[tab]; grpLast[g] = tab;
      Object.keys(GROUP_FIRST).forEach(function (k) {
        const b = document.getElementById('bn_' + k); if (b) b.className = k === g ? 'on' : '';
        const t = document.getElementById('grpBtn_' + k); if (t) t.className = k === g ? 'toggle-active' : '';
      });
      const mb = document.getElementById('bn_more'); if (mb) mb.className = '';
      const gn = document.getElementById('grpNav'); if (gn) {
        let n = 0; Array.prototype.forEach.call(gn.querySelectorAll('button'), function (b) { const on = b.getAttribute('data-grp') === g; b.style.display = on ? '' : 'none'; if (on) n++; });
        gn.style.display = n > 1 ? 'flex' : 'none';
      }
    }
    function navRecord(tab, mode) {
      try { if (mode === 'replace') history.replaceState({ tab: tab }, '', '#' + tab); else if (mode !== 'none') history.pushState({ tab: tab }, '', '#' + tab); } catch (e) {}
      try { localStorage.setItem('lastTab', tab); } catch (e) {}
    }
    // ===== 데이터 기준일 줄 =====
    // 시세는 평일 한국시간 18:01부터 갱신된다. 마지막으로 갱신됐어야 할 평일 날짜와 실제 저장된 마지막 날짜의 평일 차이를 보여준다.
    // 한국 공휴일은 알 수 없어(휴장일이면 정상) 경고 문구에 그 가능성을 적는다.
    function freshExpected(nowMs) {
      const k = new Date(nowMs + 9 * 3600000); // UTC 필드를 한국 시각으로 읽기 위해 +9시간
      let d = Date.UTC(k.getUTCFullYear(), k.getUTCMonth(), k.getUTCDate());
      const afterUpdate = k.getUTCHours() * 60 + k.getUTCMinutes() >= 18 * 60 + 30;
      if (!afterUpdate) d -= 86400000;
      while (new Date(d).getUTCDay() % 6 === 0) d -= 86400000; // 토·일 건너뛰기
      return d;
    }
    function freshLag(marketLast, nowMs) {
      const m = String(marketLast || '');
      if (!/^\\d{8}$/.test(m)) return null;
      let a = Date.UTC(Number(m.slice(0, 4)), Number(m.slice(4, 6)) - 1, Number(m.slice(6, 8)));
      const b = freshExpected(nowMs); let n = 0;
      while (a < b) { a += 86400000; if (new Date(a).getUTCDay() % 6 !== 0) n++; }
      return n;
    }
    const fmtYmd = (s) => /^\\d{8}$/.test(String(s || '')) ? s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6) : (s || '없음');
    let freshAt = 0;
    function freshRender(d, nowMs) {
      const el = document.getElementById('freshBar'); if (!el) return;
      el.style.display = 'block';
      if (!d) { el.className = 'err'; el.textContent = '데이터 기준일을 불러오지 못했습니다. 표시된 시세·순위가 최신이 아닐 수 있습니다 (눌러서 데이터 관리 열기)'; return; }
      const lag = freshLag(d.market_last, nowMs);
      const sc = d.screen_last ? ' · 저장된 주간 스크리닝 ' + fmtYmd(d.screen_last) : '';
      if (lag == null) { el.className = 'warn'; el.textContent = '⚠ 시세가 아직 저장되지 않았습니다' + sc + ' (눌러서 데이터 관리 열기)'; return; }
      if (lag >= 1) { el.className = 'warn'; el.textContent = '⚠ 시세 기준일 ' + fmtYmd(d.market_last) + ' — 평일 기준 ' + lag + '일 늦음(공휴일·휴장이면 정상)' + sc + '. 늦은 시세로 계산한 순위·수익률은 최신이 아닙니다 (눌러서 데이터 관리 열기)'; return; }
      el.className = ''; el.textContent = '📅 시세 기준일 ' + fmtYmd(d.market_last) + ' (최신)' + sc;
    }
    async function freshLoad(force) {
      const now = Date.now(); if (!force && now - freshAt < 600000) return; freshAt = now;
      try { const r = await fetch('/api/data/fresh', { cache: 'no-store' }); const d = await r.json(); if (!r.ok) throw new Error(d.error || ''); freshRender(d, Date.now()); }
      catch (e) { freshAt = 0; freshRender(null, Date.now()); }
    }
    // ===== /데이터 기준일 줄 =====
    function bootRoute() {
      freshLoad(true);
      let t = (location.hash || '').replace('#', '');
      if (TAB_IDS.indexOf(t) < 0) { t = ''; try { t = localStorage.getItem('lastTab') || ''; } catch (e) {} }
      if (TAB_IDS.indexOf(t) < 0) t = 'financial';
      if (t !== 'financial') switchTab(t, 'replace'); else { navRecord('financial', 'replace'); updateBnav('financial'); }
    }
    window.addEventListener('popstate', function (ev) {
      let t = (ev.state && ev.state.tab) || (location.hash || '').replace('#', '');
      if (TAB_IDS.indexOf(t) >= 0 && t !== curTab) switchTab(t, 'none');
    });


    // ===== DB 용량 =====
    function fmtBytes(b) { if (b == null) return 'N/A'; if (b >= 1073741824) return (b / 1073741824).toFixed(2) + ' GB'; if (b >= 1048576) return (b / 1048576).toFixed(1) + ' MB'; if (b >= 1024) return (b / 1024).toFixed(0) + ' KB'; return b + ' B'; }
    let dbUse = null, dbPrev = null, dbStopFlag = false, dbRunning = false;
    function dbBannerApply(u) {
      const el = document.getElementById('dbBanner'); if (!el) return;
      let hide = false; try { hide = sessionStorage.getItem('dbBnrHide') === '1'; } catch (e) {}
      if (u && u.warn && !hide) { document.getElementById('dbBannerTxt').textContent = '⚠ DB 용량 ' + u.pct.toFixed(1) + '% 사용 중 (' + fmtBytes(u.sizeBytes) + ' / ' + fmtBytes(u.limitBytes) + ') — 가득 차면 새 데이터 저장이 실패합니다. 정리가 필요합니다.'; el.style.display = 'flex'; }
      else el.style.display = 'none';
    }
    function dbBannerHide() { try { sessionStorage.setItem('dbBnrHide', '1'); } catch (e) {} document.getElementById('dbBanner').style.display = 'none'; }
    async function dbBannerCheck() {
      try { const res = await fetch('/api/db/usage?light=1', { cache: 'no-store' }); if (!res.ok) return; dbBannerApply(await res.json()); } catch (e) { /* 알림 실패는 조용히 */ }
    }
    async function dbLoad(refresh) {
      const g = document.getElementById('dbGauge'); g.textContent = '불러오는 중…';
      try {
        const res = await fetch('/api/db/usage' + (refresh ? '?refresh=1' : ''), { cache: 'no-store' }); const d = await res.json();
        if (!res.ok) throw new Error(d.error || '조회 실패');
        dbUse = d; dbRender(); dbBannerApply(d);
      } catch (e) { g.textContent = '용량을 불러오지 못했습니다: ' + e.message; }
    }
    function dbRender() {
      const d = dbUse; if (!d) return;
      const g = document.getElementById('dbGauge');
      if (d.sizeBytes == null) { g.innerHTML = '이 환경에서는 DB 크기를 알 수 없습니다(D1이 크기를 알려주지 않음). 아래 행 수만 참고하세요.'; }
      else {
        const cls = d.pct >= d.warnPct ? 'bad' : (d.pct >= d.warnPct - 20 ? 'warn' : '');
        g.innerHTML = '<div class="db-big">' + d.pct.toFixed(1) + '%</div><div class="db-bar"><i class="' + cls + '" style="width:' + Math.min(100, d.pct).toFixed(1) + '%"></i></div>' + fmtBytes(d.sizeBytes) + ' 사용 / 한도 ' + fmtBytes(d.limitBytes) + ' · 알림 기준 ' + d.warnPct + '%' + (d.warn ? '<br /><b style="color:#b91c1c;">⚠ 알림 기준을 넘었습니다. 아래에서 데이터를 정리하세요.</b>' : '');
      }
      document.getElementById('dbLimitMb').value = d.limitMb; document.getElementById('dbWarnPct').value = d.warnPct;
      document.getElementById('dbMeasured').textContent = d.measured ? '(실측)' : ((d.tables || []).some(function (t) { return t.capped; }) ? '(행 수는 표당 최대 20,000행까지만 세어 "+"로 표시 — 읽기 한도 보호. 표별 용량은 측정 불가)' : '(행 수 기반 추정치 — 정확한 값이 아님)');
      const tot = d.sizeBytes || (d.tables || []).reduce(function (a, t) { return a + (t.bytes || 0); }, 0) || 1;
      document.getElementById('dbTables').innerHTML = '<table><thead><tr><th>테이블</th><th>행 수</th><th>용량</th><th>비중</th></tr></thead><tbody>' + (d.tables || []).map(function (t) { return '<tr><td>' + escHtml(t.name) + '</td><td>' + (t.rows == null ? 'N/A' : t.rows.toLocaleString('ko-KR') + (t.capped ? '+' : '')) + '</td><td>' + fmtBytes(t.bytes) + '</td><td>' + (t.bytes == null ? 'N/A' : (t.bytes / tot * 100).toFixed(1) + '%') + '</td></tr>'; }).join('') + '</tbody></table>';
      document.getElementById('dbCapTxt').textContent = '하루 ' + d.purgeCap.perDay.toLocaleString('ko-KR') + '행 (오늘 남은 한도 ' + d.purgeCap.left.toLocaleString('ko-KR') + '행)';
      document.getElementById('dbMsg').textContent = '표의 행 수는 읽기 한도를 쓰므로 1시간 동안 저장된 값을 보여줍니다' + (d.tablesAt ? ' (계산: ' + new Date(d.tablesAt).toLocaleString('ko-KR') + ')' : '') + '. ' + (d.protected ? '' : '※ 접근 키(PF_KEY)가 없어 삭제 기능은 잠겨 있습니다.');
      dbKindChange();
    }
    async function dbSaveSettings() {
      const r = await adminFetch('/api/db/settings', { limit_mb: Number(document.getElementById('dbLimitMb').value), warn_pct: Number(document.getElementById('dbWarnPct').value) });
      document.getElementById('dbMsg').textContent = r.ok ? '저장했습니다.' : ('저장 실패: ' + ((r.data && r.data.error) || r.status));
      if (r.ok) dbLoad(false);
    }
    const DB_KIND_HELP = {
      price_old: '지정한 날짜 이전의 일별 시세(종가·거래량)를 지웁니다. 모멘텀·52주 계산에 필요한 최근 약 1년 치(380일)는 지울 수 없습니다. 날짜는 YYYYMMDD입니다.',
      price_inactive: '현재 ④에서 정한 저장 기준(거래대금·시가총액)을 통과하지 못하는 종목의 과거 시세를 지웁니다. 기준을 높인 뒤 용량을 줄일 때 씁니다.',
      screen_old: '저장된 주간 스크리닝 결과 중 최근 N회만 남기고 지웁니다(결과는 다시 계산할 수 있습니다).',
      fin_company: '한 종목의 저장된 DART 재무데이터를 모두 지웁니다. 다시 필요하면 종목분석에서 새로 받을 수 있습니다.',
      fin_old: '전 종목에서 지정 연도 미만의 재무데이터를 지웁니다. 10년 추세·백테스트에 쓰이므로 신중히 정하세요.',
      opincome_cache: '스크리닝 화면에 쓰는 영업이익 조회 캐시를 비웁니다. 다음 스크리닝 때 필요한 만큼 다시 조회합니다.',
    };
    function dbKindChange() {
      const k = document.getElementById('dbKind').value; document.getElementById('dbKindHelp').textContent = DB_KIND_HELP[k] || '';
      const box = document.getElementById('dbParam'); const prevK = box.getAttribute('data-k'); if (prevK === k) return; box.setAttribute('data-k', k);
      const dt = new Date(Date.now() - 420 * 86400000).toISOString().slice(0, 10).replace(/-/g, '');
      box.innerHTML = k === 'price_old' ? '<input id="dbP" type="text" inputmode="numeric" maxlength="8" value="' + dt + '" placeholder="기준일 YYYYMMDD" style="flex:1;" />'
        : k === 'screen_old' ? '<label style="font-size:13px;">최근 <input id="dbP" type="number" inputmode="numeric" min="1" max="104" value="8" style="width:80px;" /> 회만 남김</label>'
        : k === 'fin_company' ? '<span class="combo"><input id="dbP" placeholder="종목명" autocomplete="off" /></span>'
        : k === 'fin_old' ? '<label style="font-size:13px;"><input id="dbP" type="number" inputmode="numeric" min="2000" value="2015" style="width:100px;" /> 년 미만 삭제</label>' : '';
      if (k === 'fin_company') attachCombo(document.getElementById('dbP'));
      dbPrev = null; document.getElementById('dbPreviewBox').textContent = ''; document.getElementById('dbRunBtn').style.display = 'none';
    }
    function dbBody() {
      const k = document.getElementById('dbKind').value; const e = document.getElementById('dbP'); const v = e ? e.value.trim() : '';
      const param = k === 'price_old' ? { before: v } : k === 'screen_old' ? { keep: Number(v) } : k === 'fin_company' ? { corp_name: v } : k === 'fin_old' ? { before_year: Number(v) } : {};
      return { kind: k, param: param };
    }
    async function dbPreview() {
      const box = document.getElementById('dbPreviewBox'); document.getElementById('dbRunBtn').style.display = 'none'; dbPrev = null;
      box.style.color = ''; box.textContent = '계산 중…';
      const b = dbBody(); b.preview = true;
      const r = await adminFetch('/api/db/purge', b);
      if (!r.ok) { box.style.color = '#b91c1c'; box.textContent = (r.data && r.data.error) || ('오류 ' + r.status); return; }
      const d = r.data; dbPrev = { body: dbBody(), count: d.count, label: d.label, capped: !!d.capped };
      if (!d.count) { box.textContent = '「' + d.label + '」 — 지울 데이터가 없습니다.'; return; }
      box.innerHTML = '「' + escHtml(d.label) + '」 <b>' + d.count.toLocaleString('ko-KR') + (d.capped ? '행 이상' : '행') + '</b> 삭제 대상 · 쓰기 약 ' + d.estWrites.toLocaleString('ko-KR') + '건 · 하루 ' + d.perDay.toLocaleString('ko-KR') + '행씩 지우면 약 <b>' + d.daysNeeded + '일</b> (오늘 남은 한도 ' + d.capLeft.toLocaleString('ko-KR') + '행)';
      document.getElementById('dbRunBtn').style.display = '';
    }
    function dbStop() { dbStopFlag = true; }
    async function dbRun() {
      if (!dbPrev || dbRunning) return;
      if (!window.confirm('「' + dbPrev.label + '」 약 ' + dbPrev.count.toLocaleString('ko-KR') + (dbPrev.capped ? '행 이상' : '행') + '을 삭제합니다. 되돌릴 수 없습니다. 계속할까요?')) return;
      dbRunning = true; dbStopFlag = false; const out = document.getElementById('dbRunBox'); out.style.color = '';
      document.getElementById('dbRunBtn').style.display = 'none'; document.getElementById('dbStopBtn').style.display = '';
      const before = dbUse ? dbUse.sizeBytes : null; let total = 0, last = null, msg = '';
      try {
        for (let i = 0; i < 400; i++) {
          if (dbStopFlag) { msg = '중단했습니다.'; break; }
          const r = await adminFetch('/api/db/purge', dbPrev.body);
          if (!r.ok) { out.style.color = '#b91c1c'; msg = (r.data && r.data.error) || ('오류 ' + r.status); break; }
          total += r.data.deleted; last = r.data; out.textContent = '삭제 중… ' + total.toLocaleString('ko-KR') + '행';
          if (r.data.done || r.data.deleted === 0) { msg = r.data.done ? '완료.' : ''; break; }
        }
      } catch (e) { out.style.color = '#b91c1c'; msg = '오류: ' + e.message; }
      dbRunning = false; document.getElementById('dbStopBtn').style.display = 'none';
      out.textContent = msg + ' 총 ' + total.toLocaleString('ko-KR') + '행 삭제' + (last && last.sizeAfter != null && before != null ? ' · 용량 ' + fmtBytes(before) + ' → ' + fmtBytes(last.sizeAfter) + ' (SQLite는 빈 공간을 재사용하므로 표시 용량이 바로 줄지 않을 수 있습니다)' : '') + '.';
      dbPrev = null; dbLoad(true);
    }
    // ===== 표 도구: 글자 크기 · 전체화면 =====
    let tblFs = 13; try { tblFs = Number(localStorage.getItem('tblFs')) || 13; } catch (e) {}
    function tblApply() { document.documentElement.style.setProperty('--tfs', tblFs + 'px'); }
    function tblZoom(d) { tblFs = Math.max(10, Math.min(18, tblFs + d)); tblApply(); try { localStorage.setItem('tblFs', String(tblFs)); } catch (e) {} }
    let tblFullId = '';
    function tblFull(id) { tblFullClose(); const el = document.getElementById(id); if (!el) return; el.classList.add('tfull'); tblFullId = id; document.getElementById('tblClose').style.display = 'block'; document.body.style.overflow = 'hidden'; }
    function tblFullClose() { if (tblFullId) { const el = document.getElementById(tblFullId); if (el) el.classList.remove('tfull'); } tblFullId = ''; const c = document.getElementById('tblClose'); if (c) c.style.display = 'none'; document.body.style.overflow = ''; }
    document.addEventListener('keydown', function (ev) { if (ev.key === 'Escape') tblFullClose(); });
    tblApply();
    // ===== 종목 비교 바구니(최대 5) =====
    const CMP_MAX = 5; let cmpList = [];
    try { const l = JSON.parse(localStorage.getItem('cmpList') || '[]'); if (Array.isArray(l)) cmpList = l.filter(function (x) { return typeof x === 'string' && x; }).slice(0, CMP_MAX); } catch (e) {}
    let cmpMsgT = 0;
    function cmpMsg(t) { const el = document.getElementById('cmpToast'); el.textContent = t; el.style.display = 'block'; clearTimeout(cmpMsgT); cmpMsgT = setTimeout(function () { el.style.display = 'none'; }, 3500); }
    function cmpSave() { try { localStorage.setItem('cmpList', JSON.stringify(cmpList)); } catch (e) {} cmpTrayRender(); }
    function cmpToggleName(n) {
      n = String(n || '').trim(); if (!n) return;
      const i = cmpList.indexOf(n);
      if (i >= 0) cmpList.splice(i, 1);
      else if (cmpList.length >= CMP_MAX) { cmpMsg('비교는 최대 ' + CMP_MAX + '종목까지입니다. 아래 바에서 하나를 빼고 담아주세요.'); return; }
      else cmpList.push(n);
      cmpSave();
    }
    function cmpToggleCurrent() { const n = document.getElementById('corpName').value.trim(); if (!n) { cmpMsg('먼저 종목명을 입력하거나 종목을 분석하세요.'); return; } cmpToggleName(n); }
    function cmpTrayRender() {
      const tr = document.getElementById('cmpTray'); if (!tr) return;
      const inPanel = document.getElementById('cmpPanel').style.display !== 'none';
      document.body.classList.toggle('has-tray', cmpList.length > 0 && !inPanel);
      if (!cmpList.length || inPanel) { tr.style.display = 'none'; tr.innerHTML = ''; }
      else {
        tr.style.display = 'flex';
        tr.innerHTML = '<span style="font-size:13px;">⚖ ' + cmpList.length + '/' + CMP_MAX + '</span>' + cmpList.map(function (n, i) { return '<span class="ct-chip">' + escHtml(n) + '<button class="ct-x" data-cmprm="' + i + '" title="빼기">✕</button></span>'; }).join('') +
          '<button class="ct-go" data-cmpgo="1">' + (cmpList.length >= 2 ? '비교하기' : '1종목 더 담기') + '</button><button class="ct-clr" data-cmpclr="1">비우기</button>';
      }
      Array.prototype.forEach.call(document.querySelectorAll('[data-cmp]'), function (b) {
        const on = cmpList.indexOf(b.getAttribute('data-cmp')) >= 0; b.classList.toggle('on', on);
        b.textContent = b.classList.contains('sc-cmp-s') ? (on ? '✓' : '⚖') : (on ? '✓ 담김' : '⚖ 비교 담기');
      });
      const cur = (document.getElementById('corpName') || {}).value; const bt = document.getElementById('btnCmpAdd');
      if (bt) { const on = !!cur && cmpList.indexOf(cur.trim()) >= 0; bt.classList.toggle('on', on); bt.textContent = on ? '✓ 비교 담김' : '⚖ 비교 담기' + (cmpList.length ? ' (' + cmpList.length + '/' + CMP_MAX + ')' : ''); }
    }
    document.addEventListener('click', function (ev) {
      let b = ev.target.closest('[data-cmp]'); if (b) { cmpToggleName(b.getAttribute('data-cmp')); return; }
      b = ev.target.closest('[data-cmprm]'); if (b) { cmpList.splice(Number(b.getAttribute('data-cmprm')), 1); cmpSave(); return; }
      if (ev.target.closest('[data-cmpclr]')) { cmpList = []; cmpSave(); return; }
      if (ev.target.closest('[data-cmpgo]')) { if (cmpList.length >= 2) cmpOpen(true); else { cmpOpen(false); } }
    });
    function cmpOpen(run) {
      if (curTab !== 'financial') switchTab('financial');
      tblFullClose();
      document.getElementById('stockBody').setAttribute('data-was', document.getElementById('stockBody').style.display === 'none' ? 'hidden' : 'shown');
      document.getElementById('stockBody').style.display = 'none';
      document.getElementById('cmpPanel').style.display = ''; document.getElementById('searchCard').style.display = 'none'; cmpTrayRender();
      [0, 1, 2, 3, 4].forEach(function (i) { const e = document.getElementById('cmpName' + i); if (cmpList[i] !== undefined) e.value = cmpList[i]; });
      window.scrollTo(0, 0);
      if (run === true) compareStocks();
      else { const first = [0, 1, 2, 3, 4].filter(function (i) { return !document.getElementById('cmpName' + i).value.trim(); })[0]; if (first !== undefined) setTimeout(function () { document.getElementById('cmpName' + first).focus(); }, 80); }
    }
    function cmpClose() {
      document.getElementById('cmpPanel').style.display = 'none'; document.getElementById('searchCard').style.display = ''; cmpTrayRender();
      const sb = document.getElementById('stockBody'); if (sb.getAttribute('data-was') === 'shown') sb.style.display = '';
      window.scrollTo(0, 0);
    }
    function cmpClearSlot(i) { document.getElementById('cmpName' + i).value = ''; document.getElementById('cmpPrice' + i).value = ''; const n = [0, 1, 2, 3, 4].map(function (k) { return document.getElementById('cmpName' + k).value.trim(); }).filter(Boolean); cmpList = n; cmpSave(); }
    // ===== 입력 효율: 모바일 키보드 종류·엔터키 문구·자동 대문자/교정 끄기 =====
    function tuneInputs() {
      Array.prototype.forEach.call(document.querySelectorAll('input,textarea'), function (el) {
        if (el.__tuned) return; el.__tuned = 1;
        const t = (el.getAttribute('type') || 'text').toLowerCase();
        if (t === 'number' && !el.hasAttribute('inputmode') && el.getAttribute('min') !== null && Number(el.getAttribute('min')) >= 0) el.setAttribute('inputmode', 'decimal'); // 음수가 필요한 칸은 기본 숫자 키보드(마이너스 포함)를 유지
        if ((t === 'text' || t === 'number' || t === 'search') && !el.hasAttribute('enterkeyhint')) el.setAttribute('enterkeyhint', el.closest('.combo') ? 'search' : 'done');
        if ((t === 'text' || t === 'search') && !el.hasAttribute('autocapitalize')) el.setAttribute('autocapitalize', 'off');
        if (el.closest('.combo')) { el.setAttribute('autocorrect', 'off'); el.setAttribute('spellcheck', 'false'); }
      });
    }
    try { let _tq = 0; new MutationObserver(function () { if (_tq) return; _tq = requestAnimationFrame(function () { _tq = 0; tuneInputs(); }); }).observe(document.body, { childList: true, subtree: true }); } catch (e) {}

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

    // ===== 종목 검색 콤보박스: 입력하는 즉시 후보·저장 여부를 보여주고, 없는 이름은 서버에 묻기 전에 알려준다 =====
    let dbCompanies = null; // DB에 저장된 종목 목록 캐시(아래 검색·저장 종목 패널과 loadDbCompanies가 함께 씀)
    let STOCK_LIST = null, STOCK_LIST_P = null;
    const CHO = ['ㄱ','ㄲ','ㄴ','ㄷ','ㄸ','ㄹ','ㅁ','ㅂ','ㅃ','ㅅ','ㅆ','ㅇ','ㅈ','ㅉ','ㅊ','ㅋ','ㅌ','ㅍ','ㅎ'];
    function choseongOf(str) {
      let o = '';
      for (const ch of String(str)) { const c = ch.charCodeAt(0); o += (c >= 0xAC00 && c <= 0xD7A3) ? CHO[Math.floor((c - 0xAC00) / 588)] : ch; }
      return o;
    }
    function loadStockList() {
      if (STOCK_LIST) return Promise.resolve(STOCK_LIST);
      if (STOCK_LIST_P) return STOCK_LIST_P;
      STOCK_LIST_P = fetch('/api/stock/list').then((r) => { if (!r.ok) throw new Error('stock list ' + r.status); return r.json(); }).then((d) => {
        STOCK_LIST = (d.stocks || []).map((x) => ({ n: x.n, c: x.c, nl: x.n.toLowerCase(), cho: choseongOf(x.n) }));
        return STOCK_LIST;
      }).catch(() => { STOCK_LIST_P = null; return null; });
      return STOCK_LIST_P;
    }
    function savedMap() {
      const m = {};
      (dbCompanies || []).forEach((c) => { m[c.corp_name] = c; });
      return m;
    }
    function searchStocks(q, limit) {
      q = String(q || '').trim().toLowerCase();
      if (!q || !STOCK_LIST) return [];
      const isCho = /^[ㄱ-ㅎ]+$/.test(q), isNum = /^[0-9]+$/.test(q), sm = savedMap();
      const out = [];
      for (const s of STOCK_LIST) {
        let sc = -1;
        if (s.nl === q) sc = 0;
        else if (s.nl.indexOf(q) === 0) sc = 1;
        else if (isNum && s.c.indexOf(q) === 0) sc = 2;
        else if (s.nl.indexOf(q) > 0) sc = 3;
        else if (isCho && s.cho.indexOf(q) === 0) sc = 4;
        else if (isCho && s.cho.indexOf(q) > 0) sc = 5;
        if (sc >= 0) out.push({ s: s, sc: sc, saved: sm[s.n] ? 0 : 1 });
      }
      out.sort((a, b) => a.sc - b.sc || a.saved - b.saved || a.s.n.length - b.s.n.length || a.s.n.localeCompare(b.s.n));
      return out.slice(0, limit || 8).map((x) => x.s);
    }
    function savedBadge(name) {
      const c = savedMap()[name];
      return c ? { ok: true, text: '저장 ' + c.n + '기간' } : { ok: false, text: '미저장' };
    }
    function updateComboHint(input) {
      const hint = document.getElementById('comboHint_' + input.id);
      if (!hint) return;
      const v = input.value.trim();
      if (!v) { hint.innerHTML = ''; return; }
      if (!STOCK_LIST) { hint.innerHTML = '<span style="color:var(--text-muted);">종목 목록을 불러오는 중…</span>'; return; }
      const exact = STOCK_LIST.filter((s) => s.n === v)[0];
      if (exact && input.getAttribute('data-pf')) { hint.innerHTML = '<span class="good">✔ ' + escHtml(exact.n) + '</span> (' + exact.c + ')'; return; }
      if (exact) {
        const c = savedMap()[exact.n];
        hint.innerHTML = '<span class="good">✔ ' + escHtml(exact.n) + '</span> (' + exact.c + ') · ' + (c
          ? '재무 ' + c.n + '개 기간 저장됨 (' + c.first_year + '~' + c.last_year + ') — Enter로 바로 분석'
          : '<span class="bad">재무 미저장</span> — 분석하면 DART에서 새로 받아옵니다(약 1~2분)');
        return;
      }
      const cand = searchStocks(v, 5);
      if (!cand.length) { hint.innerHTML = '<span class="bad">✖ 일치하는 종목이 없습니다.</span> 상장사 이름·6자리 코드·초성(예: ㅅㅅㅈㅈ)으로 찾아보세요.'; return; }
      hint.innerHTML = '<span class="bad">정확히 일치하는 종목명이 아닙니다.</span> 혹시 ' + cand.map((s) => '<a href="javascript:void(0)" data-pick="' + escHtml(s.n) + '" style="color:var(--primary-dark); font-weight:600;">' + escHtml(s.n) + '</a>').join(' · ') + ' 인가요?';
    }
    // opts.analyze=true면 선택 즉시 분석(종목분석 입력창). 비교 입력창은 값만 채운다.
    function attachCombo(input, opts) {
      opts = opts || {};
      if (opts.pf) input.setAttribute('data-pf', '1');
      const box = document.createElement('div');
      box.className = 'combo-list';
      input.parentNode.appendChild(box);
      let items = [], act = -1;
      const close = () => { box.style.display = 'none'; act = -1; };
      const paint = () => {
        box.innerHTML = items.map((s, i) => {
          const b = savedBadge(s.n);
          return '<div class="combo-item' + (i === act ? ' act' : '') + '" data-i="' + i + '"><b>' + escHtml(s.n) + '</b><span class="cc">' + s.c + '</span><span class="bd ' + (b.ok ? 'ok' : 'no') + '">' + b.text + '</span></div>';
        }).join('');
        box.style.display = items.length ? 'block' : 'none';
      };
      const pick = (s) => {
        input.value = s.n; input._lastPick = s.n; close(); updateComboHint(input);
        if (input.id === 'corpName') updateWatchButtonState();
        if (opts.analyze) analyzeStock();
        if (opts.onPick) opts.onPick(s);
      };
      const refresh = () => {
        const v = input.value.trim();
        items = v ? searchStocks(v, 8) : [];
        act = items.length ? 0 : -1;
        paint(); updateComboHint(input);
      };
      input.addEventListener('focus', () => { loadStockList().then(() => { loadDbCompanies().catch(() => {}).then(() => { if (document.activeElement === input) refresh(); }); }); });
      input.addEventListener('input', () => { loadStockList().then(refresh); refresh(); });
      input.addEventListener('keydown', (ev) => {
        if (ev.key === 'ArrowDown' && items.length) { ev.preventDefault(); act = (act + 1) % items.length; paint(); }
        else if (ev.key === 'ArrowUp' && items.length) { ev.preventDefault(); act = (act - 1 + items.length) % items.length; paint(); }
        else if (ev.key === 'Escape') close();
        else if (ev.key === 'Enter') {
          ev.preventDefault();
          const v = input.value.trim();
          const exact = STOCK_LIST ? STOCK_LIST.filter((s) => s.n === v)[0] : null;
          if (exact) { close(); updateComboHint(input); if (opts.analyze) analyzeStock(); if (opts.onPick) opts.onPick(exact); return; }
          if (items.length) { pick(items[act >= 0 ? act : 0]); return; }
          if (!STOCK_LIST) { close(); if (opts.analyze) analyzeStock(); return; } // 목록을 못 받았을 땐 예전처럼 서버에 맡긴다
          updateComboHint(input);
        }
      });
      box.addEventListener('mousedown', (ev) => {
        const el = ev.target.closest('.combo-item'); if (!el) return;
        ev.preventDefault(); pick(items[Number(el.getAttribute('data-i'))]);
      });
      input.addEventListener('blur', () => setTimeout(() => {
        close();
        if (opts.onPick && STOCK_LIST) { const v = input.value.trim(), ex = STOCK_LIST.filter((x) => x.n === v)[0]; if (ex && input._lastPick !== v) opts.onPick(ex); }
      }, 160));
    }
    function renderSavedBox() {
      const list = dbCompanies || [];
      const sum = document.getElementById('savedSummary');
      if (!sum) return;
      sum.textContent = '📁 DB에 저장된 종목 ' + list.length + '개 — 눌러서 펼치고 바로 분석';
      const f = (document.getElementById('savedFilter').value || '').trim().toLowerCase();
      const fc = /^[ㄱ-ㅎ]+$/.test(f);
      indSyncList(list); indFillSelect('savedInd', list.map((c) => c.stock_code));
      const isel = document.getElementById('savedInd').value, grp = document.getElementById('savedGrp').checked;
      const shown = list.filter((c) => (!f || c.corp_name.toLowerCase().indexOf(f) >= 0 || (fc && choseongOf(c.corp_name).indexOf(f) >= 0) || String(c.stock_code || '').indexOf(f) === 0) && indMatchSel(isel, c.stock_code));
      document.getElementById('savedChips').innerHTML = shown.length
        ? indJoin(shown, (c) => c.stock_code, (c) => '<button type="button" class="saved-chip" data-name="' + escHtml(c.corp_name) + '">' + escHtml(c.corp_name) + '<small>' + c.n + '기간' + indSmall(c.stock_code) + (c.acc_mt && Number(c.acc_mt) !== 12 ? ' · ' + c.acc_mt + '월결산' : '') + '</small></button>', grp)
        : '<span style="font-size:12.5px; color:var(--text-muted);">' + (list.length ? '일치하는 저장 종목이 없습니다.' : '아직 저장된 종목이 없습니다. 종목을 분석하면 자동으로 저장됩니다.') + '</span>';
      const rec = loadRecent().filter((n) => list.some((c) => c.corp_name === n));
      document.getElementById('savedRecent').innerHTML = rec.length ? '<span style="font-size:12px; color:var(--text-muted);">최근 본 종목 </span>' + rec.map((n) => '<button type="button" class="saved-chip recent" data-name="' + escHtml(n) + '">' + escHtml(n) + '</button>').join('') : '';
    }
    const RECENT_KEY = 'fs_recent';
    function loadRecent() { try { return JSON.parse(localStorage.getItem(RECENT_KEY) || '[]'); } catch (e) { return []; } }
    function pushRecent(name) {
      if (!name) return;
      const l = [name].concat(loadRecent().filter((n) => n !== name)).slice(0, 8);
      try { localStorage.setItem(RECENT_KEY, JSON.stringify(l)); } catch (e) { /* 저장 불가여도 무시 */ }
    }
    (function () {
      attachCombo(document.getElementById('corpName'), { analyze: true });
      document.getElementById('corpName').addEventListener('focus', function () { const c = document.getElementById('searchCard'); if (c.classList.contains('compact')) { c.classList.remove('compact'); document.getElementById('btnSearchMore').textContent = '▴ 접기'; } });
      [0, 1, 2, 3, 4].forEach(function (i) { attachCombo(document.getElementById('cmpName' + i)); });
      document.getElementById('comboHint_corpName').addEventListener('click', (ev) => {
        const a = ev.target.closest('[data-pick]'); if (!a) return;
        document.getElementById('corpName').value = a.getAttribute('data-pick'); updateComboHint(document.getElementById('corpName')); updateWatchButtonState(); analyzeStock();
      });
      const sb = document.getElementById('savedBox');
      const goSaved = (ev) => {
        const b = ev.target.closest('.saved-chip'); if (!b) return;
        document.getElementById('corpName').value = b.getAttribute('data-name');
        updateComboHint(document.getElementById('corpName')); updateWatchButtonState(); analyzeStock();
        window.scrollTo({ top: 0, behavior: 'smooth' });
      };
      document.getElementById('savedChips').addEventListener('click', goSaved);
      document.getElementById('savedRecent').addEventListener('click', goSaved);
      document.getElementById('savedFilter').addEventListener('input', renderSavedBox);
      loadStockList();
      loadDbCompanies().then(renderSavedBox).catch(() => { document.getElementById('savedSummary').textContent = '📁 DB 저장 종목 목록을 불러오지 못했습니다'; });
    })();

    // 조회기간 입력 기본값: 최근 10년
    (function () {
      const thisYear = new Date().getFullYear();
      document.getElementById('startYear').value = thisYear - 9;
      document.getElementById('endYear').value = thisYear;
      renderWatchlistChips();
      document.getElementById('corpName').addEventListener('input', updateWatchButtonState);
    })();

    let rawRows = [];      // DB/DART에서 받아온, 가공 안 된 원본 기간별 데이터
    let AN_STOCK = { code: '', ind: '', name: '' }; // 현재 종목의 종목코드·업종코드(업종은 DART 기업개황에서 받아 저장한 값)
    let AN_ACCMT = null;   // 현재 종목의 결산월(모르면 null). 12월이 아니면 날짜 기반 값이 부정확하다는 경고를 띄운다.
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
      const rows = viewMode === 'quarterly' ? toQuarterlyRows(rawRows) : annualWithTTM(rawRows);
      renderTable(rows);
      ensureMacroForTable();
      if (chartSel.length) renderChartPanel(false); // 모드/데이터가 바뀌면 열려 있는 차트도 다시 그림
      syncDockView();
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

    // 연도별 시계열 보기 끝에 붙일 TTM 행: 마지막 사업보고서보다 새로운 1~3분기 자료가 있을 때만(없으면 연간 행 그대로).
    function annualWithTTM(rows) {
      const ann = toAnnualRows(rows);
      const t = buildTTMRow(toQuarterlyRows(rows));
      if (!t) return ann;
      const lastY = ann.length ? Number(ann[ann.length - 1].bsns_year) : 0;
      if (Number(t.row.bsns_year) <= lastY) return ann;
      return ann.concat([{ ...t.row, isTTM: true }]);
    }

    // TTM이 안 나오는 이유를 사람이 읽을 수 있게 설명(null이면 문제 없음 또는 TTM이 필요 없는 상태)
    function ttmDiagnose(rows) {
      const qr = toQuarterlyRows(rows);
      if (!qr.length) return '분기 자료가 저장되어 있지 않습니다.';
      const latest = qr.reduce((a, b) => (b.period_order > a.period_order ? b : a));
      const N = Number(latest.period_order) % 10, Y = Number(latest.bsns_year);
      if (N === 4) return null; // 최신이 사업보고서(4분기)면 TTM 불필요
      const find = (year, q) => qr.find((x) => x.bsns_year === String(year) && x.period_label === year + ' ' + q + '분기');
      const need = []; for (let k = 1; k <= N; k++) need.push([Y, k]); for (let k = N + 1; k <= 4; k++) need.push([Y - 1, k]);
      const miss = []; const nullKeys = [];
      const NAMES = { revenue: '매출액', cogs: '매출원가', operating_income: '영업이익', net_income: '순이익', pretax_income: '세전이익' };
      need.forEach((yq) => {
        const r = find(yq[0], yq[1]);
        if (!r) { miss.push(yq[0] + ' ' + yq[1] + '분기'); return; }
        Object.keys(NAMES).forEach((k) => { if (r[k] == null) nullKeys.push(yq[0] + ' ' + yq[1] + '분기 ' + NAMES[k]); });
      });
      const fs = new Set(need.map((yq) => find(yq[0], yq[1])).filter(Boolean).map((x) => x.fs_div).filter(Boolean));
      const out = [];
      if (miss.length) out.push('필요한 분기 자료가 없습니다: ' + miss.join(', ') + ' (4분기는 사업보고서와 1~3분기가 모두 있어야 단독값을 계산합니다. 해당 기간을 DART에서 다시 조회·저장해 주세요)');
      if (fs.size > 1) out.push('연결(CFS)과 개별(OFS)이 섞여 있어 TTM을 만들지 않습니다');
      if (nullKeys.length) out.push('값이 비어 있는 항목: ' + nullKeys.slice(0, 8).join(', ') + (nullKeys.length > 8 ? ' 외 ' + (nullKeys.length - 8) + '건' : '') + (nullKeys.some((x) => x.indexOf('4분기') >= 0) ? ' (4분기 단독값 = 사업보고서 − 1~3분기이므로, 그 해 1~3분기 중 빠진 기간이나 빈 항목이 있으면 4분기도 비게 됩니다. 해당 연도 분기들을 다시 조회해 주세요)' : ''));
      return out.length ? out.join(' / ') : null;
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
      const annualBase = toAnnualRows(rows);
      if (annualBase.length === 0) return null;
      const annualRows = annualWithTTM(rows); // 사업보고서 없는 최신 분기가 있으면 마지막에 TTM 점 추가
      const steps = compute5StepRows(annualBase);
      const hasTtm = annualRows.length > annualBase.length;
      const ttmInfo = hasTtm ? buildTTMRow(toQuarterlyRows(rows)) : null;
      if (hasTtm && ttmInfo) steps.push(computeStepMetrics(ttmInfo.row, ttmInfo.prior));
      else if (hasTtm) steps.push(computeStepMetrics(annualRows[annualRows.length - 1], null));
      const years = annualRows.map((r) => r.isTTM ? r.bsns_year + ' TTM' : r.bsns_year);
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
      // TTM 점의 성장률은 직전 사업연도가 아니라 "1년 전 같은 분기 기준 TTM"과 비교한다(기간이 같아야 의미가 있음)
      const ttmPrevRow = (hasTtm && ttmInfo && ttmInfo.prior) ? buildTTMFlowForQuarter(toQuarterlyRows(rows), ttmInfo.prior) : null;
      const prevOf = (r, i) => r.isTTM ? ttmPrevRow : annualRows[i - 1];
      const revenueGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = prevOf(r, i); if (!prev) return null;
        return (r.revenue != null && prev.revenue > 0) ? (r.revenue - prev.revenue) / prev.revenue : null;
      });
      const netIncomeGrowth = annualRows.map((r, i) => {
        if (i === 0) return null;
        const prev = prevOf(r, i); if (!prev) return null;
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
      // 금융업 전용(neutral: 좋음/주의 배지를 붙이지 않고 값만 보여 줌 — 레버리지가 높은 것이 정상인 업종이라 일반 기업 눈금을 쓰면 오해를 부른다)
      finRoe: { neutral: true, fmt: (v) => (v * 100).toFixed(2) + '%' },
      finLev: { neutral: true, fmt: (v) => v.toFixed(1) + '배' },
      finNi:  { neutral: true, fmt: (v) => (v * 100).toFixed(2) + '%' },
    };
    function judgeFactor(key, value) {
      if (value == null || !isFinite(value)) return { label: 'N/A', color: '#94a3b8', text: 'N/A' };
      const rule = FACTOR_RULES[key];
      if (!rule) return { label: '', color: '#94a3b8', text: rule ? rule.fmt(value) : String(value) };
      if (rule.neutral) return { label: '', color: '#94a3b8', text: rule.fmt(value) };
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

    // 화면 구성이 조회 때마다 달라지지 않게 하는 안전장치(아래 stepFail·guardedRender 참고)
    function stepFail(label, e, slotId) {
      try { console.error('[화면 그리기 오류] ' + label, e); } catch (x) { /* 무시 */ }
      const el = slotId ? document.getElementById(slotId) : null; if (!el) return;
      el.style.display = el.id === 'summary' ? 'block' : '';
      el.innerHTML = '<div class="card" style="border-left:4px solid var(--danger); font-size:13px; line-height:1.6;">⚠ <b>' + escHtml(label) + '</b>을(를) 표시하는 중 오류가 났습니다: ' + escHtml((e && e.message) || String(e)) + '<br /><span style="color:var(--text-muted);">다른 부분은 정상적으로 표시됩니다. 같은 종목을 다시 조회해 보세요. 계속되면 이 문구를 알려 주세요.</span></div>';
    }
    function guardedRender(label, slotId, fn) { try { return fn(); } catch (e) { stepFail(label, e, slotId); } }
    function renderSummary() { return guardedRender('요약·분석', 'summary', renderSummaryImpl); }
    function renderSummaryImpl() {
      const priceInput = Number(document.getElementById('currentPrice').value) || null;
      const m = computeSummaryMetrics(rawRows, priceInput);
      if (!m) {
        const el0 = document.getElementById('summary');
        el0.style.display = 'block';
        lastSummaryMetrics = null; AN_CTX = null; AN_STATE.key = null; // 이전 종목의 분석값이 남아 다른 화면에 쓰이지 않게 비운다
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

      let FSX = null; try { FSX = finSectorNow(); } catch (e) { FSX = null; }
      const isFinSec = !!(FSX && FSX.sec && FSX.sec.fin);
      const factorCardsFin = isFinSec ? [
        renderFactorCard('💰 Value (가치)', '#2563eb', [
          { label: 'PER', key: 'per', value: m.per, series: hist && hist.per },
          { label: 'PBR', key: 'pbr', value: m.pbr, series: hist && hist.pbr },
          { label: '이익수익률(1/PER)', key: 'earningsYield', value: m.earningsYield, series: hist && hist.earningsYield },
        ], '금융업은 PBR이 낮은 이유가 낮은 ROE일 수 있어, 낮다고 곧 싸다는 뜻이 아닙니다. 아래 「가격」 장의 정당 PBR과 함께 보세요. (배지는 일반 기업용 눈금)'),
        renderFactorCard('🏦 수익성·자본 (' + FSX.sec.n + ')', '#7c3aed', [
          { label: 'ROE 평균 (' + m.roeN + '개년)', key: 'finRoe', value: m.avgROE, series: hist && hist.avgROE },
          { label: '자기자본승수 (자산/자본)', key: 'finLev', value: anFin(m.debtRatio) ? 1 + m.debtRatio : null },
        ], '금융회사는 부채(예금·보험료·차입)가 영업의 원재료라 부채비율·이자보상배율·ROIC·매출총이익을 평가에 쓰지 않습니다. 그래서 좋음/주의 배지를 붙이지 않고 값만 보여 줍니다.'),
        renderFactorCard('🌱 주주 몫 성장 (YoY)', '#ea580c', [
          { label: '순이익 성장률', key: 'finNi', value: m.netIncomeGrowth, series: hist && hist.netIncomeGrowth },
        ], '금융회사 매출은 업종마다 정의가 달라 비교할 수 없어 매출 성장률은 숨겼습니다.'),
      ].join('') : null;
      const statTile = (label, value) => \`<div class="stat-tile"><div class="stat-label">\${label}</div><div class="stat-value">\${value}</div></div>\`;

      el.innerHTML = \`
        <div style="font-size:12px; color:var(--text-muted); margin-bottom:8px;">배지는 참고용 절대 기준(업종별 예외가 흔하니 동종업계와 비교해 참고하세요): PER↓PBR↓ 좋음, ROE·ROIC·이자보상배율·성장률은 ↑ 좋음. 작은 꺾은선은 연도별 추이(점이 최신 연도).</div>
        <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(250px, 1fr)); gap:10px;">\${factorCardsFin || factorCards}</div>

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
const AN_TH = { roicGood: 0.10, roicLow: 0.06, roeGood: 0.15, growthHigh: 0.10, growthVeryHigh: 0.15, growthLow: 0.05, fcfNiGood: 0.7, accrualMax: 0.05, perLow: 10, pbrLow: 1, taxRate: 0.24, defaultR: 0.09, omega: 0.62, minDIC: 0.10, perGraham: 15, pbrGraham: 1.5, grahamProd: 22.5, roicSpread: 0.03, pioLow: 3, shareTol: 0.01, rdcfN: 10, rdcfGT: 0.02 };
const AN_STATE = { r: AN_TH.defaultR, payout: null, ov: {}, key: null, mom: null, momFor: null, momErr: null };
let AN_CTX = null;

const anFin = (v) => v != null && typeof v === 'number' && isFinite(v);
const anDiv = (a, b) => (anFin(a) && anFin(b) && b !== 0) ? a / b : null;
const anPct = (v, d) => anFin(v) ? (v * 100).toFixed(d == null ? 1 : d) + '%' : 'N/A';
const anX = (v, d) => anFin(v) ? v.toFixed(d == null ? 2 : d) + '배' : 'N/A';
const anEok = (v) => anFin(v) ? Math.round(v / 1e8).toLocaleString() + '억' : 'N/A';
const anWon = (v) => anFin(v) ? Math.round(v).toLocaleString() + '원' : 'N/A';
const anNum = (v, d) => anFin(v) ? v.toFixed(d == null ? 2 : d) : 'N/A';
const anFmt = (kind, v) => kind === 'days' ? (anFin(v) ? Math.round(v) + '일' : 'N/A') : kind === 'eok' ? anEok(v) : kind === 'pct' ? anPct(v) : kind === 'x' ? anX(v) : kind === 'won' ? anWon(v) : anNum(v);
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
// 한 기간(연간 행 또는 TTM 행)의 지표 한 점. pv = 평균(기초·기말) 계산에 쓸 전기 행(연간은 전년 사업보고서, TTM은 1년 전 같은 분기).
function anMakePoint(r, pv) {
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
    // 현금전환주기(CCC) = 매출채권회전일(DSO) + 재고회전일(DIO) − 매입채무지급일(DPO). 연말 잔액 ÷ 연간 흐름 × 365.
    // 매출원가가 신뢰할 수 없는 회사(grossProfitOf가 null)는 DIO·DPO·CCC를 만들지 않는다. 기타채권·기타채무는 제외(영업 외 항목이 섞이지 않게).
    {
      const cg = (anFin(d.gp) && anFin(d.revenue)) ? d.revenue - d.gp : null;
      d.dso = (anRefetched(r) && anFin(r.receivables) && d.revenue > 0) ? r.receivables / d.revenue * 365 : null;
      const inv = anFlow0(r, 'inventory');
      d.dio = (anFin(inv) && anFin(cg) && cg > 0) ? inv / cg * 365 : null;
      d.dpo = (anRefetched(r) && anFin(r.payables) && anFin(cg) && cg > 0) ? r.payables / cg * 365 : null;
      d.ccc = (anFin(d.dso) && anFin(d.dio) && anFin(d.dpo)) ? d.dso + d.dio - d.dpo : null;
    }
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
}
// 최신 분기 기준 TTM 점(시계열 표시 전용 — 점수·CAGR·백테스트용 list에는 넣지 않는다: 연도 중복·부분기간이 섞여 왜곡되므로)
function anBuildTtmPoint(rows) {
  const qr = toQuarterlyRows(rows); const t = buildTTMRow(qr); if (!t) return null;
  const ann = toAnnualRows(rows); const lastY = ann.length ? Number(ann[ann.length - 1].bsns_year) : 0;
  if (Number(t.row.bsns_year) <= lastY) return null;
  const d = anMakePoint(t.row, t.prior);
  const qm = String(t.row.period_label).match(/([0-9])분기/);
  d.isTTM = true; d.q = qm ? qm[1] : ''; d.label = 'TTM(' + d.q + 'Q)';
  const pTtm = t.prior ? buildTTMFlowForQuarter(qr, t.prior) : null; // 1년 전 같은 분기 기준 TTM
  const p = pTtm ? anMakePoint(pTtm, null) : null;
  d.salesPrev = pTtm ? pTtm.revenue : null; d.ppePrev = t.prior ? t.prior.tangible_assets : null;
  d.shareChg = (p && anFin(d.shares) && anFin(p.shares) && p.shares > 0) ? d.shares / p.shares - 1 : null;
  ['revenue', 'gp', 'ebit', 'eps', 'fcf', 'ni'].forEach((k) => { d[k + 'Yoy'] = (p && anFin(d[k]) && anFin(p[k]) && p[k] > 0) ? d[k] / p[k] - 1 : null; }); // 1년 전 같은 분기 TTM 대비
  if (anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3) d.epsYoy = null;
  d.dIC = (p && anFin(d.ic) && anFin(p.ic)) ? d.ic - p.ic : null;
  d.reinv = (anFin(d.dIC) && d.nopat > 0) ? d.dIC / d.nopat : null;
  d.incRoic = null; d.incRoe = null;
  return d;
}
function anBuildSeries(rows) { return anBuildSeriesFrom(toAnnualRows(rows)); }
// annual = 연 1개씩의 행 목록(사업보고서 행, 또는 같은 분기 슬롯의 TTM 행). 각 점은 과거 점만 참조하므로 뒤쪽 점을 잘라내도 앞쪽 값이 같다.
function anBuildSeriesFrom(annual) {
  const by = {}; annual.forEach((r) => { by[r.bsns_year] = r; });
  const list = annual.map((r) => anMakePoint(r, by[String(Number(r.bsns_year) - 1)]));
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

// (3) N년 역DCF: 시장가격이 "앞으로 N년간 연 g로 FCF가 늘고, 그 뒤 영구성장 gT"를 가정해야 나오는 g를 푼다(기대투자 접근: Rappaport-Mauboussin 2001).
//   P = Σ_{t=1..N} F0(1+g)^t/(1+r)^t + [F0(1+g)^N(1+gT)/(r−gT)]/(1+r)^N   (F0 = 주당 FCF, r = 요구수익률(자기자본비용))
//   목표가가 아니라 "가격에 이미 담긴 성장 기대"를 읽는 도구. 현금흐름 기준이 음수·0이면 계산하지 않는다.
function anRdcfValue(f0, g, r, gT, N) {
  let pv = 0, f = f0;
  for (let t = 1; t <= N; t++) { f *= (1 + g); pv += f / Math.pow(1 + r, t); }
  return pv + (f * (1 + gT) / (r - gT)) / Math.pow(1 + r, N);
}
function anRdcfSolve(price, f0, r, gT, N) {
  if (!(price > 0) || !(f0 > 0) || !(r > gT) || !(N >= 1)) return null;
  let lo = -0.5, hi = 1.0;
  if (anRdcfValue(f0, lo, r, gT, N) > price || anRdcfValue(f0, hi, r, gT, N) < price) return null; // 범위 밖: 해 없음
  for (let i = 0; i < 80; i++) { const mid = (lo + hi) / 2; if (anRdcfValue(f0, mid, r, gT, N) < price) lo = mid; else hi = mid; }
  return (lo + hi) / 2;
}
function anRdcfBuild(price, fps, r) {
  if (!(price > 0) || !anFin(fps) || !(fps > 0)) return null;
  const N = AN_TH.rdcfN, gT = Math.min(AN_TH.rdcfGT, r - 0.01);
  const rs = [-0.02, -0.01, 0, 0.01, 0.02].map((d) => r + d).filter((x) => x > 0.02);
  const gts = [0, 0.01, 0.02, 0.03].filter((x) => x < Math.min.apply(null, rs) - 0.005);
  return { N, gT, r, fps, main: anRdcfSolve(price, fps, r, gT, N), rs, gts,
    grid: rs.map((rr) => gts.map((gg) => anRdcfSolve(price, fps, rr, gg, N))),
    byN: [5, 10, 15].map((n) => ({ n, g: anRdcfSolve(price, fps, r, gT, n) })) };
}

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

// ---------- FCF 유형 (현금흐름 관점의 설명용 분류) ----------
// FCF = CFO − CAPEX(유형자산 취득 + 무형자산 취득). 유형자산 취득이 확인되지 않으면 FCF를 만들지 않으므로(과대평가 방지) 해당 연도는 "산출 불가"로 센다.
// 아래 기준선(5년 중 1년 적자 허용, CAPEX/CFO 0.7, FCF/CFO 0.3, 희석 10% 등)은 VALUELAB의 가설이며 학술적으로 검증된 임계값이 아니다.
function anFcfType(c) {
  const L = c.list || [];
  const lastY = L.length ? L[L.length - 1].year : 0;
  const W = L.filter((d) => d.year > lastY - 5);
  const last = L.length ? L[L.length - 1] : null;
  const byY = {}; L.forEach((d) => { byY[d.year] = d; });
  const fcfs = W.map((d) => d.fcf).filter(anFin), ocfs = W.map((d) => d.ocf).filter(anFin);
  const nF = fcfs.length, nO = ocfs.length;
  const posF = fcfs.filter((v) => v > 0).length, negO = ocfs.filter((v) => v <= 0).length;
  const out = { nF, nO, nW: W.length, posF, negO, types: [], head: null, conf: nF >= 5 ? '높음' : nF >= 4 ? '보통' : '낮음', counters: [], m: {} };
  if (!last || nF < 3) {
    out.head = { text: '판정 보류 — FCF를 계산할 수 있는 연도가 ' + nF + '개뿐입니다(최소 3개, 4개 이상 권장)', kind: 'none' };
    return out;
  }
  const y2 = byY[last.year - 2], y1 = byY[last.year - 1], y3 = byY[last.year - 3];
  const cfom = W.map((d) => anDiv(d.ocf, d.revenue));
  const recent = anMean(cfom.slice(-2)), earlier = cfom.length >= 4 ? anMean(cfom.slice(0, -2)) : null;
  const cfomDown = (recent != null && earlier != null) ? recent < earlier - 0.005 : null; // 0.5%p 넘게 낮아야 '하락'(변화 없음·소수점 오차를 악화로 보지 않음)
  const ocfUp = (anFin(last.ocf) && y2 && anFin(y2.ocf)) ? last.ocf > y2.ocf : null;
  const fcfmUp = (anFin(last.fcfm) && y1 && anFin(y1.fcfm)) ? last.fcfm > y1.fcfm : null;
  const cx = W.slice(-3).map((d) => (anFin(d.capex) && anFin(d.ocf) && d.ocf > 0) ? d.capex / d.ocf : null).filter(anFin);
  const capexOcf3 = cx.length >= 2 ? anMean(cx) : null;
  const fcfOcf = (anFin(last.fcf) && anFin(last.ocf) && last.ocf > 0) ? last.fcf / last.ocf : null;
  const dil = (y3 && anFin(last.shares) && anFin(y3.shares) && y3.shares > 0) ? last.shares / y3.shares - 1 : null;
  const gRev = c.growthRev;
  out.m = { cfomRecent: recent, cfomEarlier: earlier, capexOcf3, fcfOcf, dil, lastFcf: last.fcf, lastOcf: last.ocf, gRev };
  const T = (txt, v) => [txt, v == null ? null : !!v];
  const pct = anPct;
  const defs = [
    { name: 'FCF Compounder', desc: '현금흐름이 꾸준히 양호하고 사업 규모도 함께 성장하는 유형',
      conds: [
        T('FCF 산출 가능 연도 4개 이상: 현재 ' + nF + '개', nF >= 4),
        T('최근 5년 중 FCF 적자 연도 1개 이하: 현재 ' + (nF - posF) + '개', nF >= 4 ? (nF - posF) <= 1 : null),
        T('최근 연도 FCF 흑자: 현재 ' + anFmt('eok', last.fcf), anFin(last.fcf) ? last.fcf > 0 : null),
        T('3년 평균 FCF 마진 양수: 현재 ' + pct(c.fcfm3), c.fcfm3 != null ? c.fcfm3 > 0 : null),
        T('매출 3년 CAGR 양수: 현재 ' + pct(gRev), gRev != null ? gRev > 0 : null),
        T('FCF 마진 악화 아님(3년 평균 ≥ 5년 평균): 3년 ' + pct(c.fcfm3) + ' / 5년 ' + pct(c.fcfm5), c.fcfDown != null ? c.fcfDown === false : null),
      ] },
    { name: 'Emerging Compounder / FCF 변곡', desc: '현재 FCF는 약하거나 최근에야 흑자이지만 CFO·마진이 개선되는 유형(변곡점 후보)',
      conds: [
        T('최근 연도 CFO 양수: 현재 ' + anFmt('eok', last.ocf), anFin(last.ocf) ? last.ocf > 0 : null),
        T('CFO가 2년 전보다 증가: ' + anFmt('eok', y2 ? y2.ocf : null) + ' → ' + anFmt('eok', last.ocf), ocfUp),
        T('FCF 마진이 전년보다 개선: ' + pct(y1 ? y1.fcfm : null) + ' → ' + pct(last.fcfm), fcfmUp),
        T('FCF가 아직 안정적이지 않음(5년 중 적자 2개 이상 또는 최근 FCF ≤ 0): 적자 ' + (nF - posF) + '개', nF >= 3 ? ((nF - posF) >= 2 || (anFin(last.fcf) && last.fcf <= 0)) : null),
        T('매출 3년 CAGR 양수: 현재 ' + pct(gRev), gRev != null ? gRev > 0 : null),
      ] },
    { name: 'Growth Reinvestment', desc: '영업현금은 늘지만 성장 CAPEX가 커서 FCF가 낮은 유형(재투자가 성과로 이어지는지 확인 필요)',
      conds: [
        T('최근 연도 CFO 양수: 현재 ' + anFmt('eok', last.ocf), anFin(last.ocf) ? last.ocf > 0 : null),
        T('CFO가 2년 전보다 증가', ocfUp),
        T('3년 평균 CAPEX/CFO 70% 이상: 현재 ' + pct(capexOcf3), capexOcf3 != null ? capexOcf3 >= 0.7 : null),
        T('최근 FCF가 CFO의 30% 이하: 현재 ' + pct(fcfOcf), fcfOcf != null ? fcfOcf <= 0.3 : null),
        T('매출 3년 CAGR ' + pct(AN_TH.growthHigh, 0) + ' 이상: 현재 ' + pct(gRev), gRev != null ? gRev >= AN_TH.growthHigh : null),
        T('CFO 마진 악화 아님(최근 2년이 이전보다 0.5%p 넘게 낮지 않음): ' + pct(recent) + ' vs ' + pct(earlier), cfomDown != null ? cfomDown === false : null),
      ] },
    { name: 'Growth Trap / FCF 악화', desc: '매출은 늘지만 현금 전환이 나빠지고 적자·희석이 반복되는 유형',
      conds: [
        T('매출 3년 CAGR 양수: 현재 ' + pct(gRev), gRev != null ? gRev > 0 : null),
        T('CFO 마진 또는 FCF 마진 하락: CFO 마진 ' + pct(recent) + ' vs ' + pct(earlier) + ' / FCF 3년 ' + pct(c.fcfm3) + ' vs 5년 ' + pct(c.fcfm5), (cfomDown == null && c.fcfDown == null) ? null : (cfomDown === true || c.fcfDown === true)),
        T('CFO 적자 2개 연도 이상 또는 3년 주식수 +10% 이상(희석): 적자 ' + negO + '개 / 희석 ' + pct(dil), (nO >= 3 || dil != null) ? (negO >= 2 || (dil != null && dil >= 0.10)) : null),
      ] },
  ];
  defs.forEach((D) => { D.pass = D.conds.filter((x) => x[1] === true).length; D.unk = D.conds.filter((x) => x[1] == null).length; D.total = D.conds.length; D.full = D.pass === D.total; });
  out.types = defs;
  const matched = defs.filter((D) => D.full);
  if (matched.length) out.head = { text: matched.map((D) => D.name).join(' / '), kind: 'match' };
  else {
    const best = defs.slice().sort((a, b) => (b.pass / b.total) - (a.pass / a.total))[0];
    out.head = (best && best.pass / best.total >= 0.6) ? { text: '가장 유사: ' + best.name + ' (' + best.pass + '/' + best.total + ' 충족)', kind: 'near' } : { text: '뚜렷한 유형 없음(혼합)', kind: 'none' };
  }
  // 반대 근거: 어떤 유형이든 투자 논리를 약하게 만드는 신호를 자동으로 찾아 나열한다(없으면 없다고 표시)
  if (dil != null && dil >= 0.05) out.counters.push('주식수가 3년간 ' + pct(dil) + ' 늘었습니다(희석) — 주당 가치가 FCF 증가보다 덜 늘 수 있음');
  const cn = anMean(anWin(L, 'cfoni', 3)); if (cn != null && cn < 0.7) out.counters.push('최근 3년 평균 CFO/순이익이 ' + cn.toFixed(2) + '배로 낮습니다 — 이익이 현금으로 충분히 이어지지 않음');
  const cxr = W.map((d) => d.capexRev), cxRecent = anMean(cxr.slice(-2)), cxEarlier = cxr.length >= 4 ? anMean(cxr.slice(0, -2)) : null;
  if (cxRecent != null && cxEarlier != null && cxRecent > cxEarlier * 1.3 && cxRecent > 0.03) out.counters.push('CAPEX/매출이 ' + pct(cxEarlier) + ' → ' + pct(cxRecent) + '로 커졌습니다 — 투자 부담 증가(성장 투자인지 유지 비용인지 확인 필요)');
  if (nF - posF >= 1) out.counters.push('최근 ' + nF + '개 연도 중 FCF 적자가 ' + (nF - posF) + '개 있습니다');
  if (nF < 5) out.counters.push('FCF를 계산할 수 있는 연도가 ' + nF + '/5개입니다 — 자료 부족(유형자산 취득 항목 미확인 연도 포함)');
  return out;
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
  const rdcf = anRdcfBuild(price, fps, r);
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
    list, pio, last, vs, r, roe3, roe5, roic3, roic5, payoutAvg, reinv3, gRoe, gRoic, def, scen, impE, impF, rdcf, expG, mom, growth, epsG, growthRev, growthEbit, growthEpsIsNi, growthN: growthParts.length,
    qualityOk, qPts: q, valLevel: valL.level, valN: valL.n, momLevel: mom ? mom.level : null, base0: roeBase,
    perLow: anFin(vs.per) ? vs.per <= 10 : null, pbrLow: anFin(vs.pbr) ? vs.pbr <= 1 : null,
    roicDown: (roic3 != null && roic5 != null) ? (roic3 < roic5 || roic3 < r) : null,
    fcfDown: (fm3 != null && fm5 != null) ? fm3 < fm5 : null, fcfm3: fm3, fcfm5: fm5,
    qg: anQuarterGrowth(rows), price, m,
    ttm: (function () { try { return anBuildTtmPoint(rows); } catch (e) { return null; } })(),
  };
  ctx.cls = anClassify(ctx);
  try { ctx.fcfType = anFcfType(ctx); } catch (e) { ctx.fcfType = null; } // 보조 분류 오류가 화면 전체를 막지 않도록 분리
  return ctx;
}

// ---------- 렌더링 부품 ----------
function anTable(list, defs, nYears) {
  const t = (typeof AN_CTX !== 'undefined' && AN_CTX && list === AN_CTX.list) ? AN_CTX.ttm : null; // 연도 시계열이면 마지막에 TTM 열 추가
  const cols = list.slice(-nYears).concat(t ? [t] : []);
  const th = (d) => d.isTTM ? '<th style="background:#b45309;" title="사업보고서가 아직 없는 최신 분기 기준 최근 4개 분기 합산(TTM)">' + d.label + '</th>' : '<th>' + d.year + '</th>';
  const head = '<tr><th class="an-first">항목</th>' + cols.map(th).join('') + '</tr>';
  const body = defs.map((df) => '<tr><td class="an-first">' + df.label + '</td>' + cols.map((d) => '<td' + (d.isTTM ? ' style="background:#fffbeb;"' : '') + '>' + anFmt(df.fmt, d[df.key]) + '</td>').join('') + '</tr>').join('');
  const note = t ? '<div style="font-size:11px; color:var(--text-muted); margin-top:3px;">※ 마지막 열 ' + t.label + ': ' + t.year + '년 ' + t.q + '분기까지 최근 4개 분기 합산(손익·현금흐름), 재무상태표는 해당 분기말. 증가율은 1년 전 같은 분기 기준 TTM 대비. 사업보고서 값과 기준 시점이 달라 직접 비교할 때 유의하세요.</div>' : '';
  return '<div class="tscroll" style="max-height:60vh;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>' + note;
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
  items.push({ name: '역DCF (시장 내재 FCF 성장률)', badge: 'practice', bl: '정의는 표준·해석은 참고',
    concept: '현재 주가를 "앞으로 N년 동안 FCF가 연 g로 늘고, 그 뒤 영구성장 gT"라는 가정으로 설명하려면 g가 얼마여야 하는지 거꾸로 푼 값입니다. 가격에 이미 들어 있는 기대를 읽는 도구입니다.',
    formula: 'P = Σ F₀(1+g)^t/(1+r)^t (t=1..N) + [F₀(1+g)^N(1+gT)/(r−gT)]/(1+r)^N 를 만족하는 g (이분법). F₀=최근 3년 평균 FCF÷주식수, r=설정한 요구수익률, N=' + AN_TH.rdcfN + '년, gT=' + anPct(AN_TH.rdcfGT, 0) + '(r−1%p를 넘지 않게 제한).',
    inputs: c.rdcf ? ('가격 ' + anWon(c.price) + ' · F₀ ' + anWon(c.rdcf.fps) + ' · r ' + anPct(c.rdcf.r, 1) + ' → g ' + anPct(c.rdcf.main)) : 'N/A (FCF 기준 값이 0 이하)',
    rule: '판정 기준 없음. 과거 FCF 성장률·지속가능성장률과 나란히 보여주기만 합니다. F₀≤0, r≤gT, 해가 −50%~100% 밖이면 N/A.',
    evidence: 'Rappaport·Mauboussin의 기대투자(Expectations Investing, 2001)와 Damodaran의 역DCF 설명이 출처입니다. 방법의 수학은 표준이지만, 이를 이용한 매매가 초과수익을 낸다는 검증은 이 앱에서 하지 않았습니다.',
    limit: 'F₀ 선택(3년 평균)에 민감하고, 영구성장 구간 비중이 커서 r·gT 가정에 크게 흔들립니다(표로 확인). 주식수·부채는 단순화했고, 금융업·적자 성장기업에는 부적합합니다.' });
  items.push({ name: '현금전환주기 CCC (DSO·DIO·DPO)', badge: 'practice', bl: '정의는 표준·해석은 참고', now: anFin(c.last.ccc) ? Math.round(c.last.ccc) + '일' : 'N/A',
    concept: '물건을 만들거나 사 와서 현금으로 돌려받기까지 자금이 묶이는 평균 일수. 짧아지면 같은 매출에 필요한 운전자본이 줄고, 길어지면 이익이 나도 현금이 덜 남을 수 있습니다.',
    formula: 'DSO = 매출채권 ÷ 매출 × 365 · DIO = 재고자산 ÷ 매출원가 × 365 · DPO = 매입채무 ÷ 매출원가 × 365 · CCC = DSO + DIO − DPO (연말 잔액 기준)',
    inputs: 'DSO ' + anNum(c.last.dso, 0) + '일 · DIO ' + anNum(c.last.dio, 0) + '일 · DPO ' + anNum(c.last.dpo, 0) + '일 → CCC ' + anNum(c.last.ccc, 0) + '일 (' + (c.last.year || 'N/A') + '년)',
    rule: '판정 기준 없음(참고 지표). 같은 회사의 과거 추세와 같은 업종 회사끼리 비교할 때 의미가 있습니다.',
    evidence: 'CCC 개념은 Richards & Laughlin(1980)이 유동성 분석에 소개했고, Deloof(2003, 벨기에 기업)는 CCC가 긴 기업일수록 수익성이 낮은 경향을 보고했습니다. 다만 상관관계이며 한국 시장·업종별 효과는 이 앱에서 검증하지 않았습니다. 「교섭력이 강하다」 같은 해석은 증거 없이 단정하지 않습니다.',
    limit: '연말 잔액만 쓰므로 계절성·연말 일회성 거래에 민감합니다. 매출원가가 공시되지 않는 업종(서비스·금융 등)은 DIO·DPO·CCC가 N/A입니다. 별도 계정(기타채권·기타채무)은 제외했지만, 회사가 「매출채권 및 기타채권」처럼 묶어 공시하면 그 합계가 쓰여 실제보다 길게 나올 수 있습니다.' });
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

// 한눈에 보기: 4개 축(점수 하나로 합치지 않음) + 데이터 기준 + 장 바로가기
function anRenderGlance(c, m) {
  const lv = (v) => v == null ? 'N/A' : v, r = c.last && c.last.r ? c.last.r : null;
  const chip = (name, v) => '<div style="flex:1 1 120px; border:1px solid var(--border); border-radius:10px; padding:8px 10px; background:#fff;"><div style="font-size:11.5px; color:var(--text-muted);">' + name + '</div><div style="font-size:16px; font-weight:800;">' + anEsc(lv(v)) + '</div></div>';
  const fd = r && r.fs_div ? (r.fs_div === 'CFS' ? '연결' : r.fs_div === 'OFS' ? '개별(별도)' : r.fs_div) : '기준 미확인';
  const filing = r && r.filing_date ? btFmtDate(r.filing_date) : '공시일 미저장';
  const px = m && anFin(m.priceInput) && m.priceInput > 0 ? Math.round(m.priceInput).toLocaleString('ko-KR') + '원' : '주가 미입력(주가가 필요한 지표는 N/A)';
  const jump = [['①', '사업의 질', '좋은 사업인가?'], ['②', '현금', '이익이 현금으로 확인되는가?'], ['③', '안전', '재무적으로 안전한가?'], ['④', '성장', '어떻게 성장하고 있는가?'], ['⑤', '자본배분', '경영진은 번 돈을 잘 쓰는가?'], ['⑥', '가격', '지금 가격은 합당한가?'], ['⑦', '시장', '시장은 어떻게 보고 있는가?'], ['⑧', '결론', '종합 결론']]
    .map((j) => '<button type="button" title="' + j[2] + '" style="font-size:12.5px; padding:4px 10px; min-height:36px;" onclick="anGoChap(\\'' + j[0] + '\\')">' + j[0] + ' ' + j[1] + '</button>').join('');
  return '<div class="card" id="anGlance" style="border-left:4px solid #0f172a;"><div style="font-weight:800; font-size:14.5px;">한눈에 보기 <span style="font-size:11.5px; font-weight:400; color:var(--text-muted);">— 4개 축은 합산하지 않고 따로 봅니다</span></div>' +
    '<div style="display:flex; flex-wrap:wrap; gap:8px; margin:8px 0;">' + chip('Growth 성장', c.grades.growth) + chip('Quality 사업의 질', c.grades.quality) + chip('Valuation 가격', c.grades.val) + chip('Momentum 주가 흐름', c.grades.mom) + '</div>' +
    '<div style="font-size:12px; color:var(--text-muted); line-height:1.55;">기준: 최근 연간 ' + (c.last ? c.last.year : 'N/A') + '년 · ' + fd + '재무제표 · 공시일 ' + filing + ' · 연간 ' + c.list.length + '개년 · 현재가 입력값 ' + px + '. 등급의 계산식·근거는 아래 「🏷 종합 분류」와 맨 아래 「참고」 장에 있습니다.</div>' +
    '<div style="margin-top:8px; font-size:12px; font-weight:600;">질문별로 바로가기 <span style="font-weight:400; color:var(--text-muted);">(누르면 해당 장이 펼쳐집니다)</span></div><div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:4px;">' + jump + '</div></div>';
}
function anGoChap(no) {
  const el = document.getElementById('analysis'); if (!el) return;
  const ch = Array.prototype.filter.call(el.querySelectorAll(':scope > .an-chap'), (d) => d.getAttribute('data-short').split(' ')[0] === no)[0]; if (!ch) return;
  anChapItems(ch).forEach((d) => { d.open = true; }); anChapUpdateLabel(ch);
  ch.scrollIntoView({ behavior: 'smooth', block: 'start' });
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
    '각 항목 옆 배지는 근거 수준입니다 — 학술 검증(논문으로 성과가 확인된 방법) · 이론 근거(이론상 맞지만 입력값은 가정) · 실무 관행(널리 쓰이나 증명 안 됨) · 가정·휴리스틱(임의 기준선). 계산식·실제 입력값·한계는 맨 아래 「참고」 장의 「지표·계산식 설명서」에서 항목별로 펼쳐 볼 수 있습니다.',
    '<div style="font-size:16px; font-weight:800; color:' + col + '; margin:6px 0;">' + h.text + '</div>' + axes + q + cards, true);
}

function anRenderFcfType(c) {
  const f = c.fcfType; if (!f) return '';
  const col = f.head.kind === 'match' ? '#0d9488' : f.head.kind === 'near' ? '#d97706' : '#64748b';
  const m = f.m || {};
  const tiles = f.types.length ? anGrid([
    anTile('FCF 산출 연도', f.nF + '/' + Math.min(5, f.nW || 5), '최근 5년 중 FCF를 계산할 수 있는 연도. 자료 신뢰도: ' + f.conf),
    anTile('FCF 흑자 연도', f.posF + '/' + f.nF, 'FCF가 양수인 연도 수'),
    anTile('최근 FCF', anFmt('eok', m.lastFcf), 'CFO ' + anFmt('eok', m.lastOcf) + ' − CAPEX'),
    anTile('CFO 마진', anPct(m.cfomRecent), '최근 2년 평균 (이전 평균 ' + anPct(m.cfomEarlier) + ')'),
    anTile('CAPEX/CFO', anPct(m.capexOcf3), '최근 3년 평균(CFO가 양수인 해만)'),
  ]) : '';
  const cards = f.types.map((L) => '<div style="border:1px solid var(--border); border-radius:8px; padding:8px 10px; margin-top:8px;"><div style="display:flex; justify-content:space-between; gap:8px;"><b>' + L.name + '</b><span style="font-size:12px; color:' + (L.full ? '#0d9488' : 'var(--text-muted)') + ';">' + L.pass + '/' + L.total + ' 충족' + (L.unk ? ' (미확인 ' + L.unk + ')' : '') + '</span></div><div style="font-size:11px; color:var(--text-muted); margin:2px 0 4px;">' + L.desc + '</div>' +
    L.conds.map((x) => '<div style="font-size:12px;">' + anMark(x[1]) + ' ' + anEsc(x[0]) + '</div>').join('') + '</div>').join('');
  const counters = f.types.length ? '<div style="margin-top:8px; padding:8px 10px; background:#fff7ed; border-radius:8px; font-size:12.5px;"><b>반대 근거(자동 탐지)</b>' +
    (f.counters.length ? f.counters.map((t) => '<div>• ' + anEsc(t) + '</div>').join('') : '<div>탐지 항목(희석·현금 전환·CAPEX 증가·FCF 적자·자료 부족)에서 해당 없음 — 다만 이 목록이 모든 위험을 뜻하지는 않습니다.</div>') + '</div>' : '';
  return anSection('💧 FCF 유형 (현금흐름 관점)', col,
    '설명용 분류이며 매수·매도 신호가 아닙니다. FCF = CFO − CAPEX(유형자산 + 무형자산 취득)이고, 유형자산 취득이 확인되지 않은 연도는 FCF를 만들지 않습니다. 유형 판정 기준(적자 1년 허용, CAPEX/CFO 70%, FCF/CFO 30%, 희석 10% 등)은 VALUELAB가 정한 가설이라 학술적으로 검증된 값이 아닙니다 ' + anBadge('assume') + ' 반대 근거를 함께 확인하세요.',
    '<div style="font-size:16px; font-weight:800; color:' + col + '; margin:6px 0;">' + anEsc(f.head.text) + '</div>' + tiles + cards + counters, false);
}

// ===== 종목분석 ↔ 내 투자 가설 메모 =====
// 가설·무효화 조건은 내 투자(pf)의 계획서에 이미 저장됩니다. 여기서는 읽어서 보여주고, 쓰러 가는 길만 만듭니다(새 테이블·서버 경로 없음).
function anThesisName() { return (rawRows && rawRows[0] && rawRows[0].corp_name) || (document.getElementById('corpName') ? document.getElementById('corpName').value.trim() : ''); }
function anThesisHtml() {
  const nm = anThesisName(); if (!nm) return '';
  const head = '<div style="font-weight:700; margin-bottom:4px;">📝 내 투자 가설 메모</div>';
  const note = '<div style="font-size:11.5px; color:var(--text-muted); margin-top:6px; line-height:1.45;">매수 전에 "왜 사는지"와 "무엇이 틀리면 접는지"를 글로 남기는 <b>습관 도구</b>입니다. 편향을 막는다고 입증된 것은 아니며, 사전부검(Klein 2007)의 아이디어를 쓴 실무 관행입니다. 손절 기준이 아니라 <b>가설이 깨졌는지 판단하는 조건</b>입니다.</div>';
  if (!PFU.st) {
    return head + '<div style="font-size:12.5px;">내 투자 탭에서 잠금을 풀면 이 종목의 가설·무효화 조건이 여기에 표시됩니다.</div>' +
      '<button onclick="anThesisGo(0)" style="margin-top:6px;">내 투자 열기</button>' + note;
  }
  const plans = (PFU.st.plans || []).filter(function (p) { return p.corp_name === nm; });
  const p = plans.length ? plans[0] : null;
  if (!p) {
    return head + '<div style="font-size:12.5px;">이 종목에 대한 가설 메모가 아직 없습니다.</div>' +
      '<button class="primary" onclick="anThesisGo(1)" style="margin-top:6px;">이 종목 투자 가설 쓰기</button>' + note;
  }
  const e = escHtml;
  return head + '<div style="font-size:12.5px; line-height:1.55;">상태 <b>' + e(p.status || '') + '</b> · 버전 v' + e(String(p.version)) + (p.review_date ? ' · 재검토 ' + e(p.review_date) : '') +
    '<div style="margin-top:4px;"><b>가설</b>: ' + (p.thesis ? e(p.thesis) : '<span style="color:var(--text-muted);">(비어 있음)</span>') + '</div>' +
    '<div style="margin-top:4px;"><b>가설 무효화 조건</b>: ' + (p.disproof ? e(p.disproof) : '<span style="color:var(--text-muted);">(비어 있음)</span>') + '</div></div>' +
    '<button onclick="anThesisGo(2)" style="margin-top:6px;">계획서 열어 수정</button>' + note;
}
function anThesisUpdate() { const el = document.getElementById('anThesisBox'); if (el) el.innerHTML = anThesisHtml(); }
function anThesisGo(mode) {
  const nm = anThesisName();
  switchTab('pf');
  if (mode === 0 || !PFU.st) return;
  pfuPanel('plan');
  const p = (PFU.st.plans || []).filter(function (x) { return x.corp_name === nm; })[0];
  if (mode === 2 && p) pfuEditPlan(p.plan_key);
  else { pfuClearPlan(); document.getElementById('pfpName').value = nm; window.scrollTo(0, 0); }
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
// ===== 시그널·수익률 분해 (투자 판단 점검) =====
// ① 주가수익률 = 배당수익률 + EPS 성장률 + PER 변화율 (항등식, 기간말 주가·연간 EPS 사용)
// ② 변화 감지: 없다가 생긴 것 / 있다가 사라진 것 (재무 부호 전환·배당 시작/중단·거래량·추세)
// ③ 판단 체크리스트(자동 참고 + 직접 체크). 점수·매수 신호를 만들지 않는다.
const SIG_BADGE = { proof: '학술 검증', theory: '이론 근거', practice: '실무 관행', assume: '가정·휴리스틱' };
const sigPct = (v, d) => anFin(v) ? (v >= 0 ? '+' : '') + (v * 100).toFixed(d == null ? 1 : d) + '%' : 'N/A';
function sigSign(v) { return !anFin(v) ? null : v > 0 ? 1 : v < 0 ? -1 : 0; }
function sigSma(a, n, end) {
  if (!a || end - n + 1 < 0 || end >= a.length) return null;
  let s = 0; for (let i = end - n + 1; i <= end; i++) { const v = a[i]; if (!(v > 0)) return null; s += v; }
  return s / n;
}
function sigAvg(a, from, to) { // [from,to) 구간 평균. 구간의 80% 이상 값이 있어야 계산
  const f = Math.max(0, from); if (!a || to <= f) return null;
  let s = 0, k = 0; for (let i = f; i < to; i++) { const v = a[i]; if (v != null && isFinite(v) && v >= 0) { s += v; k++; } }
  return (k > 0 && k >= (to - f) * 0.8) ? s / k : null;
}
function sigMedian(a) { const x = a.filter(anFin).sort((p, q) => p - q); if (!x.length) return null; const m = x.length >> 1; return x.length % 2 ? x[m] : (x[m - 1] + x[m]) / 2; }
// 연간 시계열(anBuildSeries 결과)에서 n년 구간의 수익률 분해. 계산할 수 없으면 err 사유를 돌려준다.
function sigDecomp(list, n) {
  if (!list || list.length < n + 1) return { n: n, err: '연간 자료가 ' + (n + 1) + '개 해 미만' };
  const win = list.slice(list.length - n - 1), a = win[0], b = win[n];
  if (b.year - a.year !== n) return { n: n, err: '중간에 빠진 연도가 있음' };
  if (win.slice(1).some((d) => anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3)) return { n: n, a: a.year, b: b.year, err: '유통주식수가 한 해 30% 넘게 변함(분할·증자) — 주가와 EPS 기준이 달라 분해하지 않음' };
  const px = (d) => d.r ? d.r.price_at_period_end : null;
  const p0 = px(a), p1 = px(b), e0 = a.eps, e1 = b.eps;
  if (!(p0 > 0 && p1 > 0)) return { n: n, a: a.year, b: b.year, err: '기간말 주가 없음' };
  if (!(e0 > 0 && e1 > 0)) return { n: n, a: a.year, b: b.year, err: '시작 또는 끝 EPS가 0 이하(적자) — 성장률 계산 불가' };
  const per0 = p0 / e0, per1 = p1 / e1;
  const o = { n: n, a: a.year, b: b.year, p0: p0, p1: p1, e0: e0, e1: e1, per0: per0, per1: per1 };
  o.epsG = Math.pow(e1 / e0, 1 / n) - 1; o.perCh = Math.pow(per1 / per0, 1 / n) - 1; o.priceG = Math.pow(p1 / p0, 1 / n) - 1;
  let divOk = true, prod = 1, ysum = 0;
  for (let i = 1; i <= n; i++) {
    const pp = px(win[i - 1]), pc = px(win[i]), dps = win[i].r ? win[i].r.dividend_per_share : null;
    if (!(pp > 0 && pc > 0) || dps == null || !isFinite(dps) || dps < 0) { divOk = false; break; }
    prod *= (pc + dps) / pp; ysum += dps / pp;
  }
  o.divY = divOk ? ysum / n : null;                       // 연평균 배당수익률(전년말 주가 대비 해당 연도 주당배당금)
  o.tr = divOk ? Math.pow(prod, 1 / n) - 1 : null;        // 총수익률 CAGR(배당을 해당 연도 말 주가로 재투자 가정)
  o.approx = o.divY != null ? o.epsG + o.perCh + o.divY : null; // 덧셈 근사
  o.exactPrice = (1 + o.epsG) * (1 + o.perCh) - 1;        // 곱셈(항등식): 주가 CAGR과 같아야 한다
  return o;
}
// 기대수익률 시나리오(예측이 아니라 가정의 산술): (1+g)(1+PER 변화)-1 + 배당수익률
function sigExpected(divY, g, perChgTotal, years) {
  if (!anFin(g) || !anFin(perChgTotal) || !(years >= 1) || g <= -1 || perChgTotal <= -1) return null;
  const perAnn = Math.pow(1 + perChgTotal, 1 / years) - 1;
  return (1 + g) * (1 + perAnn) - 1 + (anFin(divY) ? divY : 0);
}
// 변화 감지. c = 분석 컨텍스트(c.list, c.ttm), px = { d, c, v, tv } 일별 주가(없으면 null)
function sigDetect(c, px) {
  const items = [];
  const add = (o) => { items.push(o); };
  const L = c && c.list ? c.list : [];
  if (L.length >= 2 && L[L.length - 1].year - L[L.length - 2].year === 1) {
    const a = L[L.length - 2], b = L[L.length - 1];
    [['ebit', '영업이익'], ['ni', '순이익'], ['fcf', 'FCF']].forEach((k) => {
      const s0 = sigSign(a[k[0]]), s1 = sigSign(b[k[0]]);
      if (s0 == null || s1 == null) return;
      if (s0 <= 0 && s1 > 0) add({ id: 'chg_' + k[0], grp: 'fund', tone: 'good', label: k[1] + ' 흑자 전환 (없다가 생김)', detail: a.year + '년 ' + (a[k[0]] / 1e8).toFixed(0) + '억 → ' + b.year + '년 ' + (b[k[0]] / 1e8).toFixed(0) + '억', ev: 'proof', note: '이익 서프라이즈·이익 변화에 가격이 늦게 반응하는 현상(Ball-Brown 1968, Bernard-Thomas 1989)' });
      else if (s0 > 0 && s1 <= 0) add({ id: 'chg_' + k[0], grp: 'fund', tone: 'bad', label: k[1] + ' 적자 전환 (있다가 사라짐)', detail: a.year + '년 ' + (a[k[0]] / 1e8).toFixed(0) + '억 → ' + b.year + '년 ' + (b[k[0]] / 1e8).toFixed(0) + '억', ev: 'proof', note: '이익 악화도 같은 방향으로 늦게 반영되는 경향(Bernard-Thomas 1989)' });
    });
    const d0 = a.r ? a.r.dividend_per_share : null, d1 = b.r ? b.r.dividend_per_share : null;
    if (d0 != null && d1 != null) {
      if (!(d0 > 0) && d1 > 0) add({ id: 'chg_div', grp: 'fund', tone: 'good', label: '배당 시작 (없다가 생김)', detail: a.year + '년 0원 → ' + b.year + '년 ' + Math.round(d1).toLocaleString() + '원/주', ev: 'proof', note: '배당 개시·중단 이후 장기 초과수익/손실(Michaely-Thaler-Womack 1995, 미국 표본)' });
      else if (d0 > 0 && !(d1 > 0)) add({ id: 'chg_div', grp: 'fund', tone: 'bad', label: '배당 중단 (있다가 사라짐)', detail: a.year + '년 ' + Math.round(d0).toLocaleString() + '원/주 → ' + b.year + '년 0원', ev: 'proof', note: '배당 개시·중단 이후 장기 초과수익/손실(Michaely-Thaler-Womack 1995, 미국 표본)' });
    }
    const t = c.ttm;
    if (t && anFin(t.ebit) && anFin(b.ebit) && sigSign(t.ebit) !== sigSign(b.ebit) && sigSign(b.ebit) !== 0 && sigSign(t.ebit) !== 0) {
      add({ id: 'chg_ttm', grp: 'fund', tone: t.ebit > 0 ? 'good' : 'bad', label: '최근 4분기(TTM) 영업이익 부호가 직전 사업연도와 다름', detail: b.year + '년 ' + (b.ebit / 1e8).toFixed(0) + '억 → TTM ' + (t.ebit / 1e8).toFixed(0) + '억', ev: 'proof', note: '가장 최근 실적 변화(연간 공시 이후)를 반영' });
    }
  }
  const have = px && px.c && px.d && px.c.length >= 130;
  const meta = { priceReady: !!have };
  if (have) {
    const cl = px.c, n = cl.length, last = n - 1;
    const useTv = !!(px.tv && sigAvg(px.tv, n - 65, n) != null);
    const vs = useTv ? px.tv : px.v, vname = useTv ? '거래대금' : '거래량';
    const a5 = sigAvg(vs, n - 5, n), b60 = sigAvg(vs, n - 65, n - 5);
    if (a5 != null && b60 != null && b60 > 0) {
      const ratio = a5 / b60; meta.volRatio = ratio;
      let mx = 0; for (let i = n - 5; i < n; i++) { const v = vs[i]; if (v != null && b60 > 0) mx = Math.max(mx, v / b60); }
      const st = ratio >= 2 ? '급증' : ratio <= 0.5 ? '급감(관심 이탈)' : '보통';
      add({ id: 'vol', grp: 'px', tone: ratio >= 2 ? 'good' : ratio <= 0.5 ? 'bad' : 'flat', label: vname + ' ' + st, detail: '최근 5일 평균이 직전 60일 평균의 ' + ratio.toFixed(2) + '배 (최근 5일 중 최대 ' + mx.toFixed(1) + '배)', ev: 'proof', note: '높은 거래량 이후 수익 프리미엄(Gervais-Kaniel-Mingelgrin 2001), 투자자 관심 급증(Barber-Odean 2008). 효과는 주로 단기(약 1개월)이고 거래비용 반영 전입니다.' });
    }
    const m20 = sigSma(cl, 20, last), m60 = sigSma(cl, 60, last), m120 = sigSma(cl, 120, last), m120p = sigSma(cl, 120, last - 20), px1 = cl[last];
    if (m20 && m60 && m120 && px1 > 0) {
      let st, tone = 'flat';
      if (px1 > m20 && m20 > m60 && m60 > m120) { st = '정배열 (종가>20일>60일>120일선)'; tone = 'good'; }
      else if (m20 < m60 && m60 < m120 && px1 < m20) { st = '역배열 (종가<20일<60일<120일선)'; tone = 'bad'; }
      else if (m60 > m120 && (m120p == null || m120 >= m120p) && px1 < m20) { st = '상승추세 속 조정 (60일>120일선, 종가<20일선)'; tone = 'flat'; }
      else st = '혼조 (뚜렷한 배열 없음)';
      add({ id: 'trend', grp: 'px', tone: tone, label: '이동평균 상태: ' + st, detail: '종가 ' + Math.round(px1).toLocaleString() + ' · 20일 ' + Math.round(m20).toLocaleString() + ' · 60일 ' + Math.round(m60).toLocaleString() + ' · 120일 ' + Math.round(m120).toLocaleString(), ev: 'practice', note: '이동평균 매매는 연구 결과가 엇갈립니다(Brock-Lakonishok-LeBaron 1992는 유효, Bajgrowicz-Scaillet 2012는 데이터 스누핑·비용 반영 시 약화). 추세 필터로 낙폭을 줄이는 용도는 상대적으로 지지됩니다(Faber 2007).' });
    }
    const w = cl.slice(Math.max(0, n - 252)).filter((x) => x > 0), hi = Math.max.apply(null, w), lo = Math.min.apply(null, w);
    if (w.length >= 120 && px1 > 0) {
      meta.fromHi = px1 / hi - 1; meta.fromLo = px1 / lo - 1;
      add({ id: 'hi52', grp: 'px', tone: meta.fromHi >= -0.05 ? 'good' : 'flat', label: '52주 고점 대비 ' + sigPct(meta.fromHi), detail: '52주 고가 ' + Math.round(hi).toLocaleString() + ' · 저가 ' + Math.round(lo).toLocaleString() + ' (저가 대비 ' + sigPct(meta.fromLo) + ')', ev: 'proof', note: '52주 고점에 가까운 종목이 이후에도 상대적으로 강한 경향(George-Hwang 2004). 반대로 고점에서 멀다고 저평가인 것은 아닙니다.' });
      if (meta.fromLo <= 0.15 && m120 && px1 < m120) add({ id: 'low52', grp: 'px', tone: 'flat', label: '52주 저점권 (저가 +15% 이내, 120일선 아래)', detail: '‘바닥’은 사후에만 확인됩니다. 하락추세가 이어지는 구간일 수 있습니다.', ev: 'assume', note: '바닥을 사전에 식별하는 방법은 학술적으로 검증된 바 없습니다. 위험 확정(실적·공시) 확인 없이 저점 매수만으로 근거로 삼지 않습니다.' });
    }
    // 급등·대량거래 이력(최근 252거래일)
    let up = 0, big = 0, lastDt = null, mx21 = null;
    for (let i = Math.max(1, n - 252); i < n; i++) {
      const p0 = cl[i - 1], p1 = cl[i];
      if (p0 > 0 && p1 > 0) { const r = p1 / p0 - 1; if (r >= 0.29) { up++; lastDt = px.d[i]; } if (i >= n - 21) mx21 = mx21 == null ? r : Math.max(mx21, r); }
      if (px.v && px.v[i] >= 10000000) { big++; if (!lastDt || px.d[i] > lastDt) lastDt = px.d[i]; }
    }
    meta.up = up; meta.big = big; meta.mx21 = mx21;
    if (up > 0 || big > 0) add({ id: 'spike', grp: 'px', tone: 'warn', label: '최근 1년 상한가권(+29%↑) ' + up + '회 · 거래량 1,000만주↑ ' + big + '일', detail: lastDt ? '가장 최근 ' + lastDt : '', ev: 'assume', note: '“과거에 상한가·대량거래가 있었다”는 조건 자체는 초과수익의 근거가 아닙니다. 복권형 급등주(일간 최대수익률이 컸던 종목)는 이후 수익이 낮은 경향이 보고됐습니다(Bali-Cakici-Whitelaw 2011, 미국). 관심 종목 발굴(관찰) 단계의 필터로만 쓰고, 매수 근거는 다른 항목으로 확인하세요.' });
    else add({ id: 'spike', grp: 'px', tone: 'flat', label: '최근 1년 상한가권·1,000만주↑ 거래 이력 없음', detail: '', ev: 'assume', note: '이력이 없다는 것이 나쁘다는 뜻은 아닙니다. 관찰 단계의 참고 정보입니다.' });
    if (mx21 != null && mx21 >= 0.15) add({ id: 'maxe', grp: 'px', tone: 'warn', label: '최근 1개월 내 하루 +' + (mx21 * 100).toFixed(0) + '% 급등 발생', detail: '복권형 급등 직후는 되돌림 위험이 큰 구간일 수 있습니다.', ev: 'proof', note: 'MAX 효과(Bali-Cakici-Whitelaw 2011): 최근 일간 최대수익률이 높은 종목의 이후 수익률이 낮은 경향(미국, 한국 시장은 별도 검증 필요).' });
    if (last >= 21 && cl[last - 21] > 0) {
      const r1 = px1 / cl[last - 21] - 1; meta.r1m = r1;
      if (r1 >= 0.3) add({ id: 'rev1m', grp: 'px', tone: 'warn', label: '최근 1개월 ' + sigPct(r1) + ' 상승 (단기 과열 구간)', detail: '1개월 수익률이 큰 종목은 다음 달 되돌림이 잦은 경향이 있어, 모멘텀 계산도 최근 1개월을 제외합니다(6-1, 12-1).', ev: 'proof', note: '1개월 단기 반전(Jegadeesh 1990, Lehmann 1990). 추격 매수 전에 확인할 항목입니다.' });
    }
  }
  return { items: items, meta: meta };
}
function sigById(S, id) { for (let i = 0; i < S.items.length; i++) if (S.items[i].id === id) return S.items[i]; return null; }
function sigPx() {
  const nm = (document.getElementById('corpName').value || '').trim();
  return (typeof PRICE !== 'undefined' && PRICE.data && PRICE.name === nm) ? PRICE.data : null;
}
// ----- 체크리스트 -----
// auto: (c, S) => 참고 문구(체크는 사용자가 직접). ev는 근거 수준.
const SIG_BUY = [
  { t: '“없다가 생긴 것 / 있다가 사라진 것”이 무엇인지 답할 수 있다', ev: 'proof', why: '기대 대비 변화(서프라이즈)에 가격이 반응한다는 점은 학술적으로 잘 지지됩니다. 단, 무엇이 ‘변화’인지는 투자자 관점의 판단입니다.', auto: (c, S) => { const f = S.items.filter((x) => x.grp === 'fund'); return f.length ? '자동 감지: ' + f.map((x) => x.label).join(' / ') : '자동 감지된 재무 변화 없음(뉴스·정책 변화는 직접 확인)'; } },
  { t: '불확실성이 ‘알려진 리스크’로 확정되는 시점인지 확인했다', ev: 'assume', why: '불확실성과 리스크의 구분은 Knight(1921)의 고전적 개념입니다. ‘리스크 확정 = 바닥’이라는 규칙은 사전 검증된 적이 없어 가설로만 사용합니다.', auto: () => '' },
  { t: '유동성·실적·기대감을 각각 점검했다 (주가 = EPS × PER)', ev: 'theory', why: '주가 = EPS × PER은 항등식입니다. 유동성·기대감은 PER(할인율·성장 기대)에 반영되고 실적은 EPS에 반영됩니다. ‘(유동성+실적)×기대감’은 직관 틀이며 측정 가능한 공식은 아닙니다.', auto: (c, S) => { const e = c.last ? c.last.epsYoy : null, per = c.vs ? c.vs.per : null; return '실적: 최근 연간 EPS YoY ' + (anFin(e) ? sigPct(e) : 'N/A') + ' · 기대감: 현재 PER ' + (anFin(per) ? per.toFixed(1) + '배' : 'N/A') + ' · 유동성(거래): ' + (S.meta.volRatio != null ? '최근 5일 거래 ' + S.meta.volRatio.toFixed(2) + '배' : '주가 로딩 전'); } },
  { t: '재료·차트·거래량·시황을 함께 봤다', ev: 'practice', why: '단일 신호보다 여러 독립 신호의 일치를 보는 것은 합리적이나, 어떤 결합이 초과수익을 주는지는 백테스트 전에는 알 수 없습니다.', auto: (c, S) => { const t = sigById(S, 'trend'), v = sigById(S, 'vol'); return (t ? t.label : '이동평균: 자료 없음') + ' · ' + (v ? v.label : '거래: 자료 없음'); } },
  { t: '시장이 좋아할 재료인지(시장 참여자가 사고 싶어 할 이유) 설명할 수 있다', ev: 'assume', why: '뉴스는 제목만 네이버 검색 API로 모아 신규/소멸 단어·반복도를 보여주고(참고용), 공시(1차 자료)로 확인되는 부분은 ‘📰 재료 점검’이 요약합니다. 호재·악재 판단은 직접 하세요. 뉴스·재료에 대한 반응은 소형·비유동 종목에서 크고 빠르게 되돌려지기도 합니다(Chan 2003, 미국).', auto: () => matAutoText() },
  { t: '거래량이 잘 터지고 조정을 충분히 받은 종목이다', ev: 'practice', why: '거래량 급증은 단기 수익 프리미엄(Gervais 외 2001)이 보고되었으나, ‘조정을 잘 받았다’(눌림목)는 규칙은 검증되지 않았습니다.', auto: (c, S) => { const v = S.meta.volRatio, h = S.meta.fromHi; return '거래 최근 5일/직전 60일 ' + (v != null ? v.toFixed(2) + '배' : 'N/A') + ' · 52주 고점 대비 ' + (h != null ? sigPct(h) : 'N/A'); } },
  { t: '정부·대기업 정책·투자와 관련이 있는지 확인했다', ev: 'assume', why: '정책 수혜 테마는 선반영과 소멸이 빠릅니다. 공시(공급계약·투자)로 확인되는 사실과 기대를 구분하세요.', auto: () => '' },
  { t: '키워드 하나에만 의존하지 않고 서로 다른 근거가 2개 이상이다', ev: 'theory', why: '단일 이야기 의존은 확증편향·테마 집착의 위험을 키웁니다(분산된 근거 확인).', auto: () => '' },
  { t: '시장 전체 흐름(시황)을 고려했다', ev: 'practice', why: '시장 연동 상승은 시장 반전 때 함께 꺾입니다. 종목 고유 초과수익인지 확인합니다.', auto: (c) => { const k = AN_STATE.mom && AN_STATE.mom.kospi ? AN_STATE.mom.kospi.r6m : null, r = c.mom ? c.mom.ret6 : null; return '최근 6개월: 코스피 ' + (anFin(k) ? sigPct(k) : 'N/A') + ' · 종목 ' + (anFin(r) ? sigPct(r) : 'N/A'); } },
  { t: '매수 이유를 한 문장으로 말할 수 있다 (아래 칸에 적었다)', ev: 'theory', why: '사전에 이유·반증 조건을 적어 두는 것은 사후 합리화·뇌동매매를 줄이는 사전 약속(pre-commitment)입니다.', auto: () => '', needNote: 'why' },  { t: '최근 1년 공시에서 “새로 생긴 재료”와 “사라진 재료”를 구분해 확인했다', ev: 'proof', why: '기대 대비 변화에 가격이 반응·지연 반응한다는 연구(Ball-Brown 1968, Chan 2003 등)가 있으나 미국 중심이며, ‘1년’ 창은 사용자가 정한 관찰 기간으로 최적이라는 증거는 없습니다.', auto: () => matAutoText() },
  { t: '큰 가격 움직임에 공시·뉴스 이유가 있는지 확인했다 (이유 없는 급등락은 되돌림 가능성)', ev: 'proof', why: '뉴스 없이 크게 움직인 종목은 이후 되돌려지는 경향, 뉴스가 있는 경우는 같은 방향으로 이어지는 경향(Chan 2003, 소형·비유동 종목에서 강함, 미국). 오래된 뉴스에 대한 과잉 반응도 보고됨(Tetlock 2011).', auto: () => '‘📰 재료 점검’ ④표에서 공시 없는 큰 움직임과 그날 뉴스 링크를 확인하세요.' },
];
const SIG_HOLD = [
  { t: '매수한 종목에 과도한 기대감·욕심이 생기면 물량을 줄인다', ev: 'theory', why: '과신은 과도한 거래·성과 악화와 연관됩니다(Barber-Odean 2000). 감정이 아니라 비중 규칙(최대 비중, 위험예산)으로 줄이는 것이 지지됩니다. 내 계좌의 계획·위험예산 기능과 연결됩니다.' },
  { t: '오르기 시작해 더 사고 싶어지면 즉시 익절한다', ev: 'assume', why: '주의: 이익 종목을 너무 빨리 파는 것이 처분효과(Shefrin-Statman 1985, Odean 1998)의 전형이고, 모멘텀 효과(승자가 계속 오르는 경향)와 충돌할 수 있습니다. 감정 신호가 아니라 미리 정한 분할 익절 규칙(목표 도달 시 일부)으로 대체하는 편이 안전합니다.' },
  { t: '수익 중일 때 시장 때문인지 종목 때문인지 재점검한다', ev: 'theory', why: '시장 요인과 종목 고유 요인의 분리는 자산가격 이론의 기본입니다. 위 체크리스트의 6개월 상대수익(코스피 대비)이 단서가 됩니다.' },
  { t: '목표 수익률 도달 시 원칙대로 익절한다 (연습)', ev: 'practice', why: '사전 목표가 감정 개입을 줄입니다. 다만 목표 도달 후 전량 매도가 기대수익을 높인다는 증거는 없으므로, 일부 매도+나머지는 추세 이탈 기준 등으로 설계하세요.' },
  { t: '손절 기준을 매수 전에 정해 두고, 틀리면 인정한다', ev: 'practice', why: '손절 규칙은 손실 폭을 제한하지만 평균 수익을 높이는 것은 아닙니다(Kaminski-Lo 2014: 추세가 있는 구간에서만 이득). 늦은 손절·망설인 손절은 처분효과와 손실회피(Kahneman-Tversky 1979)의 결과입니다.' },
];
function sigStoreKey() { return 'sigChk:' + (AN_STATE.key || ''); }
function sigLoad() { try { const o = JSON.parse(localStorage.getItem(sigStoreKey()) || 'null'); return (o && typeof o === 'object') ? { c: o.c || {}, n: o.n || {} } : { c: {}, n: {} }; } catch (e) { return { c: {}, n: {} }; } }
function sigSave(o) { try { localStorage.setItem(sigStoreKey(), JSON.stringify(o)); } catch (e) { /* 저장 실패는 무시 */ } }
function sigChk(i, v) { const o = sigLoad(); o.c[i] = !!v; sigSave(o); sigUpdateSummary(); }
const SIG_NOTE_KEYS = ['why', 'disproof', 'exit'];
function sigNote(i, v) { const k = SIG_NOTE_KEYS[i]; if (!k) return; const o = sigLoad(); o.n[k] = String(v).slice(0, 500); sigSave(o); sigUpdateSummary(); }
function sigUpdateSummary() {
  const el = document.getElementById('sigChkSum'); if (!el) return;
  const o = sigLoad(); let n = 0; for (let i = 0; i < SIG_BUY.length; i++) if (o.c[i]) n++;
  const why = (o.n.why || '').trim().length > 0;
  el.innerHTML = '체크 <b>' + n + '/' + SIG_BUY.length + '</b> · 매수 이유 ' + (why ? '<b style="color:#16a34a;">작성됨</b>' : '<b style="color:#dc2626;">미작성 → 이유를 말할 수 없으면 사지 않는다</b>');
}
const SIG_TONE = { good: '#16a34a', bad: '#dc2626', warn: '#d97706', flat: '#64748b' };
function sigBadgeHtml(k) { return anBadge(k, SIG_BADGE[k] || '가정·휴리스틱'); }
function sigItemHtml(it) {
  return '<details style="border-top:1px solid var(--border); padding:6px 0;"><summary style="cursor:pointer; font-size:13px; font-weight:600; color:' + (SIG_TONE[it.tone] || '#64748b') + ';">' + anEsc(it.label) + ' ' + sigBadgeHtml(it.ev) + '</summary>' +
    '<div style="font-size:12px; margin:4px 0;">' + anEsc(it.detail || '') + '</div><div style="font-size:11.5px; color:var(--text-muted); line-height:1.5;">근거 평가: ' + anEsc(it.note || '') + '</div></details>';
}
function sigRenderDetectBody(c) {
  const px = sigPx(), S = sigDetect(c, px);
  const fund = S.items.filter((x) => x.grp === 'fund'), pxs = S.items.filter((x) => x.grp === 'px');
  let h = '<div style="font-weight:700; font-size:13px; margin-top:6px;">재무에서 없다가 생긴 것 · 있다가 사라진 것</div>';
  h += fund.length ? fund.map(sigItemHtml).join('') : '<div style="font-size:12.5px; color:var(--text-muted); margin:4px 0;">직전 사업연도 대비 흑자/적자 전환·배당 시작/중단 같은 큰 변화는 감지되지 않았습니다.</div>';
  h += '<div style="font-weight:700; font-size:13px; margin-top:12px;">가격·거래량 상태 (일별 주가 기준)</div>';
  h += px ? (pxs.length ? pxs.map(sigItemHtml).join('') : '<div style="font-size:12.5px; color:var(--text-muted);">일별 주가가 130거래일 미만이라 판정하지 않습니다.</div>') : '<div style="font-size:12.5px; color:var(--text-muted); margin:4px 0;">일별 주가를 불러오는 중이거나 불러오지 못했습니다(주가 차트와 같은 자료).</div>';
  return h;
}
function sigChecklistHtml(c) {
  const px = sigPx(), S = sigDetect(c, px), o = sigLoad();
  const row = (arr, off, withBox) => arr.map((it, i) => {
    const idx = off + i, auto = it.auto ? it.auto(c, S) : '';
    return '<div style="border-top:1px solid var(--border); padding:6px 0;"><label style="display:flex; gap:8px; align-items:flex-start; font-size:13px;">' + (withBox ? '<input type="checkbox" ' + (o.c[idx] ? 'checked ' : '') + 'onchange="sigChk(' + idx + ', this.checked)" style="margin-top:3px; flex:0 0 auto;" />' : '<span style="flex:0 0 auto;">•</span>') +
      '<span>' + anEsc(it.t) + ' ' + sigBadgeHtml(it.ev) + '</span></label>' +
      (auto ? '<div style="font-size:11.5px; color:#0369a1; margin:2px 0 0 28px;">자동 참고: ' + anEsc(auto) + '</div>' : '') +
      '<details style="margin:2px 0 0 28px;"><summary style="font-size:11.5px; color:var(--text-muted); cursor:pointer;">이 규칙의 근거 평가</summary><div style="font-size:11.5px; color:var(--text-muted); line-height:1.5;">' + anEsc(it.why) + '</div></details></div>';
  }).join('');
  const ta = (k, label, ph) => '<label style="display:block; font-size:12.5px; margin-top:8px; font-weight:600;">' + label + '<textarea rows="2" maxlength="500" placeholder="' + ph + '" oninput="sigNote(' + SIG_NOTE_KEYS.indexOf(k) + ', this.value)" style="width:100%; box-sizing:border-box; font-weight:400; margin-top:3px;">' + anEsc(o.n[k] || '') + '</textarea></label>';
  return '<div id="sigChkSum" style="font-size:12.5px; margin:6px 0;"></div>' + row(SIG_BUY, 0, true) +
    ta('why', '매수 이유 (한 문장)', '예: 영업이익이 흑자 전환했고, 거래량이 늘며 20일선을 회복했다') +
    ta('disproof', '틀렸다고 인정할 조건 (반증·손절 기준)', '예: 종가가 120일선 아래로 2일 이상 / 다음 분기 영업이익이 다시 적자') +
    ta('exit', '목표·익절 계획 (분할 기준)', '예: +25%에서 1/3, +40%에서 1/3, 나머지는 60일선 이탈 시') +
    '<div style="font-weight:700; font-size:13px; margin-top:14px;">보유 중 점검 규칙 (점수 아님 · 근거 평가 포함)</div>' + row(SIG_HOLD, 100, false) +
    '<div style="font-size:11.5px; color:var(--text-muted); margin-top:8px; line-height:1.5;">입력한 내용은 이 기기(브라우저)에만 종목별로 저장됩니다. 실제 매매 계획·일지는 내 계좌의 ‘계획’과 ‘매매일지’에서 관리하세요.</div>';
}
// 일별 시세에서 기준일(YYYYMMDD) 이전 가장 가까운 종가. 10일(연휴 감안)보다 오래된 값이나 시세 범위 밖이면 null.
function sigCloseAsOf(px, ymd) {
  if (!px || !px.d || !px.c || !px.d.length || !ymd) return null;
  if (ymd < String(px.d[0])) return null;   // 시세가 시작되기 전 날짜(위쪽 범위 밖은 아래 10일 규칙이 거른다)
  let k = -1;
  for (let i = px.d.length - 1; i >= 0; i--) { if (String(px.d[i]) <= ymd) { k = i; break; } }
  if (k < 0 || !(px.c[k] > 0)) return null;
  const gap = (Date.UTC(+ymd.slice(0, 4), +ymd.slice(4, 6) - 1, +ymd.slice(6, 8)) - Date.UTC(+String(px.d[k]).slice(0, 4), +String(px.d[k]).slice(4, 6) - 1, +String(px.d[k]).slice(6, 8))) / 86400000;
  return gap <= 10 ? px.c[k] : null;
}
// 분해로 복원한 주가변화와 실제 주가변화(일별 시세) 비교 한 줄. d = sigDecomp 결과(오류 없는 것), px = 일별 시세(없으면 null).
//  - 복원(곱셈) = (1+EPS성장)×(1+PER변화)−1. 연간 보고서의 기간말 주가·EPS로 만든 값이다.
//  - 실제(일별 시세) = 같은 두 기간말 날짜의 일별 종가로 직접 계산한 연환산 변화. 서로 다르면 보고서 주가·기간말 날짜·액면분할 반영 여부를 점검한다.
//  - 덧셈 근사 = EPS성장+PER변화+배당수익률. 교차항 때문에 총수익률과 조금 다르다(근사 오차).
function sigCompare(d, px) {
  const diff = (x, y) => (anFin(x) && anFin(y)) ? x - y : null;
  const c0 = sigCloseAsOf(px, d.a + '1231'), c1 = sigCloseAsOf(px, d.b + '1231');
  const real = (c0 > 0 && c1 > 0) ? Math.pow(c1 / c0, 1 / d.n) - 1 : null;
  const gap = diff(real, d.exactPrice);
  return {
    restored: d.exactPrice, real: real, gap: gap, c0: c0, c1: c1,
    flag: anFin(gap) && Math.abs(gap) > 0.01,   // 연환산 1%p 넘게 어긋나면 점검
    tr: d.tr, approx: d.approx, approxErr: diff(d.approx, d.tr),
  };
}
const sigPp = (v) => anFin(v) ? (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%p' : 'N/A';
function sigCompareHtml(c) {
  const L = c.list, px = sigPx(), rows = [];
  const line = (label, d) => {
    if (d.err) return '<tr><td class="an-first">' + label + '</td><td colspan="6" style="text-align:left; color:var(--text-muted);">산출 불가: ' + anEsc(d.err) + '</td></tr>';
    const k = sigCompare(d, px);
    const realTxt = k.real != null ? sigPct(k.real) : '<span style="color:var(--text-muted);">' + (px ? '시세 범위 밖' : '시세 없음') + '</span>';
    return '<tr><td class="an-first">' + label + '</td><td>' + sigPct(k.restored) + '</td><td>' + realTxt + '</td><td' + (k.flag ? ' style="color:#dc2626; font-weight:700;"' : '') + '>' + sigPp(k.gap) + (k.flag ? ' ⚠' : '') + '</td><td>' + (k.tr != null ? sigPct(k.tr) : 'N/A') + '</td><td>' + (k.approx != null ? sigPct(k.approx) : 'N/A') + '</td><td>' + sigPp(k.approxErr) + '</td></tr>';
  };
  [3, 5].forEach((n) => { const d = sigDecomp(L, n); rows.push(line(d.a && d.b ? d.a + '→' + d.b + ' (연환산 ' + n + '년)' : '최근 ' + n + '년', d)); });
  let cnt = 0;
  for (let k = L.length - 1; k >= 1 && cnt < 8; k--, cnt++) { const d = sigDecomp(L.slice(0, k + 1), 1); rows.push(line((L[k].year - 1) + '→' + L[k].year, d)); }
  const th = '<thead><tr><th class="an-first">기간</th><th>분해로 복원한<br />주가변화<br />(EPS×PER)</th><th>실제 주가변화<br />(일별 시세)</th><th>차이</th><th>총수익률<br />(배당 포함)</th><th>덧셈 근사<br />(EPS+PER+배당)</th><th>근사 오차</th></tr></thead>';
  return '<div style="font-weight:600; font-size:12.5px; margin-top:12px;">🔎 분해로 복원한 주가변화 vs 실제 주가변화</div>' +
    '<div style="font-size:12px; line-height:1.55; margin:4px 0;">① <b>복원한 주가변화</b>는 연간 보고서의 기간말 주가·EPS로 (1+EPS성장)×(1+PER변화)−1을 계산한 값이고, <b>실제 주가변화</b>는 같은 기간말 날짜의 <b>일별 시세 종가</b>로 직접 계산한 연환산 변화입니다. 둘이 연 1%p 넘게 다르면 ⚠로 표시하니 보고서 주가·기간말 날짜·액면분할 반영 여부를 점검하세요. 일별 시세는 최근 약 3년만 불러오므로 그보다 오래된 기간은 비교하지 못합니다. ② <b>덧셈 근사</b>는 세 항을 더한 값이라 배당 재투자를 반영한 <b>총수익률</b>과 조금 다릅니다(근사 오차 = 덧셈 근사 − 총수익률).</div>' +
    '<div style="overflow-x:auto;"><table class="an-tbl">' + th + '<tbody>' + rows.join('') + '</tbody></table></div>';
}
// ----- 수익률 분해 · 기대수익률 -----
const SIG_ST = { g: null, n: 3 };
function sigDecompHtml(c) {
  const L = c.list, rows = [];
  const cell = (v) => '<td>' + (anFin(v) ? sigPct(v) : 'N/A') + '</td>';
  [3, 5].forEach((n) => {
    const d = sigDecomp(L, n);
    if (d.err) rows.push('<tr><td class="an-first">최근 ' + n + '년 (연환산)</td><td colspan="6" style="text-align:left; color:var(--text-muted);">산출 불가: ' + anEsc(d.err) + '</td></tr>');
    else rows.push('<tr><td class="an-first">' + d.a + '→' + d.b + ' (연환산 ' + n + '년)</td>' + cell(d.priceG) + cell(d.epsG) + cell(d.perCh) + cell(d.divY) + cell(d.tr) + '<td>' + (d.approx != null ? sigPct(d.approx) : 'N/A') + '</td></tr>');
  });
  const yr = [];
  for (let k = L.length - 1; k >= 1 && yr.length < 8; k--) {
    const d = sigDecomp(L.slice(0, k + 1), 1);
    if (d.err) yr.push('<tr><td class="an-first">' + (L[k].year - 1) + '→' + L[k].year + '</td><td colspan="6" style="text-align:left; color:var(--text-muted);">산출 불가: ' + anEsc(d.err) + '</td></tr>');
    else yr.push('<tr><td class="an-first">' + d.a + '→' + d.b + '</td>' + cell(d.priceG) + cell(d.epsG) + cell(d.perCh) + cell(d.divY) + cell(d.tr) + '<td>' + (d.approx != null ? sigPct(d.approx) : 'N/A') + '</td></tr>');
  }
  const th = '<thead><tr><th class="an-first">기간</th><th>주가 변화</th><th>EPS 성장</th><th>PER 변화</th><th>배당수익률</th><th>총수익률</th><th>덧셈 근사</th></tr></thead>';
  return '<div style="font-size:12.5px; line-height:1.55; margin:4px 0;">주가수익률은 <b>배당수익률 + EPS 성장률 + PER 변화율</b>로 나뉩니다. 정확한 관계는 곱셈 (1+주가변화) = (1+EPS성장)×(1+PER변화)이고, 덧셈은 근사입니다(교차항·배당 재투자 때문에 총수익률과 조금 다름). ‘주가가 올랐다’가 <b>실적(EPS) 때문인지, 기대감(PER) 때문인지</b>를 가려내는 표입니다.</div>' +
    '<div style="overflow-x:auto;"><table class="an-tbl">' + th + '<tbody>' + rows.join('') + '</tbody></table></div>' +
    '<div style="font-weight:600; font-size:12.5px; margin-top:10px;">연도별</div><div style="overflow-x:auto;"><table class="an-tbl">' + th + '<tbody>' + yr.join('') + '</tbody></table></div>' +
    '<div id="sigCmpBody">' + sigCompareHtml(c) + '</div>' +
    '<div style="font-size:11px; color:var(--text-muted); margin-top:6px; line-height:1.5;">기준: 연간 보고서의 기간말 주가(period end)와 연간 EPS(지배주주 순이익÷유통주식수, 적자면 N/A), 해당 연도 주당배당금(전년말 주가 대비). 주식수가 30% 넘게 변한 해는 분해하지 않습니다. 총수익률은 배당을 해당 연도 말 주가로 재투자한다고 가정한 연복리입니다. 세금·수수료 미반영. 과거 분해는 미래 수익을 보장하지 않습니다.</div>';
}
function sigCalcVals(c) {
  const per = c.vs ? c.vs.per : null, price = c.price, dps = c.last && c.last.r ? c.last.r.dividend_per_share : null;
  const divNow = (price > 0 && dps != null && isFinite(dps)) ? dps / price : null;
  const g0 = anFin(SIG_ST.g) ? SIG_ST.g : (anFin(c.gRoe) ? c.gRoe : (anFin(c.epsG) ? c.epsG : null));
  return { per: per, divNow: divNow, g0: g0, n: SIG_ST.n };
}
// 입력칸은 다시 그리지 않는다(성장률을 고친 직후 기간 칸을 누르는 중에 칸이 교체되어 입력이 사라지는 것을 막기 위해). 표만 갱신한다.
function sigCalcHtml(c) {
  const v = sigCalcVals(c);
  return '<div class="row" style="gap:8px; align-items:flex-end; margin:6px 0;"><label style="font-size:12.5px;">EPS 성장률 가정(연 %) <input id="sigG" type="number" inputmode="decimal" step="0.5" value="' + (v.g0 != null ? (v.g0 * 100).toFixed(1) : '') + '" onchange="sigCalcSet()" style="width:100px;" /></label>' +
    '<label style="font-size:12.5px;">기간(년) <input id="sigN" type="number" inputmode="numeric" min="1" max="10" value="' + v.n + '" onchange="sigCalcSet()" style="width:70px;" /></label></div><div id="sigCalcTbl">' + sigCalcTbl(c) + '</div>';
}
function sigCalcTbl(c) {
  const v = sigCalcVals(c), g0 = v.g0, n = v.n;
  if (g0 == null) return '<div style="font-size:12.5px; color:var(--text-muted);">기본 성장률을 만들 자료(지속가능성장률·EPS CAGR)가 없습니다. 성장률을 직접 입력하세요.</div>';
  const gs = [g0 - 0.05, g0, g0 + 0.05], ps = [-0.3, 0, 0.3];
  const tone = (x) => x == null ? 'N/A' : '<span style="color:' + (x >= 0.1 ? '#16a34a' : x < 0 ? '#dc2626' : 'inherit') + ';">' + sigPct(x) + '</span>';
  const body = gs.map((g) => '<tr><td class="an-first">EPS 성장 ' + sigPct(g) + '</td>' + ps.map((p) => '<td>' + tone(sigExpected(v.divNow, g, p, n)) + '</td>').join('') + '</tr>').join('');
  return '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">연환산 기대수익률(배당 포함)</th><th>PER ' + n + '년간 −30%</th><th>PER 유지</th><th>PER +30%</th></tr></thead><tbody>' + body + '</tbody></table></div>' +
    '<div style="font-size:11px; color:var(--text-muted); margin-top:6px; line-height:1.5;">현재 PER ' + (anFin(v.per) ? v.per.toFixed(1) + '배' : 'N/A') + ' · 현재 배당수익률 ' + (v.divNow != null ? anPct(v.divNow) : 'N/A') + ' · 기본 성장률은 ROE×유보율(지속가능성장률)이며 직접 바꿀 수 있습니다. 산식: (1+EPS성장)×(1+PER변화의 연환산)−1+배당수익률. <b>예측이 아니라 가정의 산술</b>이며, PER이 줄어드는 칸(왼쪽)이 하방 위험을 보여줍니다.</div>';
}
function sigCalcSet() {
  const g = Number((document.getElementById('sigG') || {}).value), n = Math.floor(Number((document.getElementById('sigN') || {}).value));
  if (isFinite(g) && g > -50 && g < 100) SIG_ST.g = g / 100;
  if (n >= 1 && n <= 10) SIG_ST.n = n;
  const el = document.getElementById('sigCalcTbl'); if (el && AN_CTX) el.innerHTML = sigCalcTbl(AN_CTX);
}
function sigRenderSections(c) {
  const evTxt = '증거 수준 표시: 학술 검증(동료심사 연구가 있음, 한국 시장은 별도 검증 필요) · 이론 근거 · 실무 관행(수익 보장 없음) · 가정·휴리스틱(검증된 바 없음).';
  return anSection('📐 수익률 분해: 배당수익률 + EPS 성장 + PER 변화', '#7c3aed', '주가가 오른 이유를 실적과 기대감으로 나눕니다. 항등식이라 이론적으로 맞는 관계이며(Ibbotson-Chen 2003 등 장기 수익률 분해 연구), 값은 이 앱의 연간 공시 자료에서 계산합니다.', sigDecompHtml(c), false) +
    anSection('🧮 기대수익률 가정 계산 (EPS 성장 × PER 변화 + 배당)', '#7c3aed', '미래 수익률은 예측할 수 없으므로 성장률·PER 변화를 가정으로 바꿔 가며 범위를 봅니다. 하나의 목표가 대신 민감도 표를 사용합니다.', '<div id="sigCalcBody">' + sigCalcHtml(c) + '</div>', false) +
    anSection('🔔 변화 감지 시그널 (없다가 생김 · 있다가 사라짐)', '#0891b2', '시장은 ‘상태’보다 ‘변화’에 반응한다는 관점입니다. 재무 변화는 공시 자료로, 가격·거래량은 일별 주가로 계산합니다. 모든 시그널에 근거 수준을 붙였고, 매수·매도 신호나 점수를 만들지 않습니다. ' + evTxt, '<div id="sigDetectBody">' + sigRenderDetectBody(c) + '</div>', false) +
    anSection('✅ 투자 판단 체크리스트 (관찰 → 시그널 → 전략 → 반성)', '#0891b2', '매수 전 10가지 질문과 보유 중 규칙입니다. 자동 참고는 계산 가능한 부분만 보여주고, 체크는 직접 합니다. 충족 개수는 ‘매수 신호’가 아니라 내가 생각을 정리했는지의 기록입니다.', '<div id="sigChecklistBody">' + sigChecklistHtml(c) + '</div>', false);
}
function sigRefresh() {
  if (!AN_CTX) return;
  const a = document.getElementById('sigDetectBody'); if (a) a.innerHTML = sigRenderDetectBody(AN_CTX);
  const cm = document.getElementById('sigCmpBody'); if (cm) cm.innerHTML = sigCompareHtml(AN_CTX); // 일별 시세가 늦게 도착해도 비교표가 채워지게
  const b = document.getElementById('sigChecklistBody'); if (b) { b.innerHTML = sigChecklistHtml(AN_CTX); sigUpdateSummary(); }
  try { matRefresh(); } catch (e) { /* 재료 점검 갱신 실패는 무시 */ }
}
// ===== /시그널·수익률 분해 =====

// ===== 재료(공시) 점검 · 최근 1년 (없다가 생긴 것 / 있다가 사라진 것) =====
// 데이터: DART 공시목록(1차 자료) + 네이버 뉴스 검색 API(제목·날짜·링크만, 본문 없음). 뉴스는 키가 없거나 실패하면 검색 링크만 제공한다.
// 점수·매수신호를 만들지 않는다. 분류는 공시 제목 문자열 규칙(규칙 기반)이라 오분류가 있을 수 있다.
const MAT_CATS = [
  { k: 'earn', n: '실적·손익' }, { k: 'order', n: '수주·공급계약' }, { k: 'fund', n: '자금조달·희석' },
  { k: 'ret', n: '주주환원' }, { k: 'mna', n: '지분·M&A·지배구조' }, { k: 'invest', n: '시설투자' },
  { k: 'risk', n: '리스크·제재' }, { k: 'insider', n: '내부자·대량보유' }, { k: 'other', n: '기타 수시공시' },
];
const MAT_BIG_MOVE = 0.07; // 시장조정 1일 수익률 ±7% 이상 = "큰 움직임". 사전에 고정한 값(결과를 보고 바꾸지 않음)
function matNorm(s) { return String(s || '').split(' ').join('').split('ㆍ').join('').split('·').join(''); }
function matClass(nm) {
  let s = matNorm(nm), corr = false;
  if (s.indexOf('[') === 0 && s.indexOf(']') > 0) {
    const pre = s.slice(1, s.indexOf(']'));
    if (pre === '기재정정' || pre === '첨부정정' || pre === '첨부추가') corr = true;
    s = s.slice(s.indexOf(']') + 1);
  }
  const has = (a) => a.some((w) => s.indexOf(w) >= 0);
  let cat = 'other';
  if (s.indexOf('사업보고서') === 0 || s.indexOf('반기보고서') === 0 || s.indexOf('분기보고서') === 0 || s.indexOf('감사보고서') === 0 || s.indexOf('연결감사보고서') === 0) cat = 'periodic';
  else if (has(['조회공시', '불성실공시', '횡령', '배임', '소송', '상장폐지', '관리종목', '투자주의환기', '감사의견', '회생', '부도', '거래정지', '영업정지', '채무불이행', '자본잠식', '상장적격성', '과징금', '벌금'])) cat = 'risk';
  else if (has(['특정증권등소유상황', '대량보유'])) cat = 'insider';
  else if (has(['단일판매공급계약'])) cat = 'order';
  else if (has(['잠정)실적', '영업잠정실적', '손익구조', '결산실적공시예고'])) cat = 'earn';
  else if (has(['유상증자', '전환사채', '신주인수권부사채', '교환사채', '제3자배정', '감자결정', '무상감자', '일반공모'])) cat = 'fund';
  else if (has(['배당결정', '자기주식', '주식소각', '자사주'])) cat = 'ret';
  else if (has(['합병', '분할', '영업양수', '영업양도', '타법인주식', '출자증권', '최대주주변경', '주식교환', '주식이전'])) cat = 'mna';
  else if (has(['시설투자', '유형자산취득', '유형자산처분'])) cat = 'invest';
  return { cat: cat, corr: corr };
}
function matDay(ymd) { const s = String(ymd); return Date.UTC(Number(s.slice(0, 4)), Number(s.slice(4, 6)) - 1, Number(s.slice(6, 8))); }
function matAge(d, asof) { return Math.round((matDay(asof) - matDay(d)) / 86400000); }
function matValid(d) { return /^[0-9]{8}$/.test(String(d)); }
// 공시 목록 → 사건 목록(최신순). 정정 공시(corr)는 건수·반응 계산에서 제외한다(원공시와 이중 집계 방지).
function matPrep(items) {
  const out = [];
  (items || []).forEach((it) => {
    if (!it || !matValid(it.d)) return;
    const c = matClass(it.nm);
    out.push({ d: String(it.d), nm: String(it.nm || ''), no: String(it.no || ''), cat: c.cat, corr: c.corr });
  });
  out.sort((a, b) => (a.d < b.d ? 1 : a.d > b.d ? -1 : 0));
  return out;
}
const matCounted = (e) => !e.corr && e.cat !== 'periodic';
// 최근 90일 vs 그 이전(91~365일)의 유형별 건수. 최근에만 있으면 new, 이전에만 있으면 gone.
function matNewGone(events, asof) {
  const rec = {}, pri = {};
  events.forEach((e) => {
    if (!matCounted(e)) return;
    const a = matAge(e.d, asof); if (a < 0 || a > 365) return;
    if (a <= 90) rec[e.cat] = (rec[e.cat] || 0) + 1; else pri[e.cat] = (pri[e.cat] || 0) + 1;
  });
  return MAT_CATS.map((c) => {
    const r = rec[c.k] || 0, p = pri[c.k] || 0;
    return { k: c.k, n: c.n, rec: r, pri: p, st: r > 0 && p === 0 ? 'new' : (r === 0 && p > 0 ? 'gone' : (r > 0 ? 'both' : 'none')) };
  });
}
// 최근 12개월(asof가 속한 달 포함) 월별·유형별 건수
function matMonthly(events, asof) {
  const y0 = Number(String(asof).slice(0, 4)), m0 = Number(String(asof).slice(4, 6));
  const months = [];
  for (let i = 11; i >= 0; i--) { let m = m0 - i, y = y0; while (m <= 0) { m += 12; y -= 1; } months.push(y + String(m < 10 ? '0' + m : m)); }
  const cnt = {}; MAT_CATS.forEach((c) => { cnt[c.k] = months.map(() => 0); });
  events.forEach((e) => { if (!matCounted(e)) return; const i = months.indexOf(e.d.slice(0, 6)); if (i >= 0) cnt[e.cat][i]++; });
  return { months: months, cnt: cnt };
}
function matLower(arr, v) { let lo = 0, hi = arr.length; while (lo < hi) { const m = (lo + hi) >> 1; if (arr[m] < v) lo = m + 1; else hi = m; } return lo; }
function matIdxMap(ix) { const m = {}; if (ix && ix.d) ix.d.forEach((d, i) => { if (ix.c[i] > 0) m[String(d)] = ix.c[i]; }); return m; }
function matHasJump(c, a, b) { for (let k = Math.max(1, a); k <= b && k < c.length; k++) if (c[k - 1] > 0 && Math.abs(c[k] / c[k - 1] - 1) > 0.3) return true; return false; }
// 공시일 반응: 공시일 직전 거래일 종가 → 공시일 이후 h번째 거래일 종가 수익률 − 같은 기간 코스피 수익률(시장조정, 베타 조정 아님).
// 공시 시각(장중/장 마감 후)을 모르므로 1일 값은 반응의 일부만 담을 수 있다. 액면분할 등이 의심되는 ±30% 변동이 구간에 있으면 계산하지 않는다.
function matReact(date, px, ixm) {
  if (!px || !px.d || !px.d.length) return null;
  const i = matLower(px.d, String(date));
  if (i <= 0 || i >= px.d.length) return null;
  const out = {};
  [1, 5, 20].forEach((h) => {
    const j = i + h - 1;
    if (j >= px.d.length || matHasJump(px.c, i, j) || !(px.c[i - 1] > 0)) { out['r' + h] = null; out['a' + h] = null; return; }
    const r = px.c[j] / px.c[i - 1] - 1;
    const m0 = ixm[String(px.d[i - 1])], m1 = ixm[String(px.d[j])];
    out['r' + h] = r; out['a' + h] = (m0 > 0 && m1 > 0) ? r - (m1 / m0 - 1) : null;
  });
  return out;
}
// 공시 없는 큰 움직임 찾기(최근 365일): 시장조정 1일 수익률 ±MAT_BIG_MOVE 이상인 날을 모으고, 그날(또는 전 거래일 이후~당일) 재료성 공시가 있었는지 표시.
function matBigMoves(px, ixm, events, asof, thr, news) {
  const T = thr || MAT_BIG_MOVE, res = { list: [], total: 0, withD: 0, noD: 0, skipped: 0, newsOk: !!news, noDnoN: 0, noDwithN: 0, outOfRange: 0 };
  const nFrom = news && news.length ? news.reduce((m, n) => (n.d < m ? n.d : m), '99999999') : null;
  if (!px || !px.d) return res;
  const ev = events.filter((e) => matCounted(e) && e.cat !== 'insider');
  for (let k = 1; k < px.d.length; k++) {
    if (matAge(px.d[k], asof) > 365) continue;
    if (!(px.c[k - 1] > 0) || !(px.c[k] > 0)) continue;
    const r = px.c[k] / px.c[k - 1] - 1;
    if (Math.abs(r) > 0.3) { res.skipped++; continue; }
    const m0 = ixm[String(px.d[k - 1])], m1 = ixm[String(px.d[k])];
    if (!(m0 > 0 && m1 > 0)) { res.skipped++; continue; }
    const ar = r - (m1 / m0 - 1);
    if (Math.abs(ar) < T) continue;
    const near = ev.filter((e) => e.d >= String(px.d[k - 1]) && e.d <= String(px.d[k]));
    const o = { d: String(px.d[k]), ar: ar, disc: near.map((e) => e.nm).slice(0, 2), has: near.length > 0, news: null, nn: null, nt: null };
    if (news) {
      if (nFrom && String(px.d[k - 1]) >= nFrom) { const nw = news.filter((n) => n.d >= String(px.d[k - 1]) && n.d <= String(px.d[k])); o.nn = nw.length; o.nt = nw.length ? nw[0].t : null; o.nl = nw.length ? nw[0].l : null; }
      else res.outOfRange++;
    }
    [5, 20].forEach((h) => {
      const j = k + h;
      if (j >= px.d.length || matHasJump(px.c, k + 1, j)) { o['f' + h] = null; return; }
      const f = px.c[j] / px.c[k] - 1, n0 = ixm[String(px.d[k])], n1 = ixm[String(px.d[j])];
      o['f' + h] = (n0 > 0 && n1 > 0) ? f - (n1 / n0 - 1) : null;
    });
    res.total++; if (o.has) res.withD++; else res.noD++;
    if (!o.has && o.nn != null) { if (o.nn === 0) res.noDnoN++; else res.noDwithN++; }
    res.list.push(o);
  }
  res.list.sort((a, b) => Math.abs(b.ar) - Math.abs(a.ar));
  res.list = res.list.slice(0, 15);
  return res;
}
// ----- 뉴스 분석(제목 기반, 규칙 기반이라 참고용) -----
const MAT_STOP = ['관련', '대해', '통해', '위해', '이번', '올해', '지난', '오는', '기자', '뉴스', '주가', '종목', '주식', '투자', '증권', '시장', '전망', '분석', '발표', '공시', '대비', '기록', '이상', '이하', '진행', '예정', '확대', '추진', '가운데', '나선', '한다', '했다', '된다', '있다', '없다', '따르면', '밝혔다', '전했다', '특징주', '마감', '장중', '코스피', '코스닥', '거래', '상승', '하락', '급등', '급락', '오늘', '내일', '속보'];
function matTokens(title, name) {
  const nm0 = String(name || ''), t0 = String(title || '');
  const clean = (nm0 ? t0.split(nm0).join(' ') : t0).replace(/[^0-9A-Za-z가-힣]+/g, ' ');
  const out = {};
  clean.split(' ').forEach((w0) => {
    let w = w0;
    ['으로', '에서', '에게', '까지', '부터'].forEach((x) => { if (w.length >= 4 && w.slice(-x.length) === x) w = w.slice(0, -x.length); });
    ['은', '는', '이', '가', '을', '를', '의', '에', '도', '로', '와', '과'].forEach((x) => { if (w.length >= 4 && w.slice(-1) === x) w = w.slice(0, -1); });
    if (w.length < 2 || /^[0-9]+$/.test(w) || MAT_STOP.indexOf(w) >= 0) return;
    out[w] = 1;
  });
  return Object.keys(out);
}
// 최근 90일에 처음 등장한 단어(이전 구간 0회, 최근 3회 이상) / 이전엔 자주 나왔으나 최근 사라진 단어(이전 5회 이상, 최근 0회). 이전 구간이 더 길어 ‘사라짐’ 기준을 더 엄격히 둠.
function matTerms(news, name, asof) {
  const rec = {}, pri = {};
  (news || []).forEach((n) => {
    const a = matAge(n.d, asof); if (a < 0 || a > 365) return;
    matTokens(n.t, name).forEach((w) => { if (a <= 90) rec[w] = (rec[w] || 0) + 1; else pri[w] = (pri[w] || 0) + 1; });
  });
  const top = (arr) => arr.sort((x, y) => y[1] - x[1] || (x[0] < y[0] ? -1 : 1)).slice(0, 8);
  return {
    fresh: top(Object.keys(rec).filter((w) => rec[w] >= 3 && !pri[w]).map((w) => [w, rec[w]])),
    gone: top(Object.keys(pri).filter((w) => pri[w] >= 5 && !rec[w]).map((w) => [w, pri[w]])),
  };
}
// 최근 90일 기사 중, 직전 10개 기사 가운데 제목 단어가 50% 이상 겹치는 것이 있는 기사의 비율(같은 이야기 반복의 대용치). Tetlock(2011)의 ‘오래된 뉴스’ 정의(텍스트 유사도)를 단순화한 것.
function matStaleShare(news, name, asof) {
  const arr = (news || []).slice().sort((a, b) => (a.d < b.d ? -1 : a.d > b.d ? 1 : 0)).map((n) => ({ d: n.d, w: matTokens(n.t, name) }));
  let tot = 0, st = 0;
  for (let i = 0; i < arr.length; i++) {
    const a = matAge(arr[i].d, asof); if (a < 0 || a > 90) continue;
    tot++;
    for (let j = Math.max(0, i - 10); j < i; j++) {
      const A = arr[i].w, B = arr[j].w; if (!A.length || !B.length) continue;
      let inter = 0; A.forEach((w) => { if (B.indexOf(w) >= 0) inter++; });
      if (inter / (A.length + B.length - inter) >= 0.5) { st++; break; }
    }
  }
  return { n: tot, stale: st, share: tot ? st / tot : null };
}
function matNewsMonthly(news, asof) {
  const mo = matMonthly([], asof).months, cnt = mo.map(() => 0);
  (news || []).forEach((n) => { const i = mo.indexOf(String(n.d).slice(0, 6)); if (i >= 0) cnt[i]++; });
  return { months: mo, cnt: cnt };
}
function matSafeUrl(u) { const x = String(u || ''); return /^https?:[/][/]/.test(x) ? x.split('"').join('%22').split("'").join('%27').split('<').join('%3C').split(' ').join('%20') : '#'; }
// ----- 화면 -----
const MAT_UI = { open: false }; // 재료 점검 카드를 열어 뒀는지(종목을 바꿔도 사용자의 선택을 유지)
const MAT_ST = { name: null, data: null, err: null, loading: false, news: null, newsErr: null, newsNoKey: false, newsLoading: false };
function matYmdDash(d) { const s = String(d); return s.slice(0, 4) + '-' + s.slice(4, 6) + '-' + s.slice(6, 8); }
function matSigned(v) { return v == null || !isFinite(v) ? '<span style="color:#94a3b8;">N/A</span>' : '<span style="color:' + (v >= 0 ? '#dc2626' : '#2563eb') + ';">' + (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%</span>'; }
function matNewsUrl(name, from, to) { return 'https://search.naver.com/search.naver?where=news&query=' + encodeURIComponent(name) + '&nso=so%3Ar%2Cp%3Afrom' + from + 'to' + to; }
function matCatName(k) { const c = MAT_CATS.filter((x) => x.k === k)[0]; return c ? c.n : k; }
function matSummary() {
  const D = MAT_ST.data; if (!D) return '';
  const ev = matPrep(D.items), ng = matNewGone(ev, D.to);
  const nw = ng.filter((x) => x.st === 'new').map((x) => x.n), gn = ng.filter((x) => x.st === 'gone').map((x) => x.n);
  return '최근 1년 재료성 공시 ' + ev.filter(matCounted).length + '건 · 최근 3개월에 새로 나타난 유형: ' + (nw.length ? nw.join(', ') : '없음') + ' · 이전에는 있었으나 최근 3개월 없는 유형: ' + (gn.length ? gn.join(', ') : '없음') + (D.partial ? ' (공시 일부만 수신)' : '') + (MAT_ST.news ? ' · 뉴스 ' + MAT_ST.news.got + '건 수집' + (MAT_ST.news.partial ? '(일부 기간만)' : '') : '');
}
// 뉴스 구간(⑤). 공시 조회가 실패해도 뉴스는 따로 보여줄 수 있도록 함수로 분리했다. name=종목명, to=기준일(YYYYMMDD)
function matNewsBlock(name, to) {
  let h = '';
  const th = 'style="white-space:nowrap;"';
  h += '<div style="font-weight:700; font-size:13px; margin-top:12px;">⑤ 뉴스 (네이버 검색 API · 제목 기준)</div>';
  const links = '<a href="' + matNewsUrl(name, addDays365(to), to) + '" target="_blank" rel="noopener">최근 1년 뉴스 검색(네이버)</a> · <a href="' + matNewsUrl(name, addDays90(to), to) + '" target="_blank" rel="noopener">최근 3개월만</a>';
  const N = MAT_ST.news;
  if (!N) {
    h += '<div style="font-size:12.5px; line-height:1.6; margin:4px 0;">' + (MAT_ST.newsNoKey ? '뉴스 API 키(NAVER_CLIENT_ID / NAVER_CLIENT_SECRET)가 서버에 설정되지 않아 뉴스를 분석하지 못했습니다. ' : '뉴스를 불러오지 못했습니다' + (MAT_ST.newsErr ? ': ' + anEsc(MAT_ST.newsErr) : '') + '. ') + links + '</div>';
  } else {
    const nv = N.items, tm = matTerms(nv, name, to), sh = matStaleShare(nv, name, to), nm = matNewsMonthly(nv, to);
    const oldest = nv.length ? nv[nv.length - 1].d : null;
    h += '<div style="font-size:12px; line-height:1.55; margin:4px 0;">수집 ' + N.got + '건(종목명 포함 기사만, 중복 링크 제거) · 가장 오래된 기사 ' + (oldest ? matYmdDash(oldest) : 'N/A') + (N.partial ? ' <b style="color:#b45309;">⚠ 네이버는 최신순 1,000건까지만 볼 수 있어 1년 전까지 못 닿았습니다. 아래 비교는 수집된 기간 안에서만 의미가 있습니다.</b>' : '') + (N.warn ? ' ⚠ ' + anEsc(N.warn) : '') + '</div>';
    h += '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">월별 기사 수</th>' + nm.months.map((m) => '<th ' + th + '>' + m.slice(2, 4) + '.' + m.slice(4) + '</th>').join('') + '</tr></thead><tbody><tr><td class="an-first">기사</td>' + nm.cnt.map((n) => '<td' + (n ? ' style="background:rgba(219,39,119,' + Math.min(0.5, 0.1 + n * 0.02) + ');font-weight:700;"' : ' style="color:#cbd5e1;"') + '>' + (n || '·') + '</td>').join('') + '</tr></tbody></table></div>';
    const chips = (arr, col) => arr.length ? arr.map((x) => '<span style="display:inline-block; margin:2px 4px 2px 0; padding:1px 8px; border-radius:10px; border:1px solid ' + col + '; color:' + col + ';">' + anEsc(x[0]) + ' ' + x[1] + '</span>').join('') : '<span style="color:var(--text-muted);">해당 없음</span>';
    h += '<div style="font-size:12.5px; line-height:1.7; margin-top:8px;"><b>최근 3개월에 새로 등장한 단어</b>(이전 9개월엔 0회, 최근 3회 이상): ' + chips(tm.fresh, '#16a34a') + '<br /><b>이전엔 자주 나왔으나 최근 사라진 단어</b>(이전 5회 이상, 최근 0회): ' + chips(tm.gone, '#d97706') + '<br /><b>같은 이야기의 반복도</b>: 최근 3개월 기사 ' + sh.n + '건 중 직전 10개 기사와 제목이 비슷한 기사 ' + (sh.share == null ? 'N/A' : sh.stale + '건(' + (sh.share * 100).toFixed(0) + '%)') + '</div>';
    h += '<div style="font-size:11px; color:var(--text-muted); line-height:1.5; margin-top:4px;">단어는 제목을 공백·조사 단위로 단순 분리한 것이라 조사·복합어가 섞이고, 같은 사건을 여러 언론이 쓰면 중복 집계됩니다(주제·감성 분석이 아님). 반복도는 Tetlock(2011)의 ‘오래된 뉴스’(이전 기사와 텍스트 유사)를 단순화한 대용치입니다. 단어의 신규/소멸은 ‘새 재료의 단서’일 뿐 호재·악재 판단이 아닙니다.</div>';
    h += '<div style="font-weight:600; font-size:12.5px; margin-top:8px;">최근 기사 (최신 12건)</div><ul style="margin:4px 0 0 18px; padding:0; font-size:12.5px; line-height:1.6;">' + nv.slice(0, 12).map((n) => '<li>' + matYmdDash(n.d) + ' <a href="' + matSafeUrl(n.l) + '" target="_blank" rel="noopener">' + anEsc(n.t) + '</a></li>').join('') + '</ul><div style="font-size:12.5px; margin-top:6px;">' + links + '</div>';
  }
  return h;
}

function matBodyHtml() {
  const nwTo = MAT_ST.news && MAT_ST.news.to ? MAT_ST.news.to : null;
  if (MAT_ST.loading) return '<div style="font-size:12.5px;">공시 목록을 불러오는 중…' + (MAT_ST.newsLoading ? ' (뉴스도 함께 불러오는 중)' : '') + '</div>' + (nwTo ? matNewsBlock(MAT_ST.name, nwTo) : '');
  if (MAT_ST.err) return '<div style="font-size:12.5px; color:var(--danger);">공시 목록을 불러오지 못했습니다: ' + anEsc(MAT_ST.err) + ' <button onclick="matRetry()" style="padding:2px 10px; font-size:12px;">다시 시도</button></div>' + (nwTo ? matNewsBlock(MAT_ST.name, nwTo) : (MAT_ST.newsErr ? '<div style="font-size:12.5px; color:var(--danger); margin-top:6px;">뉴스도 불러오지 못했습니다: ' + anEsc(MAT_ST.newsErr) + '</div>' : (MAT_ST.newsLoading ? '<div style="font-size:12.5px; margin-top:6px;">뉴스를 불러오는 중…</div>' : '')));
  const D = MAT_ST.data; if (!D) return '<div style="font-size:12.5px; color:var(--text-muted);">이 항목을 열면 최근 1년 DART 공시 목록을 불러옵니다.</div>';
  const px = sigPx(), ixm = matIdxMap(D.idx), ev = matPrep(D.items), ng = matNewGone(ev, D.to), mo = matMonthly(ev, D.to);
  const name = D.corp_name, th = 'style="white-space:nowrap;"';
  let h = '';
  if (D.partial) h += '<div style="font-size:12px; color:#b45309; margin:6px 0;">⚠ 공시 ' + D.total_count + '건 중 ' + D.got + '건만 받았습니다(오래된 쪽이 빠졌을 수 있음). ‘새로 생김/사라짐’ 판단이 부정확할 수 있습니다.</div>';
  if (!D.items.length) h += '<div style="font-size:12.5px; margin:6px 0;">최근 1년 공시가 조회되지 않았습니다.</div>';
  // 1) 새로 생김 / 사라짐
  const stTxt = { new: '<b style="color:#16a34a;">새로 나타남</b>', gone: '<b style="color:#d97706;">사라짐</b>', both: '계속 있음', none: '<span style="color:#94a3b8;">없음</span>' };
  h += '<div style="font-weight:700; font-size:13px; margin-top:8px;">① 최근 3개월 vs 그 이전 9개월 (재료성 공시 유형별)</div><div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">유형</th><th>이전 9개월</th><th>최근 3개월</th><th>변화</th></tr></thead><tbody>' +
    ng.map((x) => '<tr><td class="an-first">' + anEsc(x.n) + '</td><td>' + x.pri + '건</td><td>' + x.rec + '건</td><td>' + stTxt[x.st] + '</td></tr>').join('') + '</tbody></table></div>';
  // 2) 월별 매트릭스
  h += '<div style="font-weight:700; font-size:13px; margin-top:12px;">② 최근 12개월 월별 건수</div><div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">유형</th>' + mo.months.map((m) => '<th ' + th + '>' + m.slice(2, 4) + '.' + m.slice(4) + '</th>').join('') + '</tr></thead><tbody>' +
    MAT_CATS.map((c) => '<tr><td class="an-first">' + anEsc(c.n) + '</td>' + mo.cnt[c.k].map((n) => '<td' + (n ? ' style="background:rgba(8,145,178,' + Math.min(0.5, 0.12 + n * 0.1) + ');font-weight:700;"' : ' style="color:#cbd5e1;"') + '>' + (n || '·') + '</td>').join('') + '</tr>').join('') + '</tbody></table></div>';
  // 3) 공시 목록 + 반응
  const shown = ev.filter(matCounted).slice(0, 40);
  h += '<div style="font-weight:700; font-size:13px; margin-top:12px;">③ 재료성 공시와 주가 반응 (최신 ' + shown.length + '건, 정기보고서·정정공시 제외)</div>';
  if (!px) h += '<div style="font-size:12px; color:var(--text-muted);">일별 주가를 아직 못 불러와 반응(수익률)은 표시하지 않습니다.</div>';
  h += '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">공시일</th><th>유형</th><th style="text-align:left;">제목</th><th>1일</th><th>5일</th><th>20일</th></tr></thead><tbody>' +
    shown.map((e) => {
      const r = px ? matReact(e.d, px, ixm) : null, c = (k) => r ? (r['a' + k] != null ? matSigned(r['a' + k]) : (r['r' + k] != null ? '<span title="코스피 자료가 없어 시장조정 못함" style="opacity:.6;">' + matSigned(r['r' + k]) + '*</span>' : matSigned(null))) : matSigned(null);
      return '<tr><td class="an-first">' + matYmdDash(e.d) + '</td><td>' + anEsc(matCatName(e.cat)) + '</td><td style="text-align:left;"><a href="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=' + encodeURIComponent(e.no) + '" target="_blank" rel="noopener">' + anEsc(e.nm) + '</a></td><td>' + c(1) + '</td><td>' + c(5) + '</td><td>' + c(20) + '</td></tr>';
    }).join('') + '</tbody></table></div>' +
    '<div style="font-size:11px; color:var(--text-muted); line-height:1.5; margin-top:4px;">반응 = 공시일 직전 거래일 종가 대비 N거래일째 종가 수익률 − 같은 기간 코스피 수익률(시장조정, 위험조정 아님). 공시 시각을 몰라 장 마감 후 공시는 1일 값에 일부만 반영되고, 같은 날 여러 공시나 가까운 공시는 서로 겹칩니다. 종목당 표본이 작아 평균·순위를 만들지 않았고, 개별 값은 우연일 수 있습니다. * = 시장조정 불가(원수익률).</div>';
  // 4) 공시 없는 큰 움직임
  if (px) {
    const NW = MAT_ST.news ? MAT_ST.news.items : null, bm = matBigMoves(px, ixm, ev, D.to, null, NW);
    h += '<div style="font-weight:700; font-size:13px; margin-top:12px;">④ 최근 1년 큰 움직임(시장조정 ±' + (MAT_BIG_MOVE * 100) + '% 이상)과 공시·뉴스 유무</div><div style="font-size:12px; margin:4px 0; line-height:1.55;">큰 움직임 ' + bm.total + '일 중 재료성 공시가 겹친 날 <b>' + bm.withD + '</b>일 · 공시가 안 보이는 날 <b>' + bm.noD + '</b>일' + (bm.skipped ? ' · 계산 제외 ' + bm.skipped + '일(분할 의심·지수 자료 없음)' : '') + '.' +
      (bm.newsOk ? ' 공시 없는 날 중 뉴스도 없던 날 <b>' + bm.noDnoN + '</b>일 · 뉴스는 있던 날 <b>' + bm.noDwithN + '</b>일' + (bm.outOfRange ? ' · 뉴스 수집 구간 밖이라 확인 못 한 날 ' + bm.outOfRange + '일' : '') + '. 공시도 뉴스도 없이 크게 움직인 종목은 이후 되돌려지는 경향이 보고되었습니다(Chan 2003, 미국) — 이 종목에서도 그런지는 아래 ‘이후 5·20일’ 열로 직접 보세요(표본이 작아 우연일 수 있음).' : ' 공시가 없어도 뉴스·루머·수급이 원인일 수 있으니 ‘뉴스’ 링크로 직접 확인하세요.') + '</div>';
    if (bm.list.length) h += '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">날짜</th><th>시장조정 1일</th><th>이후 5일</th><th>이후 20일</th><th style="text-align:left;">같은 시기 공시</th><th style="text-align:left;">뉴스</th></tr></thead><tbody>' +
      bm.list.map((o) => '<tr><td class="an-first">' + matYmdDash(o.d) + '</td><td>' + matSigned(o.ar) + '</td><td>' + matSigned(o.f5) + '</td><td>' + matSigned(o.f20) + '</td><td style="text-align:left;">' + (o.has ? anEsc(o.disc.join(' / ')) : '<b style="color:#d97706;">공시 없음</b>') + '</td><td style="text-align:left;">' +
        (o.nn == null ? '' : (o.nn === 0 ? '<b style="color:#d97706;">뉴스 없음</b> ' : o.nn + '건: <a href="' + matSafeUrl(o.nl) + '" target="_blank" rel="noopener">' + anEsc(o.nt.length > 40 ? o.nt.slice(0, 40) + '…' : o.nt) + '</a> ')) +
        '<a href="' + matNewsUrl(name, o.d, o.d) + '" target="_blank" rel="noopener">검색</a></td></tr>').join('') + '</tbody></table></div>';
  }
  // 5) 뉴스
  h += matNewsBlock(name, D.to);
  h += '<div style="font-size:12.5px; line-height:1.6; margin:6px 0;">직접 비교해 보세요: ① 최근 3개월에 처음 등장한 이야기(수주·정책·신사업)는 공시로 뒷받침되는가? ② 1년 전엔 자주 나왔지만 지금은 사라진 이야기는 무엇인가? ③ 같은 내용이 되풀이되는 기사인가?</div>';
  h += '<div style="font-size:11px; color:var(--text-muted); line-height:1.5; margin-top:6px;">공시 분류는 제목 글자 규칙이라 오분류가 있을 수 있고, 같은 사건의 정정·후속 공시는 정정만 제외됩니다. ‘1년’은 사용자가 정한 관찰 창이며 학술적으로 증명된 최적 기간이 아닙니다(아래 근거 참고).</div>';
  return h;
}
function addDays365(ymd) { const t = new Date(matDay(ymd) - 365 * 86400000); return t.getUTCFullYear() + String(t.getUTCMonth() + 1 < 10 ? '0' : '') + (t.getUTCMonth() + 1) + String(t.getUTCDate() < 10 ? '0' : '') + t.getUTCDate(); }
function addDays90(ymd) { const t = new Date(matDay(ymd) - 90 * 86400000); return t.getUTCFullYear() + String(t.getUTCMonth() + 1 < 10 ? '0' : '') + (t.getUTCMonth() + 1) + String(t.getUTCDate() < 10 ? '0' : '') + t.getUTCDate(); }
function matSectionHtml() {
  const ev = '<b>근거 요약</b> — ' +
    '(1) 식별된 기업 뉴스가 있는 날의 변동성은 뉴스 없는 날의 2배 넘게 큼(Boudoukh·Feldman·Kogan·Richardson, NBER 2013 및 JF 2019, S&P500 2000–09, 미국). ' +
    '(2) 뉴스 있는 종목의 나쁜 소식은 이후에도 한 방향으로 흘러가는 경향(drift), 뉴스 없이 크게 움직인 종목은 되돌림(reversal) 경향, 특히 소형·비유동 종목에서 강함(Chan 2003, JFE, 미국). ' +
    '(3) 오래된(반복) 뉴스에는 개인투자자가 과잉 반응해 다음 주 되돌림(Tetlock 2011, RFS, 미국). ' +
    '(4) 언론 노출이 없는 종목의 기대수익이 더 높은 ‘무보도 프리미엄’(Fang-Peress 2009, JF, 미국). ' +
    '(5) 언론의 비관적 어조는 단기 가격 하락 압력 후 되돌림(Tetlock 2007, JF, 미국). ' +
    '<b>한계</b>: 모두 미국 시장·뉴스 텍스트 연구이며 한국 개별종목에 그대로 성립하는지는 이 앱에서 검증되지 않았습니다. ‘최근 1년’이 최적 관찰 기간이라는 연구는 확인하지 못했습니다(효과 자체는 대체로 수일~수개월). 공시 유형별 평균 초과수익을 한국에서 확인하려면 별도 이벤트 스터디가 필요합니다.';
  return '<details class="card" style="border-left:4px solid #db2777;" ontoggle="matOnToggle(this)"><summary style="cursor:pointer; font-weight:700; color:#db2777; font-size:14px;">📰 재료 점검 · 최근 1년 공시 (없다가 생김 · 있다가 사라짐)</summary>' +
    '<div style="font-size:11.5px; color:var(--text-muted); margin:6px 0; line-height:1.5;">재료(뉴스·공시)가 새로 생겼는지, 사라졌는지, 주가가 이유 없이 움직였는지 확인하는 도구입니다. 점수·매수 신호는 만들지 않습니다. ' + ev + '</div><div id="matBody">' + matBodyHtml() + '</div></details>';
}
async function matLoad() {
  const name = (document.getElementById('corpName').value || '').trim(); if (!name) return;
  if (MAT_ST.name === name && (MAT_ST.data || MAT_ST.loading)) { matRefresh(); return; }
  MAT_ST.name = name; MAT_ST.data = null; MAT_ST.err = null; MAT_ST.loading = true; MAT_ST.news = null; MAT_ST.newsErr = null; MAT_ST.newsNoKey = false; MAT_ST.newsLoading = false; matRefresh();
  const getJ = async (url) => { const res = await fetch(url); let j = null; try { j = await res.json(); } catch (e) { j = null; } return { ok: res.ok, status: res.status, j: j }; };
  const enc = encodeURIComponent(name);
  const bad = (e) => ({ ok: false, j: { error: (e && e.message) || String(e) } });
  MAT_ST.newsLoading = true;
  // 공시와 뉴스는 서로 기다리지 않는다(한쪽이 느리거나 실패해도 다른 쪽을 먼저 보여준다)
  const pD = getJ('/api/disclosures?corp_name=' + enc).catch(bad).then((rd) => {
    if (MAT_ST.name !== name) return;
    if (rd.ok) MAT_ST.data = rd.j; else MAT_ST.err = (rd.j && rd.j.error) || '조회 실패';
    MAT_ST.loading = false; matRefresh();
  });
  const pN = getJ('/api/news?corp_name=' + enc).catch(bad).then((rn) => {
    if (MAT_ST.name !== name) return;
    if (rn.ok) MAT_ST.news = rn.j; else { MAT_ST.newsErr = (rn.j && rn.j.error) || '조회 실패'; MAT_ST.newsNoKey = !!(rn.j && rn.j.nokey); }
    MAT_ST.newsLoading = false; matRefresh();
  });
  await Promise.all([pD, pN]);
  if (MAT_ST.name !== name) return;
  try { sigRefresh(); } catch (e) { /* 체크리스트 갱신 실패는 무시 */ }
}
function matRetry() { MAT_ST.name = null; matLoad(); }
function matOnToggle(el) { if (el) MAT_UI.open = !!el.open; if (el && el.open) { try { matLoad(); if (typeof loadPriceSeries === 'function' && !sigPx()) loadPriceSeries(); } catch (e) { /* 무시 */ } } }
// 재료 점검 카드는 주가 화면(차트 독) 바로 아래 칸(#matSlot)에 그린다. 다시 그려도 열어 둔 상태는 유지한다.
function matSlotRender() {
  const slot = document.getElementById('matSlot'); if (!slot) return;
  slot.innerHTML = matSectionHtml();
  const d = slot.querySelector('details'); if (d && MAT_UI.open) d.open = true; // 열면 ontoggle이 공시·뉴스를 불러온다
  matRefresh();
}
function matRefresh() { const el = document.getElementById('matBody'); if (el) el.innerHTML = matBodyHtml(); }
function matAutoText() {
  const nm = (document.getElementById('corpName').value || '').trim();
  if (!MAT_ST.data || MAT_ST.name !== nm) return '‘📰 재료 점검’ 항목을 열면 최근 1년 공시로 자동 요약됩니다.';
  return matSummary();
}
// ===== /재료(공시) 점검 =====


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
// 스토리 흐름의 장(章) 머리글. 투자 대가들의 분석 순서를 종합한 것(하나의 정해진 표준 순서가 있는 것은 아님)
function anChapter(no, title, ask, lens) {
  return '<div class="an-chap" data-short="' + no + ' ' + title + '" style="margin:16px 0 6px; padding:8px 12px; border-radius:8px; background:var(--card,#fff); border:1px solid var(--border); border-left:5px solid #0f172a;">' +
    '<div style="display:flex; align-items:center; justify-content:space-between; gap:8px; cursor:pointer;" onclick="anChapToggle(this.parentNode)" title="누르면 이 장의 항목을 모두 펼치거나 접습니다"><div style="font-weight:800; font-size:14.5px;">' + no + ' ' + title + '</div><span class="an-chap-st" style="font-size:11.5px; color:var(--primary); white-space:nowrap;">▸ 항목 펼치기</span></div>' +
    '<div style="font-size:12.5px; margin-top:2px;">' + ask + '</div>' +
    '<details style="margin-top:2px;"><summary style="cursor:pointer; font-size:11.5px; color:var(--text-muted);">이 질문의 근거</summary><div style="font-size:11.5px; color:var(--text-muted); margin-top:2px; line-height:1.5;">' + lens + '</div></details></div>';
}
// ===== 금융업 · 지주회사 =====
// 일반기업용 틀(ROIC·FCF·매출총이익·유동비율·F-Score)이 맞지 않는 업종을 따로 다룬다.
//  - Damodaran, Valuing Financial Service Firms(가치평가 3판 21장): 금융회사에서는 부채(예금·차입)가 영업의 원재료라 자본(equity) 기준으로 평가하고, FCFF·EV 배수는 쓰기 어렵다.
//  - Fama-French(1992): 금융회사의 높은 레버리지는 비금융회사에서처럼 부실 신호가 아니라서 표본에서 제외했다(원문 확인).
const FIN_SEC = {
  bank: { n: '은행', fin: true }, finhold: { n: '금융지주', fin: true }, insurer: { n: '보험', fin: true }, broker: { n: '증권', fin: true },
  credit: { n: '여신금융(카드·캐피탈)', fin: true }, finsvc: { n: '기타 금융서비스', fin: true }, invest: { n: '투자·집합투자·창투', fin: true },
  spac: { n: '기업인수목적회사(스팩)', fin: true }, holding: { n: '지주회사(비금융)', fin: false },
};
const FIN_HOLD_NAMES = ['금융지주', '금융그룹', '신한지주', 'KB금융', '하나금융', '우리금융', 'BNK금융', 'JB금융', 'DGB금융', 'iM금융', '한국금융지주', '메리츠금융', '한국투자금융'];
const FIN_CREDIT_NAMES = ['카드', '캐피탈', '파이낸셜', '할부', '리스', '여신'];
const FIN_VIEW = { force: {}, lastKey: null }; // force[corp_code] = 'fin'(금융업 화면) | 'gen'(일반기업 화면)
const FIN_TH = { g: 0.02, roeSteps: [-0.04, -0.02, 0, 0.02, 0.04], keSteps: [-0.01, 0, 0.01, 0.02], gList: [0, 0.01, 0.02, 0.03, 0.04], minRoeYears: 3 };

function finHas(name, arr) { const s = String(name || ''); return arr.some(function (a) { return s.indexOf(a) >= 0; }); }
function finEnds(name, arr) { const s = String(name || '').trim(); return arr.some(function (a) { return s.length >= a.length && s.slice(s.length - a.length) === a; }); }
function finMk(k, basis) { return { k: k, n: FIN_SEC[k].n, fin: FIN_SEC[k].fin, basis: basis }; }
// 업종코드(KSIC; DART 기업개황 induty_code)와 회사 이름으로 유형을 정한다. 코드가 있으면 코드 우선(64=금융업, 65=보험·연금업, 66=금융·보험 관련 서비스업, 64992=지주회사).
// 코드가 없거나 세부를 가르는 데는 이름을 쓰며, 이름으로 추정한 경우 basis에 '이름 추정'이라고 남긴다(틀릴 수 있음).
function finSectorOf(code, name) {
  const c = String(code || '').replace(/[^0-9]/g, ''), nm = String(name || '');
  if (nm.indexOf('스팩') >= 0 || nm.indexOf('기업인수목적') >= 0) return finMk('spac', '이름 추정');
  let k = null;
  if (c) {
    const p3 = c.slice(0, 3), p2 = c.slice(0, 2);
    if (c.indexOf('64992') === 0) k = finHas(nm, FIN_HOLD_NAMES) ? 'finhold' : 'holding';
    else if (p3 === '641') k = 'bank';
    else if (p2 === '65') k = 'insurer';
    else if (p3 === '642') k = 'invest';
    else if (p2 === '64') k = finHas(nm, FIN_CREDIT_NAMES) ? 'credit' : 'invest';
    else if (p2 === '66') k = nm.indexOf('증권') >= 0 ? 'broker' : 'finsvc';
    if (k) return finMk(k, 'KSIC ' + c);
  }
  if (finHas(nm, FIN_HOLD_NAMES)) k = 'finhold';
  else if (nm.indexOf('은행') >= 0 || nm.indexOf('뱅크') >= 0) k = 'bank';
  else if (nm.indexOf('증권') >= 0) k = 'broker';
  else if (finEnds(nm, ['생명', '화재', '해상', '보험', '재보험'])) k = 'insurer';
  else if (finEnds(nm, ['카드', '캐피탈'])) k = 'credit';
  else if (finHas(nm, ['인베스트먼트', '벤처투자', '창업투자', '투자파트너스'])) k = 'invest';
  return k ? finMk(k, '이름 추정') : null;
}
// 지금 화면의 종목: 자동 분류 + 사용자가 고른 화면(금융업/일반)을 반영한 결과
function finSectorNow() {
  const key = (typeof rawRows !== 'undefined' && rawRows.length) ? rawRows[0].corp_code : '';
  const code = AN_STOCK.code, ind = (code && IND.map[code]) || AN_STOCK.ind;
  const auto = finSectorOf(ind, AN_STOCK.name);
  const f = FIN_VIEW.force[key] || null;
  let sec = auto;
  if (f === 'fin') sec = (auto && auto.fin) ? auto : { k: 'finsvc', n: '금융업(직접 선택)', fin: true, basis: '직접 선택' };
  else if (f === 'gen' && auto && auto.fin) sec = null;
  return { sec: sec, auto: auto, forced: f, key: key };
}
function finSecChanged() { const s = finSectorNow(); const k = (s.sec ? s.sec.k : '') + '|' + s.key; const ch = FIN_VIEW.lastKey !== null && FIN_VIEW.lastKey !== k; return ch; }
function finViewFin() { const key = finSectorNow().key; FIN_VIEW.force[key] = 'fin'; renderAnalysis(); }
function finViewGen() { const key = finSectorNow().key; FIN_VIEW.force[key] = 'gen'; renderAnalysis(); }
function finViewAuto() { const key = finSectorNow().key; delete FIN_VIEW.force[key]; renderAnalysis(); }

// ---------- 계산 (순수 함수: 화면과 분리해 단독 검증 가능) ----------
// 정당 PBR = (ROE − g) / (ke − g).  P0 = D1/(ke−g), D1 = E1(1−g/ROE) = B0(ROE−g)  (고든 성장 + 지속가능 배당성향). 표본·시장에 맞춘 값이 아니라 이론식이다.
function finJustPbr(roe, g, ke) { if (!anFin(roe) || !anFin(g) || !anFin(ke) || !(ke > g)) return null; return Math.max(0, (roe - g) / (ke - g)); }
// 시장 PBR이 정당화되려면 필요한 ROE (위 식을 ROE에 대해 푼 것)
function finImpliedRoe(pbr, g, ke) { if (!anFin(pbr) || !(pbr >= 0) || !anFin(g) || !anFin(ke) || !(ke > g)) return null; return g + pbr * (ke - g); }
function finDdm(dps, g, ke) { if (!anFin(dps) || !(dps > 0) || !anFin(g) || !anFin(ke) || !(ke > g)) return null; return dps * (1 + g) / (ke - g); }
function finRowsOf(list) {
  return list.map(function (d) {
    const r = d.r || {};
    const bps = (anFin(d.equity) && anFin(d.shares) && d.shares > 0) ? d.equity / d.shares : null;
    return { year: d.year, ni: d.ni, eq: d.equity, assets: d.assets, roe: d.roe, roa: anDiv(d.ni, d.avgAssets), lev: anDiv(d.assets, d.equity),
      bps: bps, eps: d.eps, dps: anFin(r.dividend_per_share) ? r.dividend_per_share : null, payout: d.payout, fs: r.fs_div || null };
  });
}
function finBuild(c) {
  const rows = finRowsOf(c.list), ke = c.r, m = c.m || {}, price = c.price;
  const roes = rows.map(function (x) { return x.roe; }).filter(anFin);
  const o = { rows: rows, ke: ke, nRoe: roes.length, g: FIN_TH.g, warn: [] };
  o.roe3 = anMean(roes.slice(-3)); o.roe5 = anMean(roes.slice(-5)); o.roeMed5 = anMedian(roes.slice(-5)); o.roeSd = anStd(roes.slice(-10));
  const l10 = rows.slice(-10);
  o.lossYears = l10.filter(function (x) { return anFin(x.ni) && x.ni < 0; }).length;
  o.nNi10 = l10.filter(function (x) { return anFin(x.ni); }).length;
  o.belowKe = l10.filter(function (x) { return anFin(x.roe) && x.roe < ke; }).length;
  o.nRoe10 = l10.filter(function (x) { return anFin(x.roe); }).length;
  o.bpsG3 = anCagr(rows, 'bps', 3); o.bpsG5 = anCagr(rows, 'bps', 5);
  o.epsG3 = anCagr(rows, 'eps', 3); o.niG3 = anCagr(rows, 'ni', 3);
  o.payoutAvg = c.payoutAvg;
  o.retG = anFin(o.roe3) ? o.roe3 * (1 - anClamp(anFin(c.payoutAvg) ? c.payoutAvg : 0, 0, 1)) : null; // 유보로 설명되는 장부가치 성장률(Higgins 1977과 같은 구조)
  const withDps = rows.filter(function (x) { return anFin(x.dps); });
  o.dps = withDps.length ? withDps[withDps.length - 1].dps : null; o.dpsYear = withDps.length ? withDps[withDps.length - 1].year : null;
  o.divYield = (anFin(price) && price > 0 && anFin(o.dps) && o.dps > 0) ? o.dps / price : null;
  o.pbr = anFin(m.pbr) ? m.pbr : null; o.per = anFin(m.per) ? m.per : null; o.bps = anFin(m.bps) ? m.bps : null;
  o.roeBase = c.def ? c.def.base0 : o.roeMed5;
  o.jPbr = finJustPbr(o.roeBase, o.g, ke);
  o.impRoe = finImpliedRoe(o.pbr, o.g, ke);
  o.jPrice = (anFin(o.jPbr) && anFin(o.bps)) ? o.jPbr * o.bps : null;
  o.ddm = finDdm(o.dps, o.g, ke);
  o.scenJ = c.def ? { bear: finJustPbr(c.def.bear.roe0, o.g, ke), base: finJustPbr(c.def.base.roe0, o.g, ke), bull: finJustPbr(c.def.bull.roe0, o.g, ke) } : null;
  o.grid = null;
  if (anFin(o.roeBase)) {
    const kes = FIN_TH.keSteps.map(function (s) { return ke + s; });
    o.grid = { kes: kes, rows: FIN_TH.roeSteps.map(function (s) { const roe = o.roeBase + s; return { roe: roe, cells: kes.map(function (k) { return finJustPbr(roe, o.g, k); }) }; }) };
  }
  // 읽는 법: 점수가 아니라 ROE(수익성)와 PBR(가격)의 관계를 4가지 경우로 설명한다
  let read = null;
  if (anFin(o.roeBase) && anFin(o.pbr) && anFin(o.jPbr)) {
    const hi = o.roeBase >= ke, rel = o.pbr / (o.jPbr > 0 ? o.jPbr : NaN);
    if (!hi) read = 'below';
    else if (anFin(rel) && rel < 0.9) read = 'cheap';
    else if (anFin(rel) && rel > 1.1) read = 'rich';
    else read = 'fair';
  }
  o.read = read;
  const fsSet = {}; rows.forEach(function (x) { if (x.fs) fsSet[x.fs] = 1; });
  if (Object.keys(fsSet).length > 1) o.warn.push('연결(CFS)과 별도(OFS) 재무제표가 섞여 있습니다. 연도별 ROE 비교가 어긋날 수 있습니다.');
  if (o.nRoe < FIN_TH.minRoeYears) o.warn.push('ROE를 계산할 수 있는 연도가 ' + o.nRoe + '개뿐입니다(3개 미만이면 정당 PBR·시나리오를 만들지 않습니다).');
  if (!(anFin(price) && price > 0)) o.warn.push('현재가가 없어 PBR·PER·배당수익률은 N/A입니다.');
  if (!rows.some(function (x) { return anFin(x.dps); })) o.warn.push('배당 자료(주당배당금)가 없어 배당성향은 0%(전액 유보)로 가정됩니다. 정당 PBR의 성장률 g 계산에는 쓰이지 않고(g는 고정 입력), 잔여이익 시나리오에만 영향을 줍니다.');
  return o;
}
// 지주회사 순자산가치(NAV) = Σ(상장 자회사 시가총액 × 지분율) + 기타 자산 − 순차입금. 단위: 억원
function finNav(items, other, debt, mcap, shares) {
  let sum = 0, used = 0, miss = [];
  (items || []).forEach(function (x) {
    const s = Number(x.stake), cap = Number(x.cap);
    if (!(s > 0) || !(s <= 100)) { miss.push((x.name || x.code || '?') + '(지분율)'); return; }
    if (!(cap > 0)) { miss.push((x.name || x.code || '?') + '(시총)'); return; }
    sum += s / 100 * cap; used++;
  });
  const o = anFin(Number(other)) ? Number(other) : 0, d = anFin(Number(debt)) ? Number(debt) : 0;
  const nav = sum + o - d;
  const mc = anFin(mcap) && mcap > 0 ? mcap / 1e8 : null; // 원 → 억
  const out = { sumListed: sum, used: used, miss: miss, other: o, debt: d, nav: nav, mcapEok: mc, navPerShare: null, discount: null };
  if (anFin(shares) && shares > 0) out.navPerShare = nav * 1e8 / shares;
  if (mc != null && nav > 0) out.discount = 1 - mc / nav; // +면 NAV보다 싸게 거래(할인), −면 할증
  return out;
}

// ---------- 업종별 설명(근거 수준과 함께) ----------
const FIN_NOTES = {
  bank: { why: '은행은 예금·차입이 영업의 원재료입니다. 그래서 "차입금을 빼는" EV·FCF·ROIC가 정의되지 않고, 자본(equity)을 기준으로 ROE와 PBR로 평가하는 것이 표준입니다.',
    miss: [['순이자마진(NIM)', '예대마진 수익성'], ['고정이하여신비율·대손비용률', '자산 건전성과 이익의 질'], ['보통주자본비율(CET1)·BIS 비율', '배당·성장 여력(자본규제)']], where: '각사 사업보고서·분기보고서 본문, 금융감독원 금융통계정보시스템(FISIS)' },
  finhold: { why: '금융지주는 연결재무제표가 은행·보험·증권 자회사를 합친 것이라 은행과 같은 방식(ROE·PBR)으로 봅니다. 별도 재무제표만 보면 자회사 배당이 수익의 대부분이라 사업 규모를 알 수 없습니다.',
    miss: [['자회사별 이익 비중', '어느 사업이 이익을 내는지'], ['보통주자본비율(CET1)·BIS 비율', '주주환원 여력'], ['자회사 건전성 지표', '은행 NPL, 보험 지급여력 등']], where: '각사 사업보고서 본문과 IR 자료' },
  insurer: { why: '보험회사는 보험료(부채)를 받아 운용하는 사업이라 은행처럼 자본 기준으로 평가합니다. IFRS 17 이후 이익이 보험계약마진(CSM) 상각·투자손익·금리 효과에 함께 좌우되어 연도별 ROE가 흔들릴 수 있습니다.',
    miss: [['지급여력비율(K-ICS)', '자본규제 여력'], ['보험계약마진(CSM) 잔액·상각', '앞으로 인식할 보험이익의 쌓임'], ['보험손익 vs 투자손익 분해', '이익의 출처와 지속성']], where: '각사 공시, 생명보험협회·손해보험협회 공시' },
  broker: { why: '증권회사는 위탁·IB·자기매매 수익이 증시 사이클에 크게 연동됩니다. 자본 기준(ROE·PBR)으로 보되, 호황과 불황의 ROE 차이가 커서 한두 해 ROE로 판단하지 않습니다.',
    miss: [['순자본비율(NCR)·레버리지', '자본규제 여력'], ['수익원 구성(위탁·IB·자기매매·이자)', '이익의 지속성']], where: '각사 사업보고서, 금융투자협회 전자공시' },
  credit: { why: '카드·캐피탈은 차입으로 조달해 대출해 주는 사업이라 은행과 같이 자본 기준으로 봅니다. 조달금리와 연체율에 민감합니다.',
    miss: [['연체율·고정이하여신비율', '자산 건전성'], ['조정자기자본비율(레버리지 한도)', '자본규제 여력'], ['조달금리·조달 구조', '이익 변동 요인']], where: '각사 사업보고서, 여신금융협회·금융감독원 통계' },
  finsvc: { why: '금융 관련 서비스업(증권 중개, 금융시장 운영, 투자자문 등)은 회사마다 구조가 달라 일반 기업용 지표를 그대로 쓰면 왜곡됩니다. 자본 기준 지표를 기본으로 보고, 구조를 직접 확인하세요.', miss: [['사업 구조(수수료·이자·자기자본투자 비중)', '이익의 성격']], where: '사업보고서 본문' },
  invest: { why: '투자·창투회사는 보유 자산을 공정가치로 평가해 이익에 반영하므로 이익 변동이 큽니다. 이런 회사는 ROE보다 PBR(주가가 순자산가치의 몇 배인가)이 핵심이고, 폐쇄형 펀드가 순자산가치보다 싸게 거래되는 현상(Lee·Shleifer·Thaler 1991)이 참고가 됩니다. 다만 그 논문은 미국 폐쇄형 펀드 이야기라 이 회사들에 그대로 적용된다는 증거는 아닙니다.',
    miss: [['보유 투자자산 구성·평가손익 비중', '순자산의 질'], ['투자조합 운용 규모·성과보수', '수수료 수익의 지속성']], where: '사업보고서 본문(투자 현황 주석)' },
  spac: { why: '기업인수목적회사(스팩)는 합병 전까지 사업이 없고 자산이 신탁금입니다. 이 앱의 평가 틀로 의미 있는 판단을 할 수 없어 수치를 참고용으로만 표시합니다.', miss: [['신탁금 규모·합병 기한', '실제로 중요한 정보']], where: '증권신고서·공시' },
};

function finSafe(label, fn) { try { return fn(); } catch (e) { return '<div class="card" style="color:var(--danger); font-size:12.5px;">' + anEsc(label) + ' 표시 오류: ' + anEsc(e && e.message ? e.message : e) + '</div>'; } }
function finGuideLink(id) { return ' <a href="#" onclick="guideOpen(this.getAttribute(\\'data-g\\')); return false;" data-g="' + id + '" style="font-size:11.5px;">ⓘ 쉬운 설명</a>'; }

function finRenderGlance(S, c, o) {
  const sec = S.sec, lv = function (v) { return v == null ? 'N/A' : v; };
  const chip = function (name, v, sub) { return '<div style="flex:1 1 120px; border:1px solid var(--border); border-radius:10px; padding:8px 10px; background:#fff;"><div style="font-size:11.5px; color:var(--text-muted);">' + name + '</div><div style="font-size:16px; font-weight:800;">' + anEsc(lv(v)) + '</div>' + (sub ? '<div style="font-size:10.5px; color:var(--text-muted);">' + sub + '</div>' : '') + '</div>'; };
  const r = c.last && c.last.r ? c.last.r : null;
  const fd = r && r.fs_div ? (r.fs_div === 'CFS' ? '연결' : r.fs_div === 'OFS' ? '개별(별도)' : r.fs_div) : '기준 미확인';
  const filing = r && r.filing_date ? btFmtDate(r.filing_date) : '공시일 미저장';
  const jump = [['①', '수익성'], ['②', '주주환원'], ['③', '자본·레버리지'], ['④', '성장'], ['⑤', '가격'], ['⑥', '시장']].map(function (j) {
    return '<button type="button" style="font-size:12.5px; padding:4px 10px; min-height:36px;" onclick="anGoChap(this.getAttribute(\\'data-n\\'))" data-n="' + j[0] + '">' + j[0] + ' ' + j[1] + '</button>'; }).join('');
  const toggle = S.forced === 'fin' ? '<button type="button" onclick="finViewAuto()" style="font-size:12px; padding:3px 8px;">자동 분류로 되돌리기</button>' :
    '<button type="button" onclick="finViewGen()" style="font-size:12px; padding:3px 8px;" title="분류가 틀렸다고 생각하면 일반 기업용 지표 화면으로 봅니다">일반 기업용 화면으로 보기</button>';
  return '<div class="card" id="anGlance" style="border-left:4px solid #7c3aed;"><div style="font-weight:800; font-size:14.5px;">한눈에 보기 <span style="font-size:11.5px; font-weight:400; color:var(--text-muted);">— 금융업은 점수 대신 숫자를 그대로 보여 줍니다</span></div>' +
    '<div style="margin:6px 0; font-size:12.5px;"><span style="display:inline-block; background:#7c3aed; color:#fff; border-radius:10px; padding:1px 9px; font-weight:700;">' + anEsc(sec.n) + '</span> <span style="color:var(--text-muted);">분류 근거: ' + anEsc(sec.basis || '') + (String(sec.basis || '').indexOf('이름') >= 0 ? ' — 업종코드가 없어 이름으로 추정했습니다. 틀리면 아래 버튼으로 바꿀 수 있습니다' : '') + '</span> ' + toggle + '</div>' +
    '<div style="display:flex; flex-wrap:wrap; gap:8px; margin:8px 0;">' +
    chip('ROE (최근 3년 평균)', anFin(o.roe3) ? anPct(o.roe3) : null, '요구수익률 ke ' + anPct(o.ke)) + chip('PBR', anFin(o.pbr) ? anX(o.pbr) : null, '주가 ÷ 주당순자산') +
    chip('정당 PBR (기준값)', anFin(o.jPbr) ? anX(o.jPbr) : null, '(ROE−g)/(ke−g), g ' + anPct(o.g)) + chip('Momentum 주가 흐름', c.grades.mom, null) + '</div>' +
    '<div style="font-size:12px; color:var(--text-muted); line-height:1.55;">기준: 최근 연간 ' + (c.last ? c.last.year : 'N/A') + '년 · ' + fd + '재무제표 · 공시일 ' + filing + ' · 연간 ' + c.list.length + '개년. ROIC·FCF·F-Score·매출총이익·유동비율은 이 업종에서 의미가 없어 계산하지 않습니다' + finGuideLink('finwhy') + '</div>' +
    '<div style="margin-top:8px; font-size:12px; font-weight:600;">질문별로 바로가기</div><div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:4px;">' + jump + '</div></div>';
}

function finRenderWhy(S, c, o) {
  const nt = FIN_NOTES[S.sec.k] || FIN_NOTES.finsvc;
  const miss = nt.miss.map(function (x) { return '<tr><td class="an-first">' + anEsc(x[0]) + '</td><td style="text-align:left;">' + anEsc(x[1]) + '</td><td>미수집</td></tr>'; }).join('');
  const warn = o.warn.length ? '<div style="margin:6px 0; padding:8px 10px; background:#fffbeb; border:1px solid #fcd34d; border-radius:8px; font-size:12.5px;">' + o.warn.map(function (w) { return '⚠ ' + anEsc(w); }).join('<br>') + '</div>' : '';
  const html = '<div style="font-size:12.5px; line-height:1.6;">' + anEsc(nt.why) + '</div>' + warn +
    '<div style="font-size:12.5px; font-weight:700; margin-top:10px;">이 업종에서 중요하지만 이 앱이 수집하지 않는 지표 ' + finGuideLink('finmiss') + '</div>' +
    '<div class="tscroll"><table class="an-tbl"><thead><tr><th class="an-first">지표</th><th style="text-align:left;">무엇을 알려주나</th><th>상태</th></tr></thead><tbody>' + miss + '</tbody></table></div>' +
    '<div style="font-size:12px; color:var(--text-muted); margin-top:4px;">확인 위치: ' + anEsc(nt.where) + '. 재무제표 항목(DART 재무제표 API)에는 이런 규제·건전성 비율이 들어 있지 않아 값을 만들어 채우지 않았습니다.</div>';
  return anSection('🏛 ' + S.sec.n + ' — 어떻게 평가하나, 무엇이 비어 있나', '#7c3aed', '', html, true);
}

function finRenderProfit(o) {
  const defs = [{ label: '지배주주 순이익', key: 'ni', fmt: 'eok' }, { label: 'ROE (평균자본)', key: 'roe', fmt: 'pct' }, { label: 'ROA (순이익/평균자산)', key: 'roa', fmt: 'pct' }, { label: '자기자본승수 (자산/자본)', key: 'lev', fmt: 'x' }];
  const tiles = anGrid([
    anTile('ROE 3Y / 5Y 평균', anPct(o.roe3) + ' / ' + anPct(o.roe5), '지배주주순이익 ÷ 평균 지배주주지분'),
    anTile('ROE 변동(표준편차)', anFin(o.roeSd) ? anPct(o.roeSd) : 'N/A', '최근 10년 이내, 클수록 이익이 불안정'),
    anTile('ROE가 ke 미만인 해', o.nRoe10 ? o.belowKe + ' / ' + o.nRoe10 : 'N/A', 'ke ' + anPct(o.ke) + ' 기준, 최근 10년 이내'),
    anTile('적자 연도', o.nNi10 ? o.lossYears + ' / ' + o.nNi10 : 'N/A', '최근 10년 이내'),
  ]);
  const note = '<div style="font-size:12px; color:var(--text-muted); line-height:1.55;">ROE = ROA × 자기자본승수(듀폰 분해). 금융회사는 승수가 10배 이상으로 높은 것이 정상이며 부실 신호가 아닙니다(Fama-French 1992도 이 이유로 금융회사를 제외) ' + anBadge('proof') + '. ROE는 레버리지(자본규제가 상한)와 ROA 중 어느 쪽이 만든 것인지 같이 봅니다. ROA는 지배주주 순이익을 총자산으로 나눈 근사치입니다.</div>';
  return anSection('📈 수익성 — ROE와 그 분해', '#7c3aed', '', tiles + anTable(o.rows, defs, 10) + note, true);
}
function finRenderReturn(o) {
  const defs = [{ label: 'EPS (원)', key: 'eps', fmt: 'won' }, { label: '주당배당금 DPS (원)', key: 'dps', fmt: 'won' }, { label: '배당성향', key: 'payout', fmt: 'pct' }];
  const hiPay = o.rows.filter(function (x) { return anFin(x.payout) && x.payout > 1; }).map(function (x) { return x.year; });
  const tiles = anGrid([
    anTile('최근 배당금 (' + (o.dpsYear || 'N/A') + ')', anFin(o.dps) ? anWon(o.dps) : 'N/A', '사업보고서 기준 주당배당금'),
    anTile('배당수익률', anFin(o.divYield) ? anPct(o.divYield) : 'N/A', '최근 DPS ÷ 현재가'),
    anTile('배당성향 평균', anFin(o.payoutAvg) ? anPct(o.payoutAvg, 0) : 'N/A', '자료 있는 연도 평균'),
  ]);
  const note = '<div style="font-size:12px; color:var(--text-muted); line-height:1.55;">금융회사는 이익을 다 배당하면 자본이 늘지 않아 대출·보험 인수를 못 늘립니다(자본규제 때문). 배당성향이 높으면 성장은 그만큼 느려집니다(지속가능 성장률 = ROE × 유보율). ' + (hiPay.length ? '<b>배당성향 100% 초과 연도: ' + hiPay.join(', ') + '</b> — 이익보다 많이 지급해 자본이 줄었을 수 있습니다.' : '') + '</div>';
  return anSection('💵 주주환원 — 이익이 배당으로 이어지는가', '#7c3aed', '', tiles + anTable(o.rows, defs, 10) + note, false);
}
function finRenderCapital(S, o) {
  const defs = [{ label: '지배주주지분 (자본)', key: 'eq', fmt: 'eok' }, { label: '총자산', key: 'assets', fmt: 'eok' }, { label: '자기자본승수', key: 'lev', fmt: 'x' }, { label: 'BPS (주당순자산, 원)', key: 'bps', fmt: 'won' }];
  const levs = o.rows.map(function (x) { return x.lev; }).filter(anFin);
  const trend = levs.length >= 4 ? (levs[levs.length - 1] > levs[0] ? '상승' : '하락') : null;
  const note = '<div style="font-size:12px; color:var(--text-muted); line-height:1.55;">자기자본승수가 ' + (trend ? '처음 대비 <b>' + trend + '</b>했습니다. ' : '') + '금융회사의 건전성은 재무상태표 숫자가 아니라 <b>규제 자본비율</b>(은행 CET1·BIS, 보험 K-ICS, 증권 NCR 등)로 판단합니다. 그 비율은 재무제표 API에 없어 이 앱은 계산하지 않았습니다. 위의 「미수집 지표」 표에서 확인할 곳을 안내합니다.</div>';
  return anSection('🛡 자본과 레버리지', '#7c3aed', '', anTable(o.rows, defs, 10) + note, false);
}
function finRenderGrowth(o) {
  const tiles = anGrid([
    anTile('BPS 3Y / 5Y CAGR', anPct(o.bpsG3) + ' / ' + anPct(o.bpsG5), '주당순자산 연평균 증가율(시작·끝이 양수일 때만)'),
    anTile('EPS 3Y CAGR', anFin(o.epsG3) ? anPct(o.epsG3) : (anFin(o.niG3) ? anPct(o.niG3) + ' (순이익)' : 'N/A'), '주식수 변동이 크면 순이익으로 대체'),
    anTile('유보로 설명되는 성장', anFin(o.retG) ? anPct(o.retG) : 'N/A', 'ROE 3Y평균 × (1−배당성향)'),
  ]);
  let cmp = '';
  if (anFin(o.bpsG3) && anFin(o.retG)) {
    const d = o.bpsG3 - o.retG;
    cmp = '<div style="font-size:12.5px; margin-top:6px;">실제 BPS 증가율(' + anPct(o.bpsG3) + ')과 유보 성장(' + anPct(o.retG) + ')의 차이 <b>' + (d >= 0 ? '+' : '') + anPct(d) + 'p</b>. ' +
      (Math.abs(d) < 0.02 ? '이익을 쌓아서 자본이 늘었다는 설명과 대체로 맞습니다.' : '차이가 크면 증자·자사주·유가증권 평가손익(기타포괄손익) 등이 자본을 움직였다는 뜻이라, 자본이 늘었더라도 이익의 질과 따로 봐야 합니다.') + '</div>';
  }
  const note = '<div style="font-size:12px; color:var(--text-muted); line-height:1.55; margin-top:6px;">금융회사의 "매출"은 업종마다 정의가 달라(은행 이자수익, 보험 보험수익, 증권 영업수익) 회사끼리 비교할 수 없어 매출 성장률을 쓰지 않습니다. 대신 주주 몫인 BPS와 EPS의 성장을 봅니다.</div>';
  return anSection('🌱 성장 — 주주 몫(BPS·EPS)이 늘고 있는가', '#7c3aed', '', tiles + cmp + note, false);
}
function finGridHtml(o) {
  if (!o.grid) return '';
  const cur = o.pbr;
  const head = '<tr><th class="an-first">ROE ↓ · ke →</th>' + o.grid.kes.map(function (k) { return '<th>' + anPct(k) + '</th>'; }).join('') + '</tr>';
  const body = o.grid.rows.map(function (r) {
    return '<tr><td class="an-first">' + anPct(r.roe) + (Math.abs(r.roe - o.roeBase) < 1e-9 ? ' (기준)' : '') + '</td>' + r.cells.map(function (v, i) {
      const base = Math.abs(r.roe - o.roeBase) < 1e-9 && Math.abs(o.grid.kes[i] - o.ke) < 1e-9;
      return '<td' + (base ? ' style="background:#ede9fe; font-weight:700;"' : '') + '>' + (v == null ? 'N/A' : v.toFixed(2) + '배') + '</td>'; }).join('') + '</tr>'; }).join('');
  return '<div style="font-size:12.5px; font-weight:700; margin-top:10px;">정당 PBR 민감도 (g ' + anPct(o.g) + ' 고정)' + (anFin(cur) ? ' — 현재 PBR ' + anX(cur) : '') + '</div><div class="tscroll"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
}
function finReadText(o) {
  const t = {
    below: 'ROE(' + anPct(o.roeBase) + ')가 요구수익률 ke(' + anPct(o.ke) + ')보다 낮습니다. 이론상 이런 회사의 정당 PBR은 1배 미만이 정상이므로, 낮은 PBR 자체는 저평가의 증거가 아닙니다(자본비용을 못 넘기는 자본은 장부가 이하로 평가됨, Damodaran). ROE가 개선될 근거가 있는지가 관건입니다.',
    cheap: 'ROE가 ke 이상인데 시장 PBR(' + anX(o.pbr) + ')이 정당 PBR(' + anX(o.jPbr) + ')보다 10% 넘게 낮습니다. 가능한 설명은 두 가지입니다 — ① 시장이 앞으로 ROE가 내려갈 것(충당금·금리·규제·이익의 일회성)이라고 본다 ② 단순히 저평가다. 이 계산만으로는 둘을 가를 수 없으므로 이익의 질과 배당·자사주 지속성을 확인해야 합니다.',
    rich: 'ROE가 ke 이상이고 시장 PBR(' + anX(o.pbr) + ')이 정당 PBR(' + anX(o.jPbr) + ')보다 10% 넘게 높습니다. 시장이 ROE 개선이나 더 높은 성장(g)을 이미 가격에 담았다는 뜻입니다. 그 기대가 근거 있는지 확인이 필요합니다.',
    fair: 'ROE가 ke 이상이고 시장 PBR(' + anX(o.pbr) + ')이 정당 PBR(' + anX(o.jPbr) + ')과 ±10% 안에 있습니다. 현재 가격은 과거 ROE를 지속한다는 가정과 대체로 맞습니다.',
  };
  return o.read ? t[o.read] : '';
}
function finRenderValue(S, c, o) {
  const tiles = anGrid([
    anTile('PBR', anFin(o.pbr) ? anX(o.pbr) : 'N/A', '주가 ÷ 주당순자산(BPS ' + anWon(o.bps) + ')'),
    anTile('PER', anFin(o.per) ? anX(o.per, 1) : 'N/A', '순이익이 양수일 때만'),
    anTile('정당 PBR (기준)', anFin(o.jPbr) ? anX(o.jPbr) : 'N/A', 'ROE ' + anPct(o.roeBase) + ' · ke ' + anPct(o.ke) + ' · g ' + anPct(o.g)),
    anTile('시장이 요구하는 ROE', anFin(o.impRoe) ? anPct(o.impRoe) : 'N/A', '현재 PBR이 정당화되려면 필요한 ROE = g + PBR×(ke−g). 실제 ROE ' + anPct(o.roeBase) + '와 비교'),
    anTile('정당 PBR 환산 주가', anFin(o.jPrice) ? anWon(o.jPrice) : 'N/A', anFin(o.jPrice) && anFin(c.price) && c.price > 0 ? '현재가 대비 ' + anPct(o.jPrice / c.price - 1) : '현재가 필요'),
    anTile('배당할인(고든) 가치', anFin(o.ddm) ? anWon(o.ddm) : 'N/A', anFin(o.dps) ? '최근 DPS ' + anWon(o.dps) + ' × (1+g) ÷ (ke−g). 배당이 이익보다 적으면 낮게 나옴' : '배당 자료 없음'),
  ]);
  const sj = o.scenJ ? '<div style="font-size:12.5px; margin:6px 0;">시나리오별 정당 PBR — Bear <b>' + (o.scenJ.bear == null ? 'N/A' : o.scenJ.bear.toFixed(2) + '배') + '</b> · Base <b>' + (o.scenJ.base == null ? 'N/A' : o.scenJ.base.toFixed(2) + '배') + '</b> · Bull <b>' + (o.scenJ.bull == null ? 'N/A' : o.scenJ.bull.toFixed(2) + '배') + '</b> (ROE만 시나리오값으로 바꾸고 ke·g는 같음)</div>' : '';
  const read = finReadText(o) ? '<div style="margin:8px 0; padding:8px 10px; background:#f5f3ff; border:1px solid #ddd6fe; border-radius:8px; font-size:12.5px; line-height:1.6;"><b>읽는 법</b> — ' + finReadText(o) + ' (매수·매도 의견이 아닙니다)</div>' : '<div style="font-size:12.5px; color:var(--text-muted);">ROE나 PBR을 계산할 수 없어 해석 문장을 만들지 않았습니다.</div>';
  const ev = '<details style="margin-top:8px;"><summary style="cursor:pointer; font-size:12px; color:var(--text-muted);">근거와 한계</summary><div style="font-size:12px; line-height:1.6; color:var(--text-muted);">' +
    '정당 PBR = (ROE−g)/(ke−g)는 고든 성장모형에 "배당성향 = 1 − g/ROE"를 넣어 정리한 식입니다 ' + anBadge('theory') + ' Damodaran(가치평가 3판 21장)이 금융회사에 쓰는 배당할인·초과수익 모형과 같은 틀이며, 이 식 자체는 교재에 그대로 적혀 있지 않고 이 앱이 유도한 것입니다. ' +
    '입력(ROE 지속성, ke, g)은 모두 가정입니다. g는 장기 명목성장률보다 높게 둘 수 없어 2%를 기본으로 하고 0~4%를 민감도로 봅니다. ke는 아래 시나리오 카드의 요구수익률 r과 같은 값입니다. ' +
    '자본규제상 자본을 마음대로 늘릴 수 없고, 보험·증권은 ROE 변동이 커서 한두 해 값에 흔들립니다. 정당 PBR과 시장 PBR의 차이가 초과수익으로 이어진다는 검증은 이 앱에서 하지 않았습니다.</div></details>';
  const html = tiles + sj + read + finGridHtml(o) + ev;
  return anSection('💹 가격 — PBR과 ROE의 관계 (정당 PBR · 시장이 요구하는 ROE)', '#7c3aed', '', html, true);
}
function finRenderEvidence() {
  const rows = [
    ['금융회사는 자본(equity) 기준으로 평가, EV·FCFF 배수는 부적합', 'Damodaran, Valuing Financial Service Firms (가치평가 3판 21장, pp.581-610)', '원문 확인', '교재. 한국 시장에 대한 실증은 아님'],
    ['금융회사의 높은 레버리지는 부실 신호가 아니다(표본에서 제외)', 'Fama & French (1992), The Cross-Section of Expected Stock Returns, Section I.A', '원문 문장 확인', '제외 이유 설명이지 금융업 평가법은 아님'],
    ['정당 PBR = (ROE−g)/(ke−g)', '고든 성장모형 + 지속가능 배당성향(Damodaran 21장의 구성요소). 식은 이 앱이 유도', '이론', 'ROE 지속·g·ke가 모두 가정'],
    ['잔여이익모형(RIM): 가치 = 장부가 + 초과이익의 현재가치', 'Ohlson (1995) 등. 아래 시나리오 카드에서 사용', '제목·연도만 알고 이번 세션에 원문은 열지 않음', '가정에 민감'],
    ['RIM 기반 가치/가격이 이후 주식수익률을 예측', 'Frankel & Lee (1998), Journal of Accounting and Economics 25(3):283-319', '서지 확인(요지는 검색 초록 수준)', '미국 표본. 한국 금융업에서 이 앱이 검증하지 않음'],
    ['폐쇄형 펀드는 순자산가치 대비 할인·할증 거래되며 개인 심리와 함께 움직임', 'Lee, Shleifer & Thaler (1991), Journal of Finance 46(1):75-109', '서지·초록 확인', '투자·창투회사에 대한 직접 증거는 아님(유추)'],
    ['한국 상장사(비금융, KOSPI 1995-2010)에서 다각화 할인이 관찰됨', 'DOI 10.22510/KJOFM.2019.36.4.002 (초록만 확인, 저자·학술지명은 확인하지 못함)', '초록 수준', '지주회사 할인을 직접 검증한 연구가 아님'],
    ['지주회사 NAV(자회사 시가 합계) 대비 할인율로 평가', '증권업계 실무 관행', '실무 관행', '학술적으로 이 할인율이 수익을 예측한다는 증명은 없음'],
  ];
  const body = rows.map(function (r) { return '<tr><td class="an-first" style="white-space:normal; min-width:150px;">' + anEsc(r[0]) + '</td><td style="text-align:left; white-space:normal; min-width:170px;">' + anEsc(r[1]) + '</td><td style="white-space:normal;">' + anEsc(r[2]) + '</td><td style="text-align:left; white-space:normal; min-width:130px;">' + anEsc(r[3]) + '</td></tr>'; }).join('');
  return anSection('📚 이 화면의 근거와 확인 수준', '#475569', '이번에 직접 확인한 것과 확인하지 못한 것을 나눠 적었습니다.', '<div class="tscroll"><table class="an-tbl"><thead><tr><th class="an-first">주장</th><th>출처</th><th>확인 수준</th><th>한계</th></tr></thead><tbody>' + body + '</tbody></table></div>', false);
}

// 금융업 전용 분석 화면 전체 (일반 기업용 화면 대신 사용)
function finRenderAnalysis(S, c, m, notice) {
  const o = finBuild(c); c.fin = o;
  const mom = finSafe('모멘텀', function () { return anRenderMomentum() + anRenderTrend(c); });
  return notice + finSafe('한눈에 보기', function () { return finRenderGlance(S, c, o); }) +
    finSafe('안내', function () { return finRenderWhy(S, c, o); }) +
    anChapter('①', '자본을 잘 굴리는가?', '자본 1원이 1년에 얼마를 버는가(ROE), 그리고 그것이 마진(ROA)과 레버리지 중 어디서 오는가', 'Damodaran: 금융회사는 자본 기준 수익성(ROE)이 가치의 핵심. 레버리지는 규제 한도 안에서 사업의 일부.') +
    finSafe('수익성', function () { return finRenderProfit(o); }) +
    anChapter('②', '이익이 주주에게 돌아오는가?', '번 돈이 배당으로 나가는지, 얼마나 꾸준한지', '지속가능 성장률 = ROE × 유보율. 배당이 많으면 자본 성장이 느려집니다.') +
    finSafe('주주환원', function () { return finRenderReturn(o); }) +
    anChapter('③', '자본은 충분한가?', '레버리지 추이. 규제 자본비율은 이 앱이 수집하지 않아 확인할 곳을 안내합니다', '금융회사의 안전성은 규제 자본비율로 판단합니다. 재무상태표의 부채비율로 보지 않습니다.') +
    finSafe('자본', function () { return finRenderCapital(S, o); }) +
    anChapter('④', '주주 몫이 늘고 있는가?', '매출이 아니라 주당순자산(BPS)과 EPS의 성장', '금융회사 매출은 정의가 업종마다 달라 회사 간 비교가 안 됩니다.') +
    finSafe('성장', function () { return finRenderGrowth(o); }) +
    anChapter('⑤', '지금 가격은 합당한가?', 'PBR을 ROE로 설명해 보고(정당 PBR), 잔여이익모형(RIM)으로 Bear/Base/Bull 가치 범위를 봅니다', '자본 기준 평가(Damodaran 21장). 한 숫자가 아니라 가정에 따른 범위로 봅니다.') +
    finSafe('가격', function () { return finRenderValue(S, c, o); }) + finSafe('시나리오', function () { return anRenderScenario(c); }) +
    anChapter('⑥', '시장은 지금 이 주식을 어떻게 보고 있는가?', '가격 추세와 상대강도', '모멘텀은 가치와 별개의 독립된 정보입니다(Jegadeesh-Titman 1993).') + mom +
    anChapter('⑦', '판단 기록', '내 판단 과정을 기록해 둡니다', '결론을 대신 내리지 않고 판단 과정을 기록하게 돕습니다.') + '<div id="anThesisBox" class="card">' + anThesisHtml() + '</div>' +
    anChapter('참고', '근거 · 설정', '이 화면의 근거 수준과 요구수익률 등 설정', '확인한 것과 확인하지 못한 것을 구분해 적었습니다.') + finRenderEvidence() + anRenderSettings();
}

// ---------- 지주회사(비금융): 순자산가치(NAV) 계산기 ----------
// NAV = Σ(상장 자회사 시가총액 × 지분율) + 기타 자산 − 순차입금. 지분율은 사업보고서의 「타법인 출자 현황」에서 직접 옮겨 적어야 한다(이 앱은 자동 수집하지 않음).
const FIN_NAV = { key: '', rows: [], other: 0, debt: 0, msg: '' };
function finNavLoad(key) {
  if (FIN_NAV.key === key) return;
  FIN_NAV.key = key; FIN_NAV.rows = []; FIN_NAV.other = 0; FIN_NAV.debt = 0; FIN_NAV.msg = '';
  try { const s = localStorage.getItem('finNav_' + key); if (s) { const j = JSON.parse(s); if (j && j.rows) { FIN_NAV.rows = j.rows; FIN_NAV.other = j.other || 0; FIN_NAV.debt = j.debt || 0; } } } catch (e) { /* 저장 불가 환경이면 메모리만 */ }
  if (!FIN_NAV.rows.length) FIN_NAV.rows = [{ name: '', code: '', stake: '', cap: '', auto: false }];
}
function finNavSave() { try { localStorage.setItem('finNav_' + FIN_NAV.key, JSON.stringify({ rows: FIN_NAV.rows, other: FIN_NAV.other, debt: FIN_NAV.debt })); } catch (e) { /* 무시 */ } }
function finNavRead() {
  const g = function (id) { const e = document.getElementById(id); return e ? e.value : ''; };
  FIN_NAV.rows = FIN_NAV.rows.map(function (x, i) {
    const cap = g('finNavCap' + i), prev = x.cap;
    return { name: g('finNavNm' + i), code: g('finNavCd' + i).replace(/[^0-9A-Za-z]/g, '').slice(0, 6), stake: g('finNavSt' + i), cap: cap, auto: x.auto && String(cap) === String(prev) };
  });
  FIN_NAV.other = Number(g('finNavOther')) || 0; FIN_NAV.debt = Number(g('finNavDebt')) || 0;
}
function finNavInput() { finNavRead(); finNavSave(); finNavOut(); }
function finNavAdd() { finNavRead(); FIN_NAV.rows.push({ name: '', code: '', stake: '', cap: '', auto: false }); finNavSave(); finNavRedraw(); }
function finNavDel(btn) { finNavRead(); const i = Number(btn.getAttribute('data-i')); FIN_NAV.rows.splice(i, 1); if (!FIN_NAV.rows.length) FIN_NAV.rows.push({ name: '', code: '', stake: '', cap: '', auto: false }); finNavSave(); finNavRedraw(); }
function finNavRedraw() { const e = document.getElementById('finNavBox'); if (e) e.innerHTML = finNavInner(); finNavOut(); }
async function finNavFetch() {
  finNavRead();
  const codes = FIN_NAV.rows.map(function (x) { return x.code; }).filter(function (x) { return x.length === 6; });
  if (!codes.length) { FIN_NAV.msg = '종목코드(6자리)를 입력한 줄이 없습니다.'; finNavRedraw(); return; }
  FIN_NAV.msg = '시가총액을 조회하는 중...'; finNavRedraw();
  try {
    const res = await fetch('/api/market/caps?codes=' + encodeURIComponent(codes.join(','))); const d = await res.json();
    if (!res.ok) throw new Error(d.error || '조회 실패');
    let got = 0, none = [];
    FIN_NAV.rows.forEach(function (x) {
      if (x.code.length !== 6) return;
      const v = d.caps && d.caps[x.code];
      if (v && v.cap > 0) { x.cap = String(Math.round(v.cap / 1e8)); x.auto = true; got++; } else none.push(x.code);
    });
    FIN_NAV.msg = '시가총액 기준일 ' + (d.date ? fmtYmd(d.date) : '알 수 없음') + ' · ' + got + '개 채움' + (none.length ? ' · 시장 DB에 없는 종목(직접 입력 필요): ' + none.join(', ') : '') + '. 시장 DB는 시가총액 500억 원 이상 종목만 저장하는 경우가 많습니다.';
    finNavSave();
  } catch (e) { FIN_NAV.msg = '시가총액 조회 실패: ' + (e.message || e); }
  finNavRedraw();
}
function finNavOut() {
  const el = document.getElementById('finNavOut'); if (!el) return;
  const c = AN_CTX, m = lastSummaryMetrics;
  if (!c || !m) { el.innerHTML = ''; return; }
  const r = finNav(FIN_NAV.rows, FIN_NAV.other, FIN_NAV.debt, m.marketCap, c.last ? c.last.shares : null);
  const hasInput = FIN_NAV.rows.some(function (x) { return Number(x.stake) > 0 || Number(x.cap) > 0; });
  if (!hasInput) { el.innerHTML = '<div style="font-size:12.5px; color:var(--text-muted);">자회사 지분율과 시가총액을 입력하면 NAV와 할인율이 계산됩니다.</div>'; return; }
  const tiles = anGrid([
    anTile('상장 자회사 지분가치 합', Math.round(r.sumListed).toLocaleString() + '억', r.used + '개 반영'),
    anTile('NAV (순자산가치)', Math.round(r.nav).toLocaleString() + '억', '지분가치 + 기타자산 ' + Math.round(r.other).toLocaleString() + '억 − 순차입금 ' + Math.round(r.debt).toLocaleString() + '억'),
    anTile('지주회사 시가총액', r.mcapEok == null ? 'N/A' : Math.round(r.mcapEok).toLocaleString() + '억', '현재가 × 주식수'),
    anTile('주당 NAV', r.navPerShare == null ? 'N/A' : anWon(r.navPerShare), '자기주식 제외 주식수 기준'),
    anTile('NAV 대비 할인율', r.discount == null ? 'N/A' : anPct(r.discount), r.discount == null ? 'NAV가 양수여야 계산' : (r.discount >= 0 ? '시가총액이 NAV보다 낮음(할인)' : '시가총액이 NAV보다 높음(할증)')),
  ]);
  const miss = r.miss.length ? '<div style="font-size:12px; color:#b45309; margin-top:4px;">⚠ 계산에서 빠진 줄: ' + r.miss.map(anEsc).join(', ') + ' — 지분율이나 시가총액이 비어 있습니다. 빠진 만큼 NAV가 작게 나옵니다.</div>' : '';
  el.innerHTML = tiles + miss;
}
function finNavInner() {
  const row = function (x, i) {
    const inp = function (id, v, ph, w, extra) { return '<input id="' + id + '" value="' + escHtml(String(v == null ? '' : v)) + '" placeholder="' + ph + '" oninput="finNavInput()" style="width:' + w + '; padding:4px 6px; font-size:13px;" ' + (extra || '') + '>'; };
    return '<div style="display:flex; flex-wrap:wrap; gap:6px; align-items:center; margin:4px 0;">' + inp('finNavNm' + i, x.name, '자회사 이름', '120px') + inp('finNavCd' + i, x.code, '종목코드', '78px', 'inputmode="numeric"') +
      inp('finNavSt' + i, x.stake, '지분율 %', '72px', 'inputmode="decimal"') + inp('finNavCap' + i, x.cap, '시총 억원', '92px', 'inputmode="decimal"') +
      (x.auto ? '<span style="font-size:11px; color:var(--text-muted);">자동</span>' : '') + '<button type="button" data-i="' + i + '" onclick="finNavDel(this)" style="font-size:12px; padding:2px 8px;">삭제</button></div>';
  };
  return FIN_NAV.rows.map(row).join('') +
    '<div class="row" style="margin:6px 0;"><button type="button" onclick="finNavAdd()">+ 자회사 추가</button> <button type="button" onclick="finNavFetch()" title="종목코드가 있는 줄의 시가총액을 이 앱의 시장 DB에서 채웁니다">시가총액 자동 채우기</button></div>' +
    (FIN_NAV.msg ? '<div style="font-size:12px; color:var(--text-muted);">' + anEsc(FIN_NAV.msg) + '</div>' : '') +
    '<div style="display:flex; flex-wrap:wrap; gap:12px; margin:8px 0;"><label style="font-size:12px;">기타 자산 가치(억원)<br><input id="finNavOther" value="' + (FIN_NAV.other || '') + '" placeholder="비상장 지분·현금 등" oninput="finNavInput()" inputmode="decimal" style="width:130px; padding:4px 6px;"></label>' +
    '<label style="font-size:12px;">순차입금(억원)<br><input id="finNavDebt" value="' + (FIN_NAV.debt || '') + '" placeholder="지주회사 자체 차입−현금" oninput="finNavInput()" inputmode="decimal" style="width:130px; padding:4px 6px;"></label></div>';
}
function finRenderHolding(S, c, m) {
  const key = (typeof rawRows !== 'undefined' && rawRows.length) ? rawRows[0].corp_code : '';
  finNavLoad(key);
  const r = c.last && c.last.r ? c.last.r : null;
  const ofs = r && r.fs_div === 'OFS';
  const fsWarn = ofs ? '<div style="margin:6px 0; padding:8px 10px; background:#fffbeb; border:1px solid #fcd34d; border-radius:8px; font-size:12.5px;">⚠ 이 종목은 연결재무제표가 없어 <b>개별(별도) 재무제표</b>가 저장되어 있습니다. 지주회사의 별도 재무제표는 매출이 배당금수익·지분법·임대료 정도라 자회사 사업의 크기가 보이지 않습니다. 매출·이익률 같은 일반 지표 해석에 주의하세요.</div>' : '';
  const html = '<div style="font-size:12.5px; line-height:1.6;">지주회사는 가치의 대부분이 자회사 지분입니다. 연결재무제표로 보면 자회사 사업이 합쳐져 일반 지표(ROIC·FCF)도 해석할 수 있지만, 시장 가격은 자회사 지분 가치의 합(NAV)보다 낮게 형성되는 경우가 많아 <b>NAV 대비 할인율</b>을 함께 봅니다 ' + anBadge('practice') + finGuideLink('holdnav') + '</div>' + fsWarn +
    '<div id="finNavBox" style="margin-top:8px;">' + finNavInner() + '</div><div id="finNavOut"></div>' +
    '<details style="margin-top:8px;"><summary style="cursor:pointer; font-size:12px; color:var(--text-muted);">근거와 한계</summary><div style="font-size:12px; line-height:1.6; color:var(--text-muted);">' +
    'NAV 접근은 증권업계 실무 관행입니다. 할인율이 크다고 수익이 난다는 학술 증명은 이 앱이 확인하지 못했습니다. 한국 비금융 상장사(KOSPI, 1995-2010)에서 다각화 할인이 관찰되고 최대주주 지분이 낮을수록 할인이 크다는 연구(DOI 10.22510/KJOFM.2019.36.4.002, 초록 수준 확인)가 있으나 지주회사 할인 자체를 검증한 것은 아니며 2010년 이후 표본은 포함하지 않습니다. ' +
    '지분율은 사업보고서의 「타법인 출자 현황」에서 직접 옮겨 적어야 하고(자동 수집하지 않음), 상장 자회사는 시가총액 × 지분율, 비상장 자회사와 기타 자산은 장부가나 직접 추정한 값을 「기타 자산」에 넣으세요. 시가총액은 입력일 기준이라 시점이 다르면 할인율이 달라집니다. 자회사 간 상호출자, 우선주, 자회사의 순차입금은 반영하지 않았습니다. 입력값은 이 브라우저에만 저장됩니다.</div></details>';
  return anSection('🧩 지주회사 — 순자산가치(NAV) 대비 할인율', '#0f766e', '', html, true);
}

// ===== /금융업 · 지주회사 =====
// ===== 업종 (DART 기업개황의 업종코드 = 한국표준산업분류 KSIC) =====
// 코드는 DART 값을 그대로 저장하고, 화면에는 앞 2자리(중분류)로 묶어 이름을 붙인다. 이름표는 KSIC 중분류를 요약한 것이며 거래소·네이버의 업종 분류와는 다르다.
// 표에 없는 코드는 "KSIC 코드"로 그대로 보여 준다(틀린 이름을 붙이지 않는다).
const IND_NAMES = { '01': '농업', '02': '임업', '03': '어업', '05': '석탄·원유·가스 광업', '06': '금속 광업', '07': '비금속광물 광업', '08': '광업 지원', '10': '식료품', '11': '음료', '12': '담배', '13': '섬유제품', '14': '의복·모피', '15': '가죽·가방·신발', '16': '목재·나무제품', '17': '펄프·종이', '18': '인쇄·기록매체', '19': '코크스·석유정제', '20': '화학물질·화학제품', '21': '의약품', '22': '고무·플라스틱', '23': '비금속 광물제품', '24': '1차 금속', '25': '금속가공제품', '26': '전자부품·컴퓨터·통신장비', '27': '의료·정밀·광학기기', '28': '전기장비', '29': '기타 기계·장비', '30': '자동차·트레일러', '31': '기타 운송장비', '32': '가구', '33': '기타 제품', '34': '산업용 기계 수리', '35': '전기·가스·증기 공급', '36': '수도', '37': '하수·폐수 처리', '38': '폐기물·원료재생', '39': '환경 정화', '41': '종합 건설', '42': '전문직별 공사', '45': '자동차 판매·수리', '46': '도매·상품중개', '47': '소매', '49': '육상·파이프라인 운송', '50': '수상 운송', '51': '항공 운송', '52': '창고·운송 서비스', '55': '숙박', '56': '음식점·주점', '58': '출판', '59': '영상·오디오 제작·배급', '60': '방송', '61': '통신', '62': '컴퓨터 프로그래밍·SI', '63': '정보서비스', '64': '금융', '65': '보험·연금', '66': '금융·보험 관련 서비스', '68': '부동산', '70': '연구개발', '71': '전문 서비스', '72': '건축기술·엔지니어링', '73': '기타 과학기술 서비스', '74': '사업시설 관리·조경', '75': '사업지원 서비스', '76': '임대', '84': '공공행정', '85': '교육', '86': '보건', '87': '사회복지', '90': '창작·예술·여가', '91': '스포츠·오락', '94': '협회·단체', '95': '개인·소비용품 수리', '96': '기타 개인 서비스' };
const IND = { map: {}, tried: {}, busy: false };
function indGroup(code) { return code ? String(code).replace(/[^0-9]/g, '').slice(0, 2) : ''; }
function indLabel(code) { const g = indGroup(code); if (!g) return ''; return IND_NAMES[g] || ('KSIC ' + String(code)); }
function indPaint() {
  Array.prototype.forEach.call(document.querySelectorAll('[data-ind]'), function (e) { const c = e.getAttribute('data-ind'); const l = indLabel(IND.map[c]); e.textContent = l ? ' · ' + l : ''; });
}
// 저장된 업종을 읽는다(80개씩). 결과가 없는 코드는 빈 문자열로 기억해 다시 묻지 않는다.
async function indLookup(codes) {
  const need = codes.filter(function (c) { return c && IND.map[c] === undefined; });
  for (let i = 0; i < need.length; i += 80) {
    const part = need.slice(i, i + 80);
    try {
      const res = await fetch('/api/industry/lookup?codes=' + encodeURIComponent(part.join(','))); const d = await res.json(); if (!res.ok) break;
      part.forEach(function (c) { IND.map[c] = (d.map && d.map[c]) || ''; });
    } catch (e) { break; }
  }
}
// 저장된 업종이 없는 종목을 DART에서 3개씩 받아 저장한다(종목당 1회뿐). 실패한 종목은 이번 접속에서 다시 시도하지 않는다.
async function indFill(codes, onStep) {
  const miss = codes.filter(function (c) { return c && !IND.map[c] && !IND.tried[c]; });
  for (let i = 0; i < miss.length; i += 3) {
    const part = miss.slice(i, i + 3); part.forEach(function (c) { IND.tried[c] = 1; });
    try {
      const res = await fetch('/api/industry/fill', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ codes: part }) });
      const d = await res.json(); if (!res.ok) break;
      Object.keys(d.saved || {}).forEach(function (c) { IND.map[c] = d.saved[c]; });
    } catch (e) { break; }
    if (onStep) onStep(Math.min(i + 3, miss.length), miss.length);
  }
}
async function indEnsure(codes, onStep) {
  await indLookup(codes); indPaint();
  if (IND.busy) return; IND.busy = true;
  try { await indFill(codes, function (a, b) { indPaint(); if (onStep) onStep(a, b); }); } finally { IND.busy = false; }
  indPaint();
}
function indEnsureScreen() { const codes = []; Array.prototype.forEach.call(document.querySelectorAll('[data-ind]'), function (e) { const c = e.getAttribute('data-ind'); if (codes.indexOf(c) < 0) codes.push(c); }); if (codes.length) indEnsure(codes.slice(0, 60)); }
// 저장 종목 목록(종목분석·데이터 관리)과 백테스트 표에서 쓰는 공통 도우미
function indSyncList(list) { (list || []).forEach(function (c) { if (c && c.stock_code && c.ind) IND.map[c.stock_code] = c.ind; }); }
function indKey(code) { return indGroup(IND.map[code]) || ''; }
function indSmall(code) { const l = indLabel(IND.map[code]); return l ? ' · ' + escHtml(l) : ''; }
function indFillSelect(id, codes) {
  const sel = document.getElementById(id); if (!sel) return;
  const cur = sel.value, cnt = {}; let un = 0;
  codes.forEach(function (c) { const g = indKey(c); if (g) cnt[g] = (cnt[g] || 0) + 1; else un++; });
  sel.innerHTML = '<option value="">업종: 전체</option>' + Object.keys(cnt).sort(function (a, b) { return cnt[b] - cnt[a] || (a < b ? -1 : 1); }).map(function (g) { return '<option value="' + g + '">' + escHtml(IND_NAMES[g] || ('KSIC ' + g)) + ' (' + cnt[g] + ')</option>'; }).join('') + (un ? '<option value="none">업종 미확인 (' + un + ')</option>' : '');
  sel.value = cur && Array.prototype.some.call(sel.options, function (o) { return o.value === cur; }) ? cur : '';
}
function indMatchSel(sel, code) { if (!sel) return true; const g = indKey(code); return sel === 'none' ? !g : g === sel; }
// 업종 이름순(미확인은 맨 뒤)으로 묶어 정렬. 같은 업종 안에서는 원래 순서를 유지한다.
function indSort(items, codeOf) {
  return items.map(function (it, i) { return { it: it, i: i, k: indKey(codeOf(it)) }; }).sort(function (a, b) {
    if (a.k === b.k) return a.i - b.i; if (!a.k) return 1; if (!b.k) return -1;
    return (IND_NAMES[a.k] || a.k).localeCompare(IND_NAMES[b.k] || b.k, 'ko');
  }).map(function (x) { return x.it; });
}
function indJoin(items, codeOf, chipOf, grouped) {
  if (!grouped) return items.map(function (it, i) { return chipOf(it, i); }).join('');
  const sorted = indSort(items, codeOf), cnt = {}; sorted.forEach(function (it) { const k = indKey(codeOf(it)); cnt[k] = (cnt[k] || 0) + 1; });
  let h = '', last = null;
  sorted.forEach(function (it, i) {
    const k = indKey(codeOf(it));
    if (k !== last) { last = k; h += '<div class="ind-h" style="flex-basis:100%; width:100%; font-weight:700; font-size:12.5px; margin:8px 0 2px;">' + escHtml(k === '' ? '업종 미확인' : (IND_NAMES[k] || ('KSIC ' + k))) + ' (' + cnt[k] + ')</div>'; }
    h += chipOf(it, i);
  });
  return h;
}
// 목록에서 업종이 비어 있는 종목을 DART에서 받아 채운다(종목당 1회, 3개씩).
async function indFillList(which) {
  const msg = document.getElementById(which + 'IndMsg'); const list = dbCompanies || [];
  const codes = list.map(function (c) { return c.stock_code; }).filter(function (c) { return c && !IND.map[c]; });
  const again = function () { if (which === 'saved') renderSavedBox(); else renderDbList(); };
  if (!codes.length) { if (msg) msg.textContent = '업종을 아직 못 받은 종목이 없습니다.'; return; }
  if (msg) msg.textContent = '업종을 받는 중... 0 / ' + codes.length;
  await indFill(codes, function (a, b) { if (msg) msg.textContent = '업종을 받는 중... ' + a + ' / ' + b; again(); });
  const left = codes.filter(function (c) { return !IND.map[c]; }).length;
  if (msg) msg.textContent = left ? ('완료. ' + left + '개 종목은 DART에서 업종을 받지 못했습니다.') : '완료';
  again();
}
// ===== /업종 =====

// ===== 쉬운 설명 (백테스트·지표 용어집) =====
// 비전공자용 설명집. 항목마다 "한마디로 / 쉬운 비유·예 / 이 앱에서 어디 / 조심할 점"을 적는다. 숫자 기준(예: t값 2·3)은 이 앱이 쓰는 기준이며 절대 규칙이 아니다.
const GUIDE_CATS = ["백테스트 기초", "결과 읽는 법", "속기 쉬운 함정", "수익성·효율", "현금흐름", "가치(싼지 비싼지)", "성장·재무 데이터", "안정성", "주가 흐름", "화면에서 자주 보는 말", "금융업·지주회사"];
const GUIDE = [
  { id: "bt-what", c: 0, t: "백테스트란?", k: "백테스트 과거 검증 시뮬레이션",
    one: "과거 자료로 '이 방법을 그때 썼다면 어땠을까'를 미리 해보는 연습 시험입니다.",
    ex: "예) '영업이익률이 높은 회사를 사면 이긴다'는 생각이 있다면, 2015년부터 매년 그 기준으로 종목을 골랐을 때 실제로 그 뒤 1년 수익이 좋았는지 확인합니다.",
    app: "백테스트 탭 A·A-2·B. 점수로 종목을 줄 세우고 5개 그룹으로 나눠 그룹별 실제 수익률을 비교합니다.",
    warn: "과거에 좋았다고 앞으로도 좋다는 보장은 없습니다. 이 앱의 결과는 '참고 증거'이지 수익 약속이 아닙니다." },
  { id: "bt-groups", c: 0, t: "상위 그룹 · 하위 그룹 (5분위)", k: "분위 5분위 quintile 상위 하위 그룹",
    one: "점수 순으로 종목을 5등분해, 가장 점수 높은 20%(상위)와 가장 낮은 20%(하위)의 성적을 비교합니다.",
    ex: "예) 달리기 기록으로 반을 5조로 나눈 뒤, 1조와 5조의 이후 시험 성적을 비교하는 것과 같습니다. 1조 성적이 늘 더 좋다면 '기록'이 쓸모 있는 정보라는 뜻이죠.",
    app: "백테스트 결과의 그룹별 수익률, 차트의 초록(상위)·빨강(하위)·회색(전체 평균) 선.",
    warn: "상위 그룹이 이긴 해가 많아도, 몇몇 해에 크게 이긴 것뿐일 수 있습니다. 해마다 일관적인지 함께 보세요." },
  { id: "bt-longshort", c: 1, t: "롱숏 (상위 − 하위)", k: "롱숏 long short 스프레드",
    one: "상위 그룹 수익률에서 하위 그룹 수익률을 뺀 값. 클수록 점수가 수익을 잘 가려냈다는 뜻입니다.",
    ex: "예) 상위 +12%, 하위 +2%면 롱숏은 +10%p. 시장 전체가 오르내린 영향은 서로 상쇄되어 '방법 자체의 힘'만 남습니다.",
    app: "백테스트 요약 표.", warn: "실제로 하위 그룹을 공매도하는 것은 어렵습니다. '방법의 힘을 재는 자' 정도로 이해하세요." },
  { id: "bt-ic", c: 1, t: "IC (정보계수, 순위 상관)", k: "IC 정보계수 rank ic 상관",
    one: "점수 순위와 이후 수익률 순위가 얼마나 닮았는지(−1~+1). 0이면 상관없음, 1이면 점수 순서 그대로 수익이 나옴.",
    ex: "예) 키 순서와 몸무게 순서가 비슷하면 상관이 높죠. 같은 방식으로 '점수 순서'와 '수익 순서'를 비교합니다.",
    app: "백테스트 결과의 IC 열.", warn: "현실에서 좋은 지표의 IC도 0.02~0.1 정도로 작습니다. 0.5 같은 값이 나오면 오히려 오류나 편향을 의심하세요." },
  { id: "bt-t", c: 1, t: "t 통계량 · p값 (우연일 가능성)", k: "t값 t통계량 p값 유의 우연 newey west 뉴이웨스트",
    one: "결과가 운이 아닐 가능성을 숫자로 만든 것. t의 절댓값이 클수록 우연이기 어렵습니다.",
    ex: "예) 동전을 던져 앞면이 6/10번 나온 건 우연일 수 있지만 600/1000번이면 우연이기 어렵죠. t값은 이런 '우연의 어려움'을 재는 눈금입니다.",
    app: "백테스트 판정. 이 앱은 2 미만이면 '우연과 구분 안 됨', 여러 방법을 시험했다면 3 이상을 요구합니다. 연속된 기간끼리 결과가 닮는 점을 보정한 Newey-West 방식을 씁니다.",
    warn: "t가 커도 편향이 있으면 소용없습니다. 또 많이 시험할수록 우연히 큰 값이 나옵니다(→ '많이 시험하면 생기는 착시')." },
  { id: "bt-multi", c: 2, t: "많이 시험하면 생기는 착시 (다중검정 · 데이터 스누핑)", k: "다중검정 스누핑 데이터스누핑 westfall young 보정 p 조정 harvey",
    one: "방법을 100개 시험하면 운이 좋은 몇 개는 반드시 '유의'하게 나옵니다. 그중 최고만 고르면 거짓 발견입니다.",
    ex: "예) 로또 100장을 사서 가장 많이 맞은 한 장만 보고 '이 번호가 비결'이라고 하는 셈입니다.",
    app: "백테스트의 '시도 횟수' 기록과 A-2 조합 탐색의 '보정 p값'(Westfall-Young maxT). 조합 탐색은 여러 조합을 한꺼번에 시험한 사실을 반영해 기준을 더 엄격하게 만듭니다.",
    warn: "보정된 p값도 완벽한 보증은 아닙니다. 탐색 구간에서 찾은 조합이 따로 떼어둔 확인 구간에서도 통하는지가 더 중요합니다." },
  { id: "bt-split", c: 2, t: "탐색 구간 vs 확인 구간", k: "탐색 확인 구간 분리 외표본 oos in-sample 검증",
    one: "앞부분 기간(약 2/3)으로 좋은 조합을 찾고, 한 번도 안 본 뒷부분(약 1/3)으로 정말 통하는지 다시 확인합니다.",
    ex: "예) 기출문제로 공부(탐색)하고, 처음 보는 시험지(확인)로 실력을 재는 것과 같습니다.",
    app: "백테스트 A-2 조합 탐색.", warn: "확인 구간이 짧으면 결과가 흔들립니다. 통과 못 했다고 무가치한 것도, 통과했다고 확실한 것도 아닙니다." },
  { id: "bt-lookahead", c: 2, t: "미래 정보 엿보기 (룩어헤드 편향)", k: "룩어헤드 look-ahead 공시일 미래 정보",
    one: "그 시점엔 아직 공개되지 않은 정보를 써버리는 실수. 결과가 터무니없이 좋아집니다.",
    ex: "예) 12월 결산 보고서는 이듬해 3월에야 공시됩니다. 1월에 이미 그 숫자를 안다고 가정하면 반칙입니다.",
    app: "이 앱은 재무 숫자를 공시된 뒤에만 쓰도록 설계했습니다. 분기 백테스트는 '공시 다음 거래일 종가에 사고, 다음 공시 다음 거래일 종가에 파는' 규칙입니다(종목별 보기의 공시일·매수일·매도일 열 참고).",
    warn: "정정 공시로 숫자가 나중에 바뀐 경우까지 완벽히 반영하지는 못할 수 있습니다." },
  { id: "bt-survivor", c: 2, t: "살아남은 종목만 보는 착시 (생존편향)", k: "생존편향 survivorship 상장폐지",
    one: "망해서 사라진 종목을 빼고 지금 남은 종목만 보면 과거 성적이 실제보다 좋아 보입니다.",
    ex: "예) 졸업생 명단만 보고 '이 학원 출신은 다 합격한다'고 하는 것과 같습니다. 중퇴자도 세야 공정하죠.",
    app: "저장된 종목이 지금 거래되는 회사 위주라면 이 착시가 남아 있을 수 있습니다. 결과 아래 '주의할 점'을 확인하세요.",
    warn: "상위 그룹의 수익률이 높게 나왔다면 이 편향 때문에 부풀려졌을 수 있다고 생각하세요." },
  { id: "bt-sample", c: 2, t: "표본이 적으면 믿기 어렵다", k: "표본 부족 샘플 개수 종목수",
    one: "종목 수나 기간이 적으면 몇 종목 때문에 결과가 뒤집힙니다.",
    ex: "예) 5명에게 물어본 설문과 5000명에게 물어본 설문의 신뢰도는 다르죠.",
    app: "저장된 재무가 적으면 판정이 '표본 부족'으로 나옵니다. 데이터 관리에서 종목·기간을 늘리면 좋아집니다.",
    warn: "표본 부족 결과를 '효과 없음'으로 읽지도, '효과 있음'으로 읽지도 마세요. 그냥 '모름'입니다." },
  { id: "bt-cost", c: 2, t: "거래비용 · 배당 · 체결의 현실", k: "거래비용 수수료 슬리피지 세금 배당 체결 유동성",
    one: "실제로는 수수료·세금·호가 차이가 들고, 거래가 적은 종목은 원하는 가격에 못 삽니다. 배당은 따로 받습니다.",
    ex: "예) 백테스트 수익 10%가 거래비용 3%를 빼면 7%가 됩니다. 사고파는 횟수가 많을수록 차이가 커집니다.",
    app: "백테스트 결과 아래 '주의할 점'에서 비용·배당 반영 여부를 확인하세요.",
    warn: "비용을 반영하지 않은 결과는 현실보다 좋게 나옵니다." },
  { id: "bt-rebal", c: 0, t: "리밸런싱(종목 다시 고르기)과 보유 기간", k: "리밸런싱 보유기간 12개월 연간 분기",
    one: "정해진 주기마다 점수를 다시 매겨 종목을 바꾸는 일. 이 앱의 종목별 보기는 공시 후 12개월 수익률을 봅니다.",
    ex: "예) 해마다 사업보고서가 나온 뒤 점수를 다시 계산해 상위 종목으로 갈아타는 규칙입니다.",
    app: "백테스트 A(연간/분기), 종목별 보기의 매수가·매도가.", warn: "주기를 바꾸면 결과도 바뀝니다. 한 가지 주기에서만 좋은 방법은 우연일 수 있습니다." },

  { id: "roic", c: 3, t: "ROIC (투하자본이익률)", k: "ROIC 투하자본 이익률 nopat",
    one: "회사가 사업에 넣은 돈 100원으로 세금 낸 뒤 영업으로 몇 원을 버는가.",
    ex: "예) 가게에 10억을 넣어 연 1.5억을 벌면 ROIC는 15%. 은행 이자보다 훨씬 높으면 돈을 잘 굴리는 사업입니다.",
    app: "종목분석 수익성 장, 재무 스크리닝(ROIC 3년 평균·최신).",
    warn: "계산 방식(투하자본에 뭘 넣는지)에 따라 값이 달라 회사끼리 비교할 때 조심하세요. 은행·보험 같은 금융업은 일반 기업과 구조가 달라 해석이 어렵습니다." },
  { id: "roe", c: 3, t: "ROE (자기자본이익률)", k: "ROE 자기자본 이익률",
    one: "주주 몫의 돈 100원으로 순이익을 몇 원 벌었는가.",
    ex: "예) 내 돈 1억으로 가게를 차려 1천만 원을 남기면 ROE 10%.",
    app: "종목분석, 백테스트 기존 Quality 점수.", warn: "빚을 많이 쓰면 ROE가 부풀려집니다. 부채비율을 같이 보세요." },
  { id: "opm", c: 3, t: "영업이익률", k: "영업이익률 마진 operating margin",
    one: "매출 100원 중 본업으로 남는 돈이 몇 원인가.",
    ex: "예) 매출 1,000억에 영업이익 100억이면 10%. 같은 업종에서 높을수록 가격 결정력이나 원가 경쟁력이 좋을 가능성이 있습니다.",
    app: "재무 스크리닝 '영업이익률 ≥' 조건.", warn: "업종마다 보통 수준이 크게 다릅니다(유통 3%, 소프트웨어 20% 등). 같은 업종끼리 비교하세요." },
  { id: "gm", c: 3, t: "매출총이익률 · GP/A", k: "매출총이익 gpa gross profitability 총자산",
    one: "매출에서 원가만 뺀 이익이 얼마나 남는지. GP/A는 그 이익을 회사 전체 자산 대비로 본 값.",
    ex: "예) 같은 자산으로 원가를 빼고도 많이 남기는 회사가 '장사 기본기'가 좋습니다.",
    app: "백테스트 요인 '매출총이익/총자산(Novy-Marx 2013)'.", warn: "판관비·연구개발비를 아직 안 뺀 값이라 최종 수익성은 영업이익률로 따로 확인하세요." },
  { id: "accrual", c: 3, t: "발생액 (이익의 질)", k: "발생액 accrual 이익의 질 sloan",
    one: "장부상 이익과 실제 현금 사이의 차이. 장부 이익만 크고 현금이 안 들어오면 이익의 질이 낮다고 봅니다.",
    ex: "예) 외상 매출만 늘어난 회사는 이익은 늘어도 통장엔 돈이 없습니다.",
    app: "백테스트 요인 '낮은 발생액(Sloan 1996)', 종목분석 현금전환.", warn: "큰 투자나 성장 중인 회사는 발생액이 일시적으로 클 수 있습니다." },

  { id: "cfo", c: 4, t: "영업현금흐름 (CFO)", k: "CFO 영업현금흐름 현금",
    one: "본업을 하면서 실제로 통장에 들어온 현금.",
    ex: "예) 장부 이익이 100억이어도 외상값을 못 받으면 현금은 0일 수 있습니다. CFO가 진짜 '벌어온 돈'에 가깝습니다.",
    app: "종목분석 현금흐름 장, 재무 스크리닝 상세표.", warn: "분기 보고서의 현금흐름은 연초부터의 누적값이라, 이 앱은 차감해 '단독 분기'로 바꿔 씁니다(→ 누적값과 단독 분기)." },
  { id: "capex", c: 4, t: "CAPEX (설비·시설 투자)", k: "CAPEX 설비투자 유형자산 취득",
    one: "공장·기계·건물처럼 오래 쓰는 것을 사는 데 쓴 돈.",
    ex: "예) 빵집이 오븐을 새로 사는 돈. 성장을 위한 투자일 수도, 낡은 설비를 유지하는 비용일 수도 있습니다.",
    app: "FCF 계산에 사용. 이 앱의 CAPEX는 보고서의 유형·무형자산 취득액 기준입니다.",
    warn: "'유지용'과 '성장용'을 나누는 것은 관측할 수 없는 추정이라 이 앱은 임의로 나누지 않고 전체를 뺍니다." },
  { id: "fcf", c: 4, t: "FCF (잉여현금흐름)", k: "FCF 잉여현금흐름 free cash flow",
    one: "본업에서 번 현금(CFO)에서 투자(CAPEX)를 빼고 남는, 주주나 채권자에게 돌려줄 수 있는 돈.",
    ex: "예) 월급(CFO)에서 꼭 필요한 집수리비(CAPEX)를 뺀 뒤 자유롭게 쓸 수 있는 돈과 같습니다.",
    app: "FCF = CFO − CAPEX. 종목분석·재무 스크리닝·백테스트 요인(FCF 수익률).",
    warn: "FCF가 음수라고 나쁜 회사만은 아닙니다. 성장 투자를 크게 하는 시기일 수 있으니 CFO·매출 추세를 함께 보세요. 이 값은 엄밀한 학술 정의(FCFF)와는 조금 다른 실무형 지표입니다." },
  { id: "fcfm", c: 4, t: "FCF 마진", k: "FCF 마진 fcf margin",
    one: "매출 100원 중 최종적으로 남는 자유 현금이 몇 원인가.", ex: "예) 매출 1,000억, FCF 80억이면 8%.",
    app: "재무 스크리닝 'FCF 마진 ≥' (0%면 FCF 흑자인 종목).", warn: "한 해 값은 들쭉날쭉합니다. 3~5년 흐름을 보세요." },
  { id: "fcfni", c: 4, t: "FCF / 순이익 (현금 전환)", k: "fcf 순이익 현금전환 cash conversion",
    one: "장부상 순이익이 실제 자유 현금으로 얼마나 바뀌는가. 1에 가까우면 이익이 현금으로 잘 이어집니다.",
    ex: "예) 순이익 100억에 FCF 90억이면 0.9배 — 이익의 질이 좋은 편입니다.",
    app: "재무 스크리닝 'FCF/순이익 ≥'.", warn: "순이익이 0이거나 음수면 계산하지 않습니다(N/A)." },
  { id: "fcfpos", c: 4, t: "FCF 흑자 연수", k: "fcf 흑자 연수 꾸준 5년",
    one: "최근 5년 중 FCF가 플러스였던 해의 수. 꾸준함을 봅니다.", ex: "예) 5년 중 4년 흑자면 대체로 꾸준한 편.",
    app: "재무 스크리닝 '최근 5년 중 FCF 흑자 연수 ≥'.", warn: "경기 사이클 업종은 불황기에 일시적으로 적자일 수 있습니다." },

  { id: "per", c: 5, t: "PER (주가수익비율)", k: "PER 주가수익비율 이익수익률 EP",
    one: "지금 주가가 1년 순이익의 몇 배인가. 낮을수록 이익 대비 싸다는 뜻입니다.",
    ex: "예) 시가총액 1,000억, 연 순이익 100억이면 PER 10배. 가게를 1,000억에 사면 이익으로 10년 만에 본전입니다. (EP = 1/PER, 이익수익률)",
    app: "재무 스크리닝 'PER ≤', 백테스트 요인 '이익수익률 EP'.",
    warn: "PER이 낮다고 항상 싼 것이 아닙니다. 이익이 곧 줄 회사라면 싸 보이는 함정일 수 있습니다. 순이익이 음수면 계산하지 않습니다." },
  { id: "pbr", c: 5, t: "PBR (주가순자산비율)", k: "PBR 주가순자산 장부가 BP",
    one: "주가가 회사 장부상 순자산의 몇 배인가. 1보다 낮으면 장부 가치보다 싸게 거래된다는 뜻입니다.",
    ex: "예) 집 장부가가 10억인데 시장에서 8억에 팔리면 PBR 0.8. (BP = 1/PBR)",
    app: "재무 스크리닝 'PBR ≤', 백테스트 요인 '장부가/주가 BP'.", warn: "자산 가치가 장부보다 실제로 낮은 경우(재고·설비 노후)나 수익성이 낮은 경우 PBR이 낮은 채로 계속될 수 있습니다." },
  { id: "fcfy", c: 5, t: "FCF 수익률", k: "FCF 수익률 fcf yield",
    one: "시가총액 대비 1년에 생기는 자유 현금의 비율. 높을수록 현금 대비 싸다는 뜻입니다.",
    ex: "예) 시가총액 1,000억, FCF 70억이면 7%. 은행 이자와 비교해 볼 수 있습니다.",
    app: "재무 스크리닝 'FCF 수익률 ≥', 백테스트 요인 'FCF 수익률'.", warn: "시세가 없는 종목은 계산하지 않습니다. 일시적 현금 유입(자산 매각 등)으로 높아 보일 수 있습니다." },
  { id: "graham", c: 5, t: "Graham 복합 (PER × PBR)", k: "그레이엄 graham per pbr 복합",
    one: "PER과 PBR을 곱해 둘 다 낮은 종목을 찾는 벤저민 그레이엄의 고전적 기준.",
    ex: "예) PER 10배 × PBR 1.2배 = 12. 그레이엄은 대략 22.5 이하를 선호했습니다(원전 기준).",
    app: "백테스트 요인 'Graham 복합'(실무 관행 등급).", warn: "시대와 시장이 달라 기준 숫자를 그대로 믿기보다 '낮은 쪽'이라는 방향만 쓰세요." },
  { id: "mcap", c: 5, t: "시가총액", k: "시가총액 mcap 주가 주식수",
    one: "회사 전체의 시장 가격 = 주가 × 주식 수.", ex: "예) 주가 5만 원 × 1,000만 주 = 5,000억 원.",
    app: "스크리닝·재무 스크리닝. 재무 스크리닝은 저장된 가장 최근 종가를 씁니다.", warn: "시세 기준일이 오래되면 PER·PBR·FCF 수익률도 오래된 값입니다(화면 위 시세 기준일 확인)." },

  { id: "cagr", c: 6, t: "CAGR (연평균 성장률)", k: "CAGR 연평균 성장률 3년 매출 영업이익 성장",
    one: "N년 동안 한 해 평균 몇 %씩 컸는지를 복리로 환산한 값.",
    ex: "예) 매출이 3년 동안 100억 → 133억이면 CAGR 약 10%.",
    app: "재무 스크리닝 '매출·영업이익 3년 CAGR'.", warn: "시작이나 끝 값이 0 이하면 계산하지 않습니다(N/A). 기저가 작을 때 비정상적으로 크게 나올 수 있습니다." },
  { id: "ttm", c: 6, t: "TTM (최근 12개월)", k: "TTM 최근 4분기 12개월 trailing",
    one: "가장 최근 4개 분기를 더한 값. 사업보고서(연 1회)보다 더 최신 상태를 보여줍니다.",
    ex: "예) 2026년 3분기까지 나왔다면 2025년 4분기+2026년 1·2·3분기가 TTM.",
    app: "재무 스크리닝 '최신값: TTM 우선', 종목 상세표의 주황색 TTM 열.", warn: "4개 분기가 다 있어야 만들 수 있고, 하나라도 비면 TTM을 만들지 않습니다." },
  { id: "cum", c: 6, t: "누적값과 단독 분기 (왜 빼서 쓰나)", k: "누적 단독 분기 현금흐름 손익 차감",
    one: "DART의 분기 현금흐름·손익은 '연초부터 지금까지의 합계'로 공시됩니다. 분기 하나의 값을 알려면 뺄셈이 필요합니다.",
    ex: "예) 1분기 100, 반기 260이면 2분기 단독은 260 − 100 = 160. 4분기는 연간 − 3분기 누적입니다.",
    app: "TTM 계산 전 단계.", warn: "비교할 누적 기준이 없으면 무리하게 빼지 않고 N/A로 둡니다." },
  { id: "cfs", c: 6, t: "연결 vs 별도, 결산월", k: "연결 별도 cfs ofs 결산월 12월 결산",
    one: "연결은 자회사까지 합친 숫자, 별도는 본체만의 숫자. 결산월은 회계연도가 끝나는 달입니다.",
    ex: "예) 지주회사는 별도 매출이 작아도 연결에서는 크게 나옵니다.",
    app: "기본은 연결 우선. 12월 결산이 아닌 회사는 공시일 계산이 달라 백테스트에서 제외하거나 경고합니다.", warn: "연결·별도 숫자를 한 줄로 섞어 비교하면 왜곡됩니다." },
  { id: "fscore", c: 7, t: "F-Score (피오트로스키 점수)", k: "fscore f-score piotroski 피오트로스키 9점",
    one: "수익성·재무 건전성·효율 9가지를 하나씩 확인해 0~9점으로 매깁니다. 높을수록 재무가 탄탄해지는 중이라고 봅니다.",
    ex: "예) 순이익 흑자, 영업현금 흑자, 부채 감소, 주식 수 증가 없음 같은 항목을 하나씩 체크합니다.",
    app: "재무 스크리닝 'F-Score ≥', 백테스트 요인.", warn: "판정 가능한 항목이 8개 미만이면 N/A. 원 논문은 미국 시장에서 가치주 안의 선별에 쓴 것이라 한국에서는 따로 검증이 필요합니다(근거: Piotroski 2000)." },
  { id: "de", c: 7, t: "부채비율 · 순차입금/EBITDA", k: "부채비율 순차입금 EBITDA 레버리지 빚",
    one: "빚이 얼마나 되는지. 부채비율은 자본 대비 부채, 순차입금/EBITDA는 번 돈으로 순빚을 몇 년이면 갚는지입니다.",
    ex: "예) 순차입금/EBITDA 2배는 현재 벌이로 약 2년이면 순빚을 다 갚는다는 뜻.",
    app: "재무 스크리닝 상세표.", warn: "금융업은 부채 구조가 달라 일반 기업과 같은 눈금으로 보면 안 됩니다." },
  { id: "ccc", c: 7, t: "현금전환주기 (CCC)", k: "ccc 현금전환주기 매출채권 재고 매입채무 dso dio dpo",
    one: "원료 사서 팔고 돈을 회수하기까지 현금이 묶이는 날수. 짧을수록 자금 부담이 적습니다.",
    ex: "예) 재고 30일 + 외상 회수 40일 − 외상 지급 50일 = 20일 동안 내 돈이 묶입니다.",
    app: "재무 스크리닝 상세표 '현금전환주기(일)', 종목분석.", warn: "금융업 등에선 의미가 약합니다." },

  { id: "rs", c: 8, t: "모멘텀 · 상대강도(RS)", k: "모멘텀 RS 상대강도 20일 60일 120일 52주",
    one: "최근 주가가 시장(코스피)보다 얼마나 더 올랐는지. 오르던 종목이 한동안 더 오르는 경향을 쓰려는 지표입니다.",
    ex: "예) 60일 RS가 +10%p면 최근 60일간 코스피보다 10%p 더 올랐다는 뜻.",
    app: "스크리닝 탭의 20·60·120일 RS와 모멘텀 점수.",
    warn: "시장 방향이 갑자기 바뀌면 모멘텀 전략은 크게 손실을 낼 수 있습니다. 가격이 오른 것은 사업 가치가 올랐다는 증거가 아닙니다." },
  { id: "attention", c: 8, t: "거래대금 관심도 (Attention)", k: "거래대금 관심도 attention 거래량",
    one: "평소보다 거래가 얼마나 늘었는지. 후보를 발견하는 용도이며, 좋은 회사라는 뜻이 아닙니다.",
    ex: "예) 갑자기 손님이 몰린 가게 — 이유는 좋을 수도 나쁠 수도 있습니다.", app: "스크리닝 탭.", warn: "관심도가 높다고 사도 되는 것은 아닙니다. 재무·가치와 따로 확인하세요." },

  { id: "ksic", c: 9, t: "업종 (KSIC)", k: "업종 ksic 산업분류 induty",
    one: "회사가 주로 하는 사업의 종류. 이 앱은 DART 기업개황의 한국표준산업분류(KSIC) 코드를 중분류(앞 2자리) 이름으로 보여줍니다.",
    ex: "예) 26 = 전자부품·컴퓨터·통신장비. '삼성전자'는 여기 속합니다.",
    app: "스크리닝·재무 스크리닝·종목분석·백테스트 상위 종목·저장 종목 목록.",
    warn: "거래소나 네이버 금융이 쓰는 업종과 분류가 다릅니다. 이름표는 이 앱이 요약한 것이므로 정확한 분류는 DART 기업개황을 확인하세요." },
  { id: "na", c: 9, t: "N/A · 자료 없어 제외", k: "na 산출불가 자료없음 제외 결측",
    one: "필요한 값이 없어 계산하지 않았다는 뜻. 임의 값을 채워 넣지 않습니다.",
    ex: "예) 순이익이 음수면 PER은 의미가 없어 N/A, 시세가 없으면 FCF 수익률이 N/A.",
    app: "재무 스크리닝 단계표의 '자료 없어 제외'.", warn: "N/A가 많으면 그 조건 때문에 후보가 빠르게 줄어듭니다. 단계표에서 어느 조건이 얼마나 줄였는지 보세요." },
  { id: "badge", c: 9, t: "근거 등급 배지 (학술 검증·이론 근거·실무 관행·가정)", k: "배지 근거 등급 학술 검증 실무 가정 휴리스틱",
    one: "그 지표가 얼마나 단단한 근거를 갖는지 색으로 알려줍니다.",
    ex: "초록 '학술 검증' = 논문으로 반복 확인, 파랑 '이론 근거', 주황 '실무 관행' = 전문가들이 쓰지만 보장 없음, 회색 '가정·휴리스틱' = 이 앱의 임의 기준.",
    app: "백테스트 요인 이름 옆, 종목분석 항목.", warn: "초록이어도 미국 등 다른 시장·기간의 결과일 수 있어 한국에서 따로 확인이 필요합니다." },
  { id: "funnel", c: 9, t: "단계표 (깔때기)", k: "단계표 깔때기 funnel 남은 종목",
    one: "조건을 하나씩 적용할 때마다 종목이 몇 개 남는지 보여주는 표.",
    ex: "예) 전체 300개 → ROIC 조건 120개 → FCF 조건 40개 → 업종 조건 6개.",
    app: "재무 스크리닝 맨 위.", warn: "조건을 너무 많이 겹치면 우연히 맞는 소수만 남습니다. 결과 개수가 너무 적으면 조건 수를 줄여 보세요." },
];
GUIDE.push(
  { id: "finwhy", c: 10, t: "은행·보험·증권은 왜 다른 지표로 보나", k: "금융업 은행 보험 증권 roic fcf 부채비율 레버리지 금융회사",
    one: "일반 회사에게 빚은 '자금'이지만, 은행 같은 금융회사에게 빚(예금·보험료)은 장사하는 '재료'입니다. 그래서 일반 회사용 지표(ROIC, FCF, 부채비율)가 맞지 않습니다.",
    ex: "예) 은행의 부채비율이 1,100%여도 정상입니다. 예금을 많이 받았다는 뜻이니까요. 제조업이 1,100%라면 위험 신호입니다.",
    app: "종목분석(금융업 화면), 상단 요약 카드, 재무 스크리닝의 유형 선택.",
    warn: "근거는 Damodaran의 가치평가 교재 21장(금융회사는 자본 기준으로 평가)과 Fama-French(1992)가 같은 이유로 금융회사를 표본에서 뺀 것입니다. 한국 시장에서 이 방식의 성과를 이 앱이 검증하지는 않았습니다." },
  { id: "pbrroe", c: 10, t: "PBR이 낮으면 싼 걸까? (ROE와 정당 PBR)", k: "pbr roe 정당 pbr 저pbr 장부가 요구수익률 금융업 가치평가",
    one: "PBR 1배 미만은 '장부가보다 싸다'는 뜻이지만, 그 회사가 자본으로 돈을 못 번다면 싼 게 아니라 정상 가격일 수 있습니다. ROE와 같이 봐야 합니다.",
    ex: "예) 요구수익률이 9%인데 ROE가 6%인 은행은 이론상 PBR이 1배보다 낮은 게 맞습니다. 반대로 ROE가 12%인데 PBR이 0.5배라면 시장이 ROE가 곧 떨어질 거라고 보거나, 저평가이거나 둘 중 하나입니다.",
    app: "종목분석 금융업 화면의 「가격」 장(정당 PBR, 시장이 요구하는 ROE, 민감도 표).",
    warn: "정당 PBR = (ROE−g)/(ke−g)는 고든 성장모형에서 유도한 이론식입니다. ROE가 계속될지, 요구수익률 ke, 성장률 g가 모두 가정이라 한 숫자를 믿지 말고 민감도 표를 보세요." },
  { id: "finmiss", c: 10, t: "재무제표에 없는 금융업 지표 (NIM·NPL·BIS·K-ICS·CSM·NCR)", k: "nim npl bis cet1 k-ics csm ncr 순이자마진 지급여력 자본비율",
    one: "금융회사의 진짜 건강검진 숫자는 재무제표 항목에 들어 있지 않습니다. 이 앱은 값을 만들어 채우지 않고 확인할 곳만 안내합니다.",
    ex: "NIM(순이자마진)은 예금이자와 대출이자 차이로 남기는 폭, NPL(고정이하여신)은 못 받을 가능성이 큰 대출 비율, BIS·CET1은 은행이 손실을 버틸 자기자본 비율, K-ICS는 보험사가 보험금을 줄 수 있는 능력 비율, CSM은 보험사가 앞으로 이익으로 인식할 보험계약 마진, NCR은 증권사 순자본비율입니다.",
    app: "종목분석 금융업 화면 맨 위 「미수집 지표」 표.",
    warn: "정확한 정의와 최신 값은 각사 공시와 금융감독원 자료를 확인하세요. 이 앱의 ROE·PBR만으로 금융회사의 안전성을 판단하면 안 됩니다." },
  { id: "holdnav", c: 10, t: "지주회사 NAV(순자산가치)와 할인", k: "지주회사 nav 순자산가치 할인 자회사 지분 sotp",
    one: "지주회사가 가진 자회사 지분을 시가로 전부 더한 값(NAV)과 지주회사 시가총액을 비교합니다. 시가총액이 NAV보다 낮으면 '할인'된 가격에 거래되는 것입니다.",
    ex: "예) 상장 자회사 지분 합이 10조 원이고 순차입금이 1조 원이면 NAV는 9조 원. 시가총액이 4.5조 원이면 할인율은 50%입니다.",
    app: "종목분석에서 지주회사(비금융)로 분류된 종목의 NAV 계산기.",
    warn: "증권업계 실무 관행입니다. 할인이 크다고 주가가 오른다는 증명은 확인하지 못했고, 할인이 오래 지속되는 이유(지배구조, 배당 정책 등)도 있습니다. 지분율은 사업보고서의 타법인 출자 현황에서 직접 옮겨 적어야 합니다." },
  { id: "finsec", c: 10, t: "금융업·지주회사를 구분하는 방법", k: "금융업 분류 ksic 64 65 66 64992 지주회사 이름 추정",
    one: "업종코드(KSIC) 앞자리로 구분합니다: 64=금융업, 65=보험·연금, 66=금융 관련 서비스, 64992=지주회사. 코드가 없으면 회사 이름으로 추정하고 이름 추정에는 물음표(?)를 붙입니다.",
    ex: "예) '신한지주'는 지주회사 코드지만 금융지주라서 은행처럼 봅니다(이 구분은 이름 목록으로 합니다).",
    app: "종목분석 한눈에 보기, 재무 스크리닝의 유형 열, 백테스트 대상 선택.",
    warn: "이름 추정은 틀릴 수 있습니다. 종목분석에서 「일반 기업용 화면으로 보기」「금융업 화면으로 보기」로 직접 바꿀 수 있습니다." },
  { id: "btuniv", c: 10, t: "백테스트에서 금융업을 빼는 이유", k: "백테스트 금융업 제외 대상 종목 표본 레버리지",
    one: "많은 주식 연구가 금융회사를 표본에서 뺍니다. 금융회사의 높은 부채는 부실 신호가 아니라서, 부채가 위험하다는 가정이 섞인 지표가 왜곡되기 때문입니다.",
    ex: "예) F-Score에는 부채가 늘면 감점하는 항목이 있는데, 금융회사에는 맞지 않습니다.",
    app: "백테스트 A 카드의 「대상 종목」 선택(전체 / 금융업 제외 / 금융업·지주회사 제외 / 금융업만).",
    warn: "제외 여부를 바꿔 가며 결과를 비교해 좋은 쪽을 고르면 데이터 스누핑이 됩니다. 시험하기 전에 어떤 대상으로 볼지 정하세요. 분류는 이 앱의 방식(코드+이름)이라 틀린 종목이 섞일 수 있습니다." }
);
function guideSearchText(g) { return (g.t + " " + g.k + " " + g.one + " " + g.ex + " " + g.app + " " + g.warn).toLowerCase(); }
function guideEntryHtml(g) {
  const row = function (lab, v) { return v ? '<div style="margin:5px 0; line-height:1.6;"><b>' + lab + '</b> ' + escHtml(v) + '</div>' : ""; };
  return '<details id="gd_' + g.id + '" class="gd" style="border-top:1px solid var(--border); padding:6px 0;"><summary style="cursor:pointer; font-weight:600; font-size:14px;">' + escHtml(g.t) + '</summary>' +
    '<div style="font-size:13px; padding:4px 2px 6px;">' + '<div style="background:#f0f9ff; border-radius:8px; padding:6px 10px; line-height:1.6;"><b>한마디로</b> ' + escHtml(g.one) + '</div>' +
    row("쉬운 예", g.ex) + row("이 앱에서", g.app) + row("조심할 점", g.warn) + '</div></details>';
}
function guideRender() {
  const out = document.getElementById("guideOut"); if (!out) return;
  const q = (document.getElementById("guideQ").value || "").trim().toLowerCase();
  const cat = document.getElementById("guideCat").value;
  let h = "", n = 0;
  GUIDE_CATS.forEach(function (name, ci) {
    if (cat !== "" && String(ci) !== cat) return;
    const items = GUIDE.filter(function (g) { return g.c === ci && (!q || guideSearchText(g).indexOf(q) >= 0); });
    if (!items.length) return;
    n += items.length;
    h += '<div style="font-weight:700; font-size:14px; margin:14px 0 2px; color:var(--primary-dark);">' + escHtml(name) + ' <span style="font-weight:400; color:var(--text-muted); font-size:12px;">(' + items.length + ')</span></div>' + items.map(guideEntryHtml).join("");
  });
  out.innerHTML = n ? h : '<div class="meta-line">일치하는 설명이 없습니다. 다른 단어(예: 현금, 수익률, 우연)로 찾아 보세요.</div>';
  document.getElementById("guideCount").textContent = n + "개 설명";
}
function guideInit() {
  const sel = document.getElementById("guideCat"); if (!sel || sel.getAttribute("data-ready")) { return; }
  sel.setAttribute("data-ready", "1");
  sel.innerHTML = '<option value="">전체 분야</option>' + GUIDE_CATS.map(function (c, i) { return '<option value="' + i + '">' + escHtml(c) + '</option>'; }).join("");
  guideRender();
}
// 다른 화면의 「쉬운 설명」 링크에서 호출: 설명집을 열고 해당 항목을 펼쳐 보여 준다.
function guideOpen(id) {
  switchTab("guide"); guideInit();
  document.getElementById("guideQ").value = ""; document.getElementById("guideCat").value = ""; guideRender();
  const d = document.getElementById("gd_" + id);
  if (d) { d.open = true; setTimeout(function () { if (d.scrollIntoView) d.scrollIntoView({ block: "start" }); }, 30); }
}
// ===== /쉬운 설명 =====


    // ===== 재무 스크리닝 (재무가 저장된 종목 안에서 ROIC·성장·FCF·가치로 거르기) =====
    // 자료: 저장된 재무(financial_raw) + 시장 DB의 최신 종가. 점수를 합산하지 않고 조건별 통과 수와 지표를 그대로 보여준다.
    // 시세가 없는 종목은 PER·PBR·FCF수익률이 N/A이며, 그 조건을 켜면 제외된다(대체값을 만들지 않는다).
    const FSC = { stocks: null, rows: null, priceDate: null, nPrice: 0, sortKey: 'roic3', sortDir: -1, shown: 100, open: null, loading: false };
    const FSC_FILTERS = [
      { k: 'roic3', g: '품질', label: 'ROIC(최근 3년 평균) ≥', unit: '%', def: 10, fmt: 'pct', op: '>=', tip: '투하자본 대비 세후영업이익. 업종·회계기준에 따라 수준이 크게 다름' },
      { k: 'roe3', g: '품질', label: 'ROE(최근 3년 평균) ≥', unit: '%', def: 8, fmt: 'pct', op: '>=', tip: '지배주주순이익 ÷ 평균 지배주주지분. 금융업은 ROIC 대신 이것으로 봅니다' },
      { k: 'opm', g: '품질', label: '영업이익률 ≥', unit: '%', def: 8, fmt: 'pct', op: '>=' },
      { k: 'fscore', g: '품질', label: 'F-Score(9점) ≥', unit: '점', def: 7, fmt: 'num0', op: '>=', tip: 'Piotroski(2000). 판정 가능한 신호가 8개 미만이면 N/A' },
      { k: 'revG3', g: '성장', label: '매출 3년 CAGR ≥', unit: '%', def: 10, fmt: 'pct', op: '>=' },
      { k: 'ebitG3', g: '성장', label: '영업이익 3년 CAGR ≥', unit: '%', def: 10, fmt: 'pct', op: '>=', tip: '시작·끝 값이 모두 양수일 때만 계산' },
      { k: 'fcfm', g: 'FCF', label: 'FCF 마진 ≥', unit: '%', def: 0, fmt: 'pct', op: '>=', tip: '0%면 FCF가 흑자인 종목' },
      { k: 'fcfni', g: 'FCF', label: 'FCF/순이익 ≥', unit: '배', def: 0.7, fmt: 'x2', op: '>=', tip: '순이익이 양수일 때만 계산' },
      { k: 'fcfPos', g: 'FCF', label: '최근 5년 중 FCF 흑자 연수 ≥', unit: '년', def: 4, fmt: 'num0', op: '>=' },
      { k: 'fcfy', g: '가치', label: 'FCF 수익률 ≥', unit: '%', def: 5, fmt: 'pct', op: '>=', tip: 'FCF ÷ (최신 종가 × 주식수). 시세가 있는 종목만' },
      { k: 'per', g: '가치', label: 'PER ≤', unit: '배', def: 15, fmt: 'x1', op: '<=', tip: '순이익이 양수일 때만 계산' },
      { k: 'pbr', g: '가치', label: 'PBR ≤', unit: '배', def: 1.5, fmt: 'x2', op: '<=' },
    ];
    const FSC_PRESETS = {
      none: { label: '전부 해제', on: {} },
      quality: { label: '품질+현금', on: { roic3: AN_TH.roicGood * 100, fcfm: 0, fcfni: AN_TH.fcfNiGood } },
      growth: { label: '성장+재투자', on: { revG3: AN_TH.growthHigh * 100, roic3: AN_TH.roicLow * 100 } },
      value: { label: '가치', on: { fcfy: 5, per: AN_TH.perGraham, roic3: AN_TH.roicLow * 100 } },
      finval: { label: '금융업 ROE+PBR', on: { roe3: 8, pbr: 1.0 }, sec: 'fin' },
    };
    function fscFmt(kind, v) {
      if (!anFin(v)) return 'N/A';
      return kind === 'pct' ? (v * 100).toFixed(1) + '%' : kind === 'x1' ? v.toFixed(1) + '배' : kind === 'x2' ? v.toFixed(2) + '배' : kind === 'num0' ? String(Math.round(v)) : String(v);
    }
    function fscMsg(t) { const e = document.getElementById('fscMsg'); if (e) e.textContent = t || ''; }
    const FSC_GUIDE = { roe3: 'roe', roic3: 'roic', opm: 'opm', fscore: 'fscore', revG3: 'cagr', ebitG3: 'cagr', fcfm: 'fcfm', fcfni: 'fcfni', fcfPos: 'fcfpos', fcfy: 'fcfy', per: 'per', pbr: 'pbr' };
    function fscInit() {
      const el = document.getElementById('fscFilters'); if (!el || el.getAttribute('data-ready')) return;
      el.setAttribute('data-ready', '1');
      let h = '', g = '';
      FSC_FILTERS.forEach(function (f) {
        if (f.g !== g) { g = f.g; h += '<div style="font-weight:700; font-size:12.5px; margin:10px 0 2px;">' + g + '</div>'; }
        h += '<label style="display:flex; align-items:center; gap:6px; font-size:13px; margin:3px 0;"' + (f.tip ? ' title="' + f.tip + '"' : '') + '><input type="checkbox" id="fscOn_' + f.k + '" onchange="fscRun()"> <span style="flex:1;">' + f.label + (FSC_GUIDE[f.k] ? ' <a href="#" onclick="guideOpen(&quot;' + FSC_GUIDE[f.k] + '&quot;); return false;" title="쉬운 설명" style="font-size:12px;">?</a>' : '') + '</span>' +
          '<input type="number" id="fscV_' + f.k + '" value="' + f.def + '" step="any" style="width:76px;" onchange="fscRun()"> <span style="width:22px; color:var(--text-muted);">' + f.unit + '</span></label>';
      });
      el.innerHTML = h;
      document.getElementById('fscPresets').innerHTML = Object.keys(FSC_PRESETS).map(function (k) { return '<button onclick="fscPreset(\\'' + k + '\\')">' + FSC_PRESETS[k].label + '</button>'; }).join(' ');
    }
    function fscPreset(k) {
      const p = FSC_PRESETS[k]; if (!p) return;
      FSC_FILTERS.forEach(function (f) {
        const on = Object.prototype.hasOwnProperty.call(p.on, f.k);
        document.getElementById('fscOn_' + f.k).checked = on;
        document.getElementById('fscV_' + f.k).value = on ? p.on[f.k] : f.def;
      });
      const secEl = document.getElementById('fscSec'); if (secEl) secEl.value = p.sec || '';
      fscRun();
    }
    // 한 종목의 지표를 계산한다. basis: 'ttm'이면 사업보고서보다 새로운 분기가 있을 때 최근 4분기 합산(TTM)을 최신 값으로 쓴다.
    function fscCompute(s, price, basis) {
      const list = anBuildSeries(s.rows);
      if (!list.length) return null;
      let ttm = null; try { ttm = anBuildTtmPoint(s.rows); } catch (e) { ttm = null; }
      const P = (basis === 'ttm' && ttm) ? ttm : list[list.length - 1];
      const out = { code: s.code, name: s.name, stock: s.stock || '', accMt: s.accMt, list: list, ttm: ttm, P: P, basisTxt: (P.isTTM ? 'TTM ' + P.year + '년 ' + P.q + 'Q' : P.year + '년 연간') };
      out.roic3 = anMean(anWin(list, 'roic', 3)); out.roicL = P.roic;
      out.roe3 = anMean(anWin(list, 'roe', 3)); out.roeL = P.roe; out.roa = anDiv(P.ni, P.avgAssets); out.lev = anDiv(P.assets, P.equity);
      out.opm = P.opm; out.fcfm = P.fcfm; out.fcfni = P.fcfni;
      out.revG3 = anCagr(list, 'revenue', 3); out.ebitG3 = anCagr(list, 'ebit', 3);
      const w5 = list.filter(function (d) { return d.year > list[list.length - 1].year - 5 && anFin(d.fcf); });
      out.fcfPos = w5.length >= 3 ? w5.filter(function (d) { return d.fcf > 0; }).length : null; out.fcfN = w5.length;
      const pio = anPiotroski(list); out.fscore = (pio && pio.known >= 8) ? pio.score : null;
      out.price = price; out.mcap = (price > 0 && anFin(P.shares) && P.shares > 0) ? price * P.shares : null;
      out.per = (out.mcap && P.ni > 0) ? out.mcap / P.ni : null;
      out.pbr = (out.mcap && P.equity > 0) ? out.mcap / P.equity : null;
      out.fcfy = (out.mcap && anFin(P.fcf)) ? P.fcf / out.mcap : null;
      return out;
    }
    async function fscLoad() {
      if (FSC.loading) return;
      FSC.loading = true;
      try {
        fscMsg('저장된 종목 목록을 확인하는 중...');
        const d0 = await (await fetch('/api/db/companies')).json();
        const comps = (d0.companies || []).filter(function (c) { return c.corp_code; });
        if (!comps.length) { fscMsg('저장된 재무 데이터가 없습니다.'); return; }
        const by = {};
        for (let i = 0; i < comps.length; i += 8) {
          fscMsg('재무 데이터 불러오는 중... ' + Math.min(i + 8, comps.length) + ' / ' + comps.length + '개 종목');
          const res = await fetch('/api/backtest/fin-batch?codes=' + encodeURIComponent(comps.slice(i, i + 8).map(function (c) { return c.corp_code; }).join(',')));
          const d = await res.json(); if (!res.ok) throw new Error(d.error || '불러오기 실패');
          (d.rows || []).forEach(function (r) { (by[r.corp_code] = by[r.corp_code] || []).push(r); });
        }
        FSC.stocks = comps.map(function (c) { return { code: c.corp_code, name: c.corp_name, stock: c.stock_code || '', accMt: c.acc_mt, ind: c.ind || '', rows: by[c.corp_code] || [] }; }).filter(function (s) { return s.rows.length; });
        fscMsg('최신 시세를 확인하는 중...');
        const px = {}; FSC.priceDate = null;
        const codes = FSC.stocks.map(function (s) { return s.stock; }).filter(Boolean);
        for (let i = 0; i < codes.length; i += 80) {
          const res = await fetch('/api/fin/last-price?codes=' + encodeURIComponent(codes.slice(i, i + 80).join(',')));
          const d = await res.json(); if (!res.ok) throw new Error(d.error || '시세 조회 실패');
          if (d.date) FSC.priceDate = d.date; Object.keys(d.prices || {}).forEach(function (k) { px[k] = d.prices[k]; });
        }
        FSC.px = px; fscRun(true);
        FSC.stocks.forEach(function (s) { if (s.ind) IND.map[s.stock] = s.ind; });
        fscIndOptions(); fscRender();
        const miss = FSC.stocks.map(function (s) { return s.stock; }).filter(function (c) { return c && !IND.map[c]; });
        if (miss.length) { fscMsg('업종을 받는 중... 0 / ' + miss.length + ' (종목당 1회만 받습니다)'); await indFill(miss, function (a, b) { fscMsg('업종을 받는 중... ' + a + ' / ' + b); fscIndOptions(); fscRender(); }); fscIndOptions(); fscRender(); }
      } catch (e) { fscMsg('오류: ' + (e.message || e)); } finally { FSC.loading = false; }
    }
    function fscRun(recompute) {
      if (!FSC.stocks) return;
      const basis = document.getElementById('fscBasis').value;
      if (recompute || FSC.basisUsed !== basis) {
        FSC.rows = FSC.stocks.map(function (s) { try { return fscCompute(s, FSC.px[s.stock] || null, basis); } catch (e) { return null; } }).filter(Boolean);
        FSC.basisUsed = basis; FSC.nPrice = FSC.rows.filter(function (r) { return r.price > 0; }).length;
      }
      FSC.shown = 100; FSC.open = null; fscRender();
    }
    function fscActive() {
      return FSC_FILTERS.filter(function (f) { return document.getElementById('fscOn_' + f.k).checked; }).map(function (f) {
        const raw = Number(document.getElementById('fscV_' + f.k).value); const th = f.unit === '%' ? raw / 100 : raw;
        return { f: f, th: th, raw: raw };
      });
    }
    function fscPass(r, a) { const v = r[a.f.k]; if (!anFin(v)) return null; return a.f.op === '>=' ? v >= a.th : v <= a.th; }
    function fscRender() { return guardedRender('재무 스크리닝 결과', 'fscOut', fscRenderImpl); }
    function fscRenderImpl() {
      const out = document.getElementById('fscOut'); if (!FSC.rows) return;
      FSC.rows.forEach(function (r) { r.indTxt = indLabel(IND.map[r.stock]) || '(미확인)'; r.sec = finSectorOf(IND.map[r.stock], r.name); r.secTxt = r.sec ? r.sec.n + (String(r.sec.basis).indexOf('이름') >= 0 ? '?' : '') : '일반'; });
      const act = fscActive();
      // 필터를 하나씩 순서대로 적용하며 남는 종목 수와 자료 없음으로 빠진 수를 센다.
      let cur = FSC.rows.slice(); const funnel = [{ t: '재무가 저장된 종목', n: cur.length, miss: 0 }];
      const indSel = document.getElementById('fscInd') ? document.getElementById('fscInd').value : '';
      if (indSel) { cur = cur.filter(function (r) { const g = indGroup(IND.map[r.stock]); return indSel === 'none' ? !g : g === indSel; }); funnel.push({ t: '업종: ' + (indSel === 'none' ? '미확인' : (IND_NAMES[indSel] || indSel)), n: cur.length, miss: 0 }); }
      const secSel = document.getElementById('fscSec') ? document.getElementById('fscSec').value : '';
      if (secSel) {
        const pick = { gen: function (x) { return !x; }, fin: function (x) { return !!(x && x.fin); }, bank: function (x) { return !!(x && (x.k === 'bank' || x.k === 'finhold')); }, insurer: function (x) { return !!(x && x.k === 'insurer'); }, broker: function (x) { return !!(x && x.k === 'broker'); }, credit: function (x) { return !!(x && x.k === 'credit'); }, invest: function (x) { return !!(x && (x.k === 'invest' || x.k === 'finsvc' || x.k === 'spac')); }, holding: function (x) { return !!(x && x.k === 'holding'); } }[secSel];
        cur = cur.filter(function (r) { return pick(r.sec); });
        funnel.push({ t: '유형: ' + (document.getElementById('fscSec').selectedOptions[0] ? document.getElementById('fscSec').selectedOptions[0].textContent : secSel), n: cur.length, miss: 0 });
      }
      act.forEach(function (a) {
        let miss = 0; const next = [];
        cur.forEach(function (r) { const p = fscPass(r, a); if (p === null) miss++; else if (p) next.push(r); });
        funnel.push({ t: a.f.label + ' ' + a.raw + a.f.unit, n: next.length, miss: miss }); cur = next;
      });
      const key = FSC.sortKey, dir = FSC.sortDir;
      cur.sort(function (x, y) { const a = x[key], b = y[key]; if (typeof a === 'string' || typeof b === 'string') { const c = String(a).localeCompare(String(b), 'ko'); return c ? dir * c : (x.name < y.name ? -1 : 1); } const fa = anFin(a), fb = anFin(b); if (!fa && !fb) return 0; if (!fa) return 1; if (!fb) return -1; return dir * (a - b) || (x.name < y.name ? -1 : 1); });
      FSC.result = cur;
      let h = '<div class="meta-line">시세 기준일 ' + (FSC.priceDate ? fmtYmd(FSC.priceDate) : '없음') + ' · 시세가 있는 종목 ' + FSC.nPrice + ' / ' + FSC.rows.length + '개 (없는 종목은 PER·PBR·FCF수익률이 N/A)</div>';
      h += '<div class="tscroll"><table class="an-tbl"><thead><tr><th class="an-first">단계</th><th>남은 종목</th><th>자료 없어 제외</th></tr></thead><tbody>' +
        funnel.map(function (s) { return '<tr><td class="an-first">' + s.t + '</td><td>' + s.n + '</td><td>' + (s.miss || '') + '</td></tr>'; }).join('') + '</tbody></table></div>';
      if (!act.length) h += '<div class="meta-line">조건을 하나도 켜지 않아 저장된 종목 전체를 보여줍니다.</div>';
      const cols = [['name', '종목', null], ['indTxt', '업종', null], ['secTxt', '유형', null], ['basisTxt', '최신 기준', null], ['roe3', 'ROE 3Y평균', 'pct'], ['roa', 'ROA', 'pct'], ['roic3', 'ROIC 3Y평균', 'pct'], ['roicL', 'ROIC 최신', 'pct'], ['opm', '영업이익률', 'pct'], ['revG3', '매출 3Y CAGR', 'pct'], ['ebitG3', '영업이익 3Y CAGR', 'pct'],
        ['fcfm', 'FCF 마진', 'pct'], ['fcfni', 'FCF/순이익', 'x2'], ['fcfPos', 'FCF 흑자(5년)', 'num0'], ['fscore', 'F-Score', 'num0'], ['fcfy', 'FCF 수익률', 'pct'], ['per', 'PER', 'x1'], ['pbr', 'PBR', 'x2']];
      h += '<div class="row" style="margin:8px 0;"><span class="meta-line">조건을 통과한 종목 <b>' + cur.length + '</b>개 · 열 제목을 누르면 정렬 · 종목을 누르면 아래에 모든 항목(연도별+TTM)</span> <button onclick="fscCsv()">CSV 저장</button></div>';
      h += '<div class="tscroll" style="max-height:70vh;"><table class="an-tbl" id="fscTbl"><thead><tr>' + cols.map(function (c) {
        return '<th' + (c[2] === null && c[0] === 'name' ? ' class="an-first"' : '') + ' style="cursor:pointer;" onclick="fscSort(\\'' + c[0] + '\\')">' + c[1] + (FSC.sortKey === c[0] ? (FSC.sortDir < 0 ? ' ▼' : ' ▲') : '') + '</th>'; }).join('') + '</tr></thead><tbody>';
      cur.slice(0, FSC.shown).forEach(function (r) {
        h += '<tr data-code="' + r.code + '" style="cursor:pointer;" onclick="fscToggle(\\'' + r.code + '\\')">' + cols.map(function (c, i) {
          if (i === 0) return '<td class="an-first">' + escHtml(r.name) + (r.accMt && r.accMt !== 12 ? ' <span title="12월 결산이 아님">⚠</span>' : '') + '</td>';
          if (c[0] === 'basisTxt') return '<td>' + r.basisTxt + '</td>';
          if (c[0] === 'secTxt') return '<td title="업종코드(KSIC)와 이름으로 분류. 이름으로 추정한 경우 물음표(?)가 붙습니다. 금융업·지주회사는 ROIC·FCF 대신 ROE·PBR로 봅니다">' + escHtml(r.secTxt) + '</td>';
          if (c[0] === 'indTxt') return '<td title="한국표준산업분류(KSIC) 중분류 기준. 거래소·네이버 업종과 다릅니다">' + escHtml(r.indTxt) + '</td>';
          return '<td>' + fscFmt(c[2], r[c[0]]) + '</td>'; }).join('') + '</tr>';
        if (FSC.open === r.code) h += '<tr><td colspan="' + cols.length + '" style="background:var(--card-bg, #fff); padding:8px 4px;">' + fscDetailHtml(r) + '</td></tr>';
      });
      h += '</tbody></table></div>';
      if (cur.length > FSC.shown) h += '<button onclick="FSC.shown += 100; fscRender();" style="margin-top:6px;">더 보기 (' + (cur.length - FSC.shown) + '개 남음)</button>';
      out.innerHTML = h;
      fscMsg('');
    }
    function fscIndOptions() {
      const sel = document.getElementById('fscInd'); if (!sel || !FSC.rows) return;
      const cur = sel.value, cnt = {};
      FSC.rows.forEach(function (r) { if (r.stock && IND.map[r.stock]) IND.map[r.stock] = IND.map[r.stock]; const g = indGroup(IND.map[r.stock]); if (g) cnt[g] = (cnt[g] || 0) + 1; });
      const un = FSC.rows.filter(function (r) { return !indGroup(IND.map[r.stock]); }).length;
      sel.innerHTML = '<option value="">업종: 전체</option>' + Object.keys(cnt).sort(function (a, b) { return cnt[b] - cnt[a] || (a < b ? -1 : 1); }).map(function (g) { return '<option value="' + g + '">' + escHtml(IND_NAMES[g] || ('KSIC ' + g)) + ' (' + cnt[g] + ')</option>'; }).join('') + (un ? '<option value="none">업종 미확인 (' + un + ')</option>' : '');
      sel.value = cur && Array.prototype.some.call(sel.options, function (o) { return o.value === cur; }) ? cur : '';
    }
    function fscSort(k) { if (FSC.sortKey === k) FSC.sortDir = -FSC.sortDir; else { FSC.sortKey = k; FSC.sortDir = (k === 'per' || k === 'pbr' || k === 'name' || k === 'indTxt') ? 1 : -1; } fscRender(); }
    function fscToggle(code) { FSC.open = FSC.open === code ? null : code; fscRender(); }
    const FSC_DETAIL = [
      ['revenue', '매출액', 'eok'], ['ebit', '영업이익', 'eok'], ['ni', '순이익', 'eok'], ['opm', '영업이익률', 'pct'], ['gm', '매출총이익률', 'pct'],
      ['ocf', '영업현금흐름(CFO)', 'eok'], ['capex', 'CAPEX', 'eok'], ['fcf', 'FCF', 'eok'], ['fcfm', 'FCF 마진', 'pct'], ['fcfni', 'FCF/순이익', 'x'],
      ['roic', 'ROIC', 'pct'], ['roe', 'ROE', 'pct'], ['revenueYoy', '매출 증가율', 'pct'], ['ebitYoy', '영업이익 증가율', 'pct'], ['fcfYoy', 'FCF 증가율', 'pct'],
      ['de', '부채비율', 'x'], ['nde', '순차입금/EBITDA', 'x'], ['ccc', '현금전환주기(일)', 'days'],
    ];
    function fscDetailHtml(r) {
      const cols = r.list.slice(-8).concat(r.ttm ? [r.ttm] : []);
      const th = function (d) { return d.isTTM ? '<th style="background:#b45309;" title="최근 4개 분기 합산(손익·현금흐름), 재무상태표는 해당 분기말">' + d.label + '</th>' : '<th>' + d.year + '</th>'; };
      const body = FSC_DETAIL.map(function (df) { return '<tr><td class="an-first">' + df[1] + '</td>' + cols.map(function (d) { return '<td' + (d.isTTM ? ' style="background:#fffbeb;"' : '') + '>' + anFmt(df[2], d[df[0]]) + '</td>'; }).join('') + '</tr>'; }).join('');
      return '<div class="row" style="margin-bottom:6px;"><b>' + escHtml(r.name) + '</b> <span class="meta-line">' + r.stock + ' · 업종 ' + escHtml(r.indTxt) + ' · 최신 기준 ' + r.basisTxt + (r.price ? ' · 종가 ' + anWon(r.price) : ' · 시세 없음') + '</span> ' +
        '<button class="primary" onclick="fscGoAnalyze(\\'' + r.code + '\\')">종목분석 ›</button></div>' +
        '<div class="tscroll" style="max-height:55vh;"><table class="an-tbl"><thead><tr><th class="an-first">항목 (금액 단위 억원)</th>' + cols.map(th).join('') + '</tr></thead><tbody>' + body + '</tbody></table></div>' +
        '<div class="meta-line">연결/별도 기준은 저장된 값 그대로입니다. TTM 열은 사업보고서보다 새로운 분기가 저장돼 있고 4개 분기가 모두 있을 때만 나옵니다.</div>';
    }
    function fscGoAnalyze(code) {
      const s = (FSC.stocks || []).filter(function (x) { return x.code === code; })[0]; if (!s) return;
      document.getElementById('corpName').value = s.name; try { updateWatchButtonState(); } catch (e) {}
      switchTab('financial'); analyzeStock();
    }
    function fscCsv() {
      if (!FSC.result) return;
      const cols = ['name', 'stock', 'indTxt', 'secTxt', 'basisTxt', 'roe3', 'roa', 'roic3', 'roicL', 'opm', 'revG3', 'ebitG3', 'fcfm', 'fcfni', 'fcfPos', 'fscore', 'fcfy', 'per', 'pbr'];
      const lines = [cols.join(',')];
      FSC.result.forEach(function (r) { lines.push(cols.map(function (k) { const v = r[k]; return '"' + (anFin(v) ? String(Math.round(v * 10000) / 10000) : v == null ? '' : String(v).split('"').join('""')) + '"'; }).join(',')); });
      const a = document.createElement('a'); a.href = URL.createObjectURL(new Blob(['﻿' + lines.join('\\n')], { type: 'text/csv;charset=utf-8' })); a.download = 'fin_screening.csv'; a.click();
    }
    // ===== /재무 스크리닝 =====

// ===== 종목분석 접기·펼치기 =====
// 장(章) 머리글을 누르면 그 장에 속한 항목(details)을 모두 펼치거나 접는다. 처음에는 요약·분류·투자 판단 요약만 펼치고 나머지는 접어,
// 첫 화면이 길지 않게 한다. 사용자가 연 항목은 같은 종목을 다시 그릴 때(가격 입력 등) 그대로 유지한다.
const AN_UI = { key: null, open: {} };
const AN_KEEP_OPEN = ['🏷 종합 분류', '🧭 투자 판단 요약', '🏛 ', '📈 수익성 —', '💹 가격 —', '🧩 지주회사 —']; // 뒤 4개는 금융업·지주회사 화면의 핵심 섹션
function anChapItems(chap) {
  const out = []; let n = chap.nextElementSibling;
  while (n && !(n.classList && n.classList.contains('an-chap'))) { if (n.tagName === 'DETAILS') out.push(n); n = n.nextElementSibling; }
  return out;
}
function anChapUpdateLabel(chap) {
  const it = anChapItems(chap), st = chap.querySelector('.an-chap-st'); if (!st) return;
  const allOpen = it.length > 0 && it.every((d) => d.open);
  st.textContent = !it.length ? '' : allOpen ? '▾ 항목 접기' : '▸ 항목 ' + it.length + '개 펼치기';
}
function anChapToggle(chap) {
  const it = anChapItems(chap); if (!it.length) return;
  const open = !it.every((d) => d.open);
  it.forEach((d) => { d.open = open; });
  anChapUpdateLabel(chap);
}
function anSecTitle(d) { const s = d.querySelector('summary'); return s ? s.textContent.trim() : ''; }
function anFoldCapture(el, key) {
  if (AN_UI.key !== key) { AN_UI.key = key; AN_UI.open = {}; return; }
  Array.prototype.forEach.call(el.children, (d) => { if (d.tagName === 'DETAILS') AN_UI.open[anSecTitle(d)] = d.open; });
}
function anFoldApply(el) {
  Array.prototype.forEach.call(el.children, (d) => {
    if (d.tagName !== 'DETAILS') return;
    const t = anSecTitle(d);
    if (Object.prototype.hasOwnProperty.call(AN_UI.open, t)) d.open = !!AN_UI.open[t];
    else d.open = AN_KEEP_OPEN.some((k) => t.indexOf(k) === 0);
    d.addEventListener('toggle', () => { const ch = anChapOf(d); if (ch) anChapUpdateLabel(ch); });
  });
  Array.prototype.forEach.call(el.querySelectorAll(':scope > .an-chap'), anChapUpdateLabel);
}
function anChapOf(d) { let n = d.previousElementSibling; while (n) { if (n.classList && n.classList.contains('an-chap')) return n; n = n.previousElementSibling; } return null; }
// ===== /종목분석 접기·펼치기 =====
function anBuildNav() {
  const el = document.getElementById('analysis'); if (!el) return;
  const ds = Array.prototype.slice.call(el.children).filter(function (x) { return x.tagName === 'DETAILS'; });
  if (ds.length < 3) return;
  const nav = document.createElement('div');
  nav.className = 'an-sticky';
  const mk = function (txt, fn, strong) { const b = document.createElement('button'); b.type = 'button'; b.textContent = txt; b.style.cssText = 'font-size:12px; padding:3px 8px; height:auto; ' + (strong ? 'font-weight:700;' : ''); b.onclick = fn; nav.appendChild(b); };
  mk('전체 펼치기', function () { ds.forEach(function (d) { d.open = true; }); }, true);
  mk('전체 접기', function () { ds.forEach(function (d) { d.open = false; }); }, true);
  { const jd = ds.filter(function (d) { const sm = d.querySelector('summary'); return sm && sm.textContent.indexOf('투자 판단 요약') >= 0; })[0]; if (jd) mk('🧭 결론 먼저', function () { jd.open = true; jd.scrollIntoView({ behavior: 'smooth', block: 'start' }); }, true); }
  Array.prototype.slice.call(el.children).forEach(function (d) {
    if (d.classList && d.classList.contains('an-chap')) { const sp = document.createElement('span'); sp.textContent = d.getAttribute('data-short').split(' ')[0]; sp.title = d.getAttribute('data-short'); sp.style.cssText = 'font-weight:800; margin-left:6px; cursor:pointer;'; sp.onclick = function () { d.scrollIntoView({ behavior: 'smooth', block: 'start' }); }; nav.appendChild(sp); return; }
    if (d.tagName !== 'DETAILS') return;
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
  const L = c.list.slice(-10).concat(c.ttm ? [c.ttm] : []);
  const have = L.filter(function (d) { return d.r4; });
  const desc = 'ROIC = <b>매출총이익률 × 총자산회전율 × 영업비용 통제력(NOPAT/매출총이익) × 투하자본 배수(총자산/투하자본)</b>. 네 항을 곱하면 NOPAT÷투하자본(ROIC)이 되는 항등식이며, 모든 항에 같은 평균(기초·기말) 기준을 써서 오차 없이 맞습니다(아래 검산 행). ①×②는 Novy-Marx(2013)의 GPA입니다. 행 이름을 <b>클릭하면 아래에 추이 차트</b>가 추가/제거됩니다.';
  if (!have.length) return anSection('🧩 ROIC 4단계 분해 (시계열)', '#0f766e', desc, '<div style="font-size:12.5px;">계산할 수 있는 연도가 없습니다. 매출총이익(매출원가)이 공시되지 않는 회사(영업비용만 공시하는 회사·금융업)이거나 투하자본 항목이 저장되지 않은 경우입니다(재조회 필요할 수 있음).</div>', true);
  const head = '<tr><th class="an-first">항목 (클릭 → 차트)</th>' + L.map(function (d) { return d.isTTM ? '<th style="background:#b45309;" title="사업보고서가 아직 없는 최신 분기 기준 최근 4개 분기 합산(TTM)">' + d.label + '</th>' : '<th>' + d.year + '</th>'; }).join('') + '</tr>';
  const body = AN_R4ROWS.map(function (rw) {
    return '<tr data-r4="' + rw.key + '" onclick="anR4Toggle(' + "'" + rw.key + "'" + ')" style="cursor:pointer;" title="' + rw.tip + ' — 클릭하면 차트"><td class="an-first"><span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:' + rw.col + ';margin-right:5px;"></span>' + rw.label + '</td>' + L.map(function (d) { return '<td>' + anFmt(rw.fmt, d[rw.key]) + '</td>'; }).join('') + '</tr>';
  }).join('') + '<tr style="color:var(--text-muted);"><td class="an-first">검산: ①×②×③×④ − ROIC (0이어야 함)</td>' + L.map(function (d) { return '<td>' + (anFin(d.r4chk) ? (Math.abs(d.r4chk) < 1e-9 ? '0' : d.r4chk.toExponential(1)) : 'N/A') + '</td>'; }).join('') + '</tr>';
  const table = '<div class="tscroll" style="max-height:60vh;"><table class="an-tbl"><thead>' + head + '</thead><tbody>' + body + '</tbody></table></div>';
  let ttmNote = '';
  if (!c.ttm) { let dg = null; try { dg = ttmDiagnose(rawRows); } catch (e) { dg = null; } if (dg) ttmNote = '<div style="font-size:12px; margin-top:6px; padding:6px 10px; background:#fffbeb; border:1px solid #fcd34d; border-radius:8px;"><b>TTM 열이 없는 이유</b>: ' + dg + '</div>'; }
  else if (!c.ttm.r4) {
    const tr = c.ttm.r || {}; const why = [];
    if (grossProfitOf(tr) == null) { let dg2 = null; try { dg2 = ttmDiagnose(rawRows); } catch (e) { dg2 = null; } why.push('TTM 매출총이익을 알 수 없음' + (dg2 ? ' — ' + dg2 : '(최근 4분기 합산 항목은 모두 있으나 매출원가가 영업비용 전체로 공시되어 매출총이익이 영업이익과 같아지는 회사일 수 있음)')); }
    if (computeIC(tr) == null || !(computeIC(tr) > 0)) why.push('투하자본 항목(채권·재고·유형자산 등)이 최신 분기에 없음');
    if (tr.total_equity == null || tr.total_liabilities == null) why.push('총자산(자본+부채) 없음');
    ttmNote = '<div style="font-size:12px; margin-top:6px; padding:6px 10px; background:#fffbeb; border:1px solid #fcd34d; border-radius:8px;"><b>' + c.ttm.label + ' ROIC 4단계가 N/A인 이유</b>: ' + (why.length ? why.join(' / ') : '1년 전 같은 분기 자료의 총자산/자본이 없어 평균 기준을 맞출 수 없음(해당 분기를 다시 조회하면 채워집니다)') + '</div>';
  }
  const note = ttmNote + '<div style="font-size:11.5px; color:var(--text-muted); margin-top:6px; line-height:1.5;">해석 시 주의: ③은 (영업이익/매출총이익)×(1−유효세율)이라 세금 영향이 섞입니다. 성장 투자(판관비·R&D)가 큰 회사는 ①이 높고 ③이 낮게 나올 수 있으나 <b>이것만으로 우량 성장주라고 단정할 수는 없고</b>, 투자 성과(매출 성장·시간에 따른 ③의 회복)를 함께 봐야 합니다. ④는 재무 레버리지가 아니라 총자산 중 영업에 투입된 자본의 비중이며, 현금·금융자산이 많거나 매입채무 등 무이자 영업부채가 클수록 커집니다. 매출총이익을 알 수 없는 회사는 N/A입니다.</div>';
  return anSection('🧩 ROIC 4단계 분해 (시계열)', '#0f766e', desc, table + '<div id="anR4Chart"></div>' + note, true);
}
function anR4Toggle(key) {
  const i = AN_R4SEL.indexOf(key);
  if (i >= 0) AN_R4SEL.splice(i, 1); else { if (AN_R4SEL.length >= 4) AN_R4SEL.shift(); AN_R4SEL.push(key); }
  anR4Draw();
}
function anR4Draw() {
  const c = AN_CTX; const box = document.getElementById('anR4Chart'); if (!box || !c) return;
  const L = c.list.slice(-10).concat(c.ttm ? [c.ttm] : []);
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
      svg += '<text x="' + X(i).toFixed(1) + '" y="' + (H - 7) + '" font-size="10" text-anchor="middle" fill="#6b7280">' + (d.isTTM ? 'TTM' + d.q + 'Q' : String(d.year).slice(2)) + '</text>';
      if (vals[i] != null) svg += '<circle cx="' + X(i).toFixed(1) + '" cy="' + Y(vals[i]).toFixed(1) + '" r="3.2" fill="' + rw.col + '"><title>' + (d.isTTM ? d.label : d.year) + ': ' + anFmt(rw.fmt, vals[i]) + '</title></circle><text x="' + X(i).toFixed(1) + '" y="' + (Y(vals[i]) - 7).toFixed(1) + '" font-size="9.5" text-anchor="middle" fill="' + rw.col + '">' + anFmt(rw.fmt, vals[i]) + '</text>';
    });
    svg += '</svg>';
    return '<div style="margin:10px 0 2px; font-size:12.5px; font-weight:600; color:' + rw.col + ';">' + rw.label + ' <span style="font-weight:400; color:var(--text-muted);">— ' + rw.tip + '</span> <button type="button" onclick="anR4Toggle(' + "'" + key + "'" + ')" style="font-size:11px; padding:0 6px; height:auto;">닫기 ×</button></div>' + svg;
  }).join('');
}

function anRdcfHtml(c) {
  const R = c.rdcf;
  const head = '<div style="font-weight:700; margin:12px 0 4px;">🔄 역DCF — 지금 가격이 담고 있는 성장 기대 ' + anBadge('practice', '실무 관행') + '</div>';
  if (!R) {
    return head + '<div style="font-size:12.5px; color:var(--text-muted); line-height:1.5;">N/A — 최근 3년 평균 FCF(주당)가 0 이하이거나 자료가 부족해 계산하지 않습니다. 현금흐름이 아직 음수인 성장 기업은 이 방식으로 가격을 읽을 수 없으므로, 위 이익 기준 내재성장률이나 시나리오를 참고하세요.</div>';
  }
  const hist = [['FCF 5년 CAGR', anCagr(c.list.map((d) => ({ year: d.year, fcf: d.fcf })), 'fcf', 5)], ['FCF 3년 CAGR', anCagr(c.list.map((d) => ({ year: d.year, fcf: d.fcf })), 'fcf', 3)], ['지속가능성장률(ROE×유보율)', c.gRoe], ['ROIC×재투자율', c.gRoic]];
  const histTxt = hist.map((h) => h[0] + ' <b>' + anPct(h[1]) + '</b>').join(' · ');
  const hl = (g) => { const base = c.rdcf.main; return (g != null && base != null && Math.abs(g - base) < 0.0005) ? 'background:#dbeafe; font-weight:700;' : ''; };
  const tbl = '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th>요구수익률 r ↓ / 영구성장 gT →</th>' + R.gts.map((g) => '<th>' + anPct(g, 0) + '</th>').join('') + '</tr></thead><tbody>' +
    R.rs.map((rr, i) => '<tr><td>' + anPct(rr, 1) + (Math.abs(rr - R.r) < 1e-9 ? ' (기준)' : '') + '</td>' + R.grid[i].map((g, j) => '<td style="' + (Math.abs(rr - R.r) < 1e-9 && Math.abs(R.gts[j] - R.gT) < 1e-9 ? 'background:#dbeafe; font-weight:700;' : '') + '">' + (g == null ? '해 없음' : anPct(g)) + '</td>').join('') + '</tr>').join('') + '</tbody></table></div>';
  return head +
    anGrid([anTile('시장이 요구하는 향후 ' + R.N + '년 연평균 FCF 성장률', anPct(R.main), 'r ' + anPct(R.r, 1) + ' · 이후 영구성장 ' + anPct(R.gT, 0) + ' · 기준 FCF/주 ' + anWon(R.fps) + '(최근 3년 평균)'),
      anTile('기간을 바꾸면 (r·gT 고정)', R.byN.map((x) => x.n + '년 ' + anPct(x.g)).join(' · '), '기간이 길수록 요구 성장률은 낮아집니다')]) +
    '<div style="font-size:12.5px; line-height:1.55; margin:4px 0;">비교 기준(과거 실적·이론): ' + histTxt + '</div>' +
    '<div style="font-size:11.5px; color:var(--text-muted); margin:6px 0 2px;">아래 표: r과 gT를 바꿨을 때 요구 성장률(칸마다 같은 가격을 맞추는 값). 가정이 조금만 변해도 크게 움직이면 그 숫자는 불안정하다는 뜻입니다.</div>' + tbl +
    '<div style="font-size:11.5px; color:var(--text-muted); margin-top:6px; line-height:1.5;">읽는 법: 요구 성장률이 과거 실적·지속가능성장률보다 훨씬 높으면 가격에 낙관이 많이 담겨 있고, 훨씬 낮으면 비관이 담겨 있다는 <b>해석의 출발점</b>입니다. 목표주가가 아니며 저평가·고평가를 단정하지 않습니다. FCF는 순환적이고 자본배분·일회성에 민감하며, 영구성장 구간이 가치의 큰 부분을 차지합니다.</div>';
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
    tiles + imp + anRdcfHtml(c) + rtab, true);
}

function anRenderAreas(c) {
  const L = c.list; const out = {};
  // Growth
  {
    const kinds = [['revenue', '매출액'], ['gp', '매출총이익'], ['ebit', '영업이익'], ['eps', 'EPS'], ['fcf', 'FCF']];
    const cagrRows = kinds.map(([k, n]) => '<tr><td class="an-first">' + n + '</td><td>' + anPct(anCagr(L, k, 3)) + '</td><td>' + anPct(anCagr(L, k, 5)) + '</td><td>' + anPct(anCagr(L, k, 10)) + '</td><td>' + anPct(L[L.length - 1][k + 'Yoy']) + '</td></tr>').join('');
    const splitWarn = L.some((d) => anFin(d.shareChg) && Math.abs(d.shareChg) > 0.3) ? '<div style="font-size:11.5px; color:#b45309; margin-top:4px;">⚠ 유통주식수가 한 해에 30% 넘게 변한 해가 있습니다(액면분할·증자 등). EPS 성장률이 왜곡될 수 있으니 순이익 기준도 함께 보세요.</div>' : '';
    const qg = c.qg;
    const qtab = qg ? '<div style="font-weight:600; font-size:12.5px; margin-top:10px;">최근 3개 분기 YoY와 가속 여부</div><div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">항목</th>' + qg.labels.map((x) => '<th>' + x + '</th>').join('') + '<th>가속도</th></tr></thead><tbody>' +
      qg.rows.map((x) => '<tr><td class="an-first">' + x.name + '</td>' + x.g.map((v) => '<td>' + anPct(v) + '</td>').join('') + '<td style="font-weight:700; color:' + (x.accel === '가속' ? '#16a34a' : x.accel === '둔화' ? '#dc2626' : 'inherit') + ';">' + (x.accel || 'N/A') + '</td></tr>').join('') + '</tbody></table></div><div style="font-size:11px; color:var(--text-muted);">가속도: 최신 분기 YoY가 직전 두 분기 평균보다 3%p 넘게 높으면 가속, 낮으면 둔화, 아니면 유지. 전년 동기가 0 이하인 경우 N/A.</div>' : '';
    out.growth = (anSection('🌱 Growth (성장)', '#ea580c', 'CAGR은 최근 연간 값과 N년 전 연간 값이 모두 양수일 때만 계산합니다(적자 구간 N/A). 성장 가속은 이익의 모멘텀(Chan·Jegadeesh·Lakonishok 1996 earnings momentum)과 연결됩니다.',
      '<div style="overflow-x:auto;"><table class="an-tbl"><thead><tr><th class="an-first">항목</th><th>3Y CAGR</th><th>5Y CAGR</th><th>10Y CAGR</th><th>최근 YoY</th></tr></thead><tbody>' + cagrRows + '</tbody></table></div>' + splitWarn + qtab +
      '<div style="font-weight:600; font-size:12.5px; margin-top:10px;">연도별 YoY</div>' + anTable(L, kinds.map(([k, n]) => ({ label: n + ' YoY', key: k + 'Yoy', fmt: 'pct' })), 10), false));
  }
  // Quality
  out.quality = (anSection('🏆 Quality (수익성·이익의 질)', '#7c3aed', 'Gross Profitability(Novy-Marx 2013), 발생액(Sloan 1996: 현금 뒷받침 없는 이익은 지속성이 낮음), ROE/ROIC 평균·변동. ROIC = 영업이익×(1−유효세율; 범위 밖이면 24%) ÷ 평균 투하자본(영업운전자본 + 유형·무형·사용권자산 + 영업권, 차입금·현금 제외).',
    anRoeStats(c) + anGrid([
      anTile('ROIC 10Y 평균', anPct(anMean(anWin(L, 'roic', 10))), anPeriodTxt(anPeriod(L, 'roic', 10))), anTile('ROIC 5Y 평균', anPct(c.roic5), anPeriodTxt(anPeriod(L, 'roic', 5))), anTile('ROIC 3Y 평균', anPct(c.roic3), anPeriodTxt(anPeriod(L, 'roic', 3))),
    ]) + anTable(L, [
      { label: 'ROE', key: 'roe', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }, { label: 'Gross Profitability (매출총이익/총자산)', key: 'gpa', fmt: 'pct' },
      { label: '영업이익률', key: 'opm', fmt: 'pct' }, { label: 'FCF 마진', key: 'fcfm', fmt: 'pct' }, { label: 'FCF/순이익', key: 'fcfni', fmt: 'x' }, { label: 'CFO/순이익', key: 'cfoni', fmt: 'x' },
      { label: '발생액(Accrual, 낮을수록 좋음)', key: 'accrual', fmt: 'pct' }, { label: '자산회전율', key: 'at', fmt: 'x' }], 10), false));
  // Capital efficiency
  const lastD = c.last; const ipR = (lastD && lastD.r && anFin(lastD.r.investment_property) && anFin(lastD.ic) && lastD.ic > 0) ? lastD.r.investment_property / lastD.ic : null;
  const ipWarn = (ipR != null && ipR >= 0.1) ? ' <b style="color:#b45309;">⚠ 투자부동산이 투하자본의 ' + anPct(ipR, 0) + '에 해당합니다. 투하자본(ROIC 분모)에서는 영업 외 자산으로 보아 제외했는데, 임대수익이 영업이익에 들어 있는 회사라면 ROIC가 과대 계산됩니다.</b>' : '';
  out.capeff = (anSection('⚙ Capital Efficiency (자본 효율 · 증분 수익)', '#0d9488', '"추가로 넣은 1원이 얼마를 버는가". 증분 ROIC = 최근 3년 ΔNOPAT ÷ 같은 기간(1년 선행) ΔInvested Capital (Mauboussin·Koller 방식). 투하자본이 늘지 않은 해는 N/A. 재투자율 = ΔIC ÷ NOPAT, 성장 = 재투자율 × ROIC (Damodaran). 투하자본은 영업 관점(영업운전자본 + 유형·무형·사용권자산 + 영업권)이며 현금·금융자산·투자부동산은 영업 외 자산으로 보아 제외합니다(Koller et al., Valuation).' + ipWarn,
    anTable(L, [{ label: 'NOPAT', key: 'nopat', fmt: 'eok' }, { label: '투하자본(IC)', key: 'ic', fmt: 'eok' }, { label: 'ΔIC (전년 대비)', key: 'dIC', fmt: 'eok' }, { label: '재투자율 (ΔIC/NOPAT)', key: 'reinv', fmt: 'pct' },
      { label: '증분 ROIC (3Y)', key: 'incRoic', fmt: 'pct' }, { label: '증분 ROE (3Y, ΔNI/Δ자본)', key: 'incRoe', fmt: 'pct' }, { label: 'ROIC', key: 'roic', fmt: 'pct' }], 10), false));
  // Cash quality
  out.cash = (anSection('💵 Cash Quality (현금흐름의 질)', '#0891b2', 'FCF = 영업CF − CAPEX(유형+무형 취득). 운전자본 = (매출채권+기타채권+재고) − (매입채무+기타채무). 현금전환주기(CCC)는 외상 대금을 받고, 재고를 팔고, 매입 대금을 치르는 데 걸리는 일수의 합입니다(짧을수록 자금이 덜 묶임, 업종마다 크게 다름).',
    anTable(L, [{ label: 'CFO (영업CF)', key: 'ocf', fmt: 'eok' }, { label: 'FCF', key: 'fcf', fmt: 'eok' }, { label: 'FCF 마진', key: 'fcfm', fmt: 'pct' }, { label: 'FCF/순이익', key: 'fcfni', fmt: 'x' }, { label: 'CFO/순이익', key: 'cfoni', fmt: 'x' },
      { label: '발생액(Accrual)', key: 'accrual', fmt: 'pct' }, { label: 'CAPEX', key: 'capex', fmt: 'eok' }, { label: 'CAPEX/매출', key: 'capexRev', fmt: 'pct' }, { label: '운전자본', key: 'wc', fmt: 'eok' }, { label: '운전자본/매출', key: 'wcRev', fmt: 'pct' },
      { label: '매출채권회전일 DSO', key: 'dso', fmt: 'days' }, { label: '재고회전일 DIO', key: 'dio', fmt: 'days' }, { label: '매입채무지급일 DPO', key: 'dpo', fmt: 'days' }, { label: '현금전환주기 CCC (DSO+DIO−DPO)', key: 'ccc', fmt: 'days' }], 10), false));
  // BS
  out.bs = (anSection('🛡 Balance Sheet (재무 안정성)', '#475569', '차입금 = 단기차입금 + 유동성장기부채 + 장기차입금 + 사채(리스부채 제외). 순차입금 = 차입금 − (현금 + 단기금융상품). 차입금·EBITDA 항목은 <b>재조회 후</b>에 채워집니다.',
    anTable(L, [{ label: '부채비율 (부채/자본)', key: 'de', fmt: 'pct' }, { label: '차입금/자본', key: 'ibde', fmt: 'pct' }, { label: '차입금', key: 'borrow', fmt: 'eok' }, { label: '순차입금', key: 'netDebt', fmt: 'eok' },
      { label: '순차입금/EBITDA', key: 'nde', fmt: 'x' }, { label: '이자보상배율', key: 'icov', fmt: 'x' }, { label: '유동비율', key: 'cur', fmt: 'x' }, { label: '현금/차입금', key: 'cashDebt', fmt: 'x' }], 10), false));
  // Capital allocation
  out.alloc = (anSection('🏦 Capital Allocation (자본 배분)', '#b45309', '이익을 배당·자사주·투자(CAPEX·R&D 대용) 중 어디에 쓰는가. (M&A·차입금 상환은 공시 계정명이 회사마다 달라 정확히 구분할 수 없어 뺐습니다.) 금액은 현금흐름표 기준(재조회 후 채워짐). 주식수 변화가 음수면 자사주 소각/매입으로 주식수가 줄어든 것(주주환원 효과, Ikenberry et al. 1995).',
    anGrid([anTile('평균 배당성향', anPct(c.m.avgPayout), '기간: ' + anPeriodTxt(anPeriod(L, 'payout', 10)).replace('기간: ', ''))]) +
    anTable(L, [{ label: '배당성향 (DPS/EPS)', key: 'payout', fmt: 'pct' }, { label: '배당금 지급', key: 'divPaid', fmt: 'eok' }, { label: '자사주 매입', key: 'buyback', fmt: 'eok' }, { label: '총주주환원성향 (배당+자사주)/순이익', key: 'shp', fmt: 'pct' },
      { label: '유통주식수 변화(YoY)', key: 'shareChg', fmt: 'pct' }, { label: 'CAPEX', key: 'capex', fmt: 'eok' }, { label: 'CAPEX/감가상각', key: 'capexDa', fmt: 'x' },
      { label: 'R&D 대용(무형자산 취득)', key: 'rd', fmt: 'eok' }], 10), false));
  return out;
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
    rows.push({ label: p.label, n, trimmed, top: groups[G - 1].slice().reverse().map((x) => ({ id: x.id, s: x.s, f: x.f, v: x.v })), gm, uni, ic, spread: gm[G - 1] - gm[0], excess: gm[G - 1] - uni, excessNet: topNet - uni, topNet, turn, bench: p.bench });
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
  const inner = '<div style="font-size:12px; line-height:1.55; margin:4px 0;">종목분석의 등급 기준을 바꿀 수 있습니다. 바꾼 값은 이 브라우저에 저장되고 즉시 모든 분류에 반영됩니다. 어떤 값이 더 나은지는 「백테스트」로 확인하세요(기준을 바꿔 가며 고르면 과최적화 위험이 있어 백테스트가 시도 횟수를 세어 보정합니다).</div>' +
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

// 결산월이 12월이 아닌 법인은 날짜 기반 값(공시일·기간말 주가 등)을 믿을 수 없어 백테스트에서 뺀다. 결산월을 아직 모르는 종목은 12월로 가정해 포함하되 개수를 알린다.
function btSplitFiscal(comps) {
  const use = [], excluded = []; let unknownN = 0;
  comps.forEach((c) => {
    if (c.acc_mt == null) { unknownN++; use.push(c); }
    else if (Number(c.acc_mt) === 12) use.push(c);
    else excluded.push(c);
  });
  return { use: use, excluded: excluded, unknownN: unknownN };
}
function fiscalBanner(accMt) {
  if (accMt == null || Number(accMt) === 12) return '';
  return '<div class="card" style="background:#fef2f2; border-color:#fca5a5; font-size:12.5px;">⚠ 이 회사는 <b>결산월이 ' + Number(accMt) + '월</b>입니다(12월 결산 아님). 재무제표 시계열(연간·분기·TTM)은 보고서 기준이라 그대로 볼 수 있지만, <b>날짜에 의존하는 값</b>(공시시점 주가·PER·PBR·FCF Yield, 기간말 주가, 공시 후 주가 등락)은 12월 결산을 전제로 만들어져 <b>이 종목에서는 비어 있거나 부정확할 수 있습니다</b>. 위에서 이 종목을 다시 조회/저장하면 그런 값은 계산하지 않고 비워둡니다.</div>';
}
async function checkFiscalMonths() {
  const st = document.getElementById('fiscalStatus');
  if (!st || st.dataset.busy === '1') return;
  st.dataset.busy = '1';
  try {
    let saved = 0, errs = 0, guard = 0, firstErr = null;
    while (guard++ < 200) {
      const r = await fetch('/api/fiscal/refresh?limit=10');
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || '결산월 확인 실패');
      saved += d.saved || 0; errs += d.errors || 0; if (d.firstError && !firstErr) firstErr = d.firstError;
      st.textContent = '결산월 확인 중... 저장 ' + saved + '개' + (errs ? ', 확인 못한 종목 ' + errs + '개' : '');
      if (d.done || !d.processed || !d.saved) break;
    }
    const r2 = await fetch('/api/fiscal/summary'); const s2 = await r2.json();
    const non = s2.nonDecember || [];
    st.textContent = '확인된 종목 ' + s2.checked + '개 중 12월 결산이 아닌 종목 ' + non.length + '개' + (non.length ? ': ' + non.map((c) => (c.corp_name || c.corp_code) + '(' + c.acc_mt + '월)').join(', ') : '') +
      (errs ? '\\n확인 못한 종목 ' + errs + '개 — 이유: ' + firstErr + ' (DART 프록시가 회사개요 조회를 지원하지 않으면 계속 실패합니다. 이 경우 모든 종목을 12월 결산으로 가정합니다.)' : '');
  } catch (e) { st.textContent = '오류: ' + (e.message || e); } finally { st.dataset.busy = '0'; }
}

async function btBackfillNextDay() {
  if (BT.backfilling) return;
  BT.backfilling = true;
  try {
    let total = 0, guard = 0, rem = null;
    while (guard++ < 400) {
      const r = await fetch('/api/backtest/backfill-next-day?limit=10');
      const d = await r.json();
      if (!r.ok) throw new Error(d.error || '채우기 실패');
      total += d.updated || 0; rem = d.remaining;
      btMsg('공시 다음 거래일 가격 채우는 중... 저장 ' + total + '건, 남음 ' + rem + '건' + (d.errors ? ' (오류 ' + d.errors + '건)' : ''));
      if (!d.processed || (!d.updated && !d.none && !d.nodata)) break; // 더 진행할 행이 없거나 최근 공시뿐
    }
    btMsg('완료: 이번에 ' + total + '건 저장. 남은 행 ' + rem + '건(최근 공시이거나 시세가 없는 행은 남을 수 있음). 다시 「저장된 종목 불러오기」를 눌러 반영하세요.');
  } catch (e) { btMsg('오류: ' + (e.message || e)); } finally { BT.backfilling = false; }
}

async function btLoadStocks() {
  if (BT.loading) return;
  BT.loading = true;
  try {
    btMsg('저장된 종목 목록을 확인하는 중...');
    const r0 = await fetch('/api/db/companies'); const d0 = await r0.json();
    const comps0 = d0.companies || [];
    if (!comps0.length) { btMsg('저장된 재무 데이터가 없습니다. 먼저 종목을 조회·저장해 주세요.'); return; }
    const fs = btSplitFiscal(comps0);
    const comps = fs.use;
    BT.fiscalNote = (fs.excluded.length ? '12월 결산이 아니라 제외: ' + fs.excluded.length + '개(' + fs.excluded.slice(0, 5).map((c) => c.corp_name).join(', ') + (fs.excluded.length > 5 ? ' 외' : '') + '). ' : '') +
      (fs.unknownN ? '결산월 미확인 ' + fs.unknownN + '개는 12월 결산으로 가정(데이터 관리의 「결산월 확인」 권장).' : '');
    if (!comps.length) { btMsg('백테스트에 쓸 수 있는 12월 결산 종목이 없습니다. ' + BT.fiscalNote); return; }
    const by = {};
    for (let i = 0; i < comps.length; i += 8) {
      const part = comps.slice(i, i + 8);
      btMsg('재무 데이터 불러오는 중... ' + Math.min(i + 8, comps.length) + ' / ' + comps.length + '개 종목');
      const res = await fetch('/api/backtest/fin-batch?codes=' + encodeURIComponent(part.map((c) => c.corp_code).join(',')));
      const d = await res.json();
      if (!res.ok) throw new Error(d.error || '불러오기 실패');
      (d.rows || []).forEach((row) => { (by[row.corp_code] = by[row.corp_code] || []).push(row); });
    }
    BT.stocks = comps.map((c) => ({ code: c.corp_code, name: c.corp_name, stock: c.stock_code || '', ind: c.ind || '', rows: by[c.corp_code] || [] })).filter((s) => s.rows.length);
    BT.stocks.forEach((s) => { if (s.stock && s.ind) IND.map[s.stock] = s.ind; });
    btMsg('불러오기 완료: ' + BT.stocks.length + '개 종목 (' + BT.stocks.reduce((a, s) => a + s.rows.length, 0) + '개 기간). ' + (BT.fiscalNote || '') + ' 아래에서 방법을 고르고 실행하세요.');
    document.getElementById('btFinInfo').textContent = '불러온 종목 ' + BT.stocks.length + '개';
  } catch (e) { btMsg('오류: ' + (e.message || e)); } finally { BT.loading = false; }
}

// 연간(재무) 백테스트: FY Y 사업보고서 공시 "다음 거래일" 종가에 매수 → 다음 해 사업보고서 공시 다음 거래일 종가에 매도(약 12개월).
// 공시 당일 종가에는 공시 전 정보가 섞일 수 있어 쓰지 않는다. 다음 거래일 가격이 없는 행은 대체값 없이 제외하고 개수를 BT.skipNoNext에 센다.
// 한계: 공시일은 법정 기한으로 상한을 둔 근사값(정정공시·지연공시는 완전히 통제하지 못함)이고, 12월 결산 법인을 전제로 한다.
function btBuildAnnual(keys, o) {
  const keepMissing = !!(o && o.keepMissing); // 신규 지표 발굴: 값이 없는 지표가 있어도 그 종목을 버리지 않는다
  const perYear = {};
  BT.skipNoNext = 0;
  BT.stocks.forEach((s) => {
    if (!btUnivOk(s)) return;
    const ann = {};
    s.rows.forEach((r) => { if (r.reprt_code === '11011' && isUsable(r)) ann[Number(r.bsns_year)] = r; });
    Object.keys(ann).forEach((ys) => {
      const Y = Number(ys), a = ann[Y], nx = ann[Y + 1];
      if (!nx) return;
      const P0 = a.price_next_day, P1 = nx.price_next_day;
      if (!(P0 > 0 && P1 > 0)) { BT.skipNoNext++; return; }
      if (a.filing_date && nx.filing_date && !(nx.filing_date > a.filing_date)) return;
      let L;
      try { L = anBuildSeries(s.rows.filter((r) => Number(r.bsns_year) <= Y)); } catch (e) { return; }
      const d = L.length ? L[L.length - 1] : null;
      if (!d || d.year !== Y) return;
      const vals = {};
      for (const k of keys) { let v = null; try { v = btFacVal(k, L, d, P0); } catch (e) { v = null; } if (!anFin(v)) { if (keepMissing) continue; return; } vals[k] = v; }
      (perYear[Y] = perYear[Y] || []).push({ id: s.code, vals, f: P1 / P0 - 1 });
    });
  });
  return btFinPeriods(perYear, keys, (Y) => 'FY' + Y + ' → ' + (Y + 1), keepMissing);
}
// 기간 키 → 종목 목록을 백테스트 기간 배열로. 방법이 여러 개면 각 방법의 횡단면 순위(퍼센타일)를 같은 비중으로 평균한다.
function btFinPeriods(perKey, keys, labelOf, raw) {
  return Object.keys(perKey).map(Number).sort((a, b) => a - b).map((K) => {
    const arr = perKey[K];
    if (raw) return { label: labelOf(K), items: arr.map((x) => ({ id: x.id, s: null, f: x.f, v: x.vals })), bench: null }; // 발굴용: 지표별로 따로 순위를 매기므로 합성 점수를 만들지 않는다
    const sc = arr.map(() => 0);
    keys.forEach((k) => { const rk = btRankAvg(arr.map((x) => x.vals[k])); rk.forEach((v, i) => { sc[i] += v / arr.length / keys.length; }); });
    return { label: labelOf(K), items: arr.map((x, i) => ({ id: x.id, s: keys.length === 1 ? x.vals[keys[0]] : sc[i], f: x.f, v: x.vals })), bench: null };
  });
}
// ===== 분기 TTM 재무 백테스트 =====
// 분기보고서(1Q·반기·3Q·사업보고서)가 공시될 때마다: 그 시점까지 알려진 "최근 4개 분기 합산(TTM)" 재무로 방법 값을 계산 →
// 공시 다음 거래일 종가에 매수 → 바로 다음 분기보고서 공시 다음 거래일 종가에 매도. 연 4회 구간(공시 간격은 1.5~3개월로 일정하지 않음).
// 연도별 변화율·3년 평균 등은 "같은 분기의 TTM을 1년씩 이어 붙인 시계열"(예: 2Q TTM끼리)로 계산해 기간 길이를 맞춘다. 미래 분기는 쓰지 않는다.
function btBuildQuarterly(keys, o) {
  const keepMissing = !!(o && o.keepMissing);
  const per = {};
  BT.skipNoNext = 0; BT.qSkipTtm = 0;
  const SLOT = { '11013': 1, '11012': 2, '11014': 3, '11011': 4 };
  BT.stocks.forEach((s) => {
    if (!btUnivOk(s)) return;
    const rep = {};
    s.rows.filter(isUsable).forEach((r) => { const n = SLOT[r.reprt_code]; if (n) rep[Number(r.bsns_year) + '|' + n] = r; });
    const qr = toQuarterlyRows(s.rows);
    const ser = {};
    for (let N = 1; N <= 4; N++) {
      const chain = [];
      Object.keys(rep).map((k) => k.split('|').map(Number)).filter((a) => a[1] === N).sort((a, b) => a[0] - b[0]).forEach((a) => {
        const Y = a[0], r = rep[Y + '|' + N];
        let t = null;
        if (N === 4) t = Object.assign({}, r, { period_label: Y + ' 연간' }); // 4분기 슬롯의 TTM = 사업보고서 연간 값
        else { const q = qr.filter((z) => z.bsns_year === String(Y) && z.period_label === Y + ' ' + N + '분기')[0]; t = q ? buildTTMFlowForQuarter(qr, q) : null; }
        if (t) chain.push(t);
      });
      ser[N] = anBuildSeriesFrom(chain);
    }
    Object.keys(rep).forEach((k) => {
      const ym = k.split('|').map(Number), Y = ym[0], N = ym[1];
      const a = rep[k], nx = rep[(N === 4 ? Y + 1 : Y) + '|' + (N === 4 ? 1 : N + 1)];
      if (!nx) return;
      const P0 = a.price_next_day, P1 = nx.price_next_day;
      if (!(P0 > 0 && P1 > 0)) { BT.skipNoNext++; return; }
      if (a.filing_date && nx.filing_date && !(nx.filing_date > a.filing_date)) return;
      const L = ser[N].filter((d) => d.year <= Y), d = L.length ? L[L.length - 1] : null;
      if (!d || d.year !== Y) { BT.qSkipTtm++; return; }
      const vals = {};
      for (const f of keys) { let v = null; try { v = btFacVal(f, L, d, P0); } catch (e) { v = null; } if (!anFin(v)) { if (keepMissing) continue; return; } vals[f] = v; }
      (per[Y * 10 + N] = per[Y * 10 + N] || []).push({ id: s.code, vals, f: P1 / P0 - 1 });
    });
  });
  return btFinPeriods(per, keys, (K) => { const Y = Math.floor(K / 10), N = K % 10, nm = ['', '1Q', '2Q', '3Q', '4Q']; return Y + ' ' + nm[N] + ' → ' + (N === 4 ? (Y + 1) + ' 1Q' : Y + ' ' + nm[N + 1]); }, keepMissing);
}
// ===== /분기 TTM 재무 백테스트 =====
// 분기 모멘텀: 분기말 주가만 사용. 형성기간 K분기, 건너뛰기 skip분기, 보유 1분기. (분기 자료라 "최근 1개월 제외"는 불가 — 0 또는 1분기 건너뛰기만 가능)
function btBuildMomQ(K, skip) {
  const per = {};
  BT.stocks.forEach((s) => {
    if (!btUnivOk(s)) return;
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
  try { await btRunFinImpl(); }
  catch (e) { btMsg('오류: ' + (e.message || e)); stepFail('백테스트 계산', e, 'btFinOut'); }
}
async function btRunFinImpl() {
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
    const q = mode === 'qttm';
    periods = q ? btBuildQuarterly(keys) : btBuildAnnual(keys, {}); ppy = q ? 4 : 1; title = (q ? '분기 TTM 재무: ' : '연간 재무: ') + keys.map((k) => btFacName(k).split(' (')[0] + (BT_DIR[k] === -1 ? ' [값 뒤집음]' : '')).join(' + ') + (keys.length > 1 ? ' (순위 평균 복합)' : '');
    sig = (q ? 'finq|' : 'fin|') + keys.slice().sort().join('+') + '|' + G;
  }
  const skipNote = (mode !== 'momq' && BT.skipNoNext) ? '공시 다음 거래일 가격이 없어 제외된 종목·연도: ' + BT.skipNoNext + '건 (「공시 다음 거래일 가격 채우기」를 먼저 실행하세요)' : '';
  btMsg(skipNote);
  const res = btRun(periods, { G, cost, ppy, trimLo: trim ? -0.9 : null, trimHi: trim ? 5 : null, minN: Math.max(3 * G, 10) });
  const nTr = btTrials(sig);
  BT.last = { kind: 'fin', mode, keys, G };
  const keysShown = mode === 'momq' ? [] : keys;
  const qx = mode === 'qttm' ? ['분기 TTM 방식: 보유기간이 공시 간격(사업보고서→1Q 약 1.5개월, 그 밖에는 약 3개월)이라 서로 다릅니다. 샤프·CAGR 환산은 연 4회로 가정한 근사입니다.', '같은 시장 국면에 집중된 기간이 많아 기간 간 독립성이 연간보다 낮습니다(분기 수익률이 겹치는 TTM 재무를 공유). t통계량은 낙관적일 수 있습니다.'].concat(BT.qSkipTtm ? ['TTM(최근 4개 분기)을 만들 수 없어 제외된 종목·분기: ' + BT.qSkipTtm + '건 (중간 분기 보고서가 저장돼 있지 않은 경우)'] : []) : [];
  document.getElementById('btFinOut').innerHTML = btRenderResult(res, { title, ppy, nTr, kind: mode === 'momq' ? 'momq' : 'fin', keys: keysShown, stocks: BT.stocks.filter(btUnivOk).length, extra: qx.concat(btUnivMode() === 'all' ? [] : ['대상 종목: ' + btUnivNote(true)]) });
}

// ===== 종목별 백테스트 값 보기 =====
// 연간 재무 백테스트(btBuildAnnual)가 종목마다 실제로 쓰는 값 — 재무 지표, 매수가, 매도가, 12개월 수익률 — 을 결산연도별로 한 표에 모은다. 같은 규칙을 쓴다.
const BT_FAC_SHORT = { fscore: 'F-Score', gpa: 'GP/A', lowaccr: '낮은 발생액', ep: 'EP(이익수익률)', bp: 'BP(장부가/주가)', fcfy: 'FCF수익률', graham: 'PER×PBR', roic: 'ROIC(3년평균)', growth: '성장(3년)', legacy4: 'Quality 4점' };
const BT_STATUS_TXT = { ok: '', open: '보유 중 (다음 해 공시 전)', noP0: '매수가 없음 (공시 다음 거래일 가격 채우기 필요)', noP1: '매도가 없음 (다음 해 가격 채우기 필요)', order: '공시일 순서 이상' };
function btStockTable(s) {
  const ann = {};
  (s.rows || []).forEach((r) => { if (r.reprt_code === '11011' && isUsable(r)) ann[Number(r.bsns_year)] = r; });
  const years = Object.keys(ann).map(Number).sort((a, b) => a - b), keys = Object.keys(BT_FACTORS), out = [];
  years.forEach((Y) => {
    const a = ann[Y], nx = ann[Y + 1];
    let L = null, d = null;
    try { L = anBuildSeries(s.rows.filter((r) => Number(r.bsns_year) <= Y)); d = L.length ? L[L.length - 1] : null; } catch (e) { L = null; d = null; }
    if (!d || d.year !== Y) d = null;
    const P0 = a.price_next_day > 0 ? a.price_next_day : null, P1 = nx && nx.price_next_day > 0 ? nx.price_next_day : null;
    let f = null, status = 'ok';
    if (!nx) status = 'open';
    else if (!P0) status = 'noP0';
    else if (!P1) status = 'noP1';
    else if (a.filing_date && nx.filing_date && !(nx.filing_date > a.filing_date)) status = 'order';
    else f = P1 / P0 - 1;
    const fac = {};
    keys.forEach((k) => { let v = null; if (d) { try { v = BT_FACTORS[k].fn(L, d, P0); } catch (e) { v = null; } } fac[k] = anFin(v) ? v : null; });
    out.push({
      year: Y, filing: a.filing_date || null, pPe: a.price_at_period_end > 0 ? a.price_at_period_end : null, buyDate: a.price_next_day_date || null, P0: P0, sellDate: nx ? (nx.price_next_day_date || null) : null, P1: P1, f: f, status: status, fac: fac,
      fin: d ? { revenue: d.revenue, ebit: d.ebit, ni: d.ni, fcf: d.fcf, eps: d.eps, roic: d.roic, roe: d.roe } : null,
    });
  });
  return out.reverse(); // 최신 연도가 위
}
function btFmtEok(v) { return anFin(v) ? Math.round(v / 1e8).toLocaleString('ko-KR') : 'N/A'; }
function btFmtWon(v) { return anFin(v) ? Math.round(v).toLocaleString('ko-KR') : 'N/A'; }
function btFmtPct(v, d) { return anFin(v) ? (v * 100).toFixed(d == null ? 1 : d) + '%' : 'N/A'; }
function btFacTxt(k, v) {
  if (!anFin(v)) return 'N/A';
  if (k === 'fscore') return v + '/9';
  if (k === 'legacy4') return v + '/4';
  if (k === 'bp') return v.toFixed(2) + '배';
  if (k === 'graham') return (-v).toFixed(1);
  return btFmtPct(v);
}
function btFmtDate(d) { const t = String(d || ''); return /^\\d{8}$/.test(t) ? t.slice(0, 4) + '-' + t.slice(4, 6) + '-' + t.slice(6) : (t || 'N/A'); }
function btStockHtml(name, table) {
  if (!table.length) return '<div class="meta-line">이 종목은 사업보고서(연간) 재무 데이터가 없어 보여 줄 값이 없습니다.</div>';
  const keys = Object.keys(BT_FACTORS), th = 'style="white-space:nowrap;"';
  const head = '<thead><tr><th class="an-first">결산<br />연도</th><th ' + th + '>공시일</th><th ' + th + '>기간말<br />주가</th><th ' + th + '>매수가<br />(공시 다음<br />거래일)</th><th ' + th + '>매도가<br />(다음 해)</th><th ' + th + '>12개월<br />수익률</th>' +
    '<th ' + th + '>매출<br />(억)</th><th ' + th + '>영업이익<br />(억)</th><th ' + th + '>순이익<br />(억)</th><th ' + th + '>FCF<br />(억)</th><th ' + th + '>EPS<br />(원)</th><th ' + th + '>ROIC</th><th ' + th + '>ROE</th>' +
    keys.map((k) => '<th ' + th + ' title="' + anEsc(BT_FACTORS[k].name + ' · 근거: ' + BT_FACTORS[k].ev) + '">' + anEsc(BT_FAC_SHORT[k] || k) + '</th>').join('') + '<th ' + th + '>백테스트</th></tr></thead>';
  const tone = (v) => v == null ? '' : ' style="color:' + (v >= 0 ? '#16a34a' : '#dc2626') + '; font-weight:700;"';
  const body = table.map((r) => {
    const fin = r.fin || {};
    return '<tr><td class="an-first">' + r.year + '</td><td>' + btFmtDate(r.filing) + '</td><td>' + btFmtWon(r.pPe) + '</td><td title="' + anEsc(btFmtDate(r.buyDate)) + '">' + btFmtWon(r.P0) + '</td><td title="' + anEsc(btFmtDate(r.sellDate)) + '">' + btFmtWon(r.P1) + '</td><td' + tone(r.f) + '>' + (r.f == null ? 'N/A' : (r.f >= 0 ? '+' : '') + btFmtPct(r.f)) + '</td>' +
      '<td>' + btFmtEok(fin.revenue) + '</td><td>' + btFmtEok(fin.ebit) + '</td><td>' + btFmtEok(fin.ni) + '</td><td>' + btFmtEok(fin.fcf) + '</td><td>' + btFmtWon(fin.eps) + '</td><td>' + btFmtPct(fin.roic) + '</td><td>' + btFmtPct(fin.roe) + '</td>' +
      keys.map((k) => '<td>' + btFacTxt(k, r.fac[k]) + '</td>').join('') +
      '<td style="text-align:left; font-size:11.5px; white-space:nowrap;">' + (r.status === 'ok' ? '<span style="color:#16a34a;">✓ 수익률 사용</span>' : '<span style="color:var(--text-muted);">' + anEsc(BT_STATUS_TXT[r.status] || '') + '</span>') + '</td></tr>';
  }).join('');
  const rs = table.filter((r) => r.f != null), up = rs.filter((r) => r.f > 0).length, mean = rs.length ? rs.reduce((a, r) => a + r.f, 0) / rs.length : null;
  return '<div style="font-weight:700; font-size:13px; margin-top:8px;">' + anEsc(name) + ' · 결산연도 ' + table.length + '개 중 12개월 수익률이 계산된 해 ' + rs.length + '개' + (rs.length ? ' (상승 ' + up + '개, 단순평균 ' + btFmtPct(mean) + ')' : '') + '</div>' +
    '<div style="overflow-x:auto;"><table class="an-tbl">' + head + '<tbody>' + body + '</tbody></table></div>' +
    '<div style="font-size:11px; color:var(--text-muted); line-height:1.5; margin-top:6px;">매수가·매도가 칸에 마우스를 올리면 실제 기준 거래일이 보입니다. 재무 값은 연결 우선 연간 기준이며 억 원 단위(EPS는 원)입니다. 팩터 열 머리글에 마우스를 올리면 정의와 학술 근거가 보입니다. 팩터 값은 해당 연도 매수가로 계산하므로 매수가가 없는 해는 가격이 필요한 팩터(EP·BP·FCF수익률·PER×PBR)가 N/A입니다. 수익률은 배당·거래비용을 제외한 주가 수익률이고, 한 종목의 몇 개 해는 통계적 증거가 아닙니다(백테스트는 많은 종목을 함께 봐야 의미가 있습니다).</div>';
}
function btStkCsv(list) {
  const NL = String.fromCharCode(10), keys = Object.keys(BT_FACTORS);
  const q = (v) => { if (v == null) return ''; const t = String(v); return /[",]/.test(t) || t.indexOf(NL) >= 0 ? '"' + t.replace(/"/g, '""') + '"' : t; };
  const head = ['종목', '종목코드', '업종', '결산연도', '공시일', '기간말주가', '매수일', '매수가', '매도일', '매도가', '12개월수익률', '상태', '매출', '영업이익', '순이익', 'FCF', 'EPS', 'ROIC', 'ROE'].concat(keys.map((k) => BT_FAC_SHORT[k] || k));
  const lines = [head.map(q).join(',')];
  list.forEach((it) => it.table.forEach((r) => {
    const f = r.fin || {};
    lines.push([it.name, it.code, it.ind || '', r.year, r.filing, r.pPe, r.buyDate, r.P0, r.sellDate, r.P1, r.f, r.status, f.revenue, f.ebit, f.ni, f.fcf, f.eps, f.roic, f.roe].concat(keys.map((k) => r.fac[k])).map(q).join(','));
  }));
  return String.fromCharCode(0xFEFF) + lines.join(NL);
}
// ===== /종목별 백테스트 값 보기 =====

const BT_STK = { list: null, cur: null, token: 0, cache: {}, pending: {} };
function btIndOfCorp(corp) { const c = (BT_STK.list || []).filter((z) => z.corp_code === corp)[0]; return c && c.stock_code ? (indLabel(IND.map[c.stock_code]) || '') : ''; }
async function btStkLoadList() {
  if (BT_STK.list) return;
  try {
    const r = await fetch('/api/db/companies'); const d = await r.json();
    BT_STK.list = (d.companies || []).filter((c) => c.corp_name); indSyncList(BT_STK.list);
    const dl = document.getElementById('btStkList');
    if (dl) dl.innerHTML = BT_STK.list.map((c) => '<option value="' + anEsc(c.corp_name) + '"></option>').join('');
  } catch (e) { BT_STK.list = null; }
}
async function btStkPick(raw) {
  const q = String(raw || '').trim(), msg = document.getElementById('btStkMsg'), out = document.getElementById('btStkOut');
  if (!q) { msg.textContent = '종목명을 입력하세요.'; return; }
  await btStkLoadList();
  const L = BT_STK.list || [];
  const c = L.filter((x) => x.corp_name === q)[0] || L.filter((x) => x.corp_name.indexOf(q) === 0)[0] || L.filter((x) => x.corp_name.indexOf(q) >= 0)[0];
  if (!c) { msg.textContent = '재무 데이터가 저장된 종목 중에서 「' + q + '」를 찾지 못했습니다. 데이터 관리에서 먼저 재무 데이터를 받아 주세요.'; out.innerHTML = ''; return; }
  const tk = ++BT_STK.token;
  msg.textContent = c.corp_name + ' 불러오는 중...';
  try {
    let rows = null;
    const have = BT.stocks ? BT.stocks.filter((s) => s.code === c.corp_code)[0] : null;
    if (have) rows = have.rows;
    else if (BT_STK.cache[c.corp_code]) rows = BT_STK.cache[c.corp_code];
    else {
      // 같은 종목을 입력칸의 change와 Enter가 동시에 부르더라도 서버에는 한 번만 요청한다
      BT_STK.pending[c.corp_code] = BT_STK.pending[c.corp_code] || fetch('/api/backtest/fin-batch?codes=' + encodeURIComponent(c.corp_code)).then(async (res) => { const d = await res.json(); if (!res.ok) throw new Error(d.error || '불러오기 실패'); return d.rows || []; });
      try { rows = await BT_STK.pending[c.corp_code]; BT_STK.cache[c.corp_code] = rows; } finally { delete BT_STK.pending[c.corp_code]; }
    }
    if (tk !== BT_STK.token) return;
    const table = btStockTable({ code: c.corp_code, name: c.corp_name, rows: rows });
    BT_STK.cur = { code: c.corp_code, name: c.corp_name, stock: c.stock_code || '', table: table };
    document.getElementById('btStkIn').value = c.corp_name;
    msg.textContent = '';
    const sc0 = c.stock_code || ''; if (sc0 && c.ind) IND.map[sc0] = c.ind;
    out.innerHTML = fiscalBanner(c.acc_mt) + '<div class="meta-line" style="margin:4px 0;">종목코드 ' + escHtml(sc0 || '-') + '<span data-ind="' + escHtml(sc0) + '"></span></div>' + btStockHtml(c.corp_name, table);
    if (sc0) { indPaint(); indEnsure([sc0]); }
  } catch (e) { if (tk === BT_STK.token) { msg.textContent = '오류: ' + (e.message || e); } }
}
function btStkDownload(all) {
  let list;
  if (all) {
    if (!BT.stocks) { document.getElementById('btStkMsg').textContent = '전체 종목 CSV는 위 A의 「① 저장된 종목 불러오기」를 먼저 누른 뒤 쓸 수 있습니다.'; return; }
    list = BT.stocks.map((s) => ({ name: s.name, code: s.code, ind: btIndOfCorp(s.code), table: btStockTable(s) }));
  } else {
    if (!BT_STK.cur) { document.getElementById('btStkMsg').textContent = '먼저 종목을 선택하세요.'; return; }
    list = [{ name: BT_STK.cur.name, code: BT_STK.cur.code, ind: btIndOfCorp(BT_STK.cur.code), table: BT_STK.cur.table }];
  }
  const blob = new Blob([btStkCsv(list)], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = all ? 'backtest_all_stocks.csv' : 'backtest_' + BT_STK.cur.name + '.csv';
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500);
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
      + '<div class="row" style="margin:8px 0;"><button onclick="btApplyScreen()">이 필터·가중치를 스크리닝에 적용</button><span style="font-size:11.5px; color:var(--text-muted);">적용되는 것: 시총·거래대금 하위 제외 %, 최소 주가, 신호 가중치. (건너뛰기·형성·보유 기간은 스크리닝이 21/63/126거래일로 고정이라 옮겨지지 않습니다.)</span></div>';
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

// ===== 상위 그룹 종목 상세 =====
// 백테스트에서 "상위 그룹"에 들어간 종목을 기간별로 펼쳐 보여준다. 점수 높은 순, 같은 기간의 12개월(또는 보유기간) 수익률과 함께.
const BT_TOP = {};
function btTopName(kind, id) {
  if (kind === 'scr') { const c = (BT_STK.list || []).filter((z) => z.stock_code === id)[0]; return c ? c.corp_name : ''; }
  const s = BT.stocks ? BT.stocks.filter((z) => z.code === id)[0] : null; return s ? s.name : '';
}
// 상위 종목의 종목코드·업종 이름. kind 'scr'는 id가 종목코드, 'fin'은 id가 DART 고유번호(corp_code)다.
function btTopInd(kind, id) {
  let sc = id;
  if (kind !== 'scr') { const c = (BT_STK.list || []).filter((z) => z.corp_code === id)[0]; sc = c ? c.stock_code || '' : ''; }
  return { sc: sc, label: sc ? (indLabel(IND.map[sc]) || '') : '' };
}
function btTopIndFill(kind, i) {
  const c = BT_TOP[kind]; if (!c || !BT_STK.list) return;
  const r = c.res.rows[i]; if (!r) return;
  const codes = (r.top || []).map((z) => btTopInd(kind, z.id).sc).filter(Boolean);
  if (!codes.some((x) => !IND.map[x])) return;
  indEnsure(codes).then(() => { const el = document.getElementById('btTopBody_' + kind), sel = document.getElementById('btTopSel_' + kind); if (el && sel && Number(sel.value) === i) el.innerHTML = btTopHtml(kind, i); });
}
function btTopScore(kind, m, s) {
  if (!anFin(s)) return 'N/A';
  if (kind === 'fin') return m.keys.length === 1 ? btFacTxt(m.keys[0], s) : (s * 100).toFixed(0) + '점(순위평균)';
  if (kind === 'momq') return (s * 100).toFixed(1) + '%';
  return s.toFixed(2);
}
function btTopPct(v) { return anFin(v) ? (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%' : 'N/A'; }
function btTopHtml(kind, i) {
  const c = BT_TOP[kind]; if (!c) return '';
  const r = c.res.rows[i]; if (!r) return '';
  const m = c.m, keys = kind === 'fin' ? m.keys : [];
  const list = r.top || [], win = list.filter((z) => z.f > 0).length;
  const best = list.slice().sort((a, b) => b.f - a.f);
  const sum = '<div style="font-size:12.5px; line-height:1.6; margin:6px 0;"><b>' + escHtml(r.label) + '</b> · 상위 그룹 ' + list.length + '개 종목 (전체 ' + r.n + '개 중) · 그룹 평균 <b>' + btTopPct(r.gm[c.res.G - 1]) + '</b> · 전체 평균 ' + btTopPct(r.uni) + ' · 오른 종목 ' + win + '개(' + (list.length ? Math.round(win * 100 / list.length) : 0) + '%)' +
    (best.length ? ' · 최고 ' + escHtml(btTopName(kind, best[0].id) || best[0].id) + ' ' + btTopPct(best[0].f) + ' / 최저 ' + escHtml(btTopName(kind, best[best.length - 1].id) || best[best.length - 1].id) + ' ' + btTopPct(best[best.length - 1].f) : '') + '</div>';
  const head = '<th>순위</th><th class="an-first">종목</th><th>코드</th><th>업종</th><th>점수</th>' + keys.map((k) => '<th>' + escHtml(BT_FAC_SHORT[k] || k) + '</th>').join('') + '<th>수익률</th>';
  const body = list.map((z, n) => {
    const nm = btTopName(kind, z.id);
    const cell = nm ? '<a href="#" onclick="btTopOpen(\\'' + kind + '\\',\\'' + z.id + '\\'); return false;" title="재무 값·매수가·매도가를 연도별로 보기">' + escHtml(nm) + '</a>' : '<span style="color:var(--text-muted);">이름 없음</span>';
    return '<tr><td>' + (n + 1) + '</td><td class="an-first">' + cell + '</td><td>' + escHtml(z.id) + '</td><td>' + escHtml(btTopInd(kind, z.id).label || '-') + '</td><td>' + btTopScore(kind, m, z.s) + '</td>' + keys.map((k) => '<td>' + btFacTxt(k, z.v ? z.v[k] : null) + '</td>').join('') + '<td style="color:' + (z.f >= 0 ? '#16a34a' : '#dc2626') + ';">' + btTopPct(z.f) + '</td></tr>';
  }).join('');
  return sum + '<div class="tscroll" style="max-height:55vh;"><table class="an-tbl"><thead><tr>' + head + '</tr></thead><tbody>' + body + '</tbody></table></div>';
}
function btTopBlock(kind, res, m) {
  if (!res.rows.length || !res.rows[0].top) return '';
  BT_TOP[kind] = { res, m };
  btStkLoadList().then(() => { const sel = document.getElementById('btTopSel_' + kind); if (sel) btTopShow(kind, Number(sel.value)); });
  const last = res.rows.length - 1;
  const opts = res.rows.map((r, i) => '<option value="' + i + '"' + (i === last ? ' selected' : '') + '>' + escHtml(r.label) + ' (' + btTopPct(r.gm[res.G - 1]) + ')</option>').join('');
  return '<details open style="margin-top:10px; border:1px solid #bbf7d0; background:#f0fdf4; border-radius:8px; padding:8px 10px;"><summary style="cursor:pointer; font-weight:700;">상위 그룹에 들어간 종목 상세</summary>' +
    '<div class="row" style="margin:6px 0;"><label style="font-size:12.5px;">기간 <select id="btTopSel_' + kind + '" onchange="btTopShow(\\'' + kind + '\\', Number(this.value))">' + opts + '</select></label><button onclick="btTopDownload(\\'' + kind + '\\')">전체 기간 CSV</button></div>' +
    '<div id="btTopBody_' + kind + '">' + btTopHtml(kind, last) + '</div>' +
    '<div style="font-size:11.5px; color:var(--text-muted); margin-top:6px; line-height:1.5;">점수가 높은 순입니다. 종목명을 누르면 위 「C. 종목별 보기」에서 그 종목의 연도별 재무 값·매수가·매도가를 봅니다. 한 기간의 상위 종목 몇 개는 우연의 영향이 커서 선정 근거가 될 수 없고, 과거 결과는 미래를 보장하지 않습니다.</div></details>';
}
function btTopShow(kind, i) { const el = document.getElementById('btTopBody_' + kind); if (el) { el.innerHTML = btTopHtml(kind, i); btTopIndFill(kind, i); } }
function btTopOpen(kind, id) {
  const nm = btTopName(kind, id); if (!nm) return;
  btStkPick(nm).then(() => { const el = document.getElementById('btStkOut'); if (el && el.scrollIntoView) el.scrollIntoView({ behavior: 'smooth', block: 'start' }); });
}
function btTopCsv(kind) {
  const c = BT_TOP[kind], NL = String.fromCharCode(10); if (!c) return '';
  const keys = kind === 'fin' ? c.m.keys : [];
  const q = (v) => { if (v == null) return ''; const t = String(v); return /[",]/.test(t) ? '"' + t.replace(/"/g, '""') + '"' : t; };
  const lines = [['기간', '순위', '종목', '코드', '업종', '점수', '수익률'].concat(keys.map((k) => BT_FAC_SHORT[k] || k)).map(q).join(',')];
  c.res.rows.forEach((r) => (r.top || []).forEach((z, n) => { lines.push([r.label, n + 1, btTopName(kind, z.id), z.id, btTopInd(kind, z.id).label, z.s, z.f].concat(keys.map((k) => z.v ? z.v[k] : null)).map(q).join(',')); }));
  return String.fromCharCode(0xFEFF) + lines.join(NL);
}
function btTopDownload(kind) {
  const blob = new Blob([btTopCsv(kind)], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'backtest_top_group_' + kind + '.csv';
  document.body.appendChild(a); a.click(); setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 500);
}
// ===== /상위 그룹 종목 상세 =====

// ---------- 결과 화면 ----------
function btBadgeFor(k) { const f = btFac(k); return f ? anBadge(f.badge) : ''; }
function btFacName(k) { const f = btFac(k); return f ? f.name + (BT_DIR[k] === -1 ? ' [값 뒤집음: 낮을수록 유리]' : '') : k; }
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
  const evid = m.keys ? '<div style="font-size:12px; margin:6px 0;">' + m.keys.map((k) => '<div>' + btBadgeFor(k) + ' <b>' + escHtml(btFacName(k)) + '</b> — ' + escHtml(btFac(k).ev) + '</div>').join('') + '</div>' : '';
  return '<div class="card" style="border-left:4px solid ' + vcol + ';"><div style="font-weight:800; font-size:14px;">' + escHtml(m.title) + '</div>' +
    '<div style="margin:6px 0; font-weight:700; color:' + vcol + ';">판정: ' + verdict + dir + '</div>' + evid + cards +
    '<div style="margin:8px 0; font-size:12px; line-height:1.55;"><b>표본 분할 점검</b> ' + splitTxt + '</div>' +
    btChart(res) +
    '<div class="tscroll" style="max-height:50vh; margin-top:8px;"><table class="an-tbl"><thead><tr><th class="an-first">그룹(낮은 점수 → 높은 점수)</th><th>기간당 평균</th><th>전체 대비</th><th>연환산 샤프</th><th>누적</th><th>최대낙폭</th><th>적중</th></tr></thead><tbody>' + grow + '</tbody></table></div>' + btTopBlock(m.kind, res, m) +
    '<details style="margin-top:8px;"><summary style="cursor:pointer; font-weight:600;">기간별 결과 (' + res.T + '개)</summary><div class="tscroll" style="max-height:50vh;"><table class="an-tbl"><thead><tr><th class="an-first">기간</th><th>종목수</th><th>상위</th><th>하위</th><th>롱숏</th><th>IC</th><th>코스피</th></tr></thead><tbody>' + prow + '</tbody></table></div></details>' +
    '<div style="margin-top:8px; padding:8px 10px; background:#fffbeb; border:1px solid #fde68a; border-radius:8px; font-size:12px; line-height:1.55;"><b>주의할 점</b><ul style="margin:4px 0 0 18px; padding:0;">' + warns.map((w) => '<li>' + w + '</li>').join('') + '</ul></div></div>';
}

// ----- 대상 종목(금융·지주 포함 여부) -----
function btUnivMode() { const e = document.getElementById('btUniv'); return e ? e.value : 'all'; }
function btSecOf(s) { return finSectorOf(s.ind || (s.stock && IND.map[s.stock]) || '', s.name); }
function btUnivOk(s) {
  const mode = btUnivMode(); if (mode === 'all') return true;
  const sec = btSecOf(s), isFin = !!(sec && sec.fin), isHold = !!(sec && sec.k === 'holding');
  return mode === 'exfin' ? !isFin : mode === 'exboth' ? !(isFin || isHold) : isFin;
}
function btUnivNote(short) {
  if (!BT.stocks) return '';
  const mode = btUnivMode(), n = BT.stocks.length, ok = BT.stocks.filter(btUnivOk).length;
  const byName = BT.stocks.filter((s) => { const sc = btSecOf(s); return sc && String(sc.basis).indexOf('이름') >= 0; }).length;
  const noInd = BT.stocks.filter((s) => !(s.ind || (s.stock && IND.map[s.stock]))).length;
  return (short ? '' : '') + '불러온 ' + n + '개 중 ' + ok + '개 사용' + (mode === 'all' ? '' : '(제외 ' + (n - ok) + '개)') + (noInd ? ' · 업종코드 없는 ' + noInd + '개는 이름으로 추정(이름만으로는 일부 놓칠 수 있음)' : '') + (byName ? ' · 이름 추정 분류 ' + byName + '개' : '');
}
async function btUnivChange() {
  const e = document.getElementById('btUnivNote'); if (!e) return;
  if (!BT.stocks) { e.textContent = '종목을 불러온 뒤 적용됩니다.'; return; }
  e.textContent = btUnivNote();
  if (btUnivMode() === 'all') return;
  const miss = BT.stocks.filter((s) => s.stock && !s.ind && !IND.map[s.stock]).map((s) => s.stock);
  if (miss.length) { try { await indFill(miss, function (a, b) { e.textContent = '업종코드를 받는 중... ' + a + ' / ' + b; }); } catch (x) { /* 실패하면 이름 추정으로 진행 */ } e.textContent = btUnivNote(); }
}
function btFinModeChange() {
  const m = document.getElementById('btFinMode').value;
  document.getElementById('btFinFactors').style.display = (m === 'annual' || m === 'qttm') ? '' : 'none';
  document.getElementById('btMomBox').style.display = m === 'momq' ? '' : 'none';
}
// ===== 조합 탐색 (역방향 백테스트): 재무·가치 요인 1~5개 조합 중 무엇이 우연이 아닌지 =====
// 아이디어: 조합을 수백 개 시험하면 우연히 좋아 보이는 것이 반드시 나온다(다중검정). 그래서 "수익률을 종목 사이에서 무작위로 섞은 가짜 세계"를
// 수백 번 만들어 같은 탐색을 반복하고, 가짜 세계에서 나온 '가장 좋은 조합의 |t|'가 얼마나 큰지를 기준선으로 삼는다(Westfall-Young maxT 방식).
// 진짜 데이터에서 그 기준선을 넘는 조합만 "보정 후에도 유의"로 본다. 추가로 기간을 나눠 앞 구간에서 고른 조합이 뒤 구간에서도 같은 방향인지 확인한다.
// 한계: 종목 사이에서만 섞으므로 업종·시장 동조(같은 해에 같이 오르내림)의 영향은 반영하지 못하고, 과거에 한 번 본 데이터이며 거래비용·생존편향은 별도다.
const CMB = { res: null, running: false };
function cmbRng(seed) { let a = (seed >>> 0) || 1; return function () { a = (a + 0x6D2B79F5) >>> 0; let t = a; t = Math.imul(t ^ (t >>> 15), t | 1); t ^= t + Math.imul(t ^ (t >>> 7), t | 61); return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }; }
function cmbLnGamma(x) { const c = [76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155, 0.1208650973866179e-2, -0.5395239384953e-5]; let y = x, t = x + 5.5; t -= (x + 0.5) * Math.log(t); let s = 1.000000000190015; for (let j = 0; j < 6; j++) { y += 1; s += c[j] / y; } return -t + Math.log(2.5066282746310005 * s / x); }
function cmbBetacf(a, b, x) {
  const FP = 1e-300; const qab = a + b, qap = a + 1, qam = a - 1; let c = 1, d = 1 - qab * x / qap; if (Math.abs(d) < FP) d = FP; d = 1 / d; let h = d;
  for (let m = 1; m <= 300; m++) {
    const m2 = 2 * m; let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
    d = 1 + aa * d; if (Math.abs(d) < FP) d = FP; c = 1 + aa / c; if (Math.abs(c) < FP) c = FP; d = 1 / d; h *= d * c;
    aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
    d = 1 + aa * d; if (Math.abs(d) < FP) d = FP; c = 1 + aa / c; if (Math.abs(c) < FP) c = FP; d = 1 / d; const del = d * c; h *= del; if (Math.abs(del - 1) < 3e-12) break;
  }
  return h;
}
function cmbBetai(a, b, x) { if (x <= 0) return 0; if (x >= 1) return 1; const bt = Math.exp(cmbLnGamma(a + b) - cmbLnGamma(a) - cmbLnGamma(b) + a * Math.log(x) + b * Math.log(1 - x)); return x < (a + 1) / (a + b + 2) ? bt * cmbBetacf(a, b, x) / a : 1 - bt * cmbBetacf(b, a, 1 - x) / b; }
function cmbPTwo(t, df) { if (!(df > 0) || !isFinite(t)) return null; return cmbBetai(df / 2, 0.5, df / (df + t * t)); } // 양쪽 검정 p값(스튜던트 t)
function cmbCombos(n, kmax) { const out = []; const rec = function (start, cur) { if (cur.length) out.push(cur.slice()); if (cur.length >= kmax) return; for (let i = start; i < n; i++) { cur.push(i); rec(i + 1, cur); cur.pop(); } }; rec(0, []); return out; }
const cmbTick = function () { return new Promise(function (r) { setTimeout(r, 0); }); };
function cmbTstat(sum, sum2, n) { if (n < 3) return null; const m = sum / n; const v = (sum2 - n * m * m) / (n - 1); return v > 0 ? m / Math.sqrt(v / n) : null; }
// periods: [{label, items:[{id, f, v:{요인:값}}]}] (오래된 순). 모든 요인 값이 있는 종목만 들어 있어야 한다.
async function cmbSearch(o) {
  const keys = o.keys, G = o.G || 5, minN = Math.max(3 * G, 10), B = o.B == null ? 300 : o.B, seed = o.seed || 1;
  const data = [];
  o.periods.forEach(function (p) {
    let it = p.items.filter(function (x) { return anFin(x.f) && keys.every(function (k) { return anFin(x.v[k]); }); });
    if (o.trim) it = it.filter(function (x) { return x.f > -0.9 && x.f < 5; });
    if (it.length < minN) return;
    const ranks = {}; keys.forEach(function (k) { ranks[k] = btRankAvg(it.map(function (x) { return x.v[k]; })); });
    data.push({ label: p.label, n: it.length, ids: it.map(function (x) { return x.id; }), f: Float64Array.from(it.map(function (x) { return x.f; })), ranks: ranks });
  });
  const T = data.length; if (T < 4) return { error: '계산에 쓸 수 있는 기간이 ' + T + '개뿐입니다(최소 4개 필요). 요인 수를 줄이거나 분기 방식을 쓰세요.', T: T };
  const exN = (o.split && T >= 9) ? Math.ceil(T * o.split) : T;
  const combos = cmbCombos(keys.length, o.kmax || 5);
  const M = combos.length;
  // 1) 조합마다 기간별 상위/하위 그룹 구성(수익률과 무관하므로 가짜 세계에서도 그대로 쓴다)
  const grp = new Array(M);
  for (let c = 0; c < M; c++) {
    const ck = combos[c], per = new Array(T);
    for (let t = 0; t < T; t++) {
      const d = data[t], n = d.n, sc = new Float64Array(n);
      for (let j = 0; j < ck.length; j++) { const r = d.ranks[keys[ck[j]]]; for (let i = 0; i < n; i++) sc[i] += r[i]; }
      const ord = new Array(n); for (let i = 0; i < n; i++) ord[i] = i;
      ord.sort(function (a, b) { return (sc[a] - sc[b]) || (d.ids[a] < d.ids[b] ? -1 : d.ids[a] > d.ids[b] ? 1 : 0); });
      const top = [], bot = [];
      for (let pos = 0; pos < n; pos++) { const g = Math.min(G - 1, Math.floor(pos * G / n)); if (g === G - 1) top.push(ord[pos]); else if (g === 0) bot.push(ord[pos]); }
      per[t] = { top: Int32Array.from(top), bot: Int32Array.from(bot) };
    }
    grp[c] = per;
    if (c % 40 === 39) { if (o.onProgress) o.onProgress('조합 구성 ' + (c + 1) + ' / ' + M); await cmbTick(); }
  }
  const spreadOf = function (c, t, f) { const g = grp[c][t]; let a = 0, b = 0; for (let i = 0; i < g.top.length; i++) a += f[g.top[i]]; for (let i = 0; i < g.bot.length; i++) b += f[g.bot[i]]; return a / g.top.length - b / g.bot.length; };
  // 2) 진짜 데이터의 통계(탐색 구간) + 확인 구간
  const obs = new Array(M);
  for (let c = 0; c < M; c++) {
    const sp = []; for (let t = 0; t < T; t++) sp.push(spreadOf(c, t, data[t].f));
    const ex = sp.slice(0, exN), co = sp.slice(exN);
    const sx = btStat(ex, o.ppy || 1), sc2 = co.length >= 3 ? btStat(co, o.ppy || 1) : null;
    obs[c] = { idx: combos[c], keys: combos[c].map(function (i) { return keys[i]; }), mean: sx.mean, t: sx.t, tNW: sx.tNW, hit: sx.hit, n: sx.n, pRaw: sx.t == null ? null : cmbPTwo(sx.t, sx.n - 1),
      conf: co.length ? { n: co.length, mean: btMean(co), t: sc2 ? sc2.t : null, same: sx.mean !== 0 && btMean(co) * sx.mean > 0 } : null };
  }
  // 3) 가짜 세계: 기간마다 수익률을 종목 사이에서 무작위로 섞고 같은 탐색을 반복 → 그 안에서 가장 컸던 |t|를 모은다
  const rng = cmbRng(seed), maxT = [];
  for (let b = 0; b < B; b++) {
    const fs = data.slice(0, exN).map(function (d) { const f = Float64Array.from(d.f); for (let i = f.length - 1; i > 0; i--) { const j = Math.floor(rng() * (i + 1)); const x = f[i]; f[i] = f[j]; f[j] = x; } return f; });
    let mx = 0;
    for (let c = 0; c < M; c++) {
      let s = 0, s2 = 0; for (let t = 0; t < exN; t++) { const v = spreadOf(c, t, fs[t]); s += v; s2 += v * v; }
      const tt = cmbTstat(s, s2, exN); if (tt != null && Math.abs(tt) > mx) mx = Math.abs(tt);
    }
    maxT.push(mx);
    if (b % 10 === 9) { if (o.onProgress) o.onProgress('가짜 세계 시뮬레이션 ' + (b + 1) + ' / ' + B); await cmbTick(); }
  }
  maxT.sort(function (a, b) { return a - b; });
  obs.forEach(function (r) { if (r.t == null) { r.pAdj = null; return; } let ge = 0; for (let i = maxT.length - 1; i >= 0 && maxT[i] >= Math.abs(r.t); i--) ge++; r.pAdj = (ge + 1) / (B + 1); });
  obs.sort(function (a, b) { return Math.abs(b.t || 0) - Math.abs(a.t || 0); });
  const q = function (p) { return maxT.length ? maxT[Math.min(maxT.length - 1, Math.floor(p * maxT.length))] : null; };
  return { combos: obs, M: M, T: T, exN: exN, coN: T - exN, avgN: btMean(data.map(function (d) { return d.n; })), B: B, seed: seed, nullMed: q(0.5), null95: q(0.95), null99: q(0.99), keys: keys, labels: data.map(function (d) { return d.label; }), G: G };
}
function cmbRender(R) {
  const out = document.getElementById('cmbOut');
  if (R.error) { out.innerHTML = '<div class="meta-line" style="color:var(--danger);">' + R.error + '</div>'; return; }
  const sig05 = R.combos.filter(function (r) { return r.pAdj != null && r.pAdj <= 0.05; }), sig10 = R.combos.filter(function (r) { return r.pAdj != null && r.pAdj <= 0.10; });
  const rawSig = R.combos.filter(function (r) { return r.pRaw != null && r.pRaw <= 0.05; }).length;
  const nm = function (k) { return (BT_FAC_SHORT[k] || k); };
  const pc = function (v) { return anFin(v) ? (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%' : 'N/A'; };
  let h = '<div class="meta-line" style="line-height:1.6;">시험한 조합 <b>' + R.M + '</b>개 · 기간 <b>' + R.T + '</b>개(탐색 ' + R.exN + (R.coN ? ' + 확인 ' + R.coN : '') + ') · 기간당 평균 종목 ' + R.avgN.toFixed(0) + '개 · 상·하위 ' + (100 / R.G).toFixed(0) + '% 그룹 · 가짜 세계 ' + R.B + '회(씨앗 ' + R.seed + ')</div>';
  h += '<div class="card" style="background:var(--bg); margin:8px 0; font-size:13px; line-height:1.6;">' +
    '<b>우연의 기준선</b>: 수익률을 무작위로 섞은 가짜 세계에서도 ' + R.M + '개 조합을 뒤지면 가장 좋은 |t|가 중앙값 <b>' + anNum(R.nullMed, 2) + '</b>, 95% 상한 <b>' + anNum(R.null95, 2) + '</b>까지 나옵니다. 진짜 조합이 이 값을 넘어야 "많이 뒤져서 우연히 나온 것"이 아니라고 말할 수 있습니다.<br>' +
    '원시 p≤0.05인 조합 ' + rawSig + '개(보정 안 하면 우연만으로도 약 ' + Math.round(R.M * 0.05) + '개 나옴) → 시뮬레이션 보정 후 p≤0.05 <b>' + sig05.length + '개</b>, p≤0.10 <b>' + sig10.length + '개</b>.<br>' +
    (sig05.length ? '<b>판정:</b> 보정 후에도 남은 조합이 있습니다. 아래 확인 구간 열에서 같은 방향이 유지되는지 꼭 보세요.' : '<b>판정:</b> 보정 후 유의한 조합이 없습니다. 이 데이터(' + R.T + '개 기간·평균 ' + R.avgN.toFixed(0) + '종목)로는 어떤 조합도 우연과 구별되지 않습니다. 이는 "효과가 없다"가 아니라 "이 표본으로는 확인 불가"입니다.') + '</div>';
  const show = R.combos.slice(0, 30);
  h += '<div class="tscroll" style="max-height:70vh;"><table class="an-tbl"><thead><tr><th class="an-first">조합 (|t| 큰 순 상위 30)</th><th>개수</th><th>평균 스프레드/기간</th><th>t(NW)</th><th>원시 p</th><th>보정 p</th><th>이긴 기간</th>' + (R.coN ? '<th>확인 구간 스프레드</th><th>확인 t</th><th>방향 유지</th>' : '') + '<th></th></tr></thead><tbody>';
  show.forEach(function (r, i) {
    const good = r.pAdj != null && r.pAdj <= 0.05;
    h += '<tr' + (good ? ' style="background:#ecfdf5;"' : '') + '><td class="an-first">' + r.keys.map(nm).join(' + ') + '</td><td>' + r.keys.length + '</td><td>' + pc(r.mean) + '</td><td>' + anNum(r.tNW != null ? r.tNW : r.t, 2) + '</td><td>' + (r.pRaw == null ? 'N/A' : r.pRaw.toFixed(3)) + '</td><td>' + (r.pAdj == null ? 'N/A' : r.pAdj.toFixed(3)) + '</td><td>' + (r.hit * 100).toFixed(0) + '%</td>' +
      (R.coN ? '<td>' + (r.conf ? pc(r.conf.mean) : 'N/A') + '</td><td>' + (r.conf ? anNum(r.conf.t, 2) : 'N/A') + '</td><td>' + (r.conf ? (r.conf.same ? '✓' : '✗') : '') + '</td>' : '') +
      '<td><button onclick="cmbOpen(' + i + ')" title="이 조합을 위 A의 정식 백테스트로 실행">열기</button></td></tr>';
  });
  h += '</tbody></table></div>';
  const bySize = [1, 2, 3, 4, 5].map(function (k) { const a = R.combos.filter(function (r) { return r.keys.length === k; }); return a.length ? k + '개: ' + a.length + '조합 중 보정 유의 ' + a.filter(function (r) { return r.pAdj != null && r.pAdj <= 0.05; }).length : null; }).filter(Boolean);
  h += '<div class="meta-line" style="margin-top:6px;">크기별: ' + bySize.join(' · ') + '</div>';
  if (R.coN) {
    const pool = (sig10.length ? sig10 : R.combos.slice(0, 10)).filter(function (r) { return r.conf; });
    const kept = pool.filter(function (r) { return r.conf.same; }).length;
    h += '<div class="meta-line" style="margin-top:4px;">확인 구간 점검: ' + (sig10.length ? '보정 p≤0.10' : '|t| 상위 10개') + ' 조합 ' + pool.length + '개 중 ' + kept + '개가 같은 방향 유지(효과가 전혀 없다면 약 절반이 유지). 확인 구간이 ' + R.coN + '개 기간뿐이라 검정력이 낮습니다.</div>';
  }
  h += '<div class="meta-line" style="line-height:1.55; margin-top:6px;"><b>읽는 법·한계</b> ① 모든 요인 값이 있는 종목만 쓰므로(조합 간 공정 비교) 각 요인을 따로 돌린 A의 백테스트와 종목 수가 다릅니다. ② 스프레드 = 상위 그룹 − 하위 그룹의 기간 평균 수익률(거래비용 미반영). ③ 가짜 세계는 종목 사이에서만 섞으므로 같은 해에 같이 움직이는 시장·업종 효과는 반영하지 못합니다. ④ 요인끼리 서로 비슷해서 조합 결과들은 독립이 아닙니다(그래서 단순 곱셈 보정 대신 시뮬레이션을 썼습니다). ⑤ 같은 데이터를 다시 탐색하면 앞선 선택의 영향이 남으므로, 유의 조합은 새 데이터가 쌓였을 때 다시 확인해야 합니다. 근거: 다중검정 문제는 Harvey·Liu·Zhu(2016)가 팩터 연구 전반에 지적했고, 방법은 Westfall·Young(1993)의 maxT 재표집입니다. 한국 시장에서의 성과를 보장하지 않습니다.</div>';
  out.innerHTML = h;
}
async function cmbRun() {
  if (CMB.running) return;
  if (!BT.stocks) { document.getElementById('cmbMsg').textContent = '먼저 위 A의 「① 저장된 종목 불러오기」를 눌러 주세요.'; return; }
  const keys = [].slice.call(document.querySelectorAll('.cmbFac')).filter(function (c) { return c.checked; }).map(function (c) { return c.value; });
  if (keys.length < 2) { document.getElementById('cmbMsg').textContent = '요인을 2개 이상 고르세요.'; return; }
  const mode = document.getElementById('cmbMode').value;
  CMB.running = true; const msg = document.getElementById('cmbMsg');
  try {
    msg.textContent = '기간별 요인 값을 만드는 중...'; await cmbTick();
    const periods = mode === 'qttm' ? btBuildQuarterly(keys) : btBuildAnnual(keys, {});
    const R = await cmbSearch({ periods: periods, keys: keys, kmax: Number(document.getElementById('cmbK').value) || 3, G: Number(document.getElementById('cmbG').value) || 5,
      B: Number(document.getElementById('cmbB').value) || 300, seed: Number(document.getElementById('cmbSeed').value) || 1, split: document.getElementById('cmbSplit').checked ? 2 / 3 : null,
      trim: true, ppy: mode === 'qttm' ? 4 : 1, onProgress: function (t) { msg.textContent = t; } });
    R.mode = mode; CMB.res = R; msg.textContent = ''; cmbRender(R);
  } catch (e) { msg.textContent = '오류: ' + (e.message || e); } finally { CMB.running = false; }
}
function cmbOpen(i) {
  const R = CMB.res; if (!R || !R.combos[i]) return;
  [].slice.call(document.querySelectorAll('.btFac')).forEach(function (c) { c.checked = R.combos[i].keys.indexOf(c.value) >= 0; });
  document.getElementById('btFinMode').value = R.mode === 'qttm' ? 'qttm' : 'annual'; btFinModeChange();
  btRunFin(); const el = document.getElementById('btFinOut'); if (el && el.scrollIntoView) el.scrollIntoView();
}
// ===== /조합 탐색 =====

// ===== 신규 지표 발굴 (역백테스트 2): 후보 지표 40여 개를 한꺼번에 시험해 무엇이 우연이 아닌지 찾기 =====
// 기존 BT_FACTORS(제시된 10개 방법)에 더해, 저장된 재무에서 만들 수 있는 후보 지표를 모두 같은 규칙으로 시험한다.
// 방향(높을수록 좋은지 낮을수록 좋은지)을 미리 정하지 않고 양쪽을 모두 본다(양측 검정). 지표 수가 많을수록 우연히 좋아 보이는 것이 늘어나므로
// 조합 탐색과 같은 Westfall-Young maxT(수익률을 종목 사이에서 섞은 가짜 세계의 최대 |t|)로 보정하고, 앞 2/3 기간에서 찾은 것을 뒤 1/3에서 다시 확인한다.
// 후보는 "가설 생성"용이다: 문헌 근거가 있는 것은 표시했고, 없는 것은 탐색 결과만으로 투자 근거가 되지 않는다.
const BT_DIR = {}; // 발굴 결과에서 「낮을수록 유리」로 나온 지표를 정식 백테스트(A)로 열 때 값을 뒤집기 위한 표시 { 지표키: -1 }
(function () {
  const prevOf = function (L) { return L.length >= 2 && L[L.length - 1].year - L[L.length - 2].year === 1 ? L[L.length - 2] : null; };
  const dif = function (a, b) { return anFin(a) && anFin(b) ? a - b : null; };
  const gro = function (a, b) { return anFin(a) && anFin(b) && b > 0 ? a / b - 1 : null; };
  const lastN = function (L, key, n, minN) { const v = anWin(L, key, n).filter(anFin); return v.length >= minN ? v : null; };
  const C = {};
  const add = function (k, grp, name, desc, fn, ev, badge) { C[k] = { grp: grp, name: name, desc: desc, fn: fn, ev: ev || '탐색용 후보(문헌 근거는 확인하지 않음)', badge: badge || 'assume', cand: true }; };
  add('c_roe', '수익성', 'ROE (최근 연도)', '주주 몫 자본 대비 순이익', function (L, d) { return d.roe; }, 'Fama-French(2015) 수익성 요인 계열', 'theory');
  add('c_roe3', '수익성', 'ROE 3년 평균', '최근 3년 ROE의 평균', function (L) { return anMean(anWin(L, 'roe', 3)); });
  add('c_opm', '수익성', '영업이익률 (최근)', '매출 대비 영업이익', function (L, d) { return d.opm; }, 'Fama-French(2015) 영업수익성(RMW) 계열', 'theory');
  add('c_opm3', '수익성', '영업이익률 3년 평균', '최근 3년 영업이익률의 평균', function (L) { return anMean(anWin(L, 'opm', 3)); });
  add('c_gm', '수익성', '매출총이익률', '매출 대비 매출총이익', function (L, d) { return d.gm; });
  add('c_fcfm', '수익성', 'FCF 마진', '매출 대비 잉여현금흐름', function (L, d) { return d.fcfm; });
  add('c_cfoni', '수익성', '영업현금흐름 / 순이익', '이익이 현금으로 이어지는 정도(순이익 양수일 때)', function (L, d) { return d.cfoni; });
  add('c_at', '수익성', '총자산회전율', '자산 대비 매출', function (L, d) { return d.at; });
  add('c_roic1', '수익성', 'ROIC (최근 연도)', '투하자본 대비 세후영업이익', function (L, d) { return d.roic; });
  add('c_de', '안정성', '부채비율 (부채/자본)', '값이 클수록 빚이 많음', function (L, d) { return d.de; });
  add('c_ibde', '안정성', '차입금비율 (차입금/자본)', '이자 내는 빚의 비중', function (L, d) { return d.ibde; });
  add('c_nde', '안정성', '순차입금 / EBITDA', '벌이로 순빚을 갚는 데 걸리는 햇수', function (L, d) { return d.nde; });
  add('c_icov', '안정성', '이자보상배율', '영업이익 / 이자비용', function (L, d) { return d.icov; });
  add('c_cur', '안정성', '유동비율', '유동자산 / 유동부채', function (L, d) { return d.cur; });
  add('c_cashdebt', '안정성', '현금 / 차입금', '보유 현금으로 차입금을 갚을 수 있는 정도', function (L, d) { return d.cashDebt; });
  add('c_ccc', '효율', '현금전환주기', '현금이 묶이는 일수(짧을수록 자금 부담↓)', function (L, d) { return d.ccc; });
  add('c_dso', '효율', '매출채권 회수일수', '외상을 회수하는 데 걸리는 일수', function (L, d) { return d.dso; });
  add('c_dio', '효율', '재고 보유일수', '재고가 팔리기까지 걸리는 일수', function (L, d) { return d.dio; });
  add('c_capexrev', '자본배분', 'CAPEX / 매출', '매출 대비 설비·시설 투자', function (L, d) { return anFin(d.capex) && d.revenue > 0 ? Math.abs(d.capex) / d.revenue : null; });
  add('c_capexda', '자본배분', 'CAPEX / 감가상각', '1보다 크면 설비를 늘리는 중', function (L, d) { return d.capexDa; });
  add('c_payout', '자본배분', '배당성향', '순이익 중 배당 비율', function (L, d) { return d.payout; });
  add('c_shp', '자본배분', '주주환원 / 순이익', '(배당+자사주) / 순이익', function (L, d) { return d.shp; });
  add('c_lnmcap', '가격결합', '시가총액 (로그)', '규모 효과: 작은 회사가 더 오르는지', function (L, d, P) { return anFin(d.shares) && d.shares > 0 && P > 0 ? Math.log(P * d.shares) : null; }, 'Banz(1981) 소형주 효과', 'proof');
  add('c_sp', '가격결합', '매출 / 시가총액 (SP)', '주가가 매출에 비해 싼 정도', function (L, d, P) { return anFin(d.revenue) && anFin(d.shares) && d.shares > 0 && P > 0 ? d.revenue / d.shares / P : null; });
  add('c_cfop', '가격결합', '영업현금흐름 / 시가총액 (CFO/P)', '주가가 영업현금에 비해 싼 정도', function (L, d, P) { return anFin(d.ocf) && anFin(d.shares) && d.shares > 0 && P > 0 ? d.ocf / d.shares / P : null; }, 'Lakonishok-Shleifer-Vishny(1994)', 'proof');
  add('c_evebit', '가격결합', '영업이익 / 기업가치 (EBIT/EV)', '빚까지 포함한 가격 대비 영업이익', function (L, d, P) { if (!(anFin(d.shares) && d.shares > 0 && P > 0 && anFin(d.netDebt) && d.ebit > 0)) return null; const ev = P * d.shares + d.netDebt; return ev > 0 ? d.ebit / ev : null; }, 'Loughran-Wellman(2011) 기업가치 배수', 'proof');
  add('c_dopm', '변화', '영업이익률 변화 (전년 대비)', '마진이 좋아지는 중인가', function (L, d) { const p = prevOf(L); return p ? dif(d.opm, p.opm) : null; });
  add('c_dgm', '변화', '매출총이익률 변화', '원가 경쟁력이 좋아지는 중인가', function (L, d) { const p = prevOf(L); return p ? dif(d.gm, p.gm) : null; });
  add('c_droic', '변화', 'ROIC 변화', '자본수익률이 개선되는 중인가', function (L, d) { const p = prevOf(L); return p ? dif(d.roic, p.roic) : null; });
  add('c_dat', '변화', '자산회전율 변화', '자산을 더 효율적으로 쓰는 중인가', function (L, d) { const p = prevOf(L); return p ? dif(d.at, p.at) : null; });
  add('c_dde', '변화', '부채비율 변화', '빚이 늘고 있는가', function (L, d) { const p = prevOf(L); return p ? dif(d.de, p.de) : null; });
  add('c_dfcfm', '변화', 'FCF 마진 변화', '현금창출이 좋아지는 중인가', function (L, d) { const p = prevOf(L); return p ? dif(d.fcfm, p.fcfm) : null; });
  add('c_revg', '변화', '매출 성장률 (전년 대비)', '매출이 커지는 속도', function (L, d) { const p = prevOf(L); return p ? gro(d.revenue, p.revenue) : null; }, 'Lakonishok-Shleifer-Vishny(1994)(과거 매출성장 열위)', 'proof');
  add('c_ebitg', '변화', '영업이익 성장률', '전년 영업이익이 양수일 때만 계산', function (L, d) { const p = prevOf(L); return p ? gro(d.ebit, p.ebit) : null; });
  add('c_nig', '변화', '순이익 성장률', '전년 순이익이 양수일 때만 계산', function (L, d) { const p = prevOf(L); return p ? gro(d.ni, p.ni) : null; });
  add('c_assetg', '변화', '총자산 증가율', '자산을 빠르게 늘린 회사', function (L, d) { const p = prevOf(L); return p ? gro(d.assets, p.assets) : null; }, 'Cooper-Gulen-Schill(2008) 자산 증가율', 'proof');
  add('c_sharechg', '변화', '발행주식수 증가율', '주식을 찍어 희석했는가(증자·전환)', function (L, d) { const p = prevOf(L); return p ? gro(d.shares, p.shares) : null; }, 'Pontiff-Woodgate(2008) 주식 발행', 'proof');
  add('c_roicstd', '지속성', 'ROIC 변동성 (5년 표준편차)', '수익성이 들쭉날쭉한 정도', function (L) { const v = lastN(L, 'roic', 5, 3); return v ? anStd(v) : null; });
  add('c_opmstd', '지속성', '영업이익률 변동성 (5년)', '마진이 들쭉날쭉한 정도', function (L) { const v = lastN(L, 'opm', 5, 3); return v ? anStd(v) : null; });
  add('c_fcfpos', '지속성', 'FCF 흑자 비율 (최근 5년)', '잉여현금흐름이 플러스였던 해의 비율', function (L) { const v = lastN(L, 'fcf', 5, 3); return v ? v.filter(function (x) { return x > 0; }).length / v.length : null; });
  window.BT_CANDS = C;
})();
function btFac(k) { return BT_FACTORS[k] || (typeof BT_CANDS !== 'undefined' ? BT_CANDS[k] : null); }
function btFacVal(k, L, d, P) { const f = btFac(k); let v = f.fn(L, d, P); if (BT_DIR[k] === -1 && anFin(v)) v = -v; return v; }
const DSC = { res: null, running: false };
// periods: [{label, items:[{id, f, v:{지표:값(없을 수 있음)}}]}]. 지표마다 값이 있는 종목만으로 따로 순위를 매긴다(지표별 표본이 다를 수 있음).
async function dscSearch(o) {
  const keys = o.keys, G = o.G || 5, B = o.B == null ? 300 : o.B, seed = o.seed || 1, minN = Math.max(3 * G, 10);
  const data = [];
  o.periods.forEach(function (p) {
    let it = p.items.filter(function (x) { return anFin(x.f); });
    if (o.trim) it = it.filter(function (x) { return x.f > -0.9 && x.f < 5; });
    if (it.length < minN) return;
    const g = {};
    keys.forEach(function (k) {
      const idx = []; it.forEach(function (x, i) { if (anFin(x.v[k])) idx.push(i); });
      if (idx.length < minN) { g[k] = null; return; }
      const rk = btRankAvg(idx.map(function (i) { return it[i].v[k]; }));
      const ord = idx.map(function (_, j) { return j; }).sort(function (a, b) { return (rk[a] - rk[b]) || (it[idx[a]].id < it[idx[b]].id ? -1 : it[idx[a]].id > it[idx[b]].id ? 1 : 0); });
      const top = [], bot = [], n = idx.length;
      ord.forEach(function (j, pos) { const gg = Math.min(G - 1, Math.floor(pos * G / n)); if (gg === G - 1) top.push(idx[j]); else if (gg === 0) bot.push(idx[j]); });
      g[k] = { top: Int32Array.from(top), bot: Int32Array.from(bot), n: n };
    });
    data.push({ label: p.label, n: it.length, f: Float64Array.from(it.map(function (x) { return x.f; })), g: g });
  });
  const T = data.length; if (T < 4) return { error: '계산에 쓸 수 있는 기간이 ' + T + '개뿐입니다(최소 4개 필요). 분기 방식을 쓰거나 저장 종목을 늘려 보세요.', T: T };
  const exN = (o.split && T >= 9) ? Math.ceil(T * o.split) : T;
  const spread = function (k, t, f) { const g = data[t].g[k]; if (!g) return null; let a = 0, b = 0; for (let i = 0; i < g.top.length; i++) a += f[g.top[i]]; for (let i = 0; i < g.bot.length; i++) b += f[g.bot[i]]; return a / g.top.length - b / g.bot.length; };
  const rows = [];
  for (const k of keys) {
    const ex = [], co = [], ns = [];
    for (let t = 0; t < T; t++) { const v = spread(k, t, data[t].f); if (v == null) continue; (t < exN ? ex : co).push(v); ns.push(data[t].g[k].n); }
    if (ex.length < 4) { rows.push({ key: k, insufficient: true, nPer: ex.length + co.length }); continue; }
    const sx = btStat(ex, o.ppy || 1), cm = co.length ? btMean(co) : null, sc2 = co.length >= 3 ? btStat(co, o.ppy || 1) : null;
    rows.push({ key: k, mean: sx.mean, t: sx.t, tNW: sx.tNW, hit: sx.hit, n: sx.n, pRaw: sx.t == null ? null : cmbPTwo(sx.t, sx.n - 1), avgN: btMean(ns), nPer: ex.length + co.length,
      conf: co.length ? { n: co.length, mean: cm, t: sc2 ? sc2.t : null, same: sx.mean !== 0 && cm * sx.mean > 0 } : null });
  }
  const live = rows.filter(function (r) { return !r.insufficient; });
  const rng = cmbRng(seed), maxT = [];
  for (let b = 0; b < B; b++) {
    const fs = []; for (let t = 0; t < exN; t++) { const f = Float64Array.from(data[t].f); for (let i = f.length - 1; i > 0; i--) { const j = Math.floor(rng() * (i + 1)); const x = f[i]; f[i] = f[j]; f[j] = x; } fs.push(f); }
    let mx = 0;
    for (const r of live) {
      let s = 0, s2 = 0, n = 0; for (let t = 0; t < exN; t++) { const v = spread(r.key, t, fs[t]); if (v == null) continue; s += v; s2 += v * v; n++; }
      const tt = cmbTstat(s, s2, n); if (tt != null && Math.abs(tt) > mx) mx = Math.abs(tt);
    }
    maxT.push(mx);
    if (b % 10 === 9) { if (o.onProgress) o.onProgress('가짜 세계 시뮬레이션 ' + (b + 1) + ' / ' + B); await cmbTick(); }
  }
  maxT.sort(function (a, b) { return a - b; });
  live.forEach(function (r) { if (r.t == null) { r.pAdj = null; return; } let ge = 0; for (let i = maxT.length - 1; i >= 0 && maxT[i] >= Math.abs(r.t); i--) ge++; r.pAdj = (ge + 1) / (B + 1); });
  rows.sort(function (a, b) { return Math.abs(b.t || 0) - Math.abs(a.t || 0); });
  const q = function (p) { return maxT.length ? maxT[Math.min(maxT.length - 1, Math.floor(p * maxT.length))] : null; };
  return { rows: rows, M: live.length, T: T, exN: exN, coN: T - exN, B: B, seed: seed, nullMed: q(0.5), null95: q(0.95), null99: q(0.99), labels: data.map(function (d) { return d.label; }), avgAll: btMean(data.map(function (d) { return d.n; })), G: G };
}
function dscRender(R) {
  const out = document.getElementById('dscOut');
  if (R.error) { out.innerHTML = '<div class="meta-line" style="color:var(--danger);">' + escHtml(R.error) + '</div>'; return; }
  const pc = function (v) { return anFin(v) ? (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%' : 'N/A'; };
  const def = function (k) { return btFac(k) || { name: k, grp: '', desc: '', ev: '', badge: 'assume' }; };
  const live = R.rows.filter(function (r) { return !r.insufficient; });
  const sigs = live.filter(function (r) { return r.pAdj != null && r.pAdj <= 0.05; });
  const sigNew = sigs.filter(function (r) { return def(r.key).cand; }), sigOld = sigs.filter(function (r) { return !def(r.key).cand; });
  const rawSig = live.filter(function (r) { return r.pRaw != null && r.pRaw <= 0.05; }).length;
  let h = '<div class="meta-line" style="line-height:1.6;">시험한 지표 <b>' + R.M + '</b>개(기존 제시 방법 ' + live.filter(function (r) { return !def(r.key).cand; }).length + ' + 신규 후보 ' + live.filter(function (r) { return def(r.key).cand; }).length + ') · 기간 <b>' + R.T + '</b>개(탐색 ' + R.exN + (R.coN ? ' + 확인 ' + R.coN : '') + ') · 기간당 평균 종목 약 ' + Math.round(R.avgAll) + '개 · 가짜 세계 ' + R.B + '회</div>';
  h += '<div class="card" style="background:var(--bg); margin:8px 0; font-size:13px; line-height:1.6;"><b>우연의 기준선</b>: 수익률을 무작위로 섞은 가짜 세계에서도 ' + R.M + '개 지표를 뒤지면 가장 좋은 |t|가 중앙값 <b>' + anNum(R.nullMed, 2) + '</b>, 95% 상한 <b>' + anNum(R.null95, 2) + '</b>까지 나옵니다. 진짜 데이터의 |t|가 이 선을 넘어야 우연이 아닐 가능성이 있습니다.<br />' +
    '보정하지 않은 p≤0.05 지표 ' + rawSig + '개(우연만으로도 약 ' + Math.round(R.M * 0.05) + '개 나옴) → 보정 후 p≤0.05: <b>신규 ' + sigNew.length + '개 · 기존 ' + sigOld.length + '개</b>.<br />' +
    (sigNew.length ? '<b>판정:</b> 신규 후보 중 보정 후에도 남은 것이 있습니다(초록 칸). 확인 구간에서 같은 방향인지, 표본(종목 수)이 충분한지 꼭 보세요. 이것은 <b>가설</b>이지 매매 신호가 아닙니다.' : '<b>판정:</b> 신규 후보 중 보정 후 유의한 지표가 <b>없습니다</b>. 이 데이터(기간 ' + R.T + '개)에서는 기존 방법보다 확실히 낫다고 말할 새 지표를 찾지 못했다는 뜻이며, 지표가 쓸모없다는 뜻은 아닙니다(검정력 부족 가능).') + '</div>';
  h += '<div class="tscroll" style="max-height:70vh;"><table class="an-tbl"><thead><tr><th class="an-first">지표 (|t| 큰 순)</th><th>구분</th><th>유리한 방향</th><th>평균 스프레드/기간</th><th>t</th><th>원시 p</th><th>보정 p</th><th>지표당 종목수</th>' + (R.coN ? '<th>확인 구간 평균</th><th>확인 t</th><th>같은 방향</th>' : '') + '<th></th></tr></thead><tbody>';
  R.rows.forEach(function (r, i) {
    const f = def(r.key), isNew = !!f.cand;
    if (r.insufficient) { h += '<tr style="color:var(--text-muted);"><td class="an-first">' + escHtml(f.name) + '</td><td>' + (isNew ? '신규' : '기존') + '</td><td colspan="' + (R.coN ? 10 : 7) + '">자료가 있는 기간이 ' + r.nPer + '개뿐이라 제외</td></tr>'; return; }
    const good = r.pAdj != null && r.pAdj <= 0.05, dir = r.mean >= 0 ? '높을수록' : '낮을수록';
    h += '<tr' + (good ? ' style="background:#ecfdf5;"' : '') + '><td class="an-first" title="' + escHtml((f.grp ? f.grp + ' · ' : '') + (f.desc || '') + ' · 근거: ' + (f.ev || '')) + '">' + escHtml(f.name) + '</td><td>' + (isNew ? '신규' : '기존') + '</td><td>' + dir + '</td><td>' + pc(r.mean) + '</td><td>' + anNum(r.t, 2) + '</td><td>' + (r.pRaw == null ? 'N/A' : r.pRaw.toFixed(3)) + '</td><td><b>' + (r.pAdj == null ? 'N/A' : r.pAdj.toFixed(3)) + '</b></td><td>' + Math.round(r.avgN) + '</td>' +
      (R.coN ? '<td>' + (r.conf ? pc(r.conf.mean) : 'N/A') + '</td><td>' + (r.conf ? anNum(r.conf.t, 2) : 'N/A') + '</td><td>' + (r.conf ? (r.conf.same ? '✓' : '✗') : '') + '</td>' : '') +
      '<td><button onclick="dscOpen(' + i + ')" title="이 지표를 위 A의 정식 백테스트로 실행(방향이 「낮을수록」이면 값을 뒤집어 실행)">열기</button></td></tr>';
  });
  h += '</tbody></table></div>';
  if (R.coN) {
    const pool = (sigs.length ? sigs : live.slice(0, 10)).filter(function (r) { return r.conf; });
    h += '<div class="meta-line" style="margin-top:6px;">확인 구간 점검: ' + (sigs.length ? '보정 p≤0.05' : '|t| 상위 10개') + ' 지표 ' + pool.length + '개 중 ' + pool.filter(function (r) { return r.conf.same; }).length + '개가 뒤 구간에서도 같은 방향이었습니다.</div>';
  }
  h += '<div class="meta-line" style="line-height:1.55; margin-top:6px;"><b>읽는 법·한계</b> ① 스프레드 = 상위 그룹 − 하위 그룹의 기간당 평균 수익률(값이 큰 쪽이 상위). 「유리한 방향」은 스프레드 부호로 정했으며 사전에 정한 방향이 아닙니다. ② 지표마다 값이 있는 종목만 쓰므로 지표별 표본이 다릅니다(지표당 종목수 참고). ③ 많이 뒤질수록 우연히 좋아 보이는 것이 늘어 시뮬레이션으로 보정했지만, 같은 데이터를 계속 새 지표로 뒤지면 보정이 소용없어지는 데이터 스누핑이 다시 생깁니다 — 결과는 <b>다른 기간·다른 시장에서 따로 확인</b>해야 합니다. ④ 문헌 근거가 있는 후보(툴팁 참고)도 한국 시장 검증은 이 표가 전부이며, 거래비용·생존편향·업종·시장 동조는 반영되지 않았습니다. 용어는 「쉬운 설명」 탭을 참고하세요.</div>';
  out.innerHTML = h;
}
async function dscRun() {
  if (DSC.running) return;
  const msg = document.getElementById('dscMsg');
  if (!BT.stocks) { msg.textContent = '먼저 위 A의 「① 저장된 종목 불러오기」를 눌러 주세요.'; return; }
  const mode = document.getElementById('dscMode').value;
  const scope = document.getElementById('dscScope').value;
  const keys = scope === 'cand' ? Object.keys(BT_CANDS) : Object.keys(BT_FACTORS).concat(Object.keys(BT_CANDS));
  DSC.running = true;
  try {
    msg.textContent = '지표 ' + keys.length + '개의 기간별 값을 만드는 중...'; await cmbTick();
    const periods = mode === 'qttm' ? btBuildQuarterly(keys, { keepMissing: true }) : btBuildAnnual(keys, { keepMissing: true });
    const R = await dscSearch({ periods: periods, keys: keys, G: Number(document.getElementById('dscG').value) || 5, B: Number(document.getElementById('dscB').value) || 300,
      seed: Number(document.getElementById('dscSeed').value) || 1, split: document.getElementById('dscSplit').checked ? 2 / 3 : null, trim: true, ppy: mode === 'qttm' ? 4 : 1, onProgress: function (t) { msg.textContent = t; } });
    R.mode = mode; DSC.res = R; msg.textContent = ''; dscRender(R);
  } catch (e) { msg.textContent = '오류: ' + (e.message || e); } finally { DSC.running = false; }
}
function dscOpen(i) {
  const R = DSC.res; if (!R || !R.rows[i] || R.rows[i].insufficient) return;
  const r = R.rows[i], k = r.key;
  Object.keys(BT_DIR).forEach(function (x) { delete BT_DIR[x]; });
  if (r.mean < 0) BT_DIR[k] = -1; // 낮을수록 유리로 나온 지표는 값을 뒤집어 「점수 높음 = 유리」로 맞춘다
  btCandCheckboxes();
  [].slice.call(document.querySelectorAll('.btFac')).forEach(function (c) { c.checked = c.value === k; });
  document.getElementById('btFinMode').value = R.mode === 'qttm' ? 'qttm' : 'annual'; btFinModeChange();
  btRunFin(); const el = document.getElementById('btFinOut'); if (el && el.scrollIntoView) el.scrollIntoView();
}
// 정식 백테스트(A)의 방법 목록에 신규 후보 체크박스가 아직 없으면 접이식으로 추가한다.
function btCandCheckboxes() {
  const box = document.getElementById('btFinFactors'); if (!box || document.getElementById('btCandBox')) return;
  box.insertAdjacentHTML('beforeend', '<details id="btCandBox" style="margin-top:6px;"><summary style="cursor:pointer; font-size:12.5px; font-weight:600;">신규 후보 지표 (발굴용 · 방향 미정)</summary>' +
    Object.keys(BT_CANDS).map(function (k) { return '<label style="display:block; font-size:12.5px; margin:2px 0;"><input type="checkbox" class="btFac" value="' + k + '" /> ' + escHtml(BT_CANDS[k].name) + ' ' + anBadge(BT_CANDS[k].badge) + '</label>'; }).join('') + '</details>');
}
// ===== /신규 지표 발굴 =====


let btUiReady = false;
function btInitUi() {
  if (btUiReady) return; btUiReady = true;
  try { btStkLoadList(); } catch (e) { /* 목록은 입력 시 다시 시도 */ }
  const keys = Object.keys(BT_FACTORS);
  document.getElementById('cmbPool').innerHTML = '<b>조합에 쓸 요인</b> ' + keys.map(function (k) { return '<label style="margin-right:10px; white-space:nowrap;"><input type="checkbox" class="cmbFac" value="' + k + '"' + (k === 'legacy4' ? '' : ' checked') + ' /> ' + (BT_FAC_SHORT[k] || k) + '</label>'; }).join('');
  document.getElementById('btFinFactors').innerHTML = '<div style="font-size:12.5px; font-weight:600; margin-bottom:4px;">검증할 방법 (여러 개 고르면 순위를 평균한 복합 점수)</div>' +
    keys.map((k) => '<label style="display:block; font-size:12.5px; margin:2px 0;"><input type="checkbox" class="btFac" value="' + k + '"' + (k === 'fscore' ? ' checked' : '') + ' /> ' + BT_FACTORS[k].name + ' ' + anBadge(BT_FACTORS[k].badge) + ' <span style="color:var(--text-muted); font-size:11px;">' + BT_FACTORS[k].ev + '</span></label>').join('');
}

function renderAnalysis() { return guardedRender('확장 분석', 'analysis', renderAnalysisImpl); }
function renderAnalysisImpl() {
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
  const notice = fiscalBanner(AN_ACCMT) + (needRefetch ? '<div class="card" style="background:#fffbeb; border-color:#fcd34d; font-size:12.5px;">⚠ 이 종목은 <b>감가상각·배당지급·자사주·차입금 구성 항목이 아직 저장되어 있지 않습니다</b>. EBITDA, 순차입금, 총주주환원성향 등은 N/A로 표시되며, 위쪽에서 이 종목을 <b>다시 조회/저장</b>하면 채워집니다(DART 호출량은 기존과 같음).</div>' : '');
  const S = finSectorNow(); FIN_VIEW.lastKey = (S.sec ? S.sec.k : '') + '|' + S.key;
  if (S.sec && S.sec.fin) { // 금융업: 일반 기업용 틀(ROIC·FCF·F-Score)이 맞지 않아 자본 기준 전용 화면으로 대체
    anFoldCapture(el, key);
    el.innerHTML = finRenderAnalysis(S, c, m, fiscalBanner(AN_ACCMT));
    anFoldApply(el); anBuildNav();
    try { anUpdateScenarioView(); } catch (e) { console.error('[금융업 화면] 시나리오 갱신 실패', e); }
    anLoadMomentum(); anLoadTrend();
    return;
  }
  const holdPanel = (S.sec && S.sec.k === 'holding') ? finSafe('지주회사 NAV', function () { return finRenderHolding(S, c, m); }) : '';
  const genBanner = S.forced === 'gen' ? '<div class="card" style="background:#f5f3ff; border-color:#ddd6fe; font-size:12.5px;">일반 기업용 지표 화면으로 보는 중입니다(' + anEsc(S.auto.n) + ' 자동 분류를 무시). 이 업종에서 ROIC·FCF·F-Score는 의미가 약합니다. <button type="button" onclick="finViewAuto()" style="font-size:12px; padding:2px 8px;">자동 분류로 되돌리기</button></div>' : '';
  const A = anRenderAreas(c);
  anFoldCapture(el, key);
  // 스토리 순서: 유형 → 사업의 질 → 이익의 진위 → 안전 → 성장 → 자본배분 → 가격 → 시장의 시선 → 결론, 마지막에 참고(설명서·설정)
  el.innerHTML = notice + genBanner + anRenderGlance(c, m) + holdPanel + anRenderClassify(c) + anRenderFcfType(c) +
    anChapter('①', '좋은 사업인가?', '높은 자본수익률(ROIC)을 낼 수 있는 구조이고, 그 이유가 무엇인가', '버핏·멍거(경제적 해자), 그린왈드(진입장벽), 피셔(질적 우수성)가 가장 먼저 묻는 질문. 숫자로는 ROIC·매출총이익률·자산회전율을 봅니다.') + A.quality + anRenderRoic4(c) + A.capeff +
    anChapter('②', '이익이 현금으로 확인되는가?', '장부상 이익이 실제 주주가 가져갈 수 있는 현금으로 이어지는가', '버핏의 오너어닝스, 슬로언(1996)의 발생액 연구: 현금이 뒷받침되지 않는 이익은 지속성이 낮습니다.') + A.cash +
    anChapter('③', '재무적으로 안전한가?', '빚 때문에 사업이 흔들리거나 기회를 놓치지 않는가', '그레이엄·클라먼·막스: "먼저 잃지 않는 것". 좋은 사업도 과도한 차입 앞에서는 위험합니다.') + A.bs +
    anChapter('④', '어떻게 성장하고 있는가?', '어떤 유형의 성장이며, 그 성장이 가치를 더하는가', '린치: 성장 유형을 알아야 평가 잣대가 정해집니다. 성장은 ROIC가 자본비용을 넘을 때만 가치를 더합니다(미래 성장 지속성은 약하다는 연구도 있어 보수적으로).') + A.growth +
    anChapter('⑤', '경영진은 번 돈을 잘 쓰는가?', '이익을 배당·자사주·투자·R&D 중 어디에 쓰고, 그 선택이 주주에게 이로웠는가', '버핏이 경영자의 핵심 역량으로 강조하는 자본배분. 증분 ROIC와 함께 봅니다.') + A.alloc + anRenderRD(c) +
    anChapter('⑥', '지금 가격은 합당한가?', '좋은 회사라도 비싸게 사면 나쁜 투자입니다. 배수 → 시장이 요구하는 성장 → 시나리오 순으로 확인', '그레이엄의 안전마진, 오너어닝스·잔여이익·PEG 등 여러 방법으로 가치의 범위를 보고, 한 숫자에 의존하지 않습니다.') + anRenderValuation(c) + anRenderMethods(c) + anRenderScenario(c) +
    anChapter('⑦', '시장은 지금 이 주식을 어떻게 보고 있는가?', '가격 추세와 상대강도가 펀더멘털 판단과 어긋나지 않는가', '가치투자자에게는 참고 신호이고, 미너비니·오닐에게는 핵심 조건. 모멘텀 효과는 제가디시·티트먼(1993) 등 학술 근거가 있습니다.') + anRenderMomentum() + anRenderTrend(c) +
    anChapter('⑧', '종합 결론', '위 질문들의 결과를 한곳에 모아 정리', '자동 생성 문장입니다. 점수가 아니라 숫자가 가리키는 방향의 요약입니다.') + anRenderJudge(c) +
    '<div id="anThesisBox" class="card">' + anThesisHtml() + '</div>' +
    anChapter('⑨', '시그널과 판단 점검', '실적과 기대감을 나눠 보고, 변화(없다가 생김·있다가 사라짐)와 내 판단 과정을 점검', '주가수익률 분해는 항등식, 시그널은 연구별 근거 수준을 표시합니다. 결론을 대신 내리지 않고 판단 과정을 기록하게 돕습니다.') + sigRenderSections(c) +
    anChapter('참고', '설명서 · 용어 · 설정', '계산식·실제 입력값·근거와 판정 기준 변경', '숫자의 출처를 확인하거나 기준선을 바꾸고 싶을 때 펼쳐 보세요.') + anRenderExplain(c) + anRenderSettings();
  anFoldApply(el);
  anBuildNav();
  if (holdPanel) finNavOut();
  try { sigUpdateSummary(); } catch (e) { /* 체크리스트 요약 실패는 무시 */ }
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
        if (document.getElementById('corpName').value.trim() !== corpName) return false; // 그 사이 다른 종목으로 바뀌었으면 이 응답은 버린다(남의 종가가 들어오는 것 방지)
        if (!res.ok) throw new Error(data.error || '조회 실패');
        document.getElementById('currentPrice').value = data.close;
        noteEl.textContent = data.corp_name + ' ' + data.date + ' 종가 ' + data.close.toLocaleString() + '원을 현재 주가로 사용했습니다(직접 고쳐서 다시 계산할 수 있습니다).';
        return true;
      } catch (e) {
        if (document.getElementById('corpName').value.trim() !== corpName) return false;
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
      const statusEl = document.getElementById('cmpStatus');
      const wrapEl = document.getElementById('cmpWrap');
      wrapEl.style.display = 'none';
      const slots = [0, 1, 2, 3, 4].map(function (i) { return { name: document.getElementById('cmpName' + i).value.trim(), price: Number(document.getElementById('cmpPrice' + i).value) || null }; }).filter(function (x) { return x.name; });
      const seen = {}; const items = slots.filter(function (x) { if (seen[x.name]) return false; seen[x.name] = 1; return true; });
      if (items.length < 2) { statusEl.textContent = '비교하려면 종목을 2개 이상 입력해주세요(최대 5개).'; return; }
      statusEl.textContent = '조회 중... (' + items.length + '종목)';
      const res = await Promise.all(items.map(async function (it) {
        try {
          const d = await fetchRowsFromDb(it.name);
          const price = it.price || await autoPriceFor(it.name);
          const m = computeSummaryMetrics(d.rows, price);
          let c = null; if (m) { try { c = anBuildContext(d.rows, price, m); } catch (e) { c = null; } }
          return { name: d.corp_name || it.name, m: m, c: c };
        } catch (e) { return { name: it.name, m: null, c: null, err: e.message }; }
      }));
      const okc = res.filter(function (x) { return x.m; }), bad = res.filter(function (x) { return !x.m; });
      if (okc.length < 2) { statusEl.textContent = '재무 데이터가 있는 종목이 2개 미만입니다(없음: ' + bad.map(function (x) { return x.name; }).join(', ') + '). 먼저 "DART에서 조회 + 저장"으로 데이터를 모아주세요.'; return; }
      const pctStr = function (v) { return v != null ? (v * 100).toFixed(2) + '%' : 'N/A'; };
      const wonStr = function (v) { return v != null ? Math.round(v).toLocaleString() + '원' : 'N/A'; };
      const numStr = function (v) { return v != null ? v.toFixed(2) : 'N/A'; };
      const gr = function (x, k) { return (x.c && x.c.cls && x.c.cls.grades && x.c.cls.grades[k]) ? x.c.cls.grades[k] : 'N/A'; };
      // [라벨, 값 함수, 원값 함수(강조용), 강조 방향(hi=큰 값, lo=작은 양수 값)]
      const defs = [
        ['Growth 등급', function (x) { return gr(x, 'growth'); }],
        ['Quality 등급', function (x) { return gr(x, 'quality'); }],
        ['ROIC 등급', function (x) { return gr(x, 'roic'); }],
        ['Valuation 등급', function (x) { return gr(x, 'val'); }],
        ['ROE 평균', function (x) { return pctStr(x.m.avgROE); }, function (x) { return x.m.avgROE; }, 'hi'],
        ['ROIC 평균', function (x) { return pctStr(x.m.avgROIC); }, function (x) { return x.m.avgROIC; }, 'hi'],
        ['평균 배당성향', function (x) { return pctStr(x.m.avgPayout); }],
        ['BPS 기준시점', function (x) { return x.m.latestLabel; }],
        ['BPS', function (x) { return wonStr(x.m.bps); }],
        ['현재주가', function (x) { return x.m.priceInput ? x.m.priceInput.toLocaleString() + '원' : 'N/A'; }],
        ['EPS', function (x) { return x.m.eps != null ? Math.round(x.m.eps).toLocaleString() + '원' : 'N/A'; }],
        ['PER', function (x) { return numStr(x.m.per); }, function (x) { return x.m.per; }, 'lo'],
        ['PBR', function (x) { return numStr(x.m.pbr); }, function (x) { return x.m.pbr; }, 'lo'],
        ['이익수익률(1/PER)', function (x) { return pctStr(x.m.earningsYield); }, function (x) { return x.m.earningsYield; }, 'hi'],
        ['참고 시가총액', function (x) { return x.m.marketCap != null ? Math.round(x.m.marketCap / 1e8).toLocaleString() + '억' : 'N/A'; }],
      ];
      let html = '<table><thead><tr><th>지표</th>' + okc.map(function (x) { return '<th style="text-align:right;">' + escHtml(x.name) + '</th>'; }).join('') + '</tr></thead><tbody>';
      defs.forEach(function (d) {
        let best = -1;
        if (d[2]) {
          const vals = okc.map(function (x) { const v = d[2](x); return v != null && isFinite(v) && (d[3] === 'hi' || v > 0) ? v : null; });
          const cand = vals.filter(function (v) { return v != null; });
          if (cand.length >= 2) { const t = d[3] === 'hi' ? Math.max.apply(null, cand) : Math.min.apply(null, cand); if (cand.filter(function (v) { return v === t; }).length === 1) best = vals.indexOf(t); }
        }
        html += '<tr><td>' + d[0] + '</td>' + okc.map(function (x, i) { return '<td' + (i === best ? ' class="cmp-best"' : '') + '>' + escHtml(d[1](x)) + '</td>'; }).join('') + '</tr>';
      });
      html += '</tbody></table>';
      html += '<div class="meta-line" style="padding:6px 10px; white-space:normal;">초록 강조 = ROE·ROIC·이익수익률은 큰 쪽, PER·PBR은 작은(양수) 쪽입니다. PER·PBR이 낮다고 항상 저평가는 아니며(업종·성장·일회성 이익), 평균값은 저장된 연도 범위에 따라 달라집니다.' + (bad.length ? ' <b>제외: ' + bad.map(function (x) { return escHtml(x.name); }).join(', ') + ' (재무데이터 없음)</b>' : '') + '</div>';
      statusEl.textContent = '';
      wrapEl.style.display = 'block';
      wrapEl.innerHTML = html;
      try { wrapEl.scrollIntoView({ block: 'start' }); } catch (e) {}
      cmpList = okc.map(function (x) { return x.name; }).slice(0, CMP_MAX); cmpSave();
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
          if (data.acc_mt != null) AN_ACCMT = data.acc_mt;
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

// ===== 내 계좌 (클라이언트). 계산 엔진(pf*)은 서버와 같은 소스를 그대로 주입받아 쓴다. =====
${PF_CLIENT_FUNCS}
const PFU = { key: '', st: null, panel: 'home', qc: {}, side: 'buy', emo: '', reason: '', q: null, inited: false };
async function pfuUnlock() {
  const k = keyClean(document.getElementById('pfuKey').value); const g = document.getElementById('pfuGateMsg');
  if (!k) { g.textContent = '키를 입력하세요.'; return; }
  if (keyBad(k)) { g.style.color = '#dc2626'; g.textContent = keyBad(k); return; }
  PFU.key = k; g.textContent = '확인 중...';
  try {
    const r = await pfuApi('/api/pf/ping');
    if (!r.ok) { PFU.key = ''; g.style.color = '#dc2626'; g.textContent = r.data.error || '열 수 없습니다.'; return; }
    try { sessionStorage.setItem('pfKey', k); } catch (e) {}
    document.getElementById('pfuGate').style.display = 'none'; document.getElementById('pfuMain').style.display = '';
    document.getElementById('pfuFab').style.display = 'block';
    await pfuLoad();
  } catch (e) { PFU.key = ''; g.style.color = '#dc2626'; g.textContent = '오류: ' + e.message; }
}
function pfuChips(id, opts, key) {
  const el = document.getElementById(id);
  el.innerHTML = opts.map(function (o) { return '<button type="button" data-v="' + escHtml(o) + '">' + escHtml(o) + '</button>'; }).join('');
  el.onclick = function (ev) {
    const b = ev.target.closest('button'); if (!b) return;
    const v = b.getAttribute('data-v'); PFU[key] = PFU[key] === v ? '' : v; // 다시 누르면 해제
    Array.prototype.forEach.call(el.children, function (x) { x.className = x.getAttribute('data-v') === PFU[key] ? 'on' : ''; });
  };
}
function pfuInit() {
  if (PFU.inited) return; PFU.inited = true;
  document.getElementById('pftDate').value = pfuToday();
  pfuChips('pftEmo', ['평온', '자신감', '불안', '공포', '탐욕', '후회', '지루함'], 'emo');
  pfuChips('pftReason', ['가치 도달', '가설 훼손', '가격 위험', '비중 초과', '현금 필요', '대체 기회', '비용·세금'], 'reason');
  let h = ''; for (let i = 0; i < 3; i++) h += '<div class="row" style="margin-top:6px;"><input id="pfpTc' + i + '" placeholder="' + (i + 1) + '차 조건(예: 다음 분기 실적 확인 후)" style="flex:1; min-width:150px;" /><input id="pfpTp' + i + '" type="number" placeholder="가격" style="width:90px;" /><input id="pfpTw' + i + '" type="number" placeholder="비중%" style="width:80px;" /></div>';
  document.getElementById('pfpTr').innerHTML = h;
  const items = ['매수 전에 가설과 반증 조건을 적었다', '가치평가(Bear/Base/Bull) 근거가 있었다', '비중이 정책 한도 안이었다', '당시 감정을 기록했다', '사전에 정한 규칙대로 실행했다', '하락·상승에 감정이 아니라 가설 기준으로 대응했다'];
  document.getElementById('pfjProc').innerHTML = items.map(function (t, i) { return '<label style="display:block; font-size:13px; margin:3px 0;"><input type="checkbox" class="pfjC" data-i="' + i + '" /> ' + t + '</label>'; }).join('');
  // 종목명 자동 확인(종목분석과 같은 검색창): 거래 시트·계획·계산기·일지
  attachCombo(document.getElementById('pftName'), { pf: true, onPick: pfuPickTrade });
  attachCombo(document.getElementById('pfpName'), { pf: true });
  attachCombo(document.getElementById('pfcName'), { pf: true, onPick: function (s) { pfuQuote(); } });
  attachCombo(document.getElementById('pfjName'), { pf: true });
  document.getElementById('pfuHold').addEventListener('click', function (ev) {
    const b = ev.target.closest('button[data-act]'); if (!b) return;
    const act = b.getAttribute('data-act'), nm = b.getAttribute('data-name');
    if (act === 'buy' || act === 'sell') pfuOpenSheet(act, nm);
    else if (act === 'plan') { pfuPanel('plan'); document.getElementById('pfpName').value = nm; const p = PFU.st.plans.filter(function (x) { return x.corp_name === nm; })[0]; if (p) pfuEditPlan(p.plan_key); window.scrollTo(0, 0); }
  });
  document.getElementById('pfuAlerts').addEventListener('click', function (ev) {
    const b = ev.target.closest('button[data-act]'); if (!b) return;
    const nm = b.getAttribute('data-name');
    if (b.getAttribute('data-act') === 'plan') { pfuPanel('plan'); document.getElementById('pfpName').value = nm; window.scrollTo(0, 0); }
    else if (b.getAttribute('data-act') === 'policy') pfuPanel('policy');
    else if (b.getAttribute('data-act') === 'deposit') pfuOpenSheet('deposit');
  });
  try { const k = keyGet(); if (k) { document.getElementById('pfuKey').value = k; pfuUnlock(); } } catch (e) {}
}
function pfuPanel(p) {
  PFU.panel = p;
  ['home', 'plan', 'ledger', 'check', 'policy'].forEach(function (k) {
    document.getElementById('pfuP_' + k).style.display = k === p ? '' : 'none';
    document.getElementById('pfuNb_' + k).className = k === p ? 'toggle-active' : '';
  });
}
async function pfuLoad() {
  pfuMsg('불러오는 중...');
  try {
    const r = await pfuApi('/api/pf/state');
    if (!r.ok) { pfuMsg(r.data.error || '불러오기 실패', true); return false; }
    PFU.prev = PFU.st; PFU.st = r.data; try { anThesisUpdate(); } catch (e) {} pfuRender(); pfuPanel(PFU.panel); pfuMsg(''); return true;
  } catch (e) { pfuMsg('오류: ' + e.message, true); return false; }
}
function pfuPlanOf(code) { return (PFU.st.plans || []).filter(function (p) { return p.stock_code === code; })[0] || null; }
function pfuAlertList() {
  const st = PFU.st, out = [], today = pfuToday();
  if (!st.policy) out.push({ cls: 'bad', msg: '투자 정책이 아직 없습니다 — 비중·현금 점검과 수량 계산이 꺼져 있습니다.', act: 'policy', label: '정책 입력' });
  if (st.ledger.deposits <= 0 && st.trades.length === 0) out.push({ cls: 'info', msg: '먼저 계좌 원금을 [입금]으로 기록하세요(현금·비중 계산의 출발점).', act: 'deposit', label: '입금 기록' });
  (st.summary.warnings || []).forEach(function (w) { out.push({ cls: w.level === 'warn' ? 'bad' : '', msg: w.msg }); });
  st.summary.rows.forEach(function (r) {
    const pl = pfuPlanOf(r.code);
    if (!pl) { out.push({ cls: 'info', msg: r.name + ': 투자 계획이 없습니다(가설·반증 조건·위험 기준가).', act: 'plan', label: '계획 작성', name: r.name }); return; }
    if (pl.risk_price > 0 && r.close != null && r.close <= pl.risk_price) out.push({ cls: 'bad', msg: r.name + ': 현재가 ' + pfuFmt(r.close) + '원이 위험관리 기준가 ' + pfuFmt(pl.risk_price) + '원 이하 — 가설을 먼저 재검토하세요(자동 매도 신호가 아닙니다).', act: 'plan', label: '계획 보기', name: r.name });
    if (pl.review_date && String(pl.review_date).replace(/-/g, '') <= today.replace(/-/g, '')) out.push({ cls: '', msg: r.name + ': 계획 검토일(' + pl.review_date + ')이 도래했습니다.', act: 'plan', label: '계획 보기', name: r.name });
    if (!(pl.risk_price > 0)) out.push({ cls: 'info', msg: r.name + ': 계획에 위험관리 기준가가 없습니다.', act: 'plan', label: '계획 보기', name: r.name });
  });
  return out;
}
function pfuRender() {
  const st = PFU.st, s = st.summary, pol = st.policy;
  const pc = function (v) { return v == null ? '' : v >= 0 ? 'pf-pos' : 'pf-neg'; };
  const unrealPct = s.cost > 0 && s.unreal != null ? s.unreal / s.cost : null;
  let h = '<div class="pf-kpi"><div><small>총자산(원)</small><b>' + pfuFmt(s.total) + '</b></div><div><small>평가손익(미실현)</small><b class="' + pc(s.unreal) + '">' + (unrealPct == null ? 'N/A' : (unrealPct >= 0 ? '+' : '') + (unrealPct * 100).toFixed(1) + '%') + '</b><small>' + pfuFmt(s.unreal) + '원</small></div><div><small>현금 비중</small><b>' + pfuPct(s.cashPct) + '</b><small>' + pfuFmt(s.cash) + '원' + (pol && pol.min_cash_pct ? ' · 최소 ' + pol.min_cash_pct + '%' : '') + '</small></div></div>';
  h += '<div class="meta-line">실현손익(누적, 비용 반영) ' + pfuFmt(s.realized) + '원 · 상위 1·3·5 종목 비중 ' + pfuPct(s.top1) + ' · ' + pfuPct(s.top3) + ' · ' + pfuPct(s.top5) + (s.sectors && s.sectors.length ? ' · 업종 ' + s.sectors.slice(0, 3).map(function (x) { return escHtml(x.name) + ' ' + pfuPct(x.weight, 0); }).join('/') : '') + '</div>';
  document.getElementById('pfuSummary').innerHTML = h;
  const al = pfuAlertList(), ael = document.getElementById('pfuAlerts');
  ael.style.display = al.length ? '' : 'none';
  ael.innerHTML = al.length ? '<div class="card-title">확인할 것 ' + al.length + '</div>' + al.map(function (a) { return '<div class="pf-chip ' + (a.cls || '') + '">' + escHtml(a.msg) + (a.act ? ' <button type="button" data-act="' + a.act + '" data-name="' + escHtml(a.name || '') + '" style="padding:2px 8px; font-size:12px; margin-left:4px;">' + escHtml(a.label) + '</button>' : '') + '</div>'; }).join('') : '';
  const lim = pol && pol.max_position_pct > 0 ? pol.max_position_pct : null;
  const cards = s.rows.map(function (r) {
    const w = r.weight != null ? r.weight * 100 : null, scale = Math.max(lim ? lim * 1.4 : 30, (w || 0) * 1.1, 5);
    const pl = pfuPlanOf(r.code);
    return '<div class="pfc" id="pfcard_' + r.code + '"><div class="pfc-h"><b style="font-size:15px;">' + escHtml(r.name) + '</b><span style="font-size:11.5px; color:var(--text-muted);">' + escHtml(pl ? pl.status : '계획 없음') + (r.sector ? ' · ' + escHtml(r.sector) : '') + '</span><span class="' + pc(r.unrealPct) + '" style="margin-left:auto; font-weight:700;">' + (r.unrealPct == null ? 'N/A' : (r.unrealPct >= 0 ? '+' : '') + (r.unrealPct * 100).toFixed(1) + '%') + '</span></div>' +
      '<div class="pfc-bar"><i class="' + (lim && w > lim ? 'over' : '') + '" style="width:' + Math.min(100, (w || 0) / scale * 100).toFixed(1) + '%"></i>' + (lim ? '<u style="left:' + (lim / scale * 100).toFixed(1) + '%" title="정책 최대 비중"></u>' : '') + '</div>' +
      '<div class="pfc-m">비중 <b>' + pfuPct(r.weight) + '</b>' + (lim ? ' / 한도 ' + lim + '%' : '') + ' · ' + pfuFmt(r.qty) + '주 · 평단 ' + pfuFmt(r.avg) + ' → 현재 ' + pfuFmt(r.close) + (r.priceDate ? ' (' + escHtml(r.priceDate) + ')' : '') + '<br/>평가금액 ' + pfuFmt(r.mv) + '원' + (pl && pl.risk_price > 0 ? ' · 위험기준가 ' + pfuFmt(pl.risk_price) : '') + '</div>' +
      '<div class="row" style="margin-top:6px;"><button type="button" data-act="buy" data-name="' + escHtml(r.name) + '">매수</button><button type="button" data-act="sell" data-name="' + escHtml(r.name) + '">매도</button><button type="button" data-act="plan" data-name="' + escHtml(r.name) + '">계획</button></div></div>';
  }).join('');
  document.getElementById('pfuHold').innerHTML = '<div class="card-title" style="margin:4px 2px;">보유 종목 ' + s.rows.length + '개 <span style="font-weight:400; text-transform:none;">(막대 = 비중, 검은 선 = 정책 최대 비중)</span></div>' + (cards || '<div class="card" style="font-size:13px; color:var(--text-muted);">보유 종목이 없습니다. 오른쪽 아래 [＋ 기록]으로 입금 후 매수 내역을 입력하세요.</div>');
  pfuRenderPlans(); pfuRenderLedger(); pfuRenderBeh(); pfuRenderJournal(); pfuFillPolicy();
}
// ---------- 거래 입력 시트 ----------
function pfuToast(t) { const el = document.getElementById('pfuToast'); el.textContent = t; el.style.display = 'block'; clearTimeout(PFU.tt); PFU.tt = setTimeout(function () { el.style.display = 'none'; }, 7000); }
function pfuOpenSheet(side, name) {
  document.getElementById('pfuSheet').style.display = 'block';
  document.getElementById('pftWarn').innerHTML = '';
  PFU.emo = ''; PFU.reason = ''; PFU.q = null;
  Array.prototype.forEach.call(document.querySelectorAll('#pftEmo button, #pftReason button'), function (b) { b.className = ''; });
  ['pftQty', 'pftPrice', 'pftFee', 'pftTax', 'pftNote', 'pftAmt'].forEach(function (i) { document.getElementById(i).value = ''; }); document.getElementById('pftAmtHint').textContent = '';
  document.getElementById('pftDate').value = pfuToday();
  document.getElementById('pftName').value = name || '';
  pfuSetSide(side || 'buy');
  if (name) pfuPickTrade({ n: name });
  else setTimeout(function () { if (side === 'buy' || side === 'sell') document.getElementById('pftName').focus(); }, 80);
}
function pfuAmtToQty() {
  const amt = Number(document.getElementById('pftAmt').value), px = Number(document.getElementById('pftPrice').value), h = document.getElementById('pftAmtHint');
  if (!(amt > 0)) { h.textContent = ''; return; }
  if (!(px > 0)) { h.textContent = '체결가를 먼저 입력하세요.'; return; }
  const q = Math.floor(amt / px); document.getElementById('pftQty').value = q > 0 ? q : '';
  h.textContent = q > 0 ? '= ' + q.toLocaleString('ko-KR') + '주 (약 ' + Math.round(q * px).toLocaleString('ko-KR') + '원)' : '금액이 1주 가격보다 작습니다.';
  pfuPreview();
}
function pfuCloseSheet() { document.getElementById('pfuSheet').style.display = 'none'; }
function pfuSetSide(side) {
  PFU.side = side;
  Array.prototype.forEach.call(document.querySelectorAll('#pftSeg button'), function (b) { b.classList.toggle('on', b.getAttribute('data-side') === side); });
  const cash = side === 'deposit' || side === 'withdraw';
  document.getElementById('pftStockRow').style.display = cash ? 'none' : '';
  document.getElementById('pftQty').style.display = cash ? 'none' : ''; document.getElementById('pftAmtRow').style.display = cash ? 'none' : '';
  document.getElementById('pftPrice').placeholder = cash ? '금액(원)' : '체결가';
  document.getElementById('pftReasonWrap').style.display = side === 'sell' ? '' : 'none';
  document.getElementById('pftEmoWrap').style.display = cash ? 'none' : '';
  document.getElementById('pftTax').style.display = side === 'sell' ? '' : 'none';
  document.getElementById('pftGo').textContent = { buy: '매수 기록하기', sell: '매도 기록하기', deposit: '입금 기록하기', withdraw: '출금 기록하기' }[side];
  pfuQuickBtns(); pfuPreview();
}
async function pfuPickTrade(s) {
  const nm = s.n; document.getElementById('pftName').value = nm;
  const ctx = document.getElementById('pftCtx'); ctx.textContent = '시세 조회 중...';
  PFU.q = null;
  try {
    let q = PFU.qc[nm];
    if (!q || Date.now() - q.t > 120000) { const r = await pfuApi('/api/pf/quote?corp_name=' + encodeURIComponent(nm)); if (!r.ok) throw new Error(r.data.error || '시세 실패'); q = Object.assign({ t: Date.now() }, r.data); PFU.qc[nm] = q; }
    PFU.q = q;
    if (!document.getElementById('pftPrice').value) document.getElementById('pftPrice').value = q.close;
    const row = PFU.st.summary.rows.filter(function (r) { return r.name === nm; })[0];
    const pl = PFU.st.plans.filter(function (p) { return p.corp_name === nm; })[0];
    ctx.textContent = nm + ' 종가 ' + pfuFmt(q.close) + '원 (' + q.date + ')' + (row ? ' · 보유 ' + pfuFmt(row.qty) + '주, 평단 ' + pfuFmt(row.avg) : ' · 미보유') + (pl ? ' · 계획 ' + pl.status : ' · 계획 없음');
  } catch (e) { ctx.textContent = '시세를 못 불러왔습니다(' + e.message + '). 체결가를 직접 입력하세요.'; }
  pfuQuickBtns(); pfuPreview();
}
function pfuQuickBtns() {
  const el = document.getElementById('pftQuick'); el.innerHTML = '';
  const nm = document.getElementById('pftName').value.trim(); const st = PFU.st; if (!st) return;
  const row = st.summary.rows.filter(function (r) { return r.name === nm; })[0];
  const add = function (t, fn) { const b = document.createElement('button'); b.type = 'button'; b.textContent = t; b.onclick = fn; el.appendChild(b); };
  const setQ = function (q) { document.getElementById('pftQty').value = Math.max(0, Math.floor(q)); pfuPreview(); };
  if (PFU.side === 'sell' && row) { add('25%', function () { setQ(row.qty * 0.25); }); add('50%', function () { setQ(row.qty * 0.5); }); add('전량 ' + pfuFmt(row.qty) + '주', function () { setQ(row.qty); }); }
  if (PFU.side === 'buy' && st.policy) {
    const pl = st.plans.filter(function (p) { return p.corp_name === nm; })[0];
    if (pl && pl.risk_price > 0) add('계산기 상한 적용', function () {
      const p = Number(document.getElementById('pftPrice').value) || (PFU.q && PFU.q.close);
      const res = pfSizePosition({ base: st.summary.total, riskPct: st.policy.risk_budget_pct, buyPrice: p, riskPrice: pl.risk_price, maxPosPct: st.policy.max_position_pct, curPosValue: row ? row.mv : 0, cash: st.summary.cash, minCashPct: st.policy.min_cash_pct, cashNeeds: st.policy.cash_needs_krw, feePct: st.policy.fee_rate_pct, adv: PFU.q ? PFU.q.adv20 : null, advPct: st.policy.adv_limit_pct, bearPrice: pl.bear_price });
      if (!res.ok) { document.getElementById('pftPrev').textContent = res.errors.join(' '); return; }
      setQ(res.qty);
      document.getElementById('pftPrev').innerHTML = '참고 상한 ' + pfuFmt(res.qty) + '주 (가장 작은 한도: ' + escHtml(res.binding.join(', ')) + '). 상한은 필수가 아니라 참고값입니다.';
    });
  }
}
function pfuPreview() {
  const st = PFU.st, el = document.getElementById('pftPrev'); if (!st) return;
  const side = PFU.side, p = Number(document.getElementById('pftPrice').value), q = Number(document.getElementById('pftQty').value), tot = st.summary.total, pol = st.policy || {};
  if (side === 'deposit' || side === 'withdraw') { el.innerHTML = p > 0 ? '현금 ' + pfuFmt(st.summary.cash) + ' → <b>' + pfuFmt(st.summary.cash + (side === 'deposit' ? p : -p)) + '원</b>' : '금액을 입력하세요.'; return; }
  const nm = document.getElementById('pftName').value.trim(), row = st.summary.rows.filter(function (r) { return r.name === nm; })[0];
  if (!(p > 0) || !(q > 0)) return;
  const amt = p * q, fee = pol.fee_rate_pct ? amt * pol.fee_rate_pct / 100 : 0, tax = side === 'sell' && pol.sell_tax_pct ? amt * pol.sell_tax_pct / 100 : 0;
  const cur = row && row.mv ? row.mv : 0, cash = st.summary.cash;
  const bad = function (c) { return c ? ' style="color:#dc2626;font-weight:700"' : ''; };
  if (side === 'buy') {
    const w = tot > 0 ? (cur + amt) / tot * 100 : null, cashAfter = cash - amt - fee, cp = tot > 0 ? cashAfter / tot * 100 : null;
    el.innerHTML = '금액 ' + pfuFmt(amt) + '원 (+비용 ' + pfuFmt(fee) + ') · 체결 후 비중 <span' + bad(pol.max_position_pct && w > pol.max_position_pct) + '>' + (w == null ? 'N/A' : w.toFixed(1) + '%') + '</span>' + (pol.max_position_pct ? ' / 한도 ' + pol.max_position_pct + '%' : '') + '<br/>현금 ' + pfuFmt(cash) + ' → <span' + bad(cashAfter < 0 || (pol.min_cash_pct && cp < pol.min_cash_pct)) + '>' + pfuFmt(cashAfter) + '원 (' + (cp == null ? 'N/A' : cp.toFixed(1) + '%') + ')</span>' + (pol.min_cash_pct ? ' / 최소 ' + pol.min_cash_pct + '%' : '');
  } else {
    const gain = row && row.avg != null ? (p - row.avg) * q - fee - tax : null, w = tot > 0 ? Math.max(0, cur - amt) / tot * 100 : null, cashAfter = cash + amt - fee - tax;
    el.innerHTML = '수령 약 ' + pfuFmt(amt - fee - tax) + '원 · 예상 실현손익 <b class="' + (gain == null ? '' : gain >= 0 ? 'pf-pos' : 'pf-neg') + '">' + pfuFmt(gain) + '원</b>' + (row && q > row.qty ? ' <span style="color:#dc2626;font-weight:700">(보유 ' + row.qty + '주 초과!)</span>' : '') + '<br/>체결 후 비중 ' + (w == null ? 'N/A' : w.toFixed(1) + '%') + ' · 현금 → ' + pfuFmt(cashAfter) + '원 (' + (tot > 0 ? (cashAfter / tot * 100).toFixed(1) + '%' : 'N/A') + ')';
  }
}
function pfuTradeBody(ack) {
  const side = PFU.side, cash = side === 'deposit' || side === 'withdraw';
  const b = { side: side, trade_date: pfuV('pftDate'), price: pfuV('pftPrice'), note: pfuV('pftNote') };
  if (!cash) { b.corp_name = pfuV('pftName').trim(); b.qty = pfuV('pftQty'); b.emotion = PFU.emo; b.reason = side === 'sell' ? PFU.reason : ''; }
  if (pfuV('pftFee') !== '') b.fee = pfuV('pftFee'); if (pfuV('pftTax') !== '') b.tax = pfuV('pftTax');
  if (ack) { b.ack = true; b.ack_reason = pfuV('pftAck'); b.checks = Array.prototype.map.call(document.querySelectorAll('.pftChk'), function (c) { return c.checked; }); }
  return b;
}
async function pfuSubmitTrade(ack) {
  const box = document.getElementById('pftWarn'); const b = pfuTradeBody(ack);
  if (ack && b.checks.length && b.checks.some(function (c) { return !c; })) { box.insertAdjacentHTML('afterbegin', '<div style="color:#dc2626; font-size:13px;">물타기 점검 항목을 모두 확인해야 기록됩니다. 하나라도 아니라면 기록을 취소하는 것이 맞습니다.</div>'); return; }
  const r = await pfuApi('/api/pf/trade', 'POST', b);
  if (r.status === 409 && r.data.needs_ack) {
    const chk = ['처음 세운 가설이 아직 유효하다', '반증 조건에 해당하지 않는다', '오늘 처음 보는 종목이라도 이 가격·비중에 살 것이다', '하락 원인을 확인했다', '추가 후 총 비중이 정책 한도 안이다', '손실을 만회하려는 마음이 아니다'];
    box.innerHTML = '<div style="border:1px solid #f59e0b; background:#fffbeb; border-radius:8px; padding:10px; margin-top:10px;"><b>기록 전 점검 경고</b> (막는 것이 아니라, 알고 선택했는지 남기는 절차)<ul style="font-size:13px; margin:6px 0 6px 18px; padding:0;">' + r.data.warnings.filter(function (w) { return w.level !== 'block'; }).map(function (w) { return '<li>' + escHtml(w.msg) + '</li>'; }).join('') + '</ul>' +
      (r.data.needs_check ? '<div style="font-size:13px;">물타기 점검(모두 해당해야 기록)</div>' + chk.map(function (t) { return '<label style="display:block; font-size:13px;"><input type="checkbox" class="pftChk" /> ' + t + '</label>'; }).join('') : '') +
      '<textarea id="pftAck" placeholder="그래도 기록하는 이유(5자 이상)" style="width:100%; margin-top:6px; min-height:44px;"></textarea><div class="row" style="margin-top:6px;"><button onclick="pfuSubmitTrade(true)">이유를 적었고 알고 기록합니다</button><button onclick="pfuCancelAck()" style="background:#fff; color:var(--text);">취소</button></div></div>';
    box.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    return;
  }
  if (!r.ok) { box.innerHTML = '<div style="color:#dc2626; font-size:13px; margin-top:8px;">' + escHtml(r.data.error || '기록 실패') + '</div>'; return; }
  const prev = PFU.st, nm = b.corp_name; pfuCloseSheet();
  await pfuLoad(); pfuPanel('home');
  const now = PFU.st, dTot = prev && prev.summary ? now.summary.total - prev.summary.total : null;
  const rn = nm ? now.summary.rows.filter(function (x) { return x.name === nm; })[0] : null, rp = nm && prev ? prev.summary.rows.filter(function (x) { return x.name === nm; })[0] : null;
  const sn = { buy: '매수', sell: '매도', deposit: '입금', withdraw: '출금' }[b.side];
  pfuToast('기록 완료: ' + (nm ? nm + ' ' + sn + ' ' + b.qty + '주' : sn + ' ' + pfuFmt(Number(b.price)) + '원') + (nm ? ' → 보유 ' + (rp ? pfuFmt(rp.qty) : 0) + '주 → ' + (rn ? pfuFmt(rn.qty) + '주 (비중 ' + pfuPct(rn.weight) + ')' : '없음') : '') + ' · 현금 ' + pfuPct(now.summary.cashPct) + (r.data.costNote ? ' · ' + r.data.costNote : ''));
  if (rn) { const c = document.getElementById('pfcard_' + rn.code); if (c) { c.classList.add('flash'); c.scrollIntoView({ behavior: 'smooth', block: 'center' }); } } else window.scrollTo(0, 0);
}
function pfuCancelAck() { document.getElementById('pftWarn').innerHTML = ''; }
function pfuRenderLedger() {
  const f = (document.getElementById('pflFilter') ? document.getElementById('pflFilter').value : '').trim();
  const tr = PFU.st.trades.filter(function (t) { return !f || (t.corp_name || '').indexOf(f) >= 0; }); const sn = { buy: '매수', sell: '매도', deposit: '입금', withdraw: '출금' };
  let h = (PFU.st.ledger.errors || []).map(function (e) { return '<div style="color:#dc2626; font-size:12.5px;">⚠ ' + escHtml(e.msg) + '</div>'; }).join('');
  h += tr.length ? '<div class="tscroll"><table><thead><tr><th>일자</th><th>구분</th><th>종목</th><th>수량</th><th>가격/금액</th><th>비용</th><th>감정·사유</th><th></th></tr></thead><tbody>' + tr.map(function (t) {
    return '<tr style="' + (t.voided ? 'opacity:.45; text-decoration:line-through;' : '') + '"><td>' + escHtml(t.trade_date) + '</td><td>' + sn[t.side] + '</td><td>' + escHtml(t.corp_name || '') + '</td><td>' + (t.stock_code ? pfuFmt(t.qty) : '') + '</td><td>' + pfuFmt(t.price) + '</td><td>' + pfuFmt((t.fee || 0) + (t.tax || 0)) + '</td><td>' + escHtml([t.emotion, t.reason].filter(Boolean).join(' · ')) + '</td><td>' + (t.voided ? '무효' : '<button style="padding:2px 7px; font-size:12px;" onclick="pfuVoid(' + t.id + ')">무효</button>') + '</td></tr>';
  }).join('') + '</tbody></table></div>' : '<div style="font-size:13px; color:var(--text-muted);">기록이 없습니다. 오른쪽 아래 [＋ 기록]에서 먼저 [입금]으로 계좌 원금을 기록하세요.</div>';
  document.getElementById('pfuLedger').innerHTML = h;
}
function pfuToday() { const d = new Date(Date.now() + 9 * 3600 * 1000); return d.toISOString().slice(0, 10); }
function pfuMsg(t, bad) { const el = document.getElementById('pfuMsg'); if (el) { el.style.color = bad ? '#dc2626' : 'var(--text-muted)'; el.textContent = t || ''; } }
async function pfuApi(path, method, body) {
  const opt = { method: method || 'GET', headers: { 'x-pf-key': PFU.key }, cache: 'no-store' };
  if (body) { opt.headers['content-type'] = 'application/json'; opt.body = JSON.stringify(body); }
  const res = await fetch(path, opt);
  let d = null; try { d = await res.json(); } catch (e) { d = { error: 'HTTP ' + res.status }; }
  return { status: res.status, ok: res.ok, data: d };
}
function pfuFmt(v) { return v == null ? 'N/A' : Math.round(v).toLocaleString('ko-KR'); }
function pfuPct(v, d) { return v == null ? 'N/A' : (v * 100).toFixed(d == null ? 1 : d) + '%'; }
function pfuRenderPlans() {
  const ps = PFU.st.plans;
  document.getElementById('pfuPlans').innerHTML = ps.length ? ps.map(function (p) {
    return '<div style="border-top:1px solid var(--border); padding:8px 0; font-size:13px;"><b>' + escHtml(p.corp_name) + '</b> · ' + escHtml(p.status) + ' · v' + p.version + ' <button style="padding:3px 8px; font-size:12px;" onclick="pfuEditPlan(' + p.plan_key + ')">불러와 수정</button><br/>' +
      '<span style="color:var(--text-muted)">논리:</span> ' + escHtml(p.thesis || '-') + '<br/><span style="color:var(--text-muted)">반증:</span> ' + escHtml(p.disproof || '-') + '<br/>' +
      '<span style="color:var(--text-muted)">매수구간</span> ' + pfuFmt(p.entry_low) + '~' + pfuFmt(p.entry_high) + ' · <span style="color:var(--text-muted)">위험기준가</span> ' + pfuFmt(p.risk_price) + ' · Bear/Base/Bull ' + pfuFmt(p.bear_price) + '/' + pfuFmt(p.base_price) + '/' + pfuFmt(p.bull_price) + (p.review_date ? ' · 검토일 ' + escHtml(p.review_date) : '') + '</div>';
  }).join('') : '<div style="font-size:13px; color:var(--text-muted);">저장된 계획이 없습니다.</div>';
}
function pfuV(id) { return document.getElementById(id).value; }
function pfuEditPlan(key) {
  const p = PFU.st.plans.filter(function (x) { return x.plan_key === key; })[0]; if (!p) return;
  const set = function (id, v) { document.getElementById(id).value = v == null ? '' : v; };
  set('pfpKey', p.plan_key); set('pfpName', p.corp_name); set('pfpStatus', p.status); set('pfpSector', p.sector); set('pfpThesis', p.thesis); set('pfpDis', p.disproof);
  set('pfpRisk', [p.risks, p.alt_considered].filter(Boolean).join(' / ')); set('pfpBear', p.bear_price); set('pfpBase', p.base_price); set('pfpBull', p.bull_price);
  set('pfpEL', p.entry_low); set('pfpEH', p.entry_high); set('pfpRP', p.risk_price); set('pfpMaxQ', p.max_qty); set('pfpSell', p.sell_rules); set('pfpNoAvg', p.no_avgdown_rules); set('pfpReview', p.review_date);
  let tr = []; try { tr = JSON.parse(p.tranche_json || '[]'); } catch (e) {}
  for (let i = 0; i < 3; i++) { set('pfpTc' + i, tr[i] ? tr[i].cond : ''); set('pfpTp' + i, tr[i] ? tr[i].price : ''); set('pfpTw' + i, tr[i] ? tr[i].weightPct : ''); }
  document.getElementById('pfcName').value = p.corp_name; set('pfcRisk', p.risk_price); set('pfcBear', p.bear_price);
  pfuMsg('계획을 불러왔습니다. 수정 후 저장하면 새 버전(v' + (p.version + 1) + ')이 쌓입니다.'); window.scrollTo(0, 0);
}
function pfuClearPlan() { ['pfpKey', 'pfpName', 'pfpSector', 'pfpThesis', 'pfpDis', 'pfpRisk', 'pfpBear', 'pfpBase', 'pfpBull', 'pfpEL', 'pfpEH', 'pfpRP', 'pfpMaxQ', 'pfpSell', 'pfpNoAvg', 'pfpReview'].forEach(function (i) { document.getElementById(i).value = ''; }); for (let i = 0; i < 3; i++) ['pfpTc', 'pfpTp', 'pfpTw'].forEach(function (p) { document.getElementById(p + i).value = ''; }); }
async function pfuSavePlan() {
  const tr = []; for (let i = 0; i < 3; i++) { const c = pfuV('pfpTc' + i), p = pfuV('pfpTp' + i), w = pfuV('pfpTw' + i); if (c || p || w) tr.push({ cond: c, price: p === '' ? null : Number(p), weightPct: w === '' ? null : Number(w) }); }
  const body = { plan_key: pfuV('pfpKey') || null, corp_name: pfuV('pfpName').trim(), status: pfuV('pfpStatus'), sector: pfuV('pfpSector'), thesis: pfuV('pfpThesis'), disproof: pfuV('pfpDis'), risks: pfuV('pfpRisk'), bear_price: pfuV('pfpBear'), base_price: pfuV('pfpBase'), bull_price: pfuV('pfpBull'), entry_low: pfuV('pfpEL'), entry_high: pfuV('pfpEH'), risk_price: pfuV('pfpRP'), max_qty: pfuV('pfpMaxQ'), sell_rules: pfuV('pfpSell'), no_avgdown_rules: pfuV('pfpNoAvg'), review_date: pfuV('pfpReview'), tranches: tr, data_asof: pfuToday() };
  const r = await pfuApi('/api/pf/plan', 'POST', body);
  if (!r.ok) { pfuMsg(r.data.error || '저장 실패', true); return; }
  pfuMsg('계획 저장 완료(v' + r.data.version + ')'); pfuClearPlan(); await pfuLoad();
}
async function pfuQuote() {
  const nm = pfuV('pfcName').trim(); if (!nm) { pfuMsg('종목명을 입력하세요.', true); return; }
  pfuMsg('시세 조회 중...');
  const r = await pfuApi('/api/pf/quote?corp_name=' + encodeURIComponent(nm));
  if (!r.ok) { pfuMsg(r.data.error || '조회 실패', true); return; }
  PFU.quote = r.data; document.getElementById('pfcBuy').value = r.data.close;
  document.getElementById('pfcAdv').value = r.data.adv20 == null ? '' : Math.round(r.data.adv20);
  pfuMsg(r.data.corp_name + ' 종가 ' + pfuFmt(r.data.close) + '원 (' + r.data.date + ')' + (r.data.adv20 == null ? ' · 거래대금 자료 부족' : ' · 최근 ' + r.data.advN + '일 평균 거래대금 ' + pfuFmt(r.data.adv20) + '원'));
}
function pfuCalc() {
  const st = PFU.st, pol = st.policy, out = document.getElementById('pfcOut');
  if (!pol) { out.innerHTML = '<span style="color:#dc2626">먼저 [정책] 탭에서 정책을 저장하세요.</span>'; return; }
  const nm = pfuV('pfcName').trim(); const cur = st.summary.rows.filter(function (r) { return r.name === nm; })[0];
  const res = pfSizePosition({ base: st.summary.total, riskPct: pol.risk_budget_pct, buyPrice: pfuV('pfcBuy'), riskPrice: pfuV('pfcRisk'), maxPosPct: pol.max_position_pct, curPosValue: cur ? cur.mv : 0, cash: st.summary.cash, minCashPct: pol.min_cash_pct, cashNeeds: pol.cash_needs_krw, feePct: pol.fee_rate_pct, adv: pfuV('pfcAdv'), advPct: pol.adv_limit_pct, bearPrice: pfuV('pfcBear') });
  if (!res.ok) { out.innerHTML = '<ul style="color:#dc2626; font-size:13px;">' + res.errors.map(function (e) { return '<li>' + escHtml(e) + '</li>'; }).join('') + '</ul>'; return; }
  let h = '<div style="font-size:15px;"><b>참고 상한: ' + pfuFmt(res.qty) + '주</b> (약 ' + pfuFmt(res.amount) + '원, 매수 후 비중 ' + pfuPct(res.weightAfter) + ', 매수 후 현금 ' + pfuPct(res.cashAfterPct) + ')</div>';
  h += '<div class="meta-line">가장 작은 한도: ' + escHtml(res.binding.join(', ')) + ' · ' + escHtml(res.scenarioRef) + ' 도달 시 예상 손실 ' + pfuFmt(res.scenarioLoss) + '원 (총자산의 ' + pfuPct(res.scenarioLossPct, 2) + ')</div>';
  h += '<table style="margin-top:8px;"><thead><tr><th>한도</th><th>최대 수량</th><th>계산</th></tr></thead><tbody>' + res.limits.map(function (l) { return '<tr><td>' + escHtml(l.label) + '</td><td>' + pfuFmt(l.qty) + '</td><td style="font-size:11.5px">' + escHtml(l.note) + '</td></tr>'; }).join('') + '</tbody></table>';
  const plan = st.plans.filter(function (p) { return p.corp_name === nm; })[0]; let tw = [];
  try { tw = plan ? JSON.parse(plan.tranche_json || '[]') : []; } catch (e) {}
  if (tw.length && res.qty > 0) { const qs = pfSplitTranches(res.qty, tw.map(function (x) { return x.weightPct; })); h += '<div style="margin-top:8px; font-size:13px;"><b>계획된 분할(' + tw.length + '회)에 따른 수량</b>: ' + qs.map(function (q, i) { return (i + 1) + '차 ' + pfuFmt(q) + '주'; }).join(' · ') + '</div>'; }
  h += '<ul style="font-size:12px; color:var(--text-muted); margin:8px 0 0 18px; padding:0;">' + res.warnings.map(function (w) { return '<li>' + escHtml(w) + '</li>'; }).join('') + '<li>이 계산은 고정 위험 비율 방식의 일반적인 실무 규칙입니다. 켈리 기준 등 최적 비중 공식은 입력 추정치(승률·손익비)가 불확실해 쓰지 않았습니다.</li></ul>';
  out.innerHTML = h;
}
async function pfuVoid(id) {
  const why = window.prompt('무효 처리 사유(3자 이상). 기록은 지워지지 않고 계산에서만 제외됩니다.'); if (!why) return;
  const r = await pfuApi('/api/pf/trade/void', 'POST', { id: id, reason: why });
  if (!r.ok) { pfuMsg(r.data.error || '실패', true); return; } await pfuLoad();
}
function pfuRenderBeh() {
  const b = PFU.st.behavior, d = b.disposition, a = b.activity, c = b.compliance, m = b.matrix;
  let h = '<div class="card-title">행동 점검 (내 기록에서 계산 — 판정이 아니라 거울)</div>';
  h += '<div style="font-size:13px; line-height:1.7;"><b>처분효과 점검</b> (이익 종목은 일찍 팔고 손실 종목은 오래 들고 있는 경향; 개인투자자 연구에서 반복 관찰, 너 개인에게 해당하는지는 아래 기록으로 확인)<br/>';
  if (d.enough) h += '이익 매도 ' + d.nWin + '건 평균 보유 ' + Math.round(d.winHold) + '일 / 손실 매도 ' + d.nLose + '건 평균 보유 ' + Math.round(d.loseHold) + '일 → 손실/이익 보유기간 비율 <b>' + (d.ratio == null ? 'N/A' : d.ratio.toFixed(2)) + '</b> (1보다 훨씬 크면 손실 종목을 오래 끄는 경향 가능)';
  else h += '매도 ' + d.n + '건(이익 ' + d.nWin + ', 손실 ' + d.nLose + '). 표본이 부족합니다(매도 10건 이상, 이익·손실 각 3건 이상 필요). 지금은 결론을 내지 않습니다.';
  h += '<br/><b>거래 빈도·비용</b>: 최근 90일 거래 ' + a.trades90 + '건 · 누적 비용 ' + pfuFmt(a.costTotal) + '원 (총자산의 ' + pfuPct(a.costPct, 2) + ')<br/>';
  h += '<b>계획 준수</b>: 매수 ' + c.buys + '건 중 계획 연결 ' + c.planLinked + '건(' + pfuPct(c.planLinkedPct, 0) + '), 경고 승인 후 기록 ' + c.warned + '건(' + pfuPct(c.warnedPct, 0) + '), 평균단가 아래 추가매수 ' + c.avgdown + '건<br/>';
  const emo = Object.keys(b.emotions).map(function (k) { return escHtml(k) + ' ' + b.emotions[k]; }).join(' · '); h += '<b>기록된 감정</b>: ' + (emo || '없음') + '<br/>';
  h += '<b>과정×결과 (사후검토 ' + b.matrixN + '건)</b>: 좋은 과정+좋은 결과 ' + m.goodGood + ' · 좋은 과정+나쁜 결과(운 나쁨 가능) ' + m.goodBad + ' · 나쁜 과정+좋은 결과(운 좋음 가능) ' + m.badGood + ' · 나쁜 과정+나쁜 결과 ' + m.badBad + '</div>';
  h += '<div class="meta-line">짧은 기간·적은 건수에서의 결과는 운과 실력을 구분하기 어렵습니다. 과정 점수 기준은 과정 점검 항목 70% 이상입니다(임의 기준).</div>';
  const k = PFU.st.kelly;
  h += '<div class="card-title" style="margin-top:14px;">켈리 참고값 (보조 — 수량을 정하는 도구가 아님)</div><div style="font-size:13px; line-height:1.7;">';
  if (!k || !k.enough) h += escHtml(k && k.reason ? k.reason : '계산할 수 없습니다.') + ' 지금은 계산하지 않고, 위험 예산 방식(정책·수량 계산기)만 사용합니다.';
  else {
    h += '이익 비율 ' + pfuPct(k.p, 0) + ' · 평균 이익 ' + pfuPct(k.W) + ' · 평균 손실 ' + pfuPct(k.L) + ' (손익비 ' + k.payoff.toFixed(2) + ')<br/>';
    if (!k.edge) h += '<b class="pf-neg">내 기록상 기대값이 0 이하입니다 → 켈리는 비중 0을 가리킵니다.</b> (기록된 방식이 앞으로도 이긴다는 근거가 없다는 뜻이며, 표본·기간에 따라 달라질 수 있습니다.)';
    else h += '풀 켈리 <b>' + pfuPct(k.f) + '</b> · 1/2 켈리 ' + pfuPct(k.half) + ' · <b>1/4 켈리 ' + pfuPct(k.quarter) + '</b> (한 종목에 거는 비중의 이론값)<br/>승률이 ±1 표준오차 흔들리면 풀 켈리가 ' + pfuPct(k.fLow) + ' ~ ' + pfuPct(k.fHigh) + (k.fragile ? ' → <span class="pf-neg">추정이 매우 불안정합니다. 크게 믿지 마세요.</span>' : '');
    h += '<br/>' + (k.capped ? '<span class="pf-neg">계산값이 100%를 넘어 100%로 잘랐습니다(손실을 작게 끊는 기록에서 흔히 나오며, 현물 계좌에선 의미가 없는 값).</span><br/>' : '') + '<span style="color:var(--text-muted); font-size:12px;">정책 최대 비중 ' + (PFU.st.policy ? PFU.st.policy.max_position_pct + '%' : '미설정') + '과 위험 예산 한도가 항상 우선입니다. 켈리는 장기 증가율 최대화 이론값이라 낙폭이 매우 큽니다(그래서 보통 1/2~1/4 배율). 보유기간이 다른 거래를 같은 베팅으로 보고, 거래가 독립적이며 앞으로도 같은 분포라고 가정한 단순화입니다.</span>';
  }
  h += '</div>';
  document.getElementById('pfuBeh').innerHTML = h;
}
async function pfuSaveJournal() {
  const proc = Array.prototype.map.call(document.querySelectorAll('.pfjC'), function (c) { return c.checked; });
  const body = { kind: '사후검토', corp_name: pfuV('pfjName').trim(), outcome_pct: pfuV('pfjOut'), luck_skill: pfuV('pfjLS'), emotion: pfuV('pfjEmo'), process: proc, lesson: pfuV('pfjLesson') };
  const r = await pfuApi('/api/pf/journal', 'POST', body);
  if (!r.ok) { pfuMsg(r.data.error || '저장 실패', true); return; }
  document.getElementById('pfjLesson').value = ''; Array.prototype.forEach.call(document.querySelectorAll('.pfjC'), function (c) { c.checked = false; });
  pfuMsg('일지 저장 완료(과정 점수 ' + Math.round(r.data.process_score) + ')'); await pfuLoad();
}
function pfuRenderJournal() {
  const j = PFU.st.journal;
  document.getElementById('pfuJournal').innerHTML = j.length ? j.slice(0, 20).map(function (e) { return '<div style="border-top:1px solid var(--border); padding:6px 0; font-size:12.5px;">' + escHtml(String(e.created_at).slice(0, 10)) + ' · ' + escHtml(e.corp_name || '-') + ' · 과정 ' + (e.process_score == null ? 'N/A' : Math.round(e.process_score)) + ' · 결과 ' + (e.outcome_pct == null ? 'N/A' : e.outcome_pct + '%') + (e.luck_skill ? ' · ' + escHtml(e.luck_skill) : '') + '<br/>' + escHtml(e.lesson || '') + '</div>'; }).join('') : '';
}
function pfuFillPolicy() {
  const p = PFU.st.policy; if (!p || PFU.polFilled) return; PFU.polFilled = true;
  const m = { pfoRisk: 'risk_budget_pct', pfoMax: 'max_position_pct', pfoCash: 'min_cash_pct', pfoNeeds: 'cash_needs_krw', pfoDD: 'max_dd_tolerance_pct', pfoHz: 'horizon_months', pfoFee: 'fee_rate_pct', pfoTax: 'sell_tax_pct', pfoAdv: 'adv_limit_pct', pfoCd: 'cooldown_hours', pfoNote: 'note' };
  Object.keys(m).forEach(function (id) { document.getElementById(id).value = p[m[id]] == null ? '' : p[m[id]]; });
}
async function pfuSavePolicy() {
  const body = { risk_budget_pct: pfuV('pfoRisk'), max_position_pct: pfuV('pfoMax'), min_cash_pct: pfuV('pfoCash'), cash_needs_krw: pfuV('pfoNeeds'), max_dd_tolerance_pct: pfuV('pfoDD'), horizon_months: pfuV('pfoHz'), fee_rate_pct: pfuV('pfoFee'), sell_tax_pct: pfuV('pfoTax'), adv_limit_pct: pfuV('pfoAdv'), cooldown_hours: pfuV('pfoCd'), note: pfuV('pfoNote') };
  const r = await pfuApi('/api/pf/policy', 'POST', body);
  if (!r.ok) { pfuMsg(r.data.error || '저장 실패', true); return; }
  PFU.polFilled = false; pfuMsg('정책 저장 완료(새 버전)'); await pfuLoad();
}
async function pfuExport() {
  const res = await fetch('/api/pf/export', { headers: { 'x-pf-key': PFU.key } });
  if (!res.ok) { pfuMsg('백업 실패', true); return; }
  const blob = await res.blob(); const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = 'pf_backup.json'; document.body.appendChild(a); a.click(); a.remove();
}


    function switchTab(tab, navMode) {
      const prevTab = curTab; curTab = tab; try { tblFullClose(); } catch (e) {}
      navRecord(tab, navMode || (tab === prevTab ? 'replace' : 'push')); updateBnav(tab); closeMore();
      const tabs = { financial: 'tabFinancial', momentum: 'tabMomentum', fscr: 'tabFscr', macro: 'tabMacro', data: 'tabData', bt: 'tabBt', pf: 'tabPf', db: 'tabDb', guide: 'tabGuide' };
      const btns = { financial: 'tabBtnFinancial', momentum: 'tabBtnMomentum', fscr: 'tabBtnFscr', macro: 'tabBtnMacro', data: 'tabBtnData', bt: 'tabBtnBt', pf: 'tabBtnPf', db: 'tabBtnDb', guide: 'tabBtnGuide' };
      Object.keys(tabs).forEach(function (k) {
        document.getElementById(tabs[k]).style.display = k === tab ? '' : 'none';
        document.getElementById(btns[k]).className = k === tab ? 'toggle-active' : '';
      });
      if (tab === 'momentum' && !screenAutoLoaded) { screenAutoLoaded = true; loadScreen('top', true); }
      if (tab === 'data') { finAutoLoad(); loadDataSummary(); checkMarketStatus(); loadMarketCfg(); rangeStatusShow(); }
      if (tab === 'bt') { btInitUi(); }
      if (tab === 'fscr') { fscInit(); }
      if (tab === 'db') { dbLoad(false); }
      if (tab === 'guide') { guideInit(); }
      if (tab === 'pf') { pfuInit(); }
      if (tab === 'macro' && macroCorrDirty && rawRows.length) computeMacroCorrelation();
      if (tab === 'financial' && subTab === 'table' && chartSel.length) renderChartPanel(false);
      if (tab !== prevTab) { window.scrollTo(0, 0); freshLoad(false); }
      setTimeout(setNavVars, 30); tuneInputs();
      const fab = document.getElementById('pfuFab'); if (fab) fab.style.display = (tab === 'pf' && PFU.key) ? 'block' : 'none';
    }

    async function finAutoLoad() {
      const el = document.getElementById('finAutoBox'); if (!el) return;
      try {
        const res = await fetch('/api/fin/auto-status', { cache: 'no-store' }); const d = await res.json(); if (!res.ok) throw new Error(d.error || '조회 실패');
        let h = '현재 자동 확인 대상 기간: <b>' + (d.periods.length ? d.periods.join(' · ') : '없음(공시 시즌이 아님)') + '</b> · 수집 대기 ' + d.queue + '건<br>';
        if (d.last) h += '마지막 실행 ' + escHtml(d.last.at.slice(0, 16).replace('T', ' ')) + ' UTC: 저장 종목 ' + d.last.companies + '개 중 빈 기간 ' + d.last.missing + '건 확인 → 큐 등록 ' + d.last.queued + '건' + (d.last.dry ? ' (미리보기)' : '');
        else h += '아직 자동 실행 기록이 없습니다.';
        el.innerHTML = h;
      } catch (e) { el.textContent = '오류: ' + e.message; }
    }
    async function finAutoRun(dry) {
      const el = document.getElementById('finAutoBox'); el.textContent = dry ? '확인 중...' : '등록 중...';
      try {
        const res = await adminFetch('/api/fin/auto-run' + (dry ? '?dry=1' : ''), {}); if (!res.ok) throw new Error(res.data.error || '실패');
        const d = res.data; const nm = { '11013': '1Q', '11012': '반기', '11014': '3Q', '11011': '사업' };
        const per = Object.keys(d.byPeriod).map(function (k) { const x = k.split(' '); return x[0] + ' ' + nm[x[1]] + ' ' + d.byPeriod[k] + '건'; });
        el.innerHTML = (dry ? '[미리보기] ' : '') + '대상 기간 ' + (d.periods.join(', ') || '없음') + ' · 저장 종목 ' + d.companies + '개 · 비어 있는 기간 ' + d.missing + '건' + (dry ? '(등록하지 않음)' : ' → 큐에 ' + d.queued + '건 등록') + (per.length ? '<br>기간별: ' + per.join(', ') : '');
      } catch (e) { el.textContent = '오류: ' + e.message; }
    }
    async function loadDataSummary() {
      const el = document.getElementById('dataSummary');
      try {
        const res = await fetch('/api/data/summary', { cache: 'no-store' });
        const d = await res.json();
        if (!res.ok) throw new Error(d.error || '조회 실패');
        const ml = { usdkrw: '환율', msb1y: '통안1Y', ktb3y: '국고3Y', ktb10y: '국고10Y', wti: 'WTI' };
        const macroTxt = d.macro && d.macro.length ? d.macro.map(function (m) { return (ml[m.series] || m.series) + ' ~' + m.last; }).join(' · ') : '없음(거시·상관을 쓰려면 아래에서 갱신)';
        el.innerHTML =
          '• <b>시세(스크리닝)</b>: 최근 거래일 ' + (d.market_last || '없음') + ' · 가격이력 확보 종목 ' + sN(d.backfilled) + '개 / 조건 통과 ' + sN(d.passed) + '개 (대기 ' + sN(d.market_queue) + '개)<br />' +
          '• <b>스크리닝 결과</b>: 저장된 주간 결과 ' + sN(d.screen_snaps) + '회, 최신 ' + (d.screen_last || '없음') + '<br />' +
          '• <b>재무데이터(DART)</b>: ' + sN(d.fin_companies) + '개 종목 · ' + sN(d.fin_rows) + '개 기간 저장 · 서버 대기열 ' + sN(d.fin_queue_rows) + '개 기간(' + sN(d.fin_queue_companies) + '개 종목)' + (d.fin_queue_rows ? ' — 1분에 약 1개 기간씩 처리, 약 ' + Math.ceil(d.fin_queue_rows / 60) + '시간 남음' : '') + '<br />' +
          '• <b>거시경제</b>: ' + macroTxt;
      } catch (e) {
        el.textContent = '현황을 불러오지 못했습니다: ' + e.message;
      }
    }

    // ----- 저장 기준 설정 / 기간 수집 (데이터 관리) -----
    function adminKey() { return keyGet(); }
    async function adminFetch(url, body) {
      // 접근 키는 전역 fetch 래퍼가 자동으로 붙인다. 키가 없거나 틀리면 잠금 화면이 열린다.
      const res = await fetch(url, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body) });
      const d = await res.json().catch(function () { return { error: 'HTTP ' + res.status }; });
      return { ok: res.ok, status: res.status, data: d };
    }
    async function loadMarketCfg() {
      try {
        const res = await fetch('/api/market/config', { cache: 'no-store' }); const d = await res.json();
        if (!res.ok) throw new Error(d.error || '실패');
        document.getElementById('cfgTv').value = d.cfg.minTvEok; document.getElementById('cfgMc').value = d.cfg.minMcapEok;
        document.getElementById('cfgMsg').textContent = '현재 적용 중: 하루 거래대금 ≥ ' + sN(d.cfg.minTvEok) + '억 · 시가총액 ≥ ' + sN(d.cfg.minMcapEok) + '억' + (d.cfg.custom ? ' (사용자 설정, ' + String(d.cfg.updatedAt || '').slice(0, 16).replace('T', ' ') + ')' : ' (기본값)') + (d.protected ? '' : '\\n※ 접근 키(PF_KEY)가 설정되어 있지 않아 누구나 이 기준을 바꿀 수 있습니다. 키를 등록하면 변경에 키가 필요합니다.');
      } catch (e) { document.getElementById('cfgMsg').textContent = '설정을 불러오지 못했습니다: ' + e.message; }
    }
    async function saveMarketCfg() {
      const el = document.getElementById('cfgMsg');
      const tv = document.getElementById('cfgTv').value, mc = document.getElementById('cfgMc').value;
      if (tv === '' || mc === '') { el.textContent = '두 값을 모두 입력하세요(0도 가능).'; return; }
      if (Number(tv) === 0 && Number(mc) <= 100 && !window.confirm('거래대금 0 · 시가총액 ' + mc + '억 이하는 거의 전 종목(약 2,500개 이상)을 저장합니다. 초기 수집에 한 달 이상 걸리고 쓰기 한도를 오래 점유할 수 있습니다. 그래도 저장할까요?')) return;
      el.textContent = '저장 중...';
      const r = await adminFetch('/api/market/config', { min_tv_eok: tv, min_mcap_eok: mc });
      if (!r.ok) { el.textContent = '저장 실패: ' + (r.data.error || r.status); return; }
      await loadMarketCfg();
      el.textContent += '\\n저장했습니다. 다음 일일 수집부터 적용됩니다. 바로 적용하려면 「오늘자 시세 받기」를 누르세요.';
    }
    async function previewCfg() {
      const el = document.getElementById('cfgMsg');
      const tv = document.getElementById('cfgTv').value, mc = document.getElementById('cfgMc').value;
      if (tv === '' || mc === '') { el.textContent = '두 값을 모두 입력하세요.'; return; }
      try {
        el.textContent = '현재 저장 종목 목록 확인 중...';
        const kr = await fetch('/api/market/preview?mode=known'); const kd = await kr.json(); if (!kr.ok) throw new Error(kd.error || '실패');
        const known = new Set(kd.codes); let page = 1, basDt = null, pass = 0, fresh = 0, total = null;
        for (let guard = 0; guard < 40; guard++) {
          const r = await fetch('/api/market/preview?tv=' + encodeURIComponent(tv) + '&mcap=' + encodeURIComponent(mc) + '&page=' + page + (basDt ? '&bas_dt=' + basDt : ''));
          const d = await r.json(); if (!r.ok) throw new Error(d.error || '실패');
          basDt = d.bas_dt; total = d.total; pass += d.pass.length; d.pass.forEach(function (c) { if (!known.has(c)) fresh++; });
          el.textContent = '미리보기 계산 중... ' + Math.min(page * d.size, d.total || 0) + ' / ' + (d.total || 0) + '개 확인';
          if (d.done) break; page++;
        }
        const days = Math.ceil(fresh / 150);
        el.textContent = '기준일 ' + basDt + ' 기준, 하루 거래대금 ≥ ' + sN(Number(tv)) + '억 · 시가총액 ≥ ' + sN(Number(mc)) + '억이면\\n• 통과 종목: ' + sN(pass) + '개 (거래대금 조건만 통과한 종목 ' + sN(total) + '개 중)\\n• 그중 이력이 아직 없어 새로 받아야 할 종목: ' + sN(fresh) + '개 → 하루 150종목씩 약 ' + sN(days) + '일 소요\\n• 매일 추가 저장량: 약 ' + sN(pass) + '행(쓰기 약 ' + sN(pass * 2) + '건/일, 무료 한도 10만 건 중)\\n(아직 저장하지 않았습니다. 마음에 들면 「저장」을 누르세요.)';
      } catch (e) { el.textContent = '미리보기 오류: ' + e.message; }
    }
    function rgFmt(j) {
      if (!j) return '진행 중인 기간 수집이 없습니다.';
      const d = function (x) { return x ? String(x).slice(0, 4) + '-' + String(x).slice(4, 6) + '-' + String(x).slice(6, 8) : ''; };
      return (j.done ? (j.error ? '⚠ 중단: ' + j.error : '✔ 완료') : '진행 중') + ' — ' + d(j.from) + ' ~ ' + d(j.to) + '\\n현재 날짜 ' + d(j.cur > j.to ? j.to : j.cur) + ' · 처리한 거래일 ' + sN(j.tradingDates) + '일 · 휴장 감지 ' + sN(j.skipped) + '일 · 저장 시도 ' + sN(j.rows) + '행\\n적용 기준: 거래대금 ≥ ' + sN(j.minTv / 1e8) + '억 · 시가총액 ≥ ' + sN(j.minMcap / 1e8) + '억 · 오늘 처리 ' + sN(j.budgetRows) + '행(하루 상한 15,000)';
    }
    async function rangeStatusShow() {
      const el = document.getElementById('rgMsg');
      try { const r = await fetch('/api/market/range', { cache: 'no-store' }); const d = await r.json(); if (!r.ok) throw new Error(d.error || '실패'); el.textContent = rgFmt(d.job); } catch (e) { el.textContent = '오류: ' + e.message; }
    }
    async function rangeStart() {
      const el = document.getElementById('rgMsg'); const from = document.getElementById('rgFrom').value, to = document.getElementById('rgTo').value;
      if (!from || !to) { el.textContent = '시작일과 종료일을 모두 선택하세요.'; return; }
      el.textContent = '시작 요청 중...';
      const r = await adminFetch('/api/market/range', { from: from, to: to });
      if (!r.ok) { el.textContent = '시작하지 못했습니다: ' + (r.data.error || r.status); return; }
      el.textContent = '시작했습니다. 서버가 자동으로 이어서 처리합니다(화면을 꺼도 됩니다).\\n' + rgFmt(r.data.job);
    }
    async function rangeCancel() {
      if (!window.confirm('진행 중인 기간 수집을 취소할까요? 이미 저장된 시세는 그대로 남습니다.')) return;
      const r = await adminFetch('/api/market/range', { cancel: true });
      document.getElementById('rgMsg').textContent = r.ok ? '취소했습니다.' : '취소 실패: ' + (r.data.error || r.status);
    }
    async function refreshMarketUniverse() {
      const el = document.getElementById('marketStatus');
      el.textContent = '공공데이터포털에서 오늘자 통과 종목 받는 중...';
      try {
        const res = await fetch('/api/market/refresh-universe?force=1');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '갱신 실패');
        el.textContent = data.bas_dt + ' 기준 ' + data.universe_size + '개 종목(설정한 거래대금 기준 이상)을 확인했습니다. 시세 추가 작업을 시작했고, 약 ' + (data.pages || 1) + '번의 1분 주기(홀수 분)에 나눠 서버가 알아서 처리합니다. 처음 보는 종목은 백필 큐에 등록됩니다(화면을 닫아도 계속 진행됩니다).';
        checkMarketStatus();
      } catch (e) {
        el.textContent = '오류: ' + e.message;
      }
    }

    async function showFunnel() {
      const el = document.getElementById('funnelBox'); el.textContent = '조회 중...';
      try {
        const res = await fetch('/api/market/funnel'); const d = await res.json();
        if (!res.ok) throw new Error(d.error || '조회 실패');
        const a = d.all, t = d.tv_pass, st = d.stored;
        el.textContent = '기준일 ' + d.bas_dt + ' (공공데이터 시세 기준)\\n' +
          '① 그날 시세가 있는 전체 종목(우선주 등 포함): ' + sN(a) + '개\\n' +
          '② 그중 그날 거래대금 ≥ ' + sN(d.min_tv_eok) + '억: ' + sN(t) + '개  (제외 ' + sN(a - t) + '개)\\n' +
          '③ 그중 시가총액 ≥ ' + sN(d.min_mcap_eok) + '억이라 DB에 저장된 종목: ' + sN(st) + '개  (제외 ' + sN(t - st) + '개)\\n' +
          '→ 스크리닝에서는 여기서 다시 보통주만, 그리고 화면의 시총·거래대금·주가 조건을 적용합니다.\\n' +
          '※ 거래대금 기준은 「그날 하루」 값입니다. 조용한 날 거래가 적었던 종목은 그날 저장에서 빠져 이력에 구멍이 생길 수 있고(Z-점수가 N/A), 이 기준을 낮추면 종목은 늘지만 저장·수집 부담이 커집니다.';
      } catch (e) { el.textContent = '오류: ' + e.message; }
    }
    async function checkMarketStatus() {
      const el = document.getElementById('marketStatus');
      el.textContent = '확인 중...';
      try {
        const res = await fetch('/api/market/status');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');
        const aj = data.append_job ? ('  |  일괄 시세 추가 진행: ' + (data.append_job.nextPage - 1) + '/' + Math.ceil(data.append_job.total / data.append_job.pageSize) + '쪽') : '';
        const dl = data.daily_log ? ('  |  마지막 자동 갱신 ' + new Date(data.daily_log.at).toLocaleString('ko-KR') + ': 시세 ' + (data.daily_log.universe || '-') + ', 코스피 ' + (data.daily_log.kospi || '-') + ', 빠진 날 ' + (data.daily_log.gaps || '-') + (data.daily_log.gapJob ? ' (' + data.daily_log.gapJob + ')' : '')) : '  |  자동 갱신 기록 없음';
        el.textContent = '유동성 통과: ' + data.passed_total + '개 (확인한 종목 총 ' + data.checked_total + '개)  |  백필 완료: ' + data.backfilled_total + '개  |  백필 대기: ' + data.queue_remaining + '개  |  오늘 백필: ' + data.backfilled_today + '/' + data.daily_cap + aj + dl;
      } catch (e) {
        el.textContent = '오류: ' + e.message;
      }
    }

    // ===== 모멘텀 스크리닝 화면 (모멘텀 8 + 관심도 2 = 10가지 Top 100) =====
    const SCREEN_LISTS = [
      { key: 'rw', label: '복합 점수(권장)', desc: '「2~3개월」과 「2~6개월」 수익률(최근 1개월 제외)을 후보 안에서의 퍼센타일 순위로 바꿔 가중평균한 점수(0~100)입니다. 극단적인 급등주에 덜 흔들리고 1개월 단기반전을 피합니다.' },
      { key: 'm16', label: '6-1 모멘텀', desc: '21거래일 전 종가 ÷ 126거래일 전 종가 − 1. 최근 1개월을 건너뛴 6개월 모멘텀(Jegadeesh & Titman 1993의 형태).' },
      { key: 'rs6', label: 'RS 6개월', desc: '(1+주식 6개월 수익률) ÷ (1+코스피 6개월 수익률). 1보다 크면 코스피보다 강했습니다. 최근 1개월 포함.' },
      { key: 'rs3', label: 'RS 3개월', desc: '(1+주식 3개월 수익률) ÷ (1+코스피 3개월 수익률). 최근 1개월 포함.' },
      { key: 'rs1', label: 'RS 1개월(반전 주의)', desc: '1개월 수익률은 단기 반전(되돌림) 경향이 있어 단독으로 쓰기엔 위험합니다. 과열 여부를 보는 참고용입니다.' },
      { key: 'r120', label: '120일 수익률', desc: '기준일 종가 ÷ 120거래일 전 종가 − 1. 최근 1개월 포함(참고용).' },
      { key: 'r60', label: '60일 수익률', desc: '기준일 종가 ÷ 60거래일 전 종가 − 1. 최근 1개월 포함(참고용).' },
      { key: 'z10', attn: true, label: '거래대금 Z 10D (관심도)', desc: '기준일 거래대금이 직전 10거래일(기준일 제외) 평균보다 표준편차의 몇 배 높은가. 관심도·발견용 신호이며 사업의 질이나 저평가를 뜻하지 않습니다(거래대금 급증은 뉴스·테마·작전성 매매일 수도 있음). 거래대금 0인 날과 저장 안 된 날은 관측에서 제외하고 유효 관측이 5개 미만이면 N/A입니다.' },
      { key: 'z20', attn: true, label: '거래대금 Z 20D (관심도)', desc: '위와 같되 직전 20거래일 기준. 유효 관측이 10개 미만이면 N/A입니다. 이 앱은 유동성 하한(거래대금 15억) 미만인 날을 저장하지 않으므로, 평소 거래대금이 하한 근처인 종목은 평균이 실제보다 높게 잡혀 Z가 낮게(보수적으로) 나옵니다.' },
      { key: 'r20', label: '20일 수익률', desc: '기준일 종가 ÷ 20거래일 전 종가 − 1. 단기 급등 종목이 많이 섞이며 반전 위험이 가장 큽니다(참고용).' }
    ];
    const SCREEN_PRESETS = { std: [20, 20, 1000], large: [50, 50, 1000], small: [0, 0, 1000] };
    let screenData = null;
    let scrView = ''; try { scrView = localStorage.getItem('scrView') || ''; } catch (e) {}
    function curScrView() { return scrView || (window.innerWidth <= 760 ? 'card' : 'table'); }
    function setScrView(v) { scrView = v; try { localStorage.setItem('scrView', v); } catch (e) {} applyScrView(); }
    function applyScrView() {
      const v = curScrView(), has = !!screenData;
      document.getElementById('screenCards').style.display = has && v === 'card' ? 'block' : 'none';
      document.getElementById('screenWrap').style.display = has && v === 'table' ? '' : 'none';
      document.getElementById('scrBar').style.visibility = v === 'table' ? '' : 'hidden';
      document.getElementById('scrV_card').className = v === 'card' ? 'toggle-active' : ''; document.getElementById('scrV_table').className = v === 'table' ? 'toggle-active' : '';
    }
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
        screenData = null; // 실패했는데 이전 결과 표가 새 조건의 결과처럼 남아 있지 않게 비운다
        document.getElementById('screenRegime').textContent = '표시할 스크리닝 결과가 없습니다.';
        ['screenCards', 'screenWrap', 'screenDesc'].forEach(function (id) { const x = document.getElementById(id); if (x) x.innerHTML = ''; });
        const tb = document.getElementById('screenTabs'); if (tb) { tb.innerHTML = ''; tb.style.display = 'none'; }
      }
    }

    function renderScreen() { return guardedRender('스크리닝 결과', 'screenCards', renderScreenImpl); }
    function renderScreenImpl() {
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
        stTxt = '기준일 시세 ' + sN(meta.asof_rows) + '종목 중 보통주 · ' + (meta.pcMcap != null ? '시총 하위 ' + meta.pcMcap + '% 제외 · 20일 평균 거래대금 하위 ' + meta.pcTv + '% 제외' : '시총 ≥ ' + sN(meta.minMcapEok) + '억 · 20일 평균 거래대금 ≥ ' + sN(meta.minTvEok) + '억') + (meta.cutMcEok != null ? ' (이번 컷: 시총 ' + sN(meta.cutMcEok) + '억 · 20일 평균 거래대금 ' + sX(meta.cutTvEok, 1) + '억 이상만 통과)' : '') + ' · 주가 ≥ ' + sN(meta.minPrice) + '원 통과 ' + sN(meta.passed_floor) + '종목 · 각 목록 상위 ' + (meta.topN || 50) + '개' +
          ' · 복합 가중 2~3개월 ' + sX(w.w3) + ' / 2~6개월 ' + sX(w.w6) + (screenData.saved ? ' · 주간 결과로 저장됨' : '');
      } else {
        stTxt = '구버전 결과: 시총 ≥ ' + sN(meta.minMcapEok) + '억 · 하루 거래대금 ≥ ' + sN(meta.minTvEok) + '억';
      }
      if (meta.no_mcap > 0 && meta.asof_rows > 0 && meta.no_mcap / meta.asof_rows > 0.2) {
        stTxt += ' · 주의: 기준일 행 중 시가총액이 아직 저장되지 않은 종목이 ' + sN(meta.no_mcap) + '개 있어 결과에서 빠졌습니다(데이터 관리에서 "오늘자 시세 받기"를 한 번 누르고 몇 분 뒤 다시 실행하세요).';
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
      SCREEN_LISTS.forEach((L) => { if (L.attn) return; (lists[L.key] || []).forEach((r) => { overlap[r.stock_code] = (overlap[r.stock_code] || 0) + 1; }); });
      const kc = (col, extra) => ((screenTab === col ? 'sc-key ' : '') + (extra || '')).trim();
      const fh = (n, t) => '<th class="f f' + n + '">' + t + '</th>';
      const head = '<tr>' + fh(1, '순위') + fh(2, '종목') + fh(3, '현재가') + fh(4, '시총(억)') + fh(5, '20일평균<br />거래대금(억)') + fh(6, '영업이익(억)') +
        '<th>1개월</th><th class="' + kc('r20') + '">20일</th><th class="' + kc('r60') + '">60일</th><th class="' + kc('r120') + '">120일</th><th class="' + kc('rw') + '">복합점수</th>' +
        '<th class="' + kc('m16') + '">6-1</th><th class="' + kc('rs1') + '">RS 1M</th><th class="' + kc('rs3') + '">RS 3M</th><th class="' + kc('rs6') + '">RS 6M</th><th class="' + kc('z10') + '" title="거래대금 Z-점수 10일(관심도)">Z10</th><th class="' + kc('z20') + '" title="거래대금 Z-점수 20일(관심도)">Z20</th><th title="21거래일 전 ÷ 252거래일 전 − 1 (참고용)">12-1</th><th title="기준일 종가 ÷ 252거래일 최고 종가 (참고용)">52주 고가 대비</th><th>겹친 목록</th></tr>';
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
        const extraCells = (x) => {
          const why = x.extra === 'jump' ? '가격 불연속' : '자료 부족';
          const tip = x.extra === 'jump' ? '하루 ±30% 초과 변동(액면분할·병합 등 의심)이 있어 계산하지 않음' : '저장된 가격 이력이 253거래일 미만이라 계산하지 않음';
          if (x.m121 == null && x.hi52 == null) return '<td title="' + tip + '" style="color:var(--text-muted); font-size:11px;">' + why + '</td><td title="' + tip + '" style="color:var(--text-muted); font-size:11px;">' + why + '</td>';
          return ret(x.m121, 'm121') + sCell(x.hi52 == null ? 'N/A' : x.hi52.toFixed(2), '');
        };
        const hot = r.hot ? ' <span class="sc-warn" title="최근 1개월 수익률이 후보 중 상위 5%: 단기 과열·반전 위험">⚠</span>' : '';
        const comp = newFmt ? sCell(r.rw == null ? 'N/A' : (r.rw * 100).toFixed(1), kc('rw')) : ret(r.rw, 'rw');
        return '<tr><td class="f f1">' + (i + 1) + '</td>' +
          '<td class="f f2"><a href="javascript:void(0)" onclick="goAnalyzeCode(&quot;' + r.stock_code + '&quot;)" title="클릭하면 종목분석으로 이동" style="color:var(--primary-dark); font-weight:700; text-decoration:none;">' + escHtml(r.corp_name) + '</a>' + hot + '<br /><span style="color:var(--text-muted); font-size:11px;">' + r.stock_code + '</span><span class="ind" data-ind="' + r.stock_code + '" style="color:var(--text-muted); font-size:11px;"></span>' + (r.corp_name !== r.stock_code ? '<div class="sc-ctl"><label class="sc-fin-s" title="재무데이터 받기 선택(최대 20개)"><input type="checkbox" data-finsel="' + escHtml(r.corp_name) + '"' + (finSel.indexOf(r.corp_name) >= 0 ? ' checked' : '') + ' /><span>재무</span></label><button class="sc-cmp-s" onclick="goAnalyzeCode(&quot;' + r.stock_code + '&quot;)" title="종목분석으로 이동">분석 ›</button><button class="sc-cmp-s" data-cmp="' + escHtml(r.corp_name) + '" title="비교에 담기">⚖</button></div>' : '') + '<div class="mini"><b>' + sN(r.c0) + '원</b><br />시총 ' + sN(r.mcapEok) + '억 · 대금 ' + sN(r.tvEok) + '억<br />영업이익 ' + (r.op ? sN(r.op.v) + '억' : (opFillRunning ? '조회 중…' : 'N/A')) + '</div></td>' +
          '<td class="f f3">' + sN(r.c0) + '</td><td class="f f4">' + sN(r.mcapEok) + '</td><td class="f f5">' + sN(r.tvEok) + '</td>' + opCell +
          ret(r.r21, 'r21') + ret(r.r20, 'r20') + ret(r.r60, 'r60') + ret(r.r120, 'r120') + comp + ret(r.m16, 'm16') + rs(r.rs1, 'rs1') + rs(r.rs3, 'rs3') + rs(r.rs6, 'rs6') + sCell(r.z10 == null ? 'N/A' : sX(r.z10, 1), kc('z10')) + sCell(r.z20 == null ? 'N/A' : sX(r.z20, 1), kc('z20')) + extraCells(r) +
          sCell((overlap[r.stock_code] || 0) + '/' + SCREEN_LISTS.filter((L) => !L.attn).length, overlap[r.stock_code] >= 4 ? 'sc-good' : '') + '</tr>';
      }).join('');
      const wrap = document.getElementById('screenWrap');
      wrap.innerHTML = '<table class="scr" style="width:max-content; min-width:100%;"><thead>' + head + '</thead><tbody>' + (body || '<tr><td colspan="20">' + (((screenData.meta || {}).missingLists || []).indexOf(screenTab) >= 0 ? '이 저장된 주간 결과는 이 목록이 추가되기 전에 만들어져 해당 목록이 없습니다. 위의 <b>스크리닝 실행</b>을 눌러 지금 계산하면 나타납니다(다음 토요일 자동 저장분부터는 항상 포함됩니다).' : '조건에 맞는 종목이 없습니다(가격 이력이 부족하거나 조건이 너무 엄격할 수 있습니다).') + '</td></tr>') + '</tbody></table>';
      wrap.style.display = '';
      const keyTxt = function (r) {
        const m = { rw: newFmt ? (r.rw == null ? 'N/A' : (r.rw * 100).toFixed(1)) : sP(r.rw), m16: sP(r.m16), rs1: sX(r.rs1, 3), rs3: sX(r.rs3, 3), rs6: sX(r.rs6, 3), r20: sP(r.r20), r60: sP(r.r60), r120: sP(r.r120), z10: sX(r.z10, 1), z20: sX(r.z20, 1) };
        return m[screenTab] == null ? 'N/A' : m[screenTab];
      };
      const pc = function (v) { return '<div class="' + (v != null && v > 0 ? 'sc-good' : (v != null && v < 0 ? 'sc-bad' : '')) + '">' + sP(v) + '</div>'; };
      const cards = rows.map(function (r, i) {
        const canCmp = r.corp_name !== r.stock_code;
        const op = r.op ? sN(r.op.v) + '억' + (r.op.year ? ' (' + r.op.year + ')' : '') : (opFillRunning ? '조회 중…' : 'N/A');
        const nSel = SCREEN_LISTS.filter(function (L) { return !L.attn; }).length;
        return '<div class="sc-card"><div class="sc-h"><span class="sc-rk">' + (i + 1) + '</span><div><span class="sc-nm" onclick="goAnalyzeCode(&quot;' + r.stock_code + '&quot;)">' + escHtml(r.corp_name) + '</span>' + (r.hot ? ' <span class="sc-warn" title="최근 1개월 수익률 상위 5%: 단기 과열·반전 위험">⚠</span>' : '') + '<div class="sc-cd">' + escHtml(r.stock_code) + '<span class="ind" data-ind="' + escHtml(r.stock_code) + '"></span> · 중복 ' + (overlap[r.stock_code] || 0) + '/' + nSel + '</div></div>' +
          '<div class="sc-big"><b>' + keyTxt(r) + '</b><small>' + (curL ? escHtml(curL.label.replace(/\(.*\)/, '')) : '') + '</small></div></div>' +
          '<div class="sc-grid"><div><small>1개월</small>' + pc(r.r21).replace('<div', '<span').replace('</div>', '</span>') + '</div><div><small>20일</small>' + pc(r.r20).replace('<div', '<span').replace('</div>', '</span>') + '</div><div><small>60일</small>' + pc(r.r60).replace('<div', '<span').replace('</div>', '</span>') + '</div><div><small>120일</small>' + pc(r.r120).replace('<div', '<span').replace('</div>', '</span>') + '</div></div>' +
          '<div class="sc-sub">현재가 ' + sN(r.c0) + '원 · 시총 ' + sN(r.mcapEok) + '억 · 거래대금(20일평균) ' + sN(r.tvEok) + '억<br />영업이익 ' + op + '</div>' +
          '<details class="adv"><summary>상세 지표</summary><div class="sc-grid" style="grid-template-columns:repeat(3,1fr);"><div><small>6-1</small>' + sP(r.m16) + '</div><div><small>12-1</small>' + sP(r.m121) + '</div><div><small>52주고점비</small>' + (r.hi52 == null ? 'N/A' : r.hi52.toFixed(2)) + '</div><div><small>RS 1M</small>' + sX(r.rs1, 3) + '</div><div><small>RS 3M</small>' + sX(r.rs3, 3) + '</div><div><small>RS 6M</small>' + sX(r.rs6, 3) + '</div><div><small>Z10</small>' + sX(r.z10, 1) + '</div><div><small>Z20</small>' + sX(r.z20, 1) + '</div><div><small>복합점수</small>' + (newFmt ? (r.rw == null ? 'N/A' : (r.rw * 100).toFixed(1)) : sP(r.rw)) + '</div></div></details>' +
          '<div class="sc-act"><button class="primary" onclick="goAnalyzeCode(&quot;' + r.stock_code + '&quot;)">분석 →</button>' + (canCmp ? '<button class="sc-cmp" data-cmp="' + escHtml(r.corp_name) + '">⚖ 비교 담기</button><label class="sc-fin"><input type="checkbox" data-finsel="' + escHtml(r.corp_name) + '"' + (finSel.indexOf(r.corp_name) >= 0 ? ' checked' : '') + ' />재무 받기</label>' : '') + '</div></div>';
      }).join('');
      document.getElementById('screenCards').innerHTML = cards || '<div class="meta-line">표시할 종목이 없습니다.</div>';
      applyScrView(); cmpTrayRender(); indPaint(); indEnsureScreen();
    }

    // 내가 고른 종목(최대 FIN_SEL_MAX개)의 재무데이터를 서버 대기열에 등록한다(서버가 1분에 약 1개 기간씩 DART에서 받아 저장).
    // 올해부터 11년치(올해 포함 11개 연도 × 4개 보고서). 이미 받은 기간은 서버가 건너뛴다(skip_existing).
    const FIN_SEL_MAX = 20, FIN_YEARS = 11;
    let finSel = [];
    function finSelSync() {
      Array.prototype.forEach.call(document.querySelectorAll('[data-finsel]'), function (c) { c.checked = finSel.indexOf(c.getAttribute('data-finsel')) >= 0; });
      const b = document.getElementById('btnFinPrep'); if (b) b.textContent = '📥 선택한 종목 재무데이터 받기 (' + finSel.length + '/' + FIN_SEL_MAX + ')';
    }
    function finSelToggle(name, on, box) {
      const i = finSel.indexOf(name), st = document.getElementById('prepStatus');
      if (on && i < 0) {
        if (finSel.length >= FIN_SEL_MAX) { if (box) box.checked = false; if (st) st.textContent = '최대 ' + FIN_SEL_MAX + '개까지 선택할 수 있습니다. 다른 종목을 고르려면 먼저 하나를 해제하세요.'; return; }
        finSel.push(name);
      } else if (!on && i >= 0) finSel.splice(i, 1);
      if (st) st.textContent = '';
      finSelSync();
    }
    function finSelClear() { finSel = []; const st = document.getElementById('prepStatus'); if (st) st.textContent = ''; finSelSync(); }
    document.addEventListener('change', function (ev) {
      const c = ev.target && ev.target.closest ? ev.target.closest('[data-finsel]') : null;
      if (c) finSelToggle(c.getAttribute('data-finsel'), c.checked, c);
    });
    async function prepareFinancials() {
      const st = document.getElementById('prepStatus');
      if (!finSel.length) { st.textContent = '먼저 카드나 표에서 재무데이터를 받을 종목을 고르세요(최대 ' + FIN_SEL_MAX + '개).'; return; }
      const names = finSel.slice(0, FIN_SEL_MAX);
      const y1 = new Date().getFullYear(), y0 = y1 - (FIN_YEARS - 1);
      const maxPeriods = names.length * FIN_YEARS * 4;
      if (!confirm(names.length + '개 종목(' + y0 + '~' + y1 + '년, 종목당 최대 ' + (FIN_YEARS * 4) + '개 기간)의 재무데이터를 서버 대기열에 등록합니다. 이미 받은 기간은 건너뛰고, 서버가 1분에 약 1개 기간씩 받으므로 처음 받는 종목만 있다면 최대 약 ' + Math.ceil(maxPeriods / 60) + '시간 걸립니다(화면을 꺼도 계속 진행됩니다). 아직 공시되지 않은 기간은 받지 못하고 건너뜁니다. 진행할까요?')) return;
      let ok = 0, fail = 0, queued = 0, skipped = 0;
      for (let i = 0; i < names.length; i++) {
        st.textContent = '대기열 등록 중... ' + (i + 1) + '/' + names.length;
        try {
          const res = await fetch('/api/queue-fetch?skip_existing=1&corp_name=' + encodeURIComponent(names[i]) + '&start_year=' + y0 + '&end_year=' + y1);
          if (res.ok) { ok++; const d = await res.json(); queued += d.queued || 0; skipped += d.skipped || 0; } else fail++;
        } catch (e) { fail++; }
      }
      st.textContent = ok + '개 종목 등록: 새로 받을 기간 ' + queued + '개 · 이미 있어 건너뜀 ' + skipped + '개' + (fail ? ' (' + fail + '개 종목은 등록 실패)' : '') + (queued ? ' · 예상 약 ' + Math.max(1, Math.ceil(queued / 60)) + '시간' : '') + '. 데이터 관리에서 진행 상황을 볼 수 있습니다.';
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
      const cols = ['목록', '순위', '종목명', '종목코드', '현재가', '시가총액(억원)', '20일평균거래대금(억원)', '영업이익(억원)', '영업이익연도', '영업이익출처', '1개월', '20일', '60일', '120일', '복합점수(0~100)', '6-1', 'RS1개월', 'RS3개월', 'RS6개월', 'Z10(거래대금)', 'Z20(거래대금)', '12-1(참고)', '52주고가대비(참고)'];
      const n = (v, d) => (v == null ? '' : Number(v).toFixed(d == null ? 4 : d));
      const newFmt = !!((screenData.meta || {}).weights && screenData.meta.weights.w3 != null);
      const lines = [cols.join(',')];
      const lists = screenData.lists || {};
      SCREEN_LISTS.forEach((L) => {
        (lists[L.key] || []).forEach((r, i) => {
          const o = r.op || {};
          lines.push(['"' + L.label + '"', i + 1, '"' + String(r.corp_name).split('"').join('""') + '"', r.stock_code, n(r.c0, 0), n(r.mcapEok, 0), n(r.tvEok, 1), n(o.v, 1), o.year || '', o.src || '',
            n(r.r21), n(r.r20), n(r.r60), n(r.r120), newFmt ? n(r.rw == null ? null : r.rw * 100, 1) : n(r.rw), n(r.m16), n(r.rs1), n(r.rs3), n(r.rs6), n(r.z10, 2), n(r.z20, 2), n(r.m121), n(r.hi52, 3)].join(','));
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
    // ===== 차트 독(주가 + 재무 추이 프리셋): 어느 하위 탭에서든 맨 위에 보인다 =====
    const DOCK_PRESETS = {
      growth: ['revenue', 'operating_income', '_opMargin'],
      margin: ['_grossMargin', '_opMargin'],
      cash: ['ocf', 'capex', 'fcf'],
      value: ['per_at_filing', 'pbr_at_filing', 'fcf_yield_at_filing'],
      safety: ['total_liabilities', 'total_equity', 'cash'],
      holder: ['dividend_per_share', '_payoutRatio', 'total_shares'],
    };
    let dockMode = 'price';
    function syncDockView() {
      document.getElementById('dockQ').className = viewMode === 'quarterly' ? 'toggle-active' : '';
      document.getElementById('dockA').className = viewMode === 'annual' ? 'toggle-active' : '';
    }
    function toggleDockFold() {
      const d = document.getElementById('chartDock'), f = d.classList.toggle('folded');
      document.getElementById('dockFold').textContent = f ? '▾ 펼치기' : '▴ 접기';
      if (!f) { if (dockMode === 'price') drawPrice(); else renderChartPanel(false); }
    }
    function toggleSearchCompact() {
      const c = document.getElementById('searchCard'), on = c.classList.toggle('compact');
      document.getElementById('btnSearchMore').textContent = on ? '▾ 더보기' : '▴ 접기';
    }
    function setNavVars() {
      const n = document.getElementById('mainNav'); if (n) document.documentElement.style.setProperty('--navh', n.offsetHeight + 'px');
      const sb = document.getElementById('stockSubnav'); const vis = sb && sb.offsetParent !== null;
      document.documentElement.style.setProperty('--subh', vis ? sb.offsetHeight + 'px' : '0px');
    }
    window.addEventListener('resize', setNavVars);
    window.addEventListener('scroll', function () { const f = document.getElementById('fabTop'); if (f) f.style.display = window.scrollY > 500 ? 'block' : 'none'; }, { passive: true });
    function setDock(mode) {
      dockMode = mode;
      if (document.getElementById('chartDock').classList.contains('folded')) { document.getElementById('chartDock').classList.remove('folded'); document.getElementById('dockFold').textContent = '▴ 접기'; }
      ['price', 'growth', 'margin', 'cash', 'value', 'safety', 'holder'].forEach((k) => { document.getElementById('dockBtn_' + k).className = k === mode ? 'toggle-active' : ''; });
      syncDockView();
      const isPrice = mode === 'price';
      document.getElementById('priceBox').style.display = isPrice ? '' : 'none';
      document.getElementById('dockViewRow').style.display = isPrice ? 'none' : '';
      if (isPrice) { document.getElementById('chartWrap').style.display = 'none'; drawPrice(); return; }
      if (DOCK_PRESETS[mode]) {
        chartSel = DOCK_PRESETS[mode].map((key) => { const col = COLUMNS.filter((c) => c.key === key)[0]; return { key: key, label: col ? col.label : key }; });
        chartOverlay = false; chartHoverIdx = null;
      }
      renderChartPanel(false);
    }

    // ----- 일별 주가 차트 -----
    function smaSeries(arr, n) {
      const out = new Array(arr.length).fill(null);
      let sum = 0;
      for (let i = 0; i < arr.length; i++) {
        sum += arr[i];
        if (i >= n) sum -= arr[i - n];
        if (i >= n - 1) out[i] = sum / n;
      }
      return out;
    }
    function drawdownSeries(arr) {
      let peak = -Infinity;
      return arr.map((v) => { if (v > peak) peak = v; return v / peak - 1; });
    }
    // 하루 ±30% 초과 변동이 난 인덱스(액면분할·병합 등으로 가격이 끊겼을 가능성). 상·하한가 한계가 30%라 이를 넘으면 시장 움직임이 아니다.
    function priceJumps(arr, limit) {
      const lim = limit || 0.3, out = [];
      for (let i = 1; i < arr.length; i++) { const r = arr[i] / arr[i - 1] - 1; if (r > lim || r < -lim) out.push(i); }
      return out;
    }
    function priceStats(c) {
      const n = c.length; if (n < 2) return null;
      const last = c[n - 1], w = c.slice(Math.max(0, n - 252));
      const hi = Math.max.apply(null, w), lo = Math.min.apply(null, w);
      const m50 = n >= 50 ? smaSeries(c, 50)[n - 1] : null, m200 = n >= 200 ? smaSeries(c, 200)[n - 1] : null;
      const dd = drawdownSeries(w);
      return { last: last, hi: hi, lo: lo, fromHi: last / hi - 1, fromLo: last / lo - 1, vsM50: m50 ? last / m50 - 1 : null, vsM200: m200 ? last / m200 - 1 : null, maxDD: Math.min.apply(null, dd), nWin: w.length };
    }
    let PRICE = { name: null, data: null, err: null, range: 252, hover: null, geom: null, m50: [], m200: [], listeners: false, vm: 'tv' };
    try { const vm = localStorage.getItem('pxVm'); if (vm === 'tv' || vm === 'vol' || vm === 'off') PRICE.vm = vm; } catch (e) {}
    function setVolMode(m) {
      PRICE.vm = m; try { localStorage.setItem('pxVm', m); } catch (e) {}
      ['tv', 'vol', 'off'].forEach((k) => { const b = document.getElementById('volBtn_' + k); if (b) b.className = k === m ? 'toggle-active' : ''; });
      drawPrice();
    }
    function fmtVol(v, mode) {
      if (v == null) return 'N/A';
      if (mode === 'tv') { const e = v / 1e8; return e >= 10000 ? (e / 10000).toFixed(1) + '조' : (e >= 100 ? Math.round(e).toLocaleString() : e.toFixed(1)) + '억'; }
      return v >= 1e6 ? (v / 1e4).toLocaleString(undefined, { maximumFractionDigits: 0 }) + '만' : (v >= 1e4 ? (v / 1e4).toFixed(1) + '만' : Math.round(v).toLocaleString());
    }
    function setPriceRange(n) {
      PRICE.range = n; PRICE.hover = null;
      [126, 252, 9999].forEach((k) => { document.getElementById('rngBtn_' + k).className = k === n ? 'toggle-active' : ''; });
      drawPrice();
    }
    async function loadPriceSeries() {
      const name = document.getElementById('corpName').value.trim();
      if (!name) return;
      if (PRICE.name === name && PRICE.data) { renderPriceStats(); drawPrice(); return; }
      PRICE.name = name; PRICE.data = null; PRICE.err = null;
      document.getElementById('priceStats').textContent = '일별 주가를 불러오는 중…';
      document.getElementById('pxNote').textContent = '';
      const cv = document.getElementById('priceCanvas'); cv.getContext('2d').clearRect(0, 0, cv.width, cv.height);
      try {
        const res = await fetch('/api/price/series?corp_name=' + encodeURIComponent(name));
        const data = await res.json();
        if (PRICE.name !== name) return;
        if (!res.ok) throw new Error(data.error || '조회 실패');
        PRICE.data = data; PRICE.m50 = smaSeries(data.c, 50); PRICE.m200 = smaSeries(data.c, 200);
      } catch (e) { if (PRICE.name !== name) return; PRICE.err = e.message || String(e); }
      renderPriceStats(); drawPrice();
      try { sigRefresh(); } catch (e) { /* 시그널 갱신 실패가 차트를 막지 않게 */ }
    }
    function renderPriceStats() {
      const el = document.getElementById('priceStats'), note = document.getElementById('pxNote');
      if (PRICE.err) { el.innerHTML = '<span style="color:var(--danger);">일별 주가를 불러오지 못했습니다: ' + escHtml(PRICE.err) + '</span>'; note.textContent = ''; return; }
      if (!PRICE.data) return;
      const st = priceStats(PRICE.data.c);
      if (!st) { el.textContent = '주가 데이터가 너무 적어 차트를 그리지 못합니다.'; return; }
      const pc = (v) => v == null ? 'N/A' : '<b style="color:' + (v >= 0 ? '#dc2626' : '#2563eb') + ';">' + (v >= 0 ? '+' : '') + (v * 100).toFixed(1) + '%</b>';
      el.innerHTML = '<b>' + escHtml(PRICE.data.corp_name) + '</b> ' + PRICE.data.to + ' 종가 <b>' + Math.round(st.last).toLocaleString() + '원</b><br />' +
        '52주 고가 대비 ' + pc(st.fromHi) + ' (' + Math.round(st.hi).toLocaleString() + '원) · 52주 저가 대비 ' + pc(st.fromLo) + ' · 50일선 대비 ' + pc(st.vsM50) + ' · 200일선 대비 ' + pc(st.vsM200) + ' · 최근 ' + st.nWin + '거래일 최대낙폭 ' + pc(st.maxDD);
      const jumps = priceJumps(PRICE.data.c);
      note.innerHTML = '종가 기준(장중 고저가 아님), 수정주가가 아니라 액면분할·병합·감자가 있으면 가격이 끊겨 보입니다' +
        (jumps.length ? ' — <span style="color:#b45309;">⚠ 하루 ±30% 초과 변동 ' + jumps.length + '회(' + jumps.slice(0, 3).map((i) => PRICE.data.d[i]).join(', ') + (jumps.length > 3 ? ' 등' : '') + '): 해당 구간의 낙폭·이동평균은 왜곡됐을 수 있습니다</span>' : '') +
        '. 이동평균·52주 고가 같은 추세 지표가 한국 시장에서 초과수익을 낸다는 점은 검증되지 않았습니다(참고용). 마우스를 올리거나 손가락으로 훑으면 값이 보입니다.';
    }
    function drawPrice() {
      const cv = document.getElementById('priceCanvas');
      if (!cv || !PRICE.data || dockMode !== 'price') return;
      const ctx = cv.getContext('2d'), dpr = window.devicePixelRatio || 1;
      const W = cv.clientWidth, H = cv.clientHeight;
      if (!W || !H) return;
      cv.width = W * dpr; cv.height = H * dpr; ctx.setTransform(dpr, 0, 0, dpr, 0, 0); ctx.clearRect(0, 0, W, H);
      const full = PRICE.data, n = full.c.length, k = Math.min(n, PRICE.range), from = n - k;
      const c = full.c.slice(from), d = full.d.slice(from), m50 = PRICE.m50.slice(from), m200 = PRICE.m200.slice(from);
      const dd = drawdownSeries(c);
      const showM50 = document.getElementById('chkMa50').checked, showM200 = document.getElementById('chkMa200').checked;
      const FONT = '-apple-system, "Segoe UI", "Noto Sans KR", sans-serif';
      const vmode = (PRICE.vm === 'off') ? 'off' : ((PRICE.vm === 'tv' && full.tv && full.tv.some((x) => x != null)) ? 'tv' : (full.v && full.v.some((x) => x != null) ? 'vol' : 'off'));
      const volSrc = vmode === 'tv' ? full.tv : full.v, vol = vmode === 'off' ? [] : volSrc.slice(from);
      const padL = 54, padR = 10, mainTop = 8, mainH = Math.round(H * (vmode === 'off' ? 0.66 : 0.5)), volTop = mainTop + mainH + 18, volH = vmode === 'off' ? 0 : Math.round(H * 0.15), ddTop = vmode === 'off' ? mainTop + mainH + 20 : volTop + volH + 16, ddH = H - ddTop - 20;
      const plotW = W - padL - padR, step = k > 1 ? plotW / (k - 1) : 0;
      const xs = (i) => padL + i * step;
      PRICE.geom = { padL: padL, step: step, k: k };
      const st = priceStats(full.c);
      let lo = Infinity, hi = -Infinity;
      const feed = (v) => { if (v != null) { if (v < lo) lo = v; if (v > hi) hi = v; } };
      c.forEach(feed); if (showM50) m50.forEach(feed); if (showM200) m200.forEach(feed); feed(st.hi); feed(st.lo);
      const pad = (hi - lo) * 0.06; lo -= pad; hi += pad;
      const ys = (v) => mainTop + mainH - (v - lo) / (hi - lo) * mainH;
      ctx.font = '10px ' + FONT; ctx.textAlign = 'right'; ctx.fillStyle = '#94a3b8'; ctx.strokeStyle = '#eef2f7'; ctx.lineWidth = 1;
      for (let g = 0; g <= 3; g++) { const v = lo + (hi - lo) * g / 3, y = ys(v); ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(padL + plotW, y); ctx.stroke(); ctx.fillText(Math.round(v).toLocaleString(), padL - 6, y + 3); }
      ctx.setLineDash([5, 4]); ctx.lineWidth = 1;
      [[st.hi, '#dc2626', '52주 고가'], [st.lo, '#2563eb', '52주 저가']].forEach((l) => { const y = ys(l[0]); ctx.strokeStyle = l[1]; ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(padL + plotW, y); ctx.stroke(); ctx.fillStyle = l[1]; ctx.textAlign = 'left'; ctx.fillText(l[2], padL + 3, y - 3); });
      ctx.setLineDash([]);
      const line = (arr, color, w) => { ctx.strokeStyle = color; ctx.lineWidth = w; ctx.lineJoin = 'round'; ctx.beginPath(); let s0 = false; arr.forEach((v, i) => { if (v == null) { s0 = false; return; } if (!s0) { ctx.moveTo(xs(i), ys(v)); s0 = true; } else ctx.lineTo(xs(i), ys(v)); }); ctx.stroke(); };
      if (showM200) line(m200, '#7c3aed', 1.4);
      if (showM50) line(m50, '#ea580c', 1.4);
      line(c, '#2563eb', 1.8);
      priceJumps(c).forEach((i) => { ctx.strokeStyle = '#f59e0b'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(xs(i), mainTop); ctx.lineTo(xs(i), mainTop + mainH); ctx.stroke(); });
      ctx.textAlign = 'left'; ctx.font = '11px ' + FONT; let lx = padL + 4;
      [['종가', '#2563eb', true], ['50일선', '#ea580c', showM50], ['200일선', '#7c3aed', showM200]].forEach((l) => { if (!l[2]) return; ctx.fillStyle = l[1]; ctx.fillRect(lx, mainTop + mainH + 3, 10, 3); ctx.fillStyle = '#475569'; ctx.fillText(l[0], lx + 14, mainTop + mainH + 8); lx += 14 + ctx.measureText(l[0]).width + 12; });
      if (vmode !== 'off') {
        const vmax = Math.max.apply(null, vol.filter((x) => x != null).concat([1]));
        const yv = (v) => volTop + volH - v / vmax * volH, bw = Math.max(1, Math.min(8, step * 0.7));
        ctx.strokeStyle = '#eef2f7'; ctx.lineWidth = 1; ctx.beginPath(); ctx.moveTo(padL, volTop + volH); ctx.lineTo(padL + plotW, volTop + volH); ctx.stroke();
        vol.forEach((v, i) => { if (v == null) return; const up = i > 0 ? c[i] >= c[i - 1] : true; ctx.fillStyle = up ? 'rgba(220,38,38,0.55)' : 'rgba(37,99,235,0.55)'; ctx.fillRect(xs(i) - bw / 2, yv(v), bw, volTop + volH - yv(v)); });
        const va = []; for (let i = 0; i < vol.length; i++) { if (i < 19) { va.push(null); continue; } let sum = 0, cn = 0; for (let j = i - 19; j <= i; j++) if (vol[j] != null) { sum += vol[j]; cn++; } va.push(cn >= 15 ? sum / cn : null); }
        ctx.strokeStyle = '#f59e0b'; ctx.lineWidth = 1.3; ctx.beginPath(); let s1 = false; va.forEach((v, i) => { if (v == null) { s1 = false; return; } if (!s1) { ctx.moveTo(xs(i), yv(v)); s1 = true; } else ctx.lineTo(xs(i), yv(v)); }); ctx.stroke();
        ctx.fillStyle = '#94a3b8'; ctx.textAlign = 'right'; ctx.font = '10px ' + FONT; ctx.fillText(fmtVol(vmax, vmode), padL - 6, volTop + 8); ctx.fillText('0', padL - 6, volTop + volH);
        ctx.textAlign = 'left'; ctx.fillStyle = '#64748b'; ctx.fillText((vmode === 'tv' ? '거래대금' : '거래량') + ' · 주황=20일평균 · 빨강↑ 파랑↓', padL + 3, volTop + volH + 12);
      }
      const minDD = Math.min.apply(null, dd), ddLo = Math.min(minDD * 1.1, -0.02);
      const yd = (v) => ddTop + v / ddLo * ddH;
      ctx.fillStyle = '#94a3b8'; ctx.textAlign = 'right'; ctx.font = '10px ' + FONT; ctx.fillText('0%', padL - 6, ddTop + 3); ctx.fillText((ddLo * 100).toFixed(0) + '%', padL - 6, ddTop + ddH + 3);
      ctx.strokeStyle = '#eef2f7'; ctx.beginPath(); ctx.moveTo(padL, ddTop); ctx.lineTo(padL + plotW, ddTop); ctx.stroke();
      ctx.fillStyle = 'rgba(220,38,38,0.18)'; ctx.beginPath(); ctx.moveTo(xs(0), ddTop); dd.forEach((v, i) => ctx.lineTo(xs(i), yd(v))); ctx.lineTo(xs(k - 1), ddTop); ctx.closePath(); ctx.fill();
      ctx.strokeStyle = '#dc2626'; ctx.lineWidth = 1.2; ctx.beginPath(); dd.forEach((v, i) => { if (i === 0) ctx.moveTo(xs(i), yd(v)); else ctx.lineTo(xs(i), yd(v)); }); ctx.stroke();
      ctx.fillStyle = '#64748b'; ctx.textAlign = 'left'; ctx.fillText('고점 대비 낙폭(표시 구간 기준) · 최대 ' + (minDD * 100).toFixed(1) + '%', padL + 3, ddTop + ddH + 12);
      ctx.fillStyle = '#94a3b8';
      [0, Math.floor((k - 1) / 2), k - 1].forEach((i, j) => { ctx.textAlign = j === 0 ? 'left' : (j === 2 ? 'right' : 'center'); ctx.fillText(d[i].slice(0, 4) + '-' + d[i].slice(4, 6) + '-' + d[i].slice(6, 8), j === 0 ? padL : (j === 2 ? padL + plotW : xs(i)), H - 2); });
      if (PRICE.hover != null && PRICE.hover >= 0 && PRICE.hover < k) {
        const i = PRICE.hover, x = xs(i);
        ctx.strokeStyle = '#64748b'; ctx.lineWidth = 1; ctx.setLineDash([3, 3]); ctx.beginPath(); ctx.moveTo(x, mainTop); ctx.lineTo(x, ddTop + ddH); ctx.stroke(); ctx.setLineDash([]);
        ctx.fillStyle = '#2563eb'; ctx.beginPath(); ctx.arc(x, ys(c[i]), 3.5, 0, 6.3); ctx.fill();
        const lines = [d[i].slice(0, 4) + '-' + d[i].slice(4, 6) + '-' + d[i].slice(6, 8), '종가 ' + Math.round(c[i]).toLocaleString() + '원', m50[i] != null ? '50일선 ' + Math.round(m50[i]).toLocaleString() : '', m200[i] != null ? '200일선 ' + Math.round(m200[i]).toLocaleString() : '', vmode !== 'off' ? ((vmode === 'tv' ? '거래대금 ' : '거래량 ') + fmtVol(vol[i], vmode)) : '', '낙폭 ' + (dd[i] * 100).toFixed(1) + '%'].filter(Boolean);
        ctx.font = '11px ' + FONT; const bw = Math.max.apply(null, lines.map((t) => ctx.measureText(t).width)) + 14, bh = lines.length * 15 + 8;
        let bx = x + 10; if (bx + bw > W - 2) bx = x - 10 - bw;
        ctx.fillStyle = 'rgba(15,23,42,0.88)'; ctx.fillRect(bx, mainTop + 4, bw, bh);
        ctx.fillStyle = '#fff'; ctx.textAlign = 'left'; lines.forEach((t, li) => ctx.fillText(t, bx + 7, mainTop + 18 + li * 15));
      }
      if (!PRICE.listeners) {
        const mv = (ev) => { if (!PRICE.geom) return; const r = cv.getBoundingClientRect(), t = ev.touches && ev.touches.length ? ev.touches[0] : ev; let i = Math.round((t.clientX - r.left - PRICE.geom.padL) / (PRICE.geom.step || 1)); i = Math.max(0, Math.min(PRICE.geom.k - 1, i)); if (i !== PRICE.hover) { PRICE.hover = i; drawPrice(); } };
        cv.addEventListener('mousemove', mv); cv.addEventListener('touchmove', mv, { passive: true }); cv.addEventListener('touchstart', mv, { passive: true });
        cv.addEventListener('mouseleave', () => { PRICE.hover = null; drawPrice(); });
        window.addEventListener('resize', () => { drawPrice(); });
        PRICE.listeners = true;
      }
    }

    function setSubTab(k) {
      subTab = k;
      ['summary', 'table', 'five'].forEach(function (x) {
        document.getElementById('sub_' + x).style.display = x === k ? '' : 'none';
        document.getElementById('subBtn_' + x).className = x === k ? 'toggle-active' : '';
      });
      if (k === 'table' && chartSel.length) renderChartPanel(false);
    }
    // 새 종목을 조회할 때 이전 종목의 화면·계산값·진행 중이던 요청 결과가 남지 않게 한꺼번에 비운다.
    // (선호 설정인 하위 탭·차트 항목·기간 단위 등은 그대로 둔다)
    function resetStockView() {
      rawRows = []; lastSummaryMetrics = null; AN_CTX = null; AN_ACCMT = null; AN_STOCK = { code: '', ind: '', name: '' };
      AN_STATE.key = null; AN_STATE.mom = null; AN_STATE.momFor = null; AN_STATE.momErr = null; AN_STATE.trend = null; AN_STATE.trendFor = null; AN_STATE.trendErr = null;
      MAT_ST.name = null; MAT_ST.data = null; MAT_ST.err = null; MAT_ST.loading = false; MAT_ST.news = null; MAT_ST.newsErr = null; MAT_ST.newsNoKey = false; MAT_ST.newsLoading = false;
      PRICE.name = null; PRICE.data = null; PRICE.err = null; PRICE.m50 = []; PRICE.m200 = []; PRICE.hover = null;
      const clear = function (id, hide) { const e = document.getElementById(id); if (!e) return; e.innerHTML = ''; if (hide) e.style.display = 'none'; };
      clear('summary', true); clear('matSlot'); clear('fiveStepWrap', true); clear('wrap'); clear('macroCorrWrap', true); clear('priceStats');
      const note = document.getElementById('pxNote'); if (note) note.textContent = '';
      const cv = document.getElementById('priceCanvas'); if (cv && cv.getContext) { try { cv.getContext('2d').clearRect(0, 0, cv.width, cv.height); } catch (e) { /* 무시 */ } }
      const cs = document.getElementById('macroCorrStatus'); if (cs) cs.textContent = '';
      macroCorrDirty = true;
    }
    async function analyzeStock() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      statusEl.className = '';
      if (!corpName) { statusEl.textContent = '종목명을 입력해주세요.'; return; }
      const seq = ++analyzeSeq;
      resetStockView();
      document.getElementById('stockBody').style.display = 'none'; document.getElementById('cmpPanel').style.display = 'none'; document.getElementById('searchCard').style.display = '';
      document.getElementById('stockMeta').innerHTML = '';
      document.getElementById('currentPrice').value = '';
      document.getElementById('priceNote').textContent = '';
      statusEl.textContent = '"' + corpName + '" 조회 중...';
      let data;
      try { data = await fetchRowsFromDb(corpName); }
      catch (e) { statusEl.textContent = '오류: ' + e.message; statusEl.className = 'error'; return; }
      if (seq !== analyzeSeq) return;
      rawRows = data.rows;
      AN_ACCMT = data.acc_mt != null ? data.acc_mt : null;
      AN_STOCK = { code: data.stock_code || '', ind: data.induty_code || '', name: data.corp_name || '' }; if (AN_STOCK.code && AN_STOCK.ind) IND.map[AN_STOCK.code] = AN_STOCK.ind;
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
      { const sc = document.getElementById('searchCard'); sc.classList.add('compact'); document.getElementById('btnSearchMore').style.display = ''; document.getElementById('btnSearchMore').textContent = '▾ 더보기'; }
      setTimeout(setNavVars, 50);
      guardedRender('저장된 재무데이터 안내', 'stockMeta', renderStockMeta);
      guardedRender('재무제표 표', 'wrap', applyView);
      const stEl = document.getElementById('status');
      const stTxt = stEl.textContent || '';
      if (stTxt.indexOf('조회 중') >= 0 || stTxt.indexOf('받는 중') >= 0 || stTxt.indexOf('받아옵니다') >= 0) stEl.textContent = '';
      await fetchLatestPrice();
      if (seq != null && seq !== analyzeSeq) return;
      pushRecent(document.getElementById('corpName').value.trim());
      guardedRender('일별 주가', 'priceStats', loadPriceSeries);
      loadDbCompanies(true).catch(() => {}); // 새로 저장된 종목이 있을 수 있어 목록 갱신(저장 종목 패널·배지)
      renderSummary();
      guardedRender('ROE 5단계 분해', 'fiveStepWrap', renderFiveStep);
      guardedRender('재료 점검', 'matSlot', matSlotRender); // 재료 점검은 연간 재무가 없어도 항상 같은 자리에 둔다
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
        '<button onclick="updateLatest()" style="padding:4px 10px; font-size:12.5px;" title="최근 2년 치를 DART에서 다시 받아 최신 공시를 반영합니다(약 10~20초)">최신 공시 업데이트</button>' +
        '<div id="anIndMeta" style="margin-top:3px;"></div>';
      anIndMeta();
    }
    function finMetaLink() {
      try {
        const S = finSectorNow();
        if (S.sec && S.sec.fin) return ' · <span style="color:#7c3aed;">분석 유형: ' + escHtml(S.sec.n) + '</span>';
        if (S.sec && S.sec.k === 'holding') return ' · <span style="color:#0f766e;">분석 유형: ' + escHtml(S.sec.n) + '</span>';
        return ' · <a href="#" onclick="finViewFin(); return false;" title="일반 기업용 지표(ROIC·FCF) 대신 ROE·PBR 중심 화면으로 봅니다">금융업 화면으로 보기</a>';
      } catch (e) { return ''; }
    }
    function anIndMeta() {
      const el = document.getElementById('anIndMeta'); if (!el) return;
      const code = AN_STOCK.code, ind = (code && IND.map[code]) || AN_STOCK.ind;
      if (ind) { el.innerHTML = '업종: <b>' + escHtml(indLabel(ind)) + '</b> <span style="color:var(--text-muted);" title="DART 기업개황의 업종코드(한국표준산업분류). 거래소·네이버 업종과 다릅니다">(KSIC ' + escHtml(ind) + ')</span>' + finMetaLink(); return; }
      if (!code) { el.textContent = '업종: 종목코드가 없어 확인할 수 없습니다'; return; }
      el.textContent = '업종: 확인 중...';
      indEnsure([code]).then(function () { const v = IND.map[code]; if (v) AN_STOCK.ind = v; try { if (v && finSecChanged()) renderAnalysis(); } catch (e) { /* 분류 갱신 실패는 무시 */ } const e2 = document.getElementById('anIndMeta'); if (!e2) return; if (v) anIndMeta(); else e2.textContent = '업종: 확인하지 못했습니다(DART 응답에 업종코드 없음)'; });
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
    let dbListShown = [];
    function escHtml(t) { return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;'); }
    async function loadDbCompanies(force) {
      if (dbCompanies && !force) return dbCompanies;
      const res = await fetch('/api/db/companies', force ? { cache: 'no-store' } : undefined); // 새로고침 버튼은 브라우저 30초 캐시를 건너뜀
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'DB 종목 목록 조회 실패');
      dbCompanies = data.companies;
      renderSavedBox();
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
      indSyncList(all); indFillSelect('dbListInd', all.map((c) => c.stock_code));
      const isel = document.getElementById('dbListInd').value, grp = document.getElementById('dbListGrp').checked;
      dbListShown = all.filter((c) => (!q || c.corp_name.toLowerCase().indexOf(q) >= 0 || String(c.stock_code || '').indexOf(q) >= 0) && indMatchSel(isel, c.stock_code));
      if (grp) dbListShown = indSort(dbListShown, (c) => c.stock_code);
      document.getElementById('dbListCount').textContent = '총 ' + all.length + '개 종목' + ((q || isel) ? ' 중 ' + dbListShown.length + '개 일치' : '');
      document.getElementById('dbListBody').innerHTML = dbListShown.length
        ? indJoin(dbListShown, (c) => c.stock_code, (c, i) =>
          '<button class="db-chip" onclick="pickDbCompany(' + i + ')" title="클릭하면 DB에서 바로 조회">' + escHtml(c.corp_name) +
          '<small>' + escHtml(c.stock_code || '') + indSmall(c.stock_code) + ' · ' + c.first_year + '~' + c.last_year + ' · ' + c.n + '건</small></button>', grp)
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
      if (dockMode === 'price') { setDock('custom'); return; } // 표 머리글 더블클릭으로 고른 항목을 위쪽 차트 독에 보여준다
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
      const m = String(lb).match(/^([0-9]{4}) *(.*)$/);
      if (!m) return String(lb);
      const yy = "'" + m[1].slice(2);
      const rest = m[2];
      if (rest.indexOf('TTM') >= 0) { const qm = rest.match(/([0-9])분기/); return yy + ' TTM' + (qm ? qm[1] + 'Q' : ''); }
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
      if (chartSel.length === 0 || dockMode === 'price') { wrap.style.display = 'none'; return; }
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
        : (chartOverlay ? '겹쳐보기: 각 항목의 최소=0 ~ 최대=100으로 환산한 모양 비교용입니다. 마우스를 올리면 실제 값이 나옵니다.' : '열 제목을 더블클릭하면 항목이 추가(최대 5개)되고, 다시 더블클릭하면 제거됩니다. 마우스/터치로 같은 시점의 값을 한번에 볼 수 있습니다.');
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
      if (rawRows.length === 0) { statusEl.textContent = '먼저 종목분석 탭에서 종목을 분석해주세요.'; return; }
      statusEl.textContent = '계산 중...';
      let macro;
      try {
        macro = await loadMacroSeries();
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message + ' (데이터 관리 탭에서 "거시경제 데이터 갱신"을 먼저 눌러주세요)';
        return;
      }
      if (Object.keys(macro).length === 0) {
        statusEl.textContent = '거시경제 데이터가 비어 있습니다. 데이터 관리 탭에서 "거시경제 데이터 갱신"을 먼저 눌러주세요.';
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
        '· 회계기간은 12월 결산 기준(분기말 3·6·9·12월)으로 가정합니다. 결산월이 다른 회사는 기간이 어긋나므로 이 표를 믿지 마세요(데이터 관리의 「결산월 확인」으로 12월 결산이 아닌 종목을 찾을 수 있습니다).</div>';
      statusEl.textContent = '';
      wrapEl.style.display = 'block';
      macroCorrDirty = false;
    }
    // 종목 검색 목록·DB 저장 종목은 접근 키가 확인된 뒤에 불러와야 한다(잠금 상태에서 먼저 요청하면 401로 실패하고 다시 불러오지 않던 문제).
    function initLists() {
      STOCK_LIST_P = STOCK_LIST ? STOCK_LIST_P : null;
      loadStockList();
      const sm = document.getElementById('savedSummary'); if (!dbCompanies) sm.textContent = '📁 DB에 저장된 종목 (불러오는 중…)';
      loadDbCompanies(true).then(renderSavedBox).catch(function () { sm.textContent = '📁 DB 저장 종목 목록을 불러오지 못했습니다 — 여기를 눌러 다시 시도'; });
      dbBannerCheck();
    }
    document.getElementById('savedBox').addEventListener('toggle', function () { if (this.open && !dbCompanies) initLists(); });
    tuneInputs(); cmpTrayRender();
    bootAuth().then(function (ok) { if (ok) { appBooted = true; bootRoute(); initLists(); } });
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

// ===== 뉴스(재료 점검) 수집 =====
// 네이버 검색 API(뉴스). 최신순으로 한 번에 100건, 시작 위치는 최대 1000이라 한 종목당 최대 1,000건까지만 볼 수 있다(기사가 많은 종목은 1년 전까지 못 닿는다 → partial).
const NEWS_MAX_PAGES = 10;
function newsClean(t) {
  return String(t || "").replace(/<[^>]*>/g, "").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&#39;/g, "'").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&amp;/g, "&").replace(/\s+/g, " ").trim();
}
function newsPubToYmd(pub) { // "Mon, 13 Oct 2025 07:08:00 +0900" → 한국 날짜 YYYYMMDD
  const t = new Date(pub); if (isNaN(t.getTime())) return null;
  return new Date(t.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10).replace(/-/g, "");
}
// fetchPage(start) → 네이버 응답 JSON. name: 종목명(제목·요약에 없으면 무관 기사로 보고 버림). cutoffYmd: 이보다 오래된 기사는 수집 중단.
async function collectNews(fetchPage, name, cutoffYmd) {
  const items = []; const seen = new Set(); let reached = false, ended = false, total = null, raw = 0, error = null;
  for (let pg = 0; pg < NEWS_MAX_PAGES; pg++) {
    let j;
    try { j = await fetchPage(1 + pg * 100); } catch (e) { error = (e && e.message) || String(e); break; }
    if (!j || typeof j !== "object") { error = "뉴스 응답 형식 이상"; break; }
    if (j.errorMessage || j.errorCode) { error = "네이버 뉴스 API 오류(" + String(j.errorCode || "") + " " + String(j.errorMessage || "").slice(0, 80) + ")"; break; }
    const list = Array.isArray(j.items) ? j.items : [];
    total = Number(j.total); raw += list.length;
    for (const it of list) {
      const d = newsPubToYmd(it.pubDate); if (!d) continue;
      if (d < cutoffYmd) { reached = true; continue; }
      const title = newsClean(it.title), desc = newsClean(it.description);
      if (name && title.indexOf(name) < 0 && desc.indexOf(name) < 0) continue; // 검색어와 무관하게 걸린 기사
      const link = String(it.originallink || it.link || "");
      if (seen.has(link || title)) continue; seen.add(link || title);
      items.push({ d, t: title, l: link });
    }
    if (list.length < 100) ended = true;
    if (reached || ended) break;
  }
  if (error && !items.length && !raw) return { items: [], partial: false, total: null, error };
  const partial = !reached && !ended; // 기간 끝까지 못 갔고 결과가 더 있을 수 있음(쪽 수 한도·오류)
  items.sort((a, b) => (a.d < b.d ? 1 : a.d > b.d ? -1 : 0));
  return { items, partial: partial, total: Number.isFinite(total) ? total : null, error: error && items.length ? error : null, raw };
}
async function fetchNaverNewsPage(query, start, id, secret, timeoutMs = 12000) {
  const url = new URL("https://openapi.naver.com/v1/search/news.json");
  url.searchParams.set("query", query); url.searchParams.set("display", "100"); url.searchParams.set("start", String(start)); url.searchParams.set("sort", "date");
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { headers: { "X-Naver-Client-Id": id, "X-Naver-Client-Secret": secret }, signal: controller.signal });
    return await resp.json();
  } finally { clearTimeout(timer); }
}
// ===== /뉴스(재료 점검) 수집 =====

// ===== 공시목록(재료 점검) 수집 =====
// DART 공시검색(list.json)을 페이지별로 받아 합친다. 프록시가 page_count를 무시해 10건씩만 주더라도 total_page만큼(최대 MAT_MAX_PAGES) 돈다.
// fetchPage(pageNo) → DART 응답 JSON. 반환: { items, total_count, partial, error }
const MAT_MAX_PAGES = 12;
async function collectDisclosures(fetchPage) {
  const items = []; let total = null, totalPage = 1, lastFirst = null, partial = false;
  for (let p = 1; p <= MAT_MAX_PAGES; p++) {
    let j;
    try { j = await fetchPage(p); } catch (e) { if (p === 1) return { items: [], total_count: null, partial: false, error: "공시목록 요청 실패: " + (/abort|timeout/i.test(String((e && (e.name + " " + e.message)) || e)) ? "프록시(Apps Script) 응답 시간 초과" : ((e && e.message) || String(e))) }; partial = true; break; }
    if (!j || typeof j !== "object") { if (p === 1) return { items: [], total_count: null, partial: false, error: "공시목록 응답 형식 이상" }; partial = true; break; }
    if (j.status === "013") break; // 조회된 데이터 없음
    if (j.status !== "000") { if (p === 1) return { items: [], total_count: null, partial: false, error: "DART 공시목록 오류(" + String(j.status || "?") + " " + String(j.message || "").slice(0, 80) + "). 프록시(Apps Script)가 list 엔드포인트와 page_no·page_count를 전달하는지 확인하세요." }; partial = true; break; }
    const rows = Array.isArray(j.list) ? j.list : [];
    if (!rows.length) break;
    if (lastFirst != null && rows[0].rcept_no === lastFirst) { partial = true; break; } // 프록시가 page_no를 무시하면 같은 쪽이 반복된다
    lastFirst = rows[0].rcept_no;
    for (const r of rows) items.push({ d: String(r.rcept_dt || ""), nm: String(r.report_nm || ""), no: String(r.rcept_no || ""), flr: String(r.flr_nm || "") });
    total = Number(j.total_count); totalPage = Number(j.total_page) || 1;
    if (p >= totalPage) break;
    if (p === MAT_MAX_PAGES) partial = true;
  }
  if (Number.isFinite(total) && total > items.length) partial = true;
  return { items, total_count: Number.isFinite(total) ? total : items.length, partial, error: null };
}
async function fetchDartList(corpCode, bgnDe, endDe, proxyUrl, pageNo, pageCount, timeoutMs = 25000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", "list");
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bgn_de", bgnDe);
  url.searchParams.set("end_de", endDe);
  url.searchParams.set("page_no", String(pageNo));
  url.searchParams.set("page_count", String(pageCount));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}
// ===== /공시목록(재료 점검) 수집 =====

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
// 공시일(targetDate) "다음" 첫 거래일의 {date, close}. 당일은 포함하지 않는다(당일 종가에는 공시 전 정보가 섞일 수 있음).
function firstCloseAfter(prices, targetDate) {
  const c = prices.filter((p) => p.date > targetDate).sort((a, b) => a.date.localeCompare(b.date));
  return c.length ? { date: c[0].date, close: c[0].close } : null;
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

// ============================================================
// 저장 기준(유니버스) 설정 + 기간 지정 시세 가져오기
// 기준(하루 거래대금·시가총액 하한)은 사용자가 데이터 관리에서 바꾸고, 바꾼 뒤부터 매일 자동 수집이 이 기준을 쓴다.
// 이미 저장된 과거 시세는 지우지 않는다(기준을 올려도 그대로, 내리면 새로 통과한 종목만 추가로 받는다).
// ============================================================
const UNIVERSE_LIMITS = { tvEokMax: 100000, mcapEokMax: 10000000 };
let universeCfgCache = null; // { t, cfg }
let appSettingsReady = false;
async function ensureAppSettings(db) {
  if (appSettingsReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS app_settings (k TEXT PRIMARY KEY, v TEXT, updated_at TEXT)").run();
  appSettingsReady = true;
}
function universeCfgFrom(minTvEok, minMcapEok, custom, updatedAt) {
  return { minTvEok: minTvEok, minMcapEok: minMcapEok, minTv: minTvEok * 1e8, minMcap: minMcapEok * 1e8, custom: !!custom, updatedAt: updatedAt || null };
}
async function getUniverseCfg(env, fresh) {
  const def = universeCfgFrom(MARKET_MIN_TRADING_VALUE / 1e8, MARKET_MIN_MCAP / 1e8, false, null);
  if (!fresh && universeCfgCache && Date.now() - universeCfgCache.t < 30000) return universeCfgCache.cfg;
  let cfg = def;
  try {
    await ensureAppSettings(env.DB);
    const r = await env.DB.prepare("SELECT v, updated_at FROM app_settings WHERE k = 'universe_cfg'").first();
    if (r && r.v) {
      const o = JSON.parse(r.v);
      if (Number.isFinite(o.minTvEok) && o.minTvEok >= 0 && Number.isFinite(o.minMcapEok) && o.minMcapEok >= 0) cfg = universeCfgFrom(o.minTvEok, o.minMcapEok, true, r.updated_at);
    }
  } catch (e) { /* 설정을 못 읽으면 기본값으로 계속 동작 */ }
  universeCfgCache = { t: Date.now(), cfg: cfg };
  return cfg;
}
// PF_KEY가 설정돼 있으면 설정 변경·기간 수집 시작에 키가 필요하다(공개 주소에서 아무나 쓰기 한도를 소진시키지 못하게). 미설정이면 기존 데이터 관리 기능처럼 열려 있다.
async function adminAuthError(request, env) {
  if (!pfKeyOn(env)) return null;
  return authGate(request, env);
}
function ymdOk(s) {
  if (!/^\d{8}$/.test(s)) return false;
  const y = +s.slice(0, 4), m = +s.slice(4, 6), d = +s.slice(6, 8), dt = new Date(Date.UTC(y, m - 1, d));
  return y >= 2000 && dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}
function ymdDow(s) { return new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8))).getUTCDay(); }

const RANGE_PAGE = 250;            // 한 틱에 처리하는 행 수(일일 추가와 같은 크기)
const RANGE_DAILY_ROW_CAP = 15000; // 기간 수집이 하루(UTC)에 쓰는 행 상한. 행 1개 = 쓰기 약 2건 → 약 3만 건(무료 한도 10만의 30%)
const RANGE_MAX_DAYS = 400;        // 한 번에 지정할 수 있는 최대 기간(달력일)
async function rangeStatus(env) {
  const row = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'range'").first();
  if (!row) return null;
  try { return JSON.parse(row.v); } catch (e) { return null; }
}
async function startRangeJob(env, from, to) {
  if (!ymdOk(from) || !ymdOk(to)) return { error: "날짜 형식이 올바르지 않습니다." , status: 400 };
  if (from > to) return { error: "시작일이 종료일보다 늦습니다.", status: 400 };
  const kstToday = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10).replace(/-/g, "");
  if (to > kstToday) return { error: "종료일은 오늘(" + kstToday + ")보다 늦을 수 없습니다.", status: 400 };
  const span = Math.round((Date.UTC(+to.slice(0, 4), +to.slice(4, 6) - 1, +to.slice(6, 8)) - Date.UTC(+from.slice(0, 4), +from.slice(4, 6) - 1, +from.slice(6, 8))) / 86400000) + 1;
  if (span > RANGE_MAX_DAYS) return { error: "한 번에 최대 " + RANGE_MAX_DAYS + "일까지 지정할 수 있습니다(지정: " + span + "일). 나눠서 요청하세요.", status: 400 };
  await ensureMarketQueueColumns(env.DB);
  const cur = await rangeStatus(env);
  if (cur && !cur.done) return { error: "이미 진행 중인 기간 수집이 있습니다(" + cur.from + "~" + cur.to + "). 끝나거나 취소한 뒤 다시 시작하세요.", status: 409 };
  const cfg = await getUniverseCfg(env, true);
  const job = { from: from, to: to, cur: from, page: 1, rows: 0, tradingDates: 0, skipped: 0, fails: 0, startedAt: new Date().toISOString(), minTv: cfg.minTv, minMcap: cfg.minMcap, budgetDay: todayStr(), budgetRows: 0, done: false, error: null };
  await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('range', ?)").bind(JSON.stringify(job)).run();
  return { ok: true, job: job, calendarDays: span };
}
// cron에서 호출: 진행 중인 기간 수집이 있으면 한 쪽(최대 250행)을 처리한다. 일을 했으면 true.
async function processRangeJobTick(env) {
  const row = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'range'").first();
  if (!row) return false;
  let job; try { job = JSON.parse(row.v); } catch (e) { await env.DB.prepare("DELETE FROM market_job WHERE k = 'range'").run(); return false; }
  if (job.done) return false;
  const today = todayStr();
  if (job.budgetDay !== today) { job.budgetDay = today; job.budgetRows = 0; }
  if (job.budgetRows >= RANGE_DAILY_ROW_CAP) return false; // 오늘 몫을 다 썼다 → 다른 작업에 양보하고 내일 이어서 한다
  const save = () => env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('range', ?)").bind(JSON.stringify(job)).run();
  while (job.cur <= job.to && (ymdDow(job.cur) === 0 || ymdDow(job.cur) === 6)) { job.cur = addDaysStr(job.cur, 1); job.page = 1; } // 주말은 호출하지 않는다
  if (job.cur > job.to) { job.done = true; job.finishedAt = new Date().toISOString(); await save(); return true; }
  let page;
  try {
    page = await fetchKrxPage({ basDt: job.cur, beginTrPrc: job.minTv, numOfRows: RANGE_PAGE, pageNo: job.page }, env.DATA_GO_KR_KEY);
    if (page.resultCode && page.resultCode !== "00") throw new Error("API resultCode " + page.resultCode);
  } catch (e) {
    job.fails = (job.fails || 0) + 1;
    console.error("기간 수집 쪽 조회 실패:", job.cur, job.page, (e && e.message) || e);
    if (job.fails >= 5) { job.done = true; job.error = "공공데이터 조회가 연속 실패해 중단했습니다(" + ((e && e.message) || e) + "). 같은 기간으로 다시 시작하면 이미 받은 날짜는 건너뜁니다."; }
    await save();
    return true;
  }
  job.fails = 0;
  const rows = page.items.filter((it) => it.srtnCd && it.clpr > 0 && it.mrktTotAmt != null && it.mrktTotAmt >= job.minMcap);
  // 이미 있는 행은 건드리지 않는다(쓰기 0). 단, 예전에 시가총액 없이 저장된 행은 시가총액만 채운다.
  await multiRowInsert(
    env.DB,
    "INSERT INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value, market_cap)",
    6, rows, (it) => [it.basDt, it.srtnCd, it.clpr, it.trqu, it.trPrc, it.mrktTotAmt],
    "ON CONFLICT(market_date, stock_code) DO UPDATE SET market_cap = excluded.market_cap WHERE market_raw_daily.market_cap IS NULL"
  );
  job.rows += rows.length; job.budgetRows += rows.length;
  const empty = job.page === 1 && page.items.length === 0;
  const lastPage = page.items.length < RANGE_PAGE || job.page * RANGE_PAGE >= (page.totalCount || 0);
  if (empty) { job.skipped++; job.cur = addDaysStr(job.cur, 1); job.page = 1; }
  else if (lastPage) { job.tradingDates++; job.cur = addDaysStr(job.cur, 1); job.page = 1; }
  else job.page++;
  if (job.cur > job.to) { job.done = true; job.finishedAt = new Date().toISOString(); }
  await save();
  return true;
}

// ===== DB 용량 관리 =====
// D1은 쿼리 결과의 meta.size_after로 현재 DB 크기(바이트)를 알려 준다. 한도는 요금제마다 달라(무료 플랜 DB당 500MB) 사용자가 바꿀 수 있게 둔다.
const DB_LIMIT_DEFAULT_MB = 500, DB_WARN_DEFAULT_PCT = 90;
const PURGE_CHUNK = 2000;             // 한 번 호출에서 지우는 최대 행 수
const PURGE_DAILY_ROW_CAP = 20000;    // 삭제도 D1 쓰기 한도(무료 10만행/일)를 쓰므로 하루 삭제 상한을 둔다(행 1개 ≈ 쓰기 2건, 수집 작업 몫을 남김)
const PRICE_KEEP_DAYS = 380;          // 12-1 모멘텀·52주 계산에 약 253거래일(≈365일)이 필요해 최근 이만큼은 지우지 못하게 한다
const DB_TABLE_CACHE_MS = 60 * 60 * 1000;
const COUNT_CAP = 20000;              // 표 행 수 계산이 읽는 최대 행(D1은 읽은 행 수로 하루 한도 500만을 센다 → 큰 표 전체를 세지 않는다)
const PREVIEW_COUNT_CAP = 100000;     // 삭제 미리보기가 읽는 최대 행
async function dbSizeBytes(env) {
  try { const r = await env.DB.prepare("SELECT 1").run(); const n = r && r.meta && r.meta.size_after; return Number.isFinite(n) ? n : null; } catch (e) { return null; }
}
async function getDbCapCfg(env) {
  let limitMb = DB_LIMIT_DEFAULT_MB, warnPct = DB_WARN_DEFAULT_PCT;
  try {
    await ensureAppSettings(env.DB);
    const r = await env.DB.prepare("SELECT v FROM app_settings WHERE k = 'db_cap'").first();
    if (r && r.v) { const o = JSON.parse(r.v); if (Number.isFinite(o.limitMb) && o.limitMb >= 50) limitMb = o.limitMb; if (Number.isFinite(o.warnPct) && o.warnPct >= 50 && o.warnPct <= 99) warnPct = o.warnPct; }
  } catch (e) { /* 기본값 */ }
  return { limitMb: limitMb, warnPct: warnPct };
}
async function dbUsageLight(env) {
  const size = await dbSizeBytes(env), cfg = await getDbCapCfg(env), limit = cfg.limitMb * 1048576;
  const pct = size == null ? null : size / limit * 100;
  return { sizeBytes: size, limitBytes: limit, limitMb: cfg.limitMb, warnPct: cfg.warnPct, pct: pct, warn: pct != null && pct >= cfg.warnPct };
}
async function dbTableStats(env, refresh) {
  await ensureAppSettings(env.DB);
  if (!refresh) {
    try { const r = await env.DB.prepare("SELECT v FROM app_settings WHERE k = 'db_tables'").first(); if (r && r.v) { const o = JSON.parse(r.v); if (o && Date.now() - o.t < DB_TABLE_CACHE_MS) return Object.assign({ cached: true }, o); } } catch (e) { /* 새로 계산 */ }
  }
  const { results: objs } = await env.DB.prepare("SELECT name, type, tbl_name FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '\\_cf\\_%' ESCAPE '\\'").all();
  const tables = objs.filter((o) => o.type === "table").map((o) => ({ name: o.name, rows: null, bytes: null }));
  for (const t of tables) {
    // 전체 COUNT(*)는 표 전체를 읽는다(수백만 행이면 하루 읽기 한도를 한 번에 소진). 상한까지만 센다.
    try { const r = await env.DB.prepare('SELECT COUNT(*) AS n FROM (SELECT 1 FROM "' + t.name.replace(/"/g, '""') + '" LIMIT ' + (COUNT_CAP + 1) + ')').first(); const n = r ? r.n : null; t.capped = n != null && n > COUNT_CAP; t.rows = n == null ? null : Math.min(n, COUNT_CAP); } catch (e) { t.rows = null; }
  }
  let measured = false;
  try { // 일부 환경에서만 지원되는 dbstat(실측). 안 되면 추정으로 대체한다.
    const { results: st } = await env.DB.prepare("SELECT name, SUM(pgsize) AS b FROM dbstat GROUP BY name").all();
    const owner = {}; objs.forEach((o) => { owner[o.name] = o.tbl_name; });
    const by = {}; st.forEach((x) => { const tb = owner[x.name] || x.name; by[tb] = (by[tb] || 0) + x.b; });
    tables.forEach((t) => { t.bytes = by[t.name] != null ? by[t.name] : null; });
    measured = tables.some((t) => t.bytes != null);
  } catch (e) { measured = false; }
  if (!measured && !tables.some((t) => t.capped)) {
    const size = await dbSizeBytes(env);
    let sl = 0; try { const r = await env.DB.prepare("SELECT COALESCE(SUM(LENGTH(payload)),0) AS n FROM screen_lists").first(); sl = r ? r.n : 0; } catch (e) { sl = 0; }
    const w = tables.map((t) => (t.rows || 0) * 60 + (t.name === "screen_lists" ? sl : 0));
    const tot = w.reduce((a, b) => a + b, 0) || 1;
    tables.forEach((t, i) => { t.bytes = size != null ? Math.round(size * w[i] / tot) : null; });
  }
  tables.sort((a, b) => (b.bytes || 0) - (a.bytes || 0));
  const out = { t: Date.now(), measured: measured, tables: tables };
  try { await env.DB.prepare("INSERT OR REPLACE INTO app_settings (k, v, updated_at) VALUES ('db_tables', ?, ?)").bind(JSON.stringify(out), new Date().toISOString()).run(); } catch (e) { /* 캐시 실패는 무시 */ }
  return Object.assign({ cached: false }, out);
}
// 삭제 종류별 조건. cond는 WHERE 뒤에 붙는 조건, binds는 그 값. 지울 수 없게 막아야 하면 error를 돌려준다.
async function purgeSpec(env, kind, p) {
  p = p || {};
  if (kind === "price_old") {
    const before = String(p.before || "");
    if (!ymdOk(before)) return { error: "기준일(YYYYMMDD)이 올바르지 않습니다." };
    const m = await env.DB.prepare("SELECT MAX(market_date) AS m FROM market_raw_daily").first();
    if (!m || !m.m) return { error: "저장된 시세가 없습니다." };
    const cutoff = addDaysStr(m.m, -PRICE_KEEP_DAYS);
    if (before > cutoff) return { error: "최근 " + PRICE_KEEP_DAYS + "일(" + cutoff + " 이후)의 시세는 모멘텀·52주 계산에 필요해 지울 수 없습니다. 기준일은 " + cutoff + " 이하로 지정하세요." };
    return { table: "market_raw_daily", cond: "market_date < ?", binds: [before], label: before + " 이전 일별 시세" };
  }
  if (kind === "price_inactive") {
    const r = await env.DB.prepare("SELECT COUNT(*) AS n FROM market_universe_status WHERE passed = 1").first();
    if (!r || !(r.n > 0)) return { error: "현재 기준을 통과한 종목 목록이 비어 있어 안전을 위해 실행하지 않습니다(데이터 관리에서 유니버스를 먼저 갱신하세요)." };
    return { table: "market_raw_daily", cond: "stock_code NOT IN (SELECT stock_code FROM market_universe_status WHERE passed = 1)", binds: [], label: "현재 저장 기준을 통과하지 못한 종목의 일별 시세" };
  }
  if (kind === "screen_old") {
    const keep = Math.floor(Number(p.keep));
    if (!(keep >= 1 && keep <= 104)) return { error: "남길 주간 결과 수는 1~104입니다." };
    let thr = null;
    try { const r = await env.DB.prepare("SELECT DISTINCT snap_date AS d FROM screen_lists ORDER BY snap_date DESC LIMIT 1 OFFSET ?").bind(keep - 1).first(); thr = r ? r.d : null; } catch (e) { thr = null; }
    if (!thr) return { table: "screen_lists", cond: "1 = 0", binds: [], label: "오래된 주간 스크리닝 결과(지울 것 없음)" };
    return { table: "screen_lists", cond: "snap_date < ?", binds: [thr], label: "최근 " + keep + "회를 뺀 이전 주간 스크리닝 결과" };
  }
  if (kind === "fin_company") {
    const name = String(p.corp_name || "").trim();
    if (!name) return { error: "종목명을 입력하세요." };
    const c = await env.DB.prepare("SELECT corp_code FROM corp_master WHERE corp_name = ?").bind(name).first();
    if (!c) return { error: "'" + name + "' 종목을 찾을 수 없습니다." };
    return { table: "financial_raw", cond: "corp_code = ?", binds: [c.corp_code], label: name + " 의 저장된 재무데이터 전체" };
  }
  if (kind === "fin_old") {
    const y = Math.floor(Number(p.before_year));
    if (!(y >= 2000 && y <= 2100)) return { error: "연도(예: 2015)가 올바르지 않습니다." };
    return { table: "financial_raw", cond: "bsns_year < ?", binds: [String(y)], label: y + "년 이전 재무데이터(전 종목)" };
  }
  if (kind === "opincome_cache") return { table: "op_income_cache", cond: "1 = 1", binds: [], label: "영업이익 조회 캐시(필요하면 다시 자동 조회)" };
  return { error: "알 수 없는 삭제 종류입니다." };
}
async function purgeCapLeft(env) {
  await env.DB.prepare("CREATE TABLE IF NOT EXISTS market_job (k TEXT PRIMARY KEY, v TEXT)").run();
  const day = new Date().toISOString().slice(0, 10);
  let used = 0;
  try { const r = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'purge_day'").first(); if (r) { const o = JSON.parse(r.v); if (o.day === day) used = o.rows || 0; } } catch (e) { used = 0; }
  return { day: day, used: used, left: Math.max(0, PURGE_DAILY_ROW_CAP - used) };
}
async function handleDbCapacity(request, env, pathname, searchParams) {
  try {
    if (pathname === "/api/db/usage") {
      const light = await dbUsageLight(env);
      if (searchParams.get("light") === "1") return Response.json(light);
      const st = await dbTableStats(env, searchParams.get("refresh") === "1");
      const cap = await purgeCapLeft(env);
      return Response.json(Object.assign({}, light, { tables: st.tables, measured: st.measured, tablesAt: st.t, protected: pfKeyOn(env), purgeCap: { perDay: PURGE_DAILY_ROW_CAP, left: cap.left, chunk: PURGE_CHUNK }, priceKeepDays: PRICE_KEEP_DAYS }));
    }
    if (pathname === "/api/db/settings" && request.method === "POST") {
      const bad = await adminAuthError(request, env); if (bad) return bad;
      const b = await request.json().catch(() => null) || {};
      const limitMb = Number(b.limit_mb), warnPct = Number(b.warn_pct);
      if (!(limitMb >= 50 && limitMb <= 102400)) return Response.json({ error: "용량 한도는 50~102400MB 사이로 입력하세요." }, { status: 400 });
      if (!(warnPct >= 50 && warnPct <= 99)) return Response.json({ error: "알림 기준은 50~99% 사이로 입력하세요." }, { status: 400 });
      await ensureAppSettings(env.DB);
      await env.DB.prepare("INSERT OR REPLACE INTO app_settings (k, v, updated_at) VALUES ('db_cap', ?, ?)").bind(JSON.stringify({ limitMb: limitMb, warnPct: warnPct }), new Date().toISOString()).run();
      return Response.json(await dbUsageLight(env));
    }
    if (pathname === "/api/db/purge" && request.method === "POST") {
      if (!pfKeyOn(env)) return Response.json({ error: "데이터 삭제는 접근 키(PF_KEY)가 설정된 상태에서만 쓸 수 있습니다(공개 주소에서 누구나 지우는 것을 막기 위해)." }, { status: 403 });
      const bad = await adminAuthError(request, env); if (bad) return bad;
      const b = await request.json().catch(() => null) || {};
      let spec;
      try { spec = await purgeSpec(env, String(b.kind || ""), b.param); }
      catch (e) { return Response.json({ error: "조건을 확인하지 못했습니다: " + ((e && e.message) || e) }, { status: 400 }); }
      if (spec.error) return Response.json({ error: spec.error }, { status: 400 });
      const cap = await purgeCapLeft(env);
      const T = spec.table;
      if (b.preview) {
        let n = 0; try { const r = await env.DB.prepare("SELECT COUNT(*) AS n FROM (SELECT 1 FROM " + T + " WHERE " + spec.cond + " LIMIT " + (PREVIEW_COUNT_CAP + 1) + ")").bind(...spec.binds).first(); n = r ? r.n : 0; } catch (e) { n = 0; }
        const capped = n > PREVIEW_COUNT_CAP; if (capped) n = PREVIEW_COUNT_CAP;
        return Response.json({ label: spec.label, count: n, capped: capped, estWrites: n * 2, daysNeeded: n ? Math.ceil(n / PURGE_DAILY_ROW_CAP) : 0, capLeft: cap.left, perDay: PURGE_DAILY_ROW_CAP });
      }
      const lim = Math.min(PURGE_CHUNK, cap.left);
      if (lim <= 0) return Response.json({ error: "오늘 삭제 한도(" + PURGE_DAILY_ROW_CAP.toLocaleString("ko-KR") + "행)를 모두 썼습니다. 한국시간 오전 9시 이후(UTC 날짜가 바뀐 뒤) 이어서 지우세요.", capLeft: 0, done: false }, { status: 429 });
      let r;
      try { r = await env.DB.prepare("DELETE FROM " + T + " WHERE rowid IN (SELECT rowid FROM " + T + " WHERE " + spec.cond + " LIMIT ?)").bind(...spec.binds, lim).run(); }
      catch (e) { return Response.json({ error: "삭제하지 못했습니다: " + ((e && e.message) || e) }, { status: 500 }); }
      const deleted = (r && r.meta && Number(r.meta.changes)) || 0;
      if (T === "financial_raw" && spec.binds.length && spec.cond === "corp_code = ?") { try { await env.DB.prepare("DELETE FROM corp_fiscal WHERE corp_code = ?").bind(spec.binds[0]).run(); } catch (e) { /* 보조 테이블 */ } }
      await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('purge_day', ?)").bind(JSON.stringify({ day: cap.day, rows: cap.used + deleted })).run();
      let more = false; try { const x = await env.DB.prepare("SELECT 1 AS x FROM " + T + " WHERE " + spec.cond + " LIMIT 1").bind(...spec.binds).first(); more = !!x; } catch (e) { more = false; }
      try { await env.DB.prepare("DELETE FROM app_settings WHERE k = 'db_tables'").run(); } catch (e) { /* 캐시 무효화 */ }
      const sizeAfter = (r && r.meta && Number.isFinite(r.meta.size_after)) ? r.meta.size_after : await dbSizeBytes(env);
      return Response.json({ deleted: deleted, done: !more, capLeft: Math.max(0, cap.left - deleted), sizeAfter: sizeAfter, label: spec.label });
    }
    return Response.json({ error: "알 수 없는 DB 용량 요청입니다." }, { status: 404 });
  } catch (e) {
    return Response.json({ error: "서버 오류: " + ((e && e.message) || String(e)) }, { status: 500 });
  }
}
// ===== /DB 용량 관리 =====

async function handleMarketCfg(request, env, pathname, searchParams) {
  try {
    const protectedBy = !!(env.PF_KEY && String(env.PF_KEY).length >= 12);
    if (pathname === "/api/market/config") {
      if (request.method === "POST") {
        const bad = await adminAuthError(request, env); if (bad) return bad;
        const body = await request.json().catch(() => null);
        const tv = body && body.min_tv_eok !== "" ? Number(body.min_tv_eok) : NaN, mc = body && body.min_mcap_eok !== "" ? Number(body.min_mcap_eok) : NaN;
        if (!Number.isFinite(tv) || tv < 0 || tv > UNIVERSE_LIMITS.tvEokMax) return Response.json({ error: "하루 거래대금 하한은 0~" + UNIVERSE_LIMITS.tvEokMax + "억원 범위여야 합니다." }, { status: 400 });
        if (!Number.isFinite(mc) || mc < 0 || mc > UNIVERSE_LIMITS.mcapEokMax) return Response.json({ error: "시가총액 하한은 0~" + UNIVERSE_LIMITS.mcapEokMax + "억원 범위여야 합니다." }, { status: 400 });
        await ensureAppSettings(env.DB);
        await env.DB.prepare("INSERT OR REPLACE INTO app_settings (k, v, updated_at) VALUES ('universe_cfg', ?, ?)").bind(JSON.stringify({ minTvEok: tv, minMcapEok: mc }), new Date().toISOString()).run();
        universeCfgCache = null;
        return Response.json({ ok: true, cfg: await getUniverseCfg(env, true) });
      }
      return Response.json({ cfg: await getUniverseCfg(env, true), defaults: universeCfgFrom(MARKET_MIN_TRADING_VALUE / 1e8, MARKET_MIN_MCAP / 1e8, false, null), protected: protectedBy, dailyCap: MARKET_DAILY_WRITE_CAP });
    }
    // 미리보기: 입력한 기준이면 기준일에 몇 종목이 통과하고 그중 새로 백필해야 할 종목이 몇 개인지. 한 번에 한 쪽씩(클라이언트가 이어서 호출). 쓰기 없음.
    if (pathname === "/api/market/preview") {
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      if (searchParams.get("mode") === "known") {
        const { results } = await env.DB.prepare("SELECT stock_code FROM market_universe_status WHERE backfilled = 1").all();
        return Response.json({ codes: results.map((r) => r.stock_code) });
      }
      const nz = (k) => { const v = Number(searchParams.get(k)); return Number.isFinite(v) && v >= 0 ? v : NaN; };
      const tv = nz("tv"), mc = nz("mcap"), pageNo = Math.max(1, Math.round(Number(searchParams.get("page")) || 1));
      if (!Number.isFinite(tv) || !Number.isFinite(mc)) return Response.json({ error: "거래대금(tv)·시가총액(mcap) 하한(억원)이 필요합니다." }, { status: 400 });
      let basDt = searchParams.get("bas_dt");
      if (!basDt) { const r = await env.DB.prepare("SELECT MAX(market_date) AS d FROM market_raw_daily").first(); basDt = r && r.d; }
      if (!basDt) return Response.json({ error: "저장된 시세가 없어 기준일을 정할 수 없습니다." }, { status: 404 });
      const page = await fetchKrxPage({ basDt: basDt, beginTrPrc: tv * 1e8, numOfRows: RANGE_PAGE, pageNo: pageNo }, env.DATA_GO_KR_KEY);
      const pass = page.items.filter((it) => it.srtnCd && it.clpr > 0 && it.mrktTotAmt != null && it.mrktTotAmt >= mc * 1e8).map((it) => it.srtnCd);
      return Response.json({ bas_dt: basDt, page: pageNo, size: RANGE_PAGE, total: page.totalCount, n: page.items.length, pass: pass, done: page.items.length < RANGE_PAGE || pageNo * RANGE_PAGE >= (page.totalCount || 0) });
    }
    if (pathname === "/api/market/range") {
      if (request.method === "POST") {
        const bad = await adminAuthError(request, env); if (bad) return bad;
        if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
        const body = await request.json().catch(() => null) || {};
        if (body.cancel) { await env.DB.prepare("DELETE FROM market_job WHERE k = 'range'").run(); return Response.json({ ok: true, cancelled: true }); }
        const r = await startRangeJob(env, String(body.from || "").replace(/-/g, ""), String(body.to || "").replace(/-/g, ""));
        if (r.error) return Response.json({ error: r.error }, { status: r.status || 400 });
        return Response.json(r);
      }
      return Response.json({ job: await rangeStatus(env), dailyRowCap: RANGE_DAILY_ROW_CAP, maxDays: RANGE_MAX_DAYS });
    }
    return Response.json({ error: "알 수 없는 요청입니다." }, { status: 404 });
  } catch (e) {
    return Response.json({ error: "서버 오류: " + ((e && e.message) || String(e)) }, { status: 500 });
  }
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
  const cfg = await getUniverseCfg(env, true);
  let basDt = null;
  let total = 0;
  const today = new Date();
  for (let back = 0; back <= 7; back++) {
    const d = new Date(today);
    d.setUTCDate(d.getUTCDate() - back);
    const ds = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`;
    const probe = await fetchKrxPage({ basDt: ds, beginTrPrc: cfg.minTv, numOfRows: 1, pageNo: 1 }, env.DATA_GO_KR_KEY);
    if (probe.items.length > 0) { basDt = ds; total = probe.totalCount; break; }
  }
  if (!basDt) {
    throw new Error("최근 7일 내 거래일 데이터를 찾지 못했습니다(주말/공휴일이 겹쳤을 수 있습니다).");
  }
  const lastRow = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'last'").first();
  if (!force && lastRow && lastRow.v === basDt) {
    return { bas_dt: basDt, universe_size: total, job_started: false, note: "이미 이 기준일의 시세를 추가했습니다(건너뜀)." };
  }
  const job = { basDt, nextPage: 1, pageSize: MARKET_APPEND_PAGE, total, startedAt: new Date().toISOString(), fails: 0, appended: 0, queued: 0, minTv: cfg.minTv, minMcap: cfg.minMcap };
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
    page = await fetchKrxPage({ basDt: job.basDt, beginTrPrc: job.minTv != null ? job.minTv : MARKET_MIN_TRADING_VALUE, numOfRows: job.pageSize, pageNo: job.nextPage }, env.DATA_GO_KR_KEY);
    if (page.resultCode && page.resultCode !== "00") throw new Error("API resultCode " + page.resultCode);
  } catch (e) {
    job.fails = (job.fails || 0) + 1;
    console.error("일괄 시세 추가 쪽 조회 실패:", job.nextPage, (e && e.message) || e);
    if (job.fails >= 4) await env.DB.prepare("DELETE FROM market_job WHERE k = 'append'").run(); // 계속 실패하면 포기(수동 갱신으로 재시작)
    else await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('append', ?)").bind(JSON.stringify(job)).run();
    return true;
  }

  const rows = page.items.filter((it) => it.srtnCd && it.clpr > 0 && it.mrktTotAmt != null && it.mrktTotAmt >= (job.minMcap != null ? job.minMcap : MARKET_MIN_MCAP));
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

// ============================================================
// 결산월(회계연도 종료월) — 12월 결산이 아닌 법인은 날짜 기반 값(기간말·공시일 주가, PER·PBR 등)이 틀어지므로 따로 다룬다
// ============================================================
// DART 회사개요(company)의 acc_mt(결산월)를 한 번 조회해 corp_fiscal 표에 저장한다. 조회가 안 되면(프록시 미지원·오류) 기존처럼 12월 결산으로 가정하고 진행한다.
let fiscalTableReady = false;
const fiscalLookupTried = new Set(); // 이 실행 환경에서 이미 조회를 시도한 회사(실패해도 같은 회사를 계속 두드리지 않기 위함)
async function ensureFiscalTable(db) {
  if (fiscalTableReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS corp_fiscal (corp_code TEXT PRIMARY KEY, acc_mt INTEGER NOT NULL, checked_at TEXT)").run();
  fiscalTableReady = true;
}
function parseAccMt(j) {
  if (!j || j.status !== "000" || j.acc_mt == null) return null;
  const n = Number(String(j.acc_mt).replace(/[^0-9]/g, ""));
  return Number.isInteger(n) && n >= 1 && n <= 12 ? n : null;
}
const fiscalSkipDates = (accMt) => accMt != null && accMt !== 12; // 결산월이 확인됐고 12월이 아닐 때만 날짜 기반 값을 만들지 않는다
async function fetchDartCompany(corpCode, proxyUrl, timeoutMs = 15000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", "company");
  url.searchParams.set("corp_code", corpCode);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally {
    clearTimeout(timer);
  }
}
async function getAccMt(db, corpCode) {
  await ensureFiscalTable(db);
  const r = await db.prepare("SELECT acc_mt FROM corp_fiscal WHERE corp_code = ?").bind(corpCode).first();
  return r ? r.acc_mt : null;
}
async function lookupAndSaveAccMt(db, corpCode, proxyUrl) {
  try {
    const j = await fetchDartCompany(corpCode, proxyUrl);
    const m = parseAccMt(j);
    if (m == null) return { accMt: null, error: "결산월을 찾지 못함: " + String((j && (j.message || j.status)) || "응답 없음").slice(0, 60) };
    await ensureFiscalTable(db);
    await db.prepare("INSERT OR REPLACE INTO corp_fiscal (corp_code, acc_mt, checked_at) VALUES (?, ?, ?)").bind(corpCode, m, new Date().toISOString()).run();
    return { accMt: m };
  } catch (e) {
    return { accMt: null, error: String((e && e.message) || e).slice(0, 80) };
  }
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
      price_next_day: null, price_next_day_date: null,
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

    // 결산월 확인: 12월 결산이 아닌 법인은 아래 날짜 기반 값(기간말·공시일 주가, PER·PBR·FCF Yield 등)이 12월 결산 가정으로 계산되면 틀리므로 만들지 않는다(틀린 값보다 빈 값이 낫다).
    let accMt = null;
    if (db) {
      try {
        accMt = await getAccMt(db, corpCode);
        if (accMt == null && !fiscalLookupTried.has(corpCode)) { fiscalLookupTried.add(corpCode); accMt = (await lookupAndSaveAccMt(db, corpCode, proxyUrl)).accMt; }
      } catch (e) { /* 결산월을 못 알아내도 기존대로 진행 */ }
    }
    const skipDates = fiscalSkipDates(accMt);

    // 회계기간 말일(분기말/반기말/연말) 종가 — 공시일 주가와 비교해 "실적 발표 전후 주가 상승률"을 보기 위함
    try {
      const periodEnd = periodEndDateStr(period.year, period.code);
      if (periodEnd && stockCode && !skipDates) {
        const pePrices = await fetchNaverPrices(stockCode, addDaysStr(periodEnd, -15), periodEnd, proxyUrl);
        row.price_at_period_end = closeAsOf(pePrices, periodEnd);
      }
    } catch (e) { /* 실패해도 나머지는 살림 */ }

    // 공시일자(rcept_no 앞 8자리) 기준 종가로 그 시점 PER/PBR/FCF Yield 계산
    const rceptNo = dart.list[0] && dart.list[0].rcept_no;
    if (rceptNo && stockCode && !skipDates) {
      const rceptDate = rceptNo.slice(0, 8);
      const filingDate = effectiveFilingDate(rceptDate, period.year, period.code);
      const filingCapped = filingDate !== rceptDate; // 상한 적용(정정공시 등)이면 월말이 휴장일일 수 있어 다음 영업일 종가를 쓴다
      row.filing_date = filingDate;
      try {
        const prices = await fetchNaverPrices(stockCode, addDaysStr(filingDate, -15), addDaysStr(filingDate, 10), proxyUrl);
        const price = filingCapped ? closeOnOrAfter(prices, filingDate) : closeAsOf(prices, filingDate);
        row.price_at_filing = price;
        const nextDay = firstCloseAfter(prices, filingDate); // 백테스트 매매가: 공시 다음 거래일 종가
        row.price_next_day = nextDay ? nextDay.close : null;
        row.price_next_day_date = nextDay ? nextDay.date : null;

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
  // 백테스트용 "공시 다음 거래일 종가"(공시일 당일 종가는 공시 이전 정보까지 섞일 수 있어 매매가로 쓰지 않는다)
  if (!have.has("price_next_day")) {
    try { await db.prepare("ALTER TABLE financial_raw ADD COLUMN price_next_day REAL").run(); } catch (e) { /* 이미 추가됨 */ }
  }
  if (!have.has("price_next_day_date")) {
    try { await db.prepare("ALTER TABLE financial_raw ADD COLUMN price_next_day_date TEXT").run(); } catch (e) { /* 이미 추가됨 */ }
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

// ============================================================
// 내 계좌·매매 관리(pf) — 계산 엔진. 브라우저와 서버가 "같은 함수"를 쓰도록 순수 함수로만 만든다(PF_CLIENT_FUNCS로 화면에 주입).
// 증거 수준(화면에도 표시): 평균단가·손익·비중·분산은 "확립된 정의", 위험예산 기반 수량 한도는 "확립된 계산 + 정책 선택(비율은 사용자가 정함)",
// 처분효과(Shefrin & Statman 1985; Odean 1998)·과도한 매매(Barber & Odean 2000)는 "반복 관찰된 실증(미국 중심, 한국 개별 투자자에 그대로 적용은 별도 검증 필요)",
// 사전 체크리스트·숙려 시간·일지는 "실무 관행/행동재무 문헌 기반 아이디어(효과 크기는 보편적으로 증명되지 않음)".
// 금액은 모두 원(KRW), 수량은 정수 주. 평균단가법(이동평균). 호가단위·세금 규정은 반영하지 않고 사용자가 입력한 수수료·세금만 쓴다.
// ============================================================
function pfLedger(trades) {
  const list = (trades || []).filter((t) => !t.voided).slice().sort((a, b) => String(a.trade_date).localeCompare(String(b.trade_date)) || (a.id || 0) - (b.id || 0));
  const dayDiff = (a, b) => Math.round((Date.UTC(+b.slice(0, 4), +b.slice(4, 6) - 1, +b.slice(6, 8)) - Date.UTC(+a.slice(0, 4), +a.slice(4, 6) - 1, +a.slice(6, 8))) / 86400000);
  const pos = {}; const errors = [], sells = [];
  let cash = 0, realized = 0, fees = 0, deposits = 0;
  for (const t of list) {
    const q = Number(t.qty) || 0, p = Number(t.price) || 0, fee = Number(t.fee) || 0, tax = Number(t.tax) || 0;
    if (t.side === "deposit") { cash += p; deposits += p; continue; }
    if (t.side === "withdraw") { cash -= p; deposits -= p; continue; }
    const code = t.stock_code;
    const ps = pos[code] || (pos[code] = { code: code, name: t.corp_name || code, qty: 0, cost: 0, realized: 0, openDate: null, firstBuy: null });
    if (t.corp_name) ps.name = t.corp_name;
    if (t.side === "buy") {
      if (ps.qty === 0) ps.openDate = t.trade_date;
      if (!ps.firstBuy) ps.firstBuy = t.trade_date;
      ps.qty += q; ps.cost += q * p + fee; cash -= q * p + fee; fees += fee;
    } else if (t.side === "sell") {
      if (q > ps.qty) { errors.push({ id: t.id, msg: "보유 수량(" + ps.qty + "주)보다 많이 매도한 기록입니다: " + (t.corp_name || code) + " " + t.trade_date }); continue; }
      const avg = ps.qty ? ps.cost / ps.qty : 0, removed = avg * q, proceeds = q * p - fee - tax, gain = proceeds - removed;
      ps.realized += gain; realized += gain; cash += proceeds; fees += fee + tax;
      sells.push({ id: t.id, code: code, name: ps.name, date: t.trade_date, qty: q, gain: gain, pct: removed > 0 ? gain / removed : null, holdDays: ps.openDate ? dayDiff(ps.openDate, t.trade_date) : null, emotion: t.emotion || null });
      ps.qty -= q; ps.cost -= removed;
      if (ps.qty === 0) { ps.cost = 0; ps.openDate = null; }
    }
  }
  const positions = Object.keys(pos).map((k) => pos[k]).map((x) => ({ code: x.code, name: x.name, qty: x.qty, cost: x.cost, avg: x.qty ? x.cost / x.qty : null, realized: x.realized, openDate: x.openDate }));
  return { positions: positions, cash: cash, realized: realized, fees: fees, deposits: deposits, sells: sells, errors: errors };
}

// prices: { code: { close, date } }, policy: 최신 정책(없으면 {}), sectors: { code: '업종' }, today: 'YYYYMMDD'
function pfSummary(ledger, prices, policy, sectors, today) {
  policy = policy || {}; prices = prices || {}; sectors = sectors || {};
  const held = ledger.positions.filter((x) => x.qty > 0);
  let mvSum = 0, costSum = 0, unpriced = 0;
  const rows = held.map((x) => {
    const pr = prices[x.code], close = pr && pr.close > 0 ? pr.close : null;
    const mv = close != null ? x.qty * close : null;
    if (mv != null) mvSum += mv; else unpriced++;
    costSum += x.cost;
    return { code: x.code, name: x.name, qty: x.qty, avg: x.avg, cost: x.cost, close: close, priceDate: pr ? pr.date : null, mv: mv, unreal: mv != null ? mv - x.cost : null, unrealPct: mv != null && x.cost > 0 ? mv / x.cost - 1 : null, sector: sectors[x.code] || null, realized: x.realized };
  });
  const total = ledger.cash + mvSum;
  rows.forEach((r) => { r.weight = r.mv != null && total > 0 ? r.mv / total : null; });
  rows.sort((a, b) => (b.mv || 0) - (a.mv || 0));
  const topN = (n) => total > 0 ? rows.slice(0, n).reduce((s, r) => s + (r.mv || 0), 0) / total : null;
  const sec = {};
  rows.forEach((r) => { const k = r.sector || "미분류"; sec[k] = (sec[k] || 0) + (r.mv || 0); });
  const sectors2 = Object.keys(sec).map((k) => ({ name: k, mv: sec[k], weight: total > 0 ? sec[k] / total : null })).sort((a, b) => b.mv - a.mv);
  const warnings = [];
  if (unpriced) warnings.push({ level: "warn", code: "unpriced", msg: "현재가를 못 가져온 보유 종목이 " + unpriced + "개 있어 총자산·비중이 부정확합니다." });
  if (ledger.cash < 0) warnings.push({ level: "warn", code: "cash_neg", msg: "현금이 음수입니다. 입금 기록이 빠졌거나(또는 신용·미수 사용) 거래 기록이 틀렸을 수 있습니다." });
  if (ledger.errors.length) warnings.push({ level: "warn", code: "ledger", msg: "원장 오류 " + ledger.errors.length + "건: " + ledger.errors[0].msg });
  rows.forEach((r) => {
    if (policy.max_position_pct > 0 && r.weight != null && r.weight * 100 > policy.max_position_pct) warnings.push({ level: "review", code: "over_weight", msg: r.name + " 비중 " + (r.weight * 100).toFixed(1) + "%가 정책 한도 " + policy.max_position_pct + "%를 넘었습니다(검토 요청: 가격 상승으로 인한 초과인지, 가설 변화인지 구분)." });
    if (r.priceDate && today && Number(today) - Number(r.priceDate) > 7) warnings.push({ level: "warn", code: "stale", msg: r.name + " 가격 기준일(" + r.priceDate + ")이 오래됐습니다." });
  });
  if (policy.min_cash_pct > 0 && total > 0 && ledger.cash / total * 100 < policy.min_cash_pct) warnings.push({ level: "review", code: "low_cash", msg: "현금 비중 " + (ledger.cash / total * 100).toFixed(1) + "%가 정책 최소 " + policy.min_cash_pct + "% 아래입니다." });
  if (policy.cash_needs_krw > 0 && ledger.cash < policy.cash_needs_krw) warnings.push({ level: "review", code: "cash_needs", msg: "현금이 예정 현금 수요(" + Math.round(policy.cash_needs_krw).toLocaleString() + "원)보다 적습니다." });
  return { total: total, cash: ledger.cash, cashPct: total > 0 ? ledger.cash / total : null, mv: mvSum, cost: costSum, unreal: mvSum - (costSum - rows.filter((r) => r.mv == null).reduce((s, r) => s + r.cost, 0)), realized: ledger.realized, fees: ledger.fees, rows: rows, top1: topN(1), top3: topN(3), top5: topN(5), sectors: sectors2, warnings: warnings, partial: unpriced > 0 };
}

// 포지션 크기: 세 한도(위험 예산·집중도·유동성)에 현금 한도를 더해 가장 보수적인 값을 쓴다.
// i: { base(기준 자산), riskPct, buyPrice, riskPrice, maxPosPct, curPosValue, cash, minCashPct, cashNeeds, feePct, adv(20일 평균 거래대금, 원), advPct, bearPrice }
function pfSizePosition(i) {
  const n = (v) => (v == null || v === "" || !isFinite(Number(v))) ? null : Number(v);
  const base = n(i.base), buy = n(i.buyPrice), risk = n(i.riskPrice), riskPct = n(i.riskPct), maxPct = n(i.maxPosPct);
  const errors = [];
  if (!(base > 0)) errors.push("기준 자산(계좌 총자산)이 필요합니다.");
  if (!(buy > 0)) errors.push("매수 예정가를 입력하세요.");
  if (!(risk > 0)) errors.push("위험관리 기준 가격(이 가격 아래면 가설·위험을 재검토할 가격)이 필요합니다.");
  if (buy > 0 && risk > 0 && risk >= buy) errors.push("위험관리 기준 가격이 매수가와 같거나 더 높아 손실 한도 계산이 성립하지 않습니다. 입력을 확인하세요.");
  if (!(riskPct > 0)) errors.push("정책의 위험 예산 비율(1회 매수의 허용 손실 %)이 필요합니다.");
  if (!(maxPct > 0)) errors.push("정책의 단일 종목 최대 비중이 필요합니다.");
  if (errors.length) return { ok: false, errors: errors };
  const cur = n(i.curPosValue) || 0, cash = n(i.cash), minCashPct = n(i.minCashPct) || 0, needs = n(i.cashNeeds) || 0, feePct = n(i.feePct) || 0;
  const lossBudget = base * riskPct / 100, perShare = buy - risk;
  const limits = [];
  limits.push({ key: "risk", label: "위험 예산", qty: Math.floor(lossBudget / perShare), note: "허용 손실 " + Math.round(lossBudget).toLocaleString() + "원 ÷ 주당 계획 손실 " + Math.round(perShare).toLocaleString() + "원" });
  const concAmt = Math.max(0, base * maxPct / 100 - cur);
  limits.push({ key: "conc", label: "집중도(종목 최대 비중)", qty: Math.floor(concAmt / buy), note: "최대 " + maxPct + "% = " + Math.round(base * maxPct / 100).toLocaleString() + "원 − 기보유 " + Math.round(cur).toLocaleString() + "원" });
  if (cash != null) {
    const usable = Math.max(0, cash - base * minCashPct / 100 - needs);
    limits.push({ key: "cash", label: "가용 현금", qty: Math.floor(usable / (buy * (1 + feePct / 100))), note: "현금 " + Math.round(cash).toLocaleString() + "원 − 최소 현금 " + Math.round(base * minCashPct / 100).toLocaleString() + "원 − 예정 수요 " + Math.round(needs).toLocaleString() + "원" });
  }
  const adv = n(i.adv), advPct = n(i.advPct);
  if (adv > 0 && advPct > 0) limits.push({ key: "liq", label: "유동성(하루 거래대금 대비)", qty: Math.floor(adv * advPct / 100 / buy), note: "20일 평균 거래대금 " + Math.round(adv).toLocaleString() + "원 × " + advPct + "%" });
  const minQty = Math.min.apply(null, limits.map((l) => l.qty));
  const binding = limits.filter((l) => l.qty === minQty).map((l) => l.label);
  const amount = minQty * buy, fee = amount * feePct / 100;
  const bear = n(i.bearPrice), refLoss = bear > 0 ? bear : risk;
  const cashAfter = cash != null ? cash - amount - fee : null;
  const warnings = [];
  if (adv == null || !(adv > 0)) warnings.push("유동성 자료가 없어 유동성 한도는 적용하지 못했습니다(체결 가능성을 따로 확인하세요).");
  warnings.push("손실 계산은 기준 가격에 체결된다는 가정의 근사치입니다. 갭 하락·거래정지·하한가에서는 실제 손실이 더 클 수 있습니다.");
  if (minQty <= 0) warnings.push("현재 정책·자산 기준으로는 신규 매수 한도가 0주입니다(보류·현금 보유도 유효한 선택입니다).");
  return { ok: true, qty: minQty, amount: amount, fee: fee, weightAfter: (cur + amount) / base, cashAfter: cashAfter, cashAfterPct: cashAfter != null ? cashAfter / base : null, limits: limits, binding: binding, lossBudget: lossBudget, perShareLoss: perShare, scenarioLoss: minQty * (buy - refLoss), scenarioRef: bear > 0 ? "Bear 가격" : "위험관리 기준가", scenarioLossPct: minQty * (buy - refLoss) / base, warnings: warnings };
}

// 분할 수량 배분: 가중치(%)를 정규화해 정수 주로 나누고 남는 주는 앞 회차부터 1주씩 더한다. 가중치 자체는 정답이 아니라 사용자 선택이다.
function pfSplitTranches(totalQty, weights) {
  const w = (weights || []).map((x) => Math.max(0, Number(x) || 0)), s = w.reduce((a, b) => a + b, 0);
  if (!(totalQty > 0) || !(s > 0)) return w.map(() => 0);
  const qs = w.map((x) => Math.floor(totalQty * x / s));
  let rest = totalQty - qs.reduce((a, b) => a + b, 0);
  for (let k = 0; rest > 0 && qs.length; k = (k + 1) % qs.length) { if (w[k] > 0) { qs[k]++; rest--; } }
  return qs;
}

// 거래 기록 직전 사전 점검. 결과 level: block(기록 불가) / warn(이유를 적고 승인해야 기록) / info(참고)
// c: { trade:{side,code,name,qty,price,reason}, ledger, policy, plan(해당 종목 최신 계획 또는 null), totalValue, nowMs, emotion }
function pfTradeWarnings(c) {
  const t = c.trade, pol = c.policy || {}, plan = c.plan || null, out = [];
  const q = Number(t.qty) || 0, p = Number(t.price) || 0;
  if (t.side === "deposit" || t.side === "withdraw") return out;
  if (!(q > 0) || !Number.isInteger(q)) out.push({ id: "qty", level: "block", msg: "수량은 1 이상의 정수여야 합니다." });
  if (!(p > 0)) out.push({ id: "price", level: "block", msg: "체결 가격을 입력하세요." });
  const pos = c.ledger.positions.filter((x) => x.code === t.code)[0];
  const held = pos ? pos.qty : 0;
  if (t.side === "sell") {
    if (q > held) out.push({ id: "oversell", level: "block", msg: "보유 수량(" + held + "주)보다 많이 팔 수 없습니다." });
    if (!t.reason) out.push({ id: "reason", level: "warn", msg: "매도 사유(가치 도달/가설 훼손/가격 위험/비중 초과/현금 필요/대체 기회/비용·세금)를 선택하지 않았습니다." });
    if ((c.emotion === "불안" || c.emotion === "공포") && t.reason !== "가설 훼손") out.push({ id: "emotion_sell", level: "info", msg: "불안한 상태에서 가설 훼손 이외의 이유로 매도하려 합니다. 하루 뒤 같은 판단인지 다시 확인해 보세요(실무 권고, 효과 크기는 증명되지 않음)." });
    return out;
  }
  if (!plan) out.push({ id: "noplan", level: "warn", msg: "이 종목의 투자 계획(가설·반증 조건·위험 기준가)이 없습니다. 계획 없이 매수하면 사후 평가가 어렵습니다." });
  else {
    if (plan.status === "보류" || plan.status === "종료" || plan.status === "매도검토") out.push({ id: "planstatus", level: "warn", msg: "계획 상태가 '" + plan.status + "'입니다. 신규 매수 결정이 계획과 맞는지 확인하세요." });
    const hrs = pol.cooldown_hours > 0 && plan.created_ms ? (c.nowMs - plan.created_ms) / 3600000 : null;
    if (hrs != null && hrs < pol.cooldown_hours) out.push({ id: "cooldown", level: "warn", msg: "계획을 만든 지 " + hrs.toFixed(1) + "시간 지났습니다(정책 숙려 시간 " + pol.cooldown_hours + "시간). 즉흥 매수가 아닌지 확인하세요." });
    if (plan.entry_high > 0 && p > plan.entry_high) out.push({ id: "fomo", level: "warn", msg: "체결가가 계획한 매수 구간 상단(" + Math.round(plan.entry_high).toLocaleString() + "원)보다 높습니다. 오르는 가격을 쫓는 매수(FOMO)는 아닌지 확인하세요." });
    if (plan.risk_price > 0 && p <= plan.risk_price) out.push({ id: "riskprice", level: "warn", msg: "체결가가 위험관리 기준가(" + Math.round(plan.risk_price).toLocaleString() + "원) 이하입니다. 기준가 이하에서는 가설 재검토가 먼저입니다." });
    if (plan.max_qty > 0 && held + q > plan.max_qty) out.push({ id: "maxqty", level: "warn", msg: "계획의 최대 수량(" + plan.max_qty + "주)을 넘습니다(보유 " + held + " + 매수 " + q + ")." });
  }
  if (pos && pos.avg != null && p < pos.avg) out.push({ id: "avgdown", level: "warn", needsCheck: true, msg: "평균 매입가(" + Math.round(pos.avg).toLocaleString() + "원)보다 낮은 가격의 추가 매수입니다. 평균단가를 낮추는 행위가 아니라 '지금 새로 사도 되는가'를 판단하는 별도 결정이어야 합니다." });
  if (c.totalValue > 0) {
    const curV = pos ? pos.qty * (c.priceNow || p) : 0, after = (curV + q * p) / c.totalValue;
    if (pol.max_position_pct > 0 && after * 100 > pol.max_position_pct) out.push({ id: "conc", level: "warn", msg: "이 매수 후 비중이 약 " + (after * 100).toFixed(1) + "%로 정책 한도 " + pol.max_position_pct + "%를 넘습니다." });
  }
  const cashAfter = c.ledger.cash - q * p;
  if (cashAfter < 0) out.push({ id: "cash_neg", level: "warn", msg: "이 매수 후 현금이 음수입니다(입금 기록 누락 또는 신용 사용 여부 확인)." });
  else if (c.totalValue > 0 && pol.min_cash_pct > 0 && cashAfter / c.totalValue * 100 < pol.min_cash_pct) out.push({ id: "mincash", level: "warn", msg: "이 매수 후 현금 비중이 정책 최소 " + pol.min_cash_pct + "% 아래로 내려갑니다." });
  return out;
}

// 행동 점검: 내 기록에서 계산되는 통계. 표본이 작으면 결론을 내지 않고 "표본 부족"으로 표시한다.
function pfBehavior(ledger, trades, journal, totalValue, nowMs) {
  const sells = ledger.sells || [], live = (trades || []).filter((t) => !t.voided && (t.side === "buy" || t.side === "sell"));
  const buys = live.filter((t) => t.side === "buy");
  const avg = (a) => a.length ? a.reduce((s, x) => s + x, 0) / a.length : null;
  const win = sells.filter((s) => s.gain > 0 && s.holdDays != null), lose = sells.filter((s) => s.gain < 0 && s.holdDays != null);
  const enough = sells.length >= 10 && win.length >= 3 && lose.length >= 3;
  const wH = avg(win.map((s) => s.holdDays)), lH = avg(lose.map((s) => s.holdDays));
  const disposition = { n: sells.length, nWin: win.length, nLose: lose.length, winHold: wH, loseHold: lH, ratio: wH > 0 && lH != null ? lH / wH : null, enough: enough };
  const cutoff = nowMs - 90 * 86400000;
  const recent = live.filter((t) => t.created_ms && t.created_ms >= cutoff);
  const cost = ledger.fees || 0;
  const linked = buys.filter((t) => t.plan_id).length, warned = buys.filter((t) => t.warn_json && t.warn_json !== "[]").length;
  const avgdown = buys.filter((t) => t.warn_json && t.warn_json.indexOf('"avgdown"') >= 0).length;
  const emo = {};
  live.forEach((t) => { if (t.emotion) emo[t.emotion] = (emo[t.emotion] || 0) + 1; });
  const j = (journal || []).filter((e) => e.kind === "사후검토" && e.process_score != null && e.outcome_pct != null);
  const m = { goodGood: 0, goodBad: 0, badGood: 0, badBad: 0 };
  j.forEach((e) => { const gp = e.process_score >= 70, go = e.outcome_pct >= 0; if (gp && go) m.goodGood++; else if (gp) m.goodBad++; else if (go) m.badGood++; else m.badBad++; });
  return {
    disposition: disposition,
    activity: { trades90: recent.length, buys: buys.length, sells: sells.length, costTotal: cost, costPct: totalValue > 0 ? cost / totalValue : null },
    compliance: { buys: buys.length, planLinked: linked, planLinkedPct: buys.length ? linked / buys.length : null, warned: warned, warnedPct: buys.length ? warned / buys.length : null, avgdown: avgdown },
    emotions: emo, matrix: m, matrixN: j.length,
  };
}


// 켈리 참고값(보조). 내 매도 기록의 수익률(원가 대비)에서 승률·평균 이익·평균 손실을 추정해 계산한다.
// f* = (p·W − q·L) / (W·L·(p+q))  (p=이익 비율, q=손실 비율, W=평균 이익률, L=평균 손실률; 본전은 중립). 한 번의 베팅에 거는 비중의 이론값.
// 한계: 추정치가 불안정하다(승률이 조금만 달라져도 크게 변함). 거래가 서로 독립이고 앞으로도 같은 분포라는 가정, 보유기간이 다른 거래를 같은 베팅으로 보는 단순화가 들어 있다.
// 그래서 표본이 충분할 때만 계산하고, 승률 ±1 표준오차 범위와 1/2·1/4 배율을 함께 보여 준다. 수량을 정하는 도구가 아니라 비교용 참고값이다.
function pfKelly(sells) {
  const s = (sells || []).filter((x) => x && x.pct != null && isFinite(x.pct));
  const n = s.length, w = s.filter((x) => x.pct > 0), l = s.filter((x) => x.pct < 0);
  const out = { n: n, nWin: w.length, nLose: l.length, minN: 30, minEach: 5, enough: false };
  if (n < out.minN || w.length < out.minEach || l.length < out.minEach) { out.reason = "표본 부족: 매도 " + n + "건(이익 " + w.length + ", 손실 " + l.length + "). 매도 " + out.minN + "건 이상, 이익·손실 각 " + out.minEach + "건 이상이 필요합니다."; return out; }
  const mean = (a) => a.reduce((t, x) => t + x, 0) / a.length;
  const W = mean(w.map((x) => x.pct)), L = -mean(l.map((x) => x.pct));
  const f = (p, q) => (p * W - q * L) / (W * L * (p + q));
  const p = w.length / n, q = l.length / n, se = Math.sqrt(p * (1 - p) / n);
  const cap = (v) => Math.min(1, v);
  out.enough = true; out.p = p; out.q = q; out.W = W; out.L = L; out.payoff = W / L;
  out.fRaw = f(p, q); out.capped = out.fRaw > 1; // 100% 초과는 레버리지(빌린 돈)를 뜻해 현물 계좌에선 의미가 없으므로 100%로 자른다
  out.f = cap(out.fRaw);
  const lowRaw = f(Math.max(0, p - se), Math.min(1, q + se)), highRaw = f(Math.min(1, p + se), Math.max(0, q - se));
  out.fLow = Math.max(0, cap(lowRaw)); out.fHigh = Math.max(0, cap(highRaw));
  out.edge = out.fRaw > 0;
  out.half = out.edge ? cap(out.fRaw / 2) : 0; out.quarter = out.edge ? cap(out.fRaw / 4) : 0;
  out.fragile = lowRaw <= 0 || highRaw > 2 * Math.max(out.fRaw, 0.0001); // 승률이 1 표준오차만 흔들려도 부호가 바뀌거나 값이 두 배가 되는가
  return out;
}

// ============================================================
// 내 계좌·매매 관리 — 서버(D1 + API). 모든 /api/pf/* 는 접근 키(PF_KEY, Cloudflare 비밀 변수)가 있어야 한다.
// 이 사이트 주소는 공개라서 키 없이 개인 보유 내역을 저장·조회하게 두면 누구나 볼 수 있다 → 키가 설정되지 않았으면 모두 거부(기본 닫힘).
// 원칙: 원장(거래)은 지우지 않고 "무효 처리"만 한다(이력 보존). 정책·투자 계획은 덮어쓰지 않고 새 버전을 쌓는다.
// 이 앱은 주문을 내지 않는다. 여기 기록은 "사용자가 다른 곳(증권사)에서 실제로 체결한 내용을 적은 것"이다.
// ============================================================
let pfTablesReady = false;
async function ensurePfTables(db) {
  if (pfTablesReady) return;
  const run = (sql) => db.prepare(sql).run();
  await run("CREATE TABLE IF NOT EXISTS pf_policy (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, risk_budget_pct REAL, max_position_pct REAL, min_cash_pct REAL, cash_needs_krw REAL, max_dd_tolerance_pct REAL, horizon_months INTEGER, fee_rate_pct REAL, sell_tax_pct REAL, adv_limit_pct REAL, cooldown_hours INTEGER, note TEXT)");
  await run("CREATE TABLE IF NOT EXISTS pf_trades (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, trade_date TEXT NOT NULL, stock_code TEXT, corp_name TEXT, side TEXT NOT NULL, qty INTEGER, price REAL, fee REAL DEFAULT 0, tax REAL DEFAULT 0, plan_id INTEGER, emotion TEXT, reason TEXT, note TEXT, warn_json TEXT, check_json TEXT, ack_reason TEXT, voided INTEGER DEFAULT 0, void_reason TEXT, voided_at TEXT)");
  await run("CREATE INDEX IF NOT EXISTS idx_pf_trades_code ON pf_trades (stock_code, trade_date)");
  await run("CREATE TABLE IF NOT EXISTS pf_plans (id INTEGER PRIMARY KEY AUTOINCREMENT, plan_key INTEGER NOT NULL, version INTEGER NOT NULL, created_at TEXT NOT NULL, stock_code TEXT, corp_name TEXT, status TEXT, thesis TEXT, disproof TEXT, risks TEXT, catalysts TEXT, sector TEXT, alt_considered TEXT, bear_price REAL, base_price REAL, bull_price REAL, valuation_note TEXT, entry_low REAL, entry_high REAL, risk_price REAL, max_qty INTEGER, tranche_json TEXT, sell_rules TEXT, no_avgdown_rules TEXT, review_date TEXT, data_asof TEXT, UNIQUE (plan_key, version))");
  await run("CREATE TABLE IF NOT EXISTS pf_journal (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, kind TEXT, trade_id INTEGER, plan_key INTEGER, stock_code TEXT, corp_name TEXT, emotion TEXT, process_json TEXT, process_score REAL, outcome_pct REAL, luck_skill TEXT, lesson TEXT, outcome_note TEXT, price_at_decision REAL, data_asof TEXT)");
  pfTablesReady = true;
}
function pfAuthError(request, env) {
  const key = env.PF_KEY ? String(env.PF_KEY) : "";
  if (key.length < 12) return Response.json({ error: "개인정보 보호를 위해 접근 키가 필요합니다. Cloudflare 설정에서 PF_KEY(12자 이상 무작위 문자열)를 비밀 변수로 등록한 뒤 다시 열어주세요.", code: "no_key" }, { status: 403 });
  const given = request.headers.get("x-pf-key") || "";
  let diff = given.length ^ key.length; // 길이가 달라도 끝까지 비교(시간 차이로 키를 추측하지 못하게)
  for (let i = 0; i < Math.max(given.length, key.length); i++) diff |= (given.charCodeAt(i) || 0) ^ (key.charCodeAt(i) || 0);
  if (diff !== 0) return Response.json({ error: "접근 키가 맞지 않습니다.", code: "bad_key" }, { status: 401 });
  return null;
}
const PF_STATUSES = ["관찰", "매수검토", "보유", "보류", "축소검토", "매도검토", "종료"];
const PF_SELL_REASONS = ["가치 도달", "가설 훼손", "가격 위험", "비중 초과", "현금 필요", "대체 기회", "비용·세금"];
async function pfLatestPrices(env, codes) {
  const out = {};
  if (!env.DATA_GO_KR_KEY || !codes.length) return out;
  const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
  const kst = new Date(Date.now() + 9 * 3600 * 1000);
  await Promise.all(codes.slice(0, 12).map(async (code) => {
    try {
      const h = (await fetchKrxStockHistory(code, ymd(new Date(kst.getTime() - 14 * 86400000)), ymd(kst), env.DATA_GO_KR_KEY)).filter((x) => x.close > 0);
      if (h.length) out[code] = { close: h[h.length - 1].close, date: h[h.length - 1].date };
    } catch (e) { /* 이 종목만 가격 없음으로 표시 */ }
  }));
  return out;
}
async function pfBuildState(env) {
  const db = env.DB;
  const policy = await db.prepare("SELECT * FROM pf_policy ORDER BY id DESC LIMIT 1").first();
  const { results: trades } = await db.prepare("SELECT * FROM pf_trades ORDER BY trade_date, id").all();
  const { results: plans } = await db.prepare("SELECT p.* FROM pf_plans p WHERE p.version = (SELECT MAX(version) FROM pf_plans q WHERE q.plan_key = p.plan_key) ORDER BY p.id DESC").all();
  const { results: planVersions } = await db.prepare("SELECT plan_key, version, created_at, status FROM pf_plans ORDER BY plan_key, version").all();
  const { results: journal } = await db.prepare("SELECT * FROM pf_journal ORDER BY id DESC LIMIT 200").all();
  const tr = trades.map((t) => Object.assign({}, t, { voided: !!t.voided, created_ms: Date.parse(t.created_at) }));
  const ledger = pfLedger(tr);
  const held = ledger.positions.filter((x) => x.qty > 0).map((x) => x.code);
  const prices = await pfLatestPrices(env, held);
  const { results: secRows } = await db.prepare("SELECT stock_code, sector FROM pf_plans WHERE sector IS NOT NULL AND sector <> '' ORDER BY id").all();
  const sectors = {}; secRows.forEach((p) => { sectors[p.stock_code] = p.sector; }); // 가장 최근에 적은 업종 사용(새 버전에서 비워도 유지)
  const today = todayStr();
  const summary = pfSummary(ledger, prices, policy || {}, sectors, today);
  const behavior = pfBehavior(ledger, tr, journal.map((j) => Object.assign({}, j)), summary.total, Date.now());
  return { policy: policy || null, trades: tr.slice().reverse(), plans: plans, planVersions: planVersions, journal: journal, ledger: { cash: ledger.cash, realized: ledger.realized, fees: ledger.fees, deposits: ledger.deposits, errors: ledger.errors, positions: ledger.positions }, summary: summary, behavior: behavior, kelly: pfKelly(ledger.sells), asof: new Date().toISOString(), heldWithoutPrice: held.filter((c) => !prices[c]) };
}
async function handlePf(request, env, pathname, searchParams) {
  const bad = await authGate(request, env);
  if (bad) return bad;
  try {
    await ensurePfTables(env.DB);
    const db = env.DB;
    const str = (v, max) => (v == null ? null : String(v).trim().slice(0, max || 2000)) || null;
    const num = (v) => (v == null || v === "" || !isFinite(Number(v))) ? null : Number(v);
    const body = request.method === "POST" ? await request.json().catch(() => null) : null;
    if (request.method === "POST" && !body) return Response.json({ error: "요청 본문(JSON)이 올바르지 않습니다." }, { status: 400 });
    const nowIso = new Date().toISOString();
    const resolveStock = async (name) => {
      const r = await db.prepare("SELECT corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(name).first();
      return r && r.stock_code ? r : null;
    };

    if (pathname === "/api/pf/ping") return Response.json({ ok: true });
    if (pathname === "/api/pf/state") return Response.json(await pfBuildState(env));
    if (pathname === "/api/pf/export") {
      const out = {};
      for (const t of ["pf_policy", "pf_trades", "pf_plans", "pf_journal"]) out[t] = (await db.prepare("SELECT * FROM " + t).all()).results;
      return new Response(JSON.stringify(out, null, 1), { headers: { "content-type": "application/json; charset=utf-8", "content-disposition": "attachment; filename=pf_backup.json" } });
    }
    if (pathname === "/api/pf/quote") {
      const name = searchParams.get("corp_name") || "";
      const st = await resolveStock(name);
      if (!st) return Response.json({ error: "'" + name + "' 종목을 찾을 수 없습니다." }, { status: 404 });
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
      const kst = new Date(Date.now() + 9 * 3600 * 1000);
      const h = (await fetchKrxStockHistory(st.stock_code, ymd(new Date(kst.getTime() - 40 * 86400000)), ymd(kst), env.DATA_GO_KR_KEY)).filter((x) => x.close > 0);
      if (!h.length) return Response.json({ error: "시세를 받지 못했습니다." }, { status: 502 });
      const last20 = h.slice(-20).map((x) => x.tradingValue).filter((v) => v > 0);
      return Response.json({ corp_name: st.corp_name, stock_code: st.stock_code, close: h[h.length - 1].close, date: h[h.length - 1].date, adv20: last20.length >= 10 ? last20.reduce((a, b) => a + b, 0) / last20.length : null, advN: last20.length });
    }
    if (request.method !== "POST") return Response.json({ error: "지원하지 않는 요청입니다." }, { status: 404 });

    if (pathname === "/api/pf/policy") {
      const rng = (v, lo, hi, nm, nullable) => { const x = num(v); if (x == null) { if (nullable) return null; throw new Error(nm + "을(를) 입력하세요."); } if (x < lo || x > hi) throw new Error(nm + "은(는) " + lo + "~" + hi + " 범위여야 합니다."); return x; };
      let p;
      try {
        p = { risk: rng(body.risk_budget_pct, 0.01, 10, "위험 예산(%)"), maxp: rng(body.max_position_pct, 1, 100, "단일 종목 최대 비중(%)"), minc: rng(body.min_cash_pct, 0, 99, "최소 현금 비중(%)"), needs: rng(body.cash_needs_krw, 0, 1e13, "예정 현금 수요(원)", true) || 0, dd: rng(body.max_dd_tolerance_pct, 1, 100, "감내 최대 낙폭(%)", true), hz: rng(body.horizon_months, 1, 600, "투자 기간(개월)", true), fee: rng(body.fee_rate_pct, 0, 5, "수수료율(%)", true), tax: rng(body.sell_tax_pct, 0, 5, "매도 세율(%)", true), adv: rng(body.adv_limit_pct, 0.1, 100, "하루 거래대금 대비 최대 주문 비율(%)", true), cd: rng(body.cooldown_hours, 0, 720, "숙려 시간(시간)", true) };
      } catch (e) { return Response.json({ error: e.message }, { status: 400 }); }
      await db.prepare("INSERT INTO pf_policy (created_at, risk_budget_pct, max_position_pct, min_cash_pct, cash_needs_krw, max_dd_tolerance_pct, horizon_months, fee_rate_pct, sell_tax_pct, adv_limit_pct, cooldown_hours, note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(nowIso, p.risk, p.maxp, p.minc, p.needs, p.dd, p.hz, p.fee, p.tax, p.adv, p.cd == null ? null : Math.round(p.cd), str(body.note, 1000)).run();
      return Response.json({ ok: true });
    }

    if (pathname === "/api/pf/plan") {
      const st = await resolveStock(str(body.corp_name, 100) || "");
      if (!st) return Response.json({ error: "종목을 찾을 수 없습니다. 상장사 이름을 정확히 입력하세요." }, { status: 400 });
      const status = PF_STATUSES.indexOf(body.status) >= 0 ? body.status : "관찰";
      const thesis = str(body.thesis, 4000), disproof = str(body.disproof, 4000);
      if ((status === "매수검토" || status === "보유") && (!thesis || !disproof)) return Response.json({ error: "투자 논리와 반증 조건(무엇이 관측되면 이 가설이 틀렸다고 볼지)을 적지 않으면 '" + status + "' 상태로 올릴 수 없습니다. 정의하기 어렵다면 '관찰' 또는 '보류'로 두세요." }, { status: 400 });
      let tranches = []; try { tranches = Array.isArray(body.tranches) ? body.tranches.slice(0, 6).map((t) => ({ cond: str(t.cond, 300), price: num(t.price), weightPct: num(t.weightPct) })) : []; } catch (e) { tranches = []; }
      const rp = num(body.risk_price), el = num(body.entry_low), eh = num(body.entry_high);
      if (el != null && eh != null && el > eh) return Response.json({ error: "매수 구간의 하단이 상단보다 클 수 없습니다." }, { status: 400 });
      if (rp != null && el != null && rp >= el) return Response.json({ error: "위험관리 기준가는 매수 구간 하단보다 낮아야 합니다." }, { status: 400 });
      let planKey = num(body.plan_key), version = 1;
      if (planKey != null) {
        const mx = await db.prepare("SELECT MAX(version) AS v FROM pf_plans WHERE plan_key = ? AND stock_code = ?").bind(planKey, st.stock_code).first();
        if (!mx || mx.v == null) return Response.json({ error: "수정할 계획을 찾지 못했습니다." }, { status: 404 });
        version = mx.v + 1;
      } else {
        const mk = await db.prepare("SELECT COALESCE(MAX(plan_key), 0) + 1 AS k FROM pf_plans").first();
        planKey = mk.k;
      }
      await db.prepare("INSERT INTO pf_plans (plan_key, version, created_at, stock_code, corp_name, status, thesis, disproof, risks, catalysts, sector, alt_considered, bear_price, base_price, bull_price, valuation_note, entry_low, entry_high, risk_price, max_qty, tranche_json, sell_rules, no_avgdown_rules, review_date, data_asof) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(planKey, version, nowIso, st.stock_code, st.corp_name, status, thesis, disproof, str(body.risks, 3000), str(body.catalysts, 2000), str(body.sector, 60), str(body.alt_considered, 2000), num(body.bear_price), num(body.base_price), num(body.bull_price), str(body.valuation_note, 3000), el, eh, rp, num(body.max_qty) == null ? null : Math.round(num(body.max_qty)), JSON.stringify(tranches), str(body.sell_rules, 3000), str(body.no_avgdown_rules, 2000), str(body.review_date, 10), str(body.data_asof, 40)).run();
      return Response.json({ ok: true, plan_key: planKey, version: version });
    }

    if (pathname === "/api/pf/trade") {
      const side = body.side;
      if (["buy", "sell", "deposit", "withdraw"].indexOf(side) < 0) return Response.json({ error: "구분(매수/매도/입금/출금)이 올바르지 않습니다." }, { status: 400 });
      const today = new Date(Date.now() + 9 * 3600 * 1000).toISOString().slice(0, 10).replace(/-/g, ""); // 한국 날짜 기준(UTC로 비교하면 새벽 시간대에 오늘 거래가 미래로 거부됨)
      const date = String(body.trade_date || today).replace(/-/g, "");
      if (!/^\d{8}$/.test(date) || date > today) return Response.json({ error: "거래일이 올바르지 않습니다(미래 날짜는 기록할 수 없습니다)." }, { status: 400 });
      const policy = await db.prepare("SELECT * FROM pf_policy ORDER BY id DESC LIMIT 1").first();
      if (side === "deposit" || side === "withdraw") {
        const amt = num(body.price);
        if (!(amt > 0)) return Response.json({ error: "금액(원)을 입력하세요." }, { status: 400 });
        await db.prepare("INSERT INTO pf_trades (created_at, trade_date, side, qty, price, note) VALUES (?,?,?,?,?,?)").bind(nowIso, date, side, 1, amt, str(body.note, 500)).run();
        return Response.json({ ok: true });
      }
      const st = await resolveStock(str(body.corp_name, 100) || "");
      if (!st) return Response.json({ error: "종목을 찾을 수 없습니다. 상장사 이름을 정확히 입력하세요." }, { status: 400 });
      const qty = num(body.qty), price = num(body.price);
      const { results: trs } = await db.prepare("SELECT * FROM pf_trades ORDER BY trade_date, id").all();
      const ledger = pfLedger(trs.map((t) => Object.assign({}, t, { voided: !!t.voided })));
      const plan = await db.prepare("SELECT * FROM pf_plans WHERE stock_code = ? ORDER BY plan_key DESC, version DESC LIMIT 1").bind(st.stock_code).first();
      const planC = plan ? Object.assign({}, plan, { created_ms: Date.parse(plan.created_at) }) : null;
      const book = ledger.positions.reduce((s, x) => s + x.cost, 0) + ledger.cash; // 현재가 조회 없이 장부가 기준 근사(서브요청 절약)
      const emotion = str(body.emotion, 20), reason = str(body.reason, 20);
      const warnings = pfTradeWarnings({ trade: { side: side, code: st.stock_code, name: st.corp_name, qty: qty, price: price, reason: reason }, ledger: ledger, policy: policy || {}, plan: planC, totalValue: book, nowMs: Date.now(), emotion: emotion });
      const blocks = warnings.filter((w) => w.level === "block");
      if (blocks.length) return Response.json({ error: blocks.map((w) => w.msg).join(" "), warnings: warnings }, { status: 400 });
      const warns = warnings.filter((w) => w.level === "warn");
      const checks = Array.isArray(body.checks) ? body.checks.map((x) => x === true) : null;
      const needCheck = warns.some((w) => w.needsCheck);
      const ackReason = str(body.ack_reason, 500);
      if (warns.length && (!body.ack || !ackReason || ackReason.length < 5 || (needCheck && (!checks || checks.length < 6)))) {
        return Response.json({ needs_ack: true, needs_check: needCheck, warnings: warnings, error: "사전 점검 경고가 있습니다. 내용을 확인하고 이유를 적어 승인해야 기록됩니다." }, { status: 409 });
      }
      const feeRate = policy ? policy.fee_rate_pct : null, taxRate = policy ? policy.sell_tax_pct : null;
      const fee = num(body.fee) != null ? num(body.fee) : (feeRate != null ? Math.round(qty * price * feeRate / 100) : 0);
      const tax = side === "sell" ? (num(body.tax) != null ? num(body.tax) : (taxRate != null ? Math.round(qty * price * taxRate / 100) : 0)) : 0;
      await db.prepare("INSERT INTO pf_trades (created_at, trade_date, stock_code, corp_name, side, qty, price, fee, tax, plan_id, emotion, reason, note, warn_json, check_json, ack_reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(nowIso, date, st.stock_code, st.corp_name, side, Math.round(qty), price, fee, tax, plan ? plan.id : null, emotion, reason, str(body.note, 800), JSON.stringify(warnings.map((w) => ({ id: w.id, level: w.level }))), checks ? JSON.stringify(checks) : null, ackReason).run();
      return Response.json({ ok: true, fee: fee, tax: tax, warnings: warnings, costNote: (feeRate == null || (side === "sell" && taxRate == null)) ? "정책에 수수료율·세율이 없어 비용을 0으로 기록했습니다. 실제 비용을 직접 입력하거나 정책에 입력하세요." : null });
    }

    if (pathname === "/api/pf/trade/void") {
      const id = num(body.id), why = str(body.reason, 300);
      if (!id || !why || why.length < 3) return Response.json({ error: "무효 처리 사유(3자 이상)를 적어주세요." }, { status: 400 });
      const r = await db.prepare("UPDATE pf_trades SET voided = 1, void_reason = ?, voided_at = ? WHERE id = ? AND voided = 0").bind(why, nowIso, id).run();
      return Response.json({ ok: true, changed: r.meta ? r.meta.changes : null });
    }

    if (pathname === "/api/pf/journal") {
      const kind = body.kind === "결정" ? "결정" : "사후검토";
      const st = body.corp_name ? await resolveStock(str(body.corp_name, 100)) : null;
      const proc = Array.isArray(body.process) ? body.process.slice(0, 12).map((x) => x === true) : null;
      const score = proc && proc.length ? proc.filter(Boolean).length / proc.length * 100 : null;
      await db.prepare("INSERT INTO pf_journal (created_at, kind, trade_id, plan_key, stock_code, corp_name, emotion, process_json, process_score, outcome_pct, luck_skill, lesson, outcome_note, price_at_decision, data_asof) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(nowIso, kind, num(body.trade_id), num(body.plan_key), st ? st.stock_code : null, st ? st.corp_name : null, str(body.emotion, 20), proc ? JSON.stringify(proc) : null, score, num(body.outcome_pct), str(body.luck_skill, 30), str(body.lesson, 3000), str(body.outcome_note, 3000), num(body.price_at_decision), str(body.data_asof, 40)).run();
      return Response.json({ ok: true, process_score: score });
    }
    return Response.json({ error: "알 수 없는 요청입니다." }, { status: 404 });
  } catch (e) {
    return Response.json({ error: "서버 오류: " + ((e && e.message) || String(e)) }, { status: 500 });
  }
}

// ===== 보안: 응답 헤더 · 접근 키 시도 제한 · 홈 화면 앱(PWA) 자원 =====
const APP_ICON_B64 = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAIAAADdvvtQAAAB7ElEQVR42u3dyw3CMBBAQQdRAi2ExkJZVEYLoQhu3JDyI/Z6Z+44wjyZ5MAyjNNcYKuLLUBACAgBISAQEAJCQAgIBISAEBACAgEhIASEgEBACAgBISAEBAJCQAgIAYGAEBACQkAgIASEgBAQfF1twTlez9uel98fbyeQemquIKC89bTckIBi1NNsQwIKU0+bDQkIASEgBISAQEAICAEhIBAQAkJACAgEhIAQEAICASEgBISAEBAICAEhIAQEAkJACAgBwS/pRtz1OmrOCRSgntLwqDkBBahHQ3kD6n7UnIDC1KMhT2EICAEhIAQEAkJACAgBgYAQEAJCQCAgBISAEBAICAEhIAQEAkJACAgBkVS16RymZDiBqtVT/LI4c0CmZAiolU9dQ7kCMiVDQCAgBISAEBACsgUICAEhIAQEAkJACAgBgYAQEAJCQCAgBISAEBAICAEhIASEgEBACAgBISAQEAJCQAgIBISAEBACAgEhIASEgFb4x380LVkz23V7PoGOff/LV8t23Z6/wo7ahbXrZLtuz/dA+/di2wrZrnuCYZxmd4J4CkNACAgBgYAQEAJCQCAgBISAEBAICAEhIAQEAkJACAgBISAQEAJCQAgIBISAEBACAgEhIASEgEBACAgBISAEBAJCQATzASY9VWvxWyPqAAAAAElFTkSuQmCC";
const SEC_CSP = "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob:; connect-src 'self'; manifest-src 'self'; object-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'";
function withSecurityHeaders(res, pathname) {
  const h = new Headers(res.headers);
  h.set("content-security-policy", SEC_CSP);
  h.set("x-content-type-options", "nosniff");
  h.set("x-frame-options", "DENY");
  h.set("referrer-policy", "no-referrer");
  h.set("permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()");
  h.set("strict-transport-security", "max-age=31536000");
  h.set("cross-origin-opener-policy", "same-origin");
  if (pathname.startsWith("/api/")) h.set("cache-control", "no-store");
  else if (pathname === "/" && !h.has("cache-control")) h.set("cache-control", "no-cache");
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: h });
}
const AUTH_MAX_FAILS = 8;
const AUTH_WINDOW_MS = 15 * 60 * 1000;
function pfKeyOn(env) { return !!(env.PF_KEY && String(env.PF_KEY).length >= 12); }
// 키 대조 + 실패 횟수 제한(IP당 15분에 8회). 키를 아예 보내지 않은 요청은 횟수에 넣지 않는다. 제한 장치의 DB 오류로 정상 사용이 막히지는 않게(통과) 둔다.
async function authGate(request, env) {
  if (!pfKeyOn(env)) return pfAuthError(request, env);
  const given = request.headers.get("x-pf-key") || "";
  if (!given) return pfAuthError(request, env);
  const ip = request.headers.get("cf-connecting-ip") || "unknown";
  const now = Date.now();
  let row = null;
  try {
    try { row = await env.DB.prepare("SELECT n, ws FROM auth_fail WHERE ip = ?").bind(ip).first(); }
    catch (e) { await env.DB.prepare("CREATE TABLE IF NOT EXISTS auth_fail (ip TEXT PRIMARY KEY, n INTEGER NOT NULL, ws INTEGER NOT NULL)").run(); }
    if (row && now - row.ws < AUTH_WINDOW_MS && row.n >= AUTH_MAX_FAILS) {
      const wait = Math.max(1, Math.ceil((AUTH_WINDOW_MS - (now - row.ws)) / 1000));
      return Response.json({ error: "접근 키를 여러 번 틀려 잠시 잠겼습니다. 약 " + Math.ceil(wait / 60) + "분 뒤에 다시 시도하세요.", code: "locked" }, { status: 429, headers: { "retry-after": String(wait) } });
    }
  } catch (e) { /* 제한 장치 오류는 무시 */ }
  const bad = pfAuthError(request, env);
  try {
    if (bad) {
      await env.DB.prepare("INSERT INTO auth_fail (ip, n, ws) VALUES (?, 1, ?) ON CONFLICT(ip) DO UPDATE SET n = CASE WHEN ? - ws >= ? THEN 1 ELSE n + 1 END, ws = CASE WHEN ? - ws >= ? THEN ? ELSE ws END")
        .bind(ip, now, now, AUTH_WINDOW_MS, now, AUTH_WINDOW_MS, now).run();
    } else if (row) {
      await env.DB.prepare("DELETE FROM auth_fail WHERE ip = ?").bind(ip).run();
    }
  } catch (e) { /* 기록 실패는 무시 */ }
  return bad;
}
function appIconResponse() {
  const bin = atob(APP_ICON_B64); const u8 = new Uint8Array(bin.length); for (let i = 0; i < bin.length; i++) u8[i] = bin.charCodeAt(i);
  return new Response(u8, { headers: { "content-type": "image/png", "cache-control": "public, max-age=86400" } });
}
const APP_MANIFEST = { name: "VALUELAB 한국주식 스크리닝·재무분석", short_name: "VALUELAB", start_url: "/", scope: "/", display: "standalone", background_color: "#f8fafc", theme_color: "#2563eb", lang: "ko", icons: [{ src: "/icon-192.png", sizes: "192x192", type: "image/png", purpose: "any" }] };

export default {
  async fetch(request, env) {
    try {
      return withSecurityHeaders(await handleRequest(request, env), new URL(request.url).pathname);
    } catch (e) {
      // 라우트 하나하나에 try/catch를 다 붙이는 대신, 맨 바깥에서 한 번에 받아서 "화면에 원인이 보이는"
      // 일관된 JSON 에러로 바꿔준다. 콘솔에도 남겨서 Cloudflare Observability에서 스택을 확인할 수 있다.
      console.error("처리되지 않은 오류:", e && e.stack ? e.stack : e);
      return withSecurityHeaders(Response.json({ error: (e && e.message) || "알 수 없는 오류가 발생했습니다." }, { status: 500 }), "/api/");
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

    if (pathname === "/manifest.webmanifest") return new Response(JSON.stringify(APP_MANIFEST), { headers: { "content-type": "application/manifest+json; charset=utf-8", "cache-control": "public, max-age=3600" } });
    if (pathname === "/icon-192.png" || pathname === "/apple-touch-icon.png") return appIconResponse();

    // 접근 키 상태 확인(화면이 잠금 여부를 알기 위함). 키 값은 응답에 담지 않는다.
    if (pathname === "/api/auth/status") {
      const on = pfKeyOn(env);
      if (!on) return Response.json({ protected: false, ok: true });
      if (!request.headers.get("x-pf-key")) return Response.json({ protected: true, ok: false });
      const bad = await authGate(request, env);
      if (bad && bad.status === 429) return bad;
      return Response.json({ protected: true, ok: !bad });
    }

    if (pathname.startsWith("/api/pf/")) return handlePf(request, env, pathname, searchParams);

    // PF_KEY가 설정돼 있으면 나머지 /api 호출(데이터 수집·조회 모두)도 키가 있어야 한다: 공개 주소로 외부 API 한도와 DB 쓰기 한도를 소진시키는 것을 막는다.
    if (pathname.startsWith("/api/") && pfKeyOn(env)) {
      const bad = await authGate(request, env);
      if (bad) return bad;
    }

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

    // 일별 종가 시계열(차트용). 최대 약 3년. 수정주가가 아니라서 액면분할·병합이 있으면 가격이 끊겨 보이므로 화면에서 ±30% 초과 변동일을 알려준다.
    if (pathname === "/api/price/series") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!corpRow.stock_code) return Response.json({ error: "종목코드가 없어 주가를 조회할 수 없습니다." }, { status: 400 });
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      const days = Math.min(Math.max(Number(searchParams.get("days")) || 1100, 90), 1200);
      const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");
      const kstNow = new Date(Date.now() + 9 * 3600 * 1000);
      const hist = (await fetchKrxStockHistory(corpRow.stock_code, ymd(new Date(kstNow.getTime() - days * 86400000)), ymd(kstNow), env.DATA_GO_KR_KEY)).filter((x) => x.close > 0);
      if (!hist.length) return Response.json({ error: "data.go.kr에서 이 종목의 시세를 받지 못했습니다." }, { status: 502 });
      return new Response(JSON.stringify({
        corp_name: corpRow.corp_name, stock_code: corpRow.stock_code, n: hist.length, from: hist[0].date, to: hist[hist.length - 1].date,
        d: hist.map((x) => x.date), c: hist.map((x) => x.close), v: hist.map((x) => x.volume == null ? null : x.volume), tv: hist.map((x) => x.tradingValue == null ? null : x.tradingValue),
      }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=300" } });
    }

    // 재료 점검용 뉴스: 네이버 검색 API(제목·링크·날짜만 전달, 기사 본문은 받지 않음). 키는 Worker 환경변수에서만 읽고 응답·로그에 넣지 않는다.
    if (pathname === "/api/news") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!env.NAVER_CLIENT_ID || !env.NAVER_CLIENT_SECRET) return Response.json({ error: "NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 환경변수가 설정되지 않았습니다.", nokey: true }, { status: 501 });
      const days = Math.min(Math.max(Number(searchParams.get("days")) || 365, 30), 400);
      const kst = new Date(Date.now() + 9 * 3600 * 1000);
      const endYmd = kst.toISOString().slice(0, 10).replace(/-/g, "");
      const cutoff = addDaysStr(endYmd, -days);
      const got = await collectNews((st) => fetchNaverNewsPage('"' + corpRow.corp_name + '"', st, env.NAVER_CLIENT_ID, env.NAVER_CLIENT_SECRET), corpRow.corp_name, cutoff);
      if (got.error && !got.items.length) return Response.json({ error: got.error }, { status: 502 });
      return new Response(JSON.stringify({ corp_name: corpRow.corp_name, from: cutoff, to: endYmd, days, got: got.items.length, partial: got.partial, total: got.total, warn: got.error, items: got.items }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=600" } });
    }

    // 재료 점검용: 최근 N일 DART 공시목록 + 같은 기간 코스피 일별 종가(시장조정 수익률 계산용). 뉴스는 /api/news.
    if (pathname === "/api/disclosures") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      if (!env.DART_PROXY_URL) return Response.json({ error: "DART_PROXY_URL 미설정" }, { status: 500 });
      const days = Math.min(Math.max(Number(searchParams.get("days")) || 365, 30), 400);
      const kst = new Date(Date.now() + 9 * 3600 * 1000);
      const endYmd = kst.toISOString().slice(0, 10).replace(/-/g, "");
      const bgnYmd = addDaysStr(endYmd, -days);
      // Apps Script는 첫 호출이 느리거나(콜드 스타트) DART가 잠깐 지연될 수 있어, 첫 쪽만 한 번 더 시도한다.
      const got = await collectDisclosures(async (p) => {
        try { return await fetchDartList(corpRow.corp_code, bgnYmd, endYmd, env.DART_PROXY_URL, p, 100); }
        catch (e) { if (p !== 1) throw e; return await fetchDartList(corpRow.corp_code, bgnYmd, endYmd, env.DART_PROXY_URL, p, 100); }
      });
      if (got.error) return Response.json({ error: got.error }, { status: 502 });
      let idx = { d: [], c: [] };
      try {
        const { results } = await env.DB.prepare("SELECT market_date, close_price FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 300").bind(KOSPI_NAME).all();
        const rows = (results || []).slice().reverse();
        idx = { d: rows.map((r) => r.market_date), c: rows.map((r) => r.close_price) };
      } catch (e) { /* 지수 표가 없으면 시장조정 없이 표시 */ }
      return new Response(JSON.stringify({ corp_name: corpRow.corp_name, stock_code: corpRow.stock_code, from: bgnYmd, to: endYmd, days, total_count: got.total_count, got: got.items.length, partial: got.partial, items: got.items, idx }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=600" } });
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
      let accMt = null; try { accMt = await getAccMt(env.DB, corpRow.corp_code); } catch (e) { /* 표시용이라 실패해도 진행 */ }

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, acc_mt: accMt, row });
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

      let periods = buildPeriodList(startYear, endYear);
      let skipped = 0;
      // skip_existing=1: 이미 값이 저장된 기간은 다시 받지 않는다(종목당 최대 44행만 읽음). 매출·영업이익·순이익이 모두 비어 있는 행(공시 전에 저장된 빈 행)은 없는 것으로 보고 다시 받는다.
      if (searchParams.get("skip_existing") === "1") {
        const { results: haveRows } = await env.DB.prepare(
          "SELECT bsns_year, reprt_code FROM financial_raw WHERE corp_code = ? AND bsns_year >= ? AND bsns_year <= ? AND (revenue IS NOT NULL OR net_income IS NOT NULL OR operating_income IS NOT NULL)"
        ).bind(corpRow.corp_code, String(startYear), String(endYear)).all();
        const have = new Set((haveRows || []).map((r) => r.bsns_year + "|" + r.reprt_code));
        const before = periods.length;
        periods = periods.filter((p) => !have.has(p.year + "|" + p.code));
        skipped = before - periods.length;
      }
      const now = new Date().toISOString();
      if (!periods.length) return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, queued: 0, skipped });
      // INSERT OR IGNORE: 이미 큐에 있는(아직 처리 안 된) 기간은 중복 등록하지 않음
      const stmts = periods.map((p) => env.DB.prepare(
        "INSERT OR IGNORE INTO fetch_queue (corp_code, corp_name, stock_code, bsns_year, reprt_code, period_order, queued_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
      ).bind(corpRow.corp_code, corpRow.corp_name, corpRow.stock_code, String(p.year), p.code, p.period_order, now));
      await env.DB.batch(stmts);

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, queued: periods.length, skipped });
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

    // 종목 검색(자동완성)용 상장사 목록. 브라우저가 하루 동안 캐시하고 검색은 브라우저에서 한다(타이핑마다 서버·DB를 읽지 않음).
    if (pathname === "/api/stock/list") {
      const { results } = await env.DB.prepare("SELECT corp_name, stock_code FROM corp_master WHERE stock_code IS NOT NULL AND stock_code != '' ORDER BY corp_name").all();
      const stocks = results.map((r) => ({ n: r.corp_name, c: r.stock_code }));
      return new Response(JSON.stringify({ stocks }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=86400" } });
    }

    if (pathname === "/api/db/companies") {
      // financial_raw의 PK 인덱스(corp_code, bsns_year, reprt_code)만으로 집계되는 쿼리라 테이블 본문을 읽지 않는다.
      await ensureFiscalTable(env.DB); await ensureIndustryTable(env.DB);
      const { results } = await env.DB.prepare(
        "SELECT f.corp_code AS corp_code, m.corp_name AS corp_name, m.stock_code AS stock_code, x.acc_mt AS acc_mt, i.induty_code AS ind, COUNT(*) AS n, MIN(f.bsns_year) AS first_year, MAX(f.bsns_year) AS last_year " +
        "FROM financial_raw f LEFT JOIN corp_master m ON m.corp_code = f.corp_code LEFT JOIN corp_fiscal x ON x.corp_code = f.corp_code LEFT JOIN corp_industry i ON i.corp_code = f.corp_code GROUP BY f.corp_code ORDER BY m.corp_name"
      ).all();
      const companies = results.filter((r) => r.corp_name);
      return new Response(JSON.stringify({ companies }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=30" } });
    }

    if (pathname === "/api/financial-history-db") {
      const corpName = searchParams.get("corp_name");
      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name, stock_code FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const { results } = await env.DB
        .prepare("SELECT * FROM financial_raw WHERE corp_code = ? ORDER BY period_order")
        .bind(corpRow.corp_code)
        .all();

      let accMt = null; try { accMt = await getAccMt(env.DB, corpRow.corp_code); } catch (e) { /* 표시용이라 실패해도 진행 */ }
      let indCode = null; try { await ensureIndustryTable(env.DB); const ir = await env.DB.prepare("SELECT induty_code FROM corp_industry WHERE corp_code = ?").bind(corpRow.corp_code).first(); indCode = ir ? ir.induty_code : null; } catch (e) { /* 표시용이라 실패해도 진행 */ }
      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, stock_code: corpRow.stock_code || null, induty_code: indCode, acc_mt: accMt, rows: results });
    }

    if (pathname === "/api/fiscal/refresh") {
      // 저장된 종목 중 결산월을 아직 모르는 회사를 한 번에 최대 15개씩 조회해 저장한다(무료 플랜 하위요청 한도 때문). 같은 호출을 반복해도 결과는 같다.
      await ensureFiscalTable(env.DB);
      const limit = Math.min(Math.max(Number(searchParams.get("limit")) || 10, 1), 15);
      const { results: todo } = await env.DB.prepare(
        "SELECT f.corp_code AS corp_code FROM financial_raw f LEFT JOIN corp_fiscal x ON x.corp_code = f.corp_code WHERE x.corp_code IS NULL GROUP BY f.corp_code LIMIT ?"
      ).bind(limit).all();
      let saved = 0, errors = 0, firstError = null;
      for (const t of todo) {
        const r = await lookupAndSaveAccMt(env.DB, t.corp_code, env.DART_PROXY_URL);
        if (r.accMt != null) saved++; else { errors++; if (!firstError) firstError = r.error || "알 수 없는 오류"; }
      }
      return Response.json({ processed: todo.length, saved, errors, firstError, done: todo.length < limit });
    }
    if (pathname === "/api/fiscal/summary") {
      await ensureFiscalTable(env.DB);
      const tot = await env.DB.prepare("SELECT COUNT(*) AS n FROM corp_fiscal").first();
      const { results: non } = await env.DB.prepare(
        "SELECT x.corp_code AS corp_code, m.corp_name AS corp_name, x.acc_mt AS acc_mt FROM corp_fiscal x LEFT JOIN corp_master m ON m.corp_code = x.corp_code WHERE x.acc_mt != 12 ORDER BY x.acc_mt, m.corp_name"
      ).all();
      return Response.json({ checked: tot ? tot.n : 0, nonDecember: non });
    }

    if (pathname === "/api/backtest/fin-batch") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter(Boolean).slice(0, 8);
      if (!codes.length) return Response.json({ error: "codes 필요" }, { status: 400 });
      const { results } = await env.DB.prepare("SELECT * FROM financial_raw WHERE corp_code IN (" + codes.map(() => "?").join(",") + ") ORDER BY corp_code, period_order").bind(...codes).all();
      return new Response(JSON.stringify({ rows: results }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=60" } });
    }
    if (pathname === "/api/backtest/backfill-next-day") {
      // 이미 저장된 행에 "공시 다음 거래일 종가"를 채운다. 무료 플랜 하위요청 한도를 위해 한 번에 최대 15행만 처리하고,
      // 화면 버튼이 remaining이 0이 될 때까지 반복 호출한다. 같은 호출을 다시 해도 같은 결과(멱등)다.
      await ensureFinancialColumns(env.DB);
      if (searchParams.get("retry_nodata") === "1") {
        await env.DB.prepare("UPDATE financial_raw SET price_next_day_date = NULL WHERE price_next_day_date IN ('none','nodata')").run();
      }
      const limit = Math.min(Math.max(Number(searchParams.get("limit")) || 10, 1), 15);
      const { results: todo } = await env.DB.prepare(
        "SELECT f.corp_code AS corp_code, f.bsns_year AS bsns_year, f.reprt_code AS reprt_code, f.filing_date AS filing_date, m.stock_code AS stock_code " +
        "FROM financial_raw f JOIN corp_master m ON m.corp_code = f.corp_code " +
        "WHERE f.filing_date IS NOT NULL AND f.price_next_day_date IS NULL AND m.stock_code IS NOT NULL ORDER BY f.filing_date LIMIT ?"
      ).bind(limit).all();
      const staleBefore = addDaysStr(todayStr(), -14); // 이 날짜보다 오래된 공시인데도 다음 거래일 가격이 없으면 '없음'으로 표시해 반복 조회를 막는다
      let updated = 0, none = 0, nodata = 0, tooRecent = 0, errors = 0;
      for (const t of todo) {
        try {
          const prices = await fetchNaverPrices(t.stock_code, addDaysStr(t.filing_date, -15), addDaysStr(t.filing_date, 10), env.DART_PROXY_URL);
          const nd = firstCloseAfter(prices, t.filing_date);
          if (nd) {
            await env.DB.prepare("UPDATE financial_raw SET price_next_day = ?, price_next_day_date = ? WHERE corp_code = ? AND bsns_year = ? AND reprt_code = ?")
              .bind(nd.close, nd.date, t.corp_code, t.bsns_year, t.reprt_code).run();
            updated++;
          } else if (t.filing_date < staleBefore) {
            // 시세 응답 자체가 비었으면 'nodata'(프록시 오류일 수 있어 retry_nodata=1로 다시 시도 가능), 시세는 있는데 이후 거래일이 없으면 'none'(거래정지·상장폐지 등)
            const mark = prices.length ? "none" : "nodata";
            await env.DB.prepare("UPDATE financial_raw SET price_next_day = NULL, price_next_day_date = ? WHERE corp_code = ? AND bsns_year = ? AND reprt_code = ?")
              .bind(mark, t.corp_code, t.bsns_year, t.reprt_code).run();
            if (mark === "none") none++; else nodata++;
          } else {
            tooRecent++; // 공시가 최근이라 아직 다음 거래일 시세가 없을 수 있음 — 다음에 다시 시도
          }
        } catch (e) { errors++; }
      }
      const rem = await env.DB.prepare("SELECT COUNT(*) AS n FROM financial_raw WHERE filing_date IS NOT NULL AND price_next_day_date IS NULL").first();
      return Response.json({ processed: todo.length, updated, none, nodata, tooRecent, errors, remaining: rem ? rem.n : null });
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

    if (pathname === "/api/market/config" || pathname === "/api/market/preview" || pathname === "/api/market/range") return handleMarketCfg(request, env, pathname, searchParams);
    if (pathname === "/api/db/usage" || pathname === "/api/db/settings" || pathname === "/api/db/purge") return handleDbCapacity(request, env, pathname, searchParams);

    if (pathname === "/api/market/refresh-universe") {
      // 실제 로직은 scheduled()의 일일 자동 갱신과 공유하는 runUniverseRefresh()에 있다.
      // 이 라우트는 일부러 가볍게 만들었다: KRX 조회 + market_fetch_queue에 등록만 하고, 실제 D1 쓰기(가격
      // 이력 백필/상태 upsert)는 전부 Cron(scheduled())에서 몇 개씩 나눠 처리한다.
      const result = await runUniverseRefresh(env, searchParams.get("force") === "1");
      return Response.json(result);
    }

    // 유니버스 깔때기: 전체 상장 종목 중 어떤 단계에서 몇 종목이 빠지는지 보여 주는 읽기 전용 진단(API 3회 이내, 쓰기 없음)
    if (pathname === "/api/market/funnel") {
      if (!env.DATA_GO_KR_KEY) return Response.json({ error: "DATA_GO_KR_KEY 미설정" }, { status: 500 });
      const lastRow = await env.DB.prepare("SELECT MAX(market_date) AS d FROM market_raw_daily").first();
      const basDt = lastRow && lastRow.d;
      if (!basDt) return Response.json({ error: "저장된 시세가 없습니다." }, { status: 404 });
      const cfgF = await getUniverseCfg(env);
      const [allP, tvP, stored] = await Promise.all([
        fetchKrxPage({ basDt: basDt, numOfRows: 1, pageNo: 1 }, env.DATA_GO_KR_KEY),
        fetchKrxPage({ basDt: basDt, beginTrPrc: cfgF.minTv, numOfRows: 1, pageNo: 1 }, env.DATA_GO_KR_KEY),
        env.DB.prepare("SELECT COUNT(*) AS c FROM market_raw_daily WHERE market_date = ?").bind(basDt).first(),
      ]);
      return Response.json({ bas_dt: basDt, all: allP.totalCount, tv_pass: tvP.totalCount, stored: stored ? stored.c : null, min_tv_eok: cfgF.minTvEok, min_mcap_eok: cfgF.minMcapEok });
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
        min_trading_value: (await getUniverseCfg(env)).minTv,
        min_mcap: (await getUniverseCfg(env)).minMcap,
        append_job: await (async () => { try { const r = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'append'").first(); return r ? JSON.parse(r.v) : null; } catch (e) { return null; } })(),
        daily_log: await (async () => { try { const r = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'daily_log'").first(); return r ? JSON.parse(r.v) : null; } catch (e) { return null; } })(),
      });
    }

    // 데이터 관리 화면의 현황판: 가벼운 집계만(각 쿼리는 PK 인덱스로 처리되는 작은 표).
    // 화면 맨 위 "데이터 기준일" 줄용 아주 가벼운 조회: 기본키 앞 열의 MAX 두 개만 읽는다(행 몇 건).
    if (pathname === "/api/industry/lookup") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter((x) => /^[0-9A-Za-z]{6}$/.test(x)).slice(0, 80);
      return new Response(JSON.stringify({ map: await industryLookup(env, codes) }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=60" } });
    }
    if (pathname === "/api/industry/fill" && request.method === "POST") {
      let body = {}; try { body = await request.json(); } catch (e) { body = {}; }
      const codes = (Array.isArray(body.codes) ? body.codes : []).map((x) => String(x).trim()).filter((x) => /^[0-9A-Za-z]{6}$/.test(x)).slice(0, 3);
      if (!codes.length) return Response.json({ saved: {}, failed: {} });
      if (!env.DART_PROXY_URL) return Response.json({ error: "DART 프록시 주소(DART_PROXY_URL)가 설정되어 있지 않습니다." }, { status: 500 });
      return Response.json(await industryFill(env, codes));
    }
    if (pathname === "/api/fin/auto-status") {
      await ensureFinAutoTable(env.DB);
      const [lg, q] = await Promise.all([
        env.DB.prepare("SELECT v FROM market_job WHERE k = 'fin_auto_log'").first(),
        env.DB.prepare("SELECT COUNT(*) AS n FROM fetch_queue").first(),
      ]);
      let last = null; try { last = lg ? JSON.parse(lg.v) : null; } catch (e) { last = null; }
      return new Response(JSON.stringify({ last, queue: q ? q.n : 0, today: kstTodayStr(), periods: filingPeriodsFor(kstTodayStr()).map((p) => p.label) }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
    }
    if (pathname === "/api/fin/auto-run" && request.method === "POST") {
      const log = await runFilingAutoUpdate(env, { dry: searchParams.get("dry") === "1" });
      return Response.json(log);
    }
    // 재무 스크리닝용: 종목코드 목록의 최신 종가. 시장 DB(market_raw_daily)의 가장 최근 날짜 한 줄씩만 읽는다(기본키 조회라 가볍다).
    if (pathname === "/api/fin/last-price") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter((x) => /^[0-9A-Za-z]{6}$/.test(x)).slice(0, 80);
      if (!codes.length) return Response.json({ date: null, prices: {} });
      const dRow = await env.DB.prepare("SELECT MAX(market_date) AS d FROM market_raw_daily").first();
      const date = dRow ? dRow.d : null;
      const prices = {};
      if (date) {
        const { results } = await env.DB.prepare("SELECT stock_code, close_price FROM market_raw_daily WHERE market_date = ? AND stock_code IN (" + codes.map(() => "?").join(",") + ")").bind(date, ...codes).all();
        results.forEach((r) => { if (r.close_price > 0) prices[r.stock_code] = r.close_price; });
      }
      return new Response(JSON.stringify({ date, prices }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=60" } });
    }
    // 지주회사 NAV 계산기용: 종목코드 목록의 최신 시가총액(원). 시장 DB의 가장 최근 시가총액 기록일 한 줄씩만 읽는다(기본키 조회).
    if (pathname === "/api/market/caps") {
      const codes = (searchParams.get("codes") || "").split(",").map((x) => x.trim()).filter((x) => /^[0-9A-Za-z]{6}$/.test(x)).slice(0, 80);
      if (!codes.length) return Response.json({ date: null, caps: {} });
      const dRow = await env.DB.prepare("SELECT market_date AS d FROM market_raw_daily WHERE market_cap IS NOT NULL ORDER BY market_date DESC LIMIT 1").first();
      const date = dRow ? dRow.d : null;
      const caps = {};
      if (date) {
        const { results } = await env.DB.prepare("SELECT stock_code, close_price, market_cap FROM market_raw_daily WHERE market_date = ? AND stock_code IN (" + codes.map(() => "?").join(",") + ")").bind(date, ...codes).all();
        results.forEach((r) => { if (r.market_cap > 0) caps[r.stock_code] = { cap: r.market_cap, close: r.close_price }; });
      }
      return new Response(JSON.stringify({ date, caps }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, max-age=60" } });
    }
    if (pathname === "/api/data/fresh") {
      await ensureScreenTables(env.DB);
      const one = async (sql) => { try { return await env.DB.prepare(sql).first(); } catch (e) { return null; } };
      const [mk, snap] = await Promise.all([
        one("SELECT MAX(market_date) AS d FROM market_raw_daily"),
        one("SELECT MAX(snap_date) AS d FROM screen_lists"),
      ]);
      return new Response(JSON.stringify({ market_last: mk ? mk.d : null, screen_last: snap ? snap.d : null }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
    }

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

    // 신규 상장 종목 정보(corp_master) 수동 동기화. 매일 자동으로도 하지만 지금 바로 확인하고 싶을 때 쓴다. force=1이면 최근에 못 찾은 코드도 다시 묻는다.
    if (pathname === "/api/corp/sync") {
      const bad = await adminAuthError(request, env); if (bad) return bad;
      try { return Response.json(await syncNewListings(env, { force: searchParams.get("force") === "1" })); }
      catch (e) { return Response.json({ error: String((e && e.message) || e) }, { status: 500 }); }
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
//  - 12-1 모멘텀·52주 신고가 근접도(George & Hwang 2004)는 보관 이력이 253거래일 이상 쌓이면 "참고 지표"로만 표시한다(점수·순위에는 미반영).
//    이력이 모자라면 계산하지 않고 null(화면: 자료 부족). 수정주가가 아니라 하루 ±30% 넘게 움직인 종목(액면분할·병합 등 의심)은 두 값 모두 null 처리.
//  - 재무(밸류·퀄리티)는 Top50에 "함께 표시"해서 직접 판단(Asness·Moskowitz·Pedersen 2013, Novy-Marx 2013).
// 무료 한도 설계: 계산은 SQL 한 번(읽기 약 수십만 행)으로 끝내고 결과(51행)만 저장한다. 평소 조회는 저장분만 읽는다.
// ============================================================
// 주간 자동 스크리닝 시각: 토요일 01:00 한국시간 = 금요일 16:00 UTC (getUTCDay 5 = 금요일)
const WEEKLY_SCREEN_UTC_DOW = 5;
const WEEKLY_SCREEN_UTC_HOUR = 16;
const KOSPI_INDEX_API = "https://apis.data.go.kr/1160100/GetMarketIndexInfoService_V2/getStockMarketIndex_V2";
const KOSPI_NAME = "코스피";
const SCREEN_TOP_N = 100;
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
const SCREEN_MOM_DAYS = 252;      // 12개월 ≈ 252거래일 (12-1 모멘텀·52주 고가 구간)
const SCREEN_JUMP_LIMIT = 0.30;   // 하루 ±30% 초과 변동 = 가격제한폭 한계 → 액면분할·병합·증자 등 가격 불연속 의심
const SCREEN_EXTRA_SQL =
  "WITH w AS (SELECT stock_code AS sc, market_date AS d, close_price AS cp," +
  " LAG(close_price) OVER (PARTITION BY stock_code ORDER BY market_date) AS pc" +
  " FROM market_raw_daily WHERE market_date BETWEEN ?1 AND ?2 AND close_price > 0)" +
  " SELECT sc, MAX(cp) AS hi, COUNT(*) AS n," +
  " MAX(CASE WHEN d = ?1 THEN cp END) AS c252, MAX(CASE WHEN d = ?3 THEN cp END) AS c21," +
  " MAX(CASE WHEN pc > 0 AND (cp / pc > 1 + ?4 OR cp / pc < 1 - ?4) THEN 1 ELSE 0 END) AS jump" +
  " FROM w GROUP BY sc";
// 순수 함수: 후보 한 종목의 12-1 모멘텀·52주 고가 근접도. 자료가 모자라거나 가격 불연속이 의심되면 null + 사유.
function screenExtraMetric(x, c0) {
  if (!x) return { m121: null, hi52: null, extra: "nodata" };
  if (x.jump) return { m121: null, hi52: null, extra: "jump" };
  if (!(x.c252 > 0) || !(x.c21 > 0) || !(x.hi > 0) || !(c0 > 0) || x.n < 200) return { m121: null, hi52: null, extra: "short" };
  const hi = Math.max(x.hi, c0);
  return { m121: x.c21 / x.c252 - 1, hi52: c0 / hi, extra: "ok" };
}
// 거래대금 Z-점수(관심도): 기준일 거래대금이 직전 N거래일(기준일 제외) 평균보다 표준편차의 몇 배 높은가. 단, 거래대금 0인 날과 저장 안 된 날은 관측에서 제외.
// D1 SQLite에서 SQRT 사용 가능 여부가 불확실해, SQL은 순위용 "부호 있는 제곱 z" = 편차×|편차|÷분산 을 내고(z와 순서가 같음) 실제 z는 여기서 복원한다.
const SCREEN_Z10_MIN_OBS = 5;
const SCREEN_Z20_MIN_OBS = 10;
function zFromSqKey(k) { return k == null || !Number.isFinite(k) ? null : (k < 0 ? -1 : 1) * Math.sqrt(Math.abs(k)); }
const SCREEN_LIST_KEYS = ["r20", "r60", "r120", "rw", "m16", "rs1", "rs3", "rs6", "z10", "z20"];
let screenTablesReady = false;

async function ensureScreenTables(db) {
  if (screenTablesReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS market_index_daily (market_date TEXT NOT NULL, idx_name TEXT NOT NULL, close_price REAL, PRIMARY KEY (market_date, idx_name))").run();
  // 주간 스냅샷: 목록 1개 = 1행(JSON). 한 번 저장에 9행(메타 1 + 목록 8)이라 쓰기가 매우 적다.
  await db.prepare("CREATE TABLE IF NOT EXISTS screen_lists (snap_date TEXT NOT NULL, list_key TEXT NOT NULL, payload TEXT, PRIMARY KEY (snap_date, list_key))").run();
  // data.go.kr(종목정보→법인등록번호→기업재무정보)로 받은 영업이익 캐시. 한 번 받으면 120일간 재사용한다.
  await db.prepare("CREATE TABLE IF NOT EXISTS op_income_cache (stock_code TEXT PRIMARY KEY, crno TEXT, biz_year INTEGER, op_profit REAL, fetched_at TEXT)").run();
  // 계산 결과 캐시: 같은 기준일·같은 조건이면 무거운 SQL(수십만~백만 행 읽기)을 다시 돌리지 않고 저장분 1행만 읽는다.
  await db.prepare("CREATE TABLE IF NOT EXISTS screen_cache (cache_key TEXT PRIMARY KEY, payload TEXT, created_at TEXT)").run();
  screenTablesReady = true;
}

// 캐시 읽기/쓰기(실패해도 계산은 계속한다). 한 행이 1.5MB를 넘으면 저장하지 않는다(D1 한 행 한도 2MB 보호).
async function screenCacheGet(env, key) {
  try {
    const r = await env.DB.prepare("SELECT payload FROM screen_cache WHERE cache_key = ?").bind(key).first();
    return r && r.payload ? JSON.parse(r.payload) : null;
  } catch (e) { return null; }
}
async function screenCachePut(env, key, obj, d0) {
  try {
    const txt = JSON.stringify(obj);
    if (txt.length > 1500000) return;
    // 기준일이 바뀌면 이전 날짜 캐시는 쓸모가 없으므로 비운다(표가 계속 커지지 않게).
    await env.DB.prepare("DELETE FROM screen_cache WHERE cache_key NOT LIKE ? OR (cache_key NOT LIKE ? AND cache_key != ?)").bind(d0 + "|%", d0 + "|extra|%", key).run();
    await env.DB.prepare("INSERT OR REPLACE INTO screen_cache (cache_key, payload, created_at) VALUES (?, ?, ?)").bind(key, txt, new Date().toISOString()).run();
  } catch (e) { console.error("스크리닝 캐시 저장 실패:", (e && e.message) || e); }
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
    // 공공데이터포털이 가끔 느려 15초 안에 못 받는 일이 있어, 시간 초과(abort)만 1회 더 시도한다(두 번째는 25초).
    let text;
    for (let attempt = 0; attempt < 2; attempt++) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), attempt === 0 ? 15000 : 25000);
      try {
        const resp = await fetch(url.toString(), { signal: controller.signal });
        text = await resp.text();
        break;
      } catch (e) {
        if (attempt === 1 || !(e && e.name === "AbortError")) {
          throw new Error((e && e.name === "AbortError" ? "공공데이터포털 지수시세 응답 지연(시간 초과)" : ((e && e.message) || String(e))) + " — 저장돼 있던 코스피 지수로 계산했습니다. 최신 코스피 값이 하루 정도 늦을 수 있으니 잠시 뒤 다시 실행해보세요.");
        }
      } finally {
        clearTimeout(timer);
      }
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
  const ucfg = await getUniverseCfg(env);
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
  const binds = [d0, dS, d3, d6, dF, minPrice, dA, ucfg.minTv, pcm, pct];
  if (hasMc) binds.push(ucfg.minMcap);
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
  const ucfg = await getUniverseCfg(env);

  const clamped = [];
  let minMcap = Number(opts.minMcapEok);
  if (!Number.isFinite(minMcap) || minMcap <= 0) minMcap = ucfg.minMcapEok; // 지정이 없으면 저장 기준(데이터 관리에서 설정)을 따른다
  let minTv = Number(opts.minTvEok);
  if (!Number.isFinite(minTv) || minTv <= 0) minTv = ucfg.minTvEok;
  let minPrice = opts.minPrice == null || opts.minPrice === "" ? NaN : Number(opts.minPrice);
  if (!Number.isFinite(minPrice) || minPrice < 0) minPrice = SCREEN_DEFAULT_MIN_PRICE;
  const pctOf = (v, d) => { const x = v == null || v === "" ? NaN : Number(v); return Number.isFinite(x) && x >= 0 && x <= 90 ? x : d; };
  const pcMcap = pctOf(opts.pcMcap, SCREEN_DEFAULT_PCT_MCAP);
  const pcTv = pctOf(opts.pcTv, SCREEN_DEFAULT_PCT_TV);
  const floorMcapEok = ucfg.minMcapEok;
  const floorTvEok = ucfg.minTvEok;
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
    .prepare("SELECT market_date FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT 270")
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

  // 2-1) 캐시 확인: 기준일 + 모든 조건이 같으면 이전 계산 결과를 그대로 쓴다(읽기 1행). 무료 읽기 한도(하루 500만 행) 보호용.
  //      키에는 "저장된 시세가 바뀌었는지"를 알려 주는 지문도 넣는다: 기준 지점 7개 날짜의 저장 행 수·시총 행 수 + 이력 확보 종목 수 + 목록 길이(TOP_N).
  //      (예전 키는 기준일·조건만 봐서, 이력 백필·빠진 날 채우기가 진행돼도 하루 종일 옛 계산(예: 50개짜리)을 그대로 보여줄 수 있었다.)
  const dateList = Object.values(pts);
  let fp = "x";
  try {
    const { results: fr } = await env.DB
      .prepare("SELECT market_date, COUNT(*) AS n, SUM(CASE WHEN market_cap IS NOT NULL THEN 1 ELSE 0 END) AS m FROM market_raw_daily WHERE market_date IN (" + dateList.map(() => "?").join(",") + ") GROUP BY market_date ORDER BY market_date")
      .bind(...dateList).all();
    const bfr = await env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled = 1").first();
    fp = (fr || []).map((r) => r.n + "/" + r.m).join(",") + "#" + (bfr ? bfr.c : 0);
  } catch (e) { fp = "x" + Date.now(); } // 지문을 못 만들면 캐시를 쓰지 않는다(매번 새로 계산)
  const cacheKey = [pts.d0, minMcap, minTv, minPrice, pcMcap, pcTv, nw.w3.toFixed(4), nw.w6.toFixed(4), cal.length >= 270 ? "h" : "s", "top" + SCREEN_TOP_N, fp].join("|");
  if (!opts.noCache) {
    const hit = await screenCacheGet(env, cacheKey);
    if (hit && hit.meta && hit.lists) {
      hit.meta.cached = true;
      hit.meta.clamped = clamped; // 안내 문구는 이번 요청 기준으로 다시 붙인다
      return hit;
    }
  }

  // 3) 코스피 값(같은 날짜 지점)
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
    "), z AS MATERIALIZED (" +
    // 단위 억원. 직전 10/20거래일(기준일 제외) 거래대금의 합·제곱합·관측 수 (모집단 표준편차, ddof=0)
    " SELECT stock_code AS sc," +
    "  COUNT(CASE WHEN market_date >= ?19 THEN 1 END) AS n10, SUM(CASE WHEN market_date >= ?19 THEN trading_value / 1e8 END) AS s10," +
    "  SUM(CASE WHEN market_date >= ?19 THEN (trading_value / 1e8) * (trading_value / 1e8) END) AS q10," +
    "  COUNT(*) AS n20, SUM(trading_value / 1e8) AS s20, SUM((trading_value / 1e8) * (trading_value / 1e8)) AS q20" +
    " FROM market_raw_daily WHERE market_date BETWEEN ?20 AND ?21 AND trading_value > 0 GROUP BY stock_code" +
    "), n AS MATERIALIZED (" +
    " SELECT m.*, a.atv AS atv, a.tv1 AS tv1, u.corp_name AS nm," +
    "  CASE WHEN z.n10 >= " + SCREEN_Z10_MIN_OBS + " AND a.tv1 > 0 AND z.q10 / z.n10 - (z.s10 / z.n10) * (z.s10 / z.n10) > 1e-9" +
    "   THEN (a.tv1 / 1e8 - z.s10 / z.n10) * ABS(a.tv1 / 1e8 - z.s10 / z.n10) / (z.q10 / z.n10 - (z.s10 / z.n10) * (z.s10 / z.n10)) END AS z10," +
    "  CASE WHEN z.n20 >= " + SCREEN_Z20_MIN_OBS + " AND a.tv1 > 0 AND z.q20 / z.n20 - (z.s20 / z.n20) * (z.s20 / z.n20) > 1e-9" +
    "   THEN (a.tv1 / 1e8 - z.s20 / z.n20) * ABS(a.tv1 / 1e8 - z.s20 / z.n20) / (z.q20 / z.n20 - (z.s20 / z.n20) * (z.s20 / z.n20)) END AS z20," +
    "  c0 * 1.0 / NULLIF(c20, 0) - 1 AS r20," +
    "  c0 * 1.0 / NULLIF(c60, 0) - 1 AS r60," +
    "  c0 * 1.0 / NULLIF(c120, 0) - 1 AS r120," +
    "  c0 * 1.0 / NULLIF(c21, 0) - 1 AS r21," +
    "  c21 * 1.0 / NULLIF(c63, 0) - 1 AS q3," +
    "  c21 * 1.0 / NULLIF(c126, 0) - 1 AS m16," +
    "  CASE WHEN c21 > 0 THEN (c0 * 1.0 / c21) / ?14 END AS rs1," +
    "  CASE WHEN c63 > 0 THEN (c0 * 1.0 / c63) / ?12 END AS rs3," +
    "  CASE WHEN c126 > 0 THEN (c0 * 1.0 / c126) / ?13 END AS rs6" +
    " FROM m JOIN a ON a.sc = m.sc LEFT JOIN z ON z.sc = m.sc LEFT JOIN market_universe_status u ON u.stock_code = m.sc" +
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
    .bind(pts.d0, pts.d20, pts.d21, pts.d60, pts.d63, pts.d120, pts.d126, minTv * 1e8, minMcap * 1e8, nw.w3, nw.w6, k0 / k63, k0 / k126, k0 / k21, minPrice, cal[i0 + SCREEN_AVG_DAYS - 1], pcMcap / 100, pcTv / 100, cal[i0 + 10], cal[i0 + 20], cal[i0 + 1]).all();
  const passedN = agg.length ? agg[0].pass_n : 0;
  const cutMcEok = agg.length ? agg[0].cut_mc / 1e8 : null; // 통과 종목 중 최소값 = 실제 컷 금액
  const cutTvEok = agg.length ? agg[0].cut_tv / 1e8 : null;

  // 4-2) 참고 지표(12-1 모멘텀, 52주 고가 근접도): 이력이 253거래일 이상일 때만 계산. 읽기 약 (252일 × 종목 수)행, 후보 목록 확정 후 1회.
  const mom = { needDays: SCREEN_MOM_DAYS + 1, haveDays: cal.length - i0, ready: i0 + SCREEN_MOM_DAYS < cal.length, jumpExcluded: 0 };
  const extraMap = new Map();
  if (mom.ready) {
    const dStart = cal[i0 + SCREEN_MOM_DAYS];
    // 이 쿼리가 가장 무겁다(252일 × 종목 수 ≈ 80만 행). 기준일이 같으면 조건을 바꿔도 결과가 같으므로 따로 캐시한다.
    const exKey = pts.d0 + "|extra|" + dStart;
    let ex = await screenCacheGet(env, exKey);
    if (!Array.isArray(ex)) {
      const got = await env.DB.prepare(SCREEN_EXTRA_SQL).bind(dStart, pts.d0, pts.d21, SCREEN_JUMP_LIMIT).all();
      ex = got.results || [];
      await screenCachePut(env, exKey, ex, pts.d0);
    }
    for (const e of ex) extraMap.set(e.sc, e);
  }
  const extraOf = (r) => {
    if (!mom.ready) return { m121: null, hi52: null, extra: "short" };
    return screenExtraMetric(extraMap.get(r.sc), r.c0);
  };
  const lists = {};
  for (const k of SCREEN_LIST_KEYS) lists[k] = [];
  for (const key of SCREEN_LIST_KEYS) {
    lists[key] = agg
      .filter((r) => r["rk_" + key] != null && r["rk_" + key] <= SCREEN_TOP_N)
      .sort((a, b) => a["rk_" + key] - b["rk_" + key])
      .map((r) => ({
        stock_code: r.sc, corp_name: r.nm || r.sc, c0: r.c0,
        tvEok: r.atv / 1e8, tv1Eok: r.tv1 != null ? r.tv1 / 1e8 : null, mcapEok: r.mc / 1e8,
        r20: r.r20, r60: r.r60, r120: r.r120, r21: r.r21, hot: r.p21 != null && r.p21 >= SCREEN_HOT_PCT ? 1 : 0, rw: r.rw, m16: r.m16, rs1: r.rs1, rs3: r.rs3, rs6: r.rs6, z10: zFromSqKey(r.z10), z20: zFromSqKey(r.z20),
        ...extraOf(r),
      }));
  }
  if (mom.ready) {
    const seen = new Set();
    for (const key of SCREEN_LIST_KEYS) for (const r of lists[key]) if (!seen.has(r.stock_code)) { seen.add(r.stock_code); if (r.extra === "jump") mom.jumpExcluded++; }
  }

  // 5) 참고 집계: 기준일 행 수, 시가총액 미수집 수, 조건 통과 수(기준일 행만 읽음 ≈ 1,300행)
  const meta0 = await env.DB.prepare(
    "SELECT COUNT(*) AS total, SUM(CASE WHEN market_cap IS NULL THEN 1 ELSE 0 END) AS nomc FROM market_raw_daily WHERE market_date = ?1"
  ).bind(pts.d0).first();

  const bfRow = await env.DB.prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled = 1").first();
  const meta = {
    backfilled: bfRow ? bfRow.c : null,
    asof: pts.d0, computed_at: new Date().toISOString(), points: pts, kospi,
    topN: SCREEN_TOP_N, minMcapEok: minMcap, minTvEok: minTv, pcMcap, pcTv, hotPct: SCREEN_HOT_PCT, cutMcEok, cutTvEok, minPrice, weights: nw, clamped, avgDays: SCREEN_AVG_DAYS,
    mom, zMinObs: { z10: SCREEN_Z10_MIN_OBS, z20: SCREEN_Z20_MIN_OBS }, asof_rows: meta0 ? meta0.total : null, no_mcap: meta0 ? meta0.nomc : null, passed_floor: passedN,
  };
  const out = { meta, lists };
  await screenCachePut(env, cacheKey, out, pts.d0);
  return out;
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
  // 저장 시점에 없던 목록(나중에 추가된 기능)은 "결과 없음"이 아니라 "이 저장분에는 없음"으로 구분해 알려준다.
  meta.missingLists = SCREEN_LIST_KEYS.filter((k) => !lists[k]);
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

// ===== 신규 상장 종목 정보 동기화 =====
// corp_master(종목명·종목코드·DART 고유번호)는 처음에 파이썬(종목정보.ipynb)으로 한 번 넣은 것이라 신규 상장 종목이 없다.
// 시세 쪽(유니버스·대기열)에 있는데 corp_master에 없는 종목코드를 찾아, 구글 Apps Script 프록시의 corpcode 기능으로 DART 고유번호를 받아 추가한다.
// DART 키는 프록시에만 있고 Worker에는 없다(corpCode.xml은 압축 30MB 안팎이라 무료 Worker에서 직접 풀 수도 없다). 프록시가 상장사만 골라 JSON으로 준다.
const CORP_SYNC_MAX = 60;          // 한 번에 조회하는 최대 종목 수(주소 길이·응답 크기 제한)
const CORP_SYNC_RETRY_DAYS = 7;    // DART에 없는 코드(ETF·외국기업 등)는 이 기간 동안 다시 묻지 않는다
function corpCodeOk(c) { return typeof c === "string" && /^[0-9A-Z]{6}$/.test(c); }
// 순수 함수: 후보 코드에서 형식이 맞고, 최근에 이미 물어봤지만 못 찾은 것(tried: {코드: YYYYMMDD})을 뺀 뒤 최대 max개
function pickCorpCodes(candidates, tried, todayYmd, retryDays, max) {
  const lim = addDaysStr(todayYmd, -retryDays), out = [], seen = new Set();
  for (const c of candidates) {
    if (!corpCodeOk(c) || seen.has(c)) continue;
    seen.add(c);
    if (tried && tried[c] && tried[c] > lim) continue;
    out.push(c);
    if (out.length >= max) break;
  }
  return out;
}
// 프록시 응답({status:"000", list:[{corp_code, corp_name, stock_code, modify_date}]})에서 쓸 수 있는 행만 골라 낸다.
function parseCorpListed(j, wanted) {
  if (!j || typeof j !== "object" || !Array.isArray(j.list)) return null;
  const want = new Set(wanted), rows = [], seen = new Set();
  for (const it of j.list) {
    const cc = String((it && it.corp_code) || "").trim(), nm = String((it && it.corp_name) || "").trim(), sc = String((it && it.stock_code) || "").trim();
    if (!/^\d{8}$/.test(cc) || !nm || !want.has(sc) || seen.has(sc)) continue;
    seen.add(sc);
    rows.push([cc, nm, sc, String((it && it.modify_date) || "").trim() || null]);
  }
  return rows;
}
async function fetchCorpListed(proxyUrl, codes, timeoutMs = 60000) {
  const url = new URL(proxyUrl);
  url.searchParams.set("endpoint", "corpcode");
  url.searchParams.set("codes", codes.join(","));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const resp = await fetch(url.toString(), { signal: controller.signal });
    return await resp.json();
  } finally { clearTimeout(timer); }
}
async function syncNewListings(env, opts) {
  opts = opts || {};
  const out = { missing: 0, asked: 0, added: 0, names: [], note: null };
  if (!env.DART_PROXY_URL) { out.note = "DART_PROXY_URL 미설정"; return out; }
  await ensureAppSettings(env.DB);
  const { results } = await env.DB.prepare(
    "SELECT stock_code FROM market_universe_status WHERE stock_code NOT IN (SELECT stock_code FROM corp_master WHERE stock_code IS NOT NULL) " +
    "UNION SELECT stock_code FROM market_fetch_queue WHERE stock_code NOT IN (SELECT stock_code FROM corp_master WHERE stock_code IS NOT NULL)"
  ).all();
  const cand = (results || []).map((r) => r.stock_code);
  out.missing = cand.length;
  if (!cand.length) return out;
  let st = {};
  try { const r = await env.DB.prepare("SELECT v FROM app_settings WHERE k = 'corp_sync'").first(); if (r && r.v) st = JSON.parse(r.v) || {}; } catch (e) { st = {}; }
  const tried = st.tried || {}, today = todayStr();
  const codes = pickCorpCodes(cand, opts.force ? null : tried, today, CORP_SYNC_RETRY_DAYS, CORP_SYNC_MAX);
  out.asked = codes.length;
  if (!codes.length) { out.note = "찾을 종목은 있지만 최근 " + CORP_SYNC_RETRY_DAYS + "일 안에 이미 물어봤으나 DART에 없던 코드입니다."; return out; }
  const j = await fetchCorpListed(env.DART_PROXY_URL, codes);
  const rows = parseCorpListed(j, codes);
  if (rows == null) { out.note = "프록시가 corpcode 기능을 지원하지 않거나 형식이 다릅니다(Apps Script 코드 추가 필요): " + String(JSON.stringify(j) || "").slice(0, 120); return out; }
  if (rows.length) {
    await multiRowInsert(env.DB, "INSERT OR IGNORE INTO corp_master (corp_code, corp_name, stock_code, modify_date)", 4, rows, (r) => r);
    out.added = rows.length; out.names = rows.map((r) => r[1]).slice(0, 10);
  }
  const found = new Set(rows.map((r) => r[2]));
  for (const c of codes) if (!found.has(c)) tried[c] = today;
  for (const c of found) delete tried[c];
  const keep = Object.keys(tried).sort((a, b) => (tried[b] > tried[a] ? 1 : -1)).slice(0, 500), t2 = {};
  keep.forEach((c) => { t2[c] = tried[c]; });
  await env.DB.prepare("INSERT OR REPLACE INTO app_settings (k, v, updated_at) VALUES ('corp_sync', ?, ?)").bind(JSON.stringify({ tried: t2, lastRun: new Date().toISOString(), lastAdded: out.added }), new Date().toISOString()).run();
  return out;
}
// ===== /신규 상장 종목 정보 동기화 =====

// ===== 일일 갱신 보완(재시도·빠진 날 채우기) =====
// 예전에는 매일 18:01(한국) 딱 한 번만 시도해서, 그 순간 DB/공공데이터가 실패하면 그날 시세가 영영 비었다.
// 이제 18:01~21:01(한국, 1시간 간격 4번) 재시도하고(이미 끝났으면 바로 건너뜀), 코스피 거래일 달력과 비교해 빠진 날이 있으면 기간 수집으로 채운다.
// 순수 함수: calDesc = 코스피 거래일(최신순), hasRaw = 시세가 저장된 날짜 집합. 저장된 가장 최근 날짜보다 오래됐는데 시세가 없는 날(=중간 구멍)만 돌려준다(오름차순).
function priceGapDates(calDesc, hasRaw) {
  let newest = null;
  for (const d of calDesc) if (hasRaw.has(d)) { newest = d; break; }
  if (!newest) return [];
  return calDesc.filter((d) => d < newest && !hasRaw.has(d)).slice().sort();
}
async function findPriceGaps(env, nDates) {
  const { results } = await env.DB.prepare("SELECT market_date FROM market_index_daily WHERE idx_name = ? ORDER BY market_date DESC LIMIT ?").bind(KOSPI_NAME, nDates || 25).all();
  const cal = (results || []).map((r) => r.market_date);
  if (!cal.length) return [];
  const rs = await env.DB.batch(cal.map((d) => env.DB.prepare("SELECT market_date FROM market_raw_daily WHERE market_date = ? LIMIT 1").bind(d))); // 날짜당 1행만 읽음
  const has = new Set();
  rs.forEach((r) => (r.results || []).forEach((x) => has.add(x.market_date)));
  return priceGapDates(cal, has);
}
async function runDailyCatchUp(env, hourUtc) {
  const msg = (e) => String((e && e.message) || e).slice(0, 160);
  const log = { at: new Date().toISOString(), hourUtc, universe: null, corp: null, kospi: null, gaps: null };
  try {
    // 진행 중인 일괄 추가를 새로 시작해 되돌리지 않는다. 단, 3시간 넘게 남아 있으면 멈춘 것으로 보고 다시 시작한다.
    const runRow = await env.DB.prepare("SELECT v FROM market_job WHERE k = 'append'").first();
    let running = false;
    if (runRow) { try { const t = Date.parse(JSON.parse(runRow.v).startedAt); running = Number.isFinite(t) && Date.now() - t < 3 * 3600 * 1000; } catch (e) { running = false; } }
    if (running) log.universe = "진행 중인 일괄 추가가 있어 새로 시작하지 않음";
    else { const r = await runUniverseRefresh(env, false); log.basDt = r.bas_dt; log.universe = r.job_started ? "시작(" + r.bas_dt + ")" : (r.note || "건너뜀"); }
  } catch (e) { log.universe = "실패: " + msg(e); console.error("일일 자동 유니버스 갱신 실패:", msg(e)); }
  try { const c = await syncNewListings(env); log.corp = c.added ? "신규 " + c.added + "종목 추가(" + c.names.join(", ") + ")" : (c.note || (c.missing ? "추가 0(미확인 " + c.missing + ")" : "신규 없음")); } catch (e) { log.corp = "실패: " + msg(e); console.error("신규 상장 종목 동기화 실패:", msg(e)); }
  try { await refreshKospi(env); log.kospi = "ok"; } catch (e) { log.kospi = "실패: " + msg(e); console.error("코스피 지수 자동 갱신 실패:", msg(e)); }
  try {
    const gaps = await findPriceGaps(env, 25);
    log.gaps = gaps.length ? gaps.join(",") : "없음";
    if (gaps.length) {
      const r = await startRangeJob(env, gaps[0], gaps[gaps.length - 1]);
      log.gapJob = r.error ? ("시작 못 함: " + r.error) : "빠진 날 " + gaps.length + "일 기간 수집 시작";
    }
  } catch (e) { log.gaps = "확인 실패: " + msg(e); console.error("빠진 시세 확인 실패:", msg(e)); }
  try { await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('daily_log', ?)").bind(JSON.stringify(log)).run(); } catch (e) { /* 기록 실패는 무시 */ }
  return log;
}
// ===== /일일 갱신 보완 =====

// ===== 업종 저장 (DART 기업개황 induty_code) =====
let industryReady = false;
async function ensureIndustryTable(db) {
  if (industryReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS corp_industry (corp_code TEXT PRIMARY KEY, stock_code TEXT, induty_code TEXT, checked_at TEXT)").run();
  await db.prepare("CREATE INDEX IF NOT EXISTS idx_corp_industry_stock ON corp_industry (stock_code)").run();
  industryReady = true;
}
function parseIndutyCode(j) {
  if (!j || j.status !== "000") return null;
  const c = String(j.induty_code == null ? "" : j.induty_code).replace(/[^0-9]/g, "");
  return c.length >= 2 ? c : null;
}
// 종목코드(6자리) 목록 → 저장된 업종코드. corp_industry의 stock_code 인덱스만 읽는다.
async function industryLookup(env, codes) {
  await ensureIndustryTable(env.DB);
  const map = {};
  if (!codes.length) return map;
  const { results } = await env.DB.prepare("SELECT stock_code, induty_code FROM corp_industry WHERE stock_code IN (" + codes.map(() => "?").join(",") + ")").bind(...codes).all();
  results.forEach((r) => { if (r.induty_code) map[r.stock_code] = r.induty_code; });
  return map;
}
// 업종이 없는 종목을 DART에서 받아 저장(요청당 최대 3종목: subrequest 한도 보호). 같은 응답의 결산월도 함께 저장한다.
async function industryFill(env, codes) {
  await ensureIndustryTable(env.DB); await ensureFiscalTable(env.DB);
  const saved = {}, failed = {};
  for (const sc of codes.slice(0, 3)) {
    try {
      const m = await env.DB.prepare("SELECT corp_code FROM corp_master WHERE stock_code = ?").bind(sc).first();
      if (!m) { failed[sc] = "corp_master에 없음"; continue; }
      const j = await fetchDartCompany(m.corp_code, env.DART_PROXY_URL);
      const ind = parseIndutyCode(j);
      if (!ind) { failed[sc] = "업종코드 없음: " + String((j && (j.message || j.status)) || "응답 없음").slice(0, 40); continue; }
      const now = new Date().toISOString();
      await env.DB.prepare("INSERT OR REPLACE INTO corp_industry (corp_code, stock_code, induty_code, checked_at) VALUES (?, ?, ?, ?)").bind(m.corp_code, sc, ind, now).run();
      const am = parseAccMt(j);
      if (am != null) await env.DB.prepare("INSERT OR REPLACE INTO corp_fiscal (corp_code, acc_mt, checked_at) VALUES (?, ?, ?)").bind(m.corp_code, am, now).run();
      saved[sc] = ind;
    } catch (e) { failed[sc] = String((e && e.message) || e).slice(0, 60); }
  }
  return { saved, failed };
}
// ===== /업종 저장 =====

// ===== 새 공시 자동 반영 =====
// 재무가 저장된 종목(12월 결산)에 대해, 이미 공시됐을 법한 최근 보고서 중 값이 비어 있는 기간을 fetch_queue에 등록한다.
// 실제 DART 조회·저장은 기존 짝수 분 큐 처리가 그대로 한다. 값이 있는 기간은 건드리지 않는다(덮어쓰기 없음).
// 시도 횟수는 fin_auto_try에 남겨 같은 기간을 3일에 한 번, 최대 10번만 다시 시도한다(공시 전 빈 행이 반복 조회되는 것을 막는다).
const FILING_PERIODS = [ // 12월 결산 기준. end = 보고기간 끝, due = 법정 제출기한(사업보고서는 다음 해 3/31)
  { code: "11013", order: 1, endMD: "0331", dueMD: "0515", dueNext: 0 },
  { code: "11012", order: 2, endMD: "0630", dueMD: "0814", dueNext: 0 },
  { code: "11014", order: 3, endMD: "0930", dueMD: "1114", dueNext: 0 },
  { code: "11011", order: 4, endMD: "1231", dueMD: "0331", dueNext: 1 },
];
const FILING_EARLIEST_DAYS = 30;   // 보고기간 끝난 뒤 이 일수가 지나야 조회 시작(그 전에 공시하는 회사는 드뭄)
const FILING_GIVEUP_DAYS = 60;     // 법정 기한 + 이 일수가 지나면 자동 시도 종료(이후는 수동 「최신 공시 업데이트」)
const FILING_RETRY_GAP_DAYS = 3;
const FILING_MAX_TRIES = 10;
const FILING_DAILY_CAP = 300;
function ymdDiffDays(a, b) { // b - a (일)
  const f = (x) => Date.UTC(Number(x.slice(0, 4)), Number(x.slice(4, 6)) - 1, Number(x.slice(6, 8)));
  return Math.round((f(b) - f(a)) / 86400000);
}
function kstTodayStr() { const d = new Date(Date.now() + 9 * 3600000); return `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`; }
// 오늘 기준으로 자동 시도 대상이 되는 보고 기간 목록
function filingPeriodsFor(today) {
  const Y = Number(today.slice(0, 4)), out = [];
  for (const y of [Y - 1, Y]) {
    for (const f of FILING_PERIODS) {
      const end = `${y}${f.endMD}`, due = `${y + f.dueNext}${f.dueMD}`;
      if (ymdDiffDays(end, today) >= FILING_EARLIEST_DAYS && ymdDiffDays(due, today) <= FILING_GIVEUP_DAYS) {
        out.push({ year: y, code: f.code, order: f.order, period_order: y * 10 + f.order, label: `${y} ${REPRT_CODES.find((r) => r.code === f.code).label}`, due });
      }
    }
  }
  return out;
}
// 순수 함수: 어느 종목의 어느 기간을 큐에 올릴지 고른다.
//  companies [{corp_code, first_year, last_year, acc_mt}], have: Set("corp|year|code") 값이 있는 기간, tries: Map("corp|year|code" → {tries, last_try}), queued: Set
function pickFilingTargets(o) {
  const out = [];
  for (const p of o.periods.slice().sort((a, b) => b.period_order - a.period_order)) {
    for (const c of o.companies) {
      if (c.acc_mt != null && c.acc_mt !== 12) continue;                 // 12월 결산이 아닌 회사는 기한이 달라 자동 대상에서 제외
      if (Number(c.last_year) < p.year - 1) continue;                    // 최근 2년 안에 자료가 없는 종목(상장폐지 등)은 제외
      if (Number(c.first_year) > p.year) continue;                       // 이 종목의 저장 시작 연도보다 이전 기간은 대상 아님
      const key = `${c.corp_code}|${p.year}|${p.code}`;
      if (o.have.has(key) || (o.queued && o.queued.has(key))) continue;
      const t = o.tries.get(key);
      if (t && (t.tries >= FILING_MAX_TRIES || (t.last_try && ymdDiffDays(t.last_try, o.today) < FILING_RETRY_GAP_DAYS))) continue;
      out.push({ corp_code: c.corp_code, corp_name: c.corp_name, stock_code: c.stock_code, year: p.year, code: p.code, period_order: p.period_order, tries: t ? t.tries : 0 });
      if (out.length >= o.cap) return out;
    }
  }
  return out;
}
let finAutoReady = false;
async function ensureFinAutoTable(db) {
  if (finAutoReady) return;
  await db.prepare("CREATE TABLE IF NOT EXISTS fin_auto_try (corp_code TEXT NOT NULL, bsns_year TEXT NOT NULL, reprt_code TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, last_try TEXT, PRIMARY KEY (corp_code, bsns_year, reprt_code))").run();
  await db.prepare("CREATE TABLE IF NOT EXISTS market_job (k TEXT PRIMARY KEY, v TEXT)").run();
  finAutoReady = true;
}
async function runFilingAutoUpdate(env, o) {
  const dry = !!(o && o.dry), today = (o && o.today) || kstTodayStr();
  await ensureFinAutoTable(env.DB); await ensureFiscalTable(env.DB);
  const periods = filingPeriodsFor(today);
  const log = { at: new Date().toISOString(), today, dry, periods: periods.map((p) => p.label), companies: 0, missing: 0, queued: 0, byPeriod: {} };
  if (periods.length) {
    const years = Array.from(new Set(periods.map((p) => String(p.year))));
    const yq = years.map(() => "?").join(",");
    const { results: comps } = await env.DB.prepare(
      "SELECT f.corp_code AS corp_code, m.corp_name AS corp_name, m.stock_code AS stock_code, x.acc_mt AS acc_mt, MIN(f.bsns_year) AS first_year, MAX(f.bsns_year) AS last_year " +
      "FROM financial_raw f JOIN corp_master m ON m.corp_code = f.corp_code LEFT JOIN corp_fiscal x ON x.corp_code = f.corp_code WHERE m.stock_code IS NOT NULL GROUP BY f.corp_code"
    ).all();
    const { results: haveRows } = await env.DB.prepare(
      "SELECT corp_code, bsns_year, reprt_code FROM financial_raw WHERE bsns_year IN (" + yq + ") AND (revenue IS NOT NULL OR net_income IS NOT NULL OR operating_income IS NOT NULL)"
    ).bind(...years).all();
    const { results: tryRows } = await env.DB.prepare("SELECT corp_code, bsns_year, reprt_code, tries, last_try FROM fin_auto_try WHERE bsns_year IN (" + yq + ")").bind(...years).all();
    const { results: qRows } = await env.DB.prepare("SELECT corp_code, bsns_year, reprt_code FROM fetch_queue WHERE bsns_year IN (" + yq + ")").bind(...years).all();
    const have = new Set(haveRows.map((r) => `${r.corp_code}|${r.bsns_year}|${r.reprt_code}`));
    const tries = new Map(tryRows.map((r) => [`${r.corp_code}|${r.bsns_year}|${r.reprt_code}`, { tries: r.tries, last_try: r.last_try }]));
    const queued = new Set(qRows.map((r) => `${r.corp_code}|${r.bsns_year}|${r.reprt_code}`));
    log.companies = comps.length;
    const targets = pickFilingTargets({ companies: comps, periods, have, tries, queued, today, cap: FILING_DAILY_CAP });
    log.missing = targets.length;
    targets.forEach((t) => { const k = `${t.year} ${t.code}`; log.byPeriod[k] = (log.byPeriod[k] || 0) + 1; });
    if (!dry && targets.length) {
      const now = new Date().toISOString();
      const stmts = [];
      for (const t of targets) {
        stmts.push(env.DB.prepare("INSERT OR IGNORE INTO fetch_queue (corp_code, corp_name, stock_code, bsns_year, reprt_code, period_order, queued_at) VALUES (?, ?, ?, ?, ?, ?, ?)").bind(t.corp_code, t.corp_name, t.stock_code, String(t.year), t.code, t.period_order, now));
        stmts.push(env.DB.prepare("INSERT INTO fin_auto_try (corp_code, bsns_year, reprt_code, tries, last_try) VALUES (?, ?, ?, 1, ?) ON CONFLICT(corp_code, bsns_year, reprt_code) DO UPDATE SET tries = tries + 1, last_try = excluded.last_try").bind(t.corp_code, String(t.year), t.code, today));
      }
      for (let i = 0; i < stmts.length; i += 60) await env.DB.batch(stmts.slice(i, i + 60));
      log.queued = targets.length;
    }
  }
  if (!dry) { try { await env.DB.prepare("INSERT OR REPLACE INTO market_job (k, v) VALUES ('fin_auto_log', ?)").bind(JSON.stringify(log)).run(); } catch (e) { /* 기록 실패는 무시 */ } }
  return log;
}
// ===== /새 공시 자동 반영 =====

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

    // 매일 18:01·19:01·20:01·21:01(한국) 최신 거래일 시세 일괄 추가를 "시작"한다(이미 끝났으면 건너뜀). 실제 처리는 이후 홀수 분마다 한 쪽씩.
    // 코스피 지수 갱신과 빠진 날 채우기도 같이 한다. 한 번의 호출에 일을 몰지 않는다(subrequest 50건 한도).
    if (hour >= 9 && hour <= 12 && minute === 1) {
      await runDailyCatchUp(env, hour);
      return;
    }

    // 매일 18:11(한국) 거시경제 지표(환율·금리·WTI) 증분 갱신: 지표별로 마지막 저장일 다음날부터만 받는다. 키가 없거나 실패해도 다른 작업에 영향 없음.
    if (hour === 9 && minute === 11) {
      try {
        const end = todayStr();
        await runMacroRefresh(env, null, end, addDaysStr(end, -9 * 365));
      } catch (e) {
        console.error("거시경제 지표 자동 갱신 실패:", (e && e.message) || e);
      }
      return;
    }

    // 매일 20:31(한국) 재무가 저장된 종목의 새 공시 후보를 fetch_queue에 등록한다(처리는 짝수 분 큐가 함). 실패해도 다른 작업에 영향 없음.
    if (hour === 11 && minute === 31) {
      try { await runFilingAutoUpdate(env, {}); } catch (e) { console.error("새 공시 자동 반영 실패:", (e && e.message) || e); }
      return;
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

    // 사용자가 지정한 기간의 시세 수집: 4분에 한 번(minute % 4 === 3)은 이쪽을 우선하고, 다른 일이 없는 틱에는 아래에서 이어서 처리한다.
    if (minute % 4 === 3) {
      try { if (await processRangeJobTick(env)) return; } catch (e) { console.error("기간 수집 틱 실패:", (e && e.message) || e); }
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
      if (left <= 0) { try { await processRangeJobTick(env); } catch (e) { console.error("기간 수집 틱 실패:", (e && e.message) || e); } return; }
      const r2 = await env.DB
        .prepare("SELECT stock_code, corp_name, is_new FROM market_fetch_queue WHERE is_new = 1 ORDER BY trv DESC LIMIT ?")
        .bind(Math.min(MARKET_NEW_BATCH_SIZE, left))
        .all();
      marketItems = r2.results;
    }
    if (marketItems.length === 0) {
      try { await processRangeJobTick(env); } catch (e) { console.error("기간 수집 틱 실패:", (e && e.message) || e); }
      return;
    }

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
