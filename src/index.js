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
const MARKET_MIN_TRADING_VALUE = 2_000_000_000; // 거래대금 20억원 미만 종목은 유니버스에서 제외(유동성 필터, data.go.kr beginTrPrc로 서버에서 바로 필터링)
const MARKET_BACKFILL_CALENDAR_DAYS = 250;      // 120거래일 이상 확보 목적으로 달력일 기준 여유있게 요청(휴일/주말 포함)
// 한 틱(홀수 분)에 처리하는 종목 수. 신규 백필 1건당: KRX 조회 1~2회 + market_raw_daily 멀티로우 INSERT
// 최대 9개(170행÷20행/문) + 상태 upsert 1개 ≈ 최대 13건. 무료 플랜 Worker 호출당 subrequest(= fetch + D1
// 쿼리 합산) 50건 한도 안에 여유있게 들어오도록 3으로 보수적으로 설정(3×13=39, 고정 조회 2건 포함 41건).
const MARKET_CRON_BATCH_SIZE = 3;
// 하루 최대 "신규 종목 백필" 수. D1은 인덱스가 걸린 컬럼에 쓸 때마다 "테이블 행 + 인덱스 행"으로
// 중복 집계된다(공식 문서: "two rows written: one to the table itself, and one to the index").
// market_raw_daily에 인덱스를 두면 종목당 ~170행이 실질적으로 ~340행으로 집계될 수 있어, 그 인덱스는
// 아예 빼고(schema_market_screening.sql 참고) 170행/종목 기준으로 보수적으로 300종목×170=51,000행으로
// 맞췄다. D1 무료 쓰기 한도(10만행/일)는 "계정의 모든 테이블 합산"이라서, 이 51,000행 외에도 매일의
// 유니버스 자동 갱신(하루 1번, 이미 백필된 종목의 당일치 추가), 재무데이터 큐, 거시경제 지표(macro_data,
// 특히 최초 백필 시 1회성으로 지표당 수천 행) 등이 같은 하루 한도를 나눠 쓴다 — 그래서 실제로 하루
// 쓰기 한도 초과 사고가 한 번 더 발생(신규 종목 백필분 + 거시지표 최초 백필 또는 중복 클릭이 겹침)한
// 뒤, 400에서 300으로 더 낮춰 다른 쓰기들을 위한 여유분을 넉넉히 남겼다.
const MARKET_DAILY_WRITE_CAP = 300;
// Momentum Score 가중치. §21: "역사적 연구가 이 가중치를 직접 입증한 것은 아니므로 하드코딩하지 말 것" → 설정값으로 분리
const MOMENTUM_WEIGHTS = { rs20: 0.20, rs60: 0.35, rs120: 0.45 };

// 분기 누적치 차감이 필요한 흐름(flow) 항목. 그 외는 시점(stock) 항목이라 그대로 둠.
const FLOW_KEYS = ["revenue", "cogs", "operating_income", "net_income", "ocf", "capex", "fcf", "parent_net_income", "pretax_income", "interest_expense"];

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
      <input id="corpName" placeholder="종목명 (예: 삼성전자)" value="삼성전자" style="flex:1; min-width:160px;" />
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
      한 번 갱신해두면(최근 15년치) 아래 "거시경제 상관관계"에서 계속 재사용합니다 — 보통 처음 한 번만 누르면 됩니다.
    </p>
    <div class="row">
      <button class="primary" onclick="refreshMacroData()">거시경제 데이터 갱신</button>
    </div>
    <div id="macroStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px; white-space:pre-line;"></div>
  </div>

  <div class="card">
    <div class="card-title">거시경제 상관관계 (현재 조회된 종목 기준)</div>
    <p style="font-size:13px; color:var(--text-muted); line-height:1.5; margin-top:0;">
      이 종목의 연간 공시 시점(사업보고서)마다, 그 시점의 거시지표 값과 "그 해 실적(매출·순이익 성장률, ROE)",
      "다음 공시까지의 주가 수익률"의 상관계수를 계산합니다. 표본이 보고서 연도 수만큼이라 적을 수 있어 참고용입니다.
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
    <div id="chartTitle" style="font-weight:600; margin-bottom:4px;"></div>
    <canvas id="chartCanvas" style="width:100%; height:220px;"></canvas>
    <p style="font-size:12px; color:var(--text-muted); margin-top:6px;">표의 열 제목을 더블클릭하면 그 항목의 추이가 여기 표시됩니다.</p>
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
        거래대금 20억원 이상 종목만 대상으로 60/120/20거래일 수익률의 순위를 매겨
        모멘텀 스코어 상위 30종목을 보여줍니다. "유니버스 갱신"을 누르면 공공데이터포털 API로 오늘(또는
        가장 최근 영업일) 기준 통과 종목을 바로 받아오고, 그중 처음 통과한 종목만 전체 가격이력을
        백그라운드로 천천히 채웁니다(화면을 닫아도 서버에서 계속 진행됩니다). 매일 한 번씩 눌러주면
        이미 채워진 종목은 하루치만 추가되고, 새로 통과한 종목만 백필 큐에 들어갑니다.
      </p>
      <div class="row">
        <button class="primary" onclick="refreshMarketUniverse()">① 유니버스 갱신 (오늘자 통과 종목 받기)</button>
        <button onclick="checkMarketStatus()">진행 상황 새로고침</button>
      </div>
      <div id="marketStatus" style="font-size:13px; color:var(--text-muted); margin-top:8px; white-space:pre-line;"></div>
    </div>

    <div class="card">
      <div class="card-title">Top 30 모멘텀 종목</div>
      <div class="row" style="font-size:13px; color:var(--text-muted);">
        <label>RS20 가중치 <input id="wRs20" type="number" step="0.05" value="0.20" style="width:70px" /></label>
        <label>RS60 가중치 <input id="wRs60" type="number" step="0.05" value="0.35" style="width:70px" /></label>
        <label>RS120 가중치 <input id="wRs120" type="number" step="0.05" value="0.45" style="width:70px" /></label>
        <span>(합이 1이 아니어도 계산은 되지만, 비교하려면 1에 맞추는 것을 권장)</span>
      </div>
      <div class="row" style="margin-top:8px;">
        <button class="primary" onclick="loadTop30()">② Top 30 계산하기</button>
      </div>
      <div id="top30Status" style="font-size:13px; color:var(--text-muted); margin-top:8px;"></div>
      <div id="top30Wrap" style="display:none; margin-top:10px; overflow-x:auto;"></div>
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

    function renderTable(rows) {
      currentRows = rows;

      // 열 정의: 관련 있는 항목끼리 그룹으로 묶는다. 그룹마다 색을 둬서 2단 헤더(그룹명 + 개별 열이름)로
      // 그리면, 45개 가까운 열이 쭉 나열될 때보다 "이 덩어리는 손익, 이 덩어리는 운전자본" 식으로 한눈에
      // 구역을 구분해 읽을 수 있다 — 넓은 재무데이터 표에서 흔히 쓰는 패턴(블룸버그·BI 툴 등).
      // type이 서식을 결정하고, key가 '_'로 시작하면 저장된 값이 아니라 그 자리에서 계산하는 파생값(매출총이익률 등).
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
        { name: '비고', color: '#64748b', cols: [
          { label: '비고', key: 'error', type: 'text' },
        ] },
      ];
      const COLUMNS = COLUMN_GROUPS.flatMap((g) => g.cols);

      function rawValue(r, col) {
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
        return Number(v).toLocaleString();
      }

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
          { label: '10년 평균 ROE', key: 'avgROE', value: m.avgROE, series: hist && hist.avgROE },
          { label: '10년 평균 ROIC', key: 'avgROIC', value: m.avgROIC, series: hist && hist.avgROIC },
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

        <div class="card" style="margin-top:4px;">
          <div class="card-title">🎯 10년 장기 밸류에이션 시뮬레이션</div>
          <div style="font-size:11.5px; color:var(--text-muted); margin:-2px 0 10px; line-height:1.4;">
            배당으로 빠져나가지 않고 재투자된 이익만 복리로 쌓인다고 가정합니다: <b>BPS × (1+배당조정ROE)^보유기간</b>. 기본값은 10년 평균 실적이고, 아래에서 직접 바꿔 "내 시나리오"로 재계산해볼 수 있습니다.
          </div>
          <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(120px, 1fr)); gap:8px; margin-bottom:12px;">
            \${statTile('10년 평균 배당성향', pctStr(m.avgPayout) + \` (\${m.payoutN}개 연도)\`)}
            \${statTile('10년 평균 ROE', pctStr(m.avgROE))}
            \${statTile('배당조정 ROE', pctStr(m.avgAdjustedROE))}
            \${statTile(\`최근 BPS (\${m.latestLabel})\`, wonStr(m.bps))}
            \${statTile(\`EPS (\${m.epsBasis})\`, m.eps != null ? Math.round(m.eps).toLocaleString() + '원' : 'N/A')}
            \${m.marketCap != null ? statTile('참고 시가총액', wonStr(m.marketCap)) : ''}
          </div>
          <div style="display:flex; flex-wrap:wrap; gap:14px; align-items:end; padding:10px 12px; background:#f8fafc; border:1px solid var(--border); border-radius:8px; margin-bottom:12px;">
            <label style="font-size:12px; color:var(--text-muted);">가정 ROE(%)<br/><input id="simRoe" type="number" step="0.1" value="\${m.avgROE != null ? (m.avgROE * 100).toFixed(1) : 0}" oninput="runValuationSim()" style="width:80px; margin-top:3px;"></label>
            <label style="font-size:12px; color:var(--text-muted);">배당성향(%)<br/><input id="simPayout" type="number" step="1" value="\${m.avgPayout != null ? (m.avgPayout * 100).toFixed(0) : 0}" oninput="runValuationSim()" style="width:70px; margin-top:3px;"></label>
            <label style="font-size:12px; color:var(--text-muted);">보유기간(년)<br/><input id="simYears" type="number" step="1" min="1" value="10" oninput="runValuationSim()" style="width:60px; margin-top:3px;"></label>
            <button onclick="resetValuationSim()" style="height:30px;">10년 평균값으로 초기화</button>
            \${m.priceInput == null ? '<span style="font-size:11.5px; color:var(--danger);">현재 주가를 입력하면 저평가/고평가 비교도 함께 나옵니다.</span>' : ''}
          </div>
          <div id="simResult" style="display:grid; grid-template-columns:repeat(auto-fit, minmax(150px, 1fr)); gap:8px;"></div>
        </div>
      \`;
      runValuationSim();
    }

    // 시뮬레이션 입력값(가정 ROE·배당성향·보유기간)으로 예상 주가·상승배수·CAGR·저평가 판정을 다시 계산해
    // #simResult만 갱신한다 — 카드 전체를 다시 그리지 않아 입력하는 동안 화면이 깜빡이지 않는다.
    function runValuationSim() {
      const m = lastSummaryMetrics;
      const resultEl = document.getElementById('simResult');
      if (!m || !resultEl) return;
      const roe = Number(document.getElementById('simRoe').value) / 100;
      const payout = Number(document.getElementById('simPayout').value) / 100;
      const years = Number(document.getElementById('simYears').value) || 10;
      const adjustedRoe = roe * (1 - payout);
      const projected = (m.bps != null && isFinite(adjustedRoe)) ? m.bps * Math.pow(1 + adjustedRoe, years) : null;
      const expectedMultiple = (projected != null && m.priceInput) ? projected / m.priceInput : null;
      const cagr = (expectedMultiple != null && expectedMultiple > 0) ? Math.pow(expectedMultiple, 1 / years) - 1 : null;
      const wonStr = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';
      const pctStr = (v) => v != null ? (v * 100).toFixed(2) + '%' : 'N/A';

      let judgeTile;
      if (projected != null && m.priceInput) {
        const under = projected > m.priceInput;
        judgeTile = \`<div class="stat-tile" style="background:\${under ? '#16a34a1a' : '#dc26261a'};"><div class="stat-label">비교 결과 (현재가 \${m.priceInput.toLocaleString()}원 기준)</div><div class="stat-value" style="color:\${under ? '#16a34a' : '#dc2626'};">\${under ? '저평가 가능성' : '고평가 가능성'}</div></div>\`;
      } else {
        judgeTile = \`<div class="stat-tile"><div class="stat-label">비교 결과</div><div class="stat-value" style="color:var(--text-muted); font-size:13px;">현재가 입력 필요</div></div>\`;
      }

      resultEl.innerHTML = \`
        <div class="stat-tile"><div class="stat-label">\${years}년 후 예상 주가</div><div class="stat-value">\${wonStr(projected)}</div></div>
        <div class="stat-tile"><div class="stat-label">예상 상승배수</div><div class="stat-value">\${expectedMultiple != null ? expectedMultiple.toFixed(2) + '배' : 'N/A'}</div></div>
        <div class="stat-tile"><div class="stat-label">연환산 기대수익률(CAGR)</div><div class="stat-value">\${pctStr(cagr)}</div></div>
        \${judgeTile}
      \`;
    }

    // 가정 입력값을 10년 평균 실적(기본 시나리오)으로 되돌린다.
    function resetValuationSim() {
      const m = lastSummaryMetrics;
      if (!m) return;
      document.getElementById('simRoe').value = m.avgROE != null ? (m.avgROE * 100).toFixed(1) : 0;
      document.getElementById('simPayout').value = m.avgPayout != null ? (m.avgPayout * 100).toFixed(0) : 0;
      document.getElementById('simYears').value = 10;
      runValuationSim();
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

    async function fetchAndSave() {      const corpName = document.getElementById('corpName').value.trim();
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
      if (tab === 'momentum') checkMarketStatus();
    }

    async function refreshMarketUniverse() {
      const el = document.getElementById('marketStatus');
      el.textContent = '공공데이터포털에서 오늘자 통과 종목 받는 중...';
      try {
        const res = await fetch('/api/market/refresh-universe');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '갱신 실패');
        el.textContent = \`\${data.bas_dt} 기준 \${data.universe_size}개 종목이 유동성 기준을 통과해 백그라운드 큐에 등록됐습니다. 처음 통과한 종목은 전체 가격이력을, 이미 받은 종목은 오늘 하루치만 자동으로 채웁니다(화면을 닫아도 계속 진행됩니다).\`;
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
        el.textContent = \`유동성 통과: \${data.passed_total}개 (확인한 종목 총 \${data.checked_total}개)  |  백필 완료: \${data.backfilled_total}개  |  백필 대기: \${data.queue_remaining}개  |  오늘 백필: \${data.backfilled_today}/\${data.daily_cap}\`;
      } catch (e) {
        el.textContent = '오류: ' + e.message;
      }
    }

    async function loadTop30() {
      const statusEl = document.getElementById('top30Status');
      const wrap = document.getElementById('top30Wrap');
      const w20 = document.getElementById('wRs20').value;
      const w60 = document.getElementById('wRs60').value;
      const w120 = document.getElementById('wRs120').value;
      statusEl.textContent = '계산 중...';
      wrap.style.display = 'none';
      try {
        const res = await fetch(\`/api/market/top30?w20=\${w20}&w60=\${w60}&w120=\${w120}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '계산 실패');
        statusEl.textContent = \`랭킹 계산 대상(120거래일 데이터 모두 확보된 종목): \${data.universe_size}개 중 상위 30개\`;
        const pct = (v) => (v * 100).toFixed(1) + '%';
        const rows = data.top30.map((r, i) => \`<tr>
          <td>\${i + 1}</td><td>\${r.corp_name}</td><td>\${r.stock_code}</td>
          <td>\${pct(r.rs20)}</td><td>\${pct(r.rs60)}</td><td>\${pct(r.rs120)}</td>
          <td>\${r.momentumScore.toFixed(3)}</td>
        </tr>\`).join('');
        wrap.innerHTML = \`<table><thead><tr>
          <th>순위</th><th>종목명</th><th>종목코드</th><th>20일</th><th>60일</th><th>120일</th><th>모멘텀 스코어</th>
        </tr></thead><tbody>\${rows}</tbody></table>\`;
        wrap.style.display = '';
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
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

    function showChart(key, label) {
      const col = (window.__columnDefs || []).find((c) => c.key === key) || { key };
      const getVal = window.__rawValue || ((r, c) => r[c.key]);
      const points = currentRows
        .filter((r) => !r.error)
        .map((r) => ({ x: r.period_label, y: getVal(r, col) }))
        .filter((p) => p.y != null);
      if (points.length === 0) { alert('표시할 데이터가 없습니다 (모두 N/A).'); return; }
      document.getElementById('chartTitle').textContent = label + ' 추이 (' + points.length + '개 기간)';
      document.getElementById('chartWrap').style.display = 'block';
      drawChart(points);
      document.getElementById('chartWrap').scrollIntoView({ behavior: 'smooth' });
    }

    function drawChart(points) {
      const canvas = document.getElementById('chartCanvas');
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const W = canvas.clientWidth, H = canvas.clientHeight;
      canvas.width = W * dpr;
      canvas.height = H * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);

      const padL = 70, padR = 10, padT = 10, padB = 40;
      const plotW = W - padL - padR, plotH = H - padT - padB;
      const values = points.map((p) => p.y);
      let min = Math.min(...values), max = Math.max(...values);
      if (min === max) { min -= 1; max += 1; }
      const range = max - min;
      const xStep = points.length > 1 ? plotW / (points.length - 1) : 0;
      const yFor = (v) => padT + plotH - ((v - min) / range) * plotH;
      const xFor = (i) => padL + i * xStep;

      if (min < 0 && max > 0) {
        ctx.strokeStyle = '#999';
        ctx.beginPath();
        ctx.moveTo(padL, yFor(0));
        ctx.lineTo(padL + plotW, yFor(0));
        ctx.stroke();
      }
      ctx.fillStyle = '#333';
      ctx.font = '11px sans-serif';
      ctx.fillText(Math.round(max).toLocaleString(), 2, yFor(max) + 4);
      ctx.fillText(Math.round(min).toLocaleString(), 2, yFor(min) + 4);

      ctx.strokeStyle = '#2563eb';
      ctx.lineWidth = 2;
      ctx.beginPath();
      points.forEach((p, i) => {
        const x = xFor(i), y = yFor(p.y);
        if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
      });
      ctx.stroke();

      ctx.fillStyle = '#2563eb';
      points.forEach((p, i) => {
        ctx.beginPath();
        ctx.arc(xFor(i), yFor(p.y), 2.5, 0, Math.PI * 2);
        ctx.fill();
      });

      ctx.fillStyle = '#333';
      ctx.font = '10px sans-serif';
      const maxLabels = 8;
      const step = Math.max(1, Math.ceil(points.length / maxLabels));
      points.forEach((p, i) => {
        if (i % step === 0 || i === points.length - 1) {
          ctx.save();
          ctx.translate(xFor(i), H - padB + 14);
          ctx.rotate(-Math.PI / 4);
          ctx.fillText(p.x, 0, 0);
          ctx.restore();
        }
      });
    }

    // --- 거시경제 지표(환율·금리·WTI) ---
    let macroSeriesCache = null; // { usdkrw: [{date,value}, ...], ... } — /api/macro/series 결과를 한 번만 받아 재사용

    async function refreshMacroData() {
      const statusEl = document.getElementById('macroStatus');
      statusEl.textContent = '거시경제 데이터를 받아오는 중입니다(최근 15년치, 지표 5개)...';
      try {
        const res = await fetch('/api/macro/refresh');
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '갱신 실패');
        macroSeriesCache = null; // 캐시 무효화 — 다음 조회 때 새로 받아오게
        const lines = Object.entries(data.summary).map(([key, s]) =>
          s.error ? \`\${key}: 실패 — \${s.error}\` : \`\${s.label}: \${s.points}개 저장\`);
        statusEl.textContent = \`\${data.start} ~ \${data.end}\\n\` + lines.join('\\n');
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

    // targetDate(YYYYMMDD) 이전(포함) 중 가장 최근 값을 찾는다 — 미래 데이터를 끌어다 쓰지 않도록(point-in-time)
    function macroValueAsOf(series, targetDate) {
      if (!series || series.length === 0) return null;
      let best = null;
      for (const p of series) {
        if (p.date <= targetDate && (!best || p.date > best.date)) best = p;
      }
      return best ? best.value : null;
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

      // 연간(사업보고서) 데이터만 사용 — filing_date가 있는 연도만 대상
      const annual = toAnnualRows(rawRows).filter((r) => r.filing_date && r.price_at_filing != null);
      if (annual.length < 3) {
        statusEl.textContent = \`사업보고서 공시일·주가 데이터가 있는 연도가 \${annual.length}개뿐이라 상관관계를 계산하기엔 부족합니다(최소 3개 필요). "DART에서 조회 + 저장"으로 더 많은 연도를 받아주세요.\`;
        return;
      }

      // 실적 지표: 매출/순이익 YoY 성장률, ROE. 주가 수익률: 이번 공시 → 다음 공시까지의 수익률(마지막 연도는 "다음"이 없어 제외)
      const points = [];
      for (let i = 0; i < annual.length; i++) {
        const r = annual[i];
        const prev = i > 0 ? annual[i - 1] : null;
        const next = i < annual.length - 1 ? annual[i + 1] : null;
        let revenueGrowth = null, netIncomeGrowth = null, roe = null, forwardReturn = null;
        if (prev && r.revenue != null && prev.revenue) revenueGrowth = (r.revenue - prev.revenue) / Math.abs(prev.revenue);
        if (prev) {
          const currNi = r.parent_net_income != null ? r.parent_net_income : r.net_income;
          const prevNi = prev.parent_net_income != null ? prev.parent_net_income : prev.net_income;
          if (currNi != null && prevNi) netIncomeGrowth = (currNi - prevNi) / Math.abs(prevNi);
        }
        if (prev) {
          const roeResult = computeROEAvg(r, prev);
          roe = roeResult ? roeResult.value : null;
        }
        if (next && next.price_at_filing != null && r.price_at_filing) forwardReturn = next.price_at_filing / r.price_at_filing - 1;

        const macroValues = {};
        for (const key of Object.keys(MACRO_LABELS)) macroValues[key] = macroValueAsOf(macro[key], r.filing_date);

        points.push({ label: r.period_label, filingDate: r.filing_date, revenueGrowth, netIncomeGrowth, roe, forwardReturn, macroValues });
      }

      const targets = [
        { key: 'revenueGrowth', label: '매출액 성장률(YoY)' },
        { key: 'netIncomeGrowth', label: '순이익 성장률(YoY)' },
        { key: 'roe', label: 'ROE' },
        { key: 'forwardReturn', label: '다음 공시까지 주가수익률' },
      ];

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
      html += '</table>';

      statusEl.textContent = \`연도 \${annual.length}개 기준 (상관계수는 -1~1, |0.5| 이상만 색으로 강조 — 표본이 적어 참고용입니다)\`;
      wrapEl.style.display = 'block';
      wrapEl.innerHTML = html;
    }

    const MACRO_LABELS = { usdkrw: '원/달러 환율', msb1y: '통안증권(1년)', ktb3y: '국고채(3년)', ktb10y: '국고채(10년)', wti: 'WTI 현물가' };
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
  total_equity: ["BS"], total_liabilities: ["BS"], cash: ["BS"], st_financial_assets: ["BS"],
  receivables: ["BS"], inventory: ["BS"], payables: ["BS"],
  short_term_trading_securities: ["BS"], fvpl_financial_assets: ["BS"], fvoci_financial_assets: ["BS"], investment_property: ["BS"],
  other_receivables: ["BS"], short_term_loans: ["BS"], other_payables: ["BS"],
  short_term_borrowings: ["BS"], current_portion_lt_debt: ["BS"], current_lease_liabilities: ["BS"],
  tangible_assets: ["BS"], intangible_assets: ["BS"], right_of_use_assets: ["BS"], parent_equity: ["BS"],
};

function sumAccount(list, ids, names, sjOrder) {
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
    if (points.length > 0) {
      await multiRowInsert(
        env.DB,
        "INSERT OR REPLACE INTO macro_data (series, date, value)",
        3,
        points,
        (p) => [seriesKey, p.date, p.value]
      );
    }
    summary[seriesKey] = { label: def.label, points: points.length, from: effectiveStart };
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
    .map((it) => ({ date: it.basDt, close: it.clpr, volume: it.trqu, tradingValue: it.trPrc }))
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

// "오늘(또는 가장 최근 영업일) 기준 거래대금 조건을 통과한 전체 종목"을 받아서 market_fetch_queue에 등록한다.
// /api/market/refresh-universe 라우트와 scheduled()의 일일 자동 갱신이 이 함수 하나를 공유한다 — 사람이
// 버튼을 누르는 걸 잊어도 매일 자동으로 돌아가야 "스크리닝 대상에서 종목이 누락되는" 일이 없다.
async function runUniverseRefresh(env) {
  if (!env.DATA_GO_KR_KEY) {
    throw new Error("DATA_GO_KR_KEY 환경변수(시크릿)가 설정되지 않았습니다. Cloudflare Workers 설정에서 추가해주세요.");
  }
  let basDt = null;
  let universe = [];
  const today = new Date();
  for (let back = 0; back <= 7; back++) {
    const d = new Date(today);
    d.setUTCDate(d.getUTCDate() - back);
    const ds = `${d.getUTCFullYear()}${String(d.getUTCMonth() + 1).padStart(2, "0")}${String(d.getUTCDate()).padStart(2, "0")}`;
    const result = await fetchKrxDailyUniverse(ds, MARKET_MIN_TRADING_VALUE, env.DATA_GO_KR_KEY);
    if (result.length > 0) { basDt = ds; universe = result; break; }
  }
  if (!basDt) {
    throw new Error("최근 7일 내 거래일 데이터를 찾지 못했습니다(주말/공휴일이 겹쳤을 수 있습니다).");
  }
  const now = new Date().toISOString();
  await multiRowInsert(
    env.DB,
    "INSERT OR IGNORE INTO market_fetch_queue (stock_code, corp_name, queued_at)",
    3,
    universe,
    (it) => [it.srtnCd, it.itmsNm, now]
  );
  return { bas_dt: basDt, universe_size: universe.length, enqueued: universe.length };
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
      vals[item.key] = sumAccount(dart.list, item.ids, item.names, SJ_BY_KEY[item.key] || ["BS", "IS", "CIS", "CF"]);
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

async function saveRowsToDb(db, rows) {
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
      const result = await runUniverseRefresh(env);
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
      });
    }

    if (pathname === "/api/market/top30") {
      const { results: passedStocks } = await env.DB
        .prepare("SELECT stock_code, corp_name FROM market_universe_status WHERE passed = 1")
        .all();
      if (passedStocks.length === 0) {
        return Response.json({ error: "아직 유동성 기준을 통과한 종목이 없습니다. 먼저 '유니버스 갱신'을 진행해주세요." }, { status: 400 });
      }

      const { results: rawRows } = await env.DB
        .prepare("SELECT stock_code, market_date, close_price FROM market_raw_daily ORDER BY stock_code, market_date")
        .all();

      const map = new Map();
      for (const r of rawRows) {
        if (!map.has(r.stock_code)) map.set(r.stock_code, []);
        map.get(r.stock_code).push(r);
      }

      const weights = {
        rs20: Number(searchParams.get("w20")) || MOMENTUM_WEIGHTS.rs20,
        rs60: Number(searchParams.get("w60")) || MOMENTUM_WEIGHTS.rs60,
        rs120: Number(searchParams.get("w120")) || MOMENTUM_WEIGHTS.rs120,
      };

      const nameByCode = new Map(passedStocks.map((s) => [s.stock_code, s.corp_name]));
      const table = computeMomentumTable(map, weights);
      const top30 = table.slice(0, 30).map((r) => ({ ...r, corp_name: nameByCode.get(r.stock_code) || r.stock_code }));

      return Response.json({ universe_size: table.length, weights, top30 });
    }

    if (pathname === "/api/macro/refresh") {
      // start를 직접 지정하지 않으면(평소 "갱신" 버튼) 지표별로 "마지막 저장일 다음날"부터만 증분 수집한다.
      // 한 번도 받은 적 없는 지표만 최근 15년(분기말·공시일 상관관계 분석에 충분)부터 최초 백필한다.
      // start를 직접 지정하면(수동 재백필용) 그 날짜부터 전체를 다시 받는다 — D1 쓰기 한도를 많이
      // 쓰므로 꼭 필요할 때만 사용할 것.
      const end = searchParams.get("end") || todayStr();
      const explicitStart = searchParams.get("start") || null;
      const defaultLookbackStart = addDaysStr(end, -15 * 365);
      const summary = await runMacroRefresh(env, explicitStart, end, defaultLookbackStart);
      return Response.json({ start: explicitStart || "(지표별 증분)", end, summary });
    }

    if (pathname === "/api/macro/series") {
      const { results } = await env.DB.prepare("SELECT series, date, value FROM macro_data ORDER BY series, date").all();
      return Response.json({ rows: results });
    }

    return new Response("Not Found", { status: 404 });
}

// Cron Trigger: 1분마다 깨어난다. 재무데이터 큐와 모멘텀 유니버스 큐가 subrequest 예산(요청당 50건)을
// 같이 나눠 쓰면 한도를 넘길 수 있어, 짝수 분/홀수 분으로 번갈아 처리해 완전히 분리한다.
async function handleScheduled(event, env, ctx) {
    const minute = new Date(event.scheduledTime).getUTCMinutes();
    const hour = new Date(event.scheduledTime).getUTCHours();

    // 하루 한 번(UTC 9시 1분 = 한국시간 18:01, 장 마감 이후) 유니버스를 자동으로 갱신한다.
    // 전에는 사람이 "유니버스 갱신" 버튼을 직접 눌러야만 새 종목이 추가됐는데, 깜빡 잊으면 그날의
    // 신규 통과 종목을 스크리닝에서 놓치게 된다 — 그 문제를 없애기 위한 자동화.
    if (hour === 9 && minute === 1) {
      try {
        await runUniverseRefresh(env);
      } catch (e) {
        console.error("일일 자동 유니버스 갱신 실패:", e.message || e);
      }
    }

    if (minute % 2 === 0) {
      // 짝수 분: 재무데이터(fetch_queue) 처리
      const { results } = await env.DB
        .prepare("SELECT corp_code, corp_name, stock_code, bsns_year, reprt_code FROM fetch_queue ORDER BY corp_code, period_order LIMIT ?")
        .bind(CRON_BATCH_SIZE)
        .all();

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
    // - 처음 통과한 종목(backfilled=0 또는 기록 없음): 전체 가격이력(약 250일치)을 받아온다.
    // - 이미 백필된 종목: 오늘 하루치만 가볍게 추가한다(매일 "유니버스 갱신"이 전체 종목을 다시 큐에 넣어주므로).
    // D1 무료 쓰기 한도(10만행/일)를 지키기 위해 "오늘 신규로 백필 완료한 종목 수"만 MARKET_DAILY_WRITE_CAP으로
    // 제한한다(이미 백필된 종목의 하루치 추가는 양이 적어 따로 제한하지 않는다).
    if (!env.DATA_GO_KR_KEY) return; // 키 미설정 시 조용히 건너뜀(재무데이터 큐는 짝수 분에 계속 처리됨)

    const todayRow = await env.DB
      .prepare("SELECT COUNT(*) AS c FROM market_universe_status WHERE backfilled_at >= ?")
      .bind(todayUtcMidnightIso())
      .first();
    const remainingCap = MARKET_DAILY_WRITE_CAP - (todayRow?.c ?? 0);

    // 이미 백필된(backfilled=1) 종목을 먼저 처리하도록 정렬한다 — 그래야 하루 백필 한도(remainingCap)를
    // 다 써서 신규 종목이 큐 앞쪽에 계속 남아있어도, 그 뒤에 있는 "하루치만 추가하면 되는" 종목들이
    // 영영 뒤로 밀리지 않는다(배치 선택 쿼리라 매 틱 같은 앞부분만 반복해서 뽑히는 걸 방지).
    const { results: marketItems } = await env.DB
      .prepare(
        `SELECT q.stock_code, q.corp_name, COALESCE(u.backfilled, 0) AS backfilled
         FROM market_fetch_queue q LEFT JOIN market_universe_status u ON u.stock_code = q.stock_code
         ORDER BY backfilled DESC
         LIMIT ?`
      )
      .bind(MARKET_CRON_BATCH_SIZE)
      .all();

    const end = todayStr();
    const begin = addDaysStr(end, -MARKET_BACKFILL_CALENDAR_DAYS);
    const nowIso = new Date().toISOString();

    for (const item of marketItems) {
      let quotaExceeded = false;
      try {
        const alreadyBackfilled = item.backfilled === 1;

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
              "INSERT OR REPLACE INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value) VALUES (?, ?, ?, ?, ?)"
            ).bind(dayRow.basDt, item.stock_code, dayRow.clpr, dayRow.trqu, dayRow.trPrc).run();
            await env.DB.prepare(
              "UPDATE market_universe_status SET passed = 1, avg_trading_value = ?, checked_at = ? WHERE stock_code = ?"
            ).bind(dayRow.trPrc, nowIso, item.stock_code).run();
          }
        } else if (remainingCap > 0) {
          // 신규 통과 종목: 전체 이력을 백필한다(하루 백필 한도 안에서만).
          const history = await fetchKrxStockHistory(item.stock_code, begin, end, env.DATA_GO_KR_KEY);
          if (history.length) {
            await multiRowInsert(
              env.DB,
              "INSERT OR REPLACE INTO market_raw_daily (market_date, stock_code, close_price, volume, trading_value)",
              5,
              history,
              (p) => [p.date, item.stock_code, p.close, p.volume, p.tradingValue]
            );
          }
          const latest = history.length ? history[history.length - 1] : null;
          await env.DB.prepare(
            `INSERT INTO market_universe_status (stock_code, corp_name, passed, avg_trading_value, days_fetched, checked_at, backfilled, backfilled_at)
             VALUES (?, ?, 1, ?, NULL, ?, 1, ?)
             ON CONFLICT(stock_code) DO UPDATE SET corp_name = excluded.corp_name, passed = 1, avg_trading_value = excluded.avg_trading_value, checked_at = excluded.checked_at, backfilled = 1, backfilled_at = excluded.backfilled_at`
          ).bind(item.stock_code, item.corp_name, latest ? latest.tradingValue : null, nowIso, nowIso).run();
        } else {
          // 오늘 백필 한도를 다 썼다 — 이 종목은 큐에서 빼지 않고 다음 날(또는 한도가 남는 다음 틱)로 미룬다.
          continue;
        }
      } catch (e) {
        const msg = (e && e.message) || String(e);
        console.error("market 처리 실패:", item.stock_code, msg);
        // D1 하루 쓰기 한도 초과는 "이 종목만의 문제"가 아니라 "오늘은 더 이상 아무 것도 못 쓴다"는
        // 뜻이다. 이걸 구분 안 하면: 바로 아래 DELETE도 똑같이 실패하고(쓰기이므로), 그 실패가
        // try/catch 밖에서 그대로 터져 이 함수 전체가 중단되면서 방금 실패한 종목이 큐에서 안 지워진
        // 채로 남는다 → 다음 틱(1분 뒤)에 조회 쿼리가 다시 같은 종목을 1순위로 뽑아 똑같이 실패를
        // 반복(한 종목에 몇 시간씩 멈춰있는 것처럼 보이는 원인). 그래서 한도 초과를 감지하면 이번
        // 종목의 DELETE 자체를 건너뛰고(한도가 풀리는 내일 다시 정상 처리되도록 큐에 남겨둠), 이번
        // 틱의 나머지 종목들도 바로 포기해서(break) data.go.kr 호출과 에러 로그를 더 낭비하지 않는다.
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
