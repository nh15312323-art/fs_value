// ============================================================
// Phase 5: ROIC/밸류에이션 계산 + 분기·연간 토글 + 헤더 고정
// ============================================================

const REPRT_CODES = [
  { code: "11013", label: "1분기", order: 1 },
  { code: "11012", label: "반기", order: 2 },
  { code: "11014", label: "3분기", order: 3 },
  { code: "11011", label: "사업보고서", order: 4 },
];

// 분기 누적치 차감이 필요한 흐름(flow) 항목. 그 외는 시점(stock) 항목이라 그대로 둠.
const FLOW_KEYS = ["revenue", "cogs", "operating_income", "net_income", "ocf", "capex", "fcf", "parent_net_income", "pretax_income", "interest_expense"];

const ACCOUNT_ITEMS = [
  { key: "revenue", ids: ["ifrs-full_Revenue", "ifrs_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"], names: ["매출액", "수익(매출액)"] },
  { key: "cogs", ids: ["ifrs-full_CostOfSales", "ifrs_CostOfSales"], names: ["매출원가"] },
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
  "filing_date", "price_at_filing", "per_at_filing", "pbr_at_filing", "fcf_yield_at_filing",
  "roa_at_filing", "peg_at_filing", "shares_source",
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
    #status { color: var(--text-muted); margin: 8px 0; white-space: pre-line; font-size: 14px; }
    #status.error { color: var(--danger); }
    #summary div { margin: 5px 0; line-height: 1.5; }
    #summary b { color: var(--text); }
    details { margin-top: 10px; }
    summary { cursor: pointer; font-size: 13px; color: var(--text-muted); padding: 6px 0; }
    #raw { white-space: pre-wrap; background: #f1f5f9; padding: 10px; font-size: 11px; border-radius: 8px; max-height: 300px; overflow: auto; }
  </style>
</head>
<body>
  <h3>📊 DART 재무데이터</h3>

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
    <button onclick="renderFiveStep()">분해해서 보기</button>
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

  <script>
    // 조회기간 입력 기본값: 최근 10년
    (function () {
      const thisYear = new Date().getFullYear();
      document.getElementById('startYear').value = thisYear - 9;
      document.getElementById('endYear').value = thisYear;
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

    function renderTable(rows) {
      currentRows = rows;

      // 열 정의: 관련 있는 항목끼리 묶어서 순서대로. type이 서식을 결정.
      // key가 '_'로 시작하면 저장된 값이 아니라 그 자리에서 계산하는 파생값(매출총이익률 등).
      const COLUMNS = [
        { label: '기간', key: 'period_label', type: 'text' },
        { label: 'fs_div', key: 'fs_div', type: 'text' },
        // 손익
        { label: '매출액', key: 'revenue', type: 'won' },
        { label: '매출원가', key: 'cogs', type: 'won' },
        { label: '매출총이익률', key: '_grossMargin', type: 'percent' },
        { label: '영업이익', key: 'operating_income', type: 'won' },
        { label: '영업이익률', key: '_opMargin', type: 'percent' },
        { label: '당기순이익', key: 'net_income', type: 'won' },
        { label: '지배주주순이익', key: 'parent_net_income', type: 'won' },
        { label: '세전이익', key: 'pretax_income', type: 'won' },
        { label: '이자비용', key: 'interest_expense', type: 'won' },
        // 현금흐름
        { label: '영업활동현금흐름', key: 'ocf', type: 'won' },
        { label: 'CapEx', key: 'capex', type: 'won' },
        { label: '잉여현금흐름', key: 'fcf', type: 'won' },
        // 자본/부채
        { label: '총자본', key: 'total_equity', type: 'won' },
        { label: '지배주주자기자본', key: 'parent_equity', type: 'won' },
        { label: '총부채', key: 'total_liabilities', type: 'won' },
        // 현금성/금융자산
        { label: '현금및현금성자산', key: 'cash', type: 'won' },
        { label: '단기금융자산', key: 'st_financial_assets', type: 'won' },
        { label: '단기매매증권', key: 'short_term_trading_securities', type: 'won' },
        { label: '당기손익FV금융자산', key: 'fvpl_financial_assets', type: 'won' },
        { label: '기타포괄손익FV금융자산', key: 'fvoci_financial_assets', type: 'won' },
        { label: '단기대여금', key: 'short_term_loans', type: 'won' },
        // 운전자본
        { label: '매출채권', key: 'receivables', type: 'won' },
        { label: '기타채권', key: 'other_receivables', type: 'won' },
        { label: '재고자산', key: 'inventory', type: 'won' },
        { label: '매입채무', key: 'payables', type: 'won' },
        { label: '기타채무', key: 'other_payables', type: 'won' },
        // 차입금/리스부채
        { label: '단기차입금', key: 'short_term_borrowings', type: 'won' },
        { label: '유동성장기부채', key: 'current_portion_lt_debt', type: 'won' },
        { label: '유동리스부채', key: 'current_lease_liabilities', type: 'won' },
        // 고정자산
        { label: '유형자산', key: 'tangible_assets', type: 'won' },
        { label: '무형자산', key: 'intangible_assets', type: 'won' },
        { label: '사용권자산', key: 'right_of_use_assets', type: 'won' },
        { label: '투자부동산', key: 'investment_property', type: 'won' },
        // 주식/배당
        { label: '총주식수', key: 'total_shares', type: 'won' },
        { label: '자기주식수', key: 'treasury_shares', type: 'won' },
        { label: '주식수 출처', key: 'shares_source', type: 'text' },
        { label: '주당배당금', key: 'dividend_per_share', type: 'won' },
        // 공시시점 밸류에이션·수익성 (모두 TTM: 과거 분기를 모아 연간화한 값)
        { label: '공시일자', key: 'filing_date', type: 'text' },
        { label: '공시시점 주가', key: 'price_at_filing', type: 'won' },
        { label: '공시시점 PER(TTM)', key: 'per_at_filing', type: 'ratio' },
        { label: '공시시점 PBR', key: 'pbr_at_filing', type: 'ratio' },
        { label: '공시시점 ROA(TTM)', key: 'roa_at_filing', type: 'percent' },
        { label: '공시시점 FCF Yield(TTM)', key: 'fcf_yield_at_filing', type: 'percent' },
        { label: '공시시점 PEG(TTM)', key: 'peg_at_filing', type: 'ratio' },
        { label: '비고', key: 'error', type: 'text' },
      ];

      function rawValue(r, col) {
        if (col.key === '_grossMargin') return (r.revenue != null && r.cogs != null && r.revenue) ? (r.revenue - r.cogs) / r.revenue : null;
        if (col.key === '_opMargin') return (r.operating_income != null && r.revenue) ? r.operating_income / r.revenue : null;
        return r[col.key];
      }

      function formatCell(v, type, key) {
        if (type === 'text') return v ?? (key === 'error' ? '' : 'N/A');
        if (v == null) return 'N/A';
        if (type === 'percent') return (v * 100).toFixed(2) + '%';
        if (type === 'ratio') return v.toFixed(2);
        return Number(v).toLocaleString();
      }

      let html = '<table><tr>' + COLUMNS.map((col) => {
        const chartable = col.type !== 'text';
        return chartable
          ? \`<th ondblclick="showChart('\${col.key}','\${col.label}')" title="더블클릭하면 그래프">\${col.label}</th>\`
          : \`<th>\${col.label}</th>\`;
      }).join('') + '</tr>';

      for (const r of rows) {
        html += '<tr>' + COLUMNS.map((col) => \`<td>\${formatCell(rawValue(r, col), col.type, col.key)}</td>\`).join('') + '</tr>';
      }
      html += '</table>';
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

      return { label: r.period_label, isOFS: r.fs_div === 'OFS', taxBurden, interestBurden, ebitMargin, assetTurnover, leverage, roeCheck, parentROE, roic };
    }

    function compute5StepRows(annualRows) {
      const byYear = {};
      annualRows.forEach((r) => { byYear[r.bsns_year] = r; });
      return annualRows.map((r) => computeStepMetrics(r, byYear[String(Number(r.bsns_year) - 1)]));
    }

    // 사업보고서가 아직 없는 최신 연도를 위한 TTM(최근 4개 분기 합산) 행 생성
    function buildTTMRow(quarterlyRows) {
      if (quarterlyRows.length === 0) return null;
      const latest = quarterlyRows.reduce((a, b) => (b.period_order > a.period_order ? b : a));
      // period_order = 연도*10 + 분기번호 (예: 20262 → 2026년 2분기)
      const N = Number(latest.period_order) % 10;
      if (!(N >= 1 && N <= 4)) return null;
      if (N === 4) return null; // 이미 사업보고서(연간) 데이터가 있음

      const Y = Number(latest.bsns_year);
      const find = (year, q) => quarterlyRows.find((r) => r.bsns_year === String(year) && r.period_label === \`\${year} \${q}분기\`);

      const needed = [];
      for (let k = 1; k <= N; k++) needed.push(find(Y, k));
      for (let k = N + 1; k <= 4; k++) needed.push(find(Y - 1, k));
      if (needed.some((r) => !r)) return null; // 4개 분기가 다 모여야 TTM 계산 가능

      const ttm = { ...latest, bsns_year: String(Y), period_label: \`\${Y} TTM (\${N}분기 기준)\` };
      for (const key of FLOW_KEYS) {
        const vals = needed.map((r) => r[key]);
        ttm[key] = vals.every((v) => v != null) ? vals.reduce((a, b) => a + b, 0) : null;
      }

      const priorSameQ = find(Y - 1, N); // 평균자기자본/평균자산 계산용: 1년 전 같은 분기
      return { row: ttm, prior: priorSameQ };
    }

    function renderFiveStep() {
      const annualRows = toAnnualRows(rawRows);
      const quarterlyRows = toQuarterlyRows(rawRows);
      const ttm = buildTTMRow(quarterlyRows);

      const steps = compute5StepRows(annualRows);
      if (ttm) steps.push(computeStepMetrics(ttm.row, ttm.prior));

      if (steps.length === 0) { alert('분석할 데이터가 없습니다.'); return; }

      const pct = (v) => v != null ? (v * 100).toFixed(1) + '%' : 'N/A';
      const num = (v) => v != null ? v.toFixed(2) : 'N/A';

      let html = '<table><tr><th>기간</th><th>세율부담<br/>(순이익/세전)</th><th>이자부담<br/>(세전/EBIT)</th><th>EBIT마진</th><th>자산회전율</th><th>레버리지</th><th>계산된 ROE<br/>(연결·평균자본)</th><th>지배주주 ROE<br/>(평균 지배자본)</th><th>ROIC</th></tr>';
      for (const s of steps) {
        html += \`<tr><td>\${s.label}</td><td>\${pct(s.taxBurden)}</td><td>\${pct(s.interestBurden)}</td><td>\${pct(s.ebitMargin)}</td><td>\${num(s.assetTurnover)}</td><td>\${num(s.leverage)}</td><td>\${pct(s.roeCheck)}</td><td>\${s.parentROE == null && s.isOFS ? '해당없음(개별)' : pct(s.parentROE)}</td><td>\${pct(s.roic)}</td></tr>\`;
      }
      html += '</table>';
      const el = document.getElementById('fiveStepWrap');
      el.style.display = 'block';
      el.innerHTML = '<div class="card-title">5단계 ROE 분해 (연도별, 최신 연도는 사업보고서 없으면 TTM)</div>' + html;
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
        .map((r) => computeROEAvg(r, byYearForROE[String(Number(r.bsns_year) - 1)]))
        .filter((x) => x && isFinite(x.value));
      const roes = roeResults.map((x) => x.value);
      const consolCnt = roeResults.filter((x) => x.basis === 'consolidated').length;
      const avgROIC = roics.length ? roics.reduce((a, b) => a + b, 0) / roics.length : null;
      const avgROE = roes.length ? roes.reduce((a, b) => a + b, 0) / roes.length : null;

      // BPS/유통주식수/EPS는 "가장 최근 사업보고서"가 아니라 실제로 가장 최근 조회된 시점(분기 포함) 기준
      const latest = latestSnapshotRow(rows) || annualRows[annualRows.length - 1];
      const outstandingShares = (latest.total_shares != null && latest.treasury_shares != null)
        ? latest.total_shares - latest.treasury_shares
        : null;
      const equityForBps = latest.parent_equity != null ? latest.parent_equity : latest.total_equity;
      const bpsBasis = latest.parent_equity != null ? '지배주주지분' : (latest.fs_div === 'OFS' ? '총자본(개별재무제표)' : '총자본(지배주주지분 자료 없음)');
      const bps = (outstandingShares && equityForBps != null) ? equityForBps / outstandingShares : null;
      const projected = (bps != null && avgROE != null) ? bps * Math.pow(1 + avgROE, 10) : null;

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

      return {
        avgROIC, roicN: roics.length, avgROE, roeN: roes.length, consolCnt,
        latestLabel: latest.period_label, bpsBasis, bps, projected,
        priceInput, marketCap, expectedMultiple, annualizedReturn,
        eps, epsBasis: earningsBasis + '·' + earningsSrcBasis, per, pbr, earningsYield,
      };
    }

    function renderSummary() {
      const priceInput = Number(document.getElementById('currentPrice').value) || null;
      const m = computeSummaryMetrics(rawRows, priceInput);
      if (!m) { alert('연간(사업보고서) 데이터가 없습니다. 먼저 조회/저장하세요.'); return; }

      const pctStr = (v) => v != null ? (v * 100).toFixed(2) + '%' : 'N/A';
      const wonStr = (v) => v != null ? Math.round(v).toLocaleString() + '원' : 'N/A';
      const numStr = (v) => v != null ? v.toFixed(2) : 'N/A';
      const roicJudge = m.avgROIC != null ? (m.avgROIC >= 0.10 ? '✅ 10% 이상' : '⚠️ 10% 미만') : '';
      let valuationJudge = '';
      if (m.projected != null && m.priceInput) {
        valuationJudge = m.projected > m.priceInput ? '✅ 예상가 > 현재가 (저평가 가능성)' : '⚠️ 예상가 ≤ 현재가 (고평가 가능성)';
      }

      const el = document.getElementById('summary');
      el.style.display = 'block';
      el.innerHTML = \`
        <div><b>10년 평균 ROIC:</b> \${pctStr(m.avgROIC)} (연도 \${m.roicN}개 평균) \${roicJudge}</div>
        <div><b>10년 평균 ROE:</b> \${pctStr(m.avgROE)} (\${m.roeN}개 연도 평균 · 지배주주순이익÷평균 지배주주자본\${m.consolCnt ? ', 지배주주 자료가 없는(개별재무제표 등) ' + m.consolCnt + '개 연도는 순이익÷평균 총자본' : ''})</div>
        <div><b>최근 BPS(\${m.latestLabel} 기준, \${m.bpsBasis}÷보통주 유통주식):</b> \${wonStr(m.bps)}</div>
        <div><b>10년 후 예상 주가 (BPS×(1+평균ROE)^10):</b> \${wonStr(m.projected)}</div>
        \${valuationJudge ? \`<div><b>비교 결과:</b> \${valuationJudge} (현재가: \${m.priceInput.toLocaleString()}원)\` : '<div style="color:#888">현재 주가를 입력하면 비교 결과가 표시됩니다.</div>'}
        \${m.annualizedReturn != null ? \`<div><b>연환산 기대수익률(CAGR):</b> \${(m.annualizedReturn * 100).toFixed(2)}% (10년간 \${m.expectedMultiple.toFixed(2)}배 상승 가정)</div>\` : ''}
        \${m.marketCap != null ? \`<div><b>참고 시가총액:</b> \${wonStr(m.marketCap)}</div>\` : ''}
        <hr style="border:none;border-top:1px solid var(--border);margin:8px 0;"/>
        <div><b>EPS(\${m.epsBasis} 기준):</b> \${m.eps != null ? Math.round(m.eps).toLocaleString() + '원' : 'N/A'}</div>
        <div><b>PER:</b> \${numStr(m.per)}\${m.per != null ? '배' : ''}</div>
        <div><b>PBR:</b> \${numStr(m.pbr)}\${m.pbr != null ? '배' : ''}</div>
        <div><b>이익수익률(1/PER, 요구수익률 관점):</b> \${pctStr(m.earningsYield)}</div>
      \`;
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
  return { total_shares: parseAmount(row.istc_totqy), treasury_shares: parseAmount(row.tesstk_co) };
}

function pickDividendPerShare(dart) {
  if (!dart || dart.status !== "000") return null;
  const row = dart.list.find((r) => r.se === "주당 현금배당금(원)" && r.stock_knd === "보통주");
  return row ? parseAmount(row.thstrm) : null;
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
    .map((row) => ({ date: String(row[0]), close: Number(row[4]) }))
    .filter((r) => /^\d{8}$/.test(r.date) && !Number.isNaN(r.close));
}

// targetDate(YYYYMMDD) 이전(포함) 중 가장 최근 거래일의 종가를 찾는다 (공시일이 휴일/주말일 수 있으므로)
function closeAsOf(prices, targetDate) {
  const candidates = prices.filter((p) => p.date <= targetDate).sort((a, b) => b.date.localeCompare(a.date));
  return candidates.length ? candidates[0].close : null;
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
      filing_date: null, price_at_filing: null, per_at_filing: null, pbr_at_filing: null, fcf_yield_at_filing: null,
      roa_at_filing: null, peg_at_filing: null,
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

    // 공시일자(rcept_no 앞 8자리) 기준 종가로 그 시점 PER/PBR/FCF Yield 계산
    const rceptNo = dart.list[0] && dart.list[0].rcept_no;
    if (rceptNo && stockCode) {
      const filingDate = rceptNo.slice(0, 8);
      row.filing_date = filingDate;
      try {
        const prices = await fetchNaverPrices(stockCode, addDaysStr(filingDate, -15), filingDate, proxyUrl);
        const price = closeAsOf(prices, filingDate);
        row.price_at_filing = price;

        const outstanding = (stockCounts.total_shares != null && stockCounts.treasury_shares != null)
          ? stockCounts.total_shares - stockCounts.treasury_shares
          : null;
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

        // ROA(TTM) = TTM 연결순이익 ÷ 평균총자산 (평균: 이번 분기말 + 1년 전 같은 분기말)
        const totalAssetsNow = (vals.total_liabilities != null && vals.total_equity != null) ? vals.total_liabilities + vals.total_equity : null;
        const priorYearBS = await getStoredPeriod(db, corpCode, period.year - 1, period.code, ["total_liabilities", "total_equity"]);
        const totalAssetsPrior = (priorYearBS && priorYearBS.total_liabilities != null && priorYearBS.total_equity != null)
          ? priorYearBS.total_liabilities + priorYearBS.total_equity
          : null;
        const avgAssets = (totalAssetsNow != null && totalAssetsPrior != null) ? (totalAssetsNow + totalAssetsPrior) / 2 : totalAssetsNow;
        row.roa_at_filing = (avgAssets && ttm && ttm.net_income != null) ? ttm.net_income / avgAssets : null;

        // PEG(TTM) = PER(TTM) ÷ TTM EPS 성장률(%) — 성장률은 "1년 전 같은 시점" TTM EPS 대비 (주식수는 현재값으로 근사)
        if (row.per_at_filing != null && outstanding) {
          const ttmPrior = await getTTMFlow(db, corpCode, period.year - 1, quarterNum, ["net_income", "parent_net_income"]);
          const ttmEarningsPrior = ttmPrior ? (ttmPrior.parent_net_income != null ? ttmPrior.parent_net_income : ttmPrior.net_income) : null;
          const epsPrior = ttmEarningsPrior != null ? ttmEarningsPrior / outstanding : null;
          if (eps != null && epsPrior != null && epsPrior > 0) {
            const growthPct = ((eps - epsPrior) / epsPrior) * 100;
            row.peg_at_filing = growthPct > 0 ? row.per_at_filing / growthPct : null; // 역성장 구간은 PEG가 의미 없어 null 처리
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

    return new Response("Not Found", { status: 404 });
  },
};
