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
const FLOW_KEYS = ["revenue", "cogs", "operating_income", "net_income", "ocf", "capex", "fcf"];

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
];

const DB_COLUMNS = [
  "corp_code", "bsns_year", "reprt_code", "period_label", "period_order", "fs_div",
  "revenue", "cogs", "operating_income", "net_income",
  "total_equity", "total_liabilities", "cash", "st_financial_assets",
  "ocf", "capex", "fcf", "receivables", "inventory", "payables",
  "total_shares", "treasury_shares", "dividend_per_share",
  "short_term_trading_securities", "fvpl_financial_assets", "fvoci_financial_assets", "investment_property",
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
      <button class="primary" onclick="renderSummary()">계산하기</button>
    </div>
    <div id="summary" style="display:none; margin-top:10px;"></div>
  </div>

  <div id="status"></div>
  <div id="wrap"></div>
  <div id="chartWrap" style="display:none; margin-top:14px;" class="card">
    <div id="chartTitle" style="font-weight:600; margin-bottom:4px;"></div>
    <canvas id="chartCanvas" style="width:100%; height:220px;"></canvas>
    <p style="font-size:12px; color:var(--text-muted); margin-top:6px;">표의 열 제목을 더블클릭하면 그 항목의 추이가 여기 표시됩니다.</p>
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

    const FLOW_KEYS = ${JSON.stringify(FLOW_KEYS)};

    function toQuarterlyRows(rows) {
      const byYear = {};
      for (const r of rows) {
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
        .filter((r) => r.reprt_code === '11011')
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
      const cols = ['기간', 'fs_div', '매출액', '매출원가', '영업이익', '당기순이익', '총자본', '총부채', '현금및현금성자산', '단기금융자산', '영업활동현금흐름', 'CapEx', '잉여현금흐름', '매출채권', '재고자산', '매입채무', '총주식수', '자기주식수', '주당배당금', '비고'];
      const keys = [null, null, 'revenue', 'cogs', 'operating_income', 'net_income', 'total_equity', 'total_liabilities', 'cash', 'st_financial_assets', 'ocf', 'capex', 'fcf', 'receivables', 'inventory', 'payables', 'total_shares', 'treasury_shares', 'dividend_per_share', null];
      let html = '<table><tr>' + cols.map((c, i) =>
        keys[i]
          ? \`<th ondblclick="showChart('\${keys[i]}','\${c}')" title="더블클릭하면 그래프">\${c}</th>\`
          : \`<th>\${c}</th>\`
      ).join('') + '</tr>';
      for (const r of rows) {
        const cells = [
          r.period_label, r.fs_div ?? '-',
          r.revenue, r.cogs, r.operating_income, r.net_income,
          r.total_equity, r.total_liabilities, r.cash, r.st_financial_assets,
          r.ocf, r.capex, r.fcf, r.receivables, r.inventory, r.payables,
          r.total_shares, r.treasury_shares, r.dividend_per_share,
        ];
        html += '<tr>' + cells.map((v, i) => {
          if (i < 2) return \`<td>\${v}</td>\`;
          return \`<td>\${v != null ? Number(v).toLocaleString() : 'N/A'}</td>\`;
        }).join('') + \`<td>\${r.error ?? ''}</td>\` + '</tr>';
      }
      document.getElementById('wrap').innerHTML = html + '</table>';
    }

    function computeIC(r) {
      if (r.total_liabilities == null || r.total_equity == null) return null;
      const sub = (v) => v || 0;
      return r.total_liabilities + r.total_equity
        - sub(r.cash) - sub(r.st_financial_assets) - sub(r.short_term_trading_securities)
        - sub(r.fvpl_financial_assets) - sub(r.fvoci_financial_assets) - sub(r.investment_property);
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

    function latestSnapshotRow(rows) {
      // 대차대조표 항목(자본총계/주식수)이 있는 기간 중 가장 최근 것 — 사업보고서가 아직 없으면 최신 분기라도 사용
      const withData = rows.filter((r) => !r.error && r.total_equity != null && r.total_shares != null && r.treasury_shares != null);
      if (withData.length === 0) return null;
      return withData.reduce((a, b) => (b.period_order > a.period_order ? b : a));
    }

    function renderSummary() {
      const annualRows = toAnnualRows(rawRows);
      if (annualRows.length === 0) {
        alert('연간(사업보고서) 데이터가 없습니다. 먼저 조회/저장하세요.');
        return;
      }

      const roics = annualRows.map(computeROIC).filter((v) => v != null);
      const roes = annualRows.map(computeROE).filter((v) => v != null);
      const avgROIC = roics.length ? roics.reduce((a, b) => a + b, 0) / roics.length : null;
      const avgROE = roes.length ? roes.reduce((a, b) => a + b, 0) / roes.length : null;

      // BPS/유통주식수는 "가장 최근 사업보고서"가 아니라 "가장 최근에 실제로 조회된 시점"(분기 포함) 기준
      const latest = latestSnapshotRow(rawRows) || annualRows[annualRows.length - 1];
      const outstandingShares = (latest.total_shares != null && latest.treasury_shares != null)
        ? latest.total_shares - latest.treasury_shares
        : null;
      const bps = (outstandingShares && latest.total_equity != null) ? latest.total_equity / outstandingShares : null;
      const projected = (bps != null && avgROE != null) ? bps * Math.pow(1 + avgROE, 10) : null;

      const priceInput = Number(document.getElementById('currentPrice').value) || null;
      const marketCap = (priceInput && outstandingShares) ? priceInput * outstandingShares : null;

      const roicPct = avgROIC != null ? (avgROIC * 100).toFixed(2) + '%' : 'N/A';
      const roicJudge = avgROIC != null ? (avgROIC >= 0.10 ? '✅ 10% 이상' : '⚠️ 10% 미만') : '';
      const bpsStr = bps != null ? Math.round(bps).toLocaleString() + '원' : 'N/A';
      const projectedStr = projected != null ? Math.round(projected).toLocaleString() + '원' : 'N/A';
      let valuationJudge = '';
      if (projected != null && priceInput) {
        valuationJudge = projected > priceInput ? '✅ 예상가 > 현재가 (저평가 가능성)' : '⚠️ 예상가 ≤ 현재가 (고평가 가능성)';
      }

      const el = document.getElementById('summary');
      el.style.display = 'block';
      el.innerHTML = \`
        <div><b>10년 평균 ROIC:</b> \${roicPct} (연도 \${roics.length}개 평균) \${roicJudge}</div>
        <div><b>10년 평균 ROE:</b> \${avgROE != null ? (avgROE * 100).toFixed(2) + '%' : 'N/A'} (연도 \${roes.length}개 평균)</div>
        <div><b>최근 BPS(\${latest.period_label} 기준, 보통주 유통주식 기준):</b> \${bpsStr}</div>
        <div><b>10년 후 예상 주가 (BPS×(1+평균ROE)^10):</b> \${projectedStr}</div>
        \${valuationJudge ? \`<div><b>비교 결과:</b> \${valuationJudge} (현재가: \${priceInput.toLocaleString()}원)\` : '<div style="color:#888">현재 주가를 입력하면 비교 결과가 표시됩니다.</div>'}
        \${marketCap != null ? \`<div><b>참고 시가총액:</b> \${Math.round(marketCap).toLocaleString()}원</div>\` : ''}
      \`;
    }

    async function fetchAndSave() {
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
      statusEl.textContent = \`저장 완료 (\${rows.length}개 기간)\`;
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

    function showChart(key, label) {
      const points = currentRows
        .filter((r) => !r.error)
        .map((r) => ({ x: r.period_label, y: r[key] }))
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
function sumAccount(list, ids, names) {
  let matches = list.filter((row) => ids.includes(row.account_id));
  if (matches.length === 0) {
    matches = list.filter((row) => names.some((n) => row.account_nm?.includes(n)));
  }
  if (matches.length === 0) return null;
  let total = 0;
  let found = false;
  for (const m of matches) {
    const v = parseAmount(m.thstrm_amount);
    if (v != null) { total += v; found = true; }
  }
  return found ? total : null;
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

async function fetchPeriodRow(corpCode, period, proxyUrl) {
  const emptyRow = (extra) => {
    const row = {
      corp_code: corpCode,
      bsns_year: String(period.year),
      reprt_code: period.code,
      period_label: `${period.year} ${period.label}`,
      period_order: period.year * 10 + period.order,
      fs_div: null,
      total_shares: null, treasury_shares: null, dividend_per_share: null,
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
    for (const item of ACCOUNT_ITEMS) vals[item.key] = sumAccount(dart.list, item.ids, item.names);

    const capex = (vals.capex_ppe != null || vals.capex_intangible != null)
      ? Math.abs(vals.capex_ppe || 0) + Math.abs(vals.capex_intangible || 0)
      : null;
    const fcf = vals.ocf != null && capex != null ? vals.ocf - capex : null;

    let stockCounts = { total_shares: null, treasury_shares: null };
    let dividendPerShare = null;
    try {
      const stockDart = await fetchDartGeneric("stockTotqySttus", corpCode, period.year, period.code, proxyUrl);
      stockCounts = pickStockCounts(stockDart);
    } catch (e) { /* 실패해도 나머지는 살림 */ }
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
    });
    for (const item of ACCOUNT_ITEMS) row[item.key] = vals[item.key];
    row.capex = capex;
    row.fcf = fcf;
    delete row.capex_ppe;
    delete row.capex_intangible;
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
    if (r.error) continue; // 실패한 기간은 저장하지 않음 (다음에 다시 시도 가능하게)
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

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const raw = fsDiv
        ? await fetchDart(corpRow.corp_code, bsnsYear, reprtCode, fsDiv, env.DART_PROXY_URL)
        : await fetchDartGeneric(kind, corpRow.corp_code, bsnsYear, reprtCode, env.DART_PROXY_URL);
      return Response.json(raw);
    }

    if (pathname === "/api/fetch-and-save") {
      const corpName = searchParams.get("corp_name");
      const year = Number(searchParams.get("year"));
      const reprtCode = searchParams.get("reprt_code");

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const periodMeta = REPRT_CODES.find((r) => r.code === reprtCode);
      if (!periodMeta) return Response.json({ error: `알 수 없는 reprt_code: ${reprtCode}` }, { status: 400 });

      const row = await fetchPeriodRow(corpRow.corp_code, { year, ...periodMeta }, env.DART_PROXY_URL);
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
