// ============================================================
// Phase 3 (개정): DART 조회를 연도 구간별로 나눠서 D1에 저장하고,
// 화면은 "DART에서 조회+저장"과 "DB에서 조회(DART 재조회 없음)"
// 두 가지 방식으로 데이터를 볼 수 있게 함.
//
// 이렇게 바꾸는 이유: Cloudflare Workers 무료 플랜은 한 번의 요청에서
// 외부로 나가는 호출(subrequest)을 최대 50개로 제한합니다. 10년치를
// 한 번에 부르면 이 한도를 넘어서 전부 실패합니다. 연도 구간을 나눠서
// 여러 번 호출하고 결과를 D1에 저장해두면, 이후 조회는 D1만 읽으므로
// (D1 조회는 subrequest로 카운트되지 않음) 이 제한과 무관해집니다.
// ============================================================

const CONCURRENCY = 3;

const REPRT_CODES = [
  { code: "11013", label: "1분기", order: 1 },
  { code: "11012", label: "반기", order: 2 },
  { code: "11014", label: "3분기", order: 3 },
  { code: "11011", label: "사업보고서", order: 4 },
];

const ACCOUNT_ITEMS = [
  { key: "revenue", ids: ["ifrs-full_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"], names: ["매출액", "수익(매출액)"] },
  { key: "cogs", ids: ["ifrs-full_CostOfSales"], names: ["매출원가"] },
  { key: "operating_income", ids: ["dart_OperatingIncomeLoss"], names: ["영업이익"] },
  { key: "net_income", ids: ["ifrs-full_ProfitLoss"], names: ["당기순이익", "반기순이익", "분기순이익", "순이익"] },
  { key: "total_equity", ids: ["ifrs-full_Equity"], names: ["자본총계"] },
  { key: "total_liabilities", ids: ["ifrs-full_Liabilities"], names: ["부채총계"] },
  { key: "cash", ids: ["ifrs-full_CashAndCashEquivalents"], names: ["현금및현금성자산"] },
  { key: "st_financial_assets", ids: [], names: ["단기금융상품", "단기금융자산"] },
  { key: "ocf", ids: ["ifrs-full_CashFlowsFromUsedInOperatingActivities"], names: ["영업활동현금흐름", "영업활동으로 인한 현금흐름", "영업활동으로인한현금흐름"] },
  { key: "capex_ppe", ids: ["ifrs-full_PurchaseOfPropertyPlantAndEquipment", "ifrs-full_PaymentsToAcquirePropertyPlantAndEquipment"], names: ["유형자산의 취득", "유형자산 취득", "유형자산의취득", "유형자산취득"] },
  { key: "capex_intangible", ids: ["ifrs-full_PurchaseOfIntangibleAssetsOtherThanGoodwill", "ifrs-full_PurchaseOfIntangibleAssets", "ifrs-full_PaymentsToAcquireIntangibleAssets"], names: ["무형자산의 취득", "무형자산 취득", "무형자산의취득", "무형자산취득"] },
  { key: "receivables", ids: ["ifrs-full_TradeAndOtherCurrentReceivables"], names: ["매출채권"] },
  { key: "inventory", ids: ["ifrs-full_Inventories"], names: ["재고자산"] },
  { key: "payables", ids: ["ifrs-full_TradeAndOtherCurrentPayables"], names: ["매입채무"] },
];

const DB_COLUMNS = [
  "corp_code", "bsns_year", "reprt_code", "period_label", "period_order", "fs_div",
  "revenue", "cogs", "operating_income", "net_income",
  "total_equity", "total_liabilities", "cash", "st_financial_assets",
  "ocf", "capex", "fcf", "receivables", "inventory", "payables",
  "total_shares", "treasury_shares", "dividend_per_share",
  "updated_at",
];

const HTML_PAGE = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DART 재무데이터</title>
  <style>
    body { font-family: sans-serif; max-width: 100%; margin: 20px auto; padding: 0 12px; }
    input, button, select { font-size: 16px; padding: 8px; margin: 4px 4px 4px 0; }
    #wrap { overflow-x: auto; margin-top: 12px; }
    table { border-collapse: collapse; white-space: nowrap; font-size: 13px; }
    td, th { border: 1px solid #ccc; padding: 6px 8px; text-align: right; }
    th { background: #f5f5f5; position: sticky; top: 0; }
    th:first-child, td:first-child { position: sticky; left: 0; background: #fff; text-align: left; }
    #status { color: #b00; margin-top: 8px; white-space: pre-line; }
  </style>
</head>
<body>
  <h3>DART 재무데이터</h3>
  <input id="corpName" placeholder="종목명 (예: 삼성전자)" value="삼성전자" />
  <select id="yearsBack">
    <option value="3">최근 3년</option>
    <option value="10" selected>최근 10년</option>
  </select>
  <br/>
  <button onclick="fetchAndSave()">DART에서 조회 + 저장</button>
  <button onclick="loadFromDb()">DB에서 조회</button>
  <button onclick="rawCheck('stockTotqySttus')">주식총수 원본보기</button>
  <button onclick="rawCheck('alotMatter')">배당현황 원본보기</button>
  <div id="status"></div>
  <pre id="raw" style="white-space:pre-wrap; background:#f5f5f5; padding:8px; font-size:11px;"></pre>
  <div id="wrap"></div>

  <script>
    async function rawCheck(kind) {
      const corpName = document.getElementById('corpName').value.trim();
      const yearsBack = Number(document.getElementById('yearsBack').value);
      const statusEl = document.getElementById('status');
      const rawEl = document.getElementById('raw');
      const thisYear = new Date().getFullYear();
      statusEl.textContent = '원본 조회 중...';
      rawEl.textContent = '';
      try {
        const res = await fetch(\`/api/raw?kind=\${kind}&corp_name=\${encodeURIComponent(corpName)}&bsns_year=\${thisYear - 1}&reprt_code=11011\`);
        const data = await res.json();
        statusEl.textContent = \`\${kind} 원본 (사업보고서, \${thisYear - 1}년)\`;
        rawEl.textContent = JSON.stringify(data, null, 2);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }
  </script>

  <script>
    function renderTable(rows) {
      const cols = ['기간', 'fs_div', '매출액', '매출원가', '영업이익', '당기순이익', '총자본', '총부채', '현금및현금성자산', '단기금융자산', '영업활동현금흐름', 'CapEx', '잉여현금흐름', '매출채권', '재고자산', '매입채무', '총주식수', '자기주식수', '주당배당금', '비고'];
      let html = '<table><tr>' + cols.map(c => \`<th>\${c}</th>\`).join('') + '</tr>';
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

    async function fetchAndSave() {
      const corpName = document.getElementById('corpName').value.trim();
      const yearsBack = Number(document.getElementById('yearsBack').value);
      const statusEl = document.getElementById('status');
      document.getElementById('wrap').innerHTML = '';

      const thisYear = new Date().getFullYear();
      const startYear = thisYear - yearsBack + 1;

      // 3년 단위로 구간을 나눠서 순서대로 호출 (항목이 늘어 호출 수가 늘었으므로 묶음을 더 작게)
      const chunks = [];
      for (let y = startYear; y <= thisYear; y += 3) {
        chunks.push([y, Math.min(y + 2, thisYear)]);
      }

      let allRows = [];
      for (let i = 0; i < chunks.length; i++) {
        const [s, e] = chunks[i];
        statusEl.textContent = \`저장 중... (\${i + 1}/\${chunks.length}구간: \${s}~\${e}년)\`;
        try {
          const res = await fetch(\`/api/fetch-and-save?corp_name=\${encodeURIComponent(corpName)}&start_year=\${s}&end_year=\${e}\`);
          const data = await res.json();
          if (!res.ok) throw new Error(data.error || '저장 실패');
          allRows = allRows.concat(data.rows);
        } catch (e) {
          statusEl.textContent = \`오류(\${s}~\${e}년 구간): \` + e.message;
          return;
        }
      }
      statusEl.textContent = \`저장 완료 (\${allRows.length}개 기간)\`;
      renderTable(allRows);
    }

    async function loadFromDb() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      document.getElementById('wrap').innerHTML = '';
      statusEl.textContent = 'DB 조회 중...';
      try {
        const res = await fetch(\`/api/financial-history-db?corp_name=\${encodeURIComponent(corpName)}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || 'DB 조회 실패');
        statusEl.textContent = \`\${data.corp_name} — DB에 저장된 \${data.rows.length}개 기간 (DART 재조회 안 함)\`;
        renderTable(data.rows);
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
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

function pickStockCounts(dart) {
  if (!dart || dart.status !== "000") return { total_shares: null, treasury_shares: null };
  const row = dart.list.find((r) => r.se === "합계");
  if (!row) return { total_shares: null, treasury_shares: null };
  return { total_shares: parseAmount(row.istc_totqy), treasury_shares: parseAmount(row.tesstk_co) };
}

function pickDividendPerShare(dart) {
  if (!dart || dart.status !== "000") return null;
  const row = dart.list.find((r) => r.se === "주당 현금배당금(원)" && r.stock_knd === "보통주");
  return row ? parseAmount(row.thstrm) : null;
}

function pickAccount(list, ids, names) {
  let item = list.find((row) => ids.includes(row.account_id));
  if (!item) item = list.find((row) => names.some((n) => row.account_nm?.includes(n)));
  if (!item) return null;
  const raw = item.thstrm_amount?.replace(/,/g, "");
  return raw ? Number(raw) : null;
}

async function mapWithConcurrency(items, limit, fn) {
  const results = new Array(items.length);
  let i = 0;
  async function worker() {
    while (i < items.length) {
      const idx = i++;
      results[idx] = await fn(items[idx], idx);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

function buildPeriods(startYear, endYear) {
  const periods = [];
  for (let y = startYear; y <= endYear; y++) {
    for (const r of REPRT_CODES) periods.push({ year: y, ...r });
  }
  return periods;
}

async function fetchPeriodRow(corpCode, period, proxyUrl) {
  const emptyRow = (extra) => ({
    corp_code: corpCode,
    bsns_year: String(period.year),
    reprt_code: period.code,
    period_label: `${period.year} ${period.label}`,
    period_order: period.year * 10 + period.order,
    fs_div: null,
    revenue: null, cogs: null, operating_income: null, net_income: null,
    total_equity: null, total_liabilities: null, cash: null, st_financial_assets: null,
    ocf: null, capex: null, fcf: null, receivables: null, inventory: null, payables: null,
    total_shares: null, treasury_shares: null, dividend_per_share: null,
    ...extra,
  });

  try {
    let fsDiv = "CFS";
    let dart = await fetchDartWithRetry(corpCode, period.year, period.code, fsDiv, proxyUrl);
    if (dart.status !== "000") {
      fsDiv = "OFS";
      dart = await fetchDartWithRetry(corpCode, period.year, period.code, fsDiv, proxyUrl);
    }
    if (dart.status !== "000") return emptyRow();

    const vals = {};
    for (const item of ACCOUNT_ITEMS) vals[item.key] = pickAccount(dart.list, item.ids, item.names);

    const capex = (vals.capex_ppe != null || vals.capex_intangible != null)
      ? Math.abs(vals.capex_ppe || 0) + Math.abs(vals.capex_intangible || 0)
      : null;
    const fcf = vals.ocf != null && capex != null ? vals.ocf - capex : null;

    // 주식총수/자기주식수는 매 기간, 배당은 사업보고서(연간)만 조회 (호출 수 절약)
    let stockCounts = { total_shares: null, treasury_shares: null };
    let dividendPerShare = null;
    try {
      const stockDart = await fetchDartGeneric("stockTotqySttus", corpCode, period.year, period.code, proxyUrl);
      stockCounts = pickStockCounts(stockDart);
    } catch (e) {
      // 실패해도 나머지 재무데이터는 살림
    }
    if (period.code === "11011") {
      try {
        const divDart = await fetchDartGeneric("alotMatter", corpCode, period.year, period.code, proxyUrl);
        dividendPerShare = pickDividendPerShare(divDart);
      } catch (e) {
        // 실패해도 나머지 재무데이터는 살림
      }
    }

    return emptyRow({
      fs_div: fsDiv,
      revenue: vals.revenue, cogs: vals.cogs, operating_income: vals.operating_income, net_income: vals.net_income,
      total_equity: vals.total_equity, total_liabilities: vals.total_liabilities, cash: vals.cash, st_financial_assets: vals.st_financial_assets,
      ocf: vals.ocf, capex, fcf, receivables: vals.receivables, inventory: vals.inventory, payables: vals.payables,
      total_shares: stockCounts.total_shares, treasury_shares: stockCounts.treasury_shares, dividend_per_share: dividendPerShare,
    });
  } catch (e) {
    return emptyRow({ error: String(e.message || e) });
  }
}

async function saveRowsToDb(db, rows) {
  const now = new Date().toISOString();
  const colSql = DB_COLUMNS.join(", ");
  const ph = "(" + DB_COLUMNS.map(() => "?").join(", ") + ")";
  for (const r of rows) {
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
      const kind = searchParams.get("kind"); // stockTotqySttus | alotMatter
      const corpName = searchParams.get("corp_name");
      const bsnsYear = searchParams.get("bsns_year");
      const reprtCode = searchParams.get("reprt_code") || "11011";

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const raw = await fetchDartGeneric(kind, corpRow.corp_code, bsnsYear, reprtCode, env.DART_PROXY_URL);
      return Response.json(raw);
    }

    if (pathname === "/api/fetch-and-save") {
      const corpName = searchParams.get("corp_name");
      const startYear = Number(searchParams.get("start_year"));
      const endYear = Number(searchParams.get("end_year"));

      const corpRow = await env.DB.prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?").bind(corpName).first();
      if (!corpRow) return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });

      const periods = buildPeriods(startYear, endYear);
      const rows = await mapWithConcurrency(periods, CONCURRENCY, (p) => fetchPeriodRow(corpRow.corp_code, p, env.DART_PROXY_URL));

      await saveRowsToDb(env.DB, rows);

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, rows });
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
