// ============================================================
// Phase 3: 최근 N년 x 4개 보고서(1분기/반기/3분기/사업보고서) 재무데이터 조회
// Phase 2와 동일한 구조(corp_master 조회 -> Apps Script 중계 -> DART)를
// 여러 기간에 대해 반복하는 것뿐입니다.
// ============================================================

const YEARS_BACK = 3; // 우선 3년으로 시작. 안정적으로 되면 10으로 늘리세요.
const CONCURRENCY = 6; // 한 번에 동시에 보낼 DART 요청 수 (너무 크게 하면 Apps Script 동시실행 한도에 걸릴 수 있음)

const REPRT_CODES = [
  { code: "11013", label: "1분기" },
  { code: "11012", label: "반기" },
  { code: "11014", label: "3분기" },
  { code: "11011", label: "사업보고서" },
];

// 항목별 DART 표준계정ID(우선) + 계정명 텍스트(폴백)
const ACCOUNT_ITEMS = [
  { key: "revenue", label: "매출액", ids: ["ifrs-full_Revenue", "ifrs-full_RevenueFromContractsWithCustomers"], names: ["매출액", "수익(매출액)"] },
  { key: "cogs", label: "매출원가", ids: ["ifrs-full_CostOfSales"], names: ["매출원가"] },
  { key: "operating_income", label: "영업이익", ids: ["dart_OperatingIncomeLoss"], names: ["영업이익"] },
  { key: "net_income", label: "당기순이익", ids: ["ifrs-full_ProfitLoss"], names: ["당기순이익", "반기순이익", "분기순이익", "순이익"] },
  { key: "total_equity", label: "총자본", ids: ["ifrs-full_Equity"], names: ["자본총계"] },
  { key: "total_liabilities", label: "총부채", ids: ["ifrs-full_Liabilities"], names: ["부채총계"] },
  { key: "cash", label: "현금및현금성자산", ids: ["ifrs-full_CashAndCashEquivalents"], names: ["현금및현금성자산"] },
  { key: "st_financial_assets", label: "단기금융자산", ids: [], names: ["단기금융상품", "단기금융자산"] },
  { key: "ocf", label: "영업활동현금흐름", ids: ["ifrs-full_CashFlowsFromUsedInOperatingActivities"], names: ["영업활동현금흐름", "영업활동으로인한현금흐름"] },
  { key: "capex_ppe", label: "__capex_ppe", ids: ["ifrs-full_PurchaseOfPropertyPlantAndEquipment"], names: ["유형자산의취득", "유형자산취득"] },
  { key: "capex_intangible", label: "__capex_intangible", ids: ["ifrs-full_PurchaseOfIntangibleAssetsOtherThanGoodwill", "ifrs-full_PurchaseOfIntangibleAssets"], names: ["무형자산의취득", "무형자산취득"] },
  { key: "receivables", label: "매출채권", ids: ["ifrs-full_TradeAndOtherCurrentReceivables"], names: ["매출채권"] },
  { key: "inventory", label: "재고자산", ids: ["ifrs-full_Inventories"], names: ["재고자산"] },
  { key: "payables", label: "매입채무", ids: ["ifrs-full_TradeAndOtherCurrentPayables"], names: ["매입채무"] },
];

const HTML_PAGE = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DART 10년 재무데이터 (3단계)</title>
  <style>
    body { font-family: sans-serif; max-width: 100%; margin: 20px auto; padding: 0 12px; }
    input, button { font-size: 16px; padding: 8px; margin: 4px 0; }
    #wrap { overflow-x: auto; margin-top: 12px; }
    table { border-collapse: collapse; white-space: nowrap; font-size: 13px; }
    td, th { border: 1px solid #ccc; padding: 6px 8px; text-align: right; }
    th { background: #f5f5f5; position: sticky; top: 0; }
    th:first-child, td:first-child { position: sticky; left: 0; background: #fff; text-align: left; }
    #status { color: #b00; margin-top: 8px; }
  </style>
</head>
<body>
  <h3>DART 재무데이터 조회 (최근 ${YEARS_BACK}년)</h3>
  <input id="corpName" placeholder="종목명 (예: 삼성전자)" value="삼성전자" />
  <button onclick="run()">조회</button>
  <div id="status"></div>
  <div id="wrap"></div>

  <script>
    async function run() {
      const corpName = document.getElementById('corpName').value.trim();
      const statusEl = document.getElementById('status');
      const wrapEl = document.getElementById('wrap');
      statusEl.textContent = '조회 중... (최대 1분 정도 걸릴 수 있습니다)';
      wrapEl.innerHTML = '';

      try {
        const res = await fetch(\`/api/financial-history?corp_name=\${encodeURIComponent(corpName)}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');

        statusEl.textContent = \`\${data.corp_name} (\${data.corp_code}) — \${data.rows.length}개 기간\`;

        const cols = ['기간', 'fs_div', '매출액', '매출원가', '영업이익', '당기순이익', '총자본', '총부채', '현금및현금성자산', '단기금융자산', '영업활동현금흐름', 'CapEx', '잉여현금흐름', '매출채권', '재고자산', '매입채무'];
        let html = '<table><tr>' + cols.map(c => \`<th>\${c}</th>\`).join('') + '</tr>';
        for (const r of data.rows) {
          html += '<tr>' + [
            r.period_label, r.fs_div ?? '-',
            r.revenue, r.cogs, r.operating_income, r.net_income,
            r.total_equity, r.total_liabilities, r.cash, r.st_financial_assets,
            r.ocf, r.capex, r.fcf, r.receivables, r.inventory, r.payables,
          ].map(v => \`<td>\${v != null ? Number(v).toLocaleString() : (v ?? 'N/A')}</td>\`).join('') + '</tr>';
        }
        html += '</table>';
        wrapEl.innerHTML = html;
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }
  </script>
</body>
</html>`;

async function fetchDart(corpCode, bsnsYear, reprtCode, fsDiv, proxyUrl) {
  const url = new URL(proxyUrl);
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bsns_year", bsnsYear);
  url.searchParams.set("reprt_code", reprtCode);
  url.searchParams.set("fs_div", fsDiv);
  const resp = await fetch(url.toString());
  return resp.json();
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

function buildPeriods(yearsBack) {
  const thisYear = new Date().getFullYear();
  const periods = [];
  for (let y = thisYear - yearsBack + 1; y <= thisYear; y++) {
    for (const r of REPRT_CODES) periods.push({ year: y, ...r });
  }
  return periods;
}

async function fetchPeriodRow(corpCode, period, proxyUrl) {
  let fsDiv = "CFS";
  let dart = await fetchDart(corpCode, period.year, period.code, fsDiv, proxyUrl);
  if (dart.status !== "000") {
    fsDiv = "OFS";
    dart = await fetchDart(corpCode, period.year, period.code, fsDiv, proxyUrl);
  }

  const row = { period_label: `${period.year} ${period.label}`, fs_div: dart.status === "000" ? fsDiv : null };
  if (dart.status !== "000") {
    for (const item of ACCOUNT_ITEMS) row[item.key] = null;
  } else {
    for (const item of ACCOUNT_ITEMS) {
      row[item.key] = pickAccount(dart.list, item.ids, item.names);
    }
  }

  const capex = (row.capex_ppe != null || row.capex_intangible != null)
    ? Math.abs(row.capex_ppe || 0) + Math.abs(row.capex_intangible || 0)
    : null;
  row.capex = capex;
  row.fcf = row.ocf != null && capex != null ? row.ocf - capex : null;
  delete row.capex_ppe;
  delete row.capex_intangible;

  return row;
}

export default {
  async fetch(request, env) {
    const { pathname, searchParams } = new URL(request.url);

    if (pathname === "/") {
      return new Response(HTML_PAGE, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    if (pathname === "/api/financial-history") {
      const corpName = searchParams.get("corp_name");

      const corpRow = await env.DB
        .prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?")
        .bind(corpName)
        .first();

      if (!corpRow) {
        return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      }

      const periods = buildPeriods(YEARS_BACK);
      const rows = await mapWithConcurrency(periods, CONCURRENCY, (p) =>
        fetchPeriodRow(corpRow.corp_code, p, env.DART_PROXY_URL)
      );

      return Response.json({ corp_name: corpRow.corp_name, corp_code: corpRow.corp_code, rows });
    }

    return new Response("Not Found", { status: 404 });
  },
};
