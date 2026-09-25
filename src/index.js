// ============================================================
// Phase 2: 웹(Cloudflare Worker)에서 DART 조회 + 파생계산이 되는지 검증
//
// 확인해야 할 것 (본인 저장소에 맞게):
//   1) wrangler.toml(또는 wrangler.jsonc)에 D1 바인딩이 있는지, 이름이 무엇인지
//        예: [[d1_databases]]
//            binding = "DB"                 <- 이 이름을 아래 env.DB 와 맞춰야 함
//            database_name = "market-value-db"
//            database_id = "..."
//   2) DART_API_KEY를 Cloudflare Worker 환경변수(Secret)로 등록
//        npx wrangler secret put DART_API_KEY
//
// 이 파일 하나만으로 동작하는 최소 예시입니다.
// 기존 저장소 구조(라우터 등)가 있다면 이 안의 fetch 핸들러 로직만
// 가져다 쓰시면 됩니다.
// ============================================================
// ============================================================
// Phase 2: 웹(Cloudflare Worker)에서 DART 조회 + 파생계산이 되는지 검증
//
// 확인해야 할 것 (본인 저장소에 맞게):
//   1) wrangler.toml(또는 wrangler.jsonc)에 D1 바인딩이 있는지, 이름이 무엇인지
//        예: [[d1_databases]]
//            binding = "DB"                 <- 이 이름을 아래 env.DB 와 맞춰야 함
//            database_name = "market-value-db"
//            database_id = "..."
//   2) DART_API_KEY를 Cloudflare Worker 환경변수(Secret)로 등록
//        npx wrangler secret put DART_API_KEY
//
// 이 파일 하나만으로 동작하는 최소 예시입니다.
// 기존 저장소 구조(라우터 등)가 있다면 이 안의 fetch 핸들러 로직만
// 가져다 쓰시면 됩니다.
// ============================================================

const HTML_PAGE = `<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>DART 조회 테스트</title>
  <style>
    body { font-family: sans-serif; max-width: 480px; margin: 40px auto; padding: 0 16px; }
    input, select, button { font-size: 16px; padding: 8px; margin: 4px 0; width: 100%; box-sizing: border-box; }
    table { width: 100%; border-collapse: collapse; margin-top: 16px; }
    td, th { border: 1px solid #ccc; padding: 8px; text-align: right; }
    th { text-align: left; background: #f5f5f5; }
    #status { color: #b00; margin-top: 8px; }
  </style>
</head>
<body>
  <h3>DART 조회 테스트 (2단계 검증용)</h3>
  <input id="corpName" placeholder="종목명 (예: 삼성전자)" value="삼성전자" />
  <select id="bsnsYear">
    <option value="2025">2025</option>
    <option value="2024">2024</option>
  </select>
  <select id="reprtCode">
    <option value="11013">1분기보고서</option>
    <option value="11012" selected>반기보고서</option>
    <option value="11014">3분기보고서</option>
    <option value="11011">사업보고서(연간)</option>
  </select>
  <button onclick="run()">조회</button>
  <div id="status"></div>
  <table id="result" style="display:none"></table>

  <script>
    async function run() {
      const corpName = document.getElementById('corpName').value.trim();
      const bsnsYear = document.getElementById('bsnsYear').value;
      const reprtCode = document.getElementById('reprtCode').value;
      const statusEl = document.getElementById('status');
      const tableEl = document.getElementById('result');
      statusEl.textContent = '조회 중...';
      tableEl.style.display = 'none';

      try {
        const res = await fetch(\`/api/financial?corp_name=\${encodeURIComponent(corpName)}&bsns_year=\${bsnsYear}&reprt_code=\${reprtCode}\`);
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || '조회 실패');

        statusEl.textContent = \`\${data.corp_name} (\${data.corp_code}) / \${data.fs_div} 기준\`;
        tableEl.innerHTML = \`
          <tr><th>항목</th><th>값</th></tr>
          <tr><td>매출액</td><td>\${data.revenue?.toLocaleString() ?? 'N/A'}</td></tr>
          <tr><td>영업이익</td><td>\${data.operating_income?.toLocaleString() ?? 'N/A'}</td></tr>
          <tr><td>당기순이익</td><td>\${data.net_income?.toLocaleString() ?? 'N/A'}</td></tr>
          <tr><td><b>영업이익률(파생)</b></td><td><b>\${data.operating_margin != null ? (data.operating_margin * 100).toFixed(2) + '%' : 'N/A'}</b></td></tr>
        \`;
        tableEl.style.display = 'table';
      } catch (e) {
        statusEl.textContent = '오류: ' + e.message;
      }
    }
  </script>
</body>
</html>`;

async function fetchDart(corpCode, bsnsYear, reprtCode, fsDiv, proxyUrl) {
  const url = new URL(proxyUrl); // Google Apps Script 웹앱 URL (DART_API_KEY는 Apps Script 쪽에만 저장)
  url.searchParams.set("corp_code", corpCode);
  url.searchParams.set("bsns_year", bsnsYear);
  url.searchParams.set("reprt_code", reprtCode);
  url.searchParams.set("fs_div", fsDiv);

  const resp = await fetch(url.toString());
  return resp.json();
}

function pickAccount(list, names) {
  // account_nm이 공시마다 조금씩 다를 수 있어 부분일치로 먼저 찾음
  // (10년치 본 구축 시엔 account_id 기반 매칭으로 고도화 예정)
  const item = list.find((row) => names.some((n) => row.account_nm?.includes(n)));
  if (!item) return null;
  const raw = item.thstrm_amount?.replace(/,/g, "");
  return raw ? Number(raw) : null;
}

export default {
  async fetch(request, env) {
    const { pathname, searchParams } = new URL(request.url);

    if (pathname === "/") {
      return new Response(HTML_PAGE, { headers: { "content-type": "text/html; charset=utf-8" } });
    }

    if (pathname === "/api/financial") {
      const corpName = searchParams.get("corp_name");
      const bsnsYear = searchParams.get("bsns_year");
      const reprtCode = searchParams.get("reprt_code");

      // 1) corp_code 조회 (D1) — env.DB는 본인 wrangler.toml의 바인딩 이름에 맞게 수정
      const row = await env.DB
        .prepare("SELECT corp_code, corp_name FROM corp_master WHERE corp_name = ?")
        .bind(corpName)
        .first();

      if (!row) {
        return Response.json({ error: `'${corpName}' 종목을 corp_master에서 찾을 수 없습니다.` }, { status: 404 });
      }

      // 2) DART 호출(Apps Script 중계기 경유): 연결(CFS) 먼저, 없으면 개별(OFS)로 폴백
      let fsDiv = "CFS";
      let dart = await fetchDart(row.corp_code, bsnsYear, reprtCode, fsDiv, env.DART_PROXY_URL);
      if (dart.status !== "000") {
        fsDiv = "OFS";
        dart = await fetchDart(row.corp_code, bsnsYear, reprtCode, fsDiv, env.DART_PROXY_URL);
      }
      if (dart.status !== "000") {
        return Response.json({ error: `DART 조회 실패 (status=${dart.status}, message=${dart.message})` }, { status: 502 });
      }

      // 3) 필요한 항목만 추출 + 파생값(영업이익률) 계산
      const revenue = pickAccount(dart.list, ["매출액", "수익(매출액)"]);
      const operatingIncome = pickAccount(dart.list, ["영업이익"]);
      const netIncome = pickAccount(dart.list, ["당기순이익"]);
      const operatingMargin = revenue && operatingIncome != null ? operatingIncome / revenue : null;

      return Response.json({
        corp_name: row.corp_name,
        corp_code: row.corp_code,
        fs_div: fsDiv,
        revenue,
        operating_income: operatingIncome,
        net_income: netIncome,
        operating_margin: operatingMargin,
      });
    }

    return new Response("Not Found", { status: 404 });
  },
};
