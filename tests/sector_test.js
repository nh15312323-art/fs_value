// 섹터 유동성 서버 로직 테스트 (node:sqlite 모의 D1, DART 프록시는 fetch 모의)
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const code = [
  between('let appSettingsReady', 'function universeCfgFrom('),
  between('async function fetchDartCompany(', 'async function getAccMt('),
  between('// ===== 섹터 유동성 =====', '// ===== /섹터 유동성 ====='),
  'return { ksicSector, sectorFlowCompute, handleSector, processSectorTick, lookupAndSaveSector, reset: () => { appSettingsReady = false; sectorTableReady = false; sectorIdleDay = null; } };',
].join('\n');
const S = new Function(code)();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const near = (a, b, t) => Math.abs(a - b) <= (t || 1e-9);
// --- 코드 해석
check('KSIC 5자리 → 중분류 26 전자부품…', S.ksicSector('26429').div === '26' && /전자부품/.test(S.ksicSector('26429').name));
check('3자리·2자리 허용', S.ksicSector('211').div === '21' && S.ksicSector('64').name === '금융');
check('잘못된 코드(빈값·문자·1자리)는 null', S.ksicSector('') === null && S.ksicSector('AB1') === null && S.ksicSector('2') === null && S.ksicSector(null) === null);
check('모르는 중분류는 기타(NN)', S.ksicSector('99999').name === '기타(99)');
// --- 순수 계산: 85거래일, A(3종목) 최근 20일 거래대금 3배, B(3종목) 불변, C(1종목) 소표본
const dates = []; for (let k = 0; k < 85; k++) dates.push(String(20240101 + k));
const rows = [], secOf = {};
const add = (code, sec, grow, tvBase, drift) => { secOf[code] = sec; dates.forEach((d, k) => { const rec = k >= 65; rows.push({ d, code, c: 1000 * Math.pow(1 + drift, k), tv: tvBase * (rec ? grow : 1) }); }); };
const A = { div: '26', name: '전자부품·컴퓨터·통신장비' }, B = { div: '10', name: '식료품' }, C = { div: '64', name: '금융' };
['a1', 'a2', 'a3', 'a4', 'a5'].forEach((c) => add(c, A, 3, 100, 0.004));
['b1', 'b2', 'b3', 'b4', 'b5'].forEach((c) => add(c, B, 1, 100, 0.0));
add('c1', C, 1, 100, -0.002);
const r = S.sectorFlowCompute(rows, secOf);
const a = r.sectors.find((x) => x.div === '26'), b = r.sectors.find((x) => x.div === '10'), c = r.sectors.find((x) => x.div === '64');
// 비중: 최근 20일 A=5*300, B=5*100, C=100 → 1500/2100 ; 직전 60일 A=500, B=500, C=100 → 500/1100
check('최근 거래대금 비중 계산', near(a.share, 1500 / 2100, 1e-12) && near(b.share, 500 / 2100, 1e-12));
check('직전 60일 비중 계산·변화(pp)', near(a.sharePrev, 500 / 1100, 1e-12) && near(a.dPP, (1500 / 2100 - 500 / 1100) * 100, 1e-9));
check('상대 유입(share/prev−1): A 양수, B 음수', a.relFlow > 0.5 && b.relFlow < 0, a.relFlow.toFixed(3) + ' ' + b.relFlow.toFixed(3));
check('종목 수·소표본 표시(5개 미만은 small)', a.n === 5 && b.n === 5 && c.n === 1 && c.small === true && a.small === false);
check('수익률: A는 20일 상승·상대강도>0, B는 0%, 비교기준은 전체 중앙값', a.ret20 > 0 && near(b.ret20, 0, 1e-12) && a.rs20 > 0 && near(r.base20, b.ret20, 1e-9) );
check('60일 수익률/상대강도도 계산', a.ret60 > 0 && a.rs60 > 0);
check('거래대금 비중 합계 = 100%', near(r.sectors.reduce((s, x) => s + x.share, 0), 1, 1e-12));
check('비중 큰 순 정렬', r.sectors[0].div === '26');
// 거래일이 적으면 오류, 직전 구간이 20일 미만이면 비중 변화는 null
check('거래일 25일 미만 → 오류(숫자 안 만듦)', !!S.sectorFlowCompute(rows.filter((x) => x.d >= dates[70]), secOf).error);
const r2 = S.sectorFlowCompute(rows.filter((x) => x.d >= dates[50]), secOf);
check('직전 구간 부족 → 비중 변화·상대유입 null', r2.hasPrior === false && r2.sectors.every((x) => x.dPP === null && x.relFlow === null));
// 업종 미분류 종목은 제외, 최근 거래 없는 종목 제외
const rows3 = rows.concat(dates.map((d) => ({ d, code: 'zz', c: 5, tv: 1e9 })));
check('업종 모르는 종목은 비중 계산에서 제외', near(S.sectorFlowCompute(rows3, secOf).sectors.find((x) => x.div === '26').share, a.share, 1e-12));
const rows4 = rows.map((x) => x.code === 'b1' && x.d >= dates[65] ? Object.assign({}, x, { tv: null }) : x);
check('최근 20일 거래대금이 전부 없는 종목은 종목 수에서 제외', S.sectorFlowCompute(rows4, secOf).sectors.find((x) => x.div === '10').n === 4);
// --- 핸들러/크론
function mkEnv() {
  const db = new DatabaseSync(':memory:');
  db.exec('CREATE TABLE corp_master (corp_code TEXT PRIMARY KEY, corp_name TEXT, stock_code TEXT); CREATE TABLE market_universe_status (stock_code TEXT PRIMARY KEY, corp_name TEXT, passed INTEGER NOT NULL); CREATE TABLE market_raw_daily (market_date TEXT NOT NULL, stock_code TEXT NOT NULL, close_price REAL, volume INTEGER, trading_value REAL, PRIMARY KEY (market_date, stock_code));');
  const wrap = (st, b) => ({ first: async () => (b ? st.get(...b) : st.get()) || null, all: async () => ({ results: b ? st.all(...b) : st.all() }), run: async () => { const x = b ? st.run(...b) : st.run(); return { meta: { changes: x.changes } }; } });
  const DB = { prepare: (sql) => { const st = db.prepare(sql); const w = wrap(st); w.bind = (...b) => wrap(st, b); return w; } };
  return { db, DB, DART_PROXY_URL: 'https://proxy.example/exec' };
}
let dartCodes = {}; let dartCalls = 0;
globalThis.fetch = async (url) => { dartCalls++; const cc = new URL(url).searchParams.get('corp_code'); const v = dartCodes[cc]; return { json: async () => (v === undefined ? { status: '013', message: '조회된 데이타가 없습니다.' } : { status: '000', induty_code: v, acc_mt: '12' }) }; };
(async () => {
  S.reset(); const env = mkEnv();
  // 8개 종목: 5개는 전자(26), 3개는 식료품(10) 중 일부 코드 없음
  const stocks = [['c1', 's1', '삼성', '26429'], ['c2', 's2', '종목2', '26110'], ['c3', 's3', '종목3', '26299'], ['c4', 's4', '종목4', '26410'], ['c5', 's5', '종목5', '26519'], ['c6', 's6', '식품1', '10711'], ['c7', 's7', '식품2', '10501'], ['c8', 's8', '무코드', undefined]];
  stocks.forEach((x) => { env.db.exec("INSERT INTO corp_master VALUES ('" + x[0] + "','" + x[2] + "','" + x[1] + "'); INSERT INTO market_universe_status VALUES ('" + x[1] + "','" + x[2] + "',1)"); if (x[3] !== undefined) dartCodes[x[0]] = x[3]; });
  env.db.exec('BEGIN'); const ins = env.db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?,?,?)');
  dates.forEach((d, k) => stocks.forEach((x, i) => ins.run(d, x[1], 1000 * (1 + 0.003 * k * (i < 5 ? 1 : 0)), 10, (i < 5 ? 200 : 100) * (k >= 65 && i < 5 ? 2 : 1))));
  env.db.exec('COMMIT');
  let t = await S.processSectorTick(env);
  check('크론 틱: 한 번에 3개만 조회·저장', t.processed === 3 && t.saved === 3 && dartCalls === 3, JSON.stringify(t));
  t = await S.processSectorTick(env); t = await S.processSectorTick(env);
  const nRows = env.db.prepare('SELECT COUNT(*) n FROM corp_sector').get().n;
  check('3번 틱 후 8개 모두 시도(코드 없는 1개는 NULL 기록)', nRows === 8 && env.db.prepare('SELECT COUNT(*) n FROM corp_sector WHERE induty_code IS NULL').get().n === 1, String(nRows));
  const calls0 = dartCalls; t = await S.processSectorTick(env);
  check('끝나면 idle, 실패 건은 하루 지나기 전엔 재시도 안 함(무한 두드림 방지)', t.done === true && dartCalls === calls0);
  t = await S.processSectorTick(env);
  check('같은 날 재호출은 DB를 읽지 않고 건너뜀', t.skipped === 'idle');
  // 종목별 조회
  let res = await S.handleSector(new Request('http://x/api/sector/stock?corp_name=' + encodeURIComponent('삼성')), env, '/api/sector/stock', new URL('http://x/api/sector/stock?corp_name=' + encodeURIComponent('삼성')).searchParams);
  let j = await res.json();
  check('종목 업종: 전자부품·컴퓨터·통신장비(26)', res.status === 200 && j.div === '26' && j.stats && j.stats.n === 5, JSON.stringify(j).slice(0, 160));
  check('업종 통계: 최근 거래대금 비중·상대유입 양수(2배↑)', j.stats.relFlow > 0 && j.stats.share > 0.5 && j.flowRank >= 1);
  check('흐름 계산이 app_settings에 캐시됨', env.db.prepare("SELECT COUNT(*) n FROM app_settings WHERE k = 'sector_flow'").get().n === 1);
  const q0 = dartCalls;
  res = await S.handleSector(new Request('http://x/'), env, '/api/sector/flow', new URL('http://x/api/sector/flow').searchParams); j = await res.json();
  check('flow API: 캐시 사용(cached=true), 매핑/유니버스 개수 보고', j.cached === true && j.mappedN === 7 && j.universeN === 8, 'mapped=' + j.mappedN + '/' + j.universeN);
  res = await S.handleSector(new Request('http://x/'), env, '/api/sector/flow', new URL('http://x/api/sector/flow?refresh=1').searchParams); j = await res.json();
  check('refresh=1은 다시 계산', j.cached === false && j.sectors.length === 2);
  // 업종 미확인 종목: 조회 후에도 코드 없으면 sector null + 안내
  res = await S.handleSector(new Request('http://x/'), env, '/api/sector/stock', new URL('http://x/api/sector/stock?corp_name=' + encodeURIComponent('무코드')).searchParams); j = await res.json();
  check('업종코드 없는 종목 → sector=null(추정하지 않음)', res.status === 200 && j.sector === null);
  res = await S.handleSector(new Request('http://x/'), env, '/api/sector/stock', new URL('http://x/api/sector/stock?corp_name=' + encodeURIComponent('없는회사')).searchParams);
  check('없는 종목명 404', res.status === 404);
  // 시세 부족
  S.reset(); const env2 = mkEnv(); env2.db.exec("INSERT INTO corp_master VALUES ('c1','가','s1'); INSERT INTO market_universe_status VALUES ('s1','가',1)");
  res = await S.handleSector(new Request('http://x/'), env2, '/api/sector/flow', new URL('http://x/api/sector/flow').searchParams); j = await res.json();
  check('저장된 시세가 없으면 404와 안내', res.status === 404 && /시세/.test(j.error));
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과'); process.exit(fail ? 1 : 0);
})();
