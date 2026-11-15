// /api/fin/market-snap: 전 종목 시세 요약(종가·52주 고저·거래대금·시가총액)을 실제 SQLite로 확인한다.
// 확인: 52주 구간(364일) 밖의 옛 값은 제외, 이력 부족·가격 불연속 표시, 거래정지(마지막 거래일이 다름), 20일 평균 거래대금, 같은 기준일 캐시, ?fresh=1 재계산, 새 기준일이면 재계산.
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('    if (pathname === "/api/fin/market-snap") {'), j = text.indexOf('    // 지주회사 NAV 계산기용', i);
if (i < 0 || j < 0) throw new Error('구간 없음');
const handler = new Function('env', 'searchParams', 'pathname', 'ensureMarketQueueColumns', 'addDaysStr', 'SCREEN_JUMP_LIMIT', 'return (async () => {\n' + text.slice(i, j) + '\n return null; })()');
const addDaysStr = (s, n) => { const d = new Date(Date.UTC(+s.slice(0, 4), +s.slice(4, 6) - 1, +s.slice(6, 8) + n)); return d.toISOString().slice(0, 10).replace(/-/g, ''); };
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE market_raw_daily (market_date TEXT NOT NULL, stock_code TEXT NOT NULL, close_price REAL, volume INTEGER, trading_value REAL, market_cap REAL, PRIMARY KEY (market_date, stock_code))');
db.exec('CREATE TABLE market_job (k TEXT PRIMARY KEY, v TEXT)');
let queries = 0;
const DB = { prepare: (sql) => { const st = db.prepare(sql); const mk = (b) => ({ first: async () => (queries++, st.get(...b) || null), all: async () => (queries++, { results: st.all(...b) }), run: async () => st.run(...b) }); return { bind: (...b) => mk(b), ...mk([]) }; } };
const ensure = async () => {};
const D0 = '20261113';
const days = []; for (let k = 0; k < 520; k++) { const d = addDaysStr(D0, -k); const wd = new Date(Date.UTC(+d.slice(0, 4), +d.slice(4, 6) - 1, +d.slice(6, 8))).getUTCDay(); if (wd % 6) days.push(d); } days.reverse(); // 오름차순 평일
const ins = db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?,?,?,?)');
const start = addDaysStr(D0, -364);
// A의 가격은 삼각파(하루 변동이 작아 불연속이 아님)
const rowsA = []; // A: 전 기간. 구간 밖(start 이전)에는 아주 높은 값 9999를 넣어 제외되는지 본다.
days.forEach((d, k) => { const cp = d < start ? 9999 : 1000 + (k % 50 < 25 ? k % 50 : 50 - (k % 50)) * 10; rowsA.push({ d, cp, tv: 1e9 + k * 1e6, vol: 1000 + k }); ins.run(d, 'AAAAAA', cp, 1000 + k, 1e9 + k * 1e6, d === D0 ? 5e12 : null); });
days.filter((d) => d >= addDaysStr(D0, -70)).forEach((d, k) => ins.run(d, 'BBBBBB', 500 + k, 10, 1e8, null)); // B: 약 50거래일뿐
days.filter((d) => d >= start).forEach((d, k) => ins.run(d, 'CCCCCC', k === 100 ? 1500 : 1000, 10, 1e8, null)); // C: 1000→1500(+50%)→1000 급변
const dHalt = days[days.length - 6]; days.filter((d) => d >= start && d <= dHalt).forEach((d) => ins.run(d, 'DDDDDD', 2000, 10, 1e8, null)); // D: 5거래일 전부터 거래 없음
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
const call = async (qs) => (await handler({ DB }, new URLSearchParams(qs || ''), '/api/fin/market-snap', ensure, addDaysStr, 0.3)).json();
(async () => {
  let a = await call();
  check('시세 기준일 = 가장 최근 거래일', a.date === D0, a.date);
  const inWin = rowsA.filter((r) => r.d >= start), A = a.rows.AAAAAA;
  check('A 종가·52주 고저(구간 밖 9999 제외)', A && A[1] === inWin[inWin.length - 1].cp && A[2] === Math.max(...inWin.map((r) => r.cp)) && A[3] === Math.min(...inWin.map((r) => r.cp)), JSON.stringify(A));
  check('A 거래일 수·거래량·거래대금·시가총액', A[4] === inWin.length && A[5] === rowsA[rowsA.length - 1].vol && A[6] === rowsA[rowsA.length - 1].tv && A[8] === 5e12, JSON.stringify(A));
  const last20 = rowsA.slice(-20), avg = Math.round(last20.reduce((s, r) => s + r.tv, 0) / 20);
  check('A 20거래일 평균 거래대금', A[7] === avg, A[7] + ' vs ' + avg);
  check('A 마지막 거래일 = 기준일, 불연속 아님', A[0] === D0 && A[9] === 0);
  check('B 이력 부족(거래일 수가 적게 나옴) — 화면에서 52주 N/A 처리', a.rows.BBBBBB && a.rows.BBBBBB[4] < 200 && a.rows.BBBBBB[4] >= 45, JSON.stringify(a.rows.BBBBBB));
  check('C 하루 +50% 급변 → 불연속 의심 1', a.rows.CCCCCC && a.rows.CCCCCC[9] === 1);
  check('D 거래정지: 마지막 거래일이 기준일과 다름', a.rows.DDDDDD && a.rows.DDDDDD[0] === dHalt && dHalt !== D0, a.rows.DDDDDD && a.rows.DDDDDD[0]);
  check('시가총액이 없으면 null(만들지 않음)', a.rows.BBBBBB[8] === null);
  check('첫 호출은 캐시 아님·종목 수 4', !a.cached && a.n === 4);
  const saved = db.prepare("SELECT v FROM market_job WHERE k = 'fin_snap'").get();
  check('market_job에 한 줄 저장', saved && JSON.parse(saved.v).date === D0);
  // 캐시: 원천 행을 지워도 같은 기준일이면 저장된 요약을 돌려준다(원천 재조회 없음)
  db.exec('DELETE FROM market_raw_daily WHERE market_date < \'' + D0 + '\'');
  const q0 = queries; let b = await call();
  check('같은 기준일 재호출은 캐시(cached=true), 값 그대로', b.cached === true && b.rows.AAAAAA[2] === A[2] && queries - q0 === 2, 'queries+' + (queries - q0));
  b = await call('fresh=1');
  check('fresh=1이면 다시 계산(지운 원천 반영)', !b.cached && b.rows.AAAAAA && b.rows.AAAAAA[4] === 1);
  // 새 기준일이 생기면 캐시 무효
  db.exec("INSERT INTO market_raw_daily VALUES ('20261116','AAAAAA',1234,1,1,null)");
  b = await call();
  check('새 시세 기준일이 생기면 자동 재계산', b.date === '20261116' && !b.cached && b.rows.AAAAAA[1] === 1234);
  db.exec('DELETE FROM market_raw_daily');
  b = await call('fresh=1');
  check('시세가 하나도 없으면 빈 결과', b.date === null && b.n === 0);
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
