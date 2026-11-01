// /api/market/caps: 시가총액이 기록된 가장 최근 날짜의 값만 돌려주고, 기록 없는 코드·0·잘못된 코드는 빠지며, D1 변수 한도(100)를 지키는지 실제 SQLite로 확인
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('    if (pathname === "/api/market/caps") {'), j = text.indexOf('    if (pathname === "/api/data/fresh") {', i);
if (i < 0 || j < 0) throw new Error('구간 없음');
const handler = new Function('env', 'searchParams', 'pathname', 'return (async () => {\n' + text.slice(i, j) + '\n return null; })()');
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, market_cap REAL, PRIMARY KEY (market_date, stock_code))');
const ins = db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?,?)');
ins.run('20261022', '000001', 100, 1e12); ins.run('20261023', '000001', 110, 1.1e12); ins.run('20261023', '000002', 50, 0); ins.run('20261023', '000004', 400, 4e12); ins.run('20261022', '000003', 300, 3e12);
ins.run('20261024', '000009', 10, null); // 최신 날짜지만 시가총액이 기록되지 않은 행 → 기준일이 되면 안 됨
let maxBinds = 0;
const DB = { prepare: (sql) => { const st = db.prepare(sql); return { first: async () => st.get() || null, bind: (...b) => (maxBinds = Math.max(maxBinds, b.length), { first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }) }) }; } };
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
(async () => {
  const run = async (codes) => { const r = await handler({ DB }, new URLSearchParams({ codes }), '/api/market/caps'); return r.json(); };
  const a = await run('000001,000002,000003,000004,abc,;;DROP');
  check('시가총액이 기록된 가장 최근 날짜(시총 없는 최신 행은 무시)', a.date === '20261023', JSON.stringify(a));
  check('그 날짜에 있는 종목만, 시가총액 0은 제외', JSON.stringify(Object.keys(a.caps).sort()) === JSON.stringify(['000001', '000004']), JSON.stringify(a.caps));
  check('값은 원 단위 그대로', a.caps['000001'].cap === 1.1e12 && a.caps['000001'].close === 110);
  check('전날에만 있는 종목(000003)은 빠짐', a.caps['000003'] === undefined);
  await run(Array.from({ length: 150 }, (_, k) => String(100000 + k)).join(','));
  check('D1 한도: 요청당 바인딩 변수 ≤ 100', maxBinds <= 100 && maxBinds > 1, 'max=' + maxBinds);
  const b = await run('zz,!!'); check('유효한 코드가 없으면 빈 결과', b.date === null && Object.keys(b.caps).length === 0);
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
