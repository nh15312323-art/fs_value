// /api/fin/last-price: 최신 날짜의 종가만 돌려주고, 시세 없는 코드는 빠지며, 잘못된 코드는 무시하는지 실제 SQLite로 확인
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const i = text.indexOf('    if (pathname === "/api/fin/last-price") {'), j = text.indexOf('    if (pathname === "/api/data/fresh") {', i);
if (i < 0 || j < 0) throw new Error('구간 없음');
const handler = new Function('env', 'searchParams', 'pathname', 'return (async () => {\n' + text.slice(i, j) + '\n return null; })()');
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE market_raw_daily (market_date TEXT, stock_code TEXT, close_price REAL, PRIMARY KEY (market_date, stock_code))');
const ins = db.prepare('INSERT INTO market_raw_daily VALUES (?,?,?)');
ins.run('20261022', '000001', 100); ins.run('20261023', '000001', 110); ins.run('20261023', '000002', 0); ins.run('20261023', '000004', 400); ins.run('20261022', '000003', 300);
const DB = { prepare: (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }) }), first: async () => st.get() || null }; } };
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
(async () => {
  const run = async (codes) => { const r = await handler({ DB }, new URLSearchParams({ codes }), '/api/fin/last-price'); return r.json(); };
  const a = await run('000001,000002,000003,000004,abc,;;DROP');
  check('가장 최근 날짜 반환', a.date === '20261023', JSON.stringify(a));
  check('최신 날짜에 있는 종목만, 종가 0은 제외', JSON.stringify(a.prices) === JSON.stringify({ '000001': 110, '000004': 400 }), JSON.stringify(a.prices));
  check('전날에만 있는 종목(000003)은 빠짐', a.prices['000003'] === undefined);
  const b = await run('zz,!!'); check('유효한 코드가 없으면 빈 결과', b.date === null && Object.keys(b.prices).length === 0);
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
