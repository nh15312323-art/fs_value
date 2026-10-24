// 새 공시 자동 반영: 대상 기간 계산, 대상 고르기, 큐 등록·재시도 제한을 실제 SQLite로 확인
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const code = [
  between('const REPRT_CODES = [', '];') + '];',
  'let fiscalTableReady = false;',
  between('async function ensureFiscalTable(', 'function parseAccMt('),
  between('// ===== 새 공시 자동 반영 =====', '// ===== /새 공시 자동 반영 ====='),
  'return { filingPeriodsFor, pickFilingTargets, runFilingAutoUpdate };',
].join('\n');
const M = new Function(code)();
function mkEnv() {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE financial_raw (corp_code TEXT, bsns_year TEXT, reprt_code TEXT, revenue REAL, net_income REAL, operating_income REAL, PRIMARY KEY (corp_code, bsns_year, reprt_code));
    CREATE TABLE corp_master (corp_code TEXT PRIMARY KEY, corp_name TEXT, stock_code TEXT);
    CREATE TABLE fetch_queue (corp_code TEXT, corp_name TEXT, stock_code TEXT, bsns_year TEXT, reprt_code TEXT, period_order INTEGER, queued_at TEXT, PRIMARY KEY (corp_code, bsns_year, reprt_code));`);
  const wrap = (st) => ({ bind: (...b) => ({ first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }), run: async () => st.run(...b), _x: () => st.run(...b) }), first: async () => st.get() || null, all: async () => ({ results: st.all() }), run: async () => st.run() });
  const DB = { prepare: (sql) => wrap(db.prepare(sql)), batch: async (arr) => { arr.forEach((a) => a._x()); return []; } };
  return { db, DB };
}
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
(async () => {
  const lab = (d) => M.filingPeriodsFor(d).map((p) => p.label).join();
  check('시즌 사이(10/25)에는 대상 기간 없음', lab('20261025') === '', lab('20261025'));
  check('9/20 → 2026 반기', lab('20260920') === '2026 반기', lab('20260920'));
  check('11/20 → 2026 3분기', lab('20261120') === '2026 3분기', lab('20261120'));
  check('4/10 → 2025 사업보고서 (1Q는 아직 30일 안 지남)', lab('20260410') === '2025 사업보고서', lab('20260410'));
  check('5/05 → 2025 사업보고서 + 2026 1분기', lab('20260505') === '2025 사업보고서,2026 1분기', lab('20260505'));
  check('기한+60일 지난 5/31엔 2025 사업보고서 종료', lab('20260531') === '2026 1분기', lab('20260531'));

  const env = mkEnv(), db = env.db;
  const co = (c, n, s) => db.prepare('INSERT INTO corp_master VALUES (?,?,?)').run(c, n, s);
  const fr = (c, y, r, rev) => db.prepare('INSERT INTO financial_raw VALUES (?,?,?,?,?,?)').run(c, y, r, rev, rev, rev);
  ['A', 'B', 'C', 'D', 'E', 'F'].forEach((c, i) => co(c, c + '사', '00000' + (i + 1)));
  // A: 2026 반기까지 있음 → 대상 아님
  fr('A', '2025', '11011', 100); fr('A', '2026', '11012', 100);
  // B: 2026 반기 행은 있으나 값이 비어 있음(공시 전 저장) → 대상
  fr('B', '2025', '11011', 100); db.prepare('INSERT INTO financial_raw VALUES (?,?,?,NULL,NULL,NULL)').run('B', '2026', '11012');
  // C: 2026 반기 행 없음 → 대상
  fr('C', '2025', '11011', 100);
  // D: 3월 결산 → 제외
  fr('D', '2025', '11011', 100); db.exec("CREATE TABLE IF NOT EXISTS corp_fiscal (corp_code TEXT PRIMARY KEY, acc_mt INTEGER NOT NULL, checked_at TEXT)"); db.prepare('INSERT INTO corp_fiscal VALUES (?,?,?)').run('D', 3, 'x');
  // E: 2019년 이후 자료 없음 → 제외
  fr('E', '2019', '11011', 100);
  // F: 이미 큐에 있음 → 제외
  fr('F', '2025', '11011', 100); db.prepare("INSERT INTO fetch_queue VALUES ('F','F사','000006','2026','11012',20262,'x')").run();

  let log = await M.runFilingAutoUpdate(env, { dry: true, today: '20260920' });
  check('미리보기: B·C 두 건 찾고 아무것도 쓰지 않음', log.missing === 2 && log.queued === 0 && db.prepare('SELECT COUNT(*) n FROM fetch_queue').get().n === 1 && db.prepare("SELECT COUNT(*) n FROM sqlite_master WHERE name='market_job'").get().n === 1 && db.prepare('SELECT COUNT(*) n FROM fin_auto_try').get().n === 0, JSON.stringify(log));
  log = await M.runFilingAutoUpdate(env, { today: '20260920' });
  const q = db.prepare("SELECT corp_code, bsns_year, reprt_code, period_order FROM fetch_queue ORDER BY corp_code").all();
  check('실행: B·C가 2026 반기(11012)로 큐에 등록, F는 그대로', q.map((r) => r.corp_code).join() === 'B,C,F' && q.every((r) => r.bsns_year === '2026' && r.reprt_code === '11012' && r.period_order === 20262), JSON.stringify(q));
  check('시도 횟수 1회 기록, 실행 로그 저장', db.prepare('SELECT COUNT(*) n FROM fin_auto_try WHERE tries = 1').get().n === 2 && !!db.prepare("SELECT v FROM market_job WHERE k='fin_auto_log'").get());
  check('값이 있는 A의 기록은 건드리지 않음', db.prepare("SELECT revenue FROM financial_raw WHERE corp_code='A' AND bsns_year='2026'").get().revenue === 100);
  db.exec('DELETE FROM fetch_queue');   // 큐가 처리되어 비었다고 가정(공시가 아직 없어 값은 계속 빔)
  log = await M.runFilingAutoUpdate(env, { today: '20260922' });
  check('2일 뒤: B·C는 다시 시도하지 않고(3일 간격), 큐가 비어 처음 대상이 된 F만 등록', log.missing === 1 && log.queued === 1 && db.prepare("SELECT corp_code FROM fetch_queue").all().map((r) => r.corp_code).join() === 'F', JSON.stringify(log));
  log = await M.runFilingAutoUpdate(env, { today: '20260923' });
  check('3일 뒤에는 B·C 다시 등록(시도 2회). F는 간격 미충족', log.queued === 2 && db.prepare('SELECT MAX(tries) m FROM fin_auto_try').get().m === 2, JSON.stringify(log));
  db.exec('DELETE FROM fetch_queue; UPDATE fin_auto_try SET tries = 10');
  log = await M.runFilingAutoUpdate(env, { today: '20260930' });
  check('10번 시도하면 더 이상 등록하지 않음', log.missing === 0, JSON.stringify(log));
  db.prepare("UPDATE financial_raw SET revenue = 5, net_income = 5, operating_income = 5 WHERE corp_code = 'B' AND bsns_year = '2026'").run();
  db.exec('UPDATE fin_auto_try SET tries = 0, last_try = NULL');
  log = await M.runFilingAutoUpdate(env, { today: '20261001' });
  check('공시 값이 들어온 B는 대상에서 빠지고 C·F만 남음', log.missing === 2 && !db.prepare('SELECT corp_code FROM fetch_queue').all().some((r) => r.corp_code === 'B'), JSON.stringify(log));
  log = await M.runFilingAutoUpdate(env, { today: '20261025' });
  check('시즌 사이에는 아무것도 하지 않음', log.periods.length === 0 && log.missing === 0);
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})().catch((e) => { console.error(e); process.exit(1); });
