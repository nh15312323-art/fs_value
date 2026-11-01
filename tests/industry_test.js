// 업종 저장: parseIndutyCode, industryLookup, industryFill을 실제 SQLite + 가짜 DART 응답으로 확인
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const code = [
  'let fiscalTableReady = false;',
  between('async function ensureFiscalTable(', 'const fiscalSkipDates'),
  between('// ===== 업종 저장', '// ===== /업종 저장 ====='),
  'return { parseIndutyCode, industryLookup, industryFill, setDart: (f) => { fetchDartCompany = f; } };',
].join('\n').replace('let fiscalTableReady = false;', 'let fiscalTableReady = false; let fetchDartCompany = async () => null;');
const M = new Function(code)();
const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE corp_master (corp_code TEXT PRIMARY KEY, corp_name TEXT, stock_code TEXT);');
let maxBinds = 0;
const wrap = (sql, st) => ({ bind: (...b) => { maxBinds = Math.max(maxBinds, b.length); return { first: async () => st.get(...b) || null, all: async () => ({ results: st.all(...b) }), run: async () => st.run(...b) }; }, run: async () => st.run() });
const env = { DB: { prepare: (sql) => wrap(sql, db.prepare(sql)) }, DART_PROXY_URL: 'http://proxy' };
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
(async () => {
  const P = M.parseIndutyCode;
  check('정상 응답 → 숫자 코드', P({ status: '000', induty_code: '26110' }) === '26110');
  check('숫자 아닌 문자 제거', P({ status: '000', induty_code: ' 264-1 ' }) === '2641');
  check('필드 없음 → null (값을 만들지 않음)', P({ status: '000' }) === null);
  check('빈 값 → null', P({ status: '000', induty_code: '' }) === null);
  check('1자리 → null', P({ status: '000', induty_code: '2' }) === null);
  check('오류 상태 → null', P({ status: '013', induty_code: '26110' }) === null);
  check('응답 없음 → null', P(null) === null);
  [['A', '가', '000001'], ['B', '나', '000002'], ['C', '다', '000003'], ['D', '라', '000004'], ['E', '마', '000005']].forEach((r) => db.prepare('INSERT INTO corp_master VALUES (?,?,?)').run(...r));
  const calls = [];
  M.setDart(async (cc) => { calls.push(cc); if (cc === 'A') return { status: '000', induty_code: '26110', acc_mt: '12' }; if (cc === 'B') return { status: '000', induty_code: '20119', acc_mt: '3' }; if (cc === 'C') return { status: '000', message: 'x' }; throw new Error('timeout'); });
  let r = await M.industryFill(env, ['000001', '000002', '000003', '000004', '000099']);
  check('요청당 3종목만 처리(한도 보호)', calls.length === 3 && calls.join() === 'A,B,C', calls.join());
  check('성공 2건 저장, 실패 1건 사유 기록', r.saved['000001'] === '26110' && r.saved['000002'] === '20119' && /업종코드 없음/.test(r.failed['000003'] || ''), JSON.stringify(r));
  r = await M.industryFill(env, ['000004', '000099']);
  check('예외(타임아웃)·corp_master 없음은 실패로만 기록', /timeout/.test(r.failed['000004']) && /corp_master/.test(r.failed['000099']) && Object.keys(r.saved).length === 0, JSON.stringify(r.failed));
  const fiscal = db.prepare('SELECT corp_code, acc_mt FROM corp_fiscal ORDER BY corp_code').all().map((x) => x.corp_code + x.acc_mt).join();
  check('같은 응답의 결산월도 저장(A=12, B=3)', fiscal === 'A12,B3', fiscal);
  const m = await M.industryLookup(env, ['000001', '000002', '000003', '000005']);
  check('조회: 저장된 것만 반환, 없는 종목은 키 없음', m['000001'] === '26110' && m['000002'] === '20119' && !('000003' in m) && !('000005' in m), JSON.stringify(m));
  await M.industryFill(env, ['000001']);
  const n = db.prepare('SELECT COUNT(*) c FROM corp_industry').get().c;
  check('재실행해도 중복 행 없음(멱등)', n === 2, String(n));
  maxBinds = 0; await M.industryLookup(env, Array.from({ length: 80 }, (_, i) => String(100000 + i)));
  check('조회 80개일 때 바인딩 ≤100', maxBinds === 80 && maxBinds <= 100, String(maxBinds));
  console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
})();
