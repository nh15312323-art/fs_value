// DB 용량 관리 서버 로직 테스트 (node:sqlite 모의 D1)
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const code = [
  text.slice(text.indexOf('function addDaysStr('), text.indexOf('function addDaysStr(') + text.slice(text.indexOf('function addDaysStr(')).indexOf('\n}\n') + 3),
  between('let appSettingsReady', 'function universeCfgFrom('),
  between('function ymdOk(', 'function ymdDow('),
  between('function pfKeyOn(', 'async function authGate('),
  'const AUTH_MAX_FAILS = 8; async function authGate(request, env) { const k = request.headers.get("x-pf-key"); return k === env.PF_KEY ? null : Response.json({ error: "unauthorized" }, { status: 401 }); }',
  'async function adminAuthError(request, env) { if (!pfKeyOn(env)) return null; return authGate(request, env); }',
  between('// ===== DB 용량 관리 =====', '// ===== /DB 용량 관리 ====='),
  'return { handleDbCapacity, PURGE_CHUNK, PURGE_DAILY_ROW_CAP, PRICE_KEEP_DAYS, resetReady: () => { appSettingsReady = false; } };',
].join('\n');
const S = new Function(code)();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
function mkEnv(extra, size) {
  const db = new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE market_raw_daily (market_date TEXT NOT NULL, stock_code TEXT NOT NULL, close_price REAL, volume INTEGER, trading_value REAL, PRIMARY KEY (market_date, stock_code));
    CREATE TABLE market_universe_status (stock_code TEXT PRIMARY KEY, corp_name TEXT, passed INTEGER NOT NULL);
    CREATE TABLE screen_lists (snap_date TEXT NOT NULL, list_key TEXT NOT NULL, payload TEXT, PRIMARY KEY (snap_date, list_key));
    CREATE TABLE corp_master (corp_code TEXT PRIMARY KEY, corp_name TEXT);
    CREATE TABLE financial_raw (corp_code TEXT NOT NULL, bsns_year TEXT NOT NULL, reprt_code TEXT NOT NULL, PRIMARY KEY (corp_code, bsns_year, reprt_code));
    CREATE TABLE corp_fiscal (corp_code TEXT PRIMARY KEY, acc_mt INTEGER NOT NULL, checked_at TEXT);
    CREATE TABLE op_income_cache (stock_code TEXT PRIMARY KEY, op_profit REAL);`);
  const holder = { size: size == null ? 100 * 1048576 : size };
  const wrap = (st, b) => { const call = (m) => async () => { const v = b ? st[m](...b) : st[m](); return v; };
    return { first: async () => (b ? st.get(...b) : st.get()) || null, all: async () => ({ results: b ? st.all(...b) : st.all() }), run: async () => { const r = b ? st.run(...b) : st.run(); return { meta: { changes: r.changes, size_after: holder.size } }; } }; };
  const DB = { prepare: (sql) => { const st = db.prepare(sql); const w = wrap(st); w.bind = (...b) => wrap(st, b); return w; } };
  return Object.assign({ db, DB, holder }, extra || {});
}
const KEY = 'abcdefghijklmnop';
const rq = (p, body, key) => new Request('http://x' + p, { method: body ? 'POST' : 'GET', headers: Object.assign({ 'content-type': 'application/json' }, key ? { 'x-pf-key': key } : {}), body: body ? JSON.stringify(body) : undefined });
const call = async (env, p, body, key) => { const u = new URL('http://x' + p); const r = await S.handleDbCapacity(rq(p, body, key), env, u.pathname, u.searchParams); return { s: r.status, d: await r.json() }; };
(async () => {
  S.resetReady();
  let env = mkEnv({}, 460 * 1048576);
  let r = await call(env, '/api/db/usage?light=1');
  check('가벼운 사용량: 460/500MB → 92%, 경고', r.s === 200 && Math.round(r.d.pct) === 92 && r.d.warn === true && r.d.limitMb === 500);
  env.holder.size = 100 * 1048576;
  r = await call(env, '/api/db/usage?light=1');
  check('20% → 경고 없음', r.d.warn === false && Math.round(r.d.pct) === 20);
  env.holder.size = null; env.DB.prepare = ((orig) => (sql) => { const w = orig(sql); const run = w.run; w.run = async () => { const x = await run(); delete x.meta.size_after; return x; }; return w; })(env.DB.prepare);
  r = await call(env, '/api/db/usage?light=1');
  check('크기 모를 때 pct=null, 경고 없음(거짓 수치 없음)', r.d.pct === null && r.d.warn === false);

  // 설정
  S.resetReady(); env = mkEnv({});
  r = await call(env, '/api/db/settings', { limit_mb: 10, warn_pct: 90 });
  check('한도 10MB 거부', r.s === 400);
  r = await call(env, '/api/db/settings', { limit_mb: 500, warn_pct: 100 });
  check('알림 100% 거부', r.s === 400);
  r = await call(env, '/api/db/settings', { limit_mb: 1000, warn_pct: 80 });
  check('설정 저장 후 반영', r.s === 200 && r.d.limitMb === 1000 && r.d.warnPct === 80);
  r = await call(env, '/api/db/usage?light=1');
  check('저장된 설정 유지', r.d.limitMb === 1000 && r.d.warnPct === 80);

  // 전체 사용량 + 테이블
  env.db.exec("INSERT INTO market_raw_daily VALUES ('20250101','005930',1,1,1),('20250102','005930',1,1,1)");
  r = await call(env, '/api/db/usage');
  const t = r.d.tables.find((x) => x.name === 'market_raw_daily');
  check('테이블별 행 수', r.s === 200 && t && t.rows === 2 && Array.isArray(r.d.tables));
  check('삭제 보호 정보 포함', r.d.protected === false && r.d.priceKeepDays === 380 && r.d.purgeCap.left === 20000);

  // 삭제: PF_KEY 미설정이면 403
  r = await call(env, '/api/db/purge', { kind: 'opincome_cache', preview: true });
  check('PF_KEY 미설정 → 삭제 403', r.s === 403);

  // PF_KEY 설정 환경
  S.resetReady(); env = mkEnv({ PF_KEY: KEY });
  r = await call(env, '/api/db/purge', { kind: 'opincome_cache', preview: true });
  check('키 없이 삭제 401', r.s === 401);
  const P = (body) => call(env, '/api/db/purge', body, KEY);
  // 시세 채우기
  const ins = env.db.prepare('INSERT INTO market_raw_daily VALUES (?,?,1,1,1)');
  const days = []; for (let i = 0; i < 600; i++) { const dt = new Date(Date.UTC(2024, 0, 1) + i * 86400000); days.push(dt.toISOString().slice(0, 10).replace(/-/g, '')); }
  env.db.exec('BEGIN'); days.forEach((d) => { ins.run(d, 'A'); ins.run(d, 'B'); }); env.db.exec('COMMIT');
  const last = days[days.length - 1];
  r = await P({ kind: 'price_old', param: { before: last } });
  check('최근 380일 보호', r.s === 400 && /지울 수 없습니다/.test(r.d.error));
  r = await P({ kind: 'price_old', param: { before: '2024-01-05' } });
  check('날짜 형식 오류 거부', r.s === 400);
  r = await P({ kind: 'price_old', param: { before: '20240201' }, preview: true });
  check('미리보기: 31일×2종목=62행', r.s === 200 && r.d.count === 62 && r.d.estWrites === 124 && r.d.daysNeeded === 1);
  check('미리보기는 삭제하지 않음', env.db.prepare('SELECT COUNT(*) n FROM market_raw_daily').get().n === 1200);
  r = await P({ kind: 'price_old', param: { before: '20240201' } });
  check('실제 삭제 62행, done', r.s === 200 && r.d.deleted === 62 && r.d.done === true);
  check('남은 행 확인·최근 데이터 보존', env.db.prepare('SELECT COUNT(*) n FROM market_raw_daily').get().n === 1138 && env.db.prepare("SELECT COUNT(*) n FROM market_raw_daily WHERE market_date=?").get(last).n === 2);
  check('크기 반환', Number.isFinite(r.d.sizeAfter));

  // 비활성 종목
  r = await P({ kind: 'price_inactive' });
  check('통과 종목 목록이 비면 거부', r.s === 400);
  env.db.exec("INSERT INTO market_universe_status VALUES ('A','a',1),('B','b',0)");
  r = await P({ kind: 'price_inactive', preview: true });
  check('미통과 종목 B의 시세만 대상', r.d.count === 569, r.d.count);

  // 주간 결과
  env.db.exec("INSERT INTO screen_lists VALUES ('20250101','x','p'),('20250108','x','p'),('20250115','x','p'),('20250115','y','p')");
  r = await P({ kind: 'screen_old', param: { keep: 2 }, preview: true });
  check('최근 2회 빼고 1행', r.d.count === 1);
  r = await P({ kind: 'screen_old', param: { keep: 0 } });
  check('keep=0 거부', r.s === 400);
  r = await P({ kind: 'screen_old', param: { keep: 5 }, preview: true });
  check('keep 많으면 0행', r.d.count === 0);

  // 재무
  env.db.exec("INSERT INTO corp_master VALUES ('1','삼성'),('2','LG'); INSERT INTO financial_raw VALUES ('1','2015','11011'),('1','2020','11011'),('2','2015','11011'); INSERT INTO corp_fiscal VALUES ('1',12,'x')");
  r = await P({ kind: 'fin_company', param: { corp_name: '없는회사' } });
  check('없는 종목명 거부', r.s === 400);
  r = await P({ kind: 'fin_company', param: { corp_name: '삼성' } });
  check('종목 재무 삭제(2행) + 보조테이블 정리', r.d.deleted === 2 && env.db.prepare('SELECT COUNT(*) n FROM corp_fiscal').get().n === 0 && env.db.prepare('SELECT COUNT(*) n FROM financial_raw').get().n === 1);
  r = await P({ kind: 'fin_old', param: { before_year: 1999 } });
  check('연도 범위 거부', r.s === 400);
  r = await P({ kind: 'fin_old', param: { before_year: 2018 } });
  check('2018년 이전 삭제', r.d.deleted === 1 && r.d.done);
  r = await P({ kind: 'bogus' });
  check('알 수 없는 종류 거부', r.s === 400);
  env.db.exec("INSERT INTO op_income_cache VALUES ('A',1)");
  r = await P({ kind: 'opincome_cache' });
  check('캐시 삭제', r.d.deleted === 1);

  // 청크와 일일 한도
  S.resetReady(); env = mkEnv({ PF_KEY: KEY });
  env.db.exec('BEGIN'); const ins2 = env.db.prepare('INSERT INTO market_raw_daily VALUES (?,?,1,1,1)');
  for (let i = 0; i < 5000; i++) ins2.run('2020' + String(1 + (i % 12)).padStart(2, '0') + '01', 'S' + i);
  ins2.run('20260101', 'Z'); env.db.exec('COMMIT');
  r = await call(env, '/api/db/purge', { kind: 'price_old', param: { before: '20210101' } }, KEY);
  check('청크: 2000행씩, 아직 남음(done=false)', r.d.deleted === 2000 && r.d.done === false && r.d.capLeft === 18000);
  r = await call(env, '/api/db/purge', { kind: 'price_old', param: { before: '20210101' } }, KEY);
  r = await call(env, '/api/db/purge', { kind: 'price_old', param: { before: '20210101' } }, KEY);
  check('3번째 호출로 끝(done=true)', r.d.deleted === 1000 && r.d.done === true);
  // 한도 소진
  env.db.exec("INSERT OR REPLACE INTO market_job (k, v) VALUES ('purge_day', '" + JSON.stringify({ day: new Date().toISOString().slice(0, 10), rows: 20000 }) + "')");
  r = await call(env, '/api/db/purge', { kind: 'opincome_cache' }, KEY);
  check('일일 한도 소진 → 429', r.s === 429 && r.d.capLeft === 0);
  r = await call(env, '/api/db/usage', null, KEY);
  check('사용량 화면에 남은 한도 0', r.d.purgeCap.left === 0);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전체 통과'); process.exit(fail ? 1 : 0);
})();
