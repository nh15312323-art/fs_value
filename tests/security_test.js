// 보안 계층 테스트: 응답 헤더, 접근 키 시도 제한, PF_KEY 설정 시 /api 전체 보호, 상태 확인 API
const fs = require('fs'), path = require('path');
const { DatabaseSync } = require('node:sqlite');
const text = fs.readFileSync(path.join(__dirname, '..', 'src', 'index.js'), 'utf8');
const between = (a, b) => { const i = text.indexOf(a), j = text.indexOf(b, i); if (i < 0 || j < 0) throw new Error('구간 없음: ' + a); return text.slice(i, j); };
const code = [
  between('function pfAuthError(', 'const PF_STATUSES'),
  between('const APP_ICON_B64', 'export default'),
  // handleRequest의 앞부분(라우팅 앞단)만 흉내: 실제 소스의 보호 구간을 그대로 사용
  'async function front(request, env) {',
  '  const { pathname, searchParams } = new URL(request.url);',
  between('    if (pathname === "/manifest.webmanifest")', '    if (pathname === "/") {').replace('return handlePf(request, env, pathname, searchParams)', 'return Response.json({ pf: true })'),
  '  return Response.json({ reached: pathname });',
  '}',
  'return { front, withSecurityHeaders, authGate, AUTH_MAX_FAILS };',
].join('\n');
const S = new Function(code)();
let fail = 0; const check = (n, ok, d) => { if (!ok) fail++; console.log((ok ? 'PASS' : 'FAIL') + '  ' + n + (d !== undefined ? '  ' + d : '')); };
function mkEnv(extra) {
  const db = new DatabaseSync(':memory:');
  const DB = { prepare: (sql) => { const st = db.prepare(sql); return { bind: (...b) => ({ first: async () => st.get(...b) || null, run: async () => { const r = st.run(...b); return { meta: { changes: r.changes } }; } }), run: async () => { const r = st.run(); return { meta: { changes: r.changes } }; } }; } };
  return Object.assign({ db, DB }, extra || {});
}
const KEY = 'abcdefghijklmnop';
const rq = (p, key, ip) => new Request('http://x' + p, { headers: Object.assign({}, key ? { 'x-pf-key': key } : {}, ip ? { 'cf-connecting-ip': ip } : {}) });
(async () => {
  // 1) 헤더
  const h = S.withSecurityHeaders(Response.json({ a: 1 }), '/api/x');
  check('API 응답은 no-store', h.headers.get('cache-control') === 'no-store');
  check('CSP: 외부 스크립트 차단·프레임 금지', /default-src 'self'/.test(h.headers.get('content-security-policy')) && /frame-ancestors 'none'/.test(h.headers.get('content-security-policy')) && !/https?:/.test(h.headers.get('content-security-policy')));
  check('nosniff · DENY · no-referrer', h.headers.get('x-content-type-options') === 'nosniff' && h.headers.get('x-frame-options') === 'DENY' && h.headers.get('referrer-policy') === 'no-referrer');
  check('HSTS', /max-age=\d+/.test(h.headers.get('strict-transport-security')));
  check('본문·상태 보존', (await h.json()).a === 1 && h.status === 200);
  const h2 = S.withSecurityHeaders(new Response('x', { status: 404 }), '/zzz');
  check('HTML 아닌 경로도 헤더, 상태 404 보존', h2.status === 404 && h2.headers.get('x-content-type-options') === 'nosniff');
  const ph = S.withSecurityHeaders(new Response('<html>', { headers: { 'content-type': 'text/html' } }), '/');
  check('메인 페이지는 no-cache', ph.headers.get('cache-control') === 'no-cache');

  // 2) PF_KEY 미설정이면 기존처럼 열림
  let env = mkEnv();
  let r = await S.front(rq('/api/queue-fetch'), env); let d = await r.json();
  check('PF_KEY 미설정: /api 열림', d.reached === '/api/queue-fetch');
  r = await S.front(rq('/api/auth/status'), env); d = await r.json();
  check('상태: 보호 안 됨', d.protected === false && d.ok === true);

  // 3) PF_KEY 설정: 전체 보호
  env = mkEnv({ PF_KEY: KEY });
  r = await S.front(rq('/api/screen/top'), env);
  check('PF_KEY 설정: 키 없이 일반 /api 401', r.status === 401);
  r = await S.front(rq('/api/fetch-and-save', 'wrong-key-123456', '1.1.1.1'), env);
  check('틀린 키 401', r.status === 401);
  r = await S.front(rq('/api/screen/top', KEY, '2.2.2.2'), env); d = await r.json();
  check('올바른 키 통과', r.status === 200 && d.reached === '/api/screen/top');
  r = await S.front(rq('/manifest.webmanifest'), env); d = await r.json();
  check('매니페스트·아이콘은 키 없이 공개(홈 화면 추가용)', r.status === 200 && d.display === 'standalone');
  r = await S.front(rq('/icon-192.png'), env); const buf = new Uint8Array(await r.arrayBuffer());
  check('아이콘은 PNG', r.headers.get('content-type') === 'image/png' && buf[0] === 0x89 && buf[1] === 0x50 && buf.length > 100);
  r = await S.front(rq('/api/auth/status'), env); d = await r.json();
  check('상태: 키 없으면 protected=true, ok=false', d.protected === true && d.ok === false);
  r = await S.front(rq('/api/auth/status', KEY, '3.3.3.3'), env); d = await r.json();
  check('상태: 올바른 키 ok=true', d.ok === true && JSON.stringify(d).indexOf(KEY) < 0);
  r = await S.front(rq('/api/auth/status', 'nope-nope-nope', '3.3.3.4'), env); d = await r.json();
  check('상태: 틀린 키 ok=false', d.ok === false);

  // 4) 시도 제한
  env = mkEnv({ PF_KEY: KEY });
  let last;
  for (let i = 0; i < S.AUTH_MAX_FAILS; i++) { last = await S.front(rq('/api/screen/top', 'bad-key-' + i + '-xxxx', '9.9.9.9'), env); }
  check('실패 ' + S.AUTH_MAX_FAILS + '회까지는 401', last.status === 401);
  last = await S.front(rq('/api/screen/top', 'bad-key-next-xxx', '9.9.9.9'), env);
  check('초과하면 429 + Retry-After', last.status === 429 && Number(last.headers.get('retry-after')) > 0);
  last = await S.front(rq('/api/screen/top', KEY, '9.9.9.9'), env);
  check('잠긴 IP는 올바른 키도 거부(무차별 대입으로 맞혀도 확인 불가)', last.status === 429);
  last = await S.front(rq('/api/screen/top', KEY, '8.8.8.8'), env);
  check('다른 IP는 영향 없음', last.status === 200);
  last = await S.front(rq('/api/screen/top', undefined, '9.9.9.9'), env);
  check('키 없는 요청은 횟수에 안 넣고 401(잠긴 IP여도 키 없음은 401)', last.status === 401);
  // 성공 시 초기화
  env = mkEnv({ PF_KEY: KEY });
  for (let i = 0; i < 3; i++) await S.front(rq('/api/screen/top', 'bad-key-' + i + '-xxxx', '7.7.7.7'), env);
  await S.front(rq('/api/screen/top', KEY, '7.7.7.7'), env);
  const row = env.db.prepare('SELECT * FROM auth_fail WHERE ip = ?').get('7.7.7.7');
  check('성공하면 실패 카운터 초기화', !row);
  // 창이 지나면 카운터 재시작
  env = mkEnv({ PF_KEY: KEY });
  await S.front(rq('/api/screen/top', 'bad-key-0-xxxx', '6.6.6.6'), env);
  env.db.prepare('UPDATE auth_fail SET n = 50, ws = ? WHERE ip = ?').run(Date.now() - 16 * 60 * 1000, '6.6.6.6');
  last = await S.front(rq('/api/screen/top', KEY, '6.6.6.6'), env);
  check('15분이 지나면 잠금 해제', last.status === 200);
  // DB 오류여도 정상 키는 통과
  env = { PF_KEY: KEY, DB: { prepare: () => { throw new Error('db down'); } } };
  last = await S.front(rq('/api/screen/top', KEY, '5.5.5.5'), env);
  check('제한 장치 DB 오류가 정상 사용을 막지 않음', last.status === 200);
  last = await S.front(rq('/api/screen/top', 'wrong-key-123456', '5.5.5.5'), env);
  check('DB 오류여도 틀린 키는 401', last.status === 401);
  console.log(fail ? '\n실패 ' + fail + '건' : '\n전부 통과'); process.exit(fail ? 1 : 0);
})();
