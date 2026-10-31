// 사용법: node tests/ui_sig_test.mjs (Playwright + Chromium 필요)
// 종목분석 첫 화면: 한눈에 보기, 장 단위 접기·펼치기, 처음 펼침 상태, 다시 그려도 사용자가 연 항목 유지를 아이폰 폭에서 확인한다.
import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path'; import { pathToFileURL, fileURLToPath } from 'node:url';
import http from 'node:http'; import { createRequire } from 'node:module';
const src = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'src', 'index.js');
const tmp = path.join(os.tmpdir(), 'fs_value_uicmb_' + process.pid + '.mjs');
fs.writeFileSync(tmp, fs.readFileSync(src, 'utf8') + '\nexport { HTML_PAGE };\n');
const { HTML_PAGE } = await import(pathToFileURL(tmp).href); fs.unlinkSync(tmp);
const require = createRequire('/opt/npm-tools/node_modules/'); const { chromium } = require('playwright');

const srv = http.createServer((q, r) => {
  const p = q.url.split('?')[0];
  if (p === '/') { r.setHeader('content-type', 'text/html;charset=utf-8'); return r.end(HTML_PAGE); }
  r.setHeader('content-type', 'application/json');
  if (p === '/api/auth/status') return r.end('{"protected":false,"ok":true}');
  r.end('{}');
}).listen(0);
const b = await chromium.launch({ args: ['--no-sandbox'] });
let fail = 0; const ok = (n, v, d) => { if (!v) fail++; console.log((v ? 'PASS' : 'FAIL') + '  ' + n + (d ? '  ' + d : '')); };
const errs = [];
const pg = await (await b.newContext({ viewport: { width: 390, height: 844 } })).newPage(); pg.on('pageerror', (e) => errs.push(String(e)));
await pg.goto('http://localhost:' + srv.address().port + '/'); await pg.waitForTimeout(500);
await pg.evaluate(() => switchTab('bt')); await pg.waitForTimeout(300);
// 1) t 분포 p값
const pv = await pg.evaluate(() => [cmbPTwo(2.228, 10), cmbPTwo(2.571, 5), cmbPTwo(1.96, 1e6), cmbPTwo(0, 7), cmbPTwo(12.706, 1)]);
ok('t분포 양쪽 p: df10 t2.228≈.05, df5 t2.571≈.05, 큰 df t1.96≈.05, t0=1, df1 t12.706≈.05', Math.abs(pv[0] - 0.05) < 1e-3 && Math.abs(pv[1] - 0.05) < 1e-3 && Math.abs(pv[2] - 0.05) < 1e-3 && Math.abs(pv[3] - 1) < 1e-9 && Math.abs(pv[4] - 0.05) < 1e-3, JSON.stringify(pv));
// 2) 조합 개수
const nc = await pg.evaluate(() => [cmbCombos(10, 5).length, cmbCombos(9, 5).length, cmbCombos(4, 4).length]);
ok('조합 수: C(10,≤5)=637, C(9,≤5)=381, C(4,≤4)=15', nc.join() === '637,381,15', nc.join());
// 합성 데이터 생성기(결정적 난수)
await pg.evaluate(() => {
  window.mkData = (seed, T, N, effect) => { const r = cmbRng(seed); const gauss = () => { let u = 0; for (let i = 0; i < 12; i++) u += r(); return u - 6; };
    const periods = []; for (let t = 0; t < T; t++) { const items = []; for (let i = 0; i < N; i++) { const v = { a: gauss(), b: gauss(), c: gauss(), d: gauss() }; items.push({ id: 's' + i, v, f: 0.01 + effect * v.a + 0.2 * gauss() }); } periods.push({ label: 'P' + t, items }); } return periods; };
});
// 3) 심어 둔 신호: a만 효과 → a가 포함된 조합이 보정 후 유의, 잡음끼리 조합은 아님
const sig = await pg.evaluate(async () => { const R = await cmbSearch({ periods: mkData(11, 24, 150, 0.04), keys: ['a', 'b', 'c', 'd'], kmax: 4, G: 5, B: 200, seed: 3, split: 2 / 3, trim: false });
  const find = (ks) => R.combos.filter((x) => x.keys.join() === ks)[0];
  return { M: R.M, T: R.T, exN: R.exN, coN: R.coN, a: find('a'), bc: find('b,c'), noise: R.combos.filter((x) => x.keys.indexOf('a') < 0).map((x) => x.pAdj), withA: R.combos.filter((x) => x.keys.indexOf('a') >= 0).map((x) => x.pAdj), top: R.combos[0].keys.join(), null95: R.null95 }; });
ok('신호 심기: 15개 조합, 탐색 16 + 확인 8 기간', sig.M === 15 && sig.T === 24 && sig.exN === 16 && sig.coN === 8, JSON.stringify([sig.M, sig.T, sig.exN, sig.coN]));
ok('신호 심기: 요인 a 단독이 보정 후 p≤0.05, 확인 구간에서도 같은 방향', sig.a.pAdj <= 0.05 && sig.a.conf.same, JSON.stringify(sig.a));
ok('신호 심기: 잡음끼리 조합(b,c)은 보정 후 유의하지 않음, a 없는 조합 중 유의 0개', sig.bc.pAdj > 0.05 && sig.noise.every((p) => p > 0.05), JSON.stringify(sig.noise));
ok('신호 심기: a가 든 조합이 상위를 차지', sig.top.split(',').indexOf('a') >= 0, sig.top);
// 4) 귀무 보정 검증: 신호 없는 데이터 30개 → 보정 후 하나라도 유의한 비율이 명목 5% 근처(≤ 15%)
const nul = await pg.evaluate(async () => { let any = 0, anyRaw = 0, N = 30; for (let s = 0; s < N; s++) { const R = await cmbSearch({ periods: mkData(100 + s, 16, 120, 0), keys: ['a', 'b', 'c', 'd'], kmax: 4, G: 5, B: 100, seed: s + 1, split: null, trim: false });
  if (R.combos.some((x) => x.pAdj <= 0.05)) any++; if (R.combos.some((x) => x.pRaw <= 0.05)) anyRaw++; } return { any, anyRaw, N }; });
ok('신호 없는 30개 데이터: 보정 후 유의가 하나라도 나온 비율 ≤ 15% (명목 5%)', nul.any / nul.N <= 0.15, JSON.stringify(nul));
ok('같은 데이터에서 보정 전(원시 p)에는 훨씬 자주 나옴(보정이 필요한 이유)', nul.anyRaw > nul.any, JSON.stringify(nul));
// 5) 기존 btRun과 단일 요인 스프레드 일치
const cons = await pg.evaluate(async () => { const P = mkData(5, 12, 100, 0.03); const R = await cmbSearch({ periods: P, keys: ['a', 'b'], kmax: 1, G: 5, B: 20, seed: 1, split: null, trim: false });
  const per = P.map((p) => ({ label: p.label, items: p.items.map((x) => ({ id: x.id, s: x.v.a, f: x.f })) })); const res = btRun(per, { G: 5, cost: 0, ppy: 1, minN: 15 });
  return { mine: R.combos.filter((x) => x.keys.join() === 'a')[0].mean, theirs: res.spreadStat.mean }; });
ok('단일 요인 스프레드가 기존 백테스트 엔진(btRun)과 일치', Math.abs(cons.mine - cons.theirs) < 1e-12, JSON.stringify(cons));
// 6) 같은 씨앗이면 결과 동일(재현성)
const rep = await pg.evaluate(async () => { const f = () => cmbSearch({ periods: mkData(7, 14, 100, 0.02), keys: ['a', 'b', 'c'], kmax: 3, G: 5, B: 50, seed: 9, split: null, trim: false }); const x = await f(), y = await f(); return JSON.stringify(x.combos.map((c) => c.pAdj)) === JSON.stringify(y.combos.map((c) => c.pAdj)) && x.null95 === y.null95; });
ok('같은 씨앗 → 같은 보정 p (재현 가능)', rep === true);
// 7) 기간 부족
const few = await pg.evaluate(async () => { const R = await cmbSearch({ periods: mkData(1, 3, 100, 0), keys: ['a', 'b'], kmax: 2, G: 5, B: 10, seed: 1, split: null, trim: false }); return R.error || ''; });
ok('기간이 4개 미만이면 오류 안내', few.indexOf('최소 4개') >= 0, few);
// 8) 화면: 합성 기간을 주입해 실행 → 결과 표시
await pg.evaluate(() => { BT.stocks = [{ code: 'x', name: 'x', rows: [] }]; window.btBuildQuarterly = () => mkData(11, 24, 150, 0.04); });
await pg.selectOption('#cmbK', '3'); await pg.selectOption('#cmbB', '200');
for (const k of ['legacy4', 'fcfy', 'graham', 'roic', 'growth', 'ep', 'bp', 'lowaccr']) await pg.uncheck('.cmbFac[value="' + k + '"]');
await pg.evaluate(() => { document.querySelectorAll('.cmbFac').forEach((c) => { if (c.value === 'fscore') c.value = 'a'; else if (c.value === 'gpa') c.value = 'b'; }); });
await pg.evaluate(() => { const box = document.getElementById('cmbPool'); box.insertAdjacentHTML('beforeend', '<label><input type="checkbox" class="cmbFac" value="c" checked></label><label><input type="checkbox" class="cmbFac" value="d" checked></label>'); });
await pg.evaluate(() => { window.BT_FAC_SHORT = window.BT_FAC_SHORT; });
await pg.click('button:has-text("조합 탐색 실행")'); await pg.waitForTimeout(6000);
const out = await pg.innerText('#cmbOut');
ok('화면: 우연의 기준선·판정·표·확인 구간·한계 문구', out.includes('우연의 기준선') && out.includes('보정 후 p≤0.05') && out.includes('확인 구간 스프레드') && out.includes('확인 구간 점검') && out.includes('Westfall'), out.slice(0, 300));
ok('화면: 열기 버튼이 있고 페이지 오류 없음', (await pg.$$('#cmbOut button')).length > 0 && errs.length === 0, errs.join(' | '));
await b.close(); srv.close(); console.log(fail ? 'FAILED ' + fail : 'ALL PASS'); process.exit(fail ? 1 : 0);
