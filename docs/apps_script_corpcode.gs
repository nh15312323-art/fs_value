// ============================================================
// 신규 상장 종목 정보 동기화용: 기존 DART 프록시(Google Apps Script)에 추가하는 코드
//
// 하는 일: Worker가 ?endpoint=corpcode&codes=005930,000660 처럼 부르면,
//          DART corpCode.xml(전체 기업 고유번호, 압축 파일)을 받아 요청한 종목코드만 골라 JSON으로 돌려준다.
//          (Worker는 DART 키가 없고, 30MB짜리 XML을 직접 풀 수도 없어서 프록시가 대신한다.)
//
// 적용 방법
//  1) 기존 프록시 스크립트(doGet이 있는 파일)에 아래 함수 corpListed_ 를 그대로 붙여 넣는다.
//  2) 기존 doGet 맨 위쪽(다른 endpoint 처리보다 먼저)에 아래 3줄을 추가한다.
//       if (e.parameter.endpoint === 'corpcode') {
//         return corpListed_(DART_KEY_변수, e.parameter.codes);   // ← DART 키를 담고 있는 기존 변수 이름으로 바꾼다
//       }
//  3) 배포 > 배포 관리 > 편집(연필) > 버전 "새 버전" > 배포. (주소는 그대로 유지된다)
//  4) 확인: 프록시주소?endpoint=corpcode&codes=005930  → {"status":"000","list":[{"corp_code":"00126380",...}]}
// ============================================================
function corpListed_(apiKey, codesCsv) {
  var want = {};
  String(codesCsv || '').split(',').forEach(function (c) { c = c.trim(); if (/^[0-9A-Z]{6}$/.test(c)) want[c] = true; });
  var out = { status: '000', list: [] };
  if (Object.keys(want).length === 0) { out.status = '100'; out.message = 'codes 없음'; return json_(out); }
  var res = UrlFetchApp.fetch('https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=' + encodeURIComponent(apiKey), { muteHttpExceptions: true });
  if (res.getResponseCode() !== 200) { out.status = String(res.getResponseCode()); out.message = 'DART 응답 오류'; return json_(out); }
  var blobs;
  try { blobs = Utilities.unzip(res.getBlob().setContentType('application/zip')); }
  catch (err) { out.status = '900'; out.message = 'zip 해제 실패(키 오류일 수 있음): ' + String(res.getContentText()).slice(0, 120); return json_(out); }
  var xml = blobs[0].getDataAsString('UTF-8');
  var blocks = xml.match(/<list>[\s\S]*?<\/list>/g) || [];
  var pick = function (b, tag) { var m = b.match(new RegExp('<' + tag + '>([^<]*)</' + tag + '>')); return m ? m[1].trim() : ''; };
  for (var i = 0; i < blocks.length; i++) {
    var sc = pick(blocks[i], 'stock_code');
    if (!sc || !want[sc]) continue;
    out.list.push({ corp_code: pick(blocks[i], 'corp_code'), corp_name: pick(blocks[i], 'corp_name'), stock_code: sc, modify_date: pick(blocks[i], 'modify_date') });
  }
  return json_(out);
}
function json_(o) { return ContentService.createTextOutput(JSON.stringify(o)).setMimeType(ContentService.MimeType.JSON); }
