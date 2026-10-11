-- 결산월(회계연도 종료월) 저장 표. 코드가 자동으로 만들지만(ensureFiscalTable) 직접 적용하려면 아래를 실행하세요.
-- acc_mt: DART 회사개요(company)의 결산월(1~12). 12월이 아니면 공시일·기간말 주가 등 날짜 기반 값을 만들지 않고 백테스트에서 제외합니다.
CREATE TABLE IF NOT EXISTS corp_fiscal (
  corp_code  TEXT PRIMARY KEY,
  acc_mt     INTEGER NOT NULL,
  checked_at TEXT
);
