# Memory Vault

벡터 기반 영구 기억 AI 데모 앱입니다.

## 핵심

- 메모를 청크로 잘라서 저장합니다.
- 각 청크는 시간, 요일, 주차, 소스, 역할, 중요도, 태그, 토픽 같은 메타데이터와 함께 벡터화됩니다.
- 검색은 코사인 유사도 + 최신성 + 중요도 가중치로 계산합니다.
- 대화는 이전 기억을 검색한 뒤 답을 구성하고, 대화 자체도 다시 메모리에 저장합니다.

## 실행

```bash
cd /root/memoryvault
source .venv/bin/activate
uvicorn app:app --host 127.0.0.1 --port 8123
```

브라우저에서 `http://127.0.0.1:8123` 을 열면 됩니다.

## 환경변수

- `GEMINI_API_KEY`: Gemini API 키
- `GOOGLE_API_KEY`: 동일 키를 넣어도 됩니다. 코드에서 둘 다 읽고, `GOOGLE_API_KEY`가 우선입니다.
- `GOOGLE_GENAI_API_KEY`: 호환용으로 같이 읽습니다.
- `GEMINI_MODEL`: `gemini-3.1-flash-lite-preview`
- `DATABASE_URL`: 외부 DB 연결 문자열. Render Postgres를 붙이면 자동으로 들어오는 값입니다.
- `MEMORYVAULT_DB_PATH`: 로컬 SQLite 폴백용 경로. `DATABASE_URL`이 없을 때만 사용합니다.

## Render 권장 설정

- Web Service는 `DATABASE_URL`을 쓰는 Postgres 기반으로 연결
- `DATABASE_URL`은 Render Postgres add-on이 자동 주입
- `Start Command`: `uvicorn app:app --host 0.0.0.0 --port $PORT`
- `Build Command`: `pip install -r requirements.txt`
