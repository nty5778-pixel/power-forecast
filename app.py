"""
Enertel 예측 추출 웹서비스 (n8n 에서 호출). DB 에는 접속하지 않고 예측 JSON 만 반환합니다.

엔드포인트:
    GET  /health                      상태 확인
    POST /run                         당일(America/Chicago) 예측 JSON 반환
    POST /run?date=YYYY-MM-DD         특정 as-of 날짜(백필/재실행)
    POST /run?lookback=0              그날 배치만 사용(환경변수 LOOKBACK_DAYS 무시, 테스트용)
    POST /run_full                    /run 과 동일 규칙, P50 외 모든 백분위(da_pXX/rt_pXX) 반환
                                      (구글 시트 전용 워크플로용, date/lookback 파라미터 동일)

인증:
    환경변수 RUN_API_KEY 설정 시, 요청 헤더 X-API-Key 가 일치해야 실행됩니다.

응답 예:
    {"status":"ok","as_of_date":"2026-07-24","node":"LZ_HOUSTON","iso":"ERCOT",
     "count":96,"da_filled":96,"rt_filled":96,
     "by_horizon":{"D+1":{"da":24,"rt":24}, ...},
     "lookback_days":1,"max_stale_days":0,"sources":{...},
     "rows":[{"as_of":"...","target_ts":"...","iso":"ERCOT","node":"LZ_HOUSTON","da":25.08,"rt":26.74}, ...]}
"""

import datetime
import os

from fastapi import FastAPI, Header, HTTPException, Query

from pipeline import extract_for_date, extract_full_for_date

app = FastAPI(title="Enertel Forecast Extractor")


def _check_key(x_api_key: str | None):
    key = os.getenv("RUN_API_KEY")
    if key and x_api_key != key:
        raise HTTPException(status_code=401, detail="invalid or missing X-API-Key")


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/run")
def run(date: str | None = Query(default=None, description="as-of 날짜 YYYY-MM-DD (미지정 시 오늘, CT)"),
        lookback: int | None = Query(default=None, ge=0, le=7,
                                     description="그날 배치가 없을 때 며칠 전 배치까지 쓸지 (미지정 시 LOOKBACK_DAYS)"),
        x_api_key: str | None = Header(default=None)):
    _check_key(x_api_key)
    d = None
    if date:
        try:
            d = datetime.date.fromisoformat(date)
        except ValueError:
            raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 여야 합니다")
    try:
        result = extract_for_date(d, lookback=lookback)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")
    return {"status": "ok", **result}


@app.post("/run_full")
def run_full(date: str | None = Query(default=None, description="as-of 날짜 YYYY-MM-DD (미지정 시 오늘, CT)"),
             lookback: int | None = Query(default=None, ge=0, le=7,
                                          description="312H(D+4) 전날 배치 대체 일수 (미지정 시 LOOKBACK_DAYS)"),
             x_api_key: str | None = Header(default=None)):
    """/run 과 같은 규칙으로 모든 백분위(da_pXX / rt_pXX)를 반환. 구글 시트 전용 워크플로에서 호출."""
    _check_key(x_api_key)
    d = None
    if date:
        try:
            d = datetime.date.fromisoformat(date)
        except ValueError:
            raise HTTPException(status_code=400, detail="date 형식은 YYYY-MM-DD 여야 합니다")
    try:
        result = extract_full_for_date(d, lookback=lookback)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}")
    return {"status": "ok", **result}