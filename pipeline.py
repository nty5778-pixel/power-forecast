"""
당일(또는 지정일) 예측 추출 → JSON 반환 (DB 미접속).
DB 적재는 n8n(Microsoft SQL 노드)에서 처리하므로 여기선 pyodbc 를 쓰지 않습니다.

추출 로직은 backfill_historical.Extractor 재사용(방식 B: 그날 배치, D+1~D+4).

환경변수:
    ENERTEL_API_TOKEN  (필수)  Enertel API 토큰
    NODE               기본 LZ_HOUSTON
    ISO                기본 ERCOT
    SLEEP              API 호출 간 대기(초), 기본 0.15
    LOOKBACK_DAYS      312H(D+4)에 한해 며칠 전 배치까지 대신 쓸지, 기본 1

소스 분리 (핵심):
    D+1~D+3 (72H/90H) : **당일 07시 배치만** 사용. 없으면 공란 — 전날 것으로 대체하지 않습니다.
                        전날 72H 는 D+3 을 커버하지도 못하고, 단기 구간은 최신 배치가 훨씬 정확.
    D+4     (312H)    : 당일 배치가 있으면 그걸, 아직 없으면 **전날 최신 배치**를 씁니다.
                        312H 는 13일치를 예측하므로 전날 배치도 D+4 를 그대로 커버합니다.
    새 배치 값이 언제나 우선이고, 오래된 배치가 새 값을 덮어쓰는 일은 없습니다.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from enertel_client import EnertelClient
from backfill_historical import Extractor

CENTRAL = ZoneInfo("America/Chicago")


def _num(v):
    """빈 값('' 또는 None)은 None(→JSON null), 그 외는 float."""
    return None if v in ("", None) else float(v)


def _split_ts(as_of_iso: str, target_ts_iso: str):
    """
    as_of / target_ts ISO 문자열을 dbo.AIagent_power 와 동일한
    (as_of DATE, forecastdate DATE, time_start TIME, time_end TIME) 로 분해.
      as_of_iso     예: '2026-07-24T07:00:00-05:00' → as_of '2026-07-24'
      target_ts_iso 예: '2026-07-25T23:00:00-05:00' → forecastdate '2026-07-25',
                        time_start '23:00:00', time_end '00:00:00'
    """
    as_of_date = as_of_iso[:10]
    forecastdate = target_ts_iso[:10]
    hour = int(target_ts_iso[11:13])
    time_start = f"{hour:02d}:00:00"
    time_end = f"{(hour + 1) % 24:02d}:00:00"
    return as_of_date, forecastdate, time_start, time_end


def extract_for_date(d: date | None = None, lookback: int | None = None) -> dict:
    node = os.getenv("NODE", "LZ_HOUSTON")
    iso = os.getenv("ISO", "ERCOT")
    if d is None:
        d = datetime.now(CENTRAL).date()
    if lookback is None:
        lookback = int(os.getenv("LOOKBACK_DAYS", "1"))

    client = EnertelClient()  # 토큰은 ENERTEL_API_TOKEN 환경변수
    ex = Extractor(client, node, sleep=float(os.getenv("SLEEP", "0.15")),
                   lookback_days=lookback)
    raw = ex.rows_for_day(d)  # [[as_of, target_ts, da, rt], ...] (da/rt: float 또는 "")

    rows = []
    for (a, t, da, rt) in raw:
        as_of_date, forecastdate, time_start, time_end = _split_ts(a, t)
        rows.append({
            "as_of": as_of_date,
            "forecastdate": forecastdate,
            "time_start": time_start,
            "time_end": time_end,
            "iso": iso,
            "node": node,
            "da": _num(da),
            "rt": _num(rt),
        })
    # 지평선(D+1~D+4)별 채움 현황 — 어느 구간이 비었는지 바로 보이게.
    by_horizon = {}
    for i in range(1, 5):
        fd = (d + timedelta(days=i)).isoformat()
        sub = [r for r in rows if r["forecastdate"] == fd]
        by_horizon[f"D+{i}"] = {
            "forecastdate": fd,
            "da": sum(1 for r in sub if r["da"] is not None),
            "rt": sum(1 for r in sub if r["rt"] is not None),
        }

    sources = getattr(ex, "sources", {}) or {}
    max_stale = max((u["stale_days"] for lst in sources.values() for u in lst), default=0)

    return {
        "as_of_date": d.isoformat(),
        "node": node,
        "iso": iso,
        "count": len(rows),
        "da_filled": sum(1 for r in rows if r["da"] is not None),
        "rt_filled": sum(1 for r in rows if r["rt"] is not None),
        "by_horizon": by_horizon,
        "lookback_days": lookback,
        "max_stale_days": max_stale,   # 0 이면 전부 그날 배치, 1 이면 312H 를 전날 배치로 메꿈
        "sources": sources,            # 타깃별로 실제 사용한 배치(id/생성일/채운 값 수)
        "rows": rows,
    }