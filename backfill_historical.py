"""
과거 historical forecast 백필 (방식 B).

각 날짜 D(2025-01-01 ~ 오늘, America/Chicago 기준)에 대해:
  - as-of 라벨 = D 07:00 (America/Chicago)
  - 데이터 = "그날(D) 생성된 예측 배치" (scheduled_at 의 Central 날짜 == D 인 런 중 최신)
  - 대상 시각 = D+1 ~ D+4 (시간대별 96행)
  - 블렌딩: D+1~D+3 = DA_72H(1467)/RT_90H(1468),  D+4 = DA_312H(2630)/RT_312H(2631)
  - 출력 컬럼: as-of, timestamp, DA, RT  (File-2 와 동일 구조)

중요한 데이터 제약(사전 확인됨):
  * 312H(2630/2631)는 2026-01 부터만 존재 → 2025년 날짜는 D+4 가 공란(결측)입니다.
  * 일부 과거 RT 배치는 해당 노드 feature 를 담지 않을 수 있어 그 구간은 공란이 됩니다.
  * 모델 선택은 '현재 랭킹 1순위(없으면 차순위, 그래도 없으면 최신 모델)'를 사용합니다.
    (과거 시점의 랭킹이 API로 제공되지 않아, 약간의 look-ahead 가 있음을 감안하세요.)

특징: 증분 저장 + 재시작(resume) 지원. 중간에 끊겨도 다시 실행하면 이어서 진행합니다.
API 호출이 수천 건이라 시간이 걸립니다(수십 분~). --sleep 로 호출 간격 조절 가능.

사용법:
  python backfill_historical.py
  python backfill_historical.py --node LZ_HOUSTON --start 2025-01-01 --out hist.csv
"""

import argparse
import csv
import os
import time as _time
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from enertel_client import EnertelClient, EnertelAPIError

CENTRAL = ZoneInfo("America/Chicago")
UTC = ZoneInfo("UTC")

TARGETS = [
    {"key": "DA_72H",  "t": 1467, "series": "DALMP"},
    {"key": "RT_90H",  "t": 1468, "series": "RTLMP"},
    {"key": "DA_312H", "t": 2630, "series": "DALMP"},
    {"key": "RT_312H", "t": 2631, "series": "RTLMP"},
]


def z(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class Extractor:
    def __init__(self, client, node, sleep=0.15):
        self.c = client
        self.node = node
        self.sleep = sleep
        self.feat = {}          # target -> feature_id
        self.rank = {}          # target -> [model_id ...] (현재 랭킹 순)
        self.detail_cache = {}  # inference_id -> {ts: p50}
        for m in TARGETS:
            f = self._resolve_feature(m["t"], m["series"])
            self.feat[m["key"]] = f
            self.rank[m["key"]] = self._rankings(m["t"], f) if f else []

    def _nap(self):
        if self.sleep:
            _time.sleep(self.sleep)

    def _resolve_feature(self, target, series):
        feats = self.c.get_target_features(target)
        self._nap()
        for f in feats:
            if f.get("object_name") == self.node and f.get("series_name") == series:
                return f["id"]
        return None

    def _rankings(self, target, feat):
        try:
            rk = self.c.get_model_rankings(target, features=str(feat))
        except EnertelAPIError:
            return []
        self._nap()
        rk = rk if isinstance(rk, list) else [rk]
        rk = [r for r in rk if r.get("model_id") is not None]
        rk.sort(key=lambda r: r.get("rank", 10**9))
        return [r["model_id"] for r in rk]

    def _choose_scenario(self, target, day):
        """그날(day) 생성된, D+1~D+4 구간을 커버하는 최신 배치."""
        start = datetime.combine(day + timedelta(days=1), time(0), CENTRAL)
        end = datetime.combine(day + timedelta(days=5), time(0), CENTRAL)
        try:
            scs = self.c.get_scenarios(target, start=z(start), end=z(end), limit=500)
        except EnertelAPIError:
            return None
        self._nap()
        cand = []
        for s in scs or []:
            sa = s.get("scheduled_at")
            if not sa:
                continue
            if parse_dt(sa).astimezone(CENTRAL).date() == day:
                cand.append(s)
        if not cand:
            return None
        cand.sort(key=lambda s: s["scheduled_at"])
        return cand[-1]  # 그날의 최신 런

    def _series_map(self, key, target, day):
        feat = self.feat[key]
        if feat is None:
            return {}
        s = self._choose_scenario(target, day)
        if s is None:
            return {}
        if s["id"] in self.detail_cache:
            return self.detail_cache[s["id"]]
        try:
            infl = self.c.get_inference_results(scenarios=str(s["id"])) or []
        except EnertelAPIError:
            return {}
        self._nap()
        # 후보 모델 순서: 현재 랭킹 우선순위 → 나머지(최신 모델 우선)
        ranked = [x for mid in self.rank[key] for x in infl if x.get("model_id") == mid]
        seen = {id(x) for x in ranked}
        rest = sorted((x for x in infl if id(x) not in seen),
                      key=lambda x: x.get("model_created_at") or "", reverse=True)
        candidates = ranked + rest

        # 해당 노드 feature 의 값이 실제로 담긴 첫 모델을 채택
        # (일부 과거 배치는 1순위 모델 inference 에 해당 feature 가 없을 수 있음)
        m = {}
        for pick in candidates[:5]:
            try:
                det = self.c.get_inference_detail(pick["id"], attributes="p50", feature_ids=str(feat))
            except EnertelAPIError:
                det = []
            self._nap()
            mm = {r["timestamp"]: round(r["p50"], 2)
                  for r in det if r.get("feature_id") == feat and r.get("p50") is not None}
            if mm:
                m = mm
                break
        self.detail_cache[s["id"]] = m
        return m

    def rows_for_day(self, day):
        maps = {m["key"]: self._series_map(m["key"], m["t"], day) for m in TARGETS}
        asof = datetime.combine(day, time(7), CENTRAL).isoformat()
        out = []
        for dd in range(1, 5):
            for h in range(24):
                ts = datetime.combine(day + timedelta(days=dd), time(h), CENTRAL).isoformat()
                if dd <= 3:
                    da = maps["DA_72H"].get(ts, "")
                    rt = maps["RT_90H"].get(ts, "")
                else:
                    da = maps["DA_312H"].get(ts, "")
                    rt = maps["RT_312H"].get(ts, "")
                out.append([asof, ts, da, rt])
        return out


def main():
    ap = argparse.ArgumentParser(description="과거 historical forecast 백필 (방식 B)")
    ap.add_argument("--node", default="LZ_HOUSTON")
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default=None, help="기본: 오늘")
    ap.add_argument("--out", default="LZ_HOUSTON_historical_asof07.csv")
    ap.add_argument("--sleep", type=float, default=0.15, help="API 호출 간 대기(초)")
    args = ap.parse_args()

    try:
        client = EnertelClient()
    except ValueError as e:
        print("설정 오류:", e); return

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else datetime.now(CENTRAL).date()

    # 재시작 지원: 진행 파일에 마지막 완료일 기록
    prog = args.out + ".progress"
    resume_from = None
    if os.path.exists(args.out) and os.path.exists(prog):
        with open(prog) as f:
            resume_from = date.fromisoformat(f.read().strip())
        print(f"이어서 진행: {resume_from} 다음 날짜부터")

    ex = Extractor(client, args.node, sleep=args.sleep)

    new_file = not (os.path.exists(args.out) and resume_from)
    f = open(args.out, "a", newline="", encoding="utf-8-sig")
    w = csv.writer(f)
    if new_file:
        w.writerow(["as-of", "timestamp", "DA", "RT"])

    d = start if resume_from is None else resume_from + timedelta(days=1)
    total = (end - d).days + 1
    done = 0
    while d <= end:
        try:
            rows = ex.rows_for_day(d)
        except EnertelAPIError as e:
            print(f"  {d} 조회 실패, 건너뜀: {e}")
            rows = []
        w.writerows(rows)
        f.flush()
        with open(prog, "w") as pf:
            pf.write(d.isoformat())
        done += 1
        da_n = sum(1 for r in rows if r[2] != "")
        rt_n = sum(1 for r in rows if r[3] != "")
        print(f"[{done}/{total}] {d}  DA {da_n}/96  RT {rt_n}/96")
        d += timedelta(days=1)

    f.close()
    print(f"\n완료: {args.out}")
    print("참고: 2025년 구간은 D+4(312H)가 공란입니다(해당 모델이 2026-01부터 존재).")


if __name__ == "__main__":
    main()
