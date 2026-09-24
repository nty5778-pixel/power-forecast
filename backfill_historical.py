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

--lookback N (기본 0):
  312H(D+4)에 한해, 그날 배치가 아직 없으면 최대 N일 전 배치까지 거슬러 올라가 **빈 칸만** 메꿉니다.
  아침 7시에 그날 312H 배치가 아직 안 나와 D+4 가 통째로 비는 걸 막는 용도입니다.
  72H/90H(D+1~D+3)는 이 옵션과 무관하게 **항상 당일 배치만** 씁니다 —
  전날 72H 는 D+3 을 커버하지도 못하고, 단기 구간은 최신 배치가 훨씬 정확하기 때문입니다.
  최신 배치 값이 항상 우선이고, 오래된 배치는 절대 덮어쓰지 않습니다.
  기본 0 이라 과거 백필 결과는 종전과 완전히 동일합니다.

특징: 증분 저장 + 재시작(resume) 지원. 중간에 끊겨도 다시 실행하면 이어서 진행합니다.
API 호출이 수천 건이라 시간이 걸립니다(수십 분~). --sleep 로 호출 간격 조절 가능.

사용법:
  python backfill_historical.py
  python backfill_historical.py --node LZ_HOUSTON --start 2025-01-01 --out hist.csv
"""

import argparse
import csv
import os
import re
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

# 전날 배치까지 거슬러 올라가도 되는 타깃(= lookback 대상).
# 72H/90H 는 '당일 07시 배치'가 관건이라 전날 것으로 대체하지 않습니다
# (전날 72H 는 D+3 을 아예 커버하지 못하고, 단기 구간은 최신 배치가 압도적으로 정확).
# 312H(D+4)만 전날 최신 배치로 메꿉니다 — 13일치를 예측하므로 D+4 를 그대로 커버합니다.
LOOKBACK_KEYS = ("DA_312H", "RT_312H")

# full=True 모드에서 백분위로 인정할 속성 이름 (p1, p05, p10, p50, p90, p99 ...)
PCT_RE = re.compile(r"^p\d{1,2}$")


def z(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_dt(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


class Extractor:
    def __init__(self, client, node, sleep=0.15, lookback_days=0,
                 cutoff_hour=None, keys=None, full=False, attributes=None):
        self.c = client
        self.node = node
        self.sleep = sleep
        # full=False: 기존 동작 그대로 {ts: p50(float)}.
        # full=True : 모든 백분위 {ts: {"p10": .., "p50": .., ...}}.
        #   attributes 를 주면 그 목록만 요청(예: "p10,p25,p50,p75,p90"),
        #   None 이면 attributes 필터 없이 요청해 응답에 담긴 p* 키를 전부 씁니다.
        self.full = full
        self.attributes = attributes
        # cutoff_hour: 그날 이 시각(Central) '이전에 생성된' 배치만 사용합니다.
        #   과거 백필에서 as_of 07 시 시점을 그대로 재현할 때 씁니다(7 지정).
        #   None 이면 제한 없음 — 실서비스는 어차피 그 시각에 도는 것이라 불필요합니다.
        self.cutoff_hour = cutoff_hour
        # keys: 일부 타깃만 쓰고 싶을 때(예: D+4 만 백필 → 312H 두 개). None 이면 전부.
        self.targets = [m for m in TARGETS if keys is None or m["key"] in keys]
        # lookback_days=0 이면 모든 타깃이 '그날 생성된 배치'만 사용(기존 백필 동작 그대로).
        # 1 이상이면 LOOKBACK_KEYS(312H)만 그날 배치가 없을 때 전날(들) 최신 배치로 메꿉니다.
        # 72H/90H 는 항상 당일 배치만 씁니다.
        n = max(0, int(lookback_days))
        self.lookback_days = n
        self.lookback = {m["key"]: (n if m["key"] in LOOKBACK_KEYS else 0) for m in self.targets}
        # 하루에 여러 런이 있을 수 있어 시도 횟수를 제한(API 호출 폭증 방지).
        self.max_scenarios = {k: (1 if v == 0 else 1 + 3 * v) for k, v in self.lookback.items()}
        self.feat = {}          # target -> feature_id
        self.rank = {}          # target -> [model_id ...] (현재 랭킹 순)
        self.detail_cache = {}  # inference_id -> {ts: p50}
        self.sources = {}       # target key -> [사용한 배치 정보 ...] (rows_for_day 후 채워짐)
        for m in self.targets:
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

    def _scenarios_for(self, target, day, lookback):
        """
        D+1~D+4 구간을 커버하는 배치 중, [day - lookback, day] 에 생성된 것들을
        최신순으로 반환합니다. (lookback=0 이면 그날 것만)

        반환: [(생성일(Central date), scenario), ...]
        """
        start = datetime.combine(day + timedelta(days=1), time(0), CENTRAL)
        end = datetime.combine(day + timedelta(days=5), time(0), CENTRAL)
        try:
            scs = self.c.get_scenarios(target, start=z(start), end=z(end), limit=500)
        except EnertelAPIError:
            return []
        self._nap()
        oldest = day - timedelta(days=lookback)
        cutoff = (datetime.combine(day, time(self.cutoff_hour), CENTRAL)
                  if self.cutoff_hour is not None else None)
        cand = []
        for s in scs or []:
            sa = s.get("scheduled_at")
            if not sa:
                continue
            sadt = parse_dt(sa)
            if cutoff is not None and sadt > cutoff:
                continue        # as_of 시점엔 아직 없던 배치 — 과거 재현 시 제외
            d0 = sadt.astimezone(CENTRAL).date()
            if oldest <= d0 <= day:
                cand.append((sa, d0, s))
        cand.sort(key=lambda x: x[0], reverse=True)   # 최신 런 우선
        return [(d0, s) for (_sa, d0, s) in cand]

    def _map_for_scenario(self, key, s):
        """배치 하나에서 {timestamp: p50} 추출 (inference_id 단위 캐시)."""
        feat = self.feat[key]
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
            attrs = self.attributes if self.full else "p50"
            try:
                det = self.c.get_inference_detail(pick["id"], attributes=attrs, feature_ids=str(feat))
            except EnertelAPIError:
                det = []
            self._nap()
            if self.full:
                mm = {}
                for r in det:
                    if r.get("feature_id") != feat:
                        continue
                    pv = {k: round(v, 2) for k, v in r.items()
                          if PCT_RE.match(k) and isinstance(v, (int, float))}
                    if pv:
                        mm[r["timestamp"]] = pv
            else:
                mm = {r["timestamp"]: round(r["p50"], 2)
                      for r in det if r.get("feature_id") == feat and r.get("p50") is not None}
            if mm:
                m = mm
                break
        self.detail_cache[s["id"]] = m
        return m

    def _series_map(self, key, target, day, need=None):
        """
        최신 배치부터 순서대로 읽어 {timestamp: p50} 를 만듭니다.
        필요한 시각(need)이 다 채워지면 멈추고, 모자라면 더 오래된 배치로 **빈 칸만** 메꿉니다.
        (새 배치 값이 항상 우선 — 오래된 배치는 덮어쓰지 않습니다)

        거슬러 올라가는 범위는 타깃별로 다릅니다: 72H/90H 는 항상 당일만, 312H 만 lookback 적용.
        """
        feat = self.feat[key]
        if feat is None:
            self.sources[key] = []
            return {}
        merged = {}
        used = []
        scens = self._scenarios_for(target, day, self.lookback[key])
        for (sday, s) in scens[: self.max_scenarios[key]]:
            m = self._map_for_scenario(key, s)
            added = 0
            for ts, v in m.items():
                if ts not in merged:
                    merged[ts] = v
                    added += 1
            if added:
                used.append({
                    "scenario_id": s.get("id"),
                    "scenario_date": sday.isoformat(),
                    "stale_days": (day - sday).days,
                    "values": added,
                })
            if need is not None:
                if need <= merged.keys():   # 필요한 시각이 전부 채워짐
                    break
            elif merged:
                break
        self.sources[key] = used
        return merged

    def rows_for_day(self, day, horizons=(1, 2, 3, 4)):
        """
        horizons: 뽑을 D+n 목록. (1,2,3,4)=96행이 기본, (4,)=D+4 24행만(백필용).
        """
        def hours(dd):
            return {datetime.combine(day + timedelta(days=dd), time(h), CENTRAL).isoformat()
                    for h in range(24)}

        hs = tuple(horizons)
        near = set().union(*[hours(dd) for dd in hs if dd <= 3]) if any(d <= 3 for d in hs) else set()
        far = hours(4) if 4 in hs else set()
        need = {"DA_72H": near, "RT_90H": near, "DA_312H": far, "RT_312H": far}

        self.sources = {}
        maps = {m["key"]: self._series_map(m["key"], m["t"], day, need[m["key"]])
                for m in self.targets}
        asof = datetime.combine(day, time(7), CENTRAL).isoformat()
        out = []
        for dd in hs:
            for h in range(24):
                ts = datetime.combine(day + timedelta(days=dd), time(h), CENTRAL).isoformat()
                if dd <= 3:
                    da = maps.get("DA_72H", {}).get(ts, "")
                    rt = maps.get("RT_90H", {}).get(ts, "")
                else:
                    da = maps.get("DA_312H", {}).get(ts, "")
                    rt = maps.get("RT_312H", {}).get(ts, "")
                out.append([asof, ts, da, rt])
        return out


def main():
    ap = argparse.ArgumentParser(description="과거 historical forecast 백필 (방식 B)")
    ap.add_argument("--node", default="LZ_HOUSTON")
    ap.add_argument("--start", default="2025-01-01")
    ap.add_argument("--end", default=None, help="기본: 오늘")
    ap.add_argument("--out", default="LZ_HOUSTON_historical_asof07.csv")
    ap.add_argument("--sleep", type=float, default=0.15, help="API 호출 간 대기(초)")
    ap.add_argument("--lookback", type=int, default=0,
                    help="그날 배치가 없을 때 며칠 전 배치까지 대신 쓸지 (기본 0 = 그날 것만)")
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

    ex = Extractor(client, args.node, sleep=args.sleep, lookback_days=args.lookback)

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
        stale = [u["stale_days"] for lst in ex.sources.values() for u in lst if u["stale_days"] > 0]
        tag = f"  (이전 배치 사용: -{max(stale)}d)" if stale else ""
        print(f"[{done}/{total}] {d}  DA {da_n}/96  RT {rt_n}/96{tag}")
        d += timedelta(days=1)

    f.close()
    print(f"\n완료: {args.out}")
    print("참고: 2025년 구간은 D+4(312H)가 공란입니다(해당 모델이 2026-01부터 존재).")


if __name__ == "__main__":
    main()