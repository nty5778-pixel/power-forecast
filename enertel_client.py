"""
Enertel / S&P Global Energy CERA Power Source API 클라이언트

전력시장 예측(wholesale power market forecasts) 데이터를 조회하는 얇은 래퍼입니다.
모든 요청은 Bearer 토큰 인증을 사용하며, 토큰은 .env 파일의 ENERTEL_API_TOKEN 에서 읽습니다.

사용 예:
    from enertel_client import EnertelClient
    client = EnertelClient()          # .env 에서 토큰 자동 로드
    me = client.me()                  # 연결 확인
    targets = client.get_targets()
"""

from __future__ import annotations

import os
from typing import Any, Iterable

import requests
from dotenv import load_dotenv

# 스크립트 실행 위치와 무관하게 .env 를 찾아 로드
load_dotenv()

DEFAULT_BASE_URL = "https://app.enertel.ai/api"


class EnertelAPIError(RuntimeError):
    """API 가 2xx 이외의 상태를 반환할 때 발생. 상태코드와 응답 본문을 담습니다."""

    def __init__(self, status_code: int, message: str, url: str):
        self.status_code = status_code
        self.url = url
        super().__init__(f"[{status_code}] {url} -> {message}")


class EnertelClient:
    def __init__(
        self,
        token: str | None = None,
        base_url: str | None = None,
        timeout: int = 30,
    ):
        self.token = token or os.getenv("ENERTEL_API_TOKEN")
        if not self.token or self.token.startswith("rtbp_여기에"):
            raise ValueError(
                "API 토큰이 설정되지 않았습니다. .env 파일에 ENERTEL_API_TOKEN 값을 넣으세요.\n"
                "발급: app.enertel.ai 로그인 → 우측 상단 프로필 → API 토큰 생성"
            )

        self.base_url = (base_url or os.getenv("ENERTEL_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/json",
            }
        )

    # ------------------------------------------------------------------ #
    # 내부 요청 헬퍼
    # ------------------------------------------------------------------ #
    def _request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.base_url}/{path.lstrip('/')}"
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)

        if not resp.ok:
            # 응답 본문이 JSON 이면 message 를, 아니면 원문 텍스트를 사용
            try:
                body = resp.json()
                message = body.get("detail") or body.get("message") or str(body)
            except ValueError:
                message = resp.text[:500]
            raise EnertelAPIError(resp.status_code, message, url)

        # CSV 다운로드 엔드포인트 등은 JSON 이 아닐 수 있음
        if "application/json" in resp.headers.get("Content-Type", ""):
            return resp.json()
        return resp.text

    def _get(self, path: str, params: dict | None = None) -> Any:
        return self._request("GET", path, params=self._clean(params))

    def _post(self, path: str, json_body: dict | None = None) -> Any:
        return self._request("POST", path, json=json_body)

    @staticmethod
    def _clean(params: dict | None) -> dict | None:
        """None 값을 제거해 불필요한 쿼리 파라미터를 보내지 않도록 함."""
        if not params:
            return None
        return {k: v for k, v in params.items() if v is not None}

    # ------------------------------------------------------------------ #
    # user
    # ------------------------------------------------------------------ #
    def me(self) -> dict:
        """GET /me — 현재 사용자 프로필. 연결/인증 확인용으로 가장 먼저 호출하세요."""
        return self._get("/me")

    # ------------------------------------------------------------------ #
    # targets & features
    # ------------------------------------------------------------------ #
    def get_targets(self) -> list[dict]:
        """GET /targets — 접근 가능한 예측 대상(target) 목록."""
        return self._get("/targets")

    def search_features(
        self,
        *,
        iso: str | None = None,
        series_name: str | None = None,
        object_name: str | None = None,
        category: str | None = None,
        target_id: int | None = None,
        max_features: int | None = None,
        **extra,
    ) -> list[dict]:
        """GET /features — 필터로 feature 검색. (파라미터를 하나도 주지 않으면 빈 배열 반환)"""
        params = {
            "iso": iso,
            "series_name": series_name,
            "object_name": object_name,
            "category": category,
            "target_id": target_id,
            "max_features": max_features,
            **extra,
        }
        return self._get("/features", params)

    def get_target_features(self, target_id: int, feature_type: str | None = None) -> list[dict]:
        """GET /target/{target_id}/features — 특정 target 에 설정된 feature 목록."""
        return self._get(f"/target/{target_id}/features", {"type": feature_type})

    # ------------------------------------------------------------------ #
    # forecasts
    # ------------------------------------------------------------------ #
    def get_latest_forecasts(
        self,
        start: str,
        end: str,
        target_id: Iterable[int] | int | None = None,
        api_version: str | None = "v3",
    ) -> Any:
        """GET /forecasts/latest — 지정 기간의 최신 예측. start/end 는 ISO 8601 문자열."""
        params = {
            "start": start,
            "end": end,
            "target_id": list(target_id) if isinstance(target_id, Iterable) and not isinstance(target_id, str) else target_id,
            "api_version": api_version,
        }
        return self._get("/forecasts/latest", params)

    def get_dashboard_forecasts(
        self,
        start: str,
        end: str,
        *,
        models: str | None = None,
        features: str | None = None,
        top_n: int | None = None,
    ) -> Any:
        """GET /dashboard/forecasts — 대시보드 저장 기본값 기준 상세 예측."""
        params = {
            "start": start,
            "end": end,
            "models": models,
            "features": features,
            "top_n": top_n,
        }
        return self._get("/dashboard/forecasts", params)

    def get_dashboards(self) -> list[dict]:
        """GET /dashboards — 저장된 대시보드 프리셋."""
        return self._get("/dashboards")

    # ------------------------------------------------------------------ #
    # models / scenarios / inference
    # ------------------------------------------------------------------ #
    def get_model_rankings(self, target_id: int, features: str | None = None) -> Any:
        """GET /model/rankings — target 의 상위 모델 랭킹."""
        return self._get("/model/rankings", {"target_id": target_id, "features": features})

    def get_scenarios(
        self,
        target_id: Iterable[int] | int,
        *,
        limit: int | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> list[dict]:
        """GET /scenarios — target 의 추론 시나리오."""
        params = {
            "target_id": list(target_id) if isinstance(target_id, Iterable) and not isinstance(target_id, str) else target_id,
            "limit": limit,
            "start": start,
            "end": end,
        }
        return self._get("/scenarios", params)

    def get_inference_results(
        self,
        *,
        scenarios: str | None = None,
        models: str | None = None,
        max_results: int | None = None,
    ) -> list[dict]:
        """GET /model/inference — 모델 추론 결과 목록."""
        return self._get(
            "/model/inference",
            {"scenarios": scenarios, "models": models, "max_results": max_results},
        )

    def get_inference_detail(
        self,
        inference_id: int,
        *,
        attributes: str | None = None,
        feature_ids: str | None = None,
    ) -> list[dict]:
        """GET /model/inference/{inference_id} — 특정 추론 결과의 상세 예측(백분위 포함)."""
        return self._get(
            f"/model/inference/{inference_id}",
            {"attributes": attributes, "feature_ids": feature_ids},
        )