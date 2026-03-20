"""REST client handling, including HotglueStream base class."""

from typing import Any, Dict, Iterable, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class HotglueStream(RESTStream):
    """Base HotGlue stream class."""

    url_base = "https://client-api.hotglue.xyz"
    records_jsonpath = "$[*]"
    replication_key = None

    # Most HotGlue endpoints don't paginate
    paginate = False

    @property
    def http_headers(self) -> dict:
        """Return headers with API key auth."""
        return {
            "x-api-key": self.config["api_key"],
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    @property
    def env_id(self) -> str:
        """Return the HotGlue environment ID from config."""
        return self.config["env_id"]

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return URL query parameters."""
        return {}

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """No pagination by default."""
        return None

    def validate_response(self, response: requests.Response) -> None:
        """Handle error responses."""
        if response.status_code == 429:
            raise RetriableAPIError(
                f"Rate limited (429): {response.text[:200]}", response
            )
        if response.status_code >= 500:
            raise RetriableAPIError(
                f"Server error ({response.status_code}): {response.text[:200]}",
                response,
            )
        if 400 <= response.status_code < 500:
            # 404 on tenant-specific endpoints is expected (deleted/inactive tenants)
            if response.status_code == 404:
                return None
            raise FatalAPIError(
                f"Client error ({response.status_code}): {response.text[:200]}"
            )

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and yield records."""
        if response.status_code == 404:
            return

        try:
            data = response.json()
        except Exception:
            return

        # Some endpoints return a dict instead of a list
        if isinstance(data, dict) and self.records_jsonpath == "$[*]":
            yield data
            return

        yield from extract_jsonpath(self.records_jsonpath, input=data)
