"""Optiply authentication module."""

from __future__ import annotations

import json
import logging
import os
import threading
import requests
import backoff
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

# Every sink's authenticator shares the target config, and each grant replaces the previous
# token, so parallel write workers must refresh one at a time.
_token_lock = threading.RLock()


class OptiplyAuthenticator:
    """API Authenticator for OAuth 2.0 password flow."""

    def __init__(
        self,
        target,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Initialize authenticator."""
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_endpoint = auth_endpoint or os.environ.get(
            "optiply_dashboard_url", "https://dashboard.acceptance.optiply.com/api"
        ) + "/auth/oauth/token"
        self._target = target

    @property
    def auth_headers(self) -> dict:
        """Get authentication headers."""
        if not self.is_token_valid():
            with _token_lock:
                if not self.is_token_valid():
                    self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._config.get('access_token')}"
        return result

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for password flow."""
        return {
            "grant_type": "password",
            "username": self._config["username"],
            "password": self._config["password"],
            "client_id": self._config["client_id"],
            "client_secret": self._config["client_secret"],
        }

    def is_token_valid(self) -> bool:
        """Check if the current access token is still valid."""
        access_token = self._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._config.get("expires_in")
        if expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False

        if not expires_in:
            return False

        return not ((expires_in - now) < 120)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        """Update the access token by making a request to the auth endpoint."""
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        logger.info(f"OAuth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}")
        
        # Prepare Basic Auth headers
        import base64
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]
        basic_auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
        headers["Authorization"] = f"Basic {basic_auth}"
        
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body, headers=headers
        )

        try:
            if (
                token_response.json().get("error_description")
                == "Rate limit exceeded: access_token not expired"
            ):
                return None
        except Exception as e:
            raise Exception(f"Failed converting response to a json, OAuth response: {token_response.text}")

        try:
            token_response.raise_for_status()
            logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
            )
        
        token_json = token_response.json()
        logger.info(f"Latest refresh token: {token_json['refresh_token']}")
        
        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = int(token_json["expires_in"]) + now

        with open(self._target.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

    def handle_401_response(self, rejected_authorization: Optional[str] = None) -> None:
        """Handle 401 Unauthorized response by refreshing the token.

        Args:
            rejected_authorization: Authorization header the API rejected. When another worker
                already replaced that token, it is reused instead of requesting a new grant.
        """
        with _token_lock:
            current_authorization = f"Bearer {self._config.get('access_token')}"
            if rejected_authorization and rejected_authorization != current_authorization:
                logger.info("Token already refreshed by another request")
                return
            logger.info("Received 401 Unauthorized response, refreshing token...")
            self.update_access_token()
            logger.info("Token refreshed after 401 response")