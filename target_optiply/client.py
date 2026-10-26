"""Optiply target sink class, which handles writing streams."""

from __future__ import annotations

import backoff
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase

from target_optiply.auth import OptiplyAuthenticator

logger = logging.getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder for datetime objects."""

    def default(self, obj):
        """Encode datetime objects."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

class OptiplySink(HotglueSink):
    """Optiply target sink class."""
    base_url = os.environ.get("optiply_base_url", "https://api.acceptance.optiply.com/v1")

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]] = None,
    ) -> None:
        """Initialize the sink.

        Args:
            target: The target instance.
            stream_name: The name of the stream.
            schema: The schema for the stream.
            key_properties: The key properties for the stream.
        """
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)
        self._authenticator = None
        self._session = None
        self._access_token = None
        self._token_expires_at = None

    @property
    def authenticator(self) -> OptiplyAuthenticator:
        """Get the authenticator instance.

        Returns:
            The authenticator instance.
        """
        if self._authenticator is None:
            # Get the config from target
            full_config = self._target._config
            self.logger.info(f"Full config keys: {list(full_config.keys())}")
            self.logger.info(f"Full config type: {type(full_config)}")
            
            # Use top-level config for authentication
            auth_config = {
                "client_id": full_config.get("client_id"),
                "client_secret": full_config.get("client_secret"),
                "username": full_config.get("username"),
                "password": full_config.get("password"),
                "access_token": full_config.get("access_token")
            }
            self.logger.info("✅ USING top-level config for authentication")
            
            # Log the final auth config being used
            self.logger.info(f"Final auth config keys: {list(auth_config.keys())}")
            self.logger.info(f"Final auth config client_id: {auth_config.get('client_id', 'NOT_FOUND')}")
            self.logger.info(f"Final auth config client_secret: {auth_config.get('client_secret', 'NOT_FOUND')[:8]}...{auth_config.get('client_secret', 'NOT_FOUND')[-4:] if len(auth_config.get('client_secret', '')) > 12 else '***'}")
            
            # Pass the target to the authenticator
            self._authenticator = OptiplyAuthenticator(self._target)
        return self._authenticator

    def http_headers(self) -> Dict[str, str]:
        """Get the HTTP headers for the request.

        Returns:
            The HTTP headers.
        """
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        headers.update({
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        })
        return headers

    def _get_error_message(self, response_text: str, status_code: int, url: str) -> str:
        """Get a meaningful error message from response text."""
        if not response_text or response_text.strip() in ['', 'null', 'None']:
            return f"No error details provided (status {status_code})"
        
        # Try to parse JSON error response
        try:
            import json
            error_data = json.loads(response_text)
            if isinstance(error_data, dict):
                if 'errors' in error_data and isinstance(error_data['errors'], list):
                    error_messages = []
                    for error in error_data['errors']:
                        if isinstance(error, dict):
                            if 'meta' in error and 'message' in error['meta']:
                                error_messages.append(error['meta']['message'])
                            elif 'detail' in error:
                                error_messages.append(error['detail'])
                            elif 'message' in error:
                                error_messages.append(error['message'])
                    if error_messages:
                        return f"API Error: {'; '.join(error_messages)}"
                elif 'message' in error_data:
                    return f"API Error: {error_data['message']}"
                elif 'error' in error_data:
                    return f"API Error: {error_data['error']}"
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
        
        # Fallback to raw response text if it's meaningful
        if len(response_text.strip()) > 0:
            return f"API Error: {response_text}"
        else:
            return f"No error details provided (status {status_code})"

    def validate_response(self, response: requests.Response) -> None:
        """Validate the response from the API.

        Args:
            response: The response to validate.

        Raises:
            FatalAPIError: If the response indicates a fatal error.
            RetriableAPIError: If the response indicates a retriable error.
        """
        if response.status_code >= 500:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise RetriableAPIError(f"Server error ({response.status_code}): {error_msg}")
        elif response.status_code == 404:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            logger.warning(f"Resource not found (404): {response.url} - {error_msg}")
            return
        elif response.status_code == 401:
            # 401 errors are handled in _request method with token refresh
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise FatalAPIError(f"Authentication failed after token refresh ({response.status_code}): {error_msg}")
        elif response.status_code >= 400:
            error_msg = self._get_error_message(response.text, response.status_code, response.url)
            raise FatalAPIError(f"Client error ({response.status_code}): {error_msg}")

    @backoff.on_exception(
        backoff.expo,
        (RetriableAPIError, requests.exceptions.ReadTimeout),
        max_tries=5,
        factor=2,
    )
    def _request(
        self, http_method, endpoint, params=None, request_data=None, headers=None
    ) -> requests.Response:
        """Make a request with automatic token refresh on 401 errors."""
        url = self.url(endpoint)
        headers = self.http_headers()

        # First attempt
        response = requests.request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data
        )
        
        # Handle 401 errors by refreshing token and retrying
        if response.status_code == 401:
            logger.info("Received 401 error, attempting to refresh token and retry")
            try:
                # Handle 401 response by refreshing token
                self.authenticator.handle_401_response(headers.get("Authorization"))
                
                # Get fresh headers with new token
                headers = self.http_headers()
                
                # Retry the request with new token
                response = requests.request(
                    method=http_method,
                    url=url,
                    params=params,
                    headers=headers,
                    json=request_data
                )
                logger.info("Successfully retried request after token refresh")
                
                # If we still get 401 after refresh, it's a fatal error
                if response.status_code == 401:
                    logger.error("Still getting 401 after token refresh - authentication failed")
                    raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
                    
            except Exception as e:
                logger.error(f"Failed to refresh token and retry: {str(e)}")
                raise
        
        self.validate_response(response)
        return response

    def url(self, endpoint: str = "") -> str:
        """Get the URL for the given endpoint.

        Args:
            endpoint: The endpoint to get the URL for.

        Returns:
            The URL for the endpoint.
        """
        # Add accountId and couplingId as query parameters if they exist
        params = {}
        if "account_id" in self.config:
            params["accountId"] = self.config["account_id"]
        if "coupling_id" in self.config:
            params["couplingId"] = self.config["coupling_id"]
        
        url = f"{self.base_url}/{endpoint}"
        if params:
            query_string = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query_string}"
        return url

    def request_api(self, http_method: str, endpoint: str = None, params: dict = {}, request_data: dict = None, headers: dict = {}) -> requests.Response:
        """Make an API request with retry logic."""
        import backoff
        
        @backoff.on_exception(backoff.expo, 
                             (requests.exceptions.RequestException, ConnectionResetError),
                             max_tries=3, max_time=30)
        def _make_request():
            # Use the url() method to properly include accountId and couplingId parameters
            url = self.url(endpoint)
            request_headers = self.http_headers().copy()
            if headers:
                request_headers.update(headers)
            
            self.logger.info(f"Making {http_method} request to: {endpoint}")
            if request_data:
                self.logger.info(f"REQUEST - endpoint: {endpoint}, request_body: {request_data}")
            
            response = requests.request(
                method=http_method,
                url=url,
                json=request_data,
                headers=request_headers,
                timeout=30
            )
            
            # Log response for debugging
            self.logger.info(f"Response status: {response.status_code}")
            if response.status_code >= 400:
                # Use the same error message helper function
                error_msg = self._get_error_message(response.text, response.status_code, url)
                self.logger.error(f"API Error: {response.status_code} - {error_msg}")
                # Log the request payload for server errors (500s)
                if response.status_code >= 500:
                    self.logger.error(f"Request payload that caused 500 error: {request_data}")
                    self.logger.error(f"Request headers: {request_headers}")
                    self.logger.error(f"Request URL: {url}")
            
            return response
        
        # Make the initial request
        response = _make_request()
        
        # Handle 401 errors by refreshing token and retrying
        if response.status_code == 401:
            logger.info("Received 401 error in request_api, attempting to refresh token and retry")
            try:
                # Handle 401 response by refreshing token
                self.authenticator.handle_401_response(response.request.headers.get("Authorization"))
                
                # Retry the request with new token
                response = _make_request()
                logger.info("Successfully retried request after token refresh")
                
                # If we still get 401 after refresh, it's a fatal error
                if response.status_code == 401:
                    logger.error("Still getting 401 after token refresh - authentication failed")
                    raise FatalAPIError(f"Authentication failed after token refresh: {response.text}")
                    
            except Exception as e:
                logger.error(f"Failed to refresh token and retry: {str(e)}")
                raise
        
        return response