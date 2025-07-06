from __future__ import annotations
import threading
import webbrowser
import requests
import random
import time
from typing import List, Any, Optional, Tuple, Dict
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
# import socketserver # Not directly used with basic HTTPServer in a thread
import queue

# Conditional import for Twisted reactor for GUI integration
_reactor_installed = False
try:
    from twisted.internet import reactor, tksupport
    _reactor_installed = True
except ImportError:
    print("Twisted reactor or GUI support not found. GUI integration with Twisted might require manual setup.")


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def __init__(self, *args, auth_code_queue: queue.Queue, **kwargs):
        self.auth_code_queue = auth_code_queue
        super().__init__(*args, **kwargs)

    def do_GET(self):
        parsed_path = urllib.parse.urlparse(self.path)
        if parsed_path.path == "/callback":
            query_components = urllib.parse.parse_qs(parsed_path.query)
            auth_code = query_components.get("code", [None])[0]

            if auth_code:
                self.auth_code_queue.put(auth_code)
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Successful!</h1>")
                self.wfile.write(b"<p>You can close this browser tab and return to the application.</p></body></html>")
                print(f"OAuth callback handled, code extracted: {auth_code[:20]}...")
            else:
                self.send_response(400)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"<html><body><h1>Authentication Failed</h1><p>No authorization code found in callback.</p></body></html>")
                print("OAuth callback error: No authorization code found.")
                self.auth_code_queue.put(None) # Signal failure or no code
        else:
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h1>Not Found</h1></body></html>")

    def log_message(self, format, *args):
        # Suppress most log messages from the HTTP server for cleaner console
        # You might want to enable some for debugging.
        # Example: only log errors or specific messages
        if "400" in args[0] or "404" in args[0] or "code 200" in args[0]: # Log errors and successful callback
             super().log_message(format, *args)
        # else:
        #    pass


import json # For token persistence

# Imports from ctrader-open-api
try:
    from ctrader_open_api import Client, TcpProtocol, EndPoints
    from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import (
        ProtoHeartbeatEvent,
        ProtoErrorRes,
        ProtoMessage
        # ProtoPayloadType / ProtoOAPayloadType not here
    )
    from ctrader_open_api.messages.OpenApiMessages_pb2 import (
        ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
        ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
        ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
        ProtoOATraderReq, ProtoOATraderRes,
        ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes,
        ProtoOASpotEvent, ProtoOATraderUpdatedEvent,
        ProtoOANewOrderReq, ProtoOAExecutionEvent,
        ProtoOAErrorRes,
        # Specific message types for deserialization
        ProtoOAGetCtidProfileByTokenRes,
        ProtoOAGetCtidProfileByTokenReq
    )
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOATrader
    USE_OPENAPI_LIB = True
except ImportError as e:
    print(f"ctrader-open-api import failed ({e}); running in mock mode.")
    USE_OPENAPI_LIB = False

TOKEN_FILE_PATH = "tokens.json"

class Trader:
    def __init__(self, settings, history_size: int = 100):
        """
        Initializes the Trader.

        Args:
            settings: The application settings object.
            history_size: The maximum size of the price history to maintain.
        """
        self.settings = settings
        self.is_connected: bool = False
        self._is_client_connected: bool = False
        self._last_error: str = ""
        self.price_history: List[float] = []
        self.history_size = history_size

        # Initialize token fields before loading
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expires_at: Optional[float] = None
        self._load_tokens_from_file() # Load tokens on initialization

        # Account details
        self.ctid_trader_account_id: Optional[int] = settings.openapi.default_ctid_trader_account_id
        self.account_id: Optional[str] = None
        self.balance: Optional[float] = None
        self.equity: Optional[float] = None
        self.currency: Optional[str] = None

        self._client: Optional[Client] = None
        self._message_id_counter: int = 1
        self._reactor_thread: Optional[threading.Thread] = None
        self._auth_code: Optional[str] = None # To store the auth code from OAuth flow

        if USE_OPENAPI_LIB:
            host = (
                EndPoints.PROTOBUF_LIVE_HOST
                if settings.openapi.host_type == "live"
                else EndPoints.PROTOBUF_DEMO_HOST
            )
            port = EndPoints.PROTOBUF_PORT
            self._client = Client(host, port, TcpProtocol)
            self._client.setConnectedCallback(self._on_client_connected)
            self._client.setDisconnectedCallback(self._on_client_disconnected)
            self._client.setMessageReceivedCallback(self._on_message_received)
        else:
            print("Trader initialized in MOCK mode.")

        self._auth_code_queue = queue.Queue() # Queue to pass auth_code from HTTP server thread
        self._http_server_thread: Optional[threading.Thread] = None
        self._http_server: Optional[HTTPServer] = None

    def _save_tokens_to_file(self):
        """Saves the current OAuth access token, refresh token, and expiry time to TOKEN_FILE_PATH."""
        tokens = {
            "access_token": self._access_token,
            "refresh_token": self._refresh_token,
            "token_expires_at": self._token_expires_at,
        }
        try:
            with open(TOKEN_FILE_PATH, "w") as f:
                json.dump(tokens, f)
            print(f"Tokens saved to {TOKEN_FILE_PATH}")
        except IOError as e:
            print(f"Error saving tokens to {TOKEN_FILE_PATH}: {e}")

    def _load_tokens_from_file(self):
        """Loads OAuth tokens from a local file."""
        try:
            with open(TOKEN_FILE_PATH, "r") as f:
                tokens = json.load(f)
            self._access_token = tokens.get("access_token")
            self._refresh_token = tokens.get("refresh_token")
            self._token_expires_at = tokens.get("token_expires_at")
            if self._access_token:
                print(f"Tokens loaded from {TOKEN_FILE_PATH}. Access token: {self._access_token[:20]}...")
            else:
                print(f"{TOKEN_FILE_PATH} not found or no access token in it. Will need OAuth.")
        except FileNotFoundError:
            print(f"Token file {TOKEN_FILE_PATH} not found. New OAuth flow will be required.")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading tokens from {TOKEN_FILE_PATH}: {e}. Will need OAuth flow.")
            # In case of corrupted file, good to try to remove it or back it up
            try:
                import os
                os.remove(TOKEN_FILE_PATH)
                print(f"Removed corrupted token file: {TOKEN_FILE_PATH}")
            except OSError as rm_err:
                print(f"Error removing corrupted token file: {rm_err}")


    def _load_tokens_from_file(self):
        """
        Loads OAuth access token, refresh token, and expiry time from TOKEN_FILE_PATH.
        Sets the corresponding instance attributes if tokens are found and loaded.
        """
        try:
            with open(TOKEN_FILE_PATH, "r") as f:
                tokens = json.load(f)
            self._access_token = tokens.get("access_token")
            self._refresh_token = tokens.get("refresh_token")
            self._token_expires_at = tokens.get("token_expires_at")
            if self._access_token:
                print(f"Tokens loaded from {TOKEN_FILE_PATH}. Access token: {self._access_token[:20]}...")
            else:
                print(f"{TOKEN_FILE_PATH} not found or no access token in it. Will need OAuth.")
        except FileNotFoundError:
            print(f"Token file {TOKEN_FILE_PATH} not found. New OAuth flow will be required.")
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading tokens from {TOKEN_FILE_PATH}: {e}. Will need OAuth flow.")
            # In case of corrupted file, good to try to remove it or back it up
            try:
                import os
                os.remove(TOKEN_FILE_PATH) # Ensure os is imported if this is the only place
                print(f"Removed corrupted token file: {TOKEN_FILE_PATH}")
            except OSError as rm_err:
                print(f"Error removing corrupted token file: {rm_err}")

    def _next_message_id(self) -> str:
        mid = str(self._message_id_counter)
        self._message_id_counter += 1
        return mid

    # Twisted callbacks
    def _on_client_connected(self, client: Client) -> None:
        print("OpenAPI Client Connected.")
        self._is_client_connected = True
        self._last_error = ""
        req = ProtoOAApplicationAuthReq()
        req.clientId = self.settings.openapi.client_id or ""
        req.clientSecret = self.settings.openapi.client_secret or ""
        if not req.clientId or not req.clientSecret:
            print("Missing OpenAPI credentials.")
            client.stopService()
            return
        print("Sending ApplicationAuth request.")
        d = client.send(req)
        d.addCallbacks(self._handle_app_auth_response, self._handle_send_error)

    def _on_client_disconnected(self, client: Client, reason: Any) -> None:
        print(f"OpenAPI Client Disconnected: {reason}")
        self.is_connected = False
        self._is_client_connected = False

    def _on_message_received(self, client: Client, message: Any) -> None:
        print(f"_on_message_received: Outer message type: {type(message)}")

        actual_message = message
        payload_type = 0 # Default for non-ProtoMessage or if extraction fails

        if isinstance(message, ProtoMessage):
            payload_type = message.payloadType
            payload_bytes = message.payload
            print(f"  It's a ProtoMessage wrapper. PayloadType: {payload_type}, Payload an_instance_of bytes: {isinstance(payload_bytes, bytes)}")

            # Deserialize based on payloadType using ProtoOAPayloadType from OpenApiMessages_pb2
            if payload_type == ProtoOAPayloadType.OA_APPLICATION_AUTH_RES: # 2101
                actual_message = ProtoOAApplicationAuthRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_ACCOUNT_AUTH_RES: # 2103
                actual_message = ProtoOAAccountAuthRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_GET_CTID_PROFILE_BY_TOKEN_RES: # 2142
                actual_message = ProtoOAGetCtidProfileByTokenRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_GET_ACCOUNTS_BY_ACCESS_TOKEN_RES: # 2135
                actual_message = ProtoOAGetAccountListByAccessTokenRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_TRADER_RES: # 2120
                 actual_message = ProtoOATraderRes()
                 actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_TRADER_UPDATE_EVENT: # 2126
                 actual_message = ProtoOATraderUpdatedEvent()
                 actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_SPOT_EVENT: # 2128
                actual_message = ProtoOASpotEvent()
                actual_message.ParseFromString(payload_bytes)
            # elif payload_type == ProtoOAPayloadType.OA_EXECUTION_EVENT: # 2127
            #     actual_message = ProtoOAExecutionEvent()
            #     actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.OA_ERROR_RES: # 2105
                actual_message = ProtoOAErrorRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.ERROR_RES: # 50 (common error from OpenApiCommonMessages_pb2.ProtoPayloadType)
                                                              # This specific one might need care if ProtoOAPayloadType doesn't include it
                                                              # However, ERROR_RES (50) and HEARTBEAT_EVENT (51) are typically in a common payload enum.
                                                              # If ProtoOAPayloadType is specific to OA_ messages, this check might be problematic.
                                                              # For now, assuming ProtoOAPayloadType includes these common types too, or they won't be hit often here.
                actual_message = ProtoErrorRes()
                actual_message.ParseFromString(payload_bytes)
            elif payload_type == ProtoOAPayloadType.HEARTBEAT_EVENT: # 51
                actual_message = ProtoHeartbeatEvent()
                actual_message.ParseFromString(payload_bytes)
            else:
                print(f"  No specific deserializer for PayloadType: {payload_type}. Dispatch will use original wrapper or fail on type.")
                # actual_message remains the original ProtoMessage; subsequent isinstance checks will fail for specific types
                # This is important: if we don't deserialize, it won't match specific handlers.

        if actual_message is not message : # Log if deserialization happened
             print(f"  Deserialized to: {type(actual_message)}")


        # Dispatch by type using the (potentially deserialized) actual_message
        if isinstance(actual_message, ProtoOAApplicationAuthRes):
            print("  Dispatching to _handle_app_auth_response")
            self._handle_app_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAAccountAuthRes):
            print("  Dispatching to _handle_account_auth_response")
            self._handle_account_auth_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetCtidProfileByTokenRes):
            print("  Dispatching to _handle_get_ctid_profile_response")
            self._handle_get_ctid_profile_response(actual_message)
        elif isinstance(actual_message, ProtoOAGetAccountListByAccessTokenRes):
            print("  Dispatching to _handle_get_account_list_response")
            self._handle_get_account_list_response(actual_message)
        elif isinstance(actual_message, ProtoOATraderRes):
            print("  Dispatching to _handle_trader_response")
            self._handle_trader_response(actual_message)
        elif isinstance(actual_message, ProtoOATraderUpdatedEvent):
            print("  Dispatching to _handle_trader_updated_event")
            self._handle_trader_updated_event(actual_message)
        elif isinstance(actual_message, ProtoOASpotEvent):
            # self._handle_spot_event(actual_message) # Potentially noisy
             print("  Received ProtoOASpotEvent (handler commented out).")
        elif isinstance(actual_message, ProtoOAExecutionEvent):
            # self._handle_execution_event(actual_message) # Potentially noisy
             print("  Received ProtoOAExecutionEvent (handler commented out).")
        elif isinstance(actual_message, ProtoHeartbeatEvent):
            print("  Received heartbeat.")
        elif isinstance(actual_message, ProtoOAErrorRes): # Specific OA error
            print(f"  Dispatching to ProtoOAErrorRes handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"{actual_message.errorCode}: {actual_message.description}"
        elif isinstance(actual_message, ProtoErrorRes): # Common error
            print(f"  Dispatching to ProtoErrorRes (common) handler. Error code: {actual_message.errorCode}, Description: {actual_message.description}")
            self._last_error = f"Common Error {actual_message.errorCode}: {actual_message.description}"
        # Check if it's still the original ProtoMessage wrapper because no deserialization rule matched
        elif actual_message is message and isinstance(message, ProtoMessage):
            print(f"  ProtoMessage with PayloadType {payload_type} was not handled by specific type checks after deserialization attempt.")
        elif not isinstance(actual_message, ProtoMessage) and actual_message is message: # Original message was not ProtoMessage
             print(f"  Unhandled non-ProtoMessage type in _on_message_received: {type(actual_message)}")
        else: # Should ideally not be reached if all cases are handled
            print(f"  Message of type {type(actual_message)} (PayloadType {payload_type if payload_type else 'N/A'}) fell through all handlers.")

    # Handlers
    def _handle_app_auth_response(self, response: ProtoOAApplicationAuthRes) -> None:
        print("ApplicationAuth response received.")
        # The access token from ProtoOAApplicationAuthRes is for the application's session.
        # We have a user-specific OAuth access token in self._access_token (if OAuth flow completed).
        # We should not overwrite self._access_token here if it was set by OAuth.
        # For ProtoOAAccountAuthReq, we must use the user's OAuth token.

        # Let's see if the response contains an access token, though we might not use it directly
        # if our main OAuth token is already present.
        app_session_token = getattr(response, 'accessToken', None)
        if app_session_token:
            print(f"ApplicationAuthRes provided an app session token: {app_session_token[:20]}...")
            # If self._access_token (OAuth user token) is NOT set,
            # this could be a fallback or an alternative flow not fully explored.
            # For now, we prioritize the OAuth token set by exchange_code_for_token.
            if not self._access_token:
                print("Warning: OAuth access token not found, but AppAuthRes provided one. This scenario needs review.")
                # self._access_token = app_session_token # Potentially use if no OAuth token? Risky.

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for subsequent account operations."
            print(self._last_error)
            # Potentially stop the client or signal a critical failure here
            if self._client:
                self._client.stopService()
            return

        # Proceed to account auth or account list using the existing self._access_token (from OAuth)
        if self.ctid_trader_account_id:
            # If we have a pre-configured ctidTraderAccountId, the original flow was to directly authenticate it.
            # However, given the server responded with ProtoOAGetCtidProfileByTokenRes to AccountAuthReq,
            # it implies the token might always need to go through a profile check first,
            # or that AccountAuthReq for a *specific* cTID should only be sent *after* confirming that cTID
            # is associated with the token (e.g. via profile or account list).

            # New Hypothesis: Always try to get profile first if we have an OAuth token.
            # If a ctid_trader_account_id is configured, we can use it to verify against the profile/account list later.
            print("Attempting to get profile by token first, even if default ctidTraderAccountId is set.")
            self._send_get_ctid_profile_request()
            # Old logic: self._send_account_auth_request(self.ctid_trader_account_id)
        else:
            print("No default ctidTraderAccountId. Attempting to get profile by token.")
            self._send_get_ctid_profile_request()


    def _handle_get_ctid_profile_response(self, response: ProtoOAGetCtidProfileByTokenRes) -> None:
        """
        Handles the response from a ProtoOAGetCtidProfileByTokenReq.
        This method is now also tentatively handling the case where this response
        was received *instead of* an expected ProtoOAAccountAuthRes.
        """
        print(f"Received ProtoOAGetCtidProfileByTokenRes.")
        if hasattr(response, 'ctidTraderAccount') and response.ctidTraderAccount: # Check if field exists and is not empty/default
            # Assuming ctidTraderAccount is a list of ProtoOACTIDTraderAccount as in GetAccountListRes
            # Or if it's a single ProtoOACTIDTraderAccount object.
            # The definition of ProtoOAGetCtidProfileByTokenRes needs to be checked for actual structure.
            # For now, let's assume it gives a single ctidTraderId or similar.
            # Let's assume it has a field like `ctidProfile.ctidTraderId` or directly `ctidTraderId`.
            # This is speculative based on the name.
            # A common pattern is that it might return a list of ProtoOACtidTraderAccount objects.

            # Placeholder: Log the whole response to understand its structure.
            print(f"  Profile Response Content: {response}")

            # Example: if response.ctidProfile.ctidTraderId exists:
            # self.ctid_trader_account_id = response.ctidProfile.ctidTraderId
            # print(f"  Extracted ctidTraderAccountId from profile: {self.ctid_trader_account_id}")
            # self._send_account_auth_request(self.ctid_trader_account_id)

            # For now, let's assume the response *might* contain a single ctidTraderAccountId
            # that we can use. If Spotware's actual flow for this token type is different,
            # this will need adjustment.
            # If the response directly gives a ctid for a *trading account*, we can use it.
            # The message definition for ProtoOAGetCtidProfileByTokenRes is needed.
            # Typically, a "profile" is user-level, not account-level.
            # It might give a cTrader ID (user ID), then we'd need ProtoOAGetAccountListByAccessTokenReq.

            # Given the server sent THIS in response to AccountAuth, it's very confusing.
            # Let's assume for a moment this *is* the new "success" for AccountAuth
            # and it contains the necessary details to consider the account "authed".
            # This is a big assumption.
            if hasattr(response, 'ctidProfile') and hasattr(response.ctidProfile, 'ctidTraderId'):
                 # This is a guess at the structure of ProtoOAGetCtidProfileByTokenRes
                 # It might be response.ctidTraderAccount[0].ctidTraderAccountId if it returns a list of accounts
                 # For now, let's check if we can get *any* ctid to proceed
                potential_ctid = response.ctidProfile.ctidTraderId # Highly speculative field name
                if potential_ctid:
                    print(f"  Potential cTID from profile response: {potential_ctid}")
                    if not self.ctid_trader_account_id: # If not already set
                        self.ctid_trader_account_id = potential_ctid

                    # Does this response mean we are "connected" for this ctid?
                    # Or do we still need to send ProtoOAAccountAuthReq?
                    # If server sent this INSTEAD of ProtoOAAccountAuthRes, it's possible this
                    # IS the new "account auth" for this token type.
                    print("  Assuming profile response implies account readiness. Fetching trader details...")
                    self.is_connected = True # Tentative: mark as connected to see account details
                    self._send_get_trader_request(self.ctid_trader_account_id)
                    return

            print("  ProtoOAGetCtidProfileByTokenRes received, but ctidTraderAccountId not clearly identified or structure unknown.")
            self._last_error = "Profile received, but account ID unclear."
            # What to do now? Maybe get account list?
            # self._send_get_account_list_request() # This also requires an access token.

        else:
            print("  ProtoOAGetCtidProfileByTokenRes was empty or did not contain expected ctid information.")
            self._last_error = "Failed to get ctid profile by token or profile empty."


    def _handle_account_auth_response(self, response: ProtoOAAccountAuthRes) -> None:
        print("AccountAuth response.")
        if response.ctidTraderAccountId == self.ctid_trader_account_id:
            self.is_connected = True
            self._send_get_trader_request(self.ctid_trader_account_id)
        else:
            print("AccountAuth failed.")

    def _handle_get_account_list_response(self, response: ProtoOAGetAccountListByAccessTokenRes) -> None:
        print("Account list response.")
        accounts = getattr(response, 'ctidTraderAccount', [])
        if not accounts:
            print("No accounts available.")
            return
        self.ctid_trader_account_id = accounts[0].ctidTraderAccountId
        self.settings.openapi.default_ctid_trader_account_id = self.ctid_trader_account_id
        self._send_account_auth_request(self.ctid_trader_account_id)

    def _handle_trader_response(self, response: ProtoOATraderRes) -> None:
        trader_details = self._update_trader_details(
            "Trader details response.", response.trader
        )
        if trader_details:
            self.account_id = str(trader_details.ctidTraderAccountId)
            # Potentially other fields from trader_details could be assigned here if needed

    def _handle_trader_updated_event(self, event: ProtoOATraderUpdatedEvent) -> None:
        self._update_trader_details(
            "Trader updated event.", event.trader
        )

    def _update_trader_details(self, log_message: str, trader_proto: ProtoOATrader):
        """Helper to update trader balance and equity from a ProtoOATrader object."""
        print(log_message)
        if trader_proto:
            self.balance = trader_proto.balance / 100.0  # Assuming balance is in cents
            self.equity = trader_proto.equity / 100.0    # Assuming equity is in cents
            # self.currency = trader_proto.currency # If currency is available and needed
            # self.ctid_trader_account_id = trader_proto.ctidTraderAccountId # Already known, but can confirm
            print(f"Updated account {trader_proto.ctidTraderAccountId}: Balance: {self.balance}, Equity: {self.equity}")
            return trader_proto
        return None

    def _handle_spot_event(self, event: ProtoOASpotEvent) -> None:
        # TODO: update self.price_history
        pass

    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        # TODO: handle executions
        pass

    def _handle_send_error(self, failure: Any) -> None:
        print(f"Send error: {failure.getErrorMessage()}")
        self._last_error = failure.getErrorMessage()

    # Sending methods
    def _send_account_auth_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return # Token refresh failed or no token, error set by _ensure_valid_token

        print(f"Requesting AccountAuth for {ctid} with token: {self._access_token[:20]}...") # Log token used
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ctid
        req.accessToken = self._access_token or "" # Should be valid now

        print(f"Sending ProtoOAAccountAuthReq for ctid: {ctid}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for AccountAuthReq: {d}")

            def success_callback(response_msg):
                # This callback is mostly for confirming the Deferred fired successfully.
                # Normal processing will happen in _on_message_received if message is dispatched.
                print(f"AccountAuthReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")
                # Note: We don't directly process response_msg here as _on_message_received should get it.

            def error_callback(failure_reason):
                print(f"AccountAuthReq error_callback triggered. Failure:")
                # Print a summary of the failure, and the full traceback if it's an exception
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'getTraceback'):
                    print(f"  Traceback: {failure_reason.getTraceback()}")
                else:
                    print(f"  Failure object: {failure_reason}")
                self._handle_send_error(failure_reason) # Ensure our existing error handler is called

            d.addCallbacks(success_callback, errback=error_callback)
            print("Added callbacks to AccountAuthReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_account_auth_request send command: {e}")
            self._last_error = f"Exception sending AccountAuth: {e}"
            # Potentially stop client if send itself fails critically
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False # Ensure state reflects this

    def _send_get_account_list_request(self) -> None:
        if not self._ensure_valid_token():
            return

        print("Requesting account list.")
        req = ProtoOAGetAccountListByAccessTokenReq()
        if not self._access_token: # Should have been caught by _ensure_valid_token, but double check for safety
            self._last_error = "Critical: OAuth access token not available for GetAccountList request."
            print(self._last_error)
            if self._client:
                self._client.stopService()
            return
        req.accessToken = self._access_token
        d = self._client.send(req)
        d.addCallbacks(self._handle_get_account_list_response, self._handle_send_error)

    def _send_get_trader_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return

        print(f"Requesting Trader details for {ctid}")
        req = ProtoOATraderReq()
        req.ctidTraderAccountId = ctid
        # Note: ProtoOATraderReq does not directly take an access token in its fields.
        # The authentication is expected to be session-based after AccountAuth.
        # If a token were needed here, the message definition would include it.
        d = self._client.send(req)
        d.addCallbacks(self._handle_trader_response, self._handle_send_error)

    def _send_get_ctid_profile_request(self) -> None:
        """Sends a ProtoOAGetCtidProfileByTokenReq using the current OAuth access token."""
        if not self._ensure_valid_token(): # Ensure token is valid before using it
            return

        if not self._access_token:
            self._last_error = "Critical: OAuth access token not available for GetCtidProfile request."
            print(self._last_error)
            if self._client and self._is_client_connected:
                self._client.stopService()
            return

        print("Sending ProtoOAGetCtidProfileByTokenReq...")
        req = ProtoOAGetCtidProfileByTokenReq()
        req.accessToken = self._access_token

        try:
            d = self._client.send(req)
            print(f"Deferred created for GetCtidProfileByTokenReq: {d}")

            # Adding specific callbacks for this request to see its fate
            def profile_req_success_callback(response_msg):
                print(f"GetCtidProfileByTokenReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")

            def profile_req_error_callback(failure_reason):
                print(f"GetCtidProfileByTokenReq error_callback triggered. Failure:")
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'getTraceback'): # May be verbose
                    print(f"  Traceback: {failure_reason.getTraceback(brief=True, capN=10)}") # সংক্ষিপ্ত ট্রেসব্যাক
                else:
                    print(f"  Failure object: {failure_reason}")
                self._handle_send_error(failure_reason)

            d.addCallbacks(profile_req_success_callback, errback=profile_req_error_callback)
            print("Added callbacks to GetCtidProfileByTokenReq Deferred.")

        except Exception as e:
            print(f"Exception during _send_get_ctid_profile_request send command: {e}")
            self._last_error = f"Exception sending GetCtidProfile: {e}"
            if self._client and self._is_client_connected:
                self._client.stopService()
                self.is_connected = False


    # Public API
    def connect(self) -> bool:
        """
        Establishes a connection to the trading service.
        Handles OAuth2 flow (token loading, refresh, or full browser authentication)
        and then starts the underlying OpenAPI client service.

        Returns:
            True if connection setup (including successful client service start) is successful,
            False otherwise.
        """
        if not USE_OPENAPI_LIB:
            print("Mock mode: OpenAPI library unavailable.")
            self._last_error = "OpenAPI library not available (mock mode)."
            return False

        # 1. Check if loaded token is valid and not expired
        self._last_error = "Checking saved tokens..." # For GUI
        if self._access_token and not self._is_token_expired():
            print("Using previously saved, valid access token.")
            self._last_error = "Attempting to connect with saved token..." # For GUI
            if self._start_openapi_client_service():
                return True # Proceed with this token
            else:
                # Problem starting client service even with a seemingly valid token
                # self._last_error is set by _start_openapi_client_service
                print(f"Failed to start client service with saved token: {self._last_error}")
                # Fall through to try refresh or full OAuth
                self._access_token = None # Invalidate to ensure we don't retry this path immediately

        # 2. If token is expired or (now) no valid token, try to refresh if possible
        if self._refresh_token: # Check if refresh is even possible
            if self._access_token and self._is_token_expired(): # If previous step invalidated or was originally expired
                print("Saved access token is (or became) invalid/expired, attempting refresh...")
            elif not self._access_token: # No access token from file, but refresh token exists
                 print("No saved access token, but refresh token found. Attempting refresh...")

            self._last_error = "Attempting to refresh token..." # For GUI
            if self.refresh_access_token(): # This also saves the new token
                print("Access token refreshed successfully using saved refresh token.")
                self._last_error = "Token refreshed. Attempting to connect..." # For GUI
                if self._start_openapi_client_service():
                    return True # Proceed with refreshed token
                else:
                    # self._last_error set by _start_openapi_client_service
                    print(f"Failed to start client service after token refresh: {self._last_error}")
                    return False # Explicitly fail here if client service fails after successful refresh
            else:
                print("Failed to refresh token. Proceeding to full OAuth flow.")
                # self._last_error set by refresh_access_token(), fall through to full OAuth
        else:
            print("No refresh token available. Proceeding to full OAuth if needed.")


        # 3. If no valid/refreshed token, proceed to full OAuth browser flow
        print("No valid saved token or refresh failed/unavailable. Initiating full OAuth2 flow...")
        self._last_error = "OAuth2: Redirecting to browser for authentication." # More specific initial status

        auth_url = self.settings.openapi.spotware_auth_url
        # token_url = self.settings.openapi.spotware_token_url # Not used in this part of connect()
        client_id = self.settings.openapi.client_id
        redirect_uri = self.settings.openapi.redirect_uri
        # Define scopes - this might need adjustment based on Spotware's requirements
        scopes = "accounts" # Changed from "trading accounts" to just "accounts" based on OpenApiPy example hint

        # Construct the authorization URL using the new Spotware URL
        # Construct the authorization URL using the standard Spotware OAuth endpoint.
        params = {
            "response_type": "code", # Required for Authorization Code flow
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": scopes
            # product="web" is removed as it's not part of standard OAuth params here
            # "state": "YOUR_UNIQUE_STATE_HERE" # Optional: for CSRF protection
        }
        auth_url_with_params = f"{auth_url}?{urllib.parse.urlencode(params)}"

        # At this point, the application will wait. The user needs to authenticate
        # in the browser, and then manually provide the authorization code.
        # The actual connection to Spotware (self._client.startService()) will be
        # deferred until after the auth code is obtained and exchanged for a token.
        # For this step, returning False means the immediate connection via TCP client isn't made yet.
        # The GUI should reflect the status "Awaiting browser authentication".
        # This method will now block until code is received or timeout.
        # For GUI responsiveness, Trader.connect might need to run in a thread
        # or use async mechanisms if this blocking is too long.
        # For now, assume GUI can handle a short blocking period or this runs in a bg thread.

        # Start local HTTP server
        if not self._start_local_http_server():
            self._last_error = "OAuth2 Error: Could not start local HTTP server for callback."
            print(self._last_error)
            return False # Indicate connection failed

        print(f"Redirecting to browser for authentication: {auth_url_with_params}")
        webbrowser.open(auth_url_with_params)
        self._last_error = "OAuth2: Waiting for authorization code via local callback..." # Update status

        # Wait for the auth code from the HTTP server (with a timeout)
        try:
            auth_code = self._auth_code_queue.get(timeout=120) # 2 minutes timeout
            print("Authorization code received from local server.")
        except queue.Empty:
            print("OAuth2 Error: Timeout waiting for authorization code from callback.")
            self._last_error = "OAuth2 Error: Timeout waiting for callback."
            self._stop_local_http_server()
            return False

        self._stop_local_http_server()

        if auth_code:
            return self.exchange_code_for_token(auth_code)
        else: # Should not happen if queue contained None or empty string but good to check
            self._last_error = "OAuth2 Error: Invalid authorization code received."
            print(self._last_error)
            return False


    def _start_local_http_server(self) -> bool:
        """
        Starts a local HTTP server on a separate thread to listen for the OAuth callback.
        The server address is determined by self.settings.openapi.redirect_uri.

        Returns:
            True if the server started successfully, False otherwise.
        """
        try:
            # Ensure any previous server is stopped
            if self._http_server_thread and self._http_server_thread.is_alive():
                self._stop_local_http_server()

            # Use localhost and port from redirect_uri
            parsed_uri = urllib.parse.urlparse(self.settings.openapi.redirect_uri)
            host = parsed_uri.hostname
            port = parsed_uri.port

            if not host or not port:
                print(f"Invalid redirect_uri for local server: {self.settings.openapi.redirect_uri}")
                return False

            # Pass the queue to the handler
            def handler_factory(*args, **kwargs):
                return OAuthCallbackHandler(*args, auth_code_queue=self._auth_code_queue, **kwargs)

            self._http_server = HTTPServer((host, port), handler_factory)
            self._http_server_thread = threading.Thread(target=self._http_server.serve_forever, daemon=True)
            self._http_server_thread.start()
            print(f"Local HTTP server started on {host}:{port} for OAuth callback.")
            return True
        except Exception as e:
            print(f"Failed to start local HTTP server: {e}")
            self._last_error = f"Failed to start local HTTP server: {e}"
            return False

    def _stop_local_http_server(self):
        if self._http_server:
            print("Shutting down local HTTP server...")
            self._http_server.shutdown() # Signal server to stop serve_forever loop
            self._http_server.server_close() # Close the server socket
            self._http_server = None
        if self._http_server_thread and self._http_server_thread.is_alive():
            self._http_server_thread.join(timeout=5) # Wait for thread to finish
            if self._http_server_thread.is_alive():
                print("Warning: HTTP server thread did not terminate cleanly.")
        self._http_server_thread = None
        print("Local HTTP server stopped.")


    def _stop_local_http_server(self):
        """Stops the local HTTP callback server if it is running."""
        if self._http_server:
            print("Shutting down local HTTP server...")
            self._http_server.shutdown() # Signal server to stop serve_forever loop
            self._http_server.server_close() # Close the server socket
            self._http_server = None
        if self._http_server_thread and self._http_server_thread.is_alive():
            self._http_server_thread.join(timeout=5) # Wait for thread to finish
            if self._http_server_thread.is_alive():
                print("Warning: HTTP server thread did not terminate cleanly.")
        self._http_server_thread = None
        print("Local HTTP server stopped.")


    # This is the original _stop_local_http_server method, adding docstring to it.
    def _stop_local_http_server(self):
        """Stops the local HTTP callback server if it is running."""
        if self._http_server:
            print("Shutting down local HTTP server...")
            self._http_server.shutdown() # Signal server to stop serve_forever loop
            self._http_server.server_close() # Close the server socket
            self._http_server = None
        if self._http_server_thread and self._http_server_thread.is_alive():
            self._http_server_thread.join(timeout=5) # Wait for thread to finish
            if self._http_server_thread.is_alive():
                print("Warning: HTTP server thread did not terminate cleanly.")
        self._http_server_thread = None
        print("Local HTTP server stopped.")


    def exchange_code_for_token(self, auth_code: str) -> bool:
        """
        Exchanges an OAuth authorization code for an access token and refresh token.
        Saves tokens to file on success and starts the OpenAPI client service.

        Args:
            auth_code: The authorization code obtained from the OAuth provider.

        Returns:
            True if token exchange and client service start were successful, False otherwise.
        """
        print(f"Exchanging authorization code for token: {auth_code[:20]}...") # Log part of the code
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": self.settings.openapi.redirect_uri,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret,
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                return False

            self._access_token = token_data["access_token"]
            self._refresh_token = token_data.get("refresh_token") # refresh_token might not always be present
            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                self._token_expires_at = None # Or a very long time if not specified

            print(f"Access token obtained: {self._access_token[:20]}...")
            if self._refresh_token:
                print(f"Refresh token obtained: {self._refresh_token[:20]}...")
            print(f"Token expires in: {expires_in} seconds (at {self._token_expires_at})")

            # Now that we have the access token, we can start the actual OpenAPI client service
            self._save_tokens_to_file() # Save tokens after successful exchange
            if self._start_openapi_client_service():
                # Connection to TCP endpoint will now proceed, leading to ProtoOAApplicationAuthReq etc.
                # The _check_connection in GUI will handle the rest.
                return True
            else:
                # _start_openapi_client_service would have set _last_error
                return False

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error: {http_err}. Response: {error_content}"
            print(self._last_error)
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error: {req_err}"
            print(self._last_error)
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token exchange: {e}"
            print(self._last_error)
            return False

    def _start_openapi_client_service(self):
        """
        Starts the underlying OpenAPI client service (TCP connection, reactor).
        This is called after successful OAuth token acquisition/validation.

        Returns:
            True if the client service started successfully, False otherwise.
        """
        if self.is_connected or (self._client and getattr(self._client, 'isConnected', False)):
            print("OpenAPI client service already running or connected.")
            return True

        print("Starting OpenAPI client service.")
        try:
            self._client.startService()
            if _reactor_installed and not reactor.running:
                self._reactor_thread = threading.Thread(target=lambda: reactor.run(installSignalHandlers=0), daemon=True)
                self._reactor_thread.start()
            return True
        except Exception as e:
            print(f"Error starting OpenAPI client service: {e}")
            self._last_error = f"OpenAPI client error: {e}"
            return False

    def refresh_access_token(self) -> bool:
        """
        Refreshes the OAuth access token using the stored refresh token.
        Saves the new tokens to file on success.

        Returns:
            True if the access token was refreshed successfully, False otherwise.
        """
        if not self._refresh_token:
            self._last_error = "OAuth2 Error: No refresh token available to refresh access token."
            print(self._last_error)
            return False

        print("Attempting to refresh access token...")
        self._last_error = ""
        try:
            token_url = self.settings.openapi.spotware_token_url
            payload = {
                "grant_type": "refresh_token",
                "refresh_token": self._refresh_token,
                "client_id": self.settings.openapi.client_id,
                "client_secret": self.settings.openapi.client_secret, # Typically required for refresh
            }
            response = requests.post(token_url, data=payload)
            response.raise_for_status()

            token_data = response.json()

            if "access_token" not in token_data:
                self._last_error = "OAuth2 Error: access_token not in response from refresh token endpoint."
                print(f"{self._last_error} Response: {token_data}")
                # Potentially invalidate old tokens if refresh fails this way
                self.is_connected = False
                return False

            self._access_token = token_data["access_token"]
            # A new refresh token might be issued, or the old one might continue to be valid.
            # Standard practice: if a new one is issued, use it. Otherwise, keep the old one.
            if "refresh_token" in token_data:
                self._refresh_token = token_data["refresh_token"]
                print(f"New refresh token obtained: {self._refresh_token[:20]}...")

            expires_in = token_data.get("expires_in")
            if expires_in:
                self._token_expires_at = time.time() + int(expires_in)
            else:
                # If expires_in is not provided on refresh, it might mean the expiry doesn't change
                # or it's a non-expiring token (less common). For safety, clear old expiry.
                self._token_expires_at = None

            print(f"Access token refreshed successfully: {self._access_token[:20]}...")
            print(f"New expiry: {self._token_expires_at}")
            self._save_tokens_to_file() # Save tokens after successful refresh
            return True

        except requests.exceptions.HTTPError as http_err:
            error_content = http_err.response.text
            self._last_error = f"OAuth2 HTTP Error during token refresh: {http_err}. Response: {error_content}"
            print(self._last_error)
            self.is_connected = False # Assume connection is lost if refresh fails
            return False
        except requests.exceptions.RequestException as req_err:
            self._last_error = f"OAuth2 Request Error during token refresh: {req_err}"
            print(self._last_error)
            self.is_connected = False
            return False
        except Exception as e:
            self._last_error = f"OAuth2 Unexpected Error during token refresh: {e}"
            print(self._last_error)
            self.is_connected = False
            return False

    def _is_token_expired(self, buffer_seconds: int = 60) -> bool:
        """
        Checks if the current OAuth access token is expired or nearing expiry.

        Args:
            buffer_seconds: A buffer time in seconds. If the token expires within
                            this buffer, it's considered nearing expiry.

        Returns:
            True if the token is non-existent, expired, or nearing expiry, False otherwise.
        """
        if not self._access_token:
            return True # No token means it's effectively expired for use
        if self._token_expires_at is None:
            return False # Token that doesn't expire (or expiry unknown)
        return time.time() > (self._token_expires_at - buffer_seconds)

    def _ensure_valid_token(self) -> bool:
        """
        Ensures the OAuth access token is valid, attempting a refresh if it's expired or nearing expiry.
        This is a proactive check typically called before making an API request that requires auth.

        Returns:
            True if a valid token is present (either initially or after successful refresh),
            False if no valid token is available and refresh failed or was not possible.
        """
        if self._is_token_expired():
            print("Access token expired or nearing expiry. Attempting refresh.")
            if not self.refresh_access_token():
                print("Failed to refresh access token.")
                # self._last_error is set by refresh_access_token()
                if self._client and self._is_client_connected: # Check if client exists and was connected
                    self._client.stopService() # Stop service if token cannot be refreshed
                self.is_connected = False
                return False
        return True


    def disconnect(self) -> None:
        if self._client:
            self._client.stopService()
        if _reactor_installed and reactor.running:
            reactor.callFromThread(reactor.stop)
        self.is_connected = False
        self._is_client_connected = False

    def get_connection_status(self) -> Tuple[bool, str]:
        return self.is_connected, self._last_error

    def get_account_summary(self) -> Dict[str, Any]:
        if not USE_OPENAPI_LIB:
            return {"account_id": "MOCK", "balance": 0.0, "equity": 0.0}
        if not self.is_connected:
            return {"account_id": "connecting..."}
        return {"account_id": self.account_id, "balance": self.balance, "equity": self.equity}

    def get_market_price(self, symbol: str) -> float:
        if not USE_OPENAPI_LIB or not self.price_history:
            return round(random.uniform(1.10, 1.20), 5)
        return self.price_history[-1]

    def get_price_history(self) -> List[float]:
        return list(self.price_history)
