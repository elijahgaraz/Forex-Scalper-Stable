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
import sys
import traceback

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
    from ctrader_open_api import Client, TcpProtocol, EndPoints, Protobuf
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
        ProtoOAGetCtidProfileByTokenReq,
        ProtoOASymbolsListReq, ProtoOASymbolsListRes, # For getting all symbols
        ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes, # For spot subscription
        ProtoOASymbolByIdReq, ProtoOASymbolByIdRes # Still useful for targeted lookups if needed
    )
    from ctrader_open_api.messages.OpenApiModelMessages_pb2 import (
        ProtoOATrader, ProtoOASymbol, ProtoOALightSymbol, ProtoOAArchivedSymbol
    )
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
        self.account_id: Optional[str] = None # Will be string representation of ctidTraderAccountId
        self.balance: Optional[float] = None
        self.equity: Optional[float] = None
        self.currency: Optional[str] = None
        self.used_margin: Optional[float] = None # For margin used

        # For live prices
        self.latest_symbol_prices: Dict[int, Dict[str, float]] = {} # Keyed by symbolId: {'bid': bid, 'ask': ask}
        self.symbol_details_cache: Dict[int, Dict[str, Any]] = {}   # Keyed by symbolId: {'digits': 5, 'name': 'EURUSD', ...}
        self.symbol_name_to_id_map: Dict[str, int] = {}            # Keyed by symbolName (e.g. "EURUSD"): symbolId
        self.price_update_callback: Optional[callable] = None      # Callback for GUI updates on new price

        self._client: Optional[Client] = None
        self._message_id_counter: int = 1
        self._reactor_thread: Optional[threading.Thread] = None
        self._auth_code: Optional[str] = None # To store the auth code from OAuth flow
        self._account_auth_initiated: bool = False # Flag to prevent duplicate account auth attempts

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
        print(f"Sending ProtoOAApplicationAuthReq: {req}")
        d = client.send(req)
        d.addCallbacks(self._handle_app_auth_response, self._handle_send_error)

    def _on_client_disconnected(self, client: Client, reason: Any) -> None:
        print(f"OpenAPI Client Disconnected: {reason}")
        self.is_connected = False
        self._is_client_connected = False
        self._account_auth_initiated = False # Reset flag

    def _on_message_received(self, client: Client, message: Any) -> None:
        print(f"Original message received (type: {type(message)}): {message}")

        # Attempt to extract and deserialize using Protobuf.extract
        try:
            actual_message = Protobuf.extract(message)
            print(f"Message extracted via Protobuf.extract (type: {type(actual_message)}): {actual_message}")
        except Exception as e:
            print(f"Error using Protobuf.extract: {e}. Falling back to manual deserialization if possible.")
            actual_message = message # Fallback to original message for manual processing attempt
            # Log additional details about the original message if it's a ProtoMessage
            if isinstance(message, ProtoMessage):
                 print(f"  Fallback: Original ProtoMessage PayloadType: {message.payloadType}, Payload Bytes: {message.payload[:64]}...") # Log first 64 bytes

        # If Protobuf.extract returned the original ProtoMessage wrapper, it means it couldn't deserialize it.
        # Or if an error occurred and we fell back.
        # We can attempt manual deserialization as before, but it's better if Protobuf.extract handles it.
        # For now, the dispatch logic below will use the result of Protobuf.extract.
        # If actual_message is still ProtoMessage, the specific isinstance checks will fail,
        # which is the correct behavior if it couldn't be properly deserialized.

        # We need to get payload_type for logging in case it's an unhandled ProtoMessage
        payload_type = 0
        if isinstance(actual_message, ProtoMessage): # If still a wrapper after extract attempt
            payload_type = actual_message.payloadType
            print(f"  Protobuf.extract did not fully deserialize. Message is still ProtoMessage wrapper with PayloadType: {payload_type}")
        elif isinstance(message, ProtoMessage) and actual_message is message: # Fallback case where actual_message was reset to original
            payload_type = message.payloadType
        # Ensure payload_type is defined for the final log message if it's an unhandled ProtoMessage
        final_payload_type_for_log = payload_type if isinstance(actual_message, ProtoMessage) else getattr(actual_message, 'payloadType', 'N/A')


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
        elif isinstance(actual_message, ProtoOASymbolByNameRes): # Added for global dispatch if not caught by deferred
            print("  Dispatching to _handle_symbol_by_name_res (globally)")
            # Note: _handle_symbol_by_name_res expects original_symbol_name, success_callback, error_callback
            # If dispatched globally, these won't be available. This handler is best used with direct deferreds.
            # For now, just log it. Proper handling would require matching clientMsgId if used, or a different design.
            print(f"  Received ProtoOASymbolByNameRes globally: {actual_message}")
            # self._handle_symbol_by_name_res(actual_message, "UNKNOWN_ORIGINAL_SYMBOL") # This would be problematic
        elif isinstance(actual_message, ProtoOASymbolsListRes): # Added for global dispatch if not caught by deferred
            print("  Dispatching to _handle_symbols_list_res (globally)")
            # This is a fallback; direct deferred callback in _request_all_symbol_details is preferred.
            self._handle_symbols_list_res(actual_message)
        elif isinstance(actual_message, ProtoOASubscribeSpotsRes): # Added for global dispatch
            print("  Dispatching to _handle_subscribe_spots_res (globally)")
            # Note: _handle_subscribe_spots_res expects symbol_id. Global dispatch lacks this.
            # This is best handled by direct deferreds.
            print(f"  Received ProtoOASubscribeSpotsRes globally: {actual_message}")
            # self._handle_subscribe_spots_res(actual_message, UNKNOWN_SYMBOL_ID) # Problematic
        elif isinstance(actual_message, ProtoOATraderRes):
            print("  Dispatching to _handle_trader_response")
            self._handle_trader_response(actual_message)
        elif isinstance(actual_message, ProtoOATraderUpdatedEvent):
            print("  Dispatching to _handle_trader_updated_event")
            self._handle_trader_updated_event(actual_message)
        elif isinstance(actual_message, ProtoOASpotEvent):
            # Dispatching to _handle_spot_event if it's a spot event
            print("  Dispatching to _handle_spot_event")
            self._handle_spot_event(actual_message)
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
        # Check if it's still the ProtoMessage wrapper (meaning Protobuf.extract didn't deserialize it further)
        elif isinstance(actual_message, ProtoMessage): # Covers actual_message is message (if message was ProtoMessage)
                                                       # and actual_message is the result of extract but still a wrapper.
            print(f"  ProtoMessage with PayloadType {actual_message.payloadType} was not handled by specific type checks.")
        elif actual_message is message and not isinstance(message, ProtoMessage): # Original message was not ProtoMessage and not handled
             print(f"  Unhandled non-ProtoMessage type in _on_message_received: {type(actual_message)}")
        else: # Should ideally not be reached if all cases are handled
            print(f"  Message of type {type(actual_message)} (PayloadType {final_payload_type_for_log}) fell through all handlers.")

    # Handlers
    def _handle_app_auth_response(self, response: ProtoOAApplicationAuthRes) -> None:
        print("ApplicationAuth response received.")

        if self._account_auth_initiated:
            print("Account authentication process already initiated, skipping duplicate _handle_app_auth_response.")
            return

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

        # Proceed to account authentication or discovery, using the OAuth access token (self._access_token)
        if self.ctid_trader_account_id and self._access_token:
            # If a ctidTraderAccountId is known (e.g., from settings) and we have an OAuth access token,
            # proceed directly with ProtoOAAccountAuthReq as per standard Spotware flow.
            print(f"Known ctidTraderAccountId: {self.ctid_trader_account_id}. Attempting ProtoOAAccountAuthReq.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_account_auth_request(self.ctid_trader_account_id)
        elif self._access_token:
            # If ctidTraderAccountId is not known, but we have an access token,
            # first try to get the account list associated with this token.
            # ProtoOAGetAccountListByAccessTokenReq is preferred over ProtoOAGetCtidProfileByTokenReq
            # if the goal is to find trading accounts. Profile is more about user details.
            print("No default ctidTraderAccountId. Attempting to get account list by access token.")
            self._account_auth_initiated = True # Set flag before sending
            self._send_get_account_list_request()
        else:
            # This case should ideally be prevented by earlier checks in the connect() flow.
            self._last_error = "Critical: Cannot proceed with account auth/discovery. Missing ctidTraderAccountId or access token after app auth."
            print(self._last_error)
            if self._client:
                self._client.stopService()


    def _handle_get_ctid_profile_response(self, response: ProtoOAGetCtidProfileByTokenRes) -> None:
        """
        Handles the response from a ProtoOAGetCtidProfileByTokenReq.
        Its primary role is to provide user profile information.
        It might also list associated ctidTraderAccountIds, which can be used if an ID isn't already known.
        This handler does NOT set self.is_connected; connection is confirmed by ProtoOAAccountAuthRes.
        """
        print(f"Received ProtoOAGetCtidProfileByTokenRes. Content: {response}")

        # Example of how you might use profile data if needed:
        # if hasattr(response, 'profile') and response.profile:
        #     print(f"  User Profile Nickname: {response.profile.nickname}")

        # Check if the response contains ctidTraderAccount details.
        # According to some message definitions, ProtoOAGetCtidProfileByTokenRes
        # might not directly list accounts. ProtoOAGetAccountListByAccessTokenRes is for that.
        # However, if it *does* provide an account ID and we don't have one, we could note it.
        # For now, this handler mainly logs. If a ctidTraderAccountId is needed and not present,
        # the flow should have gone through _send_get_account_list_request.

        # If, for some reason, this response is used to discover a ctidTraderAccountId:
        # found_ctid = None
        # if hasattr(response, 'ctidProfile') and hasattr(response.ctidProfile, 'ctidTraderId'): # Speculative
        #     found_ctid = response.ctidProfile.ctidTraderId
        #
        # if found_ctid and not self.ctid_trader_account_id:
        #     print(f"  Discovered ctidTraderAccountId from profile: {found_ctid}. Will attempt account auth.")
        #     self.ctid_trader_account_id = found_ctid
        #     self._send_account_auth_request(self.ctid_trader_account_id)
        # elif not self.ctid_trader_account_id:
        #     self._last_error = "Profile received, but no ctidTraderAccountId found to proceed with account auth."
        #     print(self._last_error)

        # This response does not confirm a live trading session for an account.
        # That's the role of ProtoOAAccountAuthRes.
        pass


    def _handle_account_auth_response(self, response: ProtoOAAccountAuthRes) -> None:
        print(f"Received ProtoOAAccountAuthRes: {response}")
        # The response contains the ctidTraderAccountId that was authenticated.
        # We should verify it matches the one we intended to authenticate.
        if response.ctidTraderAccountId == self.ctid_trader_account_id:
            print(f"Successfully authenticated account {self.ctid_trader_account_id}.")
            self.is_connected = True # Mark as connected for this specific account
            self._last_error = ""     # Clear any previous errors

            # After successful account auth, fetch initial trader details (balance, equity)
            # And then fetch all symbol details
            print("Account authenticated. Requesting trader details...")
            self._send_get_trader_request(self.ctid_trader_account_id) # This will eventually call _update_trader_details

            print("Requesting all symbol details post-authentication...")
            self._request_all_symbol_details() # Fetch all symbols

            # TODO: Subscribe to spots, etc., as needed by the application (e.g., for a default symbol)
            # This might be better handled by the GUI when it becomes active.
        else:
            print(f"AccountAuth failed. Expected ctidTraderAccountId {self.ctid_trader_account_id}, "
                  f"but response was for {response.ctidTraderAccountId if hasattr(response, 'ctidTraderAccountId') else 'unknown'}.")
            self._last_error = "Account authentication failed (ID mismatch or error)."
            self.is_connected = False
            # Consider stopping the client if account auth fails critically
            if self._client:
                self._client.stopService()

    def _handle_get_account_list_response(self, response: ProtoOAGetAccountListByAccessTokenRes) -> None:
        print("Account list response.")
        accounts = getattr(response, 'ctidTraderAccount', [])
        if not accounts:
            print("No accounts available for this access token.")
            self._last_error = "No trading accounts found for this access token."
            # Potentially disconnect or signal error more formally if no accounts mean connection cannot proceed.
            if self._client and self._is_client_connected:
                self._client.stopService() # Or some other error state
            return

        # TODO: If multiple accounts, allow user selection. For now, using the first.
        selected_account = accounts[0] # Assuming ctidTraderAccount is a list of ProtoOACtidTraderAccount
        if not selected_account.ctidTraderAccountId:
            print("Error: Account in list has no ctidTraderAccountId.")
            self._last_error = "Account found but missing ID."
            return

        self.ctid_trader_account_id = selected_account.ctidTraderAccountId
        print(f"Selected ctidTraderAccountId from list: {self.ctid_trader_account_id}")
        # Optionally save to settings if this discovery should update the default
        # self.settings.openapi.default_ctid_trader_account_id = self.ctid_trader_account_id

        # Now that we have a ctidTraderAccountId, authenticate this account
        self._send_account_auth_request(self.ctid_trader_account_id)

    def _handle_trader_response(self, response_wrapper: Any) -> None:
        # If this is called directly by a Deferred, response_wrapper might be ProtoMessage
        # If called after global _on_message_received, it's already extracted.
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_trader_response: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper # Assume it's already the specific message type

        if not isinstance(actual_message, ProtoOATraderRes):
            print(f"_handle_trader_response: Expected ProtoOATraderRes, got {type(actual_message)}. Message: {actual_message}")
            return

        # Now actual_message is definitely ProtoOATraderRes
        trader_object = actual_message.trader # Access the nested ProtoOATrader object

        trader_details_updated = self._update_trader_details(
            "Trader details response.", trader_object
        )

        if trader_details_updated and hasattr(trader_object, 'ctidTraderAccountId'):
            current_ctid = getattr(trader_object, 'ctidTraderAccountId')
            print(f"Value of trader_object.ctidTraderAccountId before assignment: {current_ctid}, type: {type(current_ctid)}")
            self.account_id = str(current_ctid)
            print(f"self.account_id set to: {self.account_id}")
        elif trader_details_updated:
            print(f"Trader details updated, but ctidTraderAccountId missing from trader_object. trader_object: {trader_object}")
        else:
            print("_handle_trader_response: _update_trader_details did not return updated details or trader_object was None.")


    def _handle_trader_updated_event(self, event_wrapper: Any) -> None:
        if isinstance(event_wrapper, ProtoMessage):
            actual_event = Protobuf.extract(event_wrapper)
            print(f"_handle_trader_updated_event: Extracted {type(actual_event)} from ProtoMessage wrapper.")
        else:
            actual_event = event_wrapper

        if not isinstance(actual_event, ProtoOATraderUpdatedEvent):
            print(f"_handle_trader_updated_event: Expected ProtoOATraderUpdatedEvent, got {type(actual_event)}. Message: {actual_event}")
            return

        self._update_trader_details(
            "Trader updated event.", actual_event.trader # Access nested ProtoOATrader
        )
        # Note: TraderUpdatedEvent might not always update self.account_id if it's already set,
        # but it will refresh balance, equity, margin if present in actual_event.trader.

    def _handle_symbol_by_name_res(self, response_wrapper: Any, original_symbol_name: str, success_callback=None, error_callback=None):
        """Handles ProtoOASymbolByNameRes, caches details, and calls callbacks."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbol_by_name_res: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolByNameRes):
            err_msg = f"Expected ProtoOASymbolByNameRes, got {type(actual_message)}. Message: {actual_message}"
            print(f"_handle_symbol_by_name_res: {err_msg}")
            if error_callback:
                error_callback(original_symbol_name, err_msg)
            return

        # ProtoOASymbolByNameRes contains a list of symbols (usually one if found by name)
        if not actual_message.symbol:
            err_msg = f"Symbol '{original_symbol_name}' not found by API."
            print(f"_handle_symbol_by_name_res: {err_msg}")
            if error_callback:
                error_callback(original_symbol_name, err_msg)
            return

        # Assuming the first symbol in the list is the one we want
        symbol_data: ProtoOASymbol = actual_message.symbol[0]
        symbol_id = getattr(symbol_data, 'symbolId', None)
        digits = getattr(symbol_data, 'digits', 5) # Default to 5 if not present
        symbol_name_from_api = getattr(symbol_data, 'symbolName', original_symbol_name) # Use API's version of name if available

        if symbol_id is None:
            err_msg = f"Symbol ID missing in ProtoOASymbolByNameRes for '{original_symbol_name}'. Data: {symbol_data}"
            print(f"_handle_symbol_by_name_res: {err_msg}")
            if error_callback:
                error_callback(original_symbol_name, err_msg)
            return

        # Normalize symbol name for consistent map key (e.g., remove '/')
        normalized_api_symbol_name = symbol_name_from_api.replace('/', '')

        self.symbol_details_cache[symbol_id] = {
            'symbolId': symbol_id,
            'name': normalized_api_symbol_name, # Store the normalized name from API
            'original_requested_name': original_symbol_name, # Could be useful for debugging
            'digits': digits,
            # Potentially cache other useful fields from symbol_data:
            # 'description': getattr(symbol_data, 'description', ''),
            # 'categoryName': getattr(symbol_data, 'categoryName', ''),
            # etc.
        }
        self.symbol_name_to_id_map[normalized_api_symbol_name] = symbol_id
        # Also map the originally requested name if it's different, for easier lookup
        if original_symbol_name.replace('/', '') != normalized_api_symbol_name:
             self.symbol_name_to_id_map[original_symbol_name.replace('/', '')] = symbol_id


        print(f"Cached symbol details for {normalized_api_symbol_name} (ID: {symbol_id}): Digits={digits}")

        if success_callback:
            success_callback(symbol_id) # Pass symbol_id to the next step (e.g., subscription)

    def fetch_symbol_details(self, symbol_name: str, success_callback=None, error_callback=None) -> Optional[int]:
        """
        Retrieves cached details for a symbol by its name (e.g., "EURUSD", "EUR/USD").
        Calls success_callback(symbol_id) if found, or error_callback if not.
        Returns the symbol_id if found, else None.
        This method now relies on the cache populated by _handle_symbols_list_res.
        """
        normalized_name = symbol_name.replace('/', '').upper()

        symbol_id = self.symbol_name_to_id_map.get(normalized_name)

        if symbol_id is not None and symbol_id in self.symbol_details_cache:
            print(f"Symbol details for '{normalized_name}' (ID: {symbol_id}) found in cache.")
            if success_callback:
                success_callback(symbol_id)
            return symbol_id
        else:
            err_msg = f"Symbol '{normalized_name}' not found in cache. Available: {list(self.symbol_name_to_id_map.keys())}"
            print(f"fetch_symbol_details: {err_msg}")
            if error_callback:
                error_callback(symbol_name, err_msg)
            return None

    def _handle_subscribe_spots_res(self, response_wrapper: Any, symbol_id: int):
        """Handles ProtoOASubscribeSpotsRes to confirm subscription."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_subscribe_spots_res: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASubscribeSpotsRes):
            print(f"_handle_subscribe_spots_res: Expected ProtoOASubscribeSpotsRes for symbolId {symbol_id}, got {type(actual_message)}. Message: {actual_message}")
            # Check if it's an error response that needs general handling
            if isinstance(actual_message, ProtoOAErrorRes):
                print(f"  Error during spot subscription for symbolId {symbol_id}: {actual_message.errorCode} - {actual_message.description}")
            elif isinstance(actual_message, ProtoErrorRes):
                 print(f"  Common Error during spot subscription for symbolId {symbol_id}: {actual_message.errorCode} - {actual_message.description}")
            return

        # ProtoOASubscribeSpotsRes doesn't have much other than confirming the subscription context (ctidTraderAccountId).
        # The actual spot events will follow separately.
        print(f"Successfully processed subscription response for symbolId {symbol_id}. Spot events should follow. Response: {actual_message}")
        # We could maintain a set of subscribed symbol IDs if needed: self.subscribed_spot_ids.add(symbol_id)


    def _proceed_to_subscribe_spots(self, symbol_id: int) -> None:
        """Internal method to send spot subscription request once symbol_id is known."""
        if not self._client or not self._is_client_connected or not self.ctid_trader_account_id:
            print(f"Cannot subscribe to spots for symbolId {symbol_id}: Client not connected or ctidTraderAccountId not set.")
            return

        print(f"Proceeding to subscribe to spots for symbolId: {symbol_id}")
        req = ProtoOASubscribeSpotsReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        req.symbolId.append(symbol_id) # symbolId is a repeated field

        print(f"Sending ProtoOASubscribeSpotsReq: {req}")
        try:
            d = self._client.send(req)
            d.addCallbacks(
                lambda response: self._handle_subscribe_spots_res(response, symbol_id),
                errback=lambda failure: (
                    print(f"Deferred error subscribing to spots for symbolId {symbol_id}: {failure.getErrorMessage()}"),
                    self._handle_send_error(failure) # Log full traceback
                )
            )
            print(f"Deferred created for ProtoOASubscribeSpotsReq for symbolId {symbol_id}.")
        except Exception as e:
            print(f"Exception during _proceed_to_subscribe_spots for symbolId {symbol_id}: {e}")

    def subscribe_to_symbol(self, symbol_name: str) -> None:
        """
        Subscribes to live spot price updates for a given symbol name (e.g., "EURUSD", "EUR/USD").
        It will first fetch symbol details if not already cached.
        """
        if not self.is_connected or not self.ctid_trader_account_id:
            print(f"Cannot subscribe to '{symbol_name}': Trader not fully connected or account ID not set.")
            return

        normalized_name = symbol_name.replace('/', '').upper() # Normalize for consistent lookup

        def _on_symbol_details_error(s_name, error_message):
            print(f"Error fetching details for '{s_name}' during subscription attempt: {error_message}")
            # Optionally, notify GUI of subscription failure

        # Check if symbol_id is already known
        # The fetch_symbol_details method now handles cache checking.
        # We always call it; it will use cached data if available or call error_callback if not.
        print(f"Attempting to subscribe to '{symbol_name}' (normalized: {normalized_name}). Checking cache via fetch_symbol_details...")
        self.fetch_symbol_details(
            symbol_name, # Pass original name, fetch_symbol_details will normalize for lookup
            success_callback=self._proceed_to_subscribe_spots, # This is called if found in cache
            error_callback=_on_symbol_details_error # This is called if not found in cache
        )

    # TODO: Implement unsubscribe_from_symbol if needed

    def _request_all_symbol_details(self) -> None:
        """Sends a request to get the list of all available symbols for the account."""
        if not self._client or not self._is_client_connected or not self.ctid_trader_account_id:
            print("Cannot request symbol list: Client not connected or ctidTraderAccountId not set.")
            return

        print("Requesting list of all symbols...")
        req = ProtoOASymbolsListReq()
        req.ctidTraderAccountId = self.ctid_trader_account_id
        # req.includeArchivedSymbols = False # Optional: defaults to false

        print(f"Sending ProtoOASymbolsListReq: {req}")
        try:
            d = self._client.send(req)
            d.addCallbacks(
                self._handle_symbols_list_res,
                errback=lambda failure: (
                    print(f"Deferred error fetching symbol list: {failure.getErrorMessage()}"),
                    self._handle_send_error(failure) # Log full traceback
                )
            )
            print("Deferred created for ProtoOASymbolsListReq.")
        except Exception as e:
            print(f"Exception during _request_all_symbol_details: {e}")

    def _handle_symbols_list_res(self, response_wrapper: Any) -> None:
        """Handles ProtoOASymbolsListRes, populates caches."""
        if isinstance(response_wrapper, ProtoMessage):
            actual_message = Protobuf.extract(response_wrapper)
            print(f"_handle_symbols_list_res: Extracted {type(actual_message)} from ProtoMessage wrapper.")
        else:
            actual_message = response_wrapper

        if not isinstance(actual_message, ProtoOASymbolsListRes):
            print(f"_handle_symbols_list_res: Expected ProtoOASymbolsListRes, got {type(actual_message)}. Message: {actual_message}")
            if isinstance(actual_message, (ProtoOAErrorRes, ProtoErrorRes)):
                 print(f"  Error fetching symbol list: {actual_message.errorCode} - {getattr(actual_message, 'description', '')}")
            return

        print(f"Received ProtoOASymbolsListRes with {len(actual_message.symbol)} light symbols and {len(actual_message.archivedSymbol)} archived symbols.")

        # Process light symbols (typically the active ones we care most about)
        for symbol_data in actual_message.symbol: # These are ProtoOALightSymbol
            symbol_id = getattr(symbol_data, 'symbolId', None)
            name = getattr(symbol_data, 'symbolName', None)
            digits = getattr(symbol_data, 'digits', 5) # ProtoOALightSymbol has digits

            if symbol_id is not None and name is not None:
                normalized_name = name.replace('/', '').upper()
                self.symbol_name_to_id_map[normalized_name] = symbol_id
                self.symbol_details_cache[symbol_id] = {
                    'symbolId': symbol_id,
                    'name': normalized_name, # Store normalized name
                    'originalName': name,   # Store original name from API
                    'digits': digits,
                    'categoryName': getattr(symbol_data, 'symbolCategoryId', None), # LightSymbol has categoryId
                    'assetClass': getattr(symbol_data, 'assetClassId', None) # LightSymbol has assetClassId
                    # Add other fields from ProtoOALightSymbol if needed
                }
                # print(f"  Cached light symbol: ID {symbol_id}, Name: {normalized_name}, Digits: {digits}")
            else:
                print(f"  Skipping light symbol due to missing ID or Name: {symbol_data}")

        # Optionally process archived symbols if needed (ProtoOAArchivedSymbol)
        # for archived_symbol_data in actual_message.archivedSymbol:
        #     symbol_id = getattr(archived_symbol_data, 'symbolId', None)
        #     # ... similar processing ...

        print(f"Symbol caches updated. Total {len(self.symbol_name_to_id_map)} names mapped, {len(self.symbol_details_cache)} details cached.")
        # Optionally, trigger a callback if the UI or other components need to know that symbols are loaded
        # if self.symbols_loaded_callback:
        #     self.symbols_loaded_callback()


    def _update_trader_details(self, log_message: str, trader_proto: ProtoOATrader):
        """Helper to update trader balance and equity from a ProtoOATrader object."""
        print(log_message)
        if trader_proto:
            print(f"Full ProtoOATrader object received in _update_trader_details: {trader_proto}")

            # Safely get ctidTraderAccountId for logging, though it's not set here directly
            logged_ctid = getattr(trader_proto, 'ctidTraderAccountId', 'N/A')

            balance_val = getattr(trader_proto, 'balance', None)
            if balance_val is not None:
                self.balance = balance_val / 100.0
                print(f"  Updated balance for {logged_ctid}: {self.balance}")
            else:
                print(f"  Balance not found in ProtoOATrader for {logged_ctid}")

            equity_val = getattr(trader_proto, 'equity', None)
            if equity_val is not None:
                self.equity = equity_val / 100.0
                print(f"  Updated equity for {logged_ctid}: {self.equity}")
            else:
                # self.equity remains as its previous value (or None if first time)
                print(f"  Equity not found in ProtoOATrader for {logged_ctid}. self.equity remains: {self.equity}")

            currency_val = getattr(trader_proto, 'depositAssetId', None) # depositAssetId is often used for currency ID
            # TODO: Convert depositAssetId to currency string if mapping is available
            # For now, just store the ID if it exists, or keep self.currency as is.
            if currency_val is not None:
                 # self.currency = str(currency_val) # Or map to symbol
                 print(f"  depositAssetId (currency ID) for {logged_ctid}: {currency_val}")


            # Placeholder for margin - we need to see what fields are available from logs
            # Example:
            # used_margin_val = getattr(trader_proto, 'usedMargin', None) # Or 'totalMarginUsed' etc.
            # if used_margin_val is not None:
            #     self.used_margin = used_margin_val / 100.0
            #     print(f"  Updated used_margin for {logged_ctid}: {self.used_margin}")
            # else:
            #     print(f"  Used margin not found in ProtoOATrader for {logged_ctid}. self.used_margin remains: {self.used_margin}")

            return trader_proto
        else:
            print("_update_trader_details received empty trader_proto.")
        return None

    def _handle_spot_event(self, event: ProtoOASpotEvent) -> None:
        """Handles incoming live spot price events."""
        symbol_id = getattr(event, 'symbolId', None)

        # ProtoOASpotEvent may contain actual bid/ask or trendbars.
        # For live ticks, we look for direct bid/ask.
        # Bid and Ask prices are optional in ProtoOASpotEvent definition.
        bid_price_raw = getattr(event, 'bid', None)
        ask_price_raw = getattr(event, 'ask', None)

        if symbol_id is None or bid_price_raw is None: # Ask can sometimes be missing if only bid updates
            # If it's a trendbar, it would be event.trendbar[0].close or similar.
            # For now, focusing on direct bid/ask ticks.
            print(f"_handle_spot_event: Received spot event without symbolId or bid price. Event: {event}")
            return

        details = self.symbol_details_cache.get(symbol_id)
        if not details:
            print(f"_handle_spot_event: WARN - No symbol details (like 'digits') cached for symbolId {symbol_id}. Cannot scale price accurately.")
            # Potentially use a default digits or skip if this happens, though ideally details are pre-cached.
            # For now, we might proceed without scaling if digits are missing, which is incorrect for actual use.
            # Or, decide to not process if details are missing.
            # Let's assume for now we try to proceed but log heavily.
            digits = 5 # Defaulting to 5 as a common case, but this is a GUESS.
        else:
            digits = details.get('digits', 5) # Default to 5 if not in cache for some reason

        # Scale prices
        # The raw prices are typically integers. e.g., 123456 for a 5-digit price of 1.23456
        scaled_bid = bid_price_raw / (10**digits)
        scaled_ask = ask_price_raw / (10**digits) if ask_price_raw is not None else None # Ask might be missing

        self.latest_symbol_prices[symbol_id] = {'bid': scaled_bid, 'ask': scaled_ask}

        # For debugging and to see price flow:
        # print(f"Spot Event: SymbolID: {symbol_id}, Bid: {scaled_bid}, Ask: {scaled_ask}")

        if self.price_update_callback:
            try:
                self.price_update_callback(symbol_id, scaled_bid, scaled_ask)
            except Exception as e:
                print(f"Error calling price_update_callback: {e}")

        # Update self.price_history if it's being used for the current symbol
        # This part needs logic to know which symbol's history to update if price_history is for one symbol.
        # For now, let's assume price_history might be for the 'active' symbol in the GUI.
        # This would require knowing the active symbolId.
        # current_active_symbol_id_in_gui = ... # This needs to be obtained or passed
        # if symbol_id == current_active_symbol_id_in_gui:
        #     if len(self.price_history) >= self.history_size:
        #         self.price_history.pop(0)
        #     mid_price = (scaled_bid + scaled_ask) / 2 if scaled_ask is not None else scaled_bid
        #     self.price_history.append(mid_price)


    def _handle_execution_event(self, event: ProtoOAExecutionEvent) -> None:
        # TODO: handle executions
        pass

    def _handle_send_error(self, failure: Any) -> None:
        print(f"Send error: {failure.getErrorMessage()}")
        if hasattr(failure, 'printTraceback'):
            print("Traceback for send error:")
            failure.printTraceback(file=sys.stderr)
        else:
            print("Failure object does not have printTraceback method. Full failure object:")
            print(failure)
        self._last_error = failure.getErrorMessage()

    # Sending methods
    def _send_account_auth_request(self, ctid: int) -> None:
        if not self._ensure_valid_token():
            return # Token refresh failed or no token, error set by _ensure_valid_token

        print(f"Requesting AccountAuth for {ctid} with token: {self._access_token[:20]}...") # Log token used
        req = ProtoOAAccountAuthReq()
        req.ctidTraderAccountId = ctid
        req.accessToken = self._access_token or "" # Should be valid now

        print(f"Sending ProtoOAAccountAuthReq for ctid {ctid}: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAAccountAuthReq: {d}")

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
                if hasattr(failure_reason, 'printTraceback'):
                    print(f"  Traceback for AccountAuthReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
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
        print(f"Sending ProtoOAGetAccountListByAccessTokenReq: {req}")
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
        print(f"Sending ProtoOATraderReq for ctid {ctid}: {req}")
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

        print(f"Sending ProtoOAGetCtidProfileByTokenReq: {req}")
        try:
            d = self._client.send(req)
            print(f"Deferred created for ProtoOAGetCtidProfileByTokenReq: {d}")

            # Adding specific callbacks for this request to see its fate
            def profile_req_success_callback(response_msg):
                print(f"GetCtidProfileByTokenReq success_callback triggered. Response type: {type(response_msg)}. Will be handled by _on_message_received.")

            def profile_req_error_callback(failure_reason):
                print(f"GetCtidProfileByTokenReq error_callback triggered. Failure:")
                if hasattr(failure_reason, 'getErrorMessage'):
                    print(f"  Error Message: {failure_reason.getErrorMessage()}")
                if hasattr(failure_reason, 'printTraceback'): # May be verbose
                    print(f"  Traceback for GetCtidProfileByTokenReq error:")
                    failure_reason.printTraceback(file=sys.stderr)
                else:
                    print(f"  Failure object (no printTraceback): {failure_reason}")
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
            return {"account_id": "MOCK", "balance": 0.0, "equity": 0.0, "margin": 0.0}
        if not self.is_connected:
            # Return current values even if not fully "connected" but some data might be partially loaded
            return {
                "account_id": self.account_id if self.account_id else "connecting...",
                "balance": self.balance,
                "equity": self.equity,
                "margin": self.used_margin
            }
        return {
            "account_id": self.account_id,
            "balance": self.balance,
            "equity": self.equity,
            "margin": self.used_margin # This will be None initially, or updated from ProtoOATrader
        }

    def get_market_price(self, symbol_name: str) -> Optional[float]:
        """
        Retrieves the latest cached market price (mid-price) for a given symbol name.
        Returns None if the symbol is not found or no price data is available.
        """
        if not USE_OPENAPI_LIB: # Mock mode
            return round(random.uniform(1.10, 1.20), 5)

        normalized_name = symbol_name.replace('/', '').upper()
        symbol_id = self.symbol_name_to_id_map.get(normalized_name)

        if symbol_id is None:
            print(f"get_market_price: Symbol name '{symbol_name}' (normalized to {normalized_name}) not found in symbol_name_to_id_map.")
            return None

        price_data = self.latest_symbol_prices.get(symbol_id)
        if not price_data or 'bid' not in price_data or 'ask' not in price_data:
            print(f"get_market_price: No bid/ask price data available for symbolId {symbol_id} ({normalized_name}).")
            return None

        bid = price_data['bid']
        ask = price_data['ask']

        if bid is None or ask is None:
            print(f"get_market_price: Bid or Ask is None for symbolId {symbol_id}. Bid: {bid}, Ask: {ask}")
            return None

        mid_price = (bid + ask) / 2.0

        symbol_details = self.symbol_details_cache.get(symbol_id)
        digits = 5 # Default digits
        if symbol_details and 'digits' in symbol_details:
            digits = symbol_details['digits']
        else:
            print(f"get_market_price: WARN - 'digits' not found in symbol_details_cache for symbolId {symbol_id}. Defaulting to 5.")

        return round(mid_price, digits)

    def get_price_history(self) -> List[float]:
        # This method needs to be adapted if self.price_history is to be used
        # with multiple symbols or if it's populated differently now.
        # For now, it returns the existing self.price_history.
        # Consider deprecating or making it symbol-specific if strategies need it.
        print("WARN: get_price_history currently returns a generic history, not tied to a specific symbol's live updates yet.")
        return list(self.price_history)
