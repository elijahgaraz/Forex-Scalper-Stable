import json
import os
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class OpenAPISettings:
    # Credentials - preferentially loaded from environment variables
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    # Connection type: "demo" or "live". This will be used with OpenApiPy's EndPoints.
    host_type: str = "demo"

    # Optional: cTrader Account ID (long integer, but often represented as string in configs)
    # This is the ID of the trading account you want to authorize for trading operations.
    # The library will likely require this for calls like GetTrader, SubscribeSpots etc.
    # This is NOT the same as client_id (which is for the application).
    default_ctid_trader_account_id: Optional[int] = None # Store as int if it's numeric

    # OAuth2 specific URLs
    spotware_auth_url: str = "https://connect.spotware.com/oauth/v2/auth" # Standard URL
    spotware_token_url: str = "https://connect.spotware.com/oauth/v2/token" # Standard URL
    redirect_uri: str = "http://localhost:5000/callback" # As specified


@dataclass
class GeneralSettings:
    default_symbol: str = "EUR/USD"
    chart_update_interval_ms: int = 500
    # Add other general app settings here if any

@dataclass
class Settings:
    openapi: OpenAPISettings
    general: GeneralSettings

    @staticmethod
    def load(path: str = "config.json") -> "Settings":
        # Load secrets from environment variables first
        env_client_id = os.environ.get("CTRADER_CLIENT_ID")
        env_client_secret = os.environ.get("CTRADER_CLIENT_SECRET")

        try:
            with open(path, 'r') as f:
                cfg_data = json.load(f)
        except FileNotFoundError:
            print(f"Warning: Settings file '{path}' not found. Using default values and environment variables.")
            cfg_data = {}
        except json.JSONDecodeError:
            print(f"Warning: Error decoding JSON from '{path}'. Using default values and environment variables.")
            cfg_data = {}

        openapi_cfg = cfg_data.get("openapi", {})
        general_cfg = cfg_data.get("general", {})

        # Prioritize env vars for secrets, then config file, then None
        client_id = env_client_id if env_client_id else openapi_cfg.get("client_id")
        client_secret = env_client_secret if env_client_secret else openapi_cfg.get("client_secret")

        if not client_id:
            print("Warning: cTrader Client ID not found in environment variables (CTRADER_CLIENT_ID) or config.json.")
        if not client_secret:
            print("Warning: cTrader Client Secret not found in environment variables (CTRADER_CLIENT_SECRET) or config.json.")

        openapi_settings = OpenAPISettings(
            client_id=client_id,
            client_secret=client_secret,
            host_type=openapi_cfg.get("host_type", "demo").lower(), # Ensure lowercase "demo" or "live"
            default_ctid_trader_account_id=openapi_cfg.get("default_ctid_trader_account_id"),
            spotware_auth_url=openapi_cfg.get("spotware_auth_url", "https://connect.spotware.com/oauth/v2/auth"), # Standard default
            spotware_token_url=openapi_cfg.get("spotware_token_url", "https://connect.spotware.com/oauth/v2/token"),
            redirect_uri=openapi_cfg.get("redirect_uri", "http://localhost:5000/callback") # Should generally not be overridden from config
        )

        general_settings = GeneralSettings(
            default_symbol=general_cfg.get("default_symbol", "EUR/USD"),
            chart_update_interval_ms=general_cfg.get("chart_update_interval_ms", 500)
        )

        return Settings(openapi=openapi_settings, general=general_settings)

    def save(self, path: str = "config.json") -> None:
        # Create a representation of settings that is safe to save (e.g., without tokens)
        # Only save configurable parts, not runtime state like access tokens.
        openapi_data_to_save = {
            "client_id": self.openapi.client_id if not os.environ.get("CTRADER_CLIENT_ID") else None,
            "client_secret": self.openapi.client_secret if not os.environ.get("CTRADER_CLIENT_SECRET") else None,
            "host_type": self.openapi.host_type,
            "default_ctid_trader_account_id": self.openapi.default_ctid_trader_account_id,
            "spotware_auth_url": self.openapi.spotware_auth_url,
            "spotware_token_url": self.openapi.spotware_token_url,
            "redirect_uri": self.openapi.redirect_uri # Typically not changed by user, but saved for completeness
        }
        # Filter out None values to keep config clean, especially for secrets from env
        # For the new URLs, they have defaults, so they won't be None unless explicitly set to None (which is unlikely)
        openapi_data_to_save = {k: v for k, v in openapi_data_to_save.items() if v is not None}

        if openapi_data_to_save.get("client_id") or openapi_data_to_save.get("client_secret"):
            print(f"Warning: Saving Client ID or Client Secret to '{path}'. "
                  "It's generally recommended to use environment variables for these secrets.")

        data_to_save = {
            "openapi": openapi_data_to_save,
            "general": {
                "default_symbol": self.general.default_symbol,
                "chart_update_interval_ms": self.general.chart_update_interval_ms,
            }
        }
        with open(path, 'w') as f:
            json.dump(data_to_save, f, indent=4)
