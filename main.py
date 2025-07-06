#!/usr/bin/env python3
# main.py: Entry point for Forex Scalper GUI

from __future__ import annotations
import traceback

import settings
from gui import MainApplication

if __name__ == "__main__":
    print("🟢 Starting Forex Scalper…")
    try:
        cfg = settings.Settings.load()
        print(f"🟢 Settings loaded: OpenAPI host_type={cfg.openapi.host_type}")
        app = MainApplication(cfg)
        print("🟢 MainApplication initialized, entering mainloop")
        app.mainloop()
        print("🔴 mainloop exited")
    except Exception:
        print("❌ Unhandled exception during startup:")
        traceback.print_exc()
        input("Press Enter to close…")
