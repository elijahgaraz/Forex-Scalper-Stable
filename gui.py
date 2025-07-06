import time
import threading
import tkinter as tk
import queue
from tkinter import ttk, messagebox, simpledialog
from trading import Trader  # adjust import path if needed
from strategies import (
    SafeStrategy, ModerateStrategy, AggressiveStrategy,
    MomentumStrategy, MeanReversionStrategy
)


class MainApplication(tk.Tk):
    def __init__(self, settings):
        super().__init__()
        self.title("Forex Scalper")

        # make window resizable
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        self.settings = settings
        self.trader = Trader(self.settings)

        container = ttk.Frame(self)
        container.grid(row=0, column=0, sticky="nsew")
        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        self.pages = {}
        for Page in (SettingsPage, TradingPage):
            page = Page(container, self)
            page.grid(row=0, column=0, sticky="nsew")
            self.pages[Page] = page

        self.show_page(SettingsPage)

    def show_page(self, page_cls):
        self.pages[page_cls].tkraise()


class SettingsPage(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent, padding=10)
        self.controller = controller
        self.columnconfigure(0, weight=1)

        # --- Login Settings ---
        # --- Account Summary ---
        # Login settings are now primarily handled by config.json and environment variables
        # for the cTrader OpenAPI connection.
        acct = ttk.Labelframe(self, text="Account Summary", padding=10)
        acct.grid(row=0, column=0, sticky="ew", pady=(0,10)) # Changed row from 1 to 0
        acct.columnconfigure(1, weight=1)

        self.account_id_var = tk.StringVar(value="–")
        ttk.Label(acct, text="Account ID:").grid(row=0, column=0, sticky="w", padx=(0,5))
        ttk.Label(acct, textvariable=self.account_id_var).grid(row=0, column=1, sticky="w")

        self.balance_var = tk.StringVar(value="–")
        ttk.Label(acct, text="Balance:").grid(row=1, column=0, sticky="w", padx=(0,5))
        ttk.Label(acct, textvariable=self.balance_var).grid(row=1, column=1, sticky="w")

        self.equity_var = tk.StringVar(value="–")
        ttk.Label(acct, text="Equity:").grid(row=2, column=0, sticky="w", padx=(0,5))
        ttk.Label(acct, textvariable=self.equity_var).grid(row=2, column=1, sticky="w")

        self.margin_var = tk.StringVar(value="–")
        ttk.Label(acct, text="Margin:").grid(row=3, column=0, sticky="w", padx=(0,5))
        ttk.Label(acct, textvariable=self.margin_var).grid(row=3, column=1, sticky="w")

        # --- Actions & Status ---
        actions = ttk.Frame(self)
        actions.grid(row=1, column=0, sticky="ew", pady=(10,0)) # Changed row from 2 to 1
        # Removed "Save Settings" button as settings are primarily config file / env var based for OpenAPI
        ttk.Button(actions, text="Connect", command=self.attempt_connection).pack(side="left", padx=5)

        self.status = ttk.Label(self, text="Disconnected", anchor="center")
        self.status.grid(row=2, column=0, sticky="ew", pady=(5,0)) # Changed row from 3 to 2

    def save_settings(self):
        # This method is now largely obsolete as OpenAPI settings are loaded from config/env
        # and there are no specific FIX settings to save from this UI page.
        # We could potentially save the `default_ctid_trader_account_id` if it's fetched and confirmed,
        # but the current flow loads it from config.json.
        # For now, we can make this a no-op or remove it if no settings are managed here.
        pass

    def attempt_connection(self):
        # self.save_settings() # No longer needed as FIX settings are removed
        t = self.controller.trader

        # Ensure trader uses the latest settings (though they are loaded at init)
        # No specific FIX params to re-init on the Trader for OpenAPI
        t.settings = self.controller.settings

        # Trader.connect() now handles the entire OAuth flow internally (blocking)
        # and then starts the client service if successful.
        # It returns True if token exchange and client service start were successful, False otherwise.

        # To prevent GUI freeze during the blocking connect() call (which includes
        # waiting for browser auth and http server), run it in a thread.

        # Initial status update
        self.status.config(text="Processing connection...", foreground="orange")

        def _connect_thread_target():
            # This runs in the worker thread
            # t.connect() is blocking and will try various connection stages.
            # It internally updates t._last_error which can be fetched if it returns False.

            # For more granular updates *during* t.connect(), t.connect() would need
            # to accept a callback, or the GUI would need to poll t.get_connection_status()
            # if t.connect() was made non-blocking and stateful.
            # Given current structure, we update before and after the blocking call.

            if t.connect(): # This blocks, then attempts to start client service
                # If connect() returns True, it means token was obtained/refreshed/validated
                # and the client service has started.
                # Now we can start polling for App/Account Auth completion from Spotware.
                self.after(0, lambda: self.status.config(text="Connection successful. Authenticating account...", foreground="orange"))
                self.after(100, self._check_connection) # Start polling for actual connection status
            else:
                # connect() returned False. An error occurred.
                # trader._last_error should have the details of what failed.
                _, msg = t.get_connection_status()
                final_msg = f"Failed: {msg}" if msg else "Connection failed."

                self.after(0, lambda: messagebox.showerror("Connection Failed", final_msg))
                self.after(0, lambda: self.status.config(text=final_msg, foreground="red"))

        connect_thread = threading.Thread(target=_connect_thread_target, daemon=True)
        connect_thread.start()


    # Poll connection status until connected or error
    def _check_connection(self):
        t = self.controller.trader
        connected, msg = t.get_connection_status()
        if connected:
            # proceed to post-connection
            self._on_successful_connection(t) # Renamed
        else:
            if msg: # If there's an error message, connection attempt failed
                messagebox.showerror("Connection Failed", msg)
                self.status.config(text=f"Failed: {msg}", foreground="red")
            else: # No error message yet, still trying
                self.after(200, self._check_connection)

    # Note: The duplicated block after the TODO was problematic.
    # The logic should be: if connected, call success. If not connected and msg exists, show error.
    # If not connected and no msg, keep polling. This is now reflected above.

    def _on_successful_connection(self, t): # Renamed from _extracted_from_attempt_connection_14
        # t.start_heartbeat() # Heartbeat is typically managed by the Trader/API library after connection
        summary = t.get_account_summary()

        # Check if essential details are populated.
        # Trader.is_connected should be True at this point (checked by _check_connection).
        # We also need at least account_id and balance to consider it fully loaded for the UI.
        account_id_from_summary = summary.get("account_id")
        balance_from_summary = summary.get("balance")

        if account_id_from_summary == "connecting..." or \
           account_id_from_summary == "–" or \
           account_id_from_summary is None or \
           balance_from_summary is None:
            # This can happen if get_account_summary is called before trader details (like account_id)
            # are fully populated after connection and ProtoOATraderRes.
            self.status.config(text="Fetching account details...", foreground="orange") # More informative status
            self.after(300, lambda: self._on_successful_connection(t)) # Retry shortly
            return

        # Account ID
        account_id_val = summary.get("account_id", "–")
        self.account_id_var.set(str(account_id_val) if account_id_val is not None else "–")

        # Balance
        balance_val = summary.get("balance")
        self.balance_var.set(f"{balance_val:.2f}" if balance_val is not None else "–")

        # Equity
        equity_val = summary.get("equity")
        self.equity_var.set(f"{equity_val:.2f}" if equity_val is not None else "–")

        # Margin
        margin_val = summary.get("margin")
        self.margin_var.set(f"{margin_val:.2f}" if margin_val is not None else "–")

        # Prepare display strings for messagebox, handling None gracefully
        display_account_id = str(account_id_val) if account_id_val is not None else "N/A"
        display_balance = f"{balance_val:.2f}" if balance_val is not None else "N/A"
        display_equity = f"{equity_val:.2f}" if equity_val is not None else "N/A"
        display_margin = f"{margin_val:.2f}" if margin_val is not None else "N/A"

        messagebox.showinfo(
            "Connected",
            f"Successfully connected!\n\n"
            f"Account ID: {display_account_id}\n"
            f"Balance: {display_balance}\n"
            f"Equity: {display_equity}\n"
            f"Margin: {display_margin}"
        )
        self.status.config(text="Connected ✅", foreground="green")

        # Update TradingPage with account info
        trading_page = self.controller.pages[TradingPage]
        trading_page.update_account_info(
            account_id=summary.get("account_id", "–"),
            balance=summary.get("balance"),
            equity=summary.get("equity")
        )
        self.controller.show_page(TradingPage)


class TradingPage(ttk.Frame):
    COMMON_PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY", "USD/CHF", "AUD/USD", "NZD/USD"]

    def __init__(self, parent, controller):
        super().__init__(parent, padding=10)
        self.controller = controller
        self.trader = controller.trader
        self.trader.price_update_callback = self.queue_ui_price_update # Set the callback

        # Thread-safe event queue for UI updates
        self._ui_queue = queue.Queue()
        self.after(100, self._process_ui_queue)

        self.is_scalping = False
        self.scalping_thread = None

        # Account Info StringVars
        self.account_id_var_tp = tk.StringVar(value="–")
        self.balance_var_tp = tk.StringVar(value="–")
        self.equity_var_tp = tk.StringVar(value="–")

        # configure grid
        # Adjusted row count for new account info section
        for r in range(12): # Increased range for new row
            self.rowconfigure(r, weight=0)
        self.rowconfigure(12, weight=1) # Adjusted log row index
        self.columnconfigure(1, weight=1)


        # ← Settings button
        ttk.Button(self, text="← Settings", command=lambda: controller.show_page(SettingsPage)).grid(
            row=0, column=0, columnspan=2, pady=(0,10), sticky="w" # columnspan to align with other full-width elements
        )

        # Account Info Display
        acc_info_frame = ttk.Labelframe(self, text="Account Information", padding=5)
        acc_info_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0,10))
        acc_info_frame.columnconfigure(1, weight=1)

        ttk.Label(acc_info_frame, text="Account ID:").grid(row=0, column=0, sticky="w", padx=(0,5))
        ttk.Label(acc_info_frame, textvariable=self.account_id_var_tp).grid(row=0, column=1, sticky="w")

        ttk.Label(acc_info_frame, text="Balance:").grid(row=1, column=0, sticky="w", padx=(0,5))
        ttk.Label(acc_info_frame, textvariable=self.balance_var_tp).grid(row=1, column=1, sticky="w")

        ttk.Label(acc_info_frame, text="Equity:").grid(row=2, column=0, sticky="w", padx=(0,5))
        ttk.Label(acc_info_frame, textvariable=self.equity_var_tp).grid(row=2, column=1, sticky="w")


        # Symbol dropdown
        # Row indices are +1 from original due to Account Info section added at row=1
        ttk.Label(self, text="Symbol:").grid(row=2, column=0, sticky="w", padx=(0,5))
        self.symbol_var = tk.StringVar(value=self.COMMON_PAIRS[0])
        cb_symbol = ttk.Combobox(self, textvariable=self.symbol_var,
                                 values=self.COMMON_PAIRS, state="readonly")
        cb_symbol.grid(row=2, column=1, sticky="ew") # Corrected from row=1
        cb_symbol.bind("<<ComboboxSelected>>", lambda e: self.refresh_price())

        # Price display + refresh
        ttk.Label(self, text="Price:").grid(row=3, column=0, sticky="w", padx=(0,5)) # Was row=2
        self.price_var = tk.StringVar(value="–")
        pf = ttk.Frame(self)
        pf.grid(row=3, column=1, sticky="ew") # Was row=2
        ttk.Label(pf, textvariable=self.price_var,
                  font=("TkDefaultFont", 12, "bold")).pack(side="left")
        ttk.Button(pf, text="↻", width=2, command=self.refresh_price).pack(side="right")

        # Profit target
        ttk.Label(self, text="Profit Target (pips):").grid(row=4, column=0, sticky="w", padx=(0,5)) # Was row=3
        self.tp_var = tk.DoubleVar(value=10.0)
        ttk.Entry(self, textvariable=self.tp_var).grid(row=4, column=1, sticky="ew") # Was row=3

        # Order size
        ttk.Label(self, text="Order Size (lots):").grid(row=5, column=0, sticky="w", padx=(0,5)) # Was row=4
        self.size_var = tk.DoubleVar(value=1.0)
        ttk.Entry(self, textvariable=self.size_var).grid(row=5, column=1, sticky="ew") # Was row=4

        # Stop-loss
        ttk.Label(self, text="Stop Loss (pips):").grid(row=6, column=0, sticky="w", padx=(0,5)) # Was row=5
        self.sl_var = tk.DoubleVar(value=5.0)
        ttk.Entry(self, textvariable=self.sl_var).grid(row=6, column=1, sticky="ew") # Was row=5

        # Strategy selector
        ttk.Label(self, text="Strategy:").grid(row=7, column=0, sticky="w", padx=(0,5)) # Was row=6
        self.strategy_var = tk.StringVar(value="Safe")
        strategy_names = ["Safe", "Moderate", "Aggressive", "Momentum", "Mean Reversion"]
        cb_strat = ttk.Combobox(self, textvariable=self.strategy_var, values=strategy_names, state="readonly")
        cb_strat.grid(row=7, column=1, sticky="ew") # Was row=6

        # Start/Stop Scalping buttons
        self.start_button = ttk.Button(self, text="Begin Scalping", command=self.start_scalping)
        self.start_button.grid(row=8, column=0, columnspan=2, pady=(10,0)) # Was row=7
        self.stop_button  = ttk.Button(self, text="Stop Scalping", command=self.stop_scalping, state="disabled")
        self.stop_button.grid(row=9, column=0, columnspan=2, pady=(5,0)) # Was row=8

        # Session Stats frame
        stats = ttk.Labelframe(self, text="Session Stats", padding=10)
        stats.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(10,0)) # Was row=9
        stats.columnconfigure(1, weight=1)

        self.pnl_var       = tk.StringVar(value="0.00")
        self.trades_var    = tk.StringVar(value="0")
        self.win_rate_var = tk.StringVar(value="0%")

        ttk.Label(stats, text="P&L:").grid(row=0, column=0, sticky="w", padx=(0,5))
        ttk.Label(stats, textvariable=self.pnl_var).grid(row=0, column=1, sticky="w")
        ttk.Label(stats, text="# Trades:").grid(row=1, column=0, sticky="w", padx=(0,5))
        ttk.Label(stats, textvariable=self.trades_var).grid(row=1, column=1, sticky="w")
        ttk.Label(stats, text="Win Rate:").grid(row=2, column=0, sticky="w", padx=(0,5))
        ttk.Label(stats, textvariable=self.win_rate_var).grid(row=2, column=1, sticky="w")

        # Output log
        self.output = tk.Text(self, height=8, wrap="word", state="disabled")
        self.output.grid(row=12, column=0, columnspan=2, sticky="nsew", pady=(10,0)) # Was row=11
        sb = ttk.Scrollbar(self, command=self.output.yview)
        sb.grid(row=12, column=2, sticky="ns") # Was row=11
        self.output.config(yscrollcommand=sb.set)

        # Internal counters
        self.total_pnl    = 0.0
        self.total_trades = 0
        self.wins         = 0

        self.refresh_price()

    def update_account_info(self, account_id: str, balance: float | None, equity: float | None):
        """Public method to update account info StringVars from outside (e.g., SettingsPage)."""
        self.account_id_var_tp.set(str(account_id) if account_id is not None else "–")

        if balance is not None:
            self.balance_var_tp.set(f"{balance:.2f}")
        else:
            self.balance_var_tp.set("–")

        if equity is not None:
            self.equity_var_tp.set(f"{equity:.2f}")
        else:
            self.equity_var_tp.set("–")

        # Note: TradingPage does not currently display margin, so no update for it here.

    def queue_ui_price_update(self, symbol_id: int, bid: float, ask: float):
        """
        Safely queues a UI update for the price display from the Trader's thread.
        This method is set as the callback in the Trader instance.
        """
        self._ui_queue.put((self._update_price_display_from_event, (symbol_id, bid, ask)))

    def _update_price_display_from_event(self, symbol_id: int, bid: float, ask: float):
        """
        Updates the price display if the incoming price event matches the selected symbol.
        This method is called by _process_ui_queue on the main Tkinter thread.
        """
        selected_symbol_name = self.symbol_var.get() # E.g., "EUR/USD"
        normalized_selected_name = selected_symbol_name.replace('/', '').upper()

        # Get the symbolId for the currently selected symbol name in the GUI
        selected_symbol_id_from_map = self.trader.symbol_name_to_id_map.get(normalized_selected_name)

        if selected_symbol_id_from_map is not None and symbol_id == selected_symbol_id_from_map:
            mid_price = (bid + ask) / 2.0

            # Get digits for formatting (optional, could default to 5)
            details = self.trader.symbol_details_cache.get(symbol_id)
            digits = 5
            if details and 'digits' in details:
                digits = details['digits']

            self.price_var.set(f"{mid_price:.{digits}f}")
            # self._log(f"Real-time price update for {selected_symbol_name}: {mid_price:.{digits}f}") # Optional: for verbose logging
        # else:
            # Log if price update is for a non-selected symbol (can be noisy)
            # print(f"Received price update for symbolId {symbol_id}, but current selection is {selected_symbol_name} (ID: {selected_symbol_id_from_map})")


    def _process_ui_queue(self):
        """Called on the mainloop to drain the UI event queue."""
        try:
            while True:
                func, args = self._ui_queue.get_nowait()
                func(*args)
        except queue.Empty:
            pass
        finally:
            self.after(100, self._process_ui_queue)

    def refresh_price(self):
        """Handles manual price refresh and subscribes to symbol if not already."""
        selected_symbol_name = self.symbol_var.get() # This is "EUR/USD" style
        if not selected_symbol_name:
            self._log("No symbol selected to refresh price for.")
            return

        # Subscribe to the selected symbol if not already (or to confirm/re-subscribe)
        # The trader.subscribe_to_symbol handles caching and prevents redundant API calls for details
        if self.trader.is_connected: # Only attempt subscription if connected
            self._log(f"Requesting subscription for {selected_symbol_name}...")
            self.trader.subscribe_to_symbol(selected_symbol_name)
            # Note: subscribe_to_symbol is async; price updates will come via callback.
            # We still try to get a price immediately via get_market_price for manual refresh feel.
        else:
            self._log("Trader not connected. Cannot subscribe or fetch live price.")
            self.price_var.set("N/C") # Not Connected
            return

        # Attempt to get the current price immediately
        # The symbol name passed to get_market_price should match what it expects (normalized or not)
        # Trader.get_market_price normalizes the name internally.
        try:
            price = self.trader.get_market_price(selected_symbol_name)
            if price is not None:
                # Formatting with dynamic digits if possible, else default
                normalized_name = selected_symbol_name.replace('/', '').upper()
                symbol_id = self.trader.symbol_name_to_id_map.get(normalized_name)
                digits = 5
                if symbol_id and symbol_id in self.trader.symbol_details_cache:
                    digits = self.trader.symbol_details_cache[symbol_id].get('digits', 5)
                self.price_var.set(f"{price:.{digits}f}")
                self._log(f"Refreshed price for {selected_symbol_name}: {price:.{digits}f}")
            else:
                self.price_var.set("–") # No price available yet / symbol not found by trader
                self._log(f"No price available yet for {selected_symbol_name} on manual refresh.")
        except Exception as e:
            self.price_var.set("ERR")
            self._log(f"Error fetching price for {selected_symbol_name}: {e}")

    def start_scalping(self):
        if self.is_scalping:
            return

        # instantiate strategy
        sel = self.strategy_var.get()
        if sel == "Safe":
            strategy = SafeStrategy()
        elif sel == "Moderate":
            strategy = ModerateStrategy()
        elif sel == "Aggressive":
            strategy = AggressiveStrategy()
        elif sel == "Mean Reversion":
            strategy = MeanReversionStrategy()
        else:
            strategy = MomentumStrategy()

        # Snapshot GUI inputs on main thread
        symbol = self.symbol_var.get().replace("/", "")
        tp     = self.tp_var.get()
        sl     = self.sl_var.get()
        size   = self.size_var.get()

        self._toggle_scalping_ui(True)
        self.scalping_thread = threading.Thread(
            target=self._scalp_loop,
            args=(symbol, tp, sl, size, strategy),
            daemon=True
        )
        self.scalping_thread.start()

    def stop_scalping(self):
        if self.is_scalping:
            self._toggle_scalping_ui(False)

    def _toggle_scalping_ui(self, on: bool):
        self.is_scalping = on
        state_start = "disabled" if on else "normal"
        state_stop  = "normal"   if on else "disabled"
        self.start_button.config(state=state_start)
        self.stop_button.config(state=state_stop)

    def _scalp_loop(self, symbol: str, tp: float, sl: float, size: float, strategy):
        """Background thread: pure logic, enqueues UI updates."""
        while self.is_scalping:
            price   = self.trader.get_market_price(symbol)
            history = self.trader.price_history
            action  = strategy.decide({"prices": history})

            if action in ("buy", "sell"):
                # enqueue an _execute_trade call
                self._ui_queue.put((
                    self._execute_trade,
                    (action, symbol, price, size, tp, sl)
                ))
            else:
                self._ui_queue.put((self._log, ("HOLD signal; skipping trade.",)))

            time.sleep(1)

    def _execute_trade(self,
                       side: str,
                       symbol: str,
                       price: float,
                       size: float,
                       tp: float,
                       sl: float):
        """Runs on the Tk mainloop—safe to update UI."""
        self._log(f"{side.upper()} scalp: {symbol} at {price:.5f} | "
                  f"size={size} lots | SL={sl} pips | TP={tp} pips")
        # Here you’d call self.trader.place_market_order(...) if real.
        import random
        result = round(random.uniform(-tp/2, tp), 2)

        # update session stats
        self.total_pnl    += result
        self.total_trades += 1
        if result > 0:
            self.wins += 1

        # update UI vars
        self.pnl_var.set(f"{self.total_pnl:.2f}")
        self.trades_var.set(str(self.total_trades))
        win_rate = (int(self.wins / self.total_trades * 100)
                    if self.total_trades else 0)
        self.win_rate_var.set(f"{win_rate}%")

        self._log(f"Result: {result:+.2f} pips | Total P&L: {self.total_pnl:+.2f}")

    def _log(self, msg: str):
        ts = time.strftime("%H:%M:%S")
        self.output.configure(state="normal")
        self.output.insert("end", f"[{ts}] {msg}\n")
        self.output.see("end")
        self.output.configure(state="disabled")


if __name__ == "__main__":
    import settings
    app = MainApplication(settings.Settings.load())
    app.mainloop()
