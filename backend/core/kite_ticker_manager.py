# backend/core/kite_ticker_manager.py

import asyncio
from kiteconnect import KiteTicker
from core import kite as kite_api 
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.strategy import Strategy

class KiteTickerManager:
    def __init__(self, strategy_instance: "Strategy", main_loop):
        print(">>> KITE TICKER MANAGER: New instance created.")
        self.kws = KiteTicker(kite_api.API_KEY, kite_api.access_token)
        
        self.strategy = strategy_instance
        self.main_loop = main_loop 
        self.is_connected = False
        
        # --- ADDED: Events to signal connection status ---
        self.connected_event = asyncio.Event()
        self.disconnected_event = asyncio.Event()

        self.kws.on_ticks = self.on_ticks
        self.kws.on_connect = self.on_connect
        self.kws.on_close = self.on_close
        self.kws.on_error = self.on_error

    def on_ticks(self, ws, ticks):
        if self.strategy:
            asyncio.run_coroutine_threadsafe(self.strategy.handle_ticks_async(ticks), self.main_loop)
    def subscribe(self, tokens):
        """
        Subscribes to an additional list of instrument tokens without
        unsubscribing from the existing ones.
        """
        if self.is_connected and self.kws:
            print(f"Subscribing to {len(tokens)} additional tokens.")
            self.kws.subscribe(tokens)
            self.kws.set_mode(self.kws.MODE_LTP, tokens)

    def on_connect(self, ws, response):
        print(">>> KITE TICKER MANAGER: 'on_connect' callback triggered.")
        self.is_connected = True
        self.disconnected_event.clear()
        
        # --- ADDED: Signal that the connection is successful ---
        self.main_loop.call_soon_threadsafe(self.connected_event.set)
        
        print("Kite Ticker connected.")
        if self.strategy:
             asyncio.run_coroutine_threadsafe(self.strategy.on_ticker_connect(), self.main_loop)

    def on_close(self, ws, code, reason):
        print(f">>> KITE TICKER MANAGER: 'on_close' callback triggered.")
        self.is_connected = False
        self.connected_event.clear()
        
        # --- UPDATED: Signal that the disconnection is complete ---
        self.main_loop.call_soon_threadsafe(self.disconnected_event.set)
        
        if self.strategy:
             asyncio.run_coroutine_threadsafe(self.strategy.on_ticker_disconnect(), self.main_loop)

    def on_error(self, ws, code, reason):
        print(f">>> KITE TICKER MANAGER: 'on_error' callback triggered.")
        # --- ADDED: Signal events on error to unblock waiting tasks ---
        self.main_loop.call_soon_threadsafe(self.connected_event.set)
        self.main_loop.call_soon_threadsafe(self.disconnected_event.set)

    def start(self):
        """
        Initiates the connection in a background thread. This method is non-blocking.
        """
        print(">>> KITE TICKER MANAGER: 'start' method called.")
        if not self.is_connected and kite_api.access_token:
            # Clear the event before attempting to connect
            self.connected_event.clear()
            self.kws.connect(threaded=True)

    async def stop(self):
        """
        Stops the WebSocket connection and waits for confirmation of disconnection.
        """
        print(">>> KITE TICKER MANAGER: 'stop' method called.")
        if self.is_connected and self.kws:
            self.disconnected_event.clear()
            self.kws.close()
            try:
                print(">>> KITE TICKER MANAGER: Waiting for disconnection confirmation...")
                await asyncio.wait_for(self.disconnected_event.wait(), timeout=7.0)
                print(">>> KITE TICKER MANAGER: Disconnection confirmed by event.")
            except asyncio.TimeoutError:
                print(">>> KITE TICKER MANAGER: Warning: Timed out waiting for ticker to close.")
            finally:
                self.kws = None
        else:
            print(">>> KITE TICKER MANAGER: 'stop' called, but not connected.")
            
    def resubscribe(self, tokens):
        """
        Subscribes to a list of instrument tokens.
        """
        if self.is_connected and self.kws:
            print(f"Resubscribing to {len(tokens)} tokens.")
            self.kws.subscribe(tokens)
            self.kws.set_mode(self.kws.MODE_LTP, tokens)

