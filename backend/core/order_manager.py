# backend/core/order_manager.py
import asyncio
from core.kite import kite
import math # ADDED: For rounding

# ADDED: A utility function to round the price to the nearest valid tick (usually 0.05 for options)
def _round_to_tick(price, tick_size=0.05):
    """Rounds a price to the nearest valid tick size."""
    return round(round(price / tick_size) * tick_size, 2)

class OrderManager:
    """
    Handles the execution and verification of orders to make them more robust.
    """
    def __init__(self, log_debug_func):
        self.log_debug = log_debug_func

    # CHANGED: The function now accepts order_type and price for more flexibility
    async def execute_order(self, transaction_type, order_type=kite.ORDER_TYPE_MARKET, price=None, **kwargs):
        """
        Places an order and then enters a loop to verify its status.
        Can handle both MARKET and LIMIT orders.
        """
        MAX_RETRIES = 3 
        RETRY_DELAY_SECONDS = 2
        VERIFICATION_TIMEOUT_SECONDS = 15

        for attempt in range(MAX_RETRIES):
            try:
                # --- 1. Place the initial order ---
                def place_order_sync():
                    # Build the order parameters dictionary
                    order_params = {
                        "variety": kite.VARIETY_REGULAR,
                        "order_type": order_type,
                        "product": kite.PRODUCT_MIS,
                        "transaction_type": transaction_type,
                        **kwargs
                    }
                    # If it's a LIMIT order, add the price
                    if order_type == kite.ORDER_TYPE_LIMIT:
                        if price is None or price <= 0:
                            raise ValueError("A valid price must be provided for LIMIT orders.")
                        order_params["price"] = price
                    
                    return kite.place_order(**order_params)
                
                order_id = await asyncio.to_thread(place_order_sync)
                
                log_price = f"at limit {price}" if order_type == kite.ORDER_TYPE_LIMIT else "at MARKET"
                await self.log_debug("OrderManager", f"Placed {transaction_type} {order_type} order for {kwargs.get('tradingsymbol')} {log_price}. ID: {order_id}. Verifying status...")

                # --- 2. Verify the order status (this part remains the same) ---
                start_time = asyncio.get_event_loop().time()
                while True:
                    if (asyncio.get_event_loop().time() - start_time) > VERIFICATION_TIMEOUT_SECONDS:
                        raise Exception(f"Order {order_id} verification timed out after {VERIFICATION_TIMEOUT_SECONDS}s.")

                    def get_order_history_sync():
                        return kite.order_history(order_id=order_id)

                    order_history = await asyncio.to_thread(get_order_history_sync)
                    
                    if order_history:
                        latest_status = order_history[-1]['status']
                        if latest_status == "COMPLETE":
                            await self.log_debug("OrderManager", f"Order {order_id} confirmed COMPLETE.")
                            return "COMPLETE"
                        
                        if latest_status in ["REJECTED", "CANCELLED"]:
                            rejection_reason = order_history[-1].get('status_message', 'No reason provided.')
                            await self.log_debug("OrderManager", f"Order {order_id} was {latest_status}. Reason: {rejection_reason}. Retrying...")
                            break

                    await asyncio.sleep(1)
            
            except Exception as e:
                await self.log_debug("OrderManager-ERROR", f"Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                else:
                    await self.log_debug("OrderManager-CRITICAL", f"Order for {kwargs.get('tradingsymbol')} failed after {MAX_RETRIES} retries.")
                    raise 