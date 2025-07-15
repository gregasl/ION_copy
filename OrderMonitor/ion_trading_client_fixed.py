#!/usr/bin/env python3
"""
ION Trading Redis Client
Simplified Python client for communicating with Java ION trading system
"""

import redis
import json
import time
import threading
from datetime import datetime
from typing import Dict, Any, Optional, Callable

class IONTradingClient:
    """ION Trading System Redis Client"""
    
    def __init__(self, redis_host='cacheuat', redis_port=6379, redis_db=0):
        """
        Initialize client
        
        Args:
            redis_host: Redis host address
            redis_port: Redis port
            redis_db: Redis database number
        """
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        
        try:
            # Redis connection - increase timeout
            self.redis_client = redis.Redis(
                host=redis_host, 
                port=redis_port, 
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=10,
                socket_timeout=60,  # Increased to 60 seconds
                health_check_interval=30
            )
        except Exception as e:
            print(f"Failed to initialize Redis client: {e}")
            raise
        
        # Message channels
        self.MARKET_DATA_CHANNEL = "market_data"
        self.ORDER_COMMAND_CHANNEL = "order_commands"
        self.ORDER_RESPONSE_CHANNEL = "order_responses"
        self.HEARTBEAT_CHANNEL = "heartbeat"
        
        # Subscription client
        self.pubsub = self.redis_client.pubsub()
        
        # Callback functions
        self.market_data_callback: Optional[Callable] = None
        self.order_response_callback: Optional[Callable] = None
        self.heartbeat_callback: Optional[Callable] = None
        
        # Running status
        self.running = False
        self.listener_thread = None
        
        print(f"ION Trading Client initialized - {redis_host}:{redis_port}")
    
    def test_connection(self) -> bool:
        """Test Redis connection"""
        try:
            response = self.redis_client.ping()
            print(f"Redis connection test: {response}")
            return response
        except Exception as e:
            print(f"Redis connection failed: {e}")
            return False
    
    def start_listening(self):
        """Start listening to Redis messages"""
        if self.running:
            print("Already listening...")
            return
        
        self.running = True
        
        # Subscribe to relevant channels
        self.pubsub.subscribe(
            self.MARKET_DATA_CHANNEL,
            self.ORDER_RESPONSE_CHANNEL,
            self.HEARTBEAT_CHANNEL
        )
        
        # Start listener thread
        self.listener_thread = threading.Thread(target=self._message_listener)
        self.listener_thread.daemon = True
        self.listener_thread.start()
        
        print("Started listening to Redis channels...")
    
    def stop_listening(self):
        """Stop listening to Redis messages"""
        self.running = False
        if self.pubsub:
            self.pubsub.unsubscribe()
            self.pubsub.close()
        if self.listener_thread:
            self.listener_thread.join(timeout=5)
        print("Stopped listening to Redis channels")
    
    def _message_listener(self):
        """Message listener thread"""
        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break
                
                if message['type'] == 'message':
                    channel = message['channel']
                    data = message['data']
                    
                    try:
                        parsed_data = json.loads(data)
                        self._handle_message(channel, parsed_data)
                    except json.JSONDecodeError as e:
                        print(f"Failed to parse message: {data}, error: {e}")
                        
        except Exception as e:
            print(f"Message listener error: {e}")
    
    def _handle_message(self, channel: str, data: Dict[str, Any]):
        """Handle received messages"""
        try:
            if channel == self.MARKET_DATA_CHANNEL and self.market_data_callback:
                self.market_data_callback(data)
            elif channel == self.ORDER_RESPONSE_CHANNEL and self.order_response_callback:
                self.order_response_callback(data)
            elif channel == self.HEARTBEAT_CHANNEL and self.heartbeat_callback:
                self.heartbeat_callback(data)
            else:
                print(f"Received message on {channel}: {data}")
                
        except Exception as e:
            print(f"Error handling message: {e}")
    
    def set_market_data_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Set market data callback function"""
        self.market_data_callback = callback
    
    def set_order_response_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Set order response callback function"""
        self.order_response_callback = callback
    
    def set_heartbeat_callback(self, callback: Callable[[Dict[str, Any]], None]):
        """Set heartbeat callback function"""
        self.heartbeat_callback = callback
    
    def create_order(self, instrument: str, side: str, quantity: float, 
                    price: float, venue: str = "AUTO") -> bool:
        """
        Create order
        
        Args:
            instrument: Financial instrument identifier
            side: Order side ("Buy" or "Sell")
            quantity: Quantity
            price: Price
            venue: Trading venue
        
        Returns:
            Whether order was sent successfully
        """
        try:
            order_command = {
                "type": "create_order",
                "instrument": instrument,
                "side": side,
                "quantity": quantity,
                "price": price,
                "venue": venue,
                "timestamp": int(time.time() * 1000),
                "client_id": f"py_client_{int(time.time())}"
            }
            
            # Display message to be sent
            print(f"Publishing to channel '{self.ORDER_COMMAND_CHANNEL}':")
            print(f"Message: {json.dumps(order_command, indent=2)}")
            
            result = self.redis_client.publish(
                self.ORDER_COMMAND_CHANNEL, 
                json.dumps(order_command)
            )
            
            print(f"Order sent: {side} {quantity} {instrument} @ {price} on {venue}")
            print(f"Redis publish result: {result} (subscribers received the message)")
            return int(result) > 0
            
        except Exception as e:
            print(f"Failed to create order: {e}")
            return False
    
    def cancel_order(self, order_id: str, venue: str = "AUTO") -> bool:
        """
        Cancel order
        
        Args:
            order_id: Order ID
            venue: Trading venue
        
        Returns:
            Whether cancel command was sent successfully
        """
        try:
            cancel_command = {
                "type": "cancel_order",
                "order_id": order_id,
                "venue": venue,
                "timestamp": int(time.time() * 1000),
                "client_id": f"py_client_{int(time.time())}"
            }
            
            result = self.redis_client.publish(
                self.ORDER_COMMAND_CHANNEL,
                json.dumps(cancel_command)
            )
            
            print(f"Cancel order sent: {order_id} on {venue}")
            return result > 0
            
        except Exception as e:
            print(f"Failed to cancel order: {e}")
            return False


def main():
    """Example usage"""
    # Create client - configure correct Redis host
    client = IONTradingClient(redis_host='cacheuat', redis_port=6379)

    # Test connection
    if not client.test_connection():
        print("Redis connection failed, exiting...")
        print("Make sure Redis server is running on cacheuat:6379")
        return
    
    # Set callback functions
    def market_data_handler(data):
        print(f"[MARKET] {data['instrument']}: "
              f"Bid {data['bid_price']}/{data['bid_size']} "
              f"Ask {data['ask_price']}/{data['ask_size']}")
    
    def order_response_handler(data):
        print(f"[ORDER] {data['order_id']}: {data['status']} - {data['message']}")
    
    def heartbeat_handler(data):
        timestamp = datetime.fromtimestamp(data['timestamp'] / 1000)
        print(f"[HEARTBEAT] {timestamp} - {data['status']}")
    
    client.set_market_data_callback(market_data_handler)
    client.set_order_response_callback(order_response_handler)
    client.set_heartbeat_callback(heartbeat_handler)
    
    # Start listening
    client.start_listening()
    
    try:
        print("\n=== ION Trading Client Started ===")
        print("Commands:")
        print("  buy <quantity> <instrument> <price> [venue]")
        print("  sell <quantity> <instrument> <price> [venue]")
        print("  cancel <order_id> [venue]")
        print("  status - Show Redis connection and channel status")
        print("  test - Send test message to verify Redis")
        print("  quit")
        print("")
        print("Example:")
        print("  buy 100 912797PQ4 4.0 FENICS_USREPO")
        print("  sell 50 912797RH2 4.05")
        print("  cancel ORDER_123456")
        print("  status")
        print("")
        
        while True:
            try:
                user_input = input(">>> ").strip()
                
                if not user_input:
                    continue
                
                # Handle angle brackets and square brackets format
                user_input = user_input.replace('<', '').replace('>', '').replace('[', '').replace(']', '')
                command = user_input.split()
                
                if not command:
                    continue
                
                if command[0] == "quit":
                    break
                
                elif command[0] in ["buy", "sell"]:
                    if len(command) < 4:
                        print("Usage: buy/sell <quantity> <instrument> <price> [venue]")
                        print("Example: buy 100 912797PQ4 4.0 FENICS_USREPO")
                        continue
                    
                    try:
                        side = "Buy" if command[0] == "buy" else "Sell"
                        quantity = float(command[1])
                        instrument = command[2]
                        price = float(command[3])
                        venue = command[4] if len(command) > 4 else "AUTO"
                        
                        print(f"Sending {side} order: {quantity} {instrument} @ {price} on {venue}")
                        client.create_order(instrument, side, quantity, price, venue)
                        
                    except ValueError as e:
                        print(f"Invalid number format: {e}")
                        print("Make sure quantity and price are valid numbers")
                        continue
                
                elif command[0] == "cancel":
                    if len(command) < 2:
                        print("Usage: cancel <order_id> [venue]")
                        print("Example: cancel ORDER_123456 FENICS_USREPO")
                        continue
                    
                    order_id = command[1]
                    venue = command[2] if len(command) > 2 else "AUTO"
                    
                    print(f"Sending cancel request: {order_id} on {venue}")
                    client.cancel_order(order_id, venue)
                
                elif command[0] == "status":
                    # Display Redis connection status and channel information
                    print("=== Redis Status ===")
                    try:
                        info = client.redis_client.info()
                        print(f"Connected clients: {info.get('connected_clients', 'N/A')}")
                        print(f"Used memory: {info.get('used_memory_human', 'N/A')}")
                        
                        # Check subscription status
                        pubsub_info = client.redis_client.pubsub_channels()
                        print(f"Active channels: {list(pubsub_info) if pubsub_info else 'None'}")
                        
                        # Check subscriber count
                        for channel in [client.MARKET_DATA_CHANNEL, client.ORDER_COMMAND_CHANNEL, 
                                      client.ORDER_RESPONSE_CHANNEL, client.HEARTBEAT_CHANNEL]:
                            subscribers = client.redis_client.pubsub_numsub(channel)
                            print(f"Channel '{channel}': {subscribers[0][1] if subscribers else 0} subscribers")
                        
                    except Exception as e:
                        print(f"Error getting Redis status: {e}")
                
                elif command[0] == "test":
                    # Send test message
                    print("Sending test message...")
                    test_msg = {"type": "test", "timestamp": int(time.time() * 1000), "message": "Hello from Python client"}
                    result = client.redis_client.publish("test_channel", json.dumps(test_msg))
                    print(f"Test message sent, {result} subscribers received it")
                
                else:
                    print("Unknown command. Available commands:")
                    print("  buy 100 912797PQ4 4.0 [FENICS_USREPO]")
                    print("  sell 50 912797RH2 4.05")
                    print("  cancel ORDER_123456")
                    print("  status - Show Redis connection status")
                    print("  test - Send test message")
                    print("  quit")
                    
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Command error: {e}")
    
    finally:
        print("\nShutting down...")
        client.stop_listening()


if __name__ == "__main__":
    main()
