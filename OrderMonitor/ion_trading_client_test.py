#!/usr/bin/env python3
"""
ION Trading System Python Client - 简化版
用于与 ION 交易系统进行交互的 Python 客户端

使用示例:
    from ion_trading_client import IONTradingClient
    
    # 创建客户端（自动连接和启动）
    client = IONTradingClient()
    
    # 创建订单并获取结果
    result = client.create_order('912797PQ4', 'Buy', 100.0, 4.0, 'FENICS_USREPO')
    print(result)  # {'order_id': '5711693938264375308_20250723', 'status': 'SUBMITTED', ...}
    
    # 取消订单并获取结果
    result = client.cancel_order('5711693938264375308_20250723', 'FENICS_USREPO')
    print(result)  # {'status': 'CANCELLED', 'message': 'Order cancelled successfully'}
"""

import redis
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
import threading
import logging
from enum import Enum
from concurrent.futures import Future, TimeoutError as FutureTimeoutError
import atexit


class OrderSide(Enum):
    """订单方向枚举"""
    BUY = "Buy"
    SELL = "Sell"


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "PENDING"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    CANCEL_PENDING = "CANCEL_PENDING"
    CANCEL_FAILED = "CANCEL_FAILED"


class Venue(Enum):
    """交易场地枚举"""
    FENICS_USREPO = "FENICS_USREPO"
    BTEC_REPO_US = "BTEC_REPO_US"
    DEALERWEB_REPO = "DEALERWEB_REPO"


class IONTradingClient:
    """
    ION 交易系统 Python 客户端 - 简化版
    自动处理连接、启动和清理
    """
    
    # Redis 频道定义
    MARKET_DATA_CHANNEL = "ION:ION_TEST:market_data"
    ORDER_COMMAND_CHANNEL = "ION:ION_TEST:ORDER_COMMANDS"
    ORDER_RESPONSE_CHANNEL = "ION:ION_TEST:ORDER_RESPONSES"
    HEARTBEAT_CHANNEL = "ION:ION_TEST:heartbeat"
    
    def __init__(self, host: str = 'cacheuat', port: int = 6379, 
                 password: Optional[str] = None, db: int = 0,
                 log_level: str = 'INFO'):
        """
        初始化客户端（自动连接并启动）
        
        Args:
            host: Redis 主机地址
            port: Redis 端口
            password: Redis 密码（如果需要）
            db: Redis 数据库编号
            log_level: 日志级别 ('DEBUG', 'INFO', 'WARNING', 'ERROR')
        """
        self.host = host
        self.port = port
        self.logger = self._setup_logger(log_level)
        
        # 创建 Redis 连接池
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            password=password,
            db=db,
            decode_responses=True
        )
        
        # 存储待处理的订单响应
        self._pending_responses = {}
        self._response_lock = threading.Lock()
        
        # 市场数据缓存
        self._market_data_cache = {}
        
        # 订阅线程
        self._subscriber_thread = None
        self._running = False
        
        # 自动启动
        self._initialize()
        
        # 注册退出清理
        atexit.register(self._cleanup)
        
    def _setup_logger(self, log_level: str) -> logging.Logger:
        """设置日志记录器"""
        logger = logging.getLogger('IONTradingClient')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%H:%M:%S'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize(self):
        """初始化客户端"""
        # 测试连接
        try:
            r = redis.Redis(connection_pool=self.pool)
            r.ping()
            self.logger.info(f"成功连接到 Redis: {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"无法连接到 Redis: {e}")
            raise
        
        # 自动启动监听
        self._start()
    
    def _start(self):
        """启动客户端（开始监听响应）"""
        if self._running:
            return
        
        self._running = True
        self._subscriber_thread = threading.Thread(target=self._subscribe_loop)
        self._subscriber_thread.daemon = True
        self._subscriber_thread.start()
        self.logger.debug("客户端监听已启动")
    
    def _cleanup(self):
        """清理资源"""
        if self._running:
            self._running = False
            if self._subscriber_thread:
                self._subscriber_thread.join(timeout=2)
            self.logger.debug("客户端已清理")
    
    def _subscribe_loop(self):
        """订阅循环"""
        r = redis.Redis(connection_pool=self.pool)
        pubsub = r.pubsub()
        
        # 订阅频道
        pubsub.subscribe(
            self.ORDER_RESPONSE_CHANNEL,
            self.MARKET_DATA_CHANNEL,
            self.HEARTBEAT_CHANNEL
        )
        
        self.logger.debug("开始监听响应...")
        
        while self._running:
            try:
                message = pubsub.get_message(timeout=1.0)
                if message and message['type'] == 'message':
                    self._handle_message(message['channel'], message['data'])
            except Exception as e:
                self.logger.error(f"订阅循环错误: {e}")
                time.sleep(1)
    
    def _handle_message(self, channel: str, data: str):
        """处理接收到的消息"""
        try:
            message = json.loads(data)
            
            if channel == self.ORDER_RESPONSE_CHANNEL:
                self._handle_order_response(message)
            elif channel == self.MARKET_DATA_CHANNEL:
                self._handle_market_data(message)
            elif channel == self.HEARTBEAT_CHANNEL:
                self._handle_heartbeat(message)
                
        except Exception as e:
            self.logger.error(f"处理消息错误: {e}")
    
    def _handle_order_response(self, message: Dict[str, Any]):
        """处理订单响应"""
        order_id = message.get('order_id')
        status = message.get('status')
        venue = message.get('venue')
        
        self.logger.info(f"收到订单响应: {order_id} -> {status} (场地: {venue})")
        
        # 更新待处理的响应
        with self._response_lock:
            # 查找匹配的请求
            for key, future in list(self._pending_responses.items()):
                if not future.done():
                    # 对于创建订单，匹配临时ID或返回的订单ID
                    if key.startswith('CREATE_') and (
                        order_id.startswith('REDIS_') or 
                        order_id.startswith('PENDING_') or
                        status in ['SUBMITTED', 'FAILED']
                    ):
                        future.set_result(message)
                        del self._pending_responses[key]
                        break
                    # 对于取消订单，匹配订单ID
                    elif key.startswith('CANCEL_') and order_id in key:
                        future.set_result(message)
                        del self._pending_responses[key]
                        break
    
    def _handle_market_data(self, message: Dict[str, Any]):
        """处理市场数据"""
        instrument = message.get('instrument')
        if instrument:
            self._market_data_cache[instrument] = message
            self.logger.debug(f"市场数据更新: {instrument}")
    
    def _handle_heartbeat(self, message: Dict[str, Any]):
        """处理心跳消息"""
        self.logger.debug("收到心跳消息")
    
    def create_order(self, 
                    instrument: str,
                    side: str,
                    quantity: float,
                    price: float,
                    venue: str,
                    timeout: float = 30.0) -> Dict[str, Any]:
        """
        创建订单并等待响应
        
        Args:
            instrument: 证券代码 (CUSIP)
            side: 买卖方向 ('Buy' 或 'Sell')
            quantity: 数量
            price: 价格
            venue: 交易场地 ('FENICS_USREPO', 'BTEC_REPO_US', 'DEALERWEB_REPO')
            timeout: 等待响应的超时时间（秒）
            
        Returns:
            订单响应字典，包含:
            - order_id: 订单ID
            - status: 订单状态
            - message: 状态消息
            - venue: 交易场地
            - timestamp: 时间戳
            
        Raises:
            ValueError: 参数无效
            TimeoutError: 等待响应超时
        """
        # 验证参数
        if side not in ['Buy', 'Sell']:
            raise ValueError("side 必须是 'Buy' 或 'Sell'")
        
        if venue not in [v.value for v in Venue]:
            raise ValueError(f"venue 必须是以下之一: {[v.value for v in Venue]}")
        
        if price <= 0:
            raise ValueError("价格必须大于0")
        
        if quantity <= 0:
            raise ValueError("数量必须大于0")
        
        # 创建订单命令
        order_command = {
            "type": "create_order",
            "instrument": instrument,
            "side": side,
            "quantity": quantity,
            "price": price,
            "venue": venue,
            "timestamp": int(time.time() * 1000)
        }
        
        # 创建Future用于等待响应
        future = Future()
        request_key = f"CREATE_{int(time.time() * 1000)}"
        
        with self._response_lock:
            self._pending_responses[request_key] = future
        
        # 发送命令
        r = redis.Redis(connection_pool=self.pool)
        r.publish(self.ORDER_COMMAND_CHANNEL, json.dumps(order_command))
        
        self.logger.info(f"发送订单: {side} {quantity} {instrument} @ {price} on {venue}")
        
        # 等待响应
        try:
            result = future.result(timeout=timeout)
            return result
        except FutureTimeoutError:
            with self._response_lock:
                if request_key in self._pending_responses:
                    del self._pending_responses[request_key]
            raise TimeoutError(f"等待订单响应超时 ({timeout}秒)")
    
    def cancel_order(self, order_id: str, venue: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        取消订单并等待响应
        
        Args:
            order_id: 订单ID
            venue: 交易场地
            timeout: 等待响应的超时时间（秒）
            
        Returns:
            取消响应字典，包含:
            - status: 取消状态 ('CANCELLED', 'CANCEL_FAILED', etc.)
            - message: 状态消息
            - order_id: 订单ID
            - venue: 交易场地
            
        Raises:
            ValueError: 参数无效
            TimeoutError: 等待响应超时
        """
        if not order_id:
            raise ValueError("订单ID不能为空")
        
        if venue not in [v.value for v in Venue]:
            raise ValueError(f"venue 必须是以下之一: {[v.value for v in Venue]}")
        
        # 创建取消命令
        cancel_command = {
            "type": "cancel_order",
            "order_id": order_id,
            "venue": venue,
            "timestamp": int(time.time() * 1000)
        }
        
        # 创建Future用于等待响应
        future = Future()
        request_key = f"CANCEL_{order_id}_{int(time.time() * 1000)}"
        
        with self._response_lock:
            self._pending_responses[request_key] = future
        
        # 发送命令
        r = redis.Redis(connection_pool=self.pool)
        r.publish(self.ORDER_COMMAND_CHANNEL, json.dumps(cancel_command))
        
        self.logger.info(f"发送取消订单: {order_id} on {venue}")
        
        # 等待响应
        try:
            result = future.result(timeout=timeout)
            return result
        except FutureTimeoutError:
            with self._response_lock:
                if request_key in self._pending_responses:
                    del self._pending_responses[request_key]
            raise TimeoutError(f"等待取消响应超时 ({timeout}秒)")
    
    def get_market_data(self, instrument: str) -> Optional[Dict[str, Any]]:
        """
        获取缓存的市场数据
        
        Args:
            instrument: 证券代码
            
        Returns:
            市场数据字典或None
        """
        return self._market_data_cache.get(instrument)
    
    def get_all_market_data(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有缓存的市场数据
        
        Returns:
            所有市场数据的字典
        """
        return self._market_data_cache.copy()
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        获取系统状态
        
        Returns:
            系统状态信息
        """
        r = redis.Redis(connection_pool=self.pool)
        
        # 检查 Redis 连接
        try:
            r.ping()
            redis_status = "connected"
        except:
            redis_status = "disconnected"
        
        return {
            "redis_status": redis_status,
            "client_running": self._running,
            "pending_requests": len(self._pending_responses),
            "cached_instruments": len(self._market_data_cache),
            "connection": f"{self.host}:{self.port}"
        }


# 使用示例和测试代码
if __name__ == "__main__":
    import sys
    
    print("=== ION Trading Client 测试 ===\n")
    
    # 创建客户端（自动连接和启动）
    client = IONTradingClient()
    
    # 显示系统状态
    status = client.get_system_status()
    print("系统状态:")
    for key, value in status.items():
        print(f"  {key}: {value}")
    print()
    
    # 交互式菜单
    while True:
        print("\n选择操作:")
        print("1. 创建买单")
        print("2. 创建卖单")
        print("3. 取消订单")
        print("4. 查看市场数据")
        print("5. 查看系统状态")
        print("0. 退出")
        
        choice = input("\n请输入选择 (0-5): ").strip()
        
        try:
            if choice == '0':
                print("\n退出程序...")
                break
                
            elif choice in ['1', '2']:
                # 创建订单
                side = 'Buy' if choice == '1' else 'Sell'
                
                instrument = input("输入CUSIP (默认: 912797PQ4): ").strip() or '912797PQ4'
                quantity = float(input("输入数量 (默认: 100): ").strip() or '100')
                price = float(input("输入价格 (默认: 4.0): ").strip() or '4.0')
                
                print("\n选择交易场地:")
                print("1. FENICS_USREPO")
                print("2. BTEC_REPO_US")
                print("3. DEALERWEB_REPO")
                venue_choice = input("选择 (1-3, 默认: 1): ").strip() or '1'
                
                venue_map = {
                    '1': 'FENICS_USREPO',
                    '2': 'BTEC_REPO_US',
                    '3': 'DEALERWEB_REPO'
                }
                venue = venue_map.get(venue_choice, 'FENICS_USREPO')
                
                print(f"\n发送{side}单: {instrument} {quantity} @ {price} on {venue}...")
                
                result = client.create_order(
                    instrument=instrument,
                    side=side,
                    quantity=quantity,
                    price=price,
                    venue=venue
                )
                
                print(f"\n订单结果:")
                print(f"  订单ID: {result.get('order_id')}")
                print(f"  状态: {result.get('status')}")
                print(f"  消息: {result.get('message')}")
                print(f"  场地: {result.get('venue')}")
                
            elif choice == '3':
                # 取消订单
                order_id = input("输入订单ID: ").strip()
                
                print("\n选择交易场地:")
                print("1. FENICS_USREPO")
                print("2. BTEC_REPO_US")
                print("3. DEALERWEB_REPO")
                venue_choice = input("选择 (1-3): ").strip()
                
                venue_map = {
                    '1': 'FENICS_USREPO',
                    '2': 'BTEC_REPO_US',
                    '3': 'DEALERWEB_REPO'
                }
                venue = venue_map.get(venue_choice)
                
                if not venue:
                    print("无效的场地选择")
                    continue
                
                print(f"\n取消订单: {order_id} on {venue}...")
                
                result = client.cancel_order(order_id, venue)
                
                print(f"\n取消结果:")
                print(f"  状态: {result.get('status')}")
                print(f"  消息: {result.get('message')}")
                
            elif choice == '4':
                # 查看市场数据
                market_data = client.get_all_market_data()
                if market_data:
                    print("\n市场数据:")
                    for instrument, data in market_data.items():
                        print(f"  {instrument}:")
                        print(f"    Bid: {data.get('bid_price')} x {data.get('bid_size')}")
                        print(f"    Ask: {data.get('ask_price')} x {data.get('ask_size')}")
                else:
                    print("\n暂无市场数据")
                    
            elif choice == '5':
                # 查看系统状态
                status = client.get_system_status()
                print("\n系统状态:")
                for key, value in status.items():
                    print(f"  {key}: {value}")
                    
            else:
                print("\n无效的选择")
                
        except Exception as e:
            print(f"\n错误: {e}")