#!/usr/bin/env python3
"""
ION Trading System Python Client - 简洁版
匹配Java客户端的输出风格
"""

import redis
import json
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set
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
    ASSUMED_FILLED = "ASSUMED_FILLED"


class Venue(Enum):
    """交易场地枚举"""
    FENICS_USREPO = "FENICS_USREPO"
    BTEC_REPO_US = "BTEC_REPO_US"
    DEALERWEB_REPO = "DEALERWEB_REPO"


class OrderDetails:
    """订单详情类"""
    def __init__(self, order_id: str, cusip: str, side: str, quantity: float, 
                 price: float, venue: str):
        self.order_id = order_id
        self.cusip = cusip
        self.side = side
        self.quantity = quantity
        self.price = price
        self.venue = venue
        self.status = OrderStatus.PENDING.value
        self.filled_quantity = 0.0
        self.create_time = datetime.now()
        self.update_time = datetime.now()
        self.error_message = ""
        self.cleanup_timer = None
        
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'order_id': self.order_id,
            'cusip': self.cusip,
            'side': self.side,
            'quantity': self.quantity,
            'price': self.price,
            'venue': self.venue,
            'status': self.status,
            'filled_quantity': self.filled_quantity,
            'create_time': self.create_time.isoformat(),
            'update_time': self.update_time.isoformat(),
            'error_message': self.error_message,
            'age_seconds': (datetime.now() - self.create_time).total_seconds()
        }


class IONTradingClient:
    """
    ION 交易系统 Python 客户端 - 简洁版
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
        初始化客户端
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
        
        # 活跃订单跟踪
        self._active_orders: Dict[str, OrderDetails] = {}
        self._active_cusips: Set[str] = set()
        self._order_history: List[OrderDetails] = []
        self._orders_lock = threading.Lock()
        
        # 订阅线程
        self._subscriber_thread = None
        self._running = False
        
        # 自动启动
        self._initialize()
        
        # 注册退出清理
        atexit.register(self._cleanup)
        
    def _setup_logger(self, log_level: str) -> logging.Logger:
        """设置日志记录器 - 极简格式"""
        logger = logging.getLogger('IONTradingClient')
        logger.setLevel(getattr(logging, log_level.upper()))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            # 极简格式：只显示时间和消息
            formatter = logging.Formatter(
                '%(asctime)s - %(message)s',
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
            # 启动信息匹配Java格式
            self.logger.info("========== SYSTEM STARTUP ==========")
            self.logger.info(f"Redis bridge ready - {self.host}:{self.port} on {self.ORDER_COMMAND_CHANNEL}")
        except Exception as e:
            self.logger.error(f"无法连接到 Redis: {e}")
            raise
        
        # 自动启动监听
        self._start()
    
    def _start(self):
        """启动客户端"""
        if self._running:
            return
        
        self._running = True
        self._subscriber_thread = threading.Thread(target=self._subscribe_loop)
        self._subscriber_thread.daemon = True
        self._subscriber_thread.start()
    
    def _cleanup(self):
        """清理资源"""
        if self._running:
            self.logger.info("========== SHUTDOWN INITIATED ==========")
            self._running = False
            if self._subscriber_thread:
                self._subscriber_thread.join(timeout=2)
            
            # 取消所有定时器
            with self._orders_lock:
                for order in self._active_orders.values():
                    if order.cleanup_timer:
                        order.cleanup_timer.cancel()
            
            self.logger.info("========== SHUTDOWN COMPLETE ==========")
    
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
                # 静默处理心跳
                pass
                
        except Exception as e:
            self.logger.error(f"处理消息错误: {e}")
    
    def _handle_order_response(self, message: Dict[str, Any]):
        """处理订单响应 - 匹配Java格式"""
        order_id = message.get('order_id')
        status = message.get('status')
        venue = message.get('venue')
        error_msg = message.get('message', '')
        
        # 处理取消响应中的特殊情况
        if status == 'CANCEL_FAILED' and 'ORDER IS NOT ACTIVE' in error_msg:
            # 订单不活跃，应该从活跃列表中移除
            with self._orders_lock:
                if order_id in self._active_orders:
                    order = self._active_orders[order_id]
                    self.logger.error(f"Cancel request failed: {error_msg}")
                    # 移除不活跃的订单
                    self._handle_order_completion(order_id, order, 'NOT_ACTIVE')
                    return
        
        # 更新活跃订单状态
        self._update_order_status(order_id, status, error_msg)
        
        # 更新待处理的响应
        with self._response_lock:
            for key, future in list(self._pending_responses.items()):
                if not future.done():
                    if key.startswith('CREATE_'):
                        future.set_result(message)
                        del self._pending_responses[key]
                        break
                    elif key.startswith('CANCEL_') and order_id in key:
                        future.set_result(message)
                        del self._pending_responses[key]
                        break
    
    def _update_order_status(self, order_id: str, status: str, message: str = ''):
        """更新订单状态 - 简洁输出"""
        with self._orders_lock:
            order_found = False
            order_to_update = None
            old_order_id = None
            
            # 首先尝试直接匹配
            if order_id in self._active_orders:
                order_to_update = self._active_orders[order_id]
                order_found = True
            else:
                # 查找PENDING订单
                for oid, order in list(self._active_orders.items()):
                    if oid.startswith('PENDING_') and (
                        (order_id.startswith('REDIS_') and status == 'SUBMITTED') or
                        (not order_id.startswith('REDIS_') and not order_id.startswith('PENDING_'))
                    ):
                        if order.status in ['PENDING', 'SUBMITTED']:
                            order_to_update = order
                            old_order_id = oid
                            order_found = True
                            break
            
            if order_found and order_to_update:
                # 更新订单状态
                order_to_update.status = status
                order_to_update.update_time = datetime.now()
                if message:
                    order_to_update.error_message = message
                
                # 如果收到真实订单ID，需要更新
                if (not order_id.startswith('REDIS_') and 
                    not order_id.startswith('PENDING_') and 
                    not order_id.startswith('ERROR_') and
                    not order_id.startswith('FAILED_')):
                    
                    if order_to_update.order_id != order_id:
                        order_to_update.order_id = order_id
                    
                    if old_order_id and old_order_id != order_id:
                        self._active_orders[order_id] = order_to_update
                        del self._active_orders[old_order_id]
                
                # 根据状态输出不同的日志
                if status == 'SUBMITTED':
                    self.logger.info(f"Order response: {order_id} (SUBMITTED)")
                    self._print_active_monitoring()
                    self.logger.info("----")
                    self._schedule_order_cleanup(order_to_update)
                elif status == 'CANCELLED':
                    self.logger.info(f"Order {order_id} cancelled successfully")
                    self._handle_order_completion(order_to_update.order_id, order_to_update, status)
                elif status in ['FAILED', 'ERROR']:
                    self.logger.error(f"Order submission failed: {message}")
                    self._handle_order_completion(order_to_update.order_id, order_to_update, status)
                elif status == 'FILLED':
                    self._handle_order_completion(order_to_update.order_id, order_to_update, status)
    
    def _handle_order_completion(self, order_id: str, order: OrderDetails, status: str):
        """处理订单完成"""
        # 从活跃订单中移除
        if order_id in self._active_orders:
            del self._active_orders[order_id]
        
        # 从活跃CUSIP中移除
        cusip_still_active = any(o.cusip == order.cusip for o in self._active_orders.values())
        if not cusip_still_active and order.cusip in self._active_cusips:
            self._active_cusips.remove(order.cusip)
            # 只在真正的状态变化时才输出日志
            if status in ['CANCELLED', 'FILLED', 'FAILED', 'ERROR']:
                self.logger.info(f"Removed {order.cusip} from active monitoring (Status: {status})")
                self._print_active_monitoring()
        
        # 添加到历史记录
        self._order_history.append(order)
        
        # 限制历史记录大小
        if len(self._order_history) > 1000:
            self._order_history = self._order_history[-1000:]
        
        # 取消清理定时器
        if order.cleanup_timer:
            order.cleanup_timer.cancel()
    
    def _schedule_order_cleanup(self, order: OrderDetails):
        """安排订单自动清理"""
        if order.cleanup_timer:
            order.cleanup_timer.cancel()
        
        def cleanup():
            with self._orders_lock:
                if order.order_id in self._active_orders and order.status == 'SUBMITTED':
                    self.logger.info(f"Auto-cleanup: Order {order.order_id} still SUBMITTED after 60 seconds, assuming filled")
                    order.status = 'ASSUMED_FILLED'
                    self._handle_order_completion(order.order_id, order, 'ASSUMED_FILLED')
        
        order.cleanup_timer = threading.Timer(60.0, cleanup)
        order.cleanup_timer.daemon = True
        order.cleanup_timer.start()
    
    def _print_active_monitoring(self):
        """打印活跃监控状态 - 匹配Java格式"""
        if not self._active_cusips:
            self.logger.info("Active monitoring: None")
        else:
            self.logger.info(f"Active monitoring: {len(self._active_cusips)} CUSIPs [{', '.join(self._active_cusips)}]")
    
    def _handle_market_data(self, message: Dict[str, Any]):
        """处理市场数据 - 静默"""
        instrument = message.get('instrument')
        if instrument:
            self._market_data_cache[instrument] = message
    
    def create_order(self, 
                    instrument: str,
                    side: str,
                    quantity: float,
                    price: float,
                    venue: str,
                    timeout: float = 30.0) -> Dict[str, Any]:
        """
        创建订单并等待响应
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
        
        # 创建临时订单ID
        temp_order_id = f"PENDING_{int(time.time() * 1000)}"
        
        # 创建订单详情并添加到活跃订单
        with self._orders_lock:
            order_details = OrderDetails(temp_order_id, instrument, side, quantity, price, venue)
            self._active_orders[temp_order_id] = order_details
            self._active_cusips.add(instrument)
            self.logger.info(f"Added {instrument} to active monitoring")
        
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
        
        self.logger.info(f"Order command sent: {side} {quantity} {instrument} @ {price} on {venue}")
        self._print_active_monitoring()
        
        # 等待响应
        try:
            result = future.result(timeout=timeout)
            return result
        except FutureTimeoutError:
            with self._response_lock:
                if request_key in self._pending_responses:
                    del self._pending_responses[request_key]
            # 超时时也要清理
            with self._orders_lock:
                if temp_order_id in self._active_orders:
                    order_details.status = 'TIMEOUT'
                    self._handle_order_completion(temp_order_id, order_details, 'TIMEOUT')
            raise TimeoutError(f"等待订单响应超时 ({timeout}秒)")
    
    def cancel_order(self, order_id: str, venue: str, timeout: float = 10.0) -> Dict[str, Any]:
        """
        取消订单并等待响应
        """
        if not order_id:
            raise ValueError("订单ID不能为空")
        
        if venue not in [v.value for v in Venue]:
            raise ValueError(f"venue 必须是以下之一: {[v.value for v in Venue]}")
        
        # 先更新本地状态为CANCEL_PENDING
        with self._orders_lock:
            if order_id in self._active_orders:
                self._active_orders[order_id].status = 'CANCEL_PENDING'
        
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
        
        self.logger.info(f"Cancel order request: {order_id} on {venue}")
        self.logger.info("----")
        
        # 等待响应
        try:
            result = future.result(timeout=timeout)
            return result
        except FutureTimeoutError:
            with self._response_lock:
                if request_key in self._pending_responses:
                    del self._pending_responses[request_key]
            raise TimeoutError(f"等待取消响应超时 ({timeout}秒)")
    
    def get_active_orders(self) -> List[Dict[str, Any]]:
        """获取所有活跃订单"""
        with self._orders_lock:
            return [order.to_dict() for order in self._active_orders.values()]
    
    def get_active_cusips(self) -> List[str]:
        """获取所有活跃的CUSIP"""
        with self._orders_lock:
            return list(self._active_cusips)
    
    def get_order_by_id(self, order_id: str) -> Optional[Dict[str, Any]]:
        """根据订单ID获取订单详情"""
        with self._orders_lock:
            # 先查找活跃订单
            if order_id in self._active_orders:
                return self._active_orders[order_id].to_dict()
            
            # 再查找历史订单
            for order in reversed(self._order_history):
                if order.order_id == order_id:
                    return order.to_dict()
        
        return None
    
    def get_orders_by_cusip(self, cusip: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """根据CUSIP获取订单"""
        with self._orders_lock:
            orders = []
            
            # 活跃订单
            for order in self._active_orders.values():
                if order.cusip == cusip:
                    orders.append(order.to_dict())
            
            # 历史订单
            if not active_only:
                for order in self._order_history:
                    if order.cusip == cusip:
                        orders.append(order.to_dict())
            
            return orders
    
    def get_order_statistics(self) -> Dict[str, Any]:
        """获取订单统计信息"""
        with self._orders_lock:
            active_count = len(self._active_orders)
            history_count = len(self._order_history)
            
            # 统计各状态订单数
            status_counts = {}
            for order in self._active_orders.values():
                status = order.status
                status_counts[status] = status_counts.get(status, 0) + 1
            
            # 统计各场地订单数
            venue_counts = {}
            for order in self._active_orders.values():
                venue = order.venue
                venue_counts[venue] = venue_counts.get(venue, 0) + 1
            
            return {
                'active_orders': active_count,
                'active_cusips': len(self._active_cusips),
                'history_orders': history_count,
                'status_breakdown': status_counts,
                'venue_breakdown': venue_counts,
                'monitoring_cusips': list(self._active_cusips)
            }
    
    def clear_history(self):
        """清空历史订单记录"""
        with self._orders_lock:
            self._order_history.clear()
            self.logger.info("历史订单记录已清空")
    
    def get_market_data(self, instrument: str) -> Optional[Dict[str, Any]]:
        """获取缓存的市场数据"""
        return self._market_data_cache.get(instrument)
    
    def get_all_market_data(self) -> Dict[str, Dict[str, Any]]:
        """获取所有缓存的市场数据"""
        return self._market_data_cache.copy()
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        r = redis.Redis(connection_pool=self.pool)
        
        # 检查 Redis 连接
        try:
            r.ping()
            redis_status = "connected"
        except:
            redis_status = "disconnected"
        
        stats = self.get_order_statistics()
        
        return {
            "redis_status": redis_status,
            "client_running": self._running,
            "pending_requests": len(self._pending_responses),
            "cached_instruments": len(self._market_data_cache),
            "connection": f"{self.host}:{self.port}",
            **stats
        }


# 使用示例和测试代码
if __name__ == "__main__":
    import sys
    
    # 创建客户端（自动连接和启动）
    client = IONTradingClient()
    
    print("\n========== SYSTEM READY ==========")
    print("\n选择操作:")
    print("1. 创建买单")
    print("2. 创建卖单")
    print("3. 取消订单")
    print("4. 查看市场数据")
    print("5. 查看系统状态")
    print("6. 查看活跃订单")
    print("7. 查看订单统计")
    print("8. 根据CUSIP查询订单")
    print("9. 清空历史记录")
    print("0. 退出")
    
    # 交互式菜单
    while True:
        choice = input("\n请输入选择 (0-9): ").strip()
        
        try:
            if choice == '0':
                break
                
            elif choice in ['1', '2']:
                # 创建订单
                side = 'Buy' if choice == '1' else 'Sell'
                
                instrument = input("输入CUSIP: ").strip()
                if not instrument:
                    print("错误: CUSIP不能为空")
                    continue
                    
                quantity_str = input("输入数量: ").strip()
                if not quantity_str:
                    print("错误: 数量不能为空")
                    continue
                try:
                    quantity = float(quantity_str)
                    if quantity <= 0:
                        print("错误: 数量必须大于0")
                        continue
                except ValueError:
                    print("错误: 无效的数量格式")
                    continue
                
                price_str = input("输入价格: ").strip()
                if not price_str:
                    print("错误: 价格不能为空")
                    continue
                try:
                    price = float(price_str)
                    if price <= 0:
                        print("错误: 价格必须大于0")
                        continue
                except ValueError:
                    print("错误: 无效的价格格式")
                    continue
                
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
                    print("错误: 无效的场地选择")
                    continue
                
                print()  # 空行
                result = client.create_order(
                    instrument=instrument,
                    side=side,
                    quantity=quantity,
                    price=price,
                    venue=venue
                )
                
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
                
                print()  # 空行
                result = client.cancel_order(order_id, venue)
                
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
                    if isinstance(value, dict):
                        print(f"  {key}:")
                        for k, v in value.items():
                            print(f"    {k}: {v}")
                    else:
                        print(f"  {key}: {value}")
                    
            elif choice == '6':
                # 查看活跃订单
                active_orders = client.get_active_orders()
                if active_orders:
                    print(f"\n活跃订单 ({len(active_orders)} 个):")
                    for order in active_orders:
                        print(f"\n  订单ID: {order['order_id']}")
                        print(f"  CUSIP: {order['cusip']}")
                        print(f"  方向: {order['side']}")
                        print(f"  数量: {order['quantity']}")
                        print(f"  价格: {order['price']}")
                        print(f"  场地: {order['venue']}")
                        print(f"  状态: {order['status']}")
                        print(f"  年龄: {order['age_seconds']:.0f}秒")
                else:
                    print("\n无活跃订单")
                    
            elif choice == '7':
                # 查看订单统计
                stats = client.get_order_statistics()
                print("\n订单统计:")
                print(f"  活跃订单数: {stats['active_orders']}")
                print(f"  活跃CUSIP数: {stats['active_cusips']}")
                print(f"  历史订单数: {stats['history_orders']}")
                
                if stats['status_breakdown']:
                    print("\n  状态分布:")
                    for status, count in stats['status_breakdown'].items():
                        print(f"    {status}: {count}")
                
                if stats['venue_breakdown']:
                    print("\n  场地分布:")
                    for venue, count in stats['venue_breakdown'].items():
                        print(f"    {venue}: {count}")
                
                if stats['monitoring_cusips']:
                    print(f"\n  监控中的CUSIP: {', '.join(stats['monitoring_cusips'])}")
                    
            elif choice == '8':
                # 根据CUSIP查询订单
                cusip = input("输入CUSIP: ").strip()
                if cusip:
                    active_only = input("只显示活跃订单? (y/n, 默认: y): ").strip().lower() != 'n'
                    
                    orders = client.get_orders_by_cusip(cusip, active_only)
                    if orders:
                        print(f"\nCUSIP {cusip} 的订单 ({len(orders)} 个):")
                        for order in orders:
                            print(f"\n  订单ID: {order['order_id']}")
                            print(f"  状态: {order['status']}")
                            print(f"  方向: {order['side']}")
                            print(f"  数量: {order['quantity']}")
                            print(f"  价格: {order['price']}")
                            print(f"  场地: {order['venue']}")
                    else:
                        print(f"\n未找到CUSIP {cusip} 的订单")
                        
            elif choice == '9':
                # 清空历史记录
                confirm = input("确认清空历史记录? (y/n): ").strip().lower()
                if confirm == 'y':
                    client.clear_history()
                    
            else:
                print("\n无效的选择")
                
        except Exception as e:
            print(f"\n错误: {e}")