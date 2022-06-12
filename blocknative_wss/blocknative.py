import asyncio
import time
import uuid
from datetime import datetime
import json
import aiohttp
from typing import List, Mapping, Callable, Union, MutableMapping
from .utils import (
    raise_error_on_status,
    network_id_to_name,
    status_to_event_code,
    is_server_echo,
    subscription_type,
    SubscriptionType,
    to_camel_case,
)
from dataclasses import dataclass, field
from queue import Queue, Empty

PING_INTERVAL = 15
PING_TIMEOUT = 10
MESSAGE_SEND_INTERVAL = 0.021  # 21ms

BN_BASE_URL = 'wss://api.blocknative.com/v0'
BN_ETHEREUM = 'ethereum'
BN_ETHEREUM_ID = 1
BN_STREAM_CLASS_VERSION = '1.1'
API_VERSION = "0.2.6"
Callback = Callable[[dict, Callable], None]


def get_uuid1():
    s = uuid.uuid1()
    return str(s)


class HeartBeat(object):
    """ 心跳
    """

    def __init__(self):
        self._count = 0  # 心跳次数
        self._interval = 1  # 服务心跳执行时间间隔(秒)
        self._print_interval = 0  # 心跳打印时间间隔(秒)，0为不打印
        self._tasks = {}  # 跟随心跳执行的回调任务列表，由 self.register 注册 {task_id: {...}}

    @property
    def count(self):
        return self._count

    def ticker(self):
        """ 启动心跳， 每秒执行一次
        """
        self._count += 1

        # 打印心跳次数
        if self._print_interval > 0:
            if self._count % self._print_interval == 0:
                print("do server heartbeat, count:", self._count)

        # 设置下一次心跳回调
        asyncio.get_event_loop().call_later(self._interval, self.ticker)

        # 执行任务回调
        for task_id, task in self._tasks.items():
            interval = task["interval"]
            if self._count % interval != 0:
                continue
            func = task["func"]
            args = task["args"]
            kwargs = task["kwargs"]
            kwargs["task_id"] = task_id
            kwargs["heart_beat_count"] = self._count
            asyncio.get_event_loop().create_task(func(*args, **kwargs))

    def register(self, func, interval=1, *args, **kwargs):
        """ 注册一个任务，在每次心跳的时候执行调用
        """
        t = {
            "func": func,
            "interval": interval,
            "args": args,
            "kwargs": kwargs
        }
        task_id = get_uuid1()
        self._tasks[task_id] = t
        return task_id

    def unregister(self, task_id):
        """ 注销一个任务
        @param task_id 任务id
        """
        if task_id in self._tasks:
            self._tasks.pop(task_id)


@dataclass
class Subscription:
    """Dataclass representing the Subscription object.

    Attributes:
        callback: Callback function that will get executed for this subscription.
        data: Data associated with a subscription.
        sub_type: The type of subscription - `ADDRESS` or `TRANSACTION`.
    """

    callback: Callback
    data: dict
    sub_type: SubscriptionType


@dataclass
class Config:
    """Dataclass representing the client configuration object.

    Attributes:
        scope: The Ethereum or Bitcoin address that this configuration applies to,
        or `global` to apply the configuration gobally.
        filters: The array of valid filters. The Blocknative service uses jsql, a JavaScript query
        language to filter events.
        abi: The valid JSON ABI that will be used to decode input data for transactions
        that occur on the contract address defined in `scope`.
        watch_address: Defines whether the service should automatically watch the
        address as defined in `scope`.
    """

    scope: str
    filters: List[dict] = None
    abi: List[dict] = None
    watch_address: bool = True

    def as_dict(self) -> dict:
        """Filters out the None values.
        Returns:
            The Config class as a dict excluding fields with a None value.
        """
        return {
            'config': {
                to_camel_case(key): self.__dict__[key]
                for key in self.__dict__
                if self.__dict__[key] is not None
            }
        }


class BlockNativeSubscriber:
    global_filters: List[dict] = None
    _subscription_registry: MutableMapping[str, Subscription] = {}
    _message_queue: Queue = Queue()
    valid_session: bool = True

    def __init__(self, api_key: str, blockchain: str = BN_ETHEREUM, network_id: int = BN_ETHEREUM_ID,
                 global_filters: List[dict] = global_filters, send_hb_interval=0, heartbeat_msg=""):
        self._url = BN_BASE_URL
        # super(BlockNativeSubscriber, self).__init__(self._url, send_hb_interval=send_hb_interval)

        self.recv_count = 0
        self.api_key = api_key
        self.blockchain = blockchain
        self.network_id = network_id
        self.global_filters = global_filters

        self._check_conn_interval = PING_INTERVAL
        self._send_hb_interval = send_hb_interval
        self.ws = None  # websocket连接对象
        self.heartbeat_msg = heartbeat_msg  # 心跳消息
        self.heartbeat_msg_count = 0

    def initialize(self):

        # 注册服务 检查连接是否正常
        print("register the task and check the connection")
        heartbeat.register(self._check_connection, self._check_conn_interval)
        # 注册服务 发送心跳
        print("register the task and send the heartbeat")
        if self._send_hb_interval > 0:
            heartbeat.register(self._send_heartbeat_msg, self._send_hb_interval)
            # self.ws.ping()
            # loop.call_later(0.5, heartbeat.ticker)
        # 建立websocket连接
        print("establish websocket connection")
        asyncio.get_event_loop().create_task(self._connect())

    async def _connect(self):
        session = aiohttp.ClientSession()
        try:
            self.ws = await session.ws_connect(self._url, timeout=10)
            print("connection has been established!")
        except Exception as e:
            print("ERROR:{},{}".format(e.__class__, e))
            self.ws = await session.ws_connect(self._url, timeout=10)
            print(self.ws)
        except aiohttp.client.ClientConnectorError:
            print("connect to server error! url:", self._url)
            return
        asyncio.get_event_loop().create_task(self.connected_callback())
        asyncio.get_event_loop().create_task(self.receive())

    async def _reconnect(self):
        """ 重新建立websocket连接
        """
        print("reconnecting websocket right now!")
        await self._connect()

    async def receive(self):
        """ 接收消息
        """
        async for msg in self.ws:
            # print(msg)
            if msg.type == aiohttp.WSMsgType.TEXT and msg.data != 'pong':
                # print(msg , msg.type)
                pass
            if msg.type == aiohttp.WSMsgType.PONG:
                data = json.loads(msg.data)
                # print(msg, data)
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    # print("json succeed:", data)
                except:
                    data = msg.data
                    if data == 'pong':
                        self.heartbeat_msg_count += 1
                    if self.heartbeat_msg_count % 100 == 0:
                        print("{} pong finished!".format(self.heartbeat_msg_count))
                    # print("json fail:", data)
                await asyncio.get_event_loop().create_task(self.process(data))
            elif msg.type == aiohttp.WSMsgType.BINARY:
                await asyncio.get_event_loop().create_task(self.process_binary(msg.data))
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                print("receive event CLOSED:", msg)
                await asyncio.get_event_loop().create_task(self._reconnect())
                return
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print("receive event ERROR:", msg)
            else:
                print("unhandled msg:", msg)

    async def process_binary(self, msg):
        """ 处理websocket上接收到的消息 binary类型
        """
        raise NotImplementedError

    async def _check_connection(self, *args, **kwargs):
        """ 检查连接是否正常
        """
        # 检查websocket连接是否关闭，如果关闭，那么立即重连
        # print("self.ws")
        if not self.ws:
            print("websocket connection not connected yet!")
            return
        if self.ws.closed:
            await asyncio.get_event_loop().create_task(self._reconnect())
            return

    def run(self):
        self.initialize()

    async def connected_callback(self):
        # pass
        if self.global_filters:
            self._send_config_message('global', False, self.global_filters)

        self._queue_init_message()

        # Iterate over the registered subscriptions and push them onto the message queue
        for sub_id, subscription in self._subscription_registry.items():
            if subscription.sub_type == SubscriptionType.TRANSACTION:
                self._send_txn_watch_message(sub_id, status=subscription.data)
            elif subscription.sub_type == SubscriptionType.ADDRESS:
                self._send_config_message(
                    sub_id, True, subscription.data['filters'], subscription.data['abi']
                )

        asyncio.create_task(self._message_dispatcher())

    async def _message_dispatcher(self):
        """In a loop: Polls send message queue for latest messages to send to server.

        Waits ``MESSAGE_SEND_INTERVAL`` seconds before sending the next message
        in order to comply with the server's limit of 50 messages per second

        Note:
            This function runs until cancelled.
        """
        while self.valid_session:
            try:
                msg = self._message_queue.get_nowait()
                await self.ws.send_json(msg)
            except Empty:
                pass
            finally:
                await asyncio.sleep(MESSAGE_SEND_INTERVAL)

    def _queue_init_message(self):
        """Sends the initialization message e.g. the checkDappId event."""
        self.send_message(
            self._build_payload(category_code='initialize', event_code='checkDappId')
        )

    def subscribe_address(
            self,
            address: str,
            callback: Callback,
            filters: List[dict] = None,
            abi: List[dict] = None,
    ):
        """Subscribes to an address to listen to any incoming and
                outgoing transactions that occur on that address.

                Args:
                    address: The address to watch for incoming and outgoing transactions.
                    callback: The callback function that will get executed for this subscription.
                    filters: The filters by which to filter the transactions associated with the address.
                    abi: The ABI of the contract. Used if `address` is a contract address.

                Examples:
                    async def txn_handler(txn)
                        print(txn)

                    stream.subscribe('0x7a250d5630b4cf539739df2c5dacb4c659f2488d', txn_handler)
                """

        if self.blockchain == BN_ETHEREUM:
            address = address.lower()

        # Add this subscription to the registry
        self._subscription_registry[address] = Subscription(
            callback, {'filters': filters, 'abi': abi}, SubscriptionType.ADDRESS
        )

        # Only send the message if we are already connected. The connection handler
        # will send the messages within the registry upon connect.
        if self._is_connected():
            self._send_config_message(address, True, filters)

    def _send_config_message(
            self,
            scope,
            watch_address=True,
            filters: List[dict] = None,
            abi: List[dict] = None,
    ):
        """Helper method which constructs and sends the payload for watching addresses.

        Args:
            scope: The scope which this config applies to.
            watch_address: Indicates whether or not to watch the address  (if scope ==  `address`).
            filters: Filters used to filter out transactions for the given scope.
            abi: The ABI of the contract. Used if `scope` is a contract address.
        """
        message = self._build_payload(
            category_code='configs',
            event_code='put',
            data=Config(scope, filters, abi, watch_address).as_dict(),
        )
        # print("message", message)
        self.send_message(message)

    def _is_connected(self) -> bool:
        """Tests whether the websocket is connected.

        Returns:
            True if the websocket is connected, False otherwise.
        """
        return self.ws and not self.ws.closed

    # async def send_message(self, message: dict):
    #     await self.ws.send_json(message)

    def send_message(self, message: dict):
        """Sends a websocket message. (Adds the message to the queue to be sent).

        Args:
            message: The message to send.
        """
        self._message_queue.put(message)

    def _build_payload(
            self,
            category_code: str,
            event_code: str,
            data: Union[Config, Mapping[str, str]] = {},
    ) -> dict:
        """Helper method to construct the payload to send to the server.

        Args:
            category_code: The category code associated with the event.
            event_code: The event code associated with the event.
            data: The data associated with this payload. Can be a configuration object
            for filtering and watching addresses or an object for watching transactions.

        Returns:
            The constructed payload to send to the server.
        """
        return {
            'timeStamp': datetime.now().isoformat(),
            'dappId': self.api_key,
            'version': API_VERSION,
            'blockchain': {
                'system': self.blockchain,
                'network': network_id_to_name(self.network_id),
            },
            'categoryCode': category_code,
            'eventCode': event_code,
            **data,
        }

    async def process(self, msg):
        await self._message_handler(msg)

    def unsubscribe(self, watched_address):
        # remove this subscription from the registry so that we don't execute the callback
        del self._subscription_registry[watched_address]

        def _unsubscribe(_):
            self.send_message(
                self._build_payload(
                    category_code='accountAddress',
                    event_code='unwatch',
                    data={'account': {'address': watched_address}},
                )
            )

        return _unsubscribe

    def _send_txn_watch_message(self, txn_hash: str, status: str = 'sent'):
        """Helper method which constructs and sends the payload for watching transactions.

        Args:
            txn_hash: The hash of the transaction to watch.
            status: The status of the transaction to receive events for.
        """
        txn = {
            'transaction': {
                'hash': txn_hash,
                'startTime': int(time.time() * 1000),
                'status': status,
            }
        }
        self.send_message(
            self._build_payload(
                'activeTransaction',
                event_code=status_to_event_code(status),
                data=txn,
            )
        )

    async def _message_handler(self, message: dict):
        """Handles incoming WebSocket messages.

        Note:
            This function runs until cancelled.

        Args:
            message: The incoming websocket message.
        """
        # This should never happen but indicates an invalid message from the server
        if not 'status' in message:
            self.valid_session = False
            return

        # Raises an exception if the status of the message is an error
        raise_error_on_status(message)

        if 'event' in message:
            event = message['event']
            # Ignore server echo and unsubscribe messages
            if is_server_echo(event['eventCode']):
                return

            if 'transaction' in event:
                event_transaction = event['transaction']
                # Checks if the messsage is for a transaction subscription
                if subscription_type(message) == SubscriptionType.TRANSACTION:
                    # Find the matching subscription and run it's callback
                    transaction_hash = event_transaction['hash']
                    if transaction_hash in self._subscription_registry:
                        transaction = self._flatten_event_to_transaction(event)
                        await self._subscription_registry[transaction_hash].callback(transaction)

                # Checks if the messsage is for an address subscription
                elif subscription_type(message) == SubscriptionType.ADDRESS:
                    watched_address = event_transaction['watchedAddress']
                    if watched_address in self._subscription_registry and watched_address is not None:
                        # Find the matching subscription and run it's callback
                        transaction = self._flatten_event_to_transaction(event)
                        await self._subscription_registry[watched_address].callback(transaction, (
                            lambda: self.unsubscribe(watched_address)))

    def _flatten_event_to_transaction(self, event: dict):
        transaction = {}
        eventcopy = dict(event)
        del eventcopy['dappId']
        if 'transaction' in eventcopy:
            txn = eventcopy['transaction']
            for k in txn.keys():
                transaction[k] = txn[k]
            del eventcopy['transaction']
        if 'blockchain' in eventcopy:
            bc = eventcopy['blockchain']
            for k in bc.keys():
                transaction[k] = bc[k]
            del eventcopy['blockchain']
        if 'contractCall' in eventcopy:
            transaction['contractCall'] = eventcopy['contractCall']
            del eventcopy['contractCall']
        for k in eventcopy:
            if not isinstance(k, dict) and not isinstance(k, list):
                transaction[k] = eventcopy[k]
        return transaction

    async def _send_heartbeat_msg(self, *args, **kwargs):
        """ 发送心跳给服务器
        """
        print("in send hb")
        if not self.ws:
            print("websocket connection not connected yet!")
            return
        try:
            if self.heartbeat_msg:
                # print(self.heartbeat_msg)
                if isinstance(self.heartbeat_msg, dict):
                    await self.ws.send_json(self.heartbeat_msg)
                elif isinstance(self.heartbeat_msg, str):
                    await self.ws.send_str(self.heartbeat_msg)
                else:
                    print("send heartbeat msg failed! heartbeat msg:", self.heartbeat_msg)
                    return
                print("send ping message:", self.heartbeat_msg)
            else:
                print("send hb")
                await self.ws.ping()
        except ConnectionResetError:
            await self._reconnect()


heartbeat = HeartBeat()
