import asyncio
import aioamqp
import inspect
import uuid
import functools
import json
import traceback

class Event:
    """ Event base.

    Attributes:
        name: Event name.
        exchange: Exchange name.
        queue: Queue name.
        routing_key: Routing key name.
        pre_fetch_count: How may message per fetched, default is 1.
        data: Message content.
    """

    def __init__(self, name=None, exchange=None, queue=None, routing_key=None, pre_fetch_count=1, data=None,
                 event_center=None):
        """Initialize."""
        self._name = name
        self._exchange = exchange
        self._queue = queue
        self._routing_key = routing_key
        self._pre_fetch_count = pre_fetch_count
        self._data = data
        self._callback = None  # Asynchronous callback function.
        self.event_center = event_center

    @property
    def name(self):
        return self._name

    @property
    def exchange(self):
        return self._exchange

    @property
    def queue(self):
        return self._queue

    @property
    def routing_key(self):
        return self._routing_key

    @property
    def prefetch_count(self):
        return self._pre_fetch_count

    @property
    def data(self):
        return self._data

    def parse(self):
        raise NotImplemented

    def dumps(self):
        d = {
            "n": self.name,
            "d": self.data
        }
        s = json.dumps(d)
        return s

    def loads(self, b):
        d = json.loads(b.decode("utf8"))
        self._name = d.get("n")
        self._data = d.get("d")
        return d

    def subscribe(self, callback, multi=False):
        """ Subscribe this event.

        Args:
            callback: Asynchronous callback function.
            multi: If subscribe multiple channels ?
        """

        self._callback = callback
        SingleTask.run(self.event_center.subscribe, self, self.callback, multi)

    def publish(self):
        """Publish this event."""
        SingleTask.run(self.event_center.publish, self)
        # print("publish the event")

    async def callback(self, channel, body, envelope, properties):
        self._exchange = envelope.exchange_name
        self._routing_key = envelope.routing_key
        self.loads(body)
        o = self.parse()
        await self._callback(o)

    def __str__(self):
        info = "EVENT: name={n}, exchange={e}, queue={q}, routing_key={r}, data={d}".format(
            e=self.exchange, q=self.queue, r=self.routing_key, n=self.name, d=self.data)
        return info

    def __repr__(self):
        return str(self)


class EventPendingTx(Event):

    def __init__(self, server_id, routing_key, event_center,data={}):
        # name = "EVENT_KLINE_15MIN"
        # exchange = "Kline.15min"
        name = "TX_PENDING"
        exchange = "pendingTx"
        routing_key = routing_key
        queue = "{server_id}.{exchange}.{routing_key}".format(server_id=server_id,
                                                              exchange=exchange,
                                                              routing_key=routing_key)
        if isinstance(data, dict):
            data = json.dumps(data)
        super(EventPendingTx, self).__init__(name, exchange, queue, routing_key, data=data, event_center=event_center)

    def parse(self):
        return self.data

class HeartBeat(object):
    """ 心跳
    """

    def __init__(self, interval=0):
        self._count = 0  # 心跳次数
        self._interval = 1  # 服务心跳执行时间间隔(秒)
        self._print_interval = interval  # 心跳打印时间间隔(秒)，0为不打印
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
        @param func 心跳的时候执行的函数
        @param interval 执行回调的时间间隔(秒)
        @return task_id 任务id
        """
        t = {
            "func": func,
            "interval": interval,
            "args": args,
            "kwargs": kwargs
        }
        task_id = uuid.uuid1()

        self._tasks[task_id] = t
        return task_id

    def unregister(self, task_id):
        """ 注销一个任务
        @param task_id 任务id
        """
        if task_id in self._tasks:
            self._tasks.pop(task_id)


heartbeat = HeartBeat()
METHOD_LOCKERS = {}


class LoopRunTask(object):
    """ Loop run task.
    """

    @classmethod
    def register(cls, func, interval=1, *args, **kwargs):
        """ Register a loop run.

        Args:
            func: Asynchronous callback function.
            interval: execute interval time(seconds), default is 1s.

        Returns:
            task_id: Task id.
        """
        task_id = heartbeat.register(func, interval, *args, **kwargs)
        return task_id

    @classmethod
    def unregister(cls, task_id):
        """ Unregister a loop run task.

        Args:
            task_id: Task id.
        """
        heartbeat.unregister(task_id)


class SingleTask:
    """ Single run task.
    """

    @classmethod
    def run(cls, func, *args, **kwargs):
        """ Create a coroutine and execute immediately.

        Args:
            func: Asynchronous callback function.
        """
        asyncio.get_event_loop().create_task(func(*args, **kwargs))

    @classmethod
    def call_later(cls, func, delay=0, *args, **kwargs):
        """ Create a coroutine and delay execute, delay time is seconds, default delay time is 0s.

        Args:
            func: Asynchronous callback function.
            delay: Delay time is seconds, default delay time is 0, you can assign a float e.g. 0.5, 2.3, 5.1 ...
        """
        if not inspect.iscoroutinefunction(func):
            asyncio.get_event_loop().call_later(delay, func, *args)
        else:
            def foo(f, *args, **kwargs):
                asyncio.get_event_loop().create_task(f(*args, **kwargs))

            asyncio.get_event_loop().call_later(delay, foo, func, *args)


def async_method_locker(name, wait=True):
    """ In order to share memory between any asynchronous coroutine methods, we should use locker to lock our method,
        so that we can avoid some un-prediction actions.

    Args:
        name: Locker name.
        wait: If waiting to be executed when the locker is locked? if True, waiting until to be executed, else return
            immediately (do not execute).

    NOTE:
        This decorator must to be used on `async method`.
    """
    assert isinstance(name, str)

    def decorating_function(method):
        global METHOD_LOCKERS
        locker = METHOD_LOCKERS.get(name)
        if not locker:
            locker = asyncio.Lock()
            METHOD_LOCKERS[name] = locker

        @functools.wraps(method)
        async def wrapper(*args, **kwargs):
            if not wait and locker.locked():
                return
            try:
                await locker.acquire()
                return await method(*args, **kwargs)
            finally:
                locker.release()

        return wrapper

    return decorating_function


class EventCenter:
    """ Event center.
    """

    def __init__(self, config={}):
        self._host = config.get("host", "localhost")
        self._port = config.get("port", 5672)
        self._username = config.get("username", "guest")
        self._password = config.get("password", "guest")
        self._protocol = None
        self._channel = None  # Connection channel.
        self._connected = False  # If connect success.
        self._subscribers = []  # e.g. [(event, callback, multi), ...]
        self._event_handler = {}  # e.g. {"exchange:routing_key": [callback_function, ...]}

        # Register a loop run task to check TCP connection's healthy.
        LoopRunTask.register(self._check_connection, 10)

    def initialize(self):
        asyncio.get_event_loop().run_until_complete(self.connect())

    @async_method_locker("EventCenter.subscribe")
    async def subscribe(self, event: Event, callback=None, multi=False):
        """ Subscribe a event.

        Args:
            event: Event type.
            callback: Asynchronous callback.
            multi: If subscribe multiple channel(routing_key) ?
        """
        print("NAME:", event.name, "EXCHANGE:", event.exchange, "QUEUE:", event.queue, "ROUTING_KEY:",
              event.routing_key)
        self._subscribers.append((event, callback, multi))

    async def publish(self, event):
        """ Publish a event.

        Args:
            event: A event to publish.
        """
        if not self._connected:
            print("RabbitMQ not ready right now!")
            return
        data = event.dumps()
        # print(data, event.exchange, event.routing_key)
        await self._channel.basic_publish(payload=data, exchange_name=event.exchange, routing_key=event.routing_key)

    async def connect(self, reconnect=False):
        """ Connect to RabbitMQ server and create default exchange.

        Args:
            reconnect: If this invoke is a re-connection ?
        """
        print("host:", self._host, "port:", self._port)
        if self._connected:
            return

        # Create a connection.
        try:
            transport, protocol = await aioamqp.connect(host=self._host, port=self._port, login=self._username,
                                                        password=self._password, login_method="PLAIN")
        except Exception as e:
            print("connection error:", "{}:{}".format(e.__class__, e))
            return
        finally:
            if self._connected:
                return
        channel = await protocol.channel()
        self._protocol = protocol
        self._channel = channel
        self._connected = True
        print("Rabbitmq initialize success!")

        # Create default exchanges.
        # exchanges = ["Orderbook", "Trade", "Kline", "Kline.5min", "Kline.15min", ]
        exchanges = ["pendingTx"]
        for name in exchanges:
            await self._channel.exchange_declare(exchange_name=name, type_name="topic")
        print("create default exchanges success!")

        if reconnect:
            self._bind_and_consume()
        else:
            # Maybe we should waiting for all modules to be initialized successfully.
            asyncio.get_event_loop().call_later(5, self._bind_and_consume)

    def _bind_and_consume(self):
        async def do_them():
            for event, callback, multi in self._subscribers:
                await self._initialize(event, callback, multi)

        SingleTask.run(do_them)

    async def _initialize(self, event: Event, callback=None, multi=False):
        if event.queue:
            await self._channel.queue_declare(queue_name=event.queue, auto_delete=True)
            queue_name = event.queue
        else:
            result = await self._channel.queue_declare(exclusive=True)
            queue_name = result["queue"]
        await self._channel.queue_bind(queue_name=queue_name, exchange_name=event.exchange,
                                       routing_key=event.routing_key)
        await self._channel.basic_qos(prefetch_count=event.prefetch_count)
        if callback:
            if multi:
                await self._channel.basic_consume(callback=callback, queue_name=queue_name, no_ack=True)
                print("multi message queue:", queue_name)
            else:
                await self._channel.basic_consume(self._on_consume_event_msg, queue_name=queue_name)
                print("queue:", queue_name)
                self._add_event_handler(event, callback)

    async def _on_consume_event_msg(self, channel, body, envelope, properties):
        # print("exchange:", envelope.exchange_name, "routing_key:", envelope.routing_key,
        #              "body:", body)
        try:
            key = "{exchange}:{routing_key}".format(exchange=envelope.exchange_name, routing_key=envelope.routing_key)
            # print(key, self._event_handler, envelope, properties)
            funcs = self._event_handler[key]
            for func in funcs:
                SingleTask.run(func, channel, body, envelope, properties)
        except Exception as e:
            print("event handle error! body:", body, e.__class__.__name__, e)
            # print(traceback.format_exc())
            return
        finally:
            await self._channel.basic_client_ack(delivery_tag=envelope.delivery_tag)  # response ack

    def _add_event_handler(self, event: Event, callback):
        key = "{exchange}:{routing_key}".format(exchange=event.exchange, routing_key=event.routing_key)
        if key in self._event_handler:
            self._event_handler[key].append(callback)
        else:
            self._event_handler[key] = [callback]
        print("event handlers:", self._event_handler.keys())

    async def _check_connection(self, *args, **kwargs):
        if self._connected and self._channel and self._channel.is_open:
            print("RabbitMQ connection ok.")
            return
        print("CONNECTION LOSE! START RECONNECT RIGHT NOW!")
        self._connected = False
        self._protocol = None
        self._channel = None
        self._event_handler = {}
        SingleTask.run(self.connect, reconnect=True)

event_center = EventCenter()