<a href="https://pypi.org/project/blocknative-sdk/">
    <img src="https://img.shields.io/pypi/v/blocknative-sdk" />
</a>

# Blocknative Python SDK



## API Key

To get started using the Blocknative Python SDK you must first obtain an API Key. You can do so by heading over to [Blocknative.com](https://explorer.blocknative.com/account)!

## Usage
 
This package is almost the same as the [blocknative-sdk](https://github.com/blocknative/python-sdk), please refer their repo for more usages. The trio package is excluded in this project and is replaced with aiohttp. 

The most import part of this package differs from blocknative-sdk is that this project is able to publish the transaction to rabbitmq so all your clients can subscribes and receive the transaction events at a time. To use this interface, you have to intall and start rabbitmq server before run any code. You can use [multiple routing key](http://whitfin.io/multiple-routing-keys-in-rabbitmq-exchanges/) to scatter data to multiple clients.

### Basic usage

```python
from blocknative_wss.blocknative import BlockNativeSubscriber
from blocknative_wss.blocknative import heartbeat
import asyncio


async def txn_handler(txn, unsubscribe):
    if txn['status'] == "pending":
        print(txn)



bns = BlockNativeSubscriber(api_key="", network_id=4)
address = ""  # address to be monitored
filters = [{
    'from': address
}]
bns.subscribe_address(address, txn_handler, filters)
bns.run()
loop = asyncio.get_event_loop()
loop.call_later(2, heartbeat.ticker)
loop.run_forever()

```

### Publish and sucscribe the pending tx event

####publish the event
```python
#listen from mem pool and publish the event to rabbitmq
from blocknative_wss.blocknative import BlockNativeSubscriber
from blocknative_wss.blocknative import heartbeat
import asyncio


async def txn_handler(txn, unsubscribe):
    if txn['status'] == "pending":
        print(txn)
        EventPendingTx(data=txn, server_id="mycomputer", routing_key="gold.common",
                       event_center=event_center).publish()



bns = BlockNativeSubscriber(api_key="", network_id=4)
address = ""  # address to be monitored
filters = [{
    'from': address
}]
bns.subscribe_address(address, txn_handler, filters)
bns.run()
loop = asyncio.get_event_loop()
loop.call_later(2, heartbeat.ticker)
loop.run_forever()
```


####client listen to the event
```python
#the first listener 
from blocknative_wss.publisher import EventPendingTx
from blocknative_wss.publisher import event_center
import asyncio


class PendingTxSubscriber:
    def __init__(self, callback):
        EventPendingTx(server_id="mycomputer", routing_key="#.gold.#", event_center=event_center).subscribe(callback,
                                                                                                              multi=True)


class PendingTxHandler:

    def __init__(self):
        PendingTxSubscriber(self.handle_tx)

    async def handle_tx(self, tx):
        print(tx)

event_center.initialize()
PendingTxHandler()
asyncio.get_event_loop().run_forever()
```

```python
#the second listener 
from blocknative_wss.publisher import EventPendingTx
from blocknative_wss.publisher import event_center
import asyncio


class PendingTxSubscriber:
    def __init__(self, callback):
        EventPendingTx(server_id="mycomputer", routing_key="#.gold.#", event_center=event_center).subscribe(callback,
                                                                                                              multi=True)


class PendingTxHandler:

    def __init__(self):
        PendingTxSubscriber(self.handle_tx)

    async def handle_tx(self, tx):
        print(tx)

event_center.initialize()
PendingTxHandler()
asyncio.get_event_loop().run_forever()
```