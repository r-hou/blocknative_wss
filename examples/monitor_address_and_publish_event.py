from blocknative_wss.blocknative import BlockNativeSubscriber
from blocknative_wss.blocknative import heartbeat
from blocknative_wss.publisher import EventPendingTx
from blocknative_wss.publisher import event_center
import asyncio


async def txn_handler(txn, unsubscribe):
    if txn['status'] == "pending":
        print(txn)
        EventPendingTx(data=txn, server_id="mycomputer", routing_key="gold.common",
                       event_center=event_center).publish()


event_center.initialize()
bns = BlockNativeSubscriber(api_key="93062b7e-7b5a-4422-b907-92587df9116c", network_id=4)
address = "0xE3754298f3258165ea32B81977A09d64832223FB"  # address to be monitored
filters = [{
    'from': address
}]
bns.subscribe_address(address, txn_handler, filters)
bns.run()
loop = asyncio.get_event_loop()
loop.call_later(2, heartbeat.ticker)
loop.run_forever()
