from blocknative_wss.publisher import EventPendingTx
from blocknative_wss.publisher import event_center
import asyncio


class PendingTxSubscriber:
    def __init__(self, callback):
        EventPendingTx(server_id="mycomputer", routing_key="#.common.#", event_center=event_center).subscribe(callback,
                                                                                                              multi=True)


class PendingTxHandler:

    def __init__(self):
        PendingTxSubscriber(self.handle_tx)

    async def handle_tx(self, tx):
        print(tx)



event_center.initialize()
PendingTxHandler()
asyncio.get_event_loop().run_forever()
