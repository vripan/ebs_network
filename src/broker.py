from connection import EBSConnection
import asyncio
import ebs_msg_pb2
from globals import MANAGER_ENDPOINT

class Broker:

    def __init__(self):
        self._managerConnection = None
        self._NB = {}
        self._LB = {}
        self._ConnectionTable = {}
        self._SubscriptionTable = {}
        self._lock = asyncio.Lock()

    async def init(self):

        async with self._lock:
            self._managerConnection = await EBSConnection.connect(
                MANAGER_ENDPOINT['host'],
                MANAGER_ENDPOINT['port'],
            )
            message_connect = ebs_msg_pb2.Connect()
            message_connect.type = ebs_msg_pb2.Connect.SrcType.BROKER
            self._managerConnection.send(message_connect)
            # init with manager
            pass

    async def handle_client(self, ebs_connection: EBSConnection):
        data = await ebs_connection.read()
        if isinstance(data, ebs_msg_pb2.Connect):
            pass
        while True:
            data = await ebs_connection.read()
            async with self._lock:
                # process data
                pass

    @staticmethod
    async def handle_client_ext(reader, writer):
        ebs_connection = EBSConnection(reader=reader, writer=writer)
        await broker.handle_client(ebs_connection)

broker = None

async def app_broker():
    global broker
    broker = Broker()
    await broker.init()
    app_server = await asyncio.start_server(Broker.handle_client_ext)


if __name__ == '__main__':
    try:
        asyncio.run(app_broker(), debug=False)
    except Exception as e:
        print(e)
        print("Exiting...")
        pass

