from connection import EBSConnection
from globals import MANAGER_ENDPOINT
import asyncio
import ebs_msg_pb2


class Manager:

    def __init__(self):
        self._brokers = set()
        self._connections = set()

        self._lock = asyncio.Lock()

    async def init(self):
        pass

    async def handle_client(self, ebs_connection: EBSConnection):
        data = await ebs_connection.read()
        async with self._lock:
            if isinstance(data, ebs_msg_pb2.Connect):
                if data.type == ebs_msg_pb2.Connect.SrcType.BROKER:
                    self._brokers.add(data.id)

        while True:
            data = await ebs_connection.read()
            async with self._lock:
                # process data
                pass

    @staticmethod
    async def handle_client_ext(reader, writer):
        ebs_connection = EBSConnection(reader=reader, writer=writer)
        await manager.handle_client(ebs_connection)


manager = Manager()


async def app_manager():
    global manager
    manager = Manager()
    await manager.init()
    app_server = await asyncio.start_server(
        client_connected_cb=Manager.handle_client_ext,
        host="localhost",
        port=MANAGER_ENDPOINT['port']
    )
    async with app_server:
        await app_server.serve_forever()


if __name__ == '__main__':
    try:
        asyncio.run(app_manager(), debug=False)
    except Exception as e:
        print(e)
        print("Exiting...")
