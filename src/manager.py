from connection import EBSConnection, EBSConnectionError
from globals import MANAGER_ENDPOINT
import asyncio
import ebs_msg_pb2
import logging
from logger import setup_logger
import utils
import loop

class Manager:

    def __init__(self):
        self._brokers = set()
        self._subscribers = set()
        self._publishers = set()
        self._connections = []

        self._brokersRegister = dict()

        self._lock = asyncio.Lock()

        self._servedBrokerIndex = 0

    async def init(self):
        pass

    async def _handle_register_broker(self, ebs_connection: EBSConnection, msg: ebs_msg_pb2.BrokerRegister):
        self._brokersRegister[msg.id] = {
            'host': msg.host,
            'port': msg.port
        }
        logging.info('Broker registered! [id:{}, host:{}, port:{}]'.format(msg.id, msg.host, msg.port))

    async def _handle_request_broker(self, ebs_connection: EBSConnection, msg: ebs_msg_pb2.RequestBroker):
        logging.info('Received request broker message from id: {}'.format(msg.id))
        fw_msg = ebs_msg_pb2.ReceiveBroker()
        if len(self._brokers) == 0:
            logging.info('No brokers available!')
            fw_msg.status = ebs_msg_pb2.ReceiveBroker.Status.FAILED_NO_BROKER_AVAILABLE
        else:
            # get next broker
            self._servedBrokerIndex = (self._servedBrokerIndex + 1) % len(self._brokers)
            brokerId = list(self._brokers)[self._servedBrokerIndex]

            # this should never happen
            assert brokerId in self._brokersRegister.keys(), "Broker not registered!"

            fw_msg.status = ebs_msg_pb2.ReceiveBroker.Status.SUCCESS
            fw_msg.id = brokerId
            fw_msg.host = self._brokersRegister[brokerId]['host']
            fw_msg.port = self._brokersRegister[brokerId]['port']

        await ebs_connection.write(fw_msg)

    async def _handle_message(self, ebs_connection: EBSConnection, msg):
        if isinstance(msg, ebs_msg_pb2.BrokerRegister):
            await self._handle_register_broker(ebs_connection, msg)
        elif isinstance(msg, ebs_msg_pb2.RequestBroker):
            await self._handle_request_broker(ebs_connection, msg)
        else:
            raise Exception('Received invalid message!')
            # assert False, 'Received invalid message!'

    async def _handle_connect(self, ebs_connection: EBSConnection, conn_msg: ebs_msg_pb2.Connect):
        logging.info('{} connected with id: {}'.format(utils.get_str_connect_str_type(conn_msg.type), conn_msg.id))
        if conn_msg.type == ebs_msg_pb2.Connect.SrcType.BROKER:
            self._brokers.add(conn_msg.id)
        elif conn_msg.type == ebs_msg_pb2.Connect.SrcType.SUBSCRIBER:
            self._subscribers.add(conn_msg.id)
        elif conn_msg.type == ebs_msg_pb2.Connect.SrcType.PUBLISHER:
            self._publishers.add(conn_msg.id)
        else:
            raise Exception('Invalid source type!')

    async def handle_client(self, ebs_connection: EBSConnection):
        cl_id, cl_type = None, None
        try:
            data = await ebs_connection.read()
            async with self._lock:
                if isinstance(data, ebs_msg_pb2.Connect):
                    await self._handle_connect(ebs_connection, data)
                    cl_id, cl_type = data.id, data.type
                else:
                    raise Exception('Invalid first message!')
            while True:
                data = await ebs_connection.read()
                async with self._lock:
                    await self._handle_message(ebs_connection, data)
        except EBSConnectionError:
            if cl_id is not None and cl_type is not None:
                logging.info('{} with id={} disconnected!'.format(utils.get_str_connect_str_type(cl_type), cl_id))
            else:
                logging.info('Client disconnected!')
        except Exception as e:
            logging.exception(e)
            logging.error('Disconecting...')

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
        host=MANAGER_ENDPOINT["host"],
        port=MANAGER_ENDPOINT['port']
    )
    logging.info("Manager node started at %s:%u" % (MANAGER_ENDPOINT["host"], MANAGER_ENDPOINT["port"]))
    async with app_server:
        await app_server.serve_forever()


if __name__ == '__main__':
    setup_logger()
    try:
        asyncio.run(app_manager(), debug=False)
    except KeyboardInterrupt:
        logging.info("Exit signal triggered by user...")
    except Exception as e:
        logging.exception(e)
        logging.fatal("Exception occured. Exiting...")
