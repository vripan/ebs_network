import argparse
import logging
from connection import EBSConnection, EBSConnectionError, NetworkEndpoint
import asyncio
import ebs_msg_pb2
from globals import MANAGER_ENDPOINT

logging.basicConfig()

logger = logging.getLogger("BrokerLog")
logger.setLevel(logging.DEBUG)

node_config = {
    'id': 1,
    'host': 'localhost',
    'port': 8081,
    'neighbours': ((2, 'localhost', 8082), (3, 'localhost', 8083))
}

broker = None


class SubWrapper:
    def __init__(self, sub: ebs_msg_pb2.Subscription):
       pass


class BrokerConnData(NetworkEndpoint):
    def __init__(self, node_id: int, host: str, port: int, incoming: EBSConnection = None, outgoing: EBSConnection = None):
        super().__init__(node_id, host, port)
        self._incoming = incoming
        self._outgoing = outgoing

    def set(self, incoming: EBSConnection = None, outgoing: EBSConnection = None):
        if incoming is not None:
            self._incoming = incoming
        if outgoing is not None:
            self._outgoing = outgoing

    async def send(self, self_id, msg):
        try:
            if self._outgoing is None:
                self._outgoing = await EBSConnection.connect(self._host, self._port)
                connect_msg = ebs_msg_pb2.Connect()
                connect_msg.type = ebs_msg_pb2.Connect.SrcType.BROKER
                connect_msg.id = self_id
                await self._outgoing.write(connect_msg)
            await self._outgoing.write(msg)
        except EBSConnectionError as e:
            logger.error('Connection to broker with id = {} failed!'.format(self._id))


class Broker:
    def __init__(self, node_id: int, host: str, port: int, neighbours):
        self._managerConnection = None
        self._ID = node_id
        self._HOST = host
        self._PORT = port
        self._NB = set()
        self._LB = set()
        self._NBConnectionTable = {}
        self._LBConnectionTable = {}
        self._SubscriptionTable = []
        self._lock = asyncio.Lock()

        # init stuff
        for _id, _host, _port in neighbours:
            self._NB.add(_id)
            self._NBConnectionTable[_id] = BrokerConnData(
                node_id=_id,
                host=_host,
                port=_port
            )

    async def init(self):
        async with self._lock:
            self._managerConnection = await EBSConnection.connect(
                MANAGER_ENDPOINT['host'],
                MANAGER_ENDPOINT['port'],
            )
            message_connect = ebs_msg_pb2.Connect()
            message_connect.type = ebs_msg_pb2.Connect.SrcType.BROKER
            message_connect.id = self._ID
            await self._managerConnection.write(message_connect)

            logger.info('Connected to manager.')

            message_reqister = ebs_msg_pb2.BrokerRegister()
            message_reqister.id = self._ID
            message_reqister.host = self._HOST
            message_reqister.port = self._PORT
            await self._managerConnection.write(message_reqister)

            logger.info('Registered to manager.')

    async def handle_connect(self, connection: EBSConnection, msg_connect: ebs_msg_pb2.Connect):
        logger.info('Client connected with id: {}'.format(msg_connect.id))

        if msg_connect.type == ebs_msg_pb2.Connect.SrcType.BROKER:
            self._NBConnectionTable[msg_connect.id].set(incoming=connection)
        elif msg_connect.type == ebs_msg_pb2.Connect.SrcType.SUBSCRIBER:
            self._LB.add(msg_connect.id)
            self._LBConnectionTable[msg_connect.id] = {
                'connection': connection
            }
        elif msg_connect.type == ebs_msg_pb2.Connect.SrcType.PUBLISHER:
            self._LB.add(msg_connect.id)
            self._LBConnectionTable[msg_connect.id] = {
                'connection': connection
            }
        else:
            assert False

    @staticmethod
    def _match_single_cond(cond: ebs_msg_pb2.Condition, pub: ebs_msg_pb2.Publication):
        cond_value_str = cond.value
        pub_value = getattr(pub, cond.field)
        pub_value_type = type(pub_value)
        cond_value = pub_value_type(cond_value_str)
        try:
            if cond.op == ebs_msg_pb2.Condition.Operator.EQ:
                return cond_value == pub_value
            if cond.op == ebs_msg_pb2.Condition.Operator.NE:
                return cond_value != pub_value
            if cond.op == ebs_msg_pb2.Condition.Operator.GT:
                return cond_value > pub_value
            if cond.op == ebs_msg_pb2.Condition.Operator.GE:
                return cond_value >= pub_value
            if cond.op == ebs_msg_pb2.Condition.Operator.LT:
                return cond_value < pub_value
            if cond.op == ebs_msg_pb2.Condition.Operator.LE:
                return cond_value < pub_value
        except Exception as e:
            logger.error('Failed comparing single cond!')
            raise e

    @staticmethod
    def _match_single_sub(sub: ebs_msg_pb2.Subscription, pub: ebs_msg_pb2.Publication):
        rez = True
        for cond in sub.condition:
            rez &= Broker._match_single_cond(cond, pub)
            if not rez:
                break
        return rez

    def _match_pub(self, pub: ebs_msg_pb2.Publication) -> set:
        matching_nodes = set()
        for sub, sub_id in self._SubscriptionTable:
            rez = Broker._match_single_sub(sub, pub)
            if rez:
                matching_nodes |= {sub.subscriber_id}
        return matching_nodes

    async def _handle_subscription(self, sub: ebs_msg_pb2.Subscription):
        logger.info('Received subscription from id: {}'.format(sub.subscriber_id))
        self._SubscriptionTable.append((sub, sub.subscriber_id))
        fw_sub = ebs_msg_pb2.Subscription()
        fw_sub.CopyFrom(sub)
        fw_sub.subscriber_id = self._ID
        # tmp: send to all NB
        for broker_id in (self._NB - {sub.subscriber_id}):
            await self._NBConnectionTable[broker_id].send(self._ID, fw_sub)

    async def _handle_publication(self, pub: ebs_msg_pb2.Publication):
        logger.info('Received publication from id: {}'.format(pub.source_id))
        # handle pub
        matching_nodes = self._match_pub(pub)
        fw_pub = ebs_msg_pb2.Publication()
        fw_pub.CopyFrom(pub)
        fw_pub.source_id = self._ID

        try:
            for node in ((matching_nodes - {self._ID}) & self._NB):
                await self._NBConnectionTable[node].send(self._ID, fw_pub)
            for node in matching_nodes & self._LB:
                if self._LBConnectionTable[node]['connection'] is not None:
                    await self._LBConnectionTable[node]['connection'].write(fw_pub)
                else:
                    logger.error('No connection from subscriber with id = {}!'.format(node))
        except:
            logger.error('Failed forwarding publication!')

    async def _handle_unsubscribe(self, unsub: ebs_msg_pb2.Unsubscribe):
        # TODO
        pass

    async def _handle_message(self, message):
        if isinstance(message, ebs_msg_pb2.Subscription):
            await self._handle_subscription(message)
        elif isinstance(message, ebs_msg_pb2.Publication):
            await self._handle_publication(message)
        elif isinstance(message, ebs_msg_pb2.Unsubscribe):
            await self._handle_unsubscribe(message)
        else:
            assert False

    async def handle_client(self, ebs_connection: EBSConnection):
        try:
            data = await ebs_connection.read()
            if isinstance(data, ebs_msg_pb2.Connect):
                await self.handle_connect(ebs_connection, data)
            while True:
                data = await ebs_connection.read()
                async with self._lock:
                    # process data
                    await self._handle_message(data)
        except:
            logger.info('Connection disconnected!')
            raise
    @staticmethod
    async def handle_client_ext(reader, writer):
        ebs_connection = EBSConnection(reader=reader, writer=writer)
        await broker.handle_client(ebs_connection)


async def app_broker():
    global broker

    broker = Broker(node_config['id'], node_config['host'], node_config['port'], node_config['neighbours'])

    await broker.init()

    app_server = await asyncio.start_server(
        client_connected_cb=Broker.handle_client_ext,
        host=node_config['host'],
        port=node_config['port']
    )

    async with app_server:
        await app_server.serve_forever()


def neighbours_type(s):
    try:
        _id, _host, _port = map(str, s.split(','))
        return int(_id), str(_host), int(_port)
    except:
        raise argparse.ArgumentTypeError("Neighbours must be _id,_host,_port")


if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser(description='Broker node.')
    arg_parser.add_argument('--id', type=int, required=True)
    arg_parser.add_argument('--host', type=str, default='localhost')
    arg_parser.add_argument('--port', type=int, required=True)
    arg_parser.add_argument('--neighbours', type=neighbours_type, nargs='+', required=True)
    args = arg_parser.parse_args()

    node_config['id'] = args.id
    node_config['host'] = args.host
    node_config['port'] = args.port
    node_config['neighbours'] = set(args.neighbours)

    try:
        asyncio.run(app_broker(), debug=False)
    except Exception as e:
        logger.error(e)
        logger.info("Exiting...")

