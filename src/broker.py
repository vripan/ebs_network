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
    'neighbours': ((1, 'localhost', 8082), (2, 'localhost', 8083))
}


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

    async def send(self, msg):
        try:
            if self._outgoing is None:
                self._outgoing = await EBSConnection.connect(self._host, self._port)
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
        self._SubscriptionTable = set()
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
        logger.info('Connected to broker.')

    async def handle_connect(self, connection: EBSConnection, msg_connect: ebs_msg_pb2.Connect):
        if msg_connect.type == ebs_msg_pb2.Connect.SrcType.BROKER:
            self._NBConnectionTable[msg_connect.id]['connections'].set(incoming=connection)
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
        logger.info('Client connected with id: {}'.format(msg_connect.id))

    @staticmethod
    def _match_single_cond(cond: ebs_msg_pb2.Condition, pub: ebs_msg_pb2.Publication):
        cond_field_str = cond.field
        pub_field = getattr(pub, cond.field)
        pub_field_type = type(pub_field)
        cond_field = pub_field_type(cond_field_str)
        try:
            if cond.op == ebs_msg_pb2.Condition.Operator.EQ:
                return cond_field == pub_field
            if cond.op == ebs_msg_pb2.Condition.Operator.NE:
                return cond_field != pub_field
            if cond.op == ebs_msg_pb2.Condition.Operator.GT:
                return cond_field > pub_field
            if cond.op == ebs_msg_pb2.Condition.Operator.GE:
                return cond_field >= pub_field
            if cond.op == ebs_msg_pb2.Condition.Operator.LT:
                return cond_field < pub_field
            if cond.op == ebs_msg_pb2.Condition.Operator.LE:
                return cond_field < pub_field
        except Exception as e:
            logger.error('Failed comparing single cond!')
            raise e

    @staticmethod
    def _match_single_sub(sub: ebs_msg_pb2.Subscription, pub: ebs_msg_pb2.Publication):
        rez = True
        for cond in sub.conditions:
            rez |= Broker._match_single_cond(cond, pub)
            if not rez:
                break
        return rez

    def _match_pub(self, pub: ebs_msg_pb2.Publication) -> set:
        matching_nodes = set()
        for sub, sub_id in self._SubscriptionTable:
            rez = Broker._match_single_sub(sub, pub)
            if rez:
                matching_nodes |= {sub}
        return matching_nodes

    async def _handle_subscription(self, sub: ebs_msg_pb2.Subscription):
        self._SubscriptionTable.add(sub, sub.id)
        fw_sub = ebs_msg_pb2.Subscription().CopyFrom(sub)
        fw_sub.source_id = self._ID
        # tmp: send to all NB
        for broker_conn in self._NBConnectionTable.values():
            await broker_conn.send(fw_sub)

    async def _handle_publication(self, pub: ebs_msg_pb2.Publication):
        # handle pub
        matching_nodes = self._match_pub(pub)
        fw_pub = ebs_msg_pb2.Publication().CopyFrom(pub)
        fw_pub.source_id = self._ID

        try:
            for node in ((matching_nodes - {self._ID}) & self._NB):
                await self._NBConnectionTable[node].send(fw_pub)
            for node in matching_nodes & self._LB:
                if self._LBConnectionTable[node]['connection'] is not None:
                    self._LBConnectionTable[node]['connection'].write(fw_pub)
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
        data = await ebs_connection.read()
        if isinstance(data, ebs_msg_pb2.Connect):
            await self.handle_connect(ebs_connection, data)
        while True:
            data = await ebs_connection.read()
            async with self._lock:
                # process data
                await self._handle_message(data)

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
        print("Exiting...")

