import socket
import asyncio
import ebs_msg_pb2
import struct
from google.protobuf.any_pb2 import Any


class EBSConnectionError(Exception):
    """ EBS Connection error. """
    def __init__(self, *args, **kwargs):
        pass


class EBSConnection:
    _known_classes = (
        (ebs_msg_pb2.Connect.DESCRIPTOR, ebs_msg_pb2.Connect),
        (ebs_msg_pb2.BrokerRegister.DESCRIPTOR, ebs_msg_pb2.BrokerRegister),
        (ebs_msg_pb2.RequestBroker.DESCRIPTOR, ebs_msg_pb2.RequestBroker),
        (ebs_msg_pb2.ReceiveBroker.DESCRIPTOR, ebs_msg_pb2.ReceiveBroker),
        (ebs_msg_pb2.Subscription.DESCRIPTOR, ebs_msg_pb2.Subscription),
        (ebs_msg_pb2.Publication.DESCRIPTOR, ebs_msg_pb2.Publication),
        (ebs_msg_pb2.Unsubscribe.DESCRIPTOR, ebs_msg_pb2.Unsubscribe)
    )

    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

    @staticmethod
    async def connect(host, port):
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            connection = EBSConnection(reader=reader, writer=writer)
            return connection
        except Exception as e:
            raise EBSConnectionError('Connection failed!')

    async def write(self, data):
        try:
            message = Any()
            message.Pack(data)
            message = message.SerializeToString()
            message_size = struct.pack('<I', len(message))
            self._writer.write(message_size + message)
            await self._writer.drain()  # Wait until it is appropriate to resume writing to the stream
        except:
            # raise EBSConnectionError
            raise EBSConnectionError('Write failed!')

    async def read(self):
        try:
            # read size
            message_size_raw = await self._reader.readexactly(n=4)
            message_size = struct.unpack('<I', message_size_raw)[0]
            # read serialized message of given size
            message_raw = await self._reader.readexactly(n=message_size)
            message = Any().FromString(message_raw)
            msg_object = message

            for cls_desc, cls_type in self._known_classes:
                if message.Is(descriptor=cls_desc):
                    msg_object = cls_type()
                    msg_object.ParseFromString(message.value)
                    break

            return msg_object
        # except asyncio.IncompleteReadError:
        except:
            raise EBSConnectionError('Read failed!')


class NetworkEndpoint:
    def __init__(self, node_id: int, host: str = None, port: int = None):
        self._id = node_id
        self._host = host
        self._port = port
