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
    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer

    @staticmethod
    async def connect(host, port, src_type: ebs_msg_pb2.Connect.SrcType = ebs_msg_pb2.Connect.SrcType.UNKNOWN):
        try:
            reader, writer = await asyncio.open_connection(host=host, port=port)
            connection = EBSConnection(reader=reader, writer=writer)
            if src_type != ebs_msg_pb2.Connect.Type.UNKNOWN:
                connect_msg = ebs_msg_pb2.Connect()
                connect_msg.type = ebs_msg_pb2.Connect.Type.BROKER
                await connection.write(connect_msg)
        except:
            raise EBSConnectionError

    async def write(self, data):
        try:
            message = data.SerializeToString()
            message_size = struct.pack('<I', len(message))
            self._writer.write(message_size + message)
            self._writer.drain()  # Wait until it is appropriate to resume writing to the stream
        except:
            raise EBSConnectionError

    async def read(self):
        try:
            # read size
            message_size_raw = await self._reader.readexactly(n=4)
            message_size = struct.unpack('<I', message_size_raw)[0]
            # read serialized message of given size
            message_raw = await self._reader.readexactly(n=message_size)
            message = Any.FromString(message_raw)
            return message
        # except asyncio.IncompleteReadError:
        except:
            raise EBSConnectionError


class NetworkEndpoint:
    def __init__(self, uuid, connection: EBSConnection):
        self.id = uuid
        self.conn = connection
