import argparse
import asyncio
import datetime

import ebs_msg_pb2
import logging
from connection import EBSConnection
from globals import MANAGER_ENDPOINT
from generator_publication import PublicationGenerator
from logger import setup_logger
import time
import loop

node_config = {
    'id': 1,
    'pubs': 0,
    'time': 0
}


class Publisher:
    def __init__(self, node_id: int, pubs: int, time: int):
        self._managerConnection = None
        self._brokerConnection = None
        self._ID = node_id
        self._PUBS = pubs
        self._current_pubs = pubs
        self._lock = asyncio.Lock()
        self._gen = PublicationGenerator()
        self._time = time

    async def init(self):
        async with self._lock:
            self._managerConnection = await EBSConnection.connect(
                MANAGER_ENDPOINT['host'],
                MANAGER_ENDPOINT['port'],
            )
            message_connect = ebs_msg_pb2.Connect()
            message_connect.type = ebs_msg_pb2.Connect.SrcType.PUBLISHER
            message_connect.id = self._ID
            await self._managerConnection.write(message_connect)

            logging.info('Connected to manager.')

            message_reqister = ebs_msg_pb2.RequestBroker()
            message_reqister.id = self._ID
            await self._managerConnection.write(message_reqister)

            receive_brk = await self._managerConnection.read()

            if receive_brk.status != ebs_msg_pb2.ReceiveBroker.Status.SUCCESS:
                raise Exception(f'Could not recevie broker: {receive_brk}')

            logging.info('Got broker.')

            self._brokerConnection = await EBSConnection.connect(
                receive_brk.host,
                receive_brk.port,
            )
            message_connect = ebs_msg_pb2.Connect()
            message_connect.type = ebs_msg_pb2.Connect.SrcType.PUBLISHER
            message_connect.id = self._ID
            await self._brokerConnection.write(message_connect)

            logging.info('Connected to broker.')

    async def run(self):
        start = time.time()
        count = 0
        send_interval_seconds = 0.2
        while self._PUBS == 0 or self._current_pubs > 0:
            start_pub = time.time()
            self._current_pubs -= 1

            publication = self._gen.get()
            publication.source_id = self._ID
            publication.publication_id = (self._gen.idx + 1) * 10 + self._ID
            logging.info(f'Sending publication with company = {publication.company}, ' +
                        f'value = {publication.value}, ' +
                        f'drop = {publication.drop}, ' +
                        f'variation = {publication.variation}, ' +
                        f'date = {publication.date}, ')
            await self._brokerConnection.write(publication)
            count += 1
            logging.info(f'log_send_publication:{publication.publication_id};{datetime.datetime.now().timestamp()};')
            if self._time != 0 and time.time() - start > self._time * 60:
                logging.info("Sending time window expired")
                break
            to_sleep = send_interval_seconds - (time.time() - start_pub)
            if to_sleep > 0:
                await asyncio.sleep(to_sleep)
        logging.info(f"Sent {count} publications")
        logging.info("Exiting")


async def app_publisher():
    global publisher

    publisher = Publisher(node_config['id'], node_config['pubs'], node_config['time'])

    await publisher.init()

    await publisher.run()


if __name__ == '__main__':
    setup_logger()

    arg_parser = argparse.ArgumentParser(description='Publisher node.')
    arg_parser.add_argument('--id', type=int, required=True)
    arg_parser.add_argument('--pubs', type=int, default=0)
    arg_parser.add_argument('--time', type=int, default=0)
    args = arg_parser.parse_args()

    node_config['id'] = args.id
    node_config['pubs'] = args.pubs
    node_config['time'] = args.time

    try:
        asyncio.run(app_publisher(), debug=False)
    except KeyboardInterrupt:
        logging.info("Exit signal triggered by user...")
    except Exception as e:
        logging.exception(e)
        logging.fatal("Exception occured. Exiting...")
