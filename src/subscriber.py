import argparse
import logging
from connection import EBSConnection, EBSConnectionError, NetworkEndpoint
import asyncio
import ebs_msg_pb2
from globals import MANAGER_ENDPOINT
from generator_subscription import SubscriptionGenerator, SubscriptionConfig
import utils
import datetime
from logger import setup_logger
import loop 

node_config = {
    'id': 1,
    'subs_count': 1000
}

subscriber = None


class Subscriber:
    def __init__(self, node_id):
        self._ID = node_id
        self._managerConnection = None
        self._brokerData = {}
        self._brokerConnection = None

    async def init(self):
        pass

    async def _handle_pub(self, pub: ebs_msg_pb2.Publication):

        logging.info('Publication received: [{}]'.format(utils.get_str_publication(pub)))
        logging.info(f'log_recv_publication:{pub.publication_id};{datetime.datetime.now().timestamp()};')

    async def _connect_to_manager(self):
        try:
            logging.info('Connecting to manager...')
            self._managerConnection = await EBSConnection.connect(
                MANAGER_ENDPOINT['host'],
                MANAGER_ENDPOINT['port']
            )
            connect_msg = ebs_msg_pb2.Connect()
            connect_msg.type = ebs_msg_pb2.Connect.SrcType.SUBSCRIBER
            connect_msg.id = self._ID

            await self._managerConnection.write(connect_msg)

            logging.info('Connected to manager.')

            # get a broker

            request_broker_msg = ebs_msg_pb2.RequestBroker()
            request_broker_msg.id = self._ID

            await self._managerConnection.write(request_broker_msg)

            logging.info('Requested broker from manager.')

            receive_broker = await self._managerConnection.read()
            assert isinstance(receive_broker, ebs_msg_pb2.ReceiveBroker)

            if receive_broker.status != ebs_msg_pb2.ReceiveBroker.Status.SUCCESS:
                logging.error('Failed to get broker!')
                raise Exception('Failed to get broker!')

            self._brokerData['id'] = receive_broker.id
            self._brokerData['host'] = receive_broker.host
            self._brokerData['port'] = receive_broker.port

            logging.info('Received broker [id={}, host={}, port={}]'.format(self._brokerData['id'], self._brokerData['host'], self._brokerData['port']))
        except:
            logging.error('Connection to manager failed!')
            raise Exception('Connection to manager failed!')

    async def _connect_to_broker(self):
        logging.info('Connecting to broker...')
        try:
            self._brokerConnection = await EBSConnection.connect(
                self._brokerData['host'],
                self._brokerData['port']
            )

            connect_msg = ebs_msg_pb2.Connect()
            connect_msg.type = ebs_msg_pb2.Connect.SrcType.SUBSCRIBER
            connect_msg.id = self._ID

            await self._brokerConnection.write(connect_msg)

            logging.info('Connected to broker.')
        except:
            logging.error('Connection to broker failed!')
            raise

    async def _send_subscriptions(self):
        sub_generator_config = SubscriptionConfig(
            count=node_config['subs_count'],
            company_probability=1.0,
            company_equal_frequency=0.25,
            value_probability=1,
            drop_probability=1,
            variation_probability=0.1,
            date_probability=0.1
        )

        sub_generator = SubscriptionGenerator(config=sub_generator_config)

        try:
            for idx in range(sub_generator_config.count):
                sub = sub_generator.get()
                sub.subscriber_id = self._ID
                sub.subscription_id = (idx + 1) * 10 + self._ID
                await self._brokerConnection.write(sub)
                logging.info('Sent Subscription: [{}] [subscription_id: {}]'.format(utils.get_str_subscription(sub), sub.subscription_id))
        except:
            logging.error('Failed sending subscriptions!')
            raise

    async def _wait_publications(self):
        try:
            while True:
                pub = await self._brokerConnection.read()
                assert isinstance(pub, ebs_msg_pb2.Publication)
                await self._handle_pub(pub)
        except:
            logging.error('Connection with broker lost!')
            raise

    async def run(self):
        try:
            await self._connect_to_manager()
            await self._connect_to_broker()
            await self._send_subscriptions()
            await self._wait_publications()
        except:
            raise


async def app_subscriber():
    global subscriber
    subscriber = Subscriber(node_id=node_config['id'])
    await subscriber.run()


if __name__ == '__main__':
    setup_logger()

    arg_parser = argparse.ArgumentParser(description='Subscriber node.')
    arg_parser.add_argument('--id', type=int, required=True)
    arg_parser.add_argument('--subs_count', type=int, default=1000)
    args = arg_parser.parse_args()

    node_config['id'] = args.id
    node_config['subs_count'] = args.subs_count

    try:
        asyncio.run(app_subscriber())
    except KeyboardInterrupt:
        logging.info("Exit signal triggered by user...")
    except Exception as e:
        logging.exception(e)
        logging.fatal("Exception occured. Exiting...")
