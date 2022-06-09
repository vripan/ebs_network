import logging
from logger import setup_logger

from generator_publication import PublicationGenerator
from generator_subscription import SubscriptionGenerator, SubscriptionConfig


def main():
    setup_logger(".run", logging.DEBUG)

    publication_generator = PublicationGenerator()
    publication = publication_generator.get()

    logging.info("publication generated:\n%s" % publication)

    logging.info("-" * 20)

    generator_config = SubscriptionConfig(
        count=10,
        company_probability=1,
        company_equal_frequency=1,
        value_probability=1,
        drop_probability=1,
        variation_probability=0.1,
        date_probability=0.1
    )
    subscription_generator = SubscriptionGenerator(generator_config)

    for _ in range(generator_config.count):
        logging.info("subscription generated: \n%s" % subscription_generator.get())

if __name__ == "__main__":
    main()