import ebs_msg_pb2

from generator_data import DataGenerator

class PublicationGenerator:
    def __init__(self):
        self.idx = 0

    def get(self):
        object = ebs_msg_pb2.Publication()
        
        object.company = DataGenerator.company()
        object.value = DataGenerator.stock_value()
        object.drop = DataGenerator.drop_value()
        object.variation = DataGenerator.variation_value()
        object.date = DataGenerator.date()

        self.idx += 1

        return object
