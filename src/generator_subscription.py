from dataclasses import dataclass
import ebs_msg_pb2
import random

from generator_data import DataGenerator

@dataclass
class SubscriptionConfig:
    count: int
    company_probability: float
    company_equal_frequency: float
    value_probability: float
    drop_probability: float
    variation_probability: float
    date_probability: float

class SubscriptionGenerator:
    def __init__(self, config):
        self.idx = 0
        self.config = config
        
        self.equals_count = config.company_probability * config.company_equal_frequency * config.count
        self.equals_idx = 0
        self.condition_idx = 0
        self.conditions = [
            [self.__company_condition, config.company_probability * config.count],
            [self.__stock_value_condition, config.value_probability * config.count],
            [self.__drop_value_condition, config.drop_probability * config.count],
            [self.__variation_value_condition, config.variation_probability * config.count],
            [self.__date_condition, config.date_probability * config.count]
        ]
        self.total_conditions = sum(list(map(lambda t: t[1], self.conditions)))
        self.conditions_per_sub = int(self.total_conditions // config.count)
        assert self.total_conditions >= config.count

    def __get_condition(self):
        found_idx = None
        for idx in range(len(self.conditions)):
            temp_idx = (self.condition_idx + idx) % len(self.conditions)
            if self.conditions[temp_idx][1] > 0:
                found_idx = temp_idx
                break

        if found_idx is None:
            return None

        self.condition_idx = (found_idx + 1) % len(self.conditions)
        condition_info = self.conditions[found_idx]
        condition_info[1] -= 1

        return condition_info[0]()

    def __number_operator(self):
        return random.choice([ebs_msg_pb2.Condition.Operator.LT,
                        ebs_msg_pb2.Condition.Operator.LE,
                        ebs_msg_pb2.Condition.Operator.EQ,
                        ebs_msg_pb2.Condition.Operator.GE,
                        ebs_msg_pb2.Condition.Operator.GT])

    def __string_operator(self):
        if self.equals_idx < self.equals_count:
            self.equals_idx += 1
            return ebs_msg_pb2.Condition.Operator.EQ
        return random.choice([ebs_msg_pb2.Condition.Operator.EQ,
                      ebs_msg_pb2.Condition.Operator.NE])


    def __company_condition(self):
        object = ebs_msg_pb2.Condition()

        object.field = "company"
        object.Op = self.__string_operator()
        object.value =  DataGenerator.company()

        return object

    def __stock_value_condition(self):
        object = ebs_msg_pb2.Condition()

        object.field = "value"
        object.Op = self.__number_operator()
        object.value =  str(DataGenerator.stock_value())

        return object

    def __drop_value_condition(self):
        object = ebs_msg_pb2.Condition()

        object.field = "drop"
        object.Op = self.__number_operator()
        object.value =  str(DataGenerator.drop_value())

        return object

    def __variation_value_condition(self):
        object = ebs_msg_pb2.Condition()

        object.field = "variation"
        object.Op = self.__number_operator()
        object.value =  str(DataGenerator.variation_value())

        return object

    def __date_condition(self):
        object = ebs_msg_pb2.Condition()

        object.field = "date"
        object.Op = self.__number_operator()
        object.value =  DataGenerator.date()

        return object

    def get(self):
        object = ebs_msg_pb2.Subscription()
        
        for _ in range(self.conditions_per_sub):
            object.condition.append(self.__get_condition())

        self.idx += 1

        return object
