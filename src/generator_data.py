import random
from datetime import datetime
from dateutil.relativedelta import relativedelta


class DataGenerator:
    @staticmethod
    def company():
        return random.choice(["Amazon",
                              "Apple", 
                              "Bitdefender", 
                              "Walmart", 
                              "ExxonMobil", 
                              "Aplhabet", 
                              "McKesson", 
                              "Microsoft", 
                              "Chevron", 
                              "Netflix"])

    @staticmethod
    def stock_value():
        return round(random.uniform(1, 500000), 2)

    @staticmethod
    def drop_value():
        return round(random.uniform(1, 500000), 2)

    @staticmethod
    def variation_value():
        return round(random.random(), 2)
    
    @staticmethod
    def date(delta_years=5):
        end = datetime.now()
        start = end - relativedelta(years=delta_years)
        random_date = start + (end - start) * random.uniform(0, 1)
        return random_date.strftime("%d.%m.%Y")
