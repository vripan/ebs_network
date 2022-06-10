import re
import datetime

with open('publisher.log') as pf:
    publisher_log = pf.read()

with open('subscriber.log') as pf:
    subscriber_log = pf.read()


pub_start = re.findall(r'log_send_publication:\d+;\d+\.?\d+?;', publisher_log)
pub_end = re.findall(r'log_recv_publication:\d+;\d+\.?\d+?;', subscriber_log)


def get_timestamp(string: str):
    return re.search(r'\d+;(\d+(\.\d+)?);', string).group(1)


def get_difference(start_time, end_time):
    time1 = datetime.datetime.fromtimestamp(float(start_time))
    time2 = datetime.datetime.fromtimestamp(float(end_time))
    time_difference = time2 - time1
    return time_difference


timedeltas = []
for reached in pub_end:
    id = int(re.search(r'\d+', reached).group())
    start = [x for x in pub_start if id == int(re.search(r'\d+', x).group())]
    if len(start) == 0:
        print(f'Not found publication {id}')
        continue
    t1 = get_timestamp(start[0])
    t2 = get_timestamp(reached)
    dif = get_difference(t1, t2)
    timedeltas.append(dif)
    print(f'Latency for publication with id {id} : {dif}')

average_timedelta = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)
print(f'Average timedelta: {average_timedelta}')
