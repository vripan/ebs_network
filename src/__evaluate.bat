start /b py manager.py
start /b py broker.py --id 1 --host localhost --port 8081 --neighbours 2,localhost,8082 3,localhost,8083
start /b py broker.py --id 2 --host localhost --port 8082 --neighbours 1,localhost,8081
start /b py broker.py --id 3 --host localhost --port 8083 --neighbours 1,localhost,8081
start /b py subscriber.py --id 6 --subs_count 100
start /b py publisher.py --id 7 --pubs 1800