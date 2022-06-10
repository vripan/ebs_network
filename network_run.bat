@echo off

start "Manager" cmd /k py -3 src\manager.py

start "Broker#1" cmd /k py -3 src\broker.py --id 1 --host localhost --port 8081 --neighbours 2,localhost,8082 3,localhost,8083
timeout /t 2

start "Broker#2" cmd /k py -3 src\broker.py --id 2 --host localhost --port 8082 --neighbours 1,localhost,8081
timeout /t 2

start "Broker#3" cmd /k py -3 src\broker.py --id 3 --host localhost --port 8083 --neighbours 1,localhost,8081
timeout /t 2

start "Subscriber#4" cmd /k py -3 src\subscriber.py --id 4 --subs_count 20
timeout /t 1

@REM start "Subscriber#4" cmd /k py -3 src\subscriber.py --id 5 --subs_count 20
@REM timeout /t 1

@REM start "Subscriber#6" cmd /k py -3 src\subscriber.py --id 6 --subs_count 20
@REM timeout /t 1

start "Publisher#7" cmd /k py -3 src\publisher.py --id 7 --pubs 100