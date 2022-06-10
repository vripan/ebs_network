@echo off

start "Manager" cmd /k py -3 src\manager.py

start "Broker#1" cmd /k py -3 src\broker.py --id 1 --host localhost --port 8081 --neighbours 2,localhost,8082 3,localhost,8083
timeout /t 2

start "Broker#2" cmd /k py -3 src\broker.py --id 2 --host localhost --port 8082 --neighbours 1,localhost,8081
timeout /t 2

start "Broker#3" cmd /k py -3 src\broker.py --id 3 --host localhost --port 8083 --neighbours 1,localhost,8081
timeout /t 2

start "Subscriber#4" cmd /k py -3 src\subscriber.py --id 4 --subs_count 10000
timeout /t 3

@REM start "Subscriber#4" cmd /k py -3 src\subscriber.py --id 5 --subs_count 20
@REM timeout /t 1

@REM start "Subscriber#6" cmd /k py -3 src\subscriber.py --id 6 --subs_count 20
@REM timeout /t 1

@REM start "Publisher#7" cmd /k py -3 src\publisher.py --id 7 --time 3
@REM start "Publisher#8" cmd /k py -3 src\publisher.py --id 8 --pubs 100
@REM start "Publisher#9" cmd /k py -3 src\publisher.py --id 9 --pubs 100
@REM start "Publisher#10" cmd /k py -3 src\publisher.py --id 10 --pubs 100
@REM start "Publisher#11" cmd /k py -3 src\publisher.py --id 11 --pubs 100
@REM start "Publisher#12" cmd /k py -3 src\publisher.py --id 12 --pubs 100