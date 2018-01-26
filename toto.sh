#!/usr/bin/env bash

set +x

unset AZURE_BACKEND_ACCESS_KEY
unset AZURE_BACKEND_ACCESS_KEY_2
unset AZURE_BACKEND_ACCOUNT_NAME
unset AZURE_BACKEND_ACCOUNT_NAME_2
unset AZURE_BACKEND_ENDPOINT
unset AZURE_BACKEND_ENDPOINT_2
unset AZURE_STORAGE_ACCOUNT
unset AZURE_STORAGE_ACCESS_KEY


node mdserver.js > mdserver.out 2>&1 &
node dataserver.js > dataserver.out 2>&1 &
while true
do nc -w 1 localhost 9991
    ret=$?
    echo "ret: $ret"
    if [ "$ret" -eq 0 ]
    then
        break
    fi
    sleep 0.5
done
while true
do nc -w 1 localhost 9990
    ret=$?
    echo "ret: $ret"
    if [ "$ret" -eq 0 ]
    then
        break
    fi
    sleep 0.5
done
npm run multiple_backend_test
