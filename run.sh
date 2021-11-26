#!/bin/bash

make build

if [ -d example ]; then
    rm -rf example
fi
mkdir -p example

# This is currently non-ha
nu-juju-watchers --api 127.0.0.1:8666 --db 127.0.0.1:9666 --dir example &
PID=$!

function cleanup {
    kill $PID
}

trap cleanup EXIT

sleep 1

# Ensure that we can see the create and update changes
curl -X POST -d bar1 http://127.0.0.1:8666/foo
curl -X POST -d bar2 http://127.0.0.1:8666/foo

# See that bar2 is available
curl http://127.0.0.1:8666/foo

# Delete should also work
curl -X DELETE http://127.0.0.1:8666/foo

# Ensure we only see the last change
i=0
while [ $i -ne 10 ]; do
        curl -X POST -d "jaz$i" http://127.0.0.1:8666/baz
        i=$(($i+1))
done

# See that jaz9 is available
curl http://127.0.0.1:8666/baz

sleep 1

echo "Example DONE"
