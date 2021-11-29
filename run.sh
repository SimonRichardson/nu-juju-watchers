#!/bin/bash

make build

if [ -d example0 ]; then
     rm -rf example0
fi
mkdir -p example0

# This is currently non-ha
nu-juju-watchers --api 127.0.0.1:8666 --db 127.0.0.1:9666 --dir example0 &
PID=$!

function cleanup {
    kill $PID || true
}
 
trap cleanup EXIT

function print {
    sed 's/^/    | /' $1
}

sleep 1
echo "Run create, update deletion..."

# Ensure that we can see the create and update changes
curl -s -X POST -d bar1 http://127.0.0.1:8666/foo | print
curl -s -X POST -d bar2 http://127.0.0.1:8666/foo | print

# See that bar2 is available
curl -s http://127.0.0.1:8666/foo | print

# Delete should also work
curl -s -X DELETE http://127.0.0.1:8666/foo | print

# Ensure that we see it again, after a delete.
curl -s -X POST -d bar3 http://127.0.0.1:8666/foo | print

rlwrap -H ~/.dqlite_repl.history socat - ./example0/juju.sock

# sleep 1
# echo "Running multiple commands at once..."
# 
# # Ensure we only see the last change
# i=0
# while [ $i -ne 10 ]; do
#         curl -s -X POST -d "jaz$i" http://127.0.0.1:8666/baz | print
#         i=$(($i+1))
# done
# 
# # See that jaz9 is available
# curl -s http://127.0.0.1:8666/baz | print
# 
# sleep 2
# 
# echo "Example DONE"
