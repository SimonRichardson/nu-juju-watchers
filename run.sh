#!/bin/bash

set -e

make build

for i in example*; do
    rm -rf $i
done
mkdir -p example0 example1 example2

function print {
    sed 's/^/    [+] /' $1
}

function header {
    sleep 1
    echo ""
    echo "# $1"
    echo ""
}

#
# Setup ha block
#
header "Setup cluster..."

nu-juju-watchers --api 127.0.0.1:8666 --db 127.0.0.1:9666 --dir example0 &
PID0=$!
nu-juju-watchers --api 127.0.0.1:8667 --db 127.0.0.1:9667 --join 127.0.0.1:9666 --dir example1 &
PID1=$!
nu-juju-watchers --api 127.0.0.1:8668 --db 127.0.0.1:9668 --join 127.0.0.1:9666 --dir example2 &
PID2=$!

sleep 10

function cleanup {
    kill $PID0 $PID1 $PID2 || true
}
 
trap cleanup EXIT

port=$(shuf -i 8666-8668 -n 1)

#
# Test that we run in a C(R)UD setup.
#
header "Run create, update deletion..."

# Ensure that we can see the create and update changes
curl -s -X POST -d bar1 http://127.0.0.1:$port/foo | print
curl -s -X POST -d bar2 http://127.0.0.1:$port/foo | print

# See that bar2 is available
curl -s http://127.0.0.1:$port/foo | print

# Delete should also work
curl -s -X DELETE http://127.0.0.1:$port/foo | print

# Ensure that we see it again, after a delete.
curl -s -X POST -d bar3 http://127.0.0.1:$port/foo | print

#
# Test that we run in order and that we find the last one.
#
header "Running multiple commands serialized..."

# Ensure we only see the last change
i=0
while [ $i -ne 10 ]; do
        port=$(shuf -i 8666-8668 -n 1)
        curl -s -X POST -d "data$i" http://127.0.0.1:$port/foobar | print
        i=$(($i+1))
done

#
# Test that we queries concurrently.
#
header "Running multiple commands concurrently..."

# Ensure we only see the last change
i=0
while [ $i -ne 10 ]; do
        port=$(shuf -i 8666-8668 -n 1)
        (curl -s -X POST -d "jaz$i" http://127.0.0.1:$port/baz | print) &
        i=$(($i+1))
done

# We're done.
header "Example DONE"

# Show the repl if requested.
if [ -n "$REPL" ]; then 
    rlwrap -H ~/.dqlite_repl.history socat - ./example0/juju.sock
fi