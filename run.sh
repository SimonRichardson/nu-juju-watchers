#!/bin/bash
# shellcheck disable=SC2120,SC2001,SC2004,SC2086

set -e

REPL="${REPL:-}"
ENABLE_HA="${ENABLE_HA:-}"

make build

for i in example*; do
    rm -rf "./$i"
done

mkdir -p example0
if [ -n "$ENABLE_HA" ]; then
    mkdir -p example1 example2
fi

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
if [ -n "$ENABLE_HA" ]; then
    nu-juju-watchers --api 127.0.0.1:8667 --db 127.0.0.1:9667 --join 127.0.0.1:9666 --dir example1 &
    PID1=$!
    nu-juju-watchers --api 127.0.0.1:8668 --db 127.0.0.1:9668 --join 127.0.0.1:9666 --dir example2 &
    PID2=$!
fi

sleep 6

function cleanup {
    kill $PID0
    if [ -n "$ENABLE_HA" ]; then
        kill $PID1 $PID2
    fi
}

function port {
    if [ -n "$ENABLE_HA" ]; then
        shuf -i 8666-8668 -n 1
        return
    fi
    echo "8666"
}

trap cleanup EXIT

#
# Test that we run in a C(R)UD setup.
#
header "Run create, update deletion..."

# Ensure that we can see the create and update changes
curl -s -X POST -d bar1 "http://127.0.0.1:8666/foo" | print
curl -s -X POST -d bar2 "http://127.0.0.1:8666/foo" | print

# See that bar2 is available
curl -s "http://127.0.0.1:8666/foo" | print

# Delete should also work
curl -s -X DELETE "http://127.0.0.1:8666/foo" | print

# Ensure that we see it again, after a delete.
curl -s -X POST -d bar3 "http://127.0.0.1:8666/foo" | print

#
# Test that we run in order and that we find the last one.
#
header "Running multiple commands serialized..."

# Ensure we only see the last change
i=0
while [ $i -ne 10 ]; do
        rnd_port=$(port)
        curl -s -X POST -d "data$i" "http://127.0.0.1:$rnd_port/foobar" | print
        i=$(($i+1))
done

#
# Test that we queries concurrently.
#
header "Running multiple commands concurrently..."

# Ensure we only see the last change
i=0
while [ $i -ne 10 ]; do
        rnd_port=$(port)
        (curl -s -X POST -d "jaz$i" "http://127.0.0.1:$rnd_port/baz" | print) &
        i=$(($i+1))
done

# We're done.
header "Example DONE"

# Show the repl if requested.
if [ -n "$REPL" ]; then 
    rlwrap -H ~/.dqlite_repl.history socat - ./example0/juju.sock
fi