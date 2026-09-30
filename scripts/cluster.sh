#!/usr/bin/env bash
# Runs a 3-node exchange on this machine and measures it.
#
#   scripts/cluster.sh latency RATE [SECONDS]   steady load at RATE orders/sec
#   scripts/cluster.sh failover RATE [SECONDS]  same, but kill -9 the leader halfway
#
# Needs: mvn package (builds server/target/trade-engine.jar)
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=${1:-latency}
RATE=${2:-10000}
SECONDS_=${3:-20}
JAR=server/target/trade-engine.jar
RUN=$(mktemp -d /tmp/trade-engine-run.XXXX)
PEERS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
CLIENTS=127.0.0.1:7200,127.0.0.1:7201,127.0.0.1:7202
JVM="-Xms1g -Xmx1g -XX:+UseZGC -XX:+AlwaysPreTouch"

pids=()
cleanup() { kill -9 "${pids[@]}" 2>/dev/null || true; }
trap cleanup EXIT

for i in 0 1 2; do
  java $JVM -jar "$JAR" node --id $i --peers $PEERS --clients $CLIENTS --data "$RUN/node-$i" \
    > "$RUN/node-$i.log" 2>&1 &
  pids+=($!)
done
echo "3 nodes started, logs in $RUN"

leader() {   # the node whose latest status line says LEADER
  for i in 0 1 2; do
    if tail -n 1 "$RUN/node-$i.log" 2>/dev/null | grep -q "role=LEADER"; then echo $i; return; fi
  done
}
until [ -n "$(leader)" ]; do sleep 0.5; done
echo "leader is node $(leader)"

java $JVM -jar "$JAR" loadgen --clients $CLIENTS --rate "$RATE" --seconds "$SECONDS_" --warmup 5 \
  > "$RUN/loadgen.log" 2>&1 &
LOADGEN=$!

if [ "$MODE" = failover ]; then
  sleep $((5 + SECONDS_ / 2))
  L=$(leader)
  echo "kill -9 leader node $L"
  kill -9 "${pids[$L]}"
fi

wait $LOADGEN
cat "$RUN/loadgen.log"
