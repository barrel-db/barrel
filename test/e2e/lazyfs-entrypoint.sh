#!/bin/sh
# Mount LazyFS over the data directory, then run the server. The shell
# stays PID 1 so the server can be SIGKILLed from inside the container.
set -e
rm -f /tmp/faults.fifo
cd /opt/lazyfs/lazyfs
./scripts/mount-lazyfs.sh -c /etc/lazyfs.toml -m /lazyfs/mnt -r /lazyfs/root
for _ in $(seq 1 50); do
    mountpoint -q /lazyfs/mnt && break
    sleep 0.2
done
mountpoint -q /lazyfs/mnt || { echo "lazyfs did not mount"; exit 1; }
barrel_server foreground &
wait $! || true
echo "barrel_server exited"
sleep infinity
