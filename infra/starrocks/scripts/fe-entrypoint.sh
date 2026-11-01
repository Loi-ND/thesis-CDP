#! /bin/bash

IFS="," read -a BE_NODE_LIST <<< "${BE_NODES}"
start_fe.sh --host_type FQDN --daemon

echo "Waiting for FE..."

while ! mysql -h 127.0.0.1 -P9030 -uroot -e "SELECT 1" >/dev/null 2>&1; do
    sleep 1
done

for BE in "${BE_NODE_LIST[@]}"; do
    echo "Waiting for $BE:9050 ..."

    while ! timeout 1 bash -c "</dev/tcp/$BE/9050"  >/dev/null 2>&1; do
        sleep 1
    done

    echo "$BE is ready"

    if mysql -h 127.0.0.1 -P9030 -uroot -N -e "SHOW BACKENDS;" | grep -q "$BE"; then
        echo "BE exists"
    else
        mysql -h 127.0.0.1 -P9030 -uroot -e \
            "ALTER SYSTEM ADD BACKEND '$BE:9050';"
    fi
done
tail -f /dev/null