#! /bin/bash

airflow db migrate

if ! airflow users list | grep -q "loinguyenduy12a1@gmail.com"; then
    airflow users create \
        --username admin \
        --password admin \
        --firstname nguyenduy \
        --lastname loi \
        --role Admin \
        --email loinguyenduy12a1@gmail.com
fi

airflow api-server --port 8080 &
airflow scheduler &
airflow triggerer &
airflow dag-processor &

wait -n

