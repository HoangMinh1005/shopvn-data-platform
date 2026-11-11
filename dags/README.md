# Airflow DAGs

DAG chỉ điều phối extract/transform/check/publish; logic tính toán nằm trong `src/shopvn/`. DAG `shopvn_direct_finance` chạy 03:00 VN, catchup tắt, pool Spark một slot, validation candidate trước publication. Xem [setup và vận hành](../infra/platform/airflow/README.md). Kafka/Superset chưa triển khai.
