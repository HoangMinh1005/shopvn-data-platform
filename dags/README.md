# Airflow DAGs

DAG chỉ điều phối extract/transform/check/publish; logic tính toán nằm trong `src/shopvn/`. DAG `shopvn_direct_finance` chạy 03:00 VN, catchup tắt, pool Spark một slot, validation candidate trước publication. Xem [setup và vận hành](../infra/platform/airflow/README.md). Kafka/Superset chưa triển khai.

`shopvn_inventory_observation` chạy 00:00 VN: chụp inventory/products thật trước khi đợi Spark pool, chỉ công bố EOD nếu source observation trễ dưới 60 giây so với cutoff dự kiến của run. Trigger thủ công mặc định current stock, không bù EOD quá khứ. Xem [runbook chặng 1](../docs/runbooks/stage1-postgres-inventory.md).
