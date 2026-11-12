# Airflow DAGs

DAG chỉ điều phối extract/transform/check/publish; logic tính toán nằm trong `src/shopvn/`. DAG `shopvn_direct_finance` chạy 03:00 VN, catchup tắt, pool Spark một slot, validation candidate trước publication. Xem [setup và vận hành](../infra/platform/airflow/README.md). Kafka/Superset chưa triển khai.

`shopvn_inventory_observation` chạy 00:00 VN: chụp inventory/products thật trước khi đợi Spark pool, chỉ công bố EOD nếu source observation trễ dưới 60 giây so với cutoff dự kiến của run. Trigger thủ công mặc định current stock, không bù EOD quá khứ. Xem [runbook chặng 1](../docs/runbooks/stage1-postgres-inventory.md).

`shopvn_marketplace_finance` chạy 04:00 VN, catchup=False, max_active_runs=1; capture → candidate → exact Trino/source gate → publish → post-check/finalize. Manual June demo truyền start_date/end_date; partial coverage không đồng nghĩa workflow fail nhưng không được coi complete/fresh. Không có verified files thì fail. Xem [runbook S2](../docs/runbooks/stage2-marketplace-finance.md).
