# Chặng 1 — PostgreSQL 9 bảng, SCD2/as-of và actual inventory

## Phạm vi

Full PostgreSQL snapshot 9 bảng trong một transaction REPEATABLE READ/READ ONLY. Thêm inventory, inventory_transactions và product_reviews vào Bronze/Silver. Không export review.comment vì free text có thể chứa PII. Contract version 2; Finance business rule vẫn 1.1.

Không dựng Gold transaction/review marts trong chặng này; chúng thuộc chặng hoàn thiện nghiệp vụ. Silver transactions giữ business_time_reliable=false, business_effective_at/warehouse_id=NULL để không suy lịch sử kho từ timestamp nguồn sai. Review rating ngoài [1,5]/NULL vào quarantine, có accepted+rejected reconciliation.

## SCD2 và lookup

- Customer/product versions theo **thời điểm đã quan sát** UTC, khoảng [valid_from,valid_to), đúng một current version mỗi business key; surrogate keys giữ ổn định.
- Hash không đổi: cập nhật last_observed_at, không tạo version. Cùng timestamp nhưng payload khác bị chặn.
- Finance apply snapshot cũ hơn watermark bị chặn trước Bronze/Silver writes. Đọc lịch sử as-of không cần sửa history; không chạy snapshot cũ để ghi đè trạng thái mới.
- Orders chỉ có ngày nghiệp vụ đáng tin cậy. Lookup tại min(cuối ngày VN, thời điểm snapshot) và ghi observed_day_asof. Không gọi đây là tier/cost chính xác tại giây mua hàng.
- Trước lần quan sát đầu tiên của entity: giữ initial_snapshot_fallback; không backdate dimension về tháng 6 và không khẳng định lịch sử thật trước go-live.
- Joins customer/product bắt buộc kèm order_date; không chỉ join business key làm nhân số dòng hoặc lấy current tier cho mọi đơn lịch sử.
- Nguồn biến mất không tự hard-delete dimension hoặc suy thời điểm xóa. Temporal correctness được kiểm tra trên fixtures độc lập; không sửa DB nguồn để giả lập tier changes.

## Bootstrap và current stock

```powershell
python scripts/bootstrap_airflow.py
docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml up -d
```

Bootstrap sinh INVENTORY_GO_LIVE_DATE còn thiếu bằng ngày bắt đầu triển khai theo VN; lần actual observation đầu phải cùng ngày đó. Policy sau khi persist ở lakehouse.ops.inventory_policy là immutable; mất env không được tự reset metadata để backfill. Với policy tồn tại, dùng lại đúng cấu hình, không rotate HMAC key.

Airflow DAG shopvn_inventory_observation:

```text
initialize → capture_source → load_observation → verify_inventory → finalize
```

Manual trigger với mode=current, business_date để trống: lấy inventory/products thật tại thời điểm chạy, đối soát mỗi warehouse và publish lakehouse.analytics.current_stock. Không phụ thuộc nguồn orders chỉ có tháng 6. Lưu observed_at, source_last_updated, source snapshot và dimension match basis. Source_last_updated vẫn NTZ, không tự áp timezone.

## EOD

DAG scheduled 00:00 VN, catchup=False, run_immediately=False. Scheduled mode=eod và business_date cố định theo logical cutoff của run; scheduler muộn sang ngày khác không được tự đổi ngày EOD. Chụp nguồn ưu tiên trước; Spark có thể xử lý sau, vì cutoff xét **source observation time**, không xét lúc Spark kết thúc. Dùng cùng Spark pool/advisory lock với Finance.

Policy được [người dùng duyệt](../business/decisions/2026-11-11-inventory-cutoff.md): 60 giây đầu sau midnight, có độ trễ; không phải stock chính xác tuyệt đối tại 00:00. Output lakehouse.gold.fact_inventory_daily_snapshot theo product/warehouse/day. First accepted capture giữ nguyên; same-payload replay không nhân bản; conflicting payload block.

Nếu capture muộn: chỉ current stock được cập nhật khi hợp lệ, DAG EOD vẫn fail. Sau 00:10, thiếu EOD ngày trước sẽ có cảnh báo độc lập scheduler qua exporter. Manual current run không làm xanh EOD coverage. Không có data trước go-live thì để missing, không copy stock ngày khác.

Rerun an toàn: clear task thất bại/downstream trong cùng run. Capture task tái dùng inventory-manifest.json đã hoàn tất; không extract lại rồi gọi timestamp mới là snapshot cũ. Candidate partial/fail xem inventory-result.json trong runtime/orchestration/runs/<key>. Finalizer không che upstream failure; audit có current_published/eod_published riêng.

## Kiểm chứng và truy vấn

```powershell
python -m pytest -q
docker compose --project-name shopvn-readiness --env-file .env -f infra/platform/spike/compose.yml run --rm --no-deps spark --master "local[2]" --driver-memory 1g /workspace/tests/integration/stage1_temporal_spark.py
python tests/integration/verify_stage1_replays.py
python scripts/verify_airflow.py --business-date 2026-06-30
Get-Content -Raw tests/integration/airflow_time_runtime.py | docker compose --project-name shopvn-ops --env-file .env -f infra/platform/airflow/compose.yml exec -T airflow-scheduler python -
python tests/integration/verify_actual_eod_replay.py
python scripts/export_stage1_verification.py
```

Không chạy writer integration và full DAG đồng thời. Không replay snapshot Finance cũ sau khi inventory đã advance product watermark: lấy full snapshot mới rồi restate ngày nghiệp vụ cần báo cáo. Snapshot cũ vẫn giữ cho audit/read-only history; không xóa để chữa lỗi.

```sql
SELECT * FROM lakehouse.ops.inventory_policy;
SELECT count(*),sum(stock_qty),max(observed_at) FROM lakehouse.analytics.current_stock;
SELECT snapshot_date,count(*),sum(stock_qty),max(capture_lag_seconds)
FROM lakehouse.gold.fact_inventory_daily_snapshot GROUP BY 1;
```

Grafana có nhóm inventory tách Finance June coverage; Prometheus có InventoryFailed và InventoryEODMissing. Cảnh báo hiện local, chưa email/Slack. Phần yêu cầu June reconstructed stock vẫn là ngoại lệ do nguồn; thêm current/EOD observed không xóa ngoại lệ đó.

## Bằng chứng lần chạy đầu

Go-live: 11/11/2026. Scheduled EOD cho ngày 11/11 có run_id scheduled__2026-11-11T17:00:00+00:00, source snapshot 1f54077fb2284b15984db7eafaa1d397. Chụp lúc 00:00:02,153998 ngày 12/11 VN (lag 2,153998 giây); 2.000 sản phẩm, 5.053.681 đơn vị tồn. DAG success, actual-capture replay không đổi daily Iceberg snapshot. Đây là quan sát thật gần cutoff, không phải số tồn chính xác tại 00:00. [Evidence aggregate](../verification/stage1.json).
