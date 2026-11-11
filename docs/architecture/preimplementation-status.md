# Trạng thái 5 việc chuẩn bị triển khai

Ngày thực hiện: 2026-11-11. Thực hiện trên repo đã đổi tên `shopvn-data-platform`, nhánh `feat/preimplementation-readiness`. Đây là báo cáo kiểm tra thực tế, không phải xác nhận platform đã chạy end-to-end.

**Cập nhật mới nhất:** core storage spike, direct Finance và Airflow/monitoring local đã PASS. Xem [bằng chứng thực thi](../verification/README.md), [core verified-lock](../../infra/platform/spike/verified-lock.json), [Airflow runbook](../../infra/platform/airflow/README.md). Những lỗi tải ở phần lịch sử bên dưới đã được vượt qua, không còn là blocker hiện tại. Kafka/Superset và full MVP chưa hoàn thành. Bảng/lịch sử bên dưới ghi nhận giai đoạn chuẩn bị ban đầu.

| Hạng mục | Kết quả | Gate |
|---|---|---|
| Ghi quyết định/KPI contracts | BR01–BR30 giữ lịch sử, bổ sung BR31/BR32 đã được duyệt; rule version 1.1 | P01 đã chốt: FIN02 là Direct Finance KPI chính, FIN03 là proxy |
| Profiling | Đọc 9 bảng bằng SELECT-only; stream/checksum 90 file dự kiến; API mẫu 50 order + lặp 1 order | P02 đã chốt phạm vi: current stock + snapshot go-live; June backfill bị chặn, không coi yêu cầu lịch sử đã hoàn thành |
| Thiết kế bảng | 8 dims, 8 facts, shared audit, data dictionary, load/identity/DQ contracts, ERD Mermaid | Sẵn sàng vertical slice direct sales; warehouse history/actual payout/item refund không tự tạo |
| Technical spike | MinIO/Spark images build xong, Spark ghi/MERGE/evolve Iceberg, Trino đọc qua JDBC Catalog | **PASS:** 3 process mới cùng count/sum; restart catalog/object store/Trino giữ kết quả |
| Nghiệm thu | 23 unit/reference cases; direct slice trên snapshot PostgreSQL read-only với 211806 orders, 444142 lines, 4215 returns | **PASS cho slice:** 3 process mới, FIN02=1648602104364.00 VND khớp source/Spark/Trino; chưa chứng nhận toàn MVP |

## Evidence

- [Profiling report](../profiling/findings.md) và [aggregate JSON](../profiling/observed-profile.json).
- [Source schema](../../schemas/sources/postgres-observed.json).
- [KPI contracts](../business/kpi-contracts-v1.md).
- [Data dictionary](../data-model/data-dictionary-v1.md), [load contracts](../data-model/load-contracts-v1.md), [ERD](../data-model/gold-erd.md).
- [Acceptance cases](../business/acceptance-cases.md), [test fixture](../../tests/fixtures/business_acceptance.json).
- [Spike runbook](../../infra/platform/spike/README.md).

## Lịch sử phát hiện trước khi spike pass

1. Docker Desktop trước đó chưa chạy; đã mở và dùng các source containers có sẵn. Không tạo bộ source trùng host port, không sửa nguồn, không xóa container/volume của người dùng.
2. Docker engine linux/amd64, khoảng 6.77 GiB RAM. Source credentials hiện tại dùng được bằng tài khoản reader SELECT-only.
3. Generic apache/spark:3.5.9 đã chạy kiểm tra `python3 --version` và `java -version`: Python 3.10.12, Java 11.0.31. Vì vậy Dockerfile readiness dùng Spark distribution trong image đó nhưng runtime riêng Python 3.11/Java 17. Image runtime mới **chưa build/execute thành công**.
4. MinIO Docker Hub image bị pull access denied, quay.io trả 401. [Upstream](https://github.com/minio/minio) xác nhận community source-only và repo đã archived. Cấu hình mới build commit 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a thuộc release 2025-10-15. Đây chỉ là demo cục bộ; không chứng nhận lựa chọn object store production.
5. BuildKit tải Iceberg JAR gặp unexpected EOF. Maven HEAD và small range requests hoạt động; probe 1 MiB chỉ nhận 638976 byte trong 25 giây rồi timeout. Downloader có range cache/retry, nhưng chưa tải và verify đủ toàn bộ artifacts.
6. Trino 484 registry manifest và digest đã resolve; image pull chưa xác nhận hoàn tất. Không xem resolve manifest là runtime compatibility test.
7. Resolve Python base image sau đó gặp TLS handshake timeout với Docker Hub auth endpoint. Đã dừng các download client do lượt làm việc này khởi tạo, giữ 100 phần cache (13107200 bytes) trong runtime/spike-build để tải tiếp; không có service spike đang chạy. Không sửa proxy/firewall hoặc cấu hình mạng của máy.
8. Sau khi BR31/BR32 được duyệt, probe nhỏ thành công nhưng lúc đó chưa có full spike. Ở lượt tiếp tục sau mất mạng, đã tải đủ artifacts, sửa checksum fallback cho PG JDBC, build images, chạy cả core và direct slice thành công. Kết quả hiện hành ở verification/, không dùng báo cáo failed lịch sử thay cho evidence mới.

## Hai quyết định bổ sung đã được duyệt

- P01 / BR31: FIN02 net customer amount là Direct Finance KPI chính, FIN03 giữ nhãn proxy.
- P02 / BR32: current stock + snapshot thực từ go-live; chặn June backfill. Không thêm lịch sử mô phỏng cho MVP.

Chi tiết và ảnh hưởng xem [biên bản phê duyệt](../business/decisions/2026-11-11-finance-inventory.md). Các quyết định này đóng câu hỏi về phạm vi, không sửa thiếu sót nguồn hoặc chứng nhận spike đã pass.

## Các chi tiết khác còn cần làm rõ
- OPS07: bộ statuses cho aggregate return-request rate cần duyệt; có thể báo status distribution trước.
- Timestamp without timezone cần chủ nguồn xác nhận, không lấy server UTC làm bằng chứng business timezone.

## Cho phép bắt đầu phần nào

Direct ingestion/Bronze/Silver/Gold/mart FIN02 và chart đã có kết quả. Bước tiếp theo là Airflow orchestration/monitoring cho slice này rồi mở rộng nguồn. Chưa công bố June EOD stock, exact product refund/net merchandise, causal campaign ROI hoặc actual bank settlement. Core đã có verified-lock; provider/Kafka/Superset và kiểm thử tải/lỗi diện rộng còn là gate trước full release.
