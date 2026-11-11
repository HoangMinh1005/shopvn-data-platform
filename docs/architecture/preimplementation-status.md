# Trạng thái 5 việc chuẩn bị triển khai

Ngày thực hiện: 2026-11-11. Thực hiện trên repo đã đổi tên `shopvn-data-platform`, nhánh `feat/preimplementation-readiness`. Đây là báo cáo kiểm tra thực tế, không phải xác nhận platform đã chạy end-to-end.

| Hạng mục | Kết quả | Gate |
|---|---|---|
| Ghi quyết định/KPI contracts | BR01–BR30 giữ lịch sử, bổ sung BR31/BR32 đã được duyệt; rule version 1.1 | P01 đã chốt: FIN02 là Direct Finance KPI chính, FIN03 là proxy |
| Profiling | Đọc 9 bảng bằng SELECT-only; stream/checksum 90 file dự kiến; API mẫu 50 order + lặp 1 order | P02 đã chốt phạm vi: current stock + snapshot go-live; June backfill bị chặn, không coi yêu cầu lịch sử đã hoàn thành |
| Thiết kế bảng | 8 dims, 8 facts, shared audit, data dictionary, load/identity/DQ contracts, ERD Mermaid | Sẵn sàng vertical slice direct sales; warehouse history/actual payout/item refund không tự tạo |
| Technical spike | Có Compose isolated, source-build MinIO, Spark Python3.11/Java17 Dockerfile, runner MERGE/schema/replay/Trino/restart | **Chưa đạt**: artifact/image downloads không hoàn tất, chưa chạy Spark MERGE hay Trino query |
| Nghiệm thu | Synthetic fixture với số tính tay và 20 unit/reference checks sau BR31/BR32 | Chứng minh reference business calculations và scope gate; chưa chứng minh production Spark pipeline |

## Evidence

- [Profiling report](../profiling/findings.md) và [aggregate JSON](../profiling/observed-profile.json).
- [Source schema](../../schemas/sources/postgres-observed.json).
- [KPI contracts](../business/kpi-contracts-v1.md).
- [Data dictionary](../data-model/data-dictionary-v1.md), [load contracts](../data-model/load-contracts-v1.md), [ERD](../data-model/gold-erd.md).
- [Acceptance cases](../business/acceptance-cases.md), [test fixture](../../tests/fixtures/business_acceptance.json).
- [Spike runbook](../../infra/platform/spike/README.md).

## Các phát hiện kỹ thuật đã xác minh

1. Docker Desktop trước đó chưa chạy; đã mở và dùng các source containers có sẵn. Không tạo bộ source trùng host port, không sửa nguồn, không xóa container/volume của người dùng.
2. Docker engine linux/amd64, khoảng 6.77 GiB RAM. Source credentials hiện tại dùng được bằng tài khoản reader SELECT-only.
3. Generic apache/spark:3.5.9 đã chạy kiểm tra `python3 --version` và `java -version`: Python 3.10.12, Java 11.0.31. Vì vậy Dockerfile readiness dùng Spark distribution trong image đó nhưng runtime riêng Python 3.11/Java 17. Image runtime mới **chưa build/execute thành công**.
4. MinIO Docker Hub image bị pull access denied, quay.io trả 401. [Upstream](https://github.com/minio/minio) xác nhận community source-only và repo đã archived. Cấu hình mới build commit 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a thuộc release 2025-10-15. Đây chỉ là demo cục bộ; không chứng nhận lựa chọn object store production.
5. BuildKit tải Iceberg JAR gặp unexpected EOF. Maven HEAD và small range requests hoạt động; probe 1 MiB chỉ nhận 638976 byte trong 25 giây rồi timeout. Downloader có range cache/retry, nhưng chưa tải và verify đủ toàn bộ artifacts.
6. Trino 484 registry manifest và digest đã resolve; image pull chưa xác nhận hoàn tất. Không xem resolve manifest là runtime compatibility test.
7. Resolve Python base image sau đó gặp TLS handshake timeout với Docker Hub auth endpoint. Đã dừng các download client do lượt làm việc này khởi tạo, giữ 100 phần cache (13107200 bytes) trong runtime/spike-build để tải tiếp; không có service spike đang chạy. Không sửa proxy/firewall hoặc cấu hình mạng của máy.
8. Sau khi BR31/BR32 được duyệt, kiểm tra lại Maven range 128 KiB thành công HTTP 206 trong 2.10 giây. Chưa tải đủ artifact, chưa build/rerun core spike; kiểm tra nhỏ này không thay đổi trạng thái chưa đạt của technical spike.

## Hai quyết định bổ sung đã được duyệt

- P01 / BR31: FIN02 net customer amount là Direct Finance KPI chính, FIN03 giữ nhãn proxy.
- P02 / BR32: current stock + snapshot thực từ go-live; chặn June backfill. Không thêm lịch sử mô phỏng cho MVP.

Chi tiết và ảnh hưởng xem [biên bản phê duyệt](../business/decisions/2026-11-11-finance-inventory.md). Các quyết định này đóng câu hỏi về phạm vi, không sửa thiếu sót nguồn hoặc chứng nhận spike đã pass.

## Các chi tiết khác còn cần làm rõ
- OPS07: bộ statuses cho aggregate return-request rate cần duyệt; có thể báo status distribution trước.
- Timestamp without timezone cần chủ nguồn xác nhận, không lấy server UTC làm bằng chứng business timezone.

## Cho phép bắt đầu phần nào

Có thể bắt đầu ingestion read-only, Bronze/Silver, direct orders/order_items vertical slice và source reconciliation theo contract. Chưa công bố June EOD stock, exact product refund/net merchandise, causal campaign ROI hoặc actual bank settlement. Cổng triển khai platform diện rộng vẫn cần spike pass và dependency lock từ runtime đã kiểm chứng.
