# Kiểm tra khả năng khôi phục PostgreSQL nguồn — 12/11/2026

Người dùng yêu cầu **kiểm tra khả năng khôi phục**, chưa yêu cầu thực hiện restore/start database. Vì vậy các kiểm tra dưới đây chỉ mount volume read-only vào helper container, không khởi động PostgreSQL trên dữ liệu nguồn, không tạo nguồn thay thế và không reset credential.

## Kết quả quan sát

- Các container nguồn postgres/api/sftp không còn tồn tại tại thời điểm kiểm tra; platform catalog/object store vẫn còn riêng.
- Volume `gr2_postgres-data` vẫn tồn tại, nhãn Docker Compose project `gr2`, tạo ngày 12/10/2026. Volume hiện không được container nào sử dụng.
- Có PG_VERSION=16, thư mục base và global/pg_control. PostgreSQL 16 pg_controldata đọc được control file: cluster state `shut down`, checkpoint cuối 12/11/2026 05:57:05 UTC, NextXID `0:863`.
- NextXID khớp PostgreSQL snapshot `863:863:` trong manifest chặng 1. Đây là dấu hiệu mạnh volume chứa nguồn cũ, **chưa phải chứng minh đầy đủ table contents/credentials** vì chưa chạy SQL trên volume đó.
- Image local `shopvn-postgres:v1.1.0` còn tồn tại, kiến trúc AMD64, binary PostgreSQL 16.14. Image upstream được cung cấp là ARM64; thử chạy shell của image upstream trong Docker hiện tại gặp `exec format error`. Không thay image hoặc cài emulation để chữa lỗi trong lượt kiểm tra này.

Kết luận: **có khả năng khôi phục cao từ volume còn lại**, không có cơ sở kết luận dữ liệu nguồn đã mất. Cũng chưa thể khẳng định restore hoàn tất hoặc mọi relation đều nguyên vẹn.

## Trình tự tiếp theo cần được đồng ý trước khi thực hiện

1. Xác nhận volume không còn writer; tạo bản sao/backup offline của đúng `gr2_postgres-data`, giữ nguyên bản gốc.
2. Dùng PostgreSQL major 16 và image tương thích; ưu tiên kiểm tra trên bản sao trước, không chạy initdb/reseed hoặc tự nâng major version.
3. Đối soát schema, quyền shopvn_reader, 9 table counts và Finance source total với evidence. Không reset HMAC key, go-live policy hoặc credentials để làm kết quả khớp.
4. Sau khi kiểm tra thành công và chốt phục hồi, cấu hình volume explicit/external, network và port nguồn; không mount nhầm platform catalog volume, không tạo bộ source trùng port.
5. Khởi động và kiểm tra Finance/inventory DAG bằng actual fresh source snapshot. Giữ failed-run audit; không bù EOD đã bỏ lỡ bằng stock hiện tại.

Counts tham chiếu: orders 211806, order_items 444142, returns 4215, customers 50000, products 2000, vouchers 200, inventory 2000, inventory_transactions 439722, product_reviews 41226. FIN02 1648602104364.00 VND; current stock tại lần quan sát đã kiểm chứng 5053681 đơn vị. Dữ liệu nguồn có thể đã thay đổi hợp lệ sau lần quan sát; mọi delta phải giải thích, không tự sửa source.

EOD ngày 11/11 và Bronze snapshots đã kiểm chứng vẫn được giữ trong lakehouse/runtime; chúng không thay thế việc khôi phục PostgreSQL nguồn cho các lần extract tiếp theo. [Bằng chứng chặng 1](../verification/stage1.json).

## Cập nhật sau khi người dùng tự phục hồi ngày 12/11/2026

Người dùng đã tự phục hồi các container nguồn và yêu cầu kiểm tra lại. Đã xác nhận postgres healthy, dùng image local shopvn-postgres:v1.1.0 và volume gr2_postgres-data. Kết nối bằng shopvn_reader trong transaction READ ONLY/REPEATABLE READ: đủ required columns, counts 9 bảng khớp baseline, không null/trùng primary keys; FIN02 và current-stock total khớp số tham chiếu ở trên.

API /health trả OK. SFTP authentication/listing thành công: Lazada 29, Shopee 30, TikTok 28 CSV. Host key SFTP khác mốc profiling trước; đã đối chiếu network fingerprint với public key trong đúng container phục hồi trước khi xác thực. Chưa recheck toàn bộ MD5 hoặc chạy lại full Airflow end-to-end trong lượt kiểm tra đọc đó. Không reset key/credential, sửa source hoặc tự xóa failed-run audit.

Như vậy mục kiểm tra khả năng phục hồi ở trên là lịch sử trước khi người dùng khôi phục; không dùng nó để khẳng định nguồn hiện vẫn mất. Công việc tiếp theo theo [kế hoạch MVP](../architecture/mvp-implementation-plan.md) là S2 — SFTP + Finance marketplace.
