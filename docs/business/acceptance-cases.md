# Bộ dữ liệu nghiệm thu nghiệp vụ

Fixture: [business_acceptance.json](../../tests/fixtures/business_acceptance.json), toàn bộ là dữ liệu giả lập. Reference functions nằm ở `src/shopvn/quality/acceptance.py`, chưa thay thế transformation Spark hoặc kiểm thử dữ liệu thật.

| Trường hợp | Kết quả tính tay / điều cần chứng minh |
|---|---|
| Direct O1: subtotal 100000, discount 10000, shipping 15000 | Merchandise 90000, customer total 105000; approved refund 5000 chưa trừ |
| Direct O2: returned/paid, subtotal 200000, shipping 20000, hai refund 30000+20000 | Merchandise less refund proxy 150000, net customer amount 170000; header chỉ tính một lần |
| Cancelled/refunded, shipping/paid, confirmed/paid | Không vào completed revenue |
| Delivered/pending hoặc delivered/failed | Không vào eligible revenue, đếm excluded |
| Discount 12000 trên subtotal 10000 | Bị loại revenue vì lỗi nguồn, không coi như free order |
| Quantity 0 bên cạnh line hợp lệ | Giữ audit, không tăng units/line denominator |
| Discount 1 chia 3 line giá trị bằng nhau | Allocation [1,0,0], tie-break stable ID; đảo input không đổi kết quả |
| Tổng direct fixture | 4 eligible orders, 3 customers, 315002 merchandise before refund, 50000 refund, 265002 merchandise-less-refund proxy, 300002 net customer amount, 8 units |
| Marketplace 100000*2, commission 8% | Merchandise 200000, source net 184000; không trừ platform discount/commission lần hai |
| Marketplace 101*3, commission 7.5% | Decimal floor = 280 VND |
| Tổng marketplace fixture | 200303 merchandise, 184280 reported net, 5 completed units |
| Shipment: delivered 2, failed 1, returned 1, in-flight 1 | Success = 2/4 = 0.5; avg duration 36 giờ; 1 delivered late |
| Không có terminal shipment | Rate NULL, không phải 0% |
| Refund toàn bộ 120 gồm merchandise 100 + shipping 20 | Net customer amount=0, merchandise-less-refund proxy=-20; không tự clamp hoặc gọi là net merchandise chính xác |
| Stock anchor 100, later movements +20,-5 | Historical balance 85 khi business timestamps/anchor đã được xác minh; synthetic only |
| Kafka cùng event_id và payload | Dedup còn 1 event; khác payload phải reject conflict |
| Chạy cùng fixture 3 lần | Business metrics không đổi; không chứng minh storage idempotency cho pipeline chưa viết |
| Finance primary theo BR31 | Fixture chọn FIN02=300002 VND; không chọn proxy 265002 VND thay headline |
| Inventory BR32 | Với go-live giả lập 2026-11-11: June 1/30 bị chặn; Nov 11/12 qua gate thời gian. Đây không phải ngày go-live production được gán sẵn |

Rule version hiện hành 1.1; tổng 20 reference/unit test cases. Inventory period gate chỉ kiểm tra phạm vi ngày, chưa xác minh đã quan sát đúng EOD/cutoff. Test dựng ngược số dư synthetic không cho phép backfill actual June trong MVP.

Chạy bằng Python 3.11:

```powershell
uv venv --python 3.11 .venv
uv pip install --python .venv/Scripts/python.exe -r requirements-preparation.txt
.venv/Scripts/python.exe -m pytest -q
```

Linux/CI dùng `.venv/bin/python` thay đường dẫn Windows. Các fixture future integration cần thêm SCD2 overlap, schema drift, API retry, checksum rejection và multi-table publication; không tuyên bố đã kiểm thử các phần chưa có implementation.
