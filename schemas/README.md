# Schema contracts

[sources/postgres-observed.json](sources/postgres-observed.json) giữ schema/constraints đã profiling. Parser schemas, PK và source export allowlist hiện hành được quản lý cùng nhau trong [contracts/direct.py](../src/shopvn/contracts/direct.py).

Event contracts Kafka/simulator chưa triển khai; sẽ tạo JSON Schema versioned khi bắt đầu slice streaming. Không giữ thư mục rỗng để thể hiện đã có implementation. Envelope dự kiến có event_id, event_type, event_version, event_time, produced_at, source, data_origin và payload; simulated data phải tách nguồn theo BR29.
