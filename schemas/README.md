# Schema contracts

`sources/postgres-observed.json` giữ schema/constraints đã profiling. Direct slice có explicit parser schemas và source reconciliation. `events/` còn là chỗ dành cho JSON Schema versioned của simulator, chưa triển khai.

Event envelope dự kiến có event_id, event_type, event_version, event_time, produced_at, source, data_origin và payload. Dùng `data_origin = simulated` cho generator, theo BR29.
