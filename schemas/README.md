# Schema contracts

`sources/` giữ schema được xác minh qua profiling; `events/` giữ JSON Schema versioned của simulator. Chưa có schema contract được kiểm chứng.

Event envelope dự kiến có event_id, event_type, event_version, event_time, produced_at, source, data_origin và payload. Dùng `data_origin = simulated` cho generator, theo BR29.
