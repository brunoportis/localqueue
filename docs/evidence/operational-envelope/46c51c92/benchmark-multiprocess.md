# localqueue benchmark

- Package: `1.1.2`
- Commit: `46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da`
- Profile: `multiprocess-ci`
- Canonical: `False`
- Overrides: `none`

## Environment

| Field | Value |
|---|---|
| architecture | x86_64 |
| cpu_model | n/a |
| filesystem_type | tmpfs |
| logical_cpu_count | 12 |
| os | Linux |
| os_release | 7.0.13-arch1-2 |
| python_executable | /tmp/localqueue-issue29-XL5IA6/venv/bin/python |
| python_implementation | CPython |
| python_version | 3.13.13 |
| sqlite_version | 3.50.4 |
| timer_implementation | clock_gettime(CLOCK_MONOTONIC) |
| timer_resolution_ns | 1 |
| total_memory_bytes | 29306621952 |
| workdir_filesystem | <benchmark-workdir> |

## Scenarios

| Scenario | Durability | P/C | Payload requested/actual | Messages | Produced/s | ACK/s | Claim p50/p95/p99 ns | Roundtrip p50/p95/p99 ns | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---|
| mp-p1-c1-payload100-normal | normal | 1/1 | 100/100 | 200 | 20567.464574599016 | 6486.2170805835285 | 72566/93916/128771 | 16030087/20502912/21013887 | passed |
| mp-p1-c1-payload100-full | full | 1/1 | 100/100 | 200 | 19982.519292123252 | 6414.959891746269 | 70332/122409/187249 | 16715097/20771635/21113323 | passed |
| mp-p4-c8-payload100-normal | normal | 4/8 | 100/100 | 200 | 3025.689999149025 | 2928.5285862900173 | 73918/1199581/13567085 | 8039940/10663672/18704022 | passed |
| mp-p4-c8-payload100-full | full | 4/8 | 100/100 | 200 | 3057.447512569243 | 2249.50691651955 | 86321/10276017/25450235 | 15097314/21100659/54354282 | passed |
| mp-p1-c1-payload100000-normal | normal | 1/1 | 100000/100000 | 200 | 878.6011952033787 | 853.6194975468449 | 342811/1531681/10887118 | 12542150/39405685/39705014 | passed |
| mp-p1-c1-payload100000-full | full | 1/1 | 100000/100000 | 200 | 880.8341266639299 | 813.9940356866218 | 325749/8723106/11633062 | 13336063/24919704/25201070 | passed |
| mp-p4-c8-payload100000-normal | normal | 4/8 | 100000/100000 | 200 | 788.8618045897724 | 785.067421001315 | 453207/16865717/59505324 | 8927057/36913530/107714226 | passed |
| mp-p4-c8-payload100000-full | full | 4/8 | 100000/100000 | 200 | 899.8634394240233 | 890.8358442803623 | 384579/25681767/30022556 | 8921857/26594152/94088381 | passed |

### mp-p1-c1-payload100-normal

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 41361408 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 41361408 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 69632 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 69632 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 69632 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4120032 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### mp-p1-c1-payload100-full

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=full`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=2`, `synchronous_name=FULL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 69632 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 69632 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 69632 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4120032 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### mp-p4-c8-payload100-normal

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-1 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-2 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-3 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-4 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-5 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-6 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-7 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-1 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-2 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-3 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 69632 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 69632 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 57344 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4120032 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### mp-p4-c8-payload100-full

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-1 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-2 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-3 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-4 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-5 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-6 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-7 | consumer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-1 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-2 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-3 | producer | passed | 0 | 41492480 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=full`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=2`, `synchronous_name=FULL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 77824 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 77824 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 61440 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4124152 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.0}`.

### mp-p1-c1-payload100000-normal

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 42848256 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 42475520 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 20205568 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 20205568 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 20205568 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4906952 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.009}`.

### mp-p1-c1-payload100000-full

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 43458560 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 43458560 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=full`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=2`, `synchronous_name=FULL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 20205568 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 20205568 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 20205568 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 4981112 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.009}`.

### mp-p4-c8-payload100000-normal

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-1 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-2 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-3 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-4 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-5 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-6 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-7 | consumer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-1 | producer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-2 | producer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-3 | producer | passed | 0 | 43556864 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=normal`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=1`, `synchronous_name=NORMAL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 20205568 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 20205568 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 19705856 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 5092352 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.01}`.

### mp-p4-c8-payload100000-full

Serializer: `localqueue.JsonSerializer`; padding: `deterministic_sha256_repetition`.

| Process | Role | Status | Exit code | Peak RSS bytes | RSS method | Error type | Error message |
|---|---|---|---:|---:|---|---|---|
| consumer-0 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-1 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-2 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-3 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-4 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-5 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-6 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| consumer-7 | consumer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-0 | producer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-1 | producer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-2 | producer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |
| producer-3 | producer | passed | 0 | 43696128 | resource.getrusage(RUSAGE_SELF).ru_maxrss | n/a | n/a |

SQLite: `busy_timeout_ms=5000`, `durability_mode=full`, `journal_mode=wal`, `page_size=4096`, `sqlite_version=3.46.0`, `synchronous=2`, `synchronous_name=FULL`.

| Phase | File | Exists | Size bytes |
|---|---|---|---:|
| after_close | database | True | 20205568 |
| after_close | shm | False | n/a |
| after_close | wal | False | n/a |
| after_drain | database | True | 20205568 |
| after_drain | shm | True | 32768 |
| after_drain | wal | True | 0 |
| after_producers | database | True | 19304448 |
| after_producers | shm | True | 32768 |
| after_producers | wal | True | 5145912 |
| before_workload | database | False | n/a |
| before_workload | shm | False | n/a |
| before_workload | wal | False | n/a |

Correctness: `True`; ID validation: `{'method': 'exact', 'expected': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'observed': {'count': 200, 'sum': 19900, 'xor': 0, 'sha256': '5691425facbd4d7f883f97d44da90ce0c1105118a8335b2345d9e7a3141df0e9', 'ids': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199]}, 'ok': True}`; stats: `{'ready': 0, 'processing': 0, 'acked': 200, 'failed': 0}`; integrity: `{'schema_version': 1, 'mode': 'full', 'max_errors': 100, 'ok': True, 'messages': ['ok'], 'elapsed_seconds': 0.011}`.

## Limitations

Monotonic timestamps are comparable only between processes on the same host. Percentiles describe deterministic samples, while throughput uses total completed messages. Scheduler, CPU frequency, cache, filesystem, temperature, and virtualization affect results; no performance threshold is applied.
