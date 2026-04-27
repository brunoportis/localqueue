# Changelog

## [0.5.0](https://github.com/brunoportis/localqueue/compare/v0.4.1...v0.5.0) (2026-04-27)


### Features

* **notification:** add in-process notification adapter ([8ae7625](https://github.com/brunoportis/localqueue/commit/8ae762555e196424a1977d10d485d8f20d54f5d9))
* **notifications:** add async push and websocket support ([11b02f4](https://github.com/brunoportis/localqueue/commit/11b02f4808a0d182b18a78750344f3903e377d8f))
* **pubsub:** implement physical fanout for static subscriptions ([63e1589](https://github.com/brunoportis/localqueue/commit/63e158936bbcd818f6d78c094912c0da39b036e6))
* **pubsub:** implement physical fanout for static subscriptions ([a1e8664](https://github.com/brunoportis/localqueue/commit/a1e8664c90cd0ebe473d07403aba279343e48c58))
* **queue:** add at-most-once delivery ([ac277cf](https://github.com/brunoportis/localqueue/commit/ac277cf708c1ddeac225eddf07ddf2b64db28c74))
* **queue:** add backpressure overflow policies ([aec6a47](https://github.com/brunoportis/localqueue/commit/aec6a47c9babb612da00804408097e1340e12154))
* **queue:** add backpressure overflow policies ([c29dbd8](https://github.com/brunoportis/localqueue/commit/c29dbd8f7406d27be7f2c17222a9fda732cd54af))
* **queue:** add best effort ordering ([0a9a4d4](https://github.com/brunoportis/localqueue/commit/0a9a4d47edab47f0f02f46b796dee52e75e888b9))
* **queue:** add best effort ordering ([ebc08ee](https://github.com/brunoportis/localqueue/commit/ebc08ee6631c2c50f8675489a5fa680ed4f1df37))
* **queue:** add commit policies ([0e6ecd9](https://github.com/brunoportis/localqueue/commit/0e6ecd9781fcc438909fb8abd90fe4327db8236f))
* **queue:** add commit policies ([46da89b](https://github.com/brunoportis/localqueue/commit/46da89b6bcdb633227a48a8216c178e289ee99ec))
* **queue:** add consumption policy ([19d4341](https://github.com/brunoportis/localqueue/commit/19d4341fe91b16e23b28cce40974140e3b96e699))
* **queue:** add deduplication policies ([5317af0](https://github.com/brunoportis/localqueue/commit/5317af065e36f7e89a2153ded356cd01b49253b2))
* **queue:** add deduplication policies ([c0af4b6](https://github.com/brunoportis/localqueue/commit/c0af4b6f7ea613d189477fd674385e3c45a5c3ef))
* **queue:** add delivery policy ([6af3c44](https://github.com/brunoportis/localqueue/commit/6af3c44f9c1410f29fd165a14419af2b07e24839))
* **queue:** add dispatch policies ([ece9348](https://github.com/brunoportis/localqueue/commit/ece934870809641e5a22bd9bebc9160c6ac09f94))
* **queue:** add dispatch policies ([3e060c5](https://github.com/brunoportis/localqueue/commit/3e060c5a1dcbc90018a9edf14d5ffa782795ef39))
* **queue:** add effectively-once delivery ([41ca19f](https://github.com/brunoportis/localqueue/commit/41ca19f89a6bc507cec2f60d508ff854929cf12e))
* **queue:** add effectively-once ledger state ([fc2b4f8](https://github.com/brunoportis/localqueue/commit/fc2b4f85fc22335f472236de756c2357c9064227))
* **queue:** add effectively-once ledger state ([181816a](https://github.com/brunoportis/localqueue/commit/181816a7bb92a72af818357f4d4823cd0d3b5a61))
* **queue:** add effectively-once result policy ([26b140a](https://github.com/brunoportis/localqueue/commit/26b140ac50a3078c730fdf994351c0fd5194672b))
* **queue:** add effectively-once result policy ([e6035dd](https://github.com/brunoportis/localqueue/commit/e6035dd05e7fd01b8d20cff0f529d030fda1eaf1))
* **queue:** add explicit policy primitives ([942bbaa](https://github.com/brunoportis/localqueue/commit/942bbaa8af44f300cea9f8072526770798c7287f))
* **queue:** add idempotency stores ([4ab060c](https://github.com/brunoportis/localqueue/commit/4ab060c79a4d0086e3755bc4cde7bb082d0ab839))
* **queue:** add idempotency stores ([8f5bb98](https://github.com/brunoportis/localqueue/commit/8f5bb987d7641ecca0fdaaa294404481c2c7d39a))
* **queue:** add lease policies ([8444d8b](https://github.com/brunoportis/localqueue/commit/8444d8b061f1f5e741ba30eb3d943b2898b9871c))
* **queue:** add lease policies ([a51fe32](https://github.com/brunoportis/localqueue/commit/a51fe32916f3b1575a52c131c26556e9ce394703))
* **queue:** add lifecycle policies ([152bfe2](https://github.com/brunoportis/localqueue/commit/152bfe255eb6c36e76468a6c0a2902ff8cb19d4b))
* **queue:** add lifecycle policies ([3a51aa8](https://github.com/brunoportis/localqueue/commit/3a51aa83987c27451c50765e16914c8828e24337))
* **queue:** add locality policies ([b744058](https://github.com/brunoportis/localqueue/commit/b744058a4e2bc463242ad210b20980636733088e))
* **queue:** add locality policies ([f749c41](https://github.com/brunoportis/localqueue/commit/f749c41c847ae135f7db14437d583d815f7beaa0))
* **queue:** add notification policies ([19a11bb](https://github.com/brunoportis/localqueue/commit/19a11bb1d24ff6bd2ed95f298c99003f622ef4fa))
* **queue:** add notification policies ([7627165](https://github.com/brunoportis/localqueue/commit/762716513702264689535c9c05828b383a10bcf1))
* **queue:** add ordering policy ([dfbd88d](https://github.com/brunoportis/localqueue/commit/dfbd88de1cfd7ff2296a56880ca7c05d7b2d9406))
* **queue:** add policy set presets ([9acb8ee](https://github.com/brunoportis/localqueue/commit/9acb8eed3e7b2bf62d8035c63aef78ebf3f6feb8))
* **queue:** add policy set presets ([fa95020](https://github.com/brunoportis/localqueue/commit/fa950206b877fac9345d2971e39344cc8d511c35))
* **queue:** add priority ordering ([cbf5294](https://github.com/brunoportis/localqueue/commit/cbf5294cf00c00ff0489d9cbab48a4eb7ea49c12))
* **queue:** add publish subscribe routing ([1b54ea3](https://github.com/brunoportis/localqueue/commit/1b54ea3115b70fc6b71ad12b20fbc3defca5f152))
* **queue:** add publish subscribe routing ([5d38e16](https://github.com/brunoportis/localqueue/commit/5d38e167e7d05b45ece7f83ae14b9f032409f37e))
* **queue:** add push consumption policy ([67474fb](https://github.com/brunoportis/localqueue/commit/67474fb16426f3dd9d2e33c87ad703f1d8533c2f))
* **queue:** add push consumption policy ([f76032c](https://github.com/brunoportis/localqueue/commit/f76032c60336ab95b140d253c5ed18e56419636a))
* **queue:** add queue policy sets ([3d10b54](https://github.com/brunoportis/localqueue/commit/3d10b54caf19484e9050f67400920504ca58ad18))
* **queue:** add queue policy sets ([cdd0d27](https://github.com/brunoportis/localqueue/commit/cdd0d2743b24ab2662e6e1661294ba9bc76a7892))
* **queue:** add result stores ([68e853e](https://github.com/brunoportis/localqueue/commit/68e853e930b766d023c67817c11555fd8e69ec97))
* **queue:** add result stores ([37d1498](https://github.com/brunoportis/localqueue/commit/37d1498cc6f4778c54f49630f1b77fd5dcede661))
* **queue:** add routing policy ([981d71d](https://github.com/brunoportis/localqueue/commit/981d71dfd19e70f9a1db186626a86d40116a1386))
* **queue:** add saga commit ([159a477](https://github.com/brunoportis/localqueue/commit/159a47767832a2733b7189a27f8013905a301ab8))
* **queue:** add saga commit ([d936226](https://github.com/brunoportis/localqueue/commit/d9362263e0750c1c68d6127cf862f7e446a8b1d4))
* **queue:** add subscription policies ([97afb69](https://github.com/brunoportis/localqueue/commit/97afb69c6fa73c74905ee2649868bcb567c16abe))
* **queue:** add subscription policies ([fc746a0](https://github.com/brunoportis/localqueue/commit/fc746a02f556f8d59c8abcc8bbfa7f57a6267fc4))
* **queue:** add transactional outbox commit ([3389951](https://github.com/brunoportis/localqueue/commit/33899518aeca21258c7d46784dface62d3602565))
* **queue:** add transactional outbox commit ([e00aac0](https://github.com/brunoportis/localqueue/commit/e00aac060608b64eac034f223b6d5eafc1909817))
* **queue:** add two-phase commit ([5623909](https://github.com/brunoportis/localqueue/commit/56239098a91bc8da474e9dafcfc302b705eb0f26))
* **queue:** add two-phase commit ([e565704](https://github.com/brunoportis/localqueue/commit/e565704a99431953e442090cf82bc43d9ea5bb0a))


### Bug Fixes

* run docker compose examples with writable volumes ([6721861](https://github.com/brunoportis/localqueue/commit/6721861caa9438703c86686bac847a7185f5a78e))
* **workflow:** publish releases only on tag push ([3707b77](https://github.com/brunoportis/localqueue/commit/3707b771424bc09531e2c25ccaef7caf89309417))
* **workflow:** publish releases only on tag push ([fc49104](https://github.com/brunoportis/localqueue/commit/fc491045901715c67c02cf3116dbd8130367bca3))


### Documentation

* add docker compose queue examples ([7930675](https://github.com/brunoportis/localqueue/commit/79306755d687d8e45c81c3dd2e3cdf9a758bfaa4))
* add docker compose queue examples ([2b73f3b](https://github.com/brunoportis/localqueue/commit/2b73f3b2e04a5b642692b659d5c1175302634b88))
* add reproducible docker compose examples ([7921592](https://github.com/brunoportis/localqueue/commit/7921592c1ef57afbe31b120a7c2e4ef148de439b))
* fix use cases nav hierarchy ([45a1230](https://github.com/brunoportis/localqueue/commit/45a12309dc597b4d89edde083e836a29dacca16c))
* **pubsub:** add physical fanout implementation plan ([17d0668](https://github.com/brunoportis/localqueue/commit/17d0668c8969781e56a627ffd6f7cb6c69c4414d))
* **pubsub:** add subscriber queue usage examples ([d5b4c64](https://github.com/brunoportis/localqueue/commit/d5b4c642597e3fa290c75a1911bd5d157a412ff1))
* **queue:** add architecture planning notes ([d034ace](https://github.com/brunoportis/localqueue/commit/d034ace8effab721214b550926444a5d6ef7bedc))
* **queue:** add architecture planning notes ([a46cce5](https://github.com/brunoportis/localqueue/commit/a46cce573a59b1551ff8d80c4be60b5a3e1697a3))
* **queue:** add long term roadmap scope ([8aad4a3](https://github.com/brunoportis/localqueue/commit/8aad4a3fae79f0b56ca667dde80175097d220a3a))
* **queue:** expand architecture roadmap ([bb2964a](https://github.com/brunoportis/localqueue/commit/bb2964a3a11f49f094312c5f8eb0aae227d544ac))
* refine use case navigation and examples ([f5bd751](https://github.com/brunoportis/localqueue/commit/f5bd751c32f2f65d65cee4fb9fa493e4f52f8864))
* simplify use case pages and nav ([3b2b99f](https://github.com/brunoportis/localqueue/commit/3b2b99f9ad6056f8c1ed740bb15ccbcf6213c33c))

## [0.4.1](https://github.com/brunoportis/localqueue/compare/v0.4.0...v0.4.1) (2026-04-25)


### Documentation

* reposition use cases and guides ([8e98379](https://github.com/brunoportis/localqueue/commit/8e98379c29091b414ad437facbdb4d7a7c45db4c))
* reposition use cases and guides ([280288b](https://github.com/brunoportis/localqueue/commit/280288b02f877d78d2df067df67ce14a800cbfe2))
* simplify queue examples ([ed488b2](https://github.com/brunoportis/localqueue/commit/ed488b2ab210369331fbcda3568760f0da24618b))
* split use cases into dedicated pages ([5496826](https://github.com/brunoportis/localqueue/commit/54968266b0733b009147c56761ce9fe29889d5e9))

## [0.4.0](https://github.com/brunoportis/localqueue/compare/v0.3.4...v0.4.0) (2026-04-24)


### Features

* add release-please automation and CLI --version option ([f8db976](https://github.com/brunoportis/localqueue/commit/f8db9767f45fc1eb5055ef0e7eb946aae3af3129))


### Bug Fixes

* **release:** target uv.lock version explicitly ([3e71f44](https://github.com/brunoportis/localqueue/commit/3e71f4450e50de69488b18c04b018a6af00bf962))
* **release:** target uv.lock version explicitly ([31b45ce](https://github.com/brunoportis/localqueue/commit/31b45ce2ba3deea070d8d49c8bebebadcbc9fb37))
* **release:** track uv.lock in release-please ([897b86a](https://github.com/brunoportis/localqueue/commit/897b86aa33d9b38cba82da7b0e2623473488b94d))
* **release:** track uv.lock in release-please ([637ef7f](https://github.com/brunoportis/localqueue/commit/637ef7fca945f1153b643f27b0d6df6ccc39f778))
* **workflow:** configure git identity for release sync ([9872e7a](https://github.com/brunoportis/localqueue/commit/9872e7a85d1382a968f344444009e721da926e50))
* **workflow:** configure git identity for release sync ([2ef15fd](https://github.com/brunoportis/localqueue/commit/2ef15fdd3b5a877569b46980ef795442c1f3089c))
* **workflow:** sync uv.lock in release-please ([f26b1fa](https://github.com/brunoportis/localqueue/commit/f26b1fac2603f5ae0016ce994f5065291b6a6ad0))
* **workflow:** sync uv.lock in release-please ([193b4c6](https://github.com/brunoportis/localqueue/commit/193b4c6d5dba208ec43d7ea6ada402c5c4f1c2a5))


### Documentation

* **use-cases:** simplify CLI examples ([84b92f2](https://github.com/brunoportis/localqueue/commit/84b92f2a420ddeb46dc8db2e6c1493fc45430bff))

## 0.3.4

- Keep `queue exec` and `queue process` alive in `--forever --block` mode when the queue starts empty.
- Preserve the batch empty-queue exit behavior for non-forever workers.

## 0.3.3

- Split queue and retry store implementations into dedicated modules.
- Refactor the CLI worker option wiring and centralize worker helpers.
- Add a SonarCloud workflow and stabilize the SQLite concurrency queue test.

## 0.3.2

- Add a CLI Docker image and publish it to GHCR on version tags.
- Raise the coverage gate to 100% and align the README badge.

## 0.3.1

- Use XDG data directories for default SQLite queue and retry store files.
- Move the exploratory retry demo from `main.py` to `examples/retry_demo.py`.
- Make `PersistentQueue` unfinished-message tracking O(1) by message id.
- Add an end-to-end SQLite worker test covering queue ack and retry cleanup.
- Make `store_path=` for persistent retries open a SQLite attempt store.
- Tighten permanent-failure classification to avoid dead-lettering common
  transient application errors such as `ValueError` and `KeyError`.
- Optimize SQLite queue stats and dead-letter retention queries with
  materialized columns and schema migration support.
- Add indexed SQLite retry-store retention for exhausted records.

## 0.3.0

- Add a concurrent SQLite stress test for multiple producers and consumers.
- Add `examples/sqlite_concurrency_benchmark.py` for measuring local queue throughput.
- Make the SQLite benchmark reset its store and retry transient lock contention.
- Add `examples/sqlite_process_harness.py` for process-level throughput and crash-recovery checks.
- Add process-based stress tests for SQLite producer and consumer coordination.
- Add dead-letter filters and summaries for `queue dead`.
- Add enqueue deduplication with `--dedupe-key` and `dedupe_key=`.
- Add queue-level retry defaults that workers can inherit from `PersistentQueue`.
- Add worker rate limiting and circuit-breaker controls.
- Add usage docs for rate limiting, circuit breaker, and queue positioning.
- Add a short-term maturity note focused on performance and guarantees.

## 0.2.0

- Add `queue stats --watch` for monitoring queue counts while workers run.
- Add `queue dead --watch` for repeatedly listing dead letters.
- Add `queue requeue-dead --all` for bulk recovery after a fix.
- Add structured `command not found` handling for `queue exec`.
- Add `examples/process_webhook.sh` as a shell/curl worker example.

## 0.1.1

- Add project URLs for PyPI metadata.
- Document `queue exec` command-failure fields stored in `last_error`.

## 0.1.0

- Add persistent retry wrappers for sync and async Tenacity retryers.
- Add SQLite-backed durable local queues with leases, delayed delivery,
  acknowledgements, release, dead-letter records, and requeue from dead-letter
  storage. LMDB remains available as an optional backend.
- Add CLI commands for config, queue add/pop/ack/release/dead-letter/stats,
  inspect, dead-letter listing, dead-letter requeue, and continuous processing.
- Add `queue exec` for processing messages with external commands that receive
  the message value as JSON on stdin.
- Add worker identity metadata with `--worker-id` and `leased_by` for inflight
  message inspection.
- Document local-worker operational boundaries, at-least-once delivery, and
  idempotency guidance.
- Add focused examples for enqueueing and processing email jobs locally.
- Add MIT license metadata for package distribution.
- Add `dead_letter_on_failure` as the preferred worker failure policy option,
  keeping `dead_letter_on_exhaustion` as a compatibility alias.
- Enable SQLite WAL journal mode and normal synchronous mode for improved
  concurrency in `SQLiteAttemptStore`.
