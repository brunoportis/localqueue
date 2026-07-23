# Changelog

All notable changes to `localqueue` are documented here.

<!-- version list -->

## v1.2.0 (2026-07-23)

### Bug Fixes

- Allow one fenced transition retry ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Audit release dependencies against complete Git history
  ([#53](https://github.com/brunoportis/localqueue/pull/53),
  [`3c62f28`](https://github.com/brunoportis/localqueue/commit/3c62f28234f8d640a5550bf38187cd794083cc0b))

- Bound SQLite soak retries by native call time
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Classify transient SQLite contention in soak
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Close backpressure correctness gaps ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Close benchmark report edge cases ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Complete benchmark evidence and bounded sampling
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Coordinate EventBus subscription runners
  ([#45](https://github.com/brunoportis/localqueue/pull/45),
  [`36415d2`](https://github.com/brunoportis/localqueue/commit/36415d29fabc2836e6b4aec427a39b816aa8d1c8))

- Enforce multiprocess correctness protocol
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Harden benchmark execution and metadata ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Harden crash harness validation paths ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Harden maintenance backup contract ([#38](https://github.com/brunoportis/localqueue/pull/38),
  [`872318a`](https://github.com/brunoportis/localqueue/commit/872318ac132a66a64bbe80c56f290a54f53b9114))

- Harden multiprocess benchmark execution ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Install benchmark dependencies for candidate wheel evidence
  ([#52](https://github.com/brunoportis/localqueue/pull/52),
  [`8f931e0`](https://github.com/brunoportis/localqueue/commit/8f931e011ca4c549a8ddc6d5133c1fc4f03986e3))

- Invoke explicit interpreters with pwsh ([#50](https://github.com/brunoportis/localqueue/pull/50),
  [`d1ef5c2`](https://github.com/brunoportis/localqueue/commit/d1ef5c22b721a0360465fafc6ce546796638f8bc))

- Isolate backup failure test configuration
  ([#41](https://github.com/brunoportis/localqueue/pull/41),
  [`71f5476`](https://github.com/brunoportis/localqueue/commit/71f5476161063c1cf4a769e744301914367a4a3e))

- Isolate benchmark workload timing and failures
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Kill chaos children before releasing failpoints
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Make chaos evidence connection-specific ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Make focused lease loss scenario ack-only
  ([#54](https://github.com/brunoportis/localqueue/pull/54),
  [`a64a6a6`](https://github.com/brunoportis/localqueue/commit/a64a6a67a567d6df5c42f7b863a54a91bf451752))

- Make multiprocess completion and timing deterministic
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Make multiprocess soak handle lease loss deterministically
  ([#54](https://github.com/brunoportis/localqueue/pull/54),
  [`a64a6a6`](https://github.com/brunoportis/localqueue/commit/a64a6a67a567d6df5c42f7b863a54a91bf451752))

- Make soak counters crash-safe ([#54](https://github.com/brunoportis/localqueue/pull/54),
  [`a64a6a6`](https://github.com/brunoportis/localqueue/commit/a64a6a67a567d6df5c42f7b863a54a91bf451752))

- Make wheel contract path portable ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Normalize diagnostic lease duration ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

- Normalize sanitized worker paths ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Preserve async handler timeout semantics
  ([#46](https://github.com/brunoportis/localqueue/pull/46),
  [`0e14146`](https://github.com/brunoportis/localqueue/commit/0e141466e5994e7fdc42a8eb6434dcc54cc45c30))

- Preserve SQLite busy error semantics ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Prioritize external timeout cleanup cancellation
  ([#46](https://github.com/brunoportis/localqueue/pull/46),
  [`0e14146`](https://github.com/brunoportis/localqueue/commit/0e141466e5994e7fdc42a8eb6434dcc54cc45c30))

- Quote soak job-id validation ([#57](https://github.com/brunoportis/localqueue/pull/57),
  [`2e874bc`](https://github.com/brunoportis/localqueue/commit/2e874bc9024d909dba891d0cf1b8f891635cfb30))

- Render typed multiprocess reports ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Repair wheels promotion audit syntax ([#58](https://github.com/brunoportis/localqueue/pull/58),
  [`d249048`](https://github.com/brunoportis/localqueue/commit/d249048969ecb20a6bba2a2a3b496f8ad022b495))

- Resolve release interpreters portably ([#50](https://github.com/brunoportis/localqueue/pull/50),
  [`d1ef5c2`](https://github.com/brunoportis/localqueue/commit/d1ef5c22b721a0360465fafc6ce546796638f8bc))

- Retry producer attempts during SQLite contention
  ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Retry SQLite contention transitions once
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Retry transient SQLite contention in multiprocess soak
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Retry transient SQLite contention in soak
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Retry transient SQLite contention in soak producers
  ([#57](https://github.com/brunoportis/localqueue/pull/57),
  [`2e874bc`](https://github.com/brunoportis/localqueue/commit/2e874bc9024d909dba891d0cf1b8f891635cfb30))

- Satisfy strict benchmark type checks ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Simplify diagnostics aggregate typing ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Stabilize diagnostics snapshot boundaries
  ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

- Use container Python for manylinux wheels
  ([#50](https://github.com/brunoportis/localqueue/pull/50),
  [`d1ef5c2`](https://github.com/brunoportis/localqueue/commit/d1ef5c22b721a0360465fafc6ce546796638f8bc))

- Validate candidate ARM64 wheel artifacts reliably
  ([#50](https://github.com/brunoportis/localqueue/pull/50),
  [`d1ef5c2`](https://github.com/brunoportis/localqueue/commit/d1ef5c22b721a0360465fafc6ce546796638f8bc))

### Chores

- Enable strict pyrefly checks ([#33](https://github.com/brunoportis/localqueue/pull/33),
  [`93e3763`](https://github.com/brunoportis/localqueue/commit/93e37638ce386925b6175f7ad1c5c18872b518d9))

- Sync lockfile with release version
  ([`4d69624`](https://github.com/brunoportis/localqueue/commit/4d69624056b24a562afd5368389f2f99085cc6e3))

### Code Style

- Format backpressure polling ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Stabilize native exception import order ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

### Continuous Integration

- Add staged release evidence and promotion gate
  ([#49](https://github.com/brunoportis/localqueue/pull/49),
  [`b191eb5`](https://github.com/brunoportis/localqueue/commit/b191eb5fe73b382baec23af2463c3e29d1e72ddf))

- Create virtualenv for crash harness ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Dispatch wheels after semantic release
  ([`39e2d6d`](https://github.com/brunoportis/localqueue/commit/39e2d6d06542e32cef0ebaa7cb1590dcd7d444f0))

- Exercise SQLite contention retry in soak
  ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Gate wheel benchmark smoke ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Preserve operational chaos diagnostics ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Provide GitHub token to Gitleaks ([#13](https://github.com/brunoportis/localqueue/pull/13),
  [`33da141`](https://github.com/brunoportis/localqueue/commit/33da141ae870d8826c94061227cb8fe0d1a69d40))

### Documentation

- Add security policy ([#13](https://github.com/brunoportis/localqueue/pull/13),
  [`33da141`](https://github.com/brunoportis/localqueue/commit/33da141ae870d8826c94061227cb8fe0d1a69d40))

- Apply Ruff 0.16 formatting ([#59](https://github.com/brunoportis/localqueue/pull/59),
  [`dc0136a`](https://github.com/brunoportis/localqueue/commit/dc0136acc05a83c5ba7f76063b8da9a80a7fd7b3))

- Clarify product-level chaos evidence ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Document canonical benchmark reports ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Document logical backlog limits ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Document runtime diagnostics contract ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

- Explain event causality chains ([#44](https://github.com/brunoportis/localqueue/pull/44),
  [`4ba0a17`](https://github.com/brunoportis/localqueue/commit/4ba0a17d70d921c3e44543d8e364e2274f5bffb8))

- Link final 1.1.2 release evidence
  ([`7fdde7b`](https://github.com/brunoportis/localqueue/commit/7fdde7b7b50cbeddd99ef198c3b5e05f0ebd61bb))

- Plan operational chaos campaign ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Publish the supported operational envelope
  ([#48](https://github.com/brunoportis/localqueue/pull/48),
  [`1ffd480`](https://github.com/brunoportis/localqueue/commit/1ffd480448e0ac1f416f6b86525825dacf20dba5))

- Refine operational envelope evidence ([#48](https://github.com/brunoportis/localqueue/pull/48),
  [`1ffd480`](https://github.com/brunoportis/localqueue/commit/1ffd480448e0ac1f416f6b86525825dacf20dba5))

### Features

- Add async EventBus handler timeouts ([#46](https://github.com/brunoportis/localqueue/pull/46),
  [`0e14146`](https://github.com/brunoportis/localqueue/commit/0e141466e5994e7fdc42a8eb6434dcc54cc45c30))

- Add atomic queue backpressure ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Add bounded EventBus subscription concurrency
  ([#45](https://github.com/brunoportis/localqueue/pull/45),
  [`36415d2`](https://github.com/brunoportis/localqueue/commit/36415d29fabc2836e6b4aec427a39b816aa8d1c8))

- Add canonical single-process benchmark harness
  ([#42](https://github.com/brunoportis/localqueue/pull/42),
  [`1199a5c`](https://github.com/brunoportis/localqueue/commit/1199a5c63f7a452f4e9536d5914e64273e770a7d))

- Add dedicated multiprocess correctness coverage
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Add event causality metadata ([#44](https://github.com/brunoportis/localqueue/pull/44),
  [`4ba0a17`](https://github.com/brunoportis/localqueue/commit/4ba0a17d70d921c3e44543d8e364e2274f5bffb8))

- Add event correlation and causation metadata
  ([#44](https://github.com/brunoportis/localqueue/pull/44),
  [`4ba0a17`](https://github.com/brunoportis/localqueue/commit/4ba0a17d70d921c3e44543d8e364e2274f5bffb8))

- Add integrity checks and online backups ([#38](https://github.com/brunoportis/localqueue/pull/38),
  [`872318a`](https://github.com/brunoportis/localqueue/commit/872318ac132a66a64bbe80c56f290a54f53b9114))

- Add large database benchmark and evidence models
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Add logical queue backpressure ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Add multiprocess benchmark profiles ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Add native diagnostics snapshot ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

- Add spawn multiprocess benchmark profiles
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

- Add typed runtime diagnostics API ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

- Expose immutable queue diagnostics ([#37](https://github.com/brunoportis/localqueue/pull/37),
  [`250978d`](https://github.com/brunoportis/localqueue/commit/250978d74367494a5f121c2c107a3cae41df656e))

### Refactoring

- Exercise public queue API in chaos scenarios
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Route multiprocess CLI through typed runner
  ([#43](https://github.com/brunoportis/localqueue/pull/43),
  [`ddde9b7`](https://github.com/brunoportis/localqueue/commit/ddde9b7ab69d69666489cea659b7bd0d1427ab47))

### Testing

- Add deterministic crash recovery harness
  ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Add deterministic operational chaos campaign
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Add deterministic SQLite soak reports ([#56](https://github.com/brunoportis/localqueue/pull/56),
  [`49b6fcf`](https://github.com/brunoportis/localqueue/commit/49b6fcf085e2a3e989b383c6358e2a09abb14698))

- Add deterministic storage crash harness ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Add deterministic storage transaction failpoints
  ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Add model-based queue state machine ([#34](https://github.com/brunoportis/localqueue/pull/34),
  [`79d415f`](https://github.com/brunoportis/localqueue/commit/79d415f07c57f4c4ab3d3898af41fe5e95dbf7e9))

- Add release storage compatibility matrix
  ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))

- Close corruption fixture before validation
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Expand deterministic operational chaos campaign
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Harden EventBus concurrency contracts ([#45](https://github.com/brunoportis/localqueue/pull/45),
  [`36415d2`](https://github.com/brunoportis/localqueue/commit/36415d29fabc2836e6b4aec427a39b816aa8d1c8))

- Isolate delayed retry timing ([#34](https://github.com/brunoportis/localqueue/pull/34),
  [`79d415f`](https://github.com/brunoportis/localqueue/commit/79d415f07c57f4c4ab3d3898af41fe5e95dbf7e9))

- Isolate state machine lease timing ([#34](https://github.com/brunoportis/localqueue/pull/34),
  [`79d415f`](https://github.com/brunoportis/localqueue/commit/79d415f07c57f4c4ab3d3898af41fe5e95dbf7e9))

- Keep chaos contract portable across platforms
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Keep compatibility tests importable on Python 3.10
  ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))

- Keep multiprocess soak portable on Windows
  ([#57](https://github.com/brunoportis/localqueue/pull/57),
  [`2e874bc`](https://github.com/brunoportis/localqueue/commit/2e874bc9024d909dba891d0cf1b8f891635cfb30))

- Lock eventbus dependencies and harden evidence
  ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))

- Make idle-reader concurrency check deterministic
  ([#51](https://github.com/brunoportis/localqueue/pull/51),
  [`ee8cee4`](https://github.com/brunoportis/localqueue/commit/ee8cee43257ce0c905b22cf5f254b55cb88f6764))

- Make purge and offline cache deterministic
  ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))

- Make reclaimed-job lease contract deterministic
  ([#55](https://github.com/brunoportis/localqueue/pull/55),
  [`dc67cd0`](https://github.com/brunoportis/localqueue/commit/dc67cd062c7821699e39c9e62b39e4b351dc0cf5))

- Reject incomplete chaos evidence ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Satisfy release dependency audit quality checks
  ([#53](https://github.com/brunoportis/localqueue/pull/53),
  [`3c62f28`](https://github.com/brunoportis/localqueue/commit/3c62f28234f8d640a5550bf38187cd794083cc0b))

- Scope receipt recovery to crash scenarios
  ([#35](https://github.com/brunoportis/localqueue/pull/35),
  [`8891e80`](https://github.com/brunoportis/localqueue/commit/8891e8050268f3f8bc85158926e1fd2f38562af3))

- Sequence native backpressure cases ([#40](https://github.com/brunoportis/localqueue/pull/40),
  [`7cae2f7`](https://github.com/brunoportis/localqueue/commit/7cae2f733409f2d22cb0905fe4de443ed0ec3ef9))

- Stabilize historical processing lease ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))

- Verify operational retries after recovery
  ([#36](https://github.com/brunoportis/localqueue/pull/36),
  [`491910e`](https://github.com/brunoportis/localqueue/commit/491910eccf294696274b003c4e2f659cf6a2d6ac))

- Wait for deterministic lease expiry ([#47](https://github.com/brunoportis/localqueue/pull/47),
  [`46c51c9`](https://github.com/brunoportis/localqueue/commit/46c51c9218cd2ee8cc84d575f8ff635d2dd2b8da))


## v1.1.2 (2026-07-21)

### Bug Fixes

- Clarify quickstart execution order
  ([`69bd317`](https://github.com/brunoportis/localqueue/commit/69bd31778640d25a47b52e36565448c0c1b341ad))

### Continuous Integration

- Pin GitHub Actions to immutable revisions
  ([`752ebed`](https://github.com/brunoportis/localqueue/commit/752ebed2709d64089a62c7bd97734d30a4ca6eb8))

- Report no-op semantic releases clearly
  ([`395af15`](https://github.com/brunoportis/localqueue/commit/395af155463d53cc3a34cef06a982c17439e8081))

- Validate release wheels without virtualenv
  ([`4cfd63e`](https://github.com/brunoportis/localqueue/commit/4cfd63e2ef9703a56f84037c14e6c016fa9a2105))

### Documentation

- Fix quickstart code fence
  ([`88f22a4`](https://github.com/brunoportis/localqueue/commit/88f22a4a6b2af0f6547ab083c836ed7fe08ee3fa))

- Record automation rollout status
  ([`9b406ea`](https://github.com/brunoportis/localqueue/commit/9b406ea814d44061ce03ae716dafa0f0e219eff7))

- Record release validation evidence
  ([`c713eff`](https://github.com/brunoportis/localqueue/commit/c713eff7e7704bc0f046c951a26c502b56e0bfb0))


## [1.1.1] - 2026-07-21

### Added

- Zensical documentation site with a durable, two-process getting-started
  guide and GitHub Pages deployment workflow.
- Python branch-coverage reporting, Ruff, Pyrefly, pre-commit, Gitleaks, and
  cargo-deny quality gates.
- Manual Conventional Commits release workflow with Python, Cargo, lockfile,
  tag, and wheel-release consistency checks.

### Changed

- The README quickstart now demonstrates a producer persisting a job before a
  later worker process consumes it.
- CI caches Rust build outputs and uses debug builds for test jobs while wheel
  builds remain optimized release builds.

## [1.1.0] - 2026-07-21

### Added

- `SimpleQueue.put_many(...)` for atomic batch enqueueing, with optional
  per-item deduplication through `EnqueueItem`.
- The optional `localqueue.bus` package, exposing `BaseEvent` and `EventBus`
  when installed with `localqueue[bus]`.
- Atomic event fan-out to durable subscriptions, wildcard handlers, consumer
  groups across processes, lease heartbeats, retries, and dead-letter handling.
- Fan-out benchmarks and multiprocess coverage for the event bus.
- Event routing now uses an explicit static `BusTopology`, allowing producers
  and consumers to run in separate processes without producers importing
  consumer handlers.
- Handler registration is separate from subscription declaration through
  `bus.subscription(...).handler(...)`.

### Changed

- Blocking queue operations and synchronous event handlers are moved off the
  asyncio event loop.
- Idle polling avoids repeated SQLite writer transactions while queues are
  empty, reducing lock contention across many idle subscriptions.
- Public docstrings, log messages, validation errors, and native queue errors
  are now consistently written in English.
- Release wheels cover CPython 3.10 through 3.14 on Linux x86-64/aarch64,
  macOS x86-64/arm64, and Windows x86-64.

### Fixed

- Reject worker heartbeat intervals that are not shorter than the queue lease,
  non-positive lease extensions, and negative NACK delays.

### Compatibility

- Existing `SimpleQueue` and `Worker` APIs remain backward compatible.
- Event bus support is optional and requires Pydantic 2 via the `bus` extra.

## Migrating from 0.5.0

Version 1.0.0 was a complete, backward-incompatible reimplementation of the
legacy `localqueue` package. Upgrading from 0.5.0 is not an in-place library or
storage migration:

- Replace `PersistentQueue` with `SimpleQueue` and adapt producers and
  consumers to the explicit `put` → `get` → `ack`/`nack` lifecycle.
- The Tenacity-backed persistent retry API and the `cli`, `lmdb`, and `sqlite`
  extras from 0.5.0 are not part of 1.x. Version 1.x uses its bundled SQLite
  engine and Rust extension; install `localqueue[bus]` only when the event bus
  is needed.
- Do not reuse a 0.5.0 database with 1.x. Drain or export pending work with
  0.5.0 first, create a new 1.x queue directory, and enqueue any work that must
  be retained.
- Imports, configuration, storage layout, and operational commands from 0.5.0
  are not guaranteed to work in 1.x. Test the migration in a separate virtual
  environment before replacing a production worker.
- Version 1.x supports Python 3.10 and newer and is licensed under Apache-2.0;
  version 0.5.0 required Python 3.11 and used the MIT license.

## [1.0.1] - 2026-07-20

### Changed

- Expanded the public README with installation, delivery guarantees,
  configuration, API, architecture, and development guidance.
- Declared and validated support for Python 3.10 through 3.14 across Linux,
  macOS, and Windows wheels.

## [1.0.0] - 2026-07-20

### Changed

- Reimplemented `localqueue` around `SimpleQueue`, a bundled SQLite database,
  and a native Rust extension.
- Replaced the legacy 0.5.0 API and storage model with explicit ACK/NACK,
  leases, bounded retries, dead-letter handling, receipt fencing, and
  multiprocess safety on one machine.

[1.1.1]: https://github.com/brunoportis/localqueue/compare/v1.1.0...v1.1.1
[1.1.0]: https://github.com/brunoportis/localqueue/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/brunoportis/localqueue/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/brunoportis/localqueue/releases/tag/v1.0.0
