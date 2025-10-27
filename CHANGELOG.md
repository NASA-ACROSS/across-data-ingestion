# Changelog

## [1.0.0](https://github.com/ACROSS-Team/across-data-ingestion/compare/v0.2.0...v1.0.0) (2025-10-27)


### ⚠ BREAKING CHANGES

* release v1

### Features

* release for v1 ([#101](https://github.com/ACROSS-Team/across-data-ingestion/issues/101)) ([72e3044](https://github.com/ACROSS-Team/across-data-ingestion/commit/72e3044fad5ea8fffe19db59f9499ac21412821e))
* release v1 ([2cb471c](https://github.com/ACROSS-Team/across-data-ingestion/commit/2cb471c9e6ebe1850eadb3ea408af48fce568cd4))

## [0.2.0](https://github.com/ACROSS-Team/across-data-ingestion/compare/v0.1.2...v0.2.0) (2025-10-20)


### Features

* add task to ingest XMM-Newton planned schedules ([#66](https://github.com/ACROSS-Team/across-data-ingestion/issues/66)) ([204c340](https://github.com/ACROSS-Team/across-data-ingestion/commit/204c340043b39c6644f0e8ef7c628fbfc46652b1))
* adding an ingestion layer for JWST ([#64](https://github.com/ACROSS-Team/across-data-ingestion/issues/64)) ([33f36f7](https://github.com/ACROSS-Team/across-data-ingestion/commit/33f36f774969b2d0658d90de4eb845a729531d24))
* **TLE:** add spacetrack config, load user/pwd from aws ssm in deployed env ([#96](https://github.com/ACROSS-Team/across-data-ingestion/issues/96)) ([cc1f753](https://github.com/ACROSS-Team/across-data-ingestion/commit/cc1f75354b706d502e7521d89c8e530ad8b0443c))


### Bug Fixes

* add http/https if missing, set check_server cron to every 30 mins ([#91](https://github.com/ACROSS-Team/across-data-ingestion/issues/91)) ([961e495](https://github.com/ACROSS-Team/across-data-ingestion/commit/961e49575fb5a7abb0c38714b5df6e452d9c12c2))
* log warning for 409 duplicate TLE response ([#95](https://github.com/ACROSS-Team/across-data-ingestion/issues/95)) ([88688ee](https://github.com/ACROSS-Team/across-data-ingestion/commit/88688eed5d95c097361ceff33ea54d93a00ecf43))
* **release:** release-please needs ci ([#87](https://github.com/ACROSS-Team/across-data-ingestion/issues/87)) ([cffb549](https://github.com/ACROSS-Team/across-data-ingestion/commit/cffb5491130c98bd14e3cab311496cd7d942972d))
* set seeded service account client credentials when running locally ([#90](https://github.com/ACROSS-Team/across-data-ingestion/issues/90)) ([7e032d9](https://github.com/ACROSS-Team/across-data-ingestion/commit/7e032d90406b23fdfb847c0be9c495bc9f02a6aa))
* tle config properly sets space-track credentials ([#98](https://github.com/ACROSS-Team/across-data-ingestion/issues/98)) ([4e41a34](https://github.com/ACROSS-Team/across-data-ingestion/commit/4e41a344c038c2781164bbd1511ffb1dbe527d63))
* update dep versions for tools and sdk ([6eb3024](https://github.com/ACROSS-Team/across-data-ingestion/commit/6eb30248e0d71cd528a84d38afd1b422e92cbcd5))
* update sdk version ([b46f329](https://github.com/ACROSS-Team/across-data-ingestion/commit/b46f329f3143f1fcd30d5f7539bba4fed5e166d2))
* use TelescopeInstrument schema ([#93](https://github.com/ACROSS-Team/across-data-ingestion/issues/93)) ([c86dadc](https://github.com/ACROSS-Team/across-data-ingestion/commit/c86dadc94fd1f5f7021cd106167dc4c4cb7283c2))

## [0.1.2](https://github.com/ACROSS-Team/across-data-ingestion/compare/v0.1.1...v0.1.2) (2025-09-30)


### Bug Fixes

* **release:** build container to be tagged and promoted with release version ([#85](https://github.com/ACROSS-Team/across-data-ingestion/issues/85)) ([4c87e1c](https://github.com/ACROSS-Team/across-data-ingestion/commit/4c87e1cd2dc02a606500396c772f3106cf06220f))

## [0.1.1](https://github.com/ACROSS-Team/across-data-ingestion/compare/v0.1.0...v0.1.1) (2025-09-30)


### Bug Fixes

* release-please PR merge triggers release-please action ([#83](https://github.com/ACROSS-Team/across-data-ingestion/issues/83)) ([49b3ba5](https://github.com/ACROSS-Team/across-data-ingestion/commit/49b3ba500fee7ebdece6b9184491167133963ac0))

## 0.1.0 (2025-09-30)


### Features

* add across_server sdk util ([#70](https://github.com/ACROSS-Team/across-data-ingestion/issues/70)) ([76c60aa](https://github.com/ACROSS-Team/across-data-ingestion/commit/76c60aaaf4c0babac1701a8eeece14f2346d7e2c))
* add cicd GHA workflows ([#75](https://github.com/ACROSS-Team/across-data-ingestion/issues/75)) ([5811fd4](https://github.com/ACROSS-Team/across-data-ingestion/commit/5811fd48c1f2ff2c09042be911db00c64ecfde05))
* add fermi LAT planned schedule ingestion task ([#25](https://github.com/ACROSS-Team/across-data-ingestion/issues/25)) ([2977d68](https://github.com/ACROSS-Team/across-data-ingestion/commit/2977d68b3eda0e8786a004d600a48e6c1974f5a7))
* add HST planned schedule ingestion task ([#57](https://github.com/ACROSS-Team/across-data-ingestion/issues/57)) ([e9210d7](https://github.com/ACROSS-Team/across-data-ingestion/commit/e9210d78c367c07548e761cc939047c3692ee9ea))
* add ixpe low-fidelity planned schedule ingestion task ([#50](https://github.com/ACROSS-Team/across-data-ingestion/issues/50)) ([97e3120](https://github.com/ACROSS-Team/across-data-ingestion/commit/97e31206b994bec61c1b54a9fbc02205d2f84197))
* add nicer low-fidelity planned schedule ingestion task ([#46](https://github.com/ACROSS-Team/across-data-ingestion/issues/46)) ([1dc8eb5](https://github.com/ACROSS-Team/across-data-ingestion/commit/1dc8eb51c6d7e58d435125758927b5bc51d3e90f))
* add nustar as-flown schedule ingestion task ([#35](https://github.com/ACROSS-Team/across-data-ingestion/issues/35)) ([75a112b](https://github.com/ACROSS-Team/across-data-ingestion/commit/75a112b161f074fb7363406b9f799803b0fe27dd))
* add nustar low fidelity planned schedule ingestion task ([#78](https://github.com/ACROSS-Team/across-data-ingestion/issues/78)) ([a2329cd](https://github.com/ACROSS-Team/across-data-ingestion/commit/a2329cd7ab80565008e6a7914fe86b644ce51639))
* add swift data ingestion layer ([0939685](https://github.com/ACROSS-Team/across-data-ingestion/commit/093968591ed5b977de17025a623deeb101eadd51))
* add swift data ingestion layer ([0939685](https://github.com/ACROSS-Team/across-data-ingestion/commit/093968591ed5b977de17025a623deeb101eadd51))
* add task to ingest TLEs for observatories with TLE ephemerides ([#52](https://github.com/ACROSS-Team/across-data-ingestion/issues/52)) ([0ceb106](https://github.com/ACROSS-Team/across-data-ingestion/commit/0ceb10684af20116ba23baaf2e89eefa35695224))
* **example:** create server repo and example repeated task ([#4](https://github.com/ACROSS-Team/across-data-ingestion/issues/4)) ([da0dac2](https://github.com/ACROSS-Team/across-data-ingestion/commit/da0dac23f90ff52cf944607d71dcd51c0cba8879))
* **logging:** Port `structlog` and `correlation_id` from `across-server` ([#24](https://github.com/ACROSS-Team/across-data-ingestion/issues/24)) ([89cb69c](https://github.com/ACROSS-Team/across-data-ingestion/commit/89cb69c10ceb3c973205f249e85d9f9b1865b221))
* **schedules:** add chandra high fidelity planned schedule ingestion task ([#49](https://github.com/ACROSS-Team/across-data-ingestion/issues/49)) ([37f89f6](https://github.com/ACROSS-Team/across-data-ingestion/commit/37f89f68a57ce993d1362b0d710107b24dd163f9))
* **tasks:** add ACROSS server api hooks to post schedules ([#21](https://github.com/ACROSS-Team/across-data-ingestion/issues/21)) ([9d82734](https://github.com/ACROSS-Team/across-data-ingestion/commit/9d827346e41cbbdede7a0026a97c0fa1bdafbcb1))
* **tasks:** use repeat_at decorator for cron functionality ([#59](https://github.com/ACROSS-Team/across-data-ingestion/issues/59)) ([17b93c2](https://github.com/ACROSS-Team/across-data-ingestion/commit/17b93c26a340895e2b94519b2bfda28ad0955164))
* **tess:** adds low fidelity planned schedule ingestion for TESS sector pointings and orbits ([#8](https://github.com/ACROSS-Team/across-data-ingestion/issues/8)) ([10bd3f6](https://github.com/ACROSS-Team/across-data-ingestion/commit/10bd3f6849193f02ad691e10510bac4c8702085b))
* use across server openapi sdk, refactor for clarity, optimization ([#67](https://github.com/ACROSS-Team/across-data-ingestion/issues/67)) ([cf28ff1](https://github.com/ACROSS-Team/across-data-ingestion/commit/cf28ff11b623d714c398b8a4ae4a768b11900c21))
* use pre-determined crons from server ([#79](https://github.com/ACROSS-Team/across-data-ingestion/issues/79)) ([dbf28a1](https://github.com/ACROSS-Team/across-data-ingestion/commit/dbf28a12058b45d6b83f61e7d3f00b4d4bbf87ef))


### Bug Fixes

* **build:** Fix `mypy` path in Makefile ([#6](https://github.com/ACROSS-Team/across-data-ingestion/issues/6)) ([a00ad28](https://github.com/ACROSS-Team/across-data-ingestion/commit/a00ad28607cd17b19178ec878730831f0b644494))
* **deploy:** optionally emit web url ([#81](https://github.com/ACROSS-Team/across-data-ingestion/issues/81)) ([0f79592](https://github.com/ACROSS-Team/across-data-ingestion/commit/0f7959249b464b883b989ee35fb857bdb29b4b14))
* Fix import ([89cb69c](https://github.com/ACROSS-Team/across-data-ingestion/commit/89cb69c10ceb3c973205f249e85d9f9b1865b221))
* Remove unneeded dep ([89cb69c](https://github.com/ACROSS-Team/across-data-ingestion/commit/89cb69c10ceb3c973205f249e85d9f9b1865b221))
* Revert imports ([89cb69c](https://github.com/ACROSS-Team/across-data-ingestion/commit/89cb69c10ceb3c973205f249e85d9f9b1865b221))
* TESS schedule ingestion needs bandpass unit ([#29](https://github.com/ACROSS-Team/across-data-ingestion/issues/29)) ([a0822d7](https://github.com/ACROSS-Team/across-data-ingestion/commit/a0822d7f76feda4da93918131d6f21173ee2080f))
* use across data ingestion, not across server ([cbaba83](https://github.com/ACROSS-Team/across-data-ingestion/commit/cbaba83f82f39688c996279bb2fdf54aef4d3dc9))


### Documentation

* rename bug.md to bug.yaml ([e4854be](https://github.com/ACROSS-Team/across-data-ingestion/commit/e4854be1a194fb2bc57c7a97e81574ac95c31038))
* rename spike.md to spike.yaml ([47792bc](https://github.com/ACROSS-Team/across-data-ingestion/commit/47792bc373ed1a61f76d4e8a59a7b1b967a8efa7))
* rename ticket.md to ticket.yaml ([7479143](https://github.com/ACROSS-Team/across-data-ingestion/commit/7479143492a9cfacb78a17255e06df62008ffe71))
* update bug.md to use yaml syntax ([96d9a1c](https://github.com/ACROSS-Team/across-data-ingestion/commit/96d9a1ca346efc28e8beff54b16571ab5daf0daa))
* update spike.md template to follow yaml syntax ([517fe0d](https://github.com/ACROSS-Team/across-data-ingestion/commit/517fe0d2645f901f81604efc06d6cf5f90377d79))
* update ticket.md template to follow yaml syntax ([a8f6475](https://github.com/ACROSS-Team/across-data-ingestion/commit/a8f6475e74147134ea3d71a51bbda1e0fda75802))
