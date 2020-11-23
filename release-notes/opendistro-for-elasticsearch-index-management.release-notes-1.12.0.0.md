## Version 1.11.0.0 2020-10-19

Compatible with Elasticsearch 7.10.0

### Features

* Adds support for Rollup feature ([#319](https://github.com/opendistro-for-elasticsearch/index-management/pull/319), [#320](https://github.com/opendistro-for-elasticsearch/index-management/pull/320), [#321](https://github.com/opendistro-for-elasticsearch/index-management/pull/321), [#322](https://github.com/opendistro-for-elasticsearch/index-management/pull/322), [#323](https://github.com/opendistro-for-elasticsearch/index-management/pull/323), [#324](https://github.com/opendistro-for-elasticsearch/index-management/pull/324), [#336](https://github.com/opendistro-for-elasticsearch/index-management/pull/336), [#337](https://github.com/opendistro-for-elasticsearch/index-management/pull/337), [#338](https://github.com/opendistro-for-elasticsearch/index-management/pull/338), [#339](https://github.com/opendistro-for-elasticsearch/index-management/pull/339), [#340](https://github.com/opendistro-for-elasticsearch/index-management/pull/340), [#341](https://github.com/opendistro-for-elasticsearch/index-management/pull/341), [#342](https://github.com/opendistro-for-elasticsearch/index-management/pull/342), [#343](https://github.com/opendistro-for-elasticsearch/index-management/pull/343), [#344](https://github.com/opendistro-for-elasticsearch/index-management/pull/344), [#345](https://github.com/opendistro-for-elasticsearch/index-management/pull/345), [#346](https://github.com/opendistro-for-elasticsearch/index-management/pull/346), [#347](https://github.com/opendistro-for-elasticsearch/index-management/pull/347), [#348](https://github.com/opendistro-for-elasticsearch/index-management/pull/348))

### Enhancements

* Adds support for Elasticsearch 7.10.0 ([#349](https://github.com/opendistro-for-elasticsearch/index-management/pull/349))

### Maintenance

* Uploads elasticsearch.log files from failed CI runs ([#336](https://github.com/opendistro-for-elasticsearch/index-management/pull/336))
* Adds support for running local cluster with security plugin enabled ([#322](https://github.com/opendistro-for-elasticsearch/index-management/pull/322))
* Updates integration tests to not wipe indices between each test to help reduce tests bleeding into each other ([#342](https://github.com/opendistro-for-elasticsearch/index-management/pull/342))
* Changes set-env command in github workflow (* Adds support for Elasticsearch 7.10.0 ([#349](https://github.com/opendistro-for-elasticsearch/index-management/pull/349)))

### Bug fixes

* Correctly handles remote transport exceptions in rollover ([#325](https://github.com/opendistro-for-elasticsearch/index-management/pull/325)) 