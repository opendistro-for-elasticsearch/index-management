## Version 1.10.0.0 (2020-8-18)

Compatible with Elasticsearch 7.9.

### Features
* Implemented allocation action which can be used in index state management [#106](https://github.com/opendistro-for-elasticsearch/index-management/pull/106)

### Enhancements
* Changes implementation of ChangePolicy REST API to use MultiGet inste… [#253](https://github.com/opendistro-for-elasticsearch/index-management/pull/253)

### Bug Fixes
* Fixes snapshot issues, adds history mapping update workflow, adds tests [#255](https://github.com/opendistro-for-elasticsearch/index-management/pull/255)
* Fixes force merge failing on long executions, changes some action mes… [#267](https://github.com/opendistro-for-elasticsearch/index-management/pull/267)

### Infrastructure
* Adds codecov yml file to reduce flakiness in coverage check [#251](https://github.com/opendistro-for-elasticsearch/index-management/pull/251)
* Adds support for multi-node run/testing and updates tests [#254](https://github.com/opendistro-for-elasticsearch/index-management/pull/254)
* Adds multi node test workflow [#256](https://github.com/opendistro-for-elasticsearch/index-management/pull/256)
* release notes automation [#258](https://github.com/opendistro-for-elasticsearch/index-management/pull/258)

### Documentation
* Adds rollup-rfc to docs [#248](https://github.com/opendistro-for-elasticsearch/index-management/pull/248)

### Maintenance
* Adds support for Elasticsearch 7.9 [#283](https://github.com/opendistro-for-elasticsearch/index-management/pull/283)