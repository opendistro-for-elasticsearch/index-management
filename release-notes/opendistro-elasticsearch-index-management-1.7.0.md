
## Version 1.7.0.0 (2020-5-4)

Compatible with Elasticsearch 7.6.1, Adds support for ODFE 1.7.0

### New Features
* Adds rollover conditions into info object [PR #208](https://github.com/opendistro-for-elasticsearch/index-management/pull/208)

### Enhancements
* Adds isIdempotent method to each step and updates ManagedIndexRunner to use it [PR #165](https://github.com/opendistro-for-elasticsearch/index-management/pull/165)

* Adds logs, fix for index creation date -1L, nullable checks [PR #170](https://github.com/opendistro-for-elasticsearch/index-management/pull/170)

* Update schema version for IndexManagementConfig mapping if available [PR #198](https://github.com/opendistro-for-elasticsearch/index-management/pull/198)

* Switches UpdateManagedIndexMetaData to batch tasks using custom executor [PR #209](https://github.com/opendistro-for-elasticsearch/index-management/pull/209)

### Bug Fixes
* Delete and close failing during snapshot in progress [PR #172](https://github.com/opendistro-for-elasticsearch/index-management/pull/172)