## Version 1.6.0.0 (2020-3-26)

### New Features
* Adds support for Elasticsearch 7.6.1 [PR #164](https://github.com/opendistro-for-elasticsearch/index-management/pull/164)
* Due to Changes in ES test framework since 7.5
    * Update Jacoco (code coverage) 
    * Update gradle tasks `integTest` and `testClusters`
    * Update debug method and new debug option `cluster.debug`

## Version 1.4.0.0

### New Features
* Adds support for Elasticsearch 7.4.2 [PR #132](https://github.com/opendistro-for-elasticsearch/index-management/pull/132)

### Bug Fixes
* Fixes issue where action timeout was using start_time from previous action [PR #133](https://github.com/opendistro-for-elasticsearch/index-management/pull/133)

## Version 1.3.0.0 (2019-12-17)

### New Features

This is the first official release of Open Distro Index Management plugin.

With Index State Management you will be able to define custom policies, to optimize and manage indices and apply them to index patterns.
Each policy contains a default state and a list of states that you define for the index to transition between.
Within each state you can define a list of actions to perform and transitions to enter a new state based off certain conditions.

Adds backend REST API used for basic CRUD operations, explain, and management of policies and managed indices.
