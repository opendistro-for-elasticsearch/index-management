## Version 1.6.0.0 (2020-03-17)

### New Features
* Adds support for Elasticsearch 7.6.1 [PR #132](https://github.com/opendistro-for-elasticsearch/index-management/pull/132)

#### Testing
* Feature [#374](https://github.com/opendistro-for-elasticsearch/sql/pull/374): Integration test with external ES cluster (issue: [353](https://github.com/opendistro-for-elasticsearch/sql/issues/353))

### Bugfixes
* BugFix [#365](https://github.com/opendistro-for-elasticsearch/sql/pull/365): Return Correct Type Information for Fields (issue: [#316](https://github.com/opendistro-for-elasticsearch/sql/issues/316))
* BugFix [#377](https://github.com/opendistro-for-elasticsearch/sql/pull/377): Return object type for field which has implicit object datatype when describe the table (issue:[sql-jdbc#57](https://github.com/opendistro-for-elasticsearch/sql-jdbc/issues/57))

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
