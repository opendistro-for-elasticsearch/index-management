## Version 1.3.0.2 (Current)

### Bug Fixes
 * Fixes issue where action timeout was using start_time from previous action (#133)
 * Fixes history index iteration breaking instead of continuing (#134)

## 2019-12-17, Version 1.3.0.1

### Bug Fixes

* Fixes rollover not checking all conditions - [#123](https://github.com/opendistro-for-elasticsearch/index-management/pull/123)
* Adds policySeqNo and policyPrimaryTerm to the policy on the ManagedIndexConfig - [#123](https://github.com/opendistro-for-elasticsearch/index-management/pull/123)
* Adds null check in cluster changed event listener - [#125](https://github.com/opendistro-for-elasticsearch/index-management/pull/125)

## 2019-11-21, Version 1.3.0.0

### New Features

This is the first official release of Open Distro Index Management plugin.

With Index State Management you will be able to define custom policies, to optimize and manage indices and apply them to index patterns.
Each policy contains a default state and a list of states that you define for the index to transition between.
Within each state you can define a list of actions to perform and transitions to enter a new state based off certain conditions.

Adds backend REST API used for basic CRUD operations, explain, and management of policies and managed indices.
