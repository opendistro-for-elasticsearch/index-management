## Version 1.13.0.0 2021-02-05

Compatible with Elasticsearch 7.10.2

### Breaking Changes

* Removes support of "index.opendistro.index_state_management.policy_id" setting ([#357](https://github.com/opendistro-for-elasticsearch/index-management/pull/357))

### Features

* Adds a new ISM Action called RollupAction which allows user to automate one-time rollups on indices ([#371](https://github.com/opendistro-for-elasticsearch/index-management/pull/371))
* Adds support for ISM templates ([#383](https://github.com/opendistro-for-elasticsearch/index-management/pull/383))

### Enhancements

* Adds a snapshot deny list cluster setting to block ISM snapshot writes to configured repositories ([#366](https://github.com/opendistro-for-elasticsearch/index-management/pull/366))
* Adds support to Explain and Get Policy APIs for getting all policies/managed indices ([#352](https://github.com/opendistro-for-elasticsearch/index-management/pull/352))

### Bug fixes

* Fixes bug for continuous rollups getting exceptions for Instant types ([#373](https://github.com/opendistro-for-elasticsearch/index-management/pull/373))
* Fixes handling various date formats for DateHistogram source field in continuous rollups ([#385](https://github.com/opendistro-for-elasticsearch/index-management/pull/385))
* Removes the metric requirement for ISM Rollup action ([#389](https://github.com/opendistro-for-elasticsearch/index-management/pull/389))
* Fixes transition step using incorrect step start time if state has no actions ([#381](https://github.com/opendistro-for-elasticsearch/index-management/pull/381))
* Fixes tests relying on exact seqNo match ([#397](https://github.com/opendistro-for-elasticsearch/index-management/pull/397))

### Infrastructure

* Adds support for https remote integration tests ([#379](https://github.com/opendistro-for-elasticsearch/index-management/pull/379))
* Renames plugin name to standardized name ([#390](https://github.com/opendistro-for-elasticsearch/index-management/pull/390))
* Fixes deb arch and renames deb/rpm artifacts to standardized names ([#391](https://github.com/opendistro-for-elasticsearch/index-management/pull/391))
* Fixes numNodes gradle property ([#393](https://github.com/opendistro-for-elasticsearch/index-management/pull/393))
* Changes release workflow to use new staging bucket for artifacts ([#378](https://github.com/opendistro-for-elasticsearch/index-management/pull/378))

### Documentation

* Adds RFC for Transforms ([#359](https://github.com/opendistro-for-elasticsearch/index-management/pull/359))

### Maintenance

* Adds support for Elasticsearch 7.10.2 ([#398](https://github.com/opendistro-for-elasticsearch/index-management/pull/398))
* Fixes reported CVEs ([#395](https://github.com/opendistro-for-elasticsearch/index-management/pull/395))
