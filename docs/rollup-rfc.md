# Rollup RFC

The purpose of this request for comments (RFC) is to introduce our plans to enhance Index Management with rollup functionality and collect feedback and discuss our plans with the community. This RFC is meant to cover the high-level functionality of the rollup functionality and does not go into implementation details and architecture.

## Problem Statement

Elasticsearch provides the ability to perform feature-rich aggregations over large data sets. Over time the time-series data sets grow to considerable sizes that can strain your clusters health, slow down your aggregations, and incurs a substantial cost. While the costs of storing data is fixed, the usefulness of the data usually goes down over time as your need for high granularity decreases.

## Proposed Solution

The proposed solution will be implemented in a phased approach. The first phase consists of the actual rolling up of data and the second phase consists of integrating rolled up data with live data in your aggregation queries.

### Rolling up data

We are proposing a few different ways to rollup data to accommodate multiple use cases and needs.

We first plan to introduce a set of APIs that allow a user to configure and manage rollup jobs. You can schedule a rollup job to run every week, day, hour, etc. and these can run on indices that are still being indexed to. This is to support situations where you might want to precompute expensive aggregations for live indices to speed up your future queries.

The second way is to allow the user to configure a one-time rollup job that will only run once instead of on a schedule. This is for when you want to rollup indices manually, do a simple one-off rollup for a new analytics investigation, or even trigger a rollup from an external system or separate plugin.

The third way will be to integrate rollups with ISM by implementing an ISM Rollup action. This will allow you to trigger rollup jobs on indices based off their lifecycle and events. This is to support use cases such as rolling up after an index has rolled over, after the index has reached 30 days, or rolling up right before deleting the index.

### Querying data

Once your data is rolled up you will want to be able to query it. You can natively query the rolled up data as they are in fact just documents in an index. These will however be stored in a different format than your original documents which would require you to query it differently.

Eventually you’ll want to query the rolled up data together with the live data. The goal is to provide a way to seamlessly integrate the two data sets while hiding the complexity from the user by allowing the usage of the native _search endpoint to query across both.

## Providing Feedback

If you have comments or feedback on our plans for Rollup, please comment on the [original GitHub issue](https://github.com/opendistro-for-elasticsearch/index-management/issues/226) in this project to discuss.