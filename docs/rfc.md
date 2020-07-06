# Index Management

The purpose of this request for comments (RFC) is to introduce our plans to enhance Open Distro for Elasticsearch with an Index Management suite and collect feedback and discuss our plans with the community. This RFC is meant to cover the high-level functionality of the Index Management suite and does not go into implementation details and architecture.

## Problem Statement

Elasticsearch provides a variety of monitoring, configuration, and operational APIs to help manage and tune indexes for optimal performance. However, open source Elasticsearch does not provide a built-in UI for monitoring and managing indexes. Furthermore, if you are running periodic operations on your indexes, there is no built-in job engine to run these operations, so you need to setup and manage an external process to run these tasks.

To ensure a cluster performs well, you need to optimize index configurations and perform operations to tune them. For example, if you notice your search requests are slowing down you may try running a forcemerge to reduce the number of segments per shard. Building on this same example, you would want to periodically run the forcemerge to keep performance optimized, but only schedule it to run in off-peak hours, so it does not impact other workloads.

## Proposed Solution

We are planning to build an open source index management suite for Elasticsearch and Kibana that will be integrated into Open Distro for Elasticsearch. It will offer index automation to simplify managing indexes, while providing a Kibana based administrative panel for monitoring and managing indexes. 

We will first focus on building an automated system for managing and optimizing indices throughout their life, Index State Management. With Index State Management you will be able to define custom policies, to optimize and move indices and apply them to index patterns. Each policy contains a default state and a list of states that you define for the index to transition between. Within each state you can define a list of actions to perform and transitions to enter a new state. 

What does this look like in practice? Let's say you are ingesting application logs and you want to it to initially be optimized for ingest, then optimized for search, and eventually deleted after 30 days. The policy would be defined as follows:

``` JSON
{
    "name": "Log Rotation",
    "schema_version": 1,
    "last_update_time": 1553112384,
    "default_state": "ingest",
    "states": [{
        "state": "ingest",
        "actions": [{ "rollover": { ... } }],
        "transitions": [{ "state": "search" }]
    }, {
        "state": "search",
        "actions": [{ "force_merge": { ... } }],
        "transitions": [{ "state": "delete", "age": "30d" }]
    }, {
        "state": "delete",
        "actions": [{ "snapshot": { ... } }, { "delete": {} }],
        "transitions": []
    }]
}
```

When updating a policy, it will not affect any current managed indices that have adopted that policy. All managed indices will have a cached version of the policy when they were created and operate based off that version. You can update a managed index to use a newer version of a policy through an API or the Kibana plugin.

Once a managed index enters a state it will sequentially execute the actions in the same order listed in the policy. 

Once all actions have been successfully completed we will periodically check state transitions until we eventually meet a true condition. If you have multiple transitions in a state it will use the first one in the list that is true.

We plan to support the following conditions to transition states:


|**Conditions Name**|**Description**|
|:------------------|:--------------|
|_`empty`_| If no condition is specified then the transition condition is automatically met.|
|`index_age`| This is the minimum age from index creation that causes the transition condition to be met.|
|`doc_count`| This is the minimum document count that causes the transition condition to be met.|
|`size`| This is the minimum size that causes the transition condition to be met.|
|`cron`| This will be a cron expression used to indicate when a state transition should occur.|

All actions will have their own retry, backoff, timeout settings that can be configured individually or ignored to use the default choices.

``` JSON
// Specifying your own retry, backoff, and timeout settings
{
    "allocation": {
        "include": { "size": "small" },
        "timeout": "1h",
        "retry": {
            "count": 3,
            "backoff": "exponential",
            "delay": "1s"
        }
    }
}

// This will use the default retry, backoff, and timeout settings for the action as defined in the documentation
{
    "allocation": {
        "include": { "size": "small" }
    }
}
```

Each action is broken down into sequential steps and if at any point an error occurs then the managed index will move to an error step (unless retry is possible). Once in an error step the user will have to fix the underlying issue (if possible) and then use the API to retry the step that failed.

As an example consider the above policy. We have a managed index that has entered the `delete` state. In this state we perform a `snapshot` right before deleting the index for retention purposes. The snapshot action is split up into multiple steps:
1. `take_snapshot` - This involves calling the snapshot API for this managed index with the configuration specified in the snapshot action
2. `wait_for_snapshot` - This involves waiting for the snapshot to finish which could be fast or slow depending on the index size and previous snapshots.

Let’s say we have an index that attempted to execute the #1 step (`take_snapshot`), but failed. It could fail for a variety of reasons ranging from intermittent issues with repositories, cluster health issues, configuration errors, or even snapshots already in progress. Let’s assume the snapshot was incorrectly configured which caused it to fail.

You fix the issue by updating the snapshot configuration. You can now call the managed index retry API which will retry the step that caused it to go into the error step.

The managed index attempts the `take_snapshot` step again and is able to start the snapshot process. Now it moves on to the `wait_for_snapshot` where it waits for the snapshot to succeed.

We plan to support the following actions when transitioning to a new state:

|**Action Name**|**Initial Release**|**Description**|
|:--------------|:---------------|:--------------|
|`allocation`| No | This action is used to change the index routing allocation settings.|
|`force_merge`| Yes | Performs the force_merge operation on an index.|
|`shrink`| No | Performs a shrink operation on an index.|
|`split`| No | Performs a split operation on an index.|
|`read_only`| Yes | This action is used to put an index into readonly mode.|
|`read_write`| Yes | This action is used to put an index into read and write mode.|
|`replica_count`| Yes | This action is used to change the replica count of an index.|
|`close`| Yes | This action closes an index|
|`open`| Yes | This action opens an index|
|`snapshot`| Yes | This action takes a snapshot of your index|
|`delete`| Yes | Delete is used to delete an index|
|`rollover`| Yes | This action is used to periodically check the rollover conditions and then call the rollover API once conditions are met. When rolling over, a new index will be created which adopts the policy from the index template (required for rollover). The old index will continue to transition through the policy.|
|`webhook`| Yes | This triggers an arbitrary webhook request with a specified body. This can be useful if you want to notify another system of index state transitions.|

After building Index State Management API, we will build an administrative panel for managing indexes and their policies. This Kibana based admin panel will allow you to view, create, update, and delete indexes. It will also support operations like shrink, split, forcemerge, and more. The panel will provide a monitoring view for indexes and shards from the Elasticsearch APIs and the Open Distro for Elasticsearch Performance Analyzer. It will expose metrics like size, search rate, doc count, unallocated shards, and more. The administration panel will also include a user experience for creating Index State Policies, Index Aliases, and Index Templates.

## Providing Feedback

If you have comments or feedback on our plans for Index Management, please comment on [the RFC Github issue](../../issues/1) in this project to discuss.
