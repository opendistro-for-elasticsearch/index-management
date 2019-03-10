# Index Management

The purpose of this request for comments (RFC) is to introduce our plans to enhance Open Distro for Elasticsearch with an Index Management suite and collect feedback and discuss our plans with the community. This RFC is meant to cover the high-level functionality of the Index Management suite and does not go into implementation details and architecture.

## Problem Statement

Elasticsearch provides a variety of monitoring, configuration, and operational APIs to help manage and tune indexes for optimal performance. However, open source Elasticsearch does not provide a built-in UI for monitoring and managing indexes. Furthermore, if you are running periodic operations on your indexes, there is no built-in job engine to run these operations, so you need to setup and manage an external process to run these tasks.

To ensure a cluster performs well, you need to optimize index configurations and perform operations to tune them. For example, if you notice your search requests are slowing down you may try running a forcemerge to reduce the number of segments per shard. Building on this same example, you would want to periodically run the forcemerge to keep performance optimized, but only schedule it to run in off-peak hours, so it does not impact other workloads.

## Proposed Solution

We are planning to build an open source index management suite for Elasticsearch and Kibana that will be integrated into Open Distro for Elasticsearch. It will offer index automation to simplify managing indexes, while providing a Kibana based administrative panel for monitoring and managing indexes. 

We will first focus on building an automated system for managing and optimizing indices throughout their life, Index State Management. With Index State Management you will be able to define custom policies, to optimize and move indices and apply them to index patterns. Each policy contains a default state and a list of states that you define for the index to transition between. Within each state you can define a list of actions to perform and triggers to enter a new state. 

What does this look like in practice? Let's say you are ingesting application logs and you want to it to initially be optimized for ingest, then optimized for search, and eventually deleted after 30 days. The policy would be defined as follows:

``` JSON
{
  "name": "Log Rotation",
  "states": {
    "default": "ingest",
      
    "ingest": {
      "actions": [],
      "next": {
        "search": { "age": "1d" },
      }
    },
    "search": {
      "actions": [<shrink>, <forcemerge>],
      "next": {
        "delete": { "age": "30d" }
      }
    },
    "delete": {
        "actions": [<delete>]
    }
  }
}
```

We plan to support the following conditions to change state:

|**Conditions Name**|**Description**|
|:------------------|:--------------|
|`actions_complete`| Once all actions in the current state successfully complete, this condition will be met.|
|`age`|This is the minimum age that causes the next state to trigger.|
|`doc_count`| This is the minimum document count that causes the next state to trigger.|
|`size`| This is the minimum size that causes the next state to trigger.|
|`cron`| This will be a cron expression used to indicate when a state trasition should occur.|
|`rollover`| This condition is used to transition a rolled over index into it's next state. For example let's say you have a rollover action configured for a lifecycle policy and a rollover condition to transition to a new state. After the rollover action is completed, the index that it is rolled over to will transition to the next state. NOTE: this is only supported on policies applied to index templates.|

We plan to support the following actions when transitioning to a new state:

|**Action Name**|**Description**|
|:--------------|:--------------|
|`allocation`|This action is used to change the index routing allocation settings.|
|`forcemerge`|Performs the forcemerge operation on an index.|
|`shrink`|Performs a shrink operation on an index.|
|`split`|Performs a split operation on an index.|
|`read_only`|This action is used to put an index into readonly mode.|
|`read_write`|This action is used to put an index into read and write mode.|
|`replica_count`|This action is used to change the replica count of an index.|
|`close`|This action closes an index|
|`open`|This action opens an index|
|`delete`|Delete is used to delete an index|
|`snapshot`|Snapshot is used to perform a snapshot on an index|
|`rollover`|This action is used to periodically call the rollover API with specified rollover conditions. Once met, the current index will be rolledover to a new index. The old index with inherit the same state or will transition to a new state if a state condition is defined for rollover. NOTE: this is only supported on policies applied to index templates.|
|`webhook`|This triggers an arbitrary webhook request with a specified body. This can be useful if you want to notify another system of index state transitions.|

After building Index State Management API, we will build an administrative panel for managing indexes and their policies. This Kibana based admin panel will allow you to view, create, update, and delete indexes. It will also support operations like shrink, split, forcemerge, and more. The panel will provide a monitoring view for indexes and shards from the Elasticsearch APIs and the Open Distro for Elasticsearch Performance Analyzer. It will expose metrics like size, search rate, doc count, unallocated shards, and more. The administration panel will also include a user experience for creating Index State Policies, Index Aliases, and Index Templates.

## Providing Feedback

If you have comments or feedback on our plans for Index Management, please comment on [the RFC Github issue](../../issues/1) in this project to discuss.
