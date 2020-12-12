# Transform RFC
The purpose of this request for comments (RFC) is to introduce our plans to enhance Index Management with transform functionality to collect feedback and discuss our plans with the community. This RFC is meant to cover the high level functionality of transform and doesn't go into the design details/architecture/implementation details.

## Problem Statement:
Elasticsearch provides the ability to perform aggregations, help to summarize and retrieve insights from the data. However in some cases, the amount of data that needs to processed can overwhelm the hardware or might not be possible because of hardware constraints and can involve building complex query expressions. Transforms help in these cases by transforming and summarizing the data to a different index in the background which can be used later for further analysis.

An example would be if there are millions of documents of user request data ingested currently and want to analyze certain subset of users based on their request time or request frequency with which they use the system. Though it is possible to do this using raw data, but its inefficient. Instead using transform a new index can be created setting user as the group, computing all the required metrics and associating with the user. This makes it much simpler to use this data later on for further analysis, and since this will be done in background it will be run in a controlled fashion.

## Proposed Solution:
### Transforming data:
We are planning to introduce a set of APIs and new ISM actions that allow a user to configure and manage transform jobs. We plan to launch the features in a phased manner.

For first phase we are planning to make the APIs available to create, delete, preview, start, stop and stats APIs for transform jobs. These transform jobs can be scheduled to be run at a requested time or immediately. We initially are planning to support single occurrence transform jobs. 

As part of the transform job you will be able to transform data based a pivot, a set of features and calculate the metrics for each of these features. We are going to have a set of commonly used aggregations and groupings to begin with and expand these with time. There will also be ability to filter the source data used for transform job if needed.

### Accessing transformed data:
Once your data is transformed into the target index you will be able to query it like any other index. The documents in the index are stored in the format described in the transform job.

## Providing feedback:
If you have comments or feedback on our plans for transforms, please comment on [github issue](https://github.com/opendistro-for-elasticsearch/index-management/issues/358)
