# Open Distro for Elasticsearch Index Management

Open Distro for Elasticsearch Index Management provides a suite of features to monitor and manage indexes.

It currently contains an automated system for managing and optimizing indices throughout their life, Index State Management.

View the original [request for comments](./RFC.md).

## Highlights

With Index State Management you will be able to define custom policies, to optimize and manage indices and apply them to index patterns.

Each policy contains a default state and a list of states that you define for the index to transition between.

Within each state you can define a list of actions to perform and transitions to enter a new state based off certain conditions.

The current supported actions are:

* Delete
* Close
* Open
* Force merge
* Notification
* Read only
* Read write
* Replica count
* Rollover

The current supported transition conditions are:

* Index doc count
* Index size
* Index age
* Cron expression

## Documentation

Please see our [documentation](https://opendistro.github.io/for-elasticsearch-docs/).

## Setup

1. Check out this package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package. 
3. To build from the command line, set `JAVA_HOME` to point to a JDK >= 13 before running `./gradlew`.
  - Unix System
    1. `export JAVA_HOME=jdk-install-dir`: Replace `jdk-install-dir` with the JAVA_HOME directory of your system.
    2. `export PATH=$JAVA_HOME/bin:$PATH`
 
  - Windows System
    1. Find **My Computers** from file directory, right click and select **properties**.
    2. Select the **Advanced** tab, select **Environment variables**.
    3. Edit **JAVA_HOME** to path of where JDK software is installed.

## Build

The project in this package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.

However, to build the `index management` plugin project, we also use the Elastic build tools for Gradle.  These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in `index management` will work the way it's described in the Gradle documentation. If you encounter such a situation, the Elastic build tools [source code](https://github.com/elastic/elasticsearch/tree/master/buildSrc/src/main/groovy/org/elasticsearch/gradle) is your best bet for figuring out what's going on.

This project currently uses the Notification subproject from the [Alerting plugin](https://github.com/opendistro-for-elasticsearch/alerting). There is an [open PR](https://github.com/opendistro-for-elasticsearch/alerting/pull/97) that introduces the maven publish task in Alerting for publishing the Notification jars. Until this PR is fully merged and jars published you will need to pull down the PR yourself and publish the jars to your local maven repository in order to build Index Management.

1. Visit the PR [here](https://github.com/opendistro-for-elasticsearch/alerting/pull/97) and pull down the Alerting plugin along with the PR changes
2. You may need to cherry-pick the changes into a separate branch if you require a specific version to be published
3. Build the Alerting plugin (w/ the changes in PR) and publish the artifacts to your local maven repository
     1. `./gradlew clean`
     2. `./gradlew build` or `./gradlew assemble` build will run the tests and build artifacts, assemble will only build the artifacts
     3. `./gradlew publishToMavenLocal` publishes artifacts to your local maven repository

### Building from the command line

1. `./gradlew build` builds and tests project.
2. `./gradlew run` launches a single node cluster with the index management (and job-scheduler) plugin installed.
3. `./gradlew integTest` launches a single node cluster with the index management (and job-scheduler) plugin installed and runs all integ tests.
4. `./gradlew integTest -Dtests.class=*RestChangePolicyActionIT` runs a single integ class
5.  `./gradlew integTest -Dtests.class=*RestChangePolicyActionIT -Dtests.method="test missing index"` runs a single integ test method (remember to quote the test method name if it contains spaces)

When launching a cluster using one of the above commands, logs are placed in `build/testclusters/integTest-0/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Debugging

Sometimes it is useful to attach a debugger to either the Elasticsearch cluster or the integ tests to see what's going on. When running unit tests, hit **Debug** from the IDE's gutter to debug the tests.  For the Elasticsearch cluster or the integ tests, first, make sure start a debugger listening on port `5005`. 

To debug the server code, run:

```
./gradlew :integTest -Dcluster.debug # to start a cluster with debugger and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The Elasticsearch server JVM will connect to a debugger attached to `localhost:5005`.

The IDE needs to listen for the remote JVM. If using Intellij you must set your debug configuration to "Listen to remote JVM" and make sure "Auto Restart" is checked.
You must start your debugger to listen for remote JVM before running the commands.

To debug code running in an integration test (which exercises the server from a separate JVM), first, setup a remote debugger listening on port `8000`, and then run:

```
./gradlew :integTest -Dtest.debug
```

The test runner JVM will connect to a debugger attached to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :integTest -Dtest.debug -Dcluster.debug
```



## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security issue notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## Licensing

See the [LICENSE](./LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.

## Copyright

Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
