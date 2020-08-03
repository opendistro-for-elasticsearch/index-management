/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Conditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ErrorNotification
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.StateFilter
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.AllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.NotificationActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadWriteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.IndexPriorityActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.SnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.destination.Chime
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.destination.CustomWebhook
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.destination.Destination
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.destination.DestinationType
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.destination.Slack
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import org.elasticsearch.test.rest.ESRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

fun randomPolicy(
    id: String = ESRestTestCase.randomAlphaOfLength(10),
    description: String = ESRestTestCase.randomAlphaOfLength(10),
    schemaVersion: Long = ESRestTestCase.randomLong(),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    errorNotification: ErrorNotification? = randomErrorNotification(),
    states: List<State> = List(ESRestTestCase.randomIntBetween(1, 10)) { randomState() }
): Policy {
    return Policy(id = id, schemaVersion = schemaVersion, lastUpdatedTime = lastUpdatedTime,
            errorNotification = errorNotification, defaultState = states[0].name, states = states, description = description)
}

fun randomState(
    name: String = ESRestTestCase.randomAlphaOfLength(10),
    actions: List<ActionConfig> = listOf(),
    transitions: List<Transition> = listOf()
): State {
    return State(name = name, actions = actions, transitions = transitions)
}

fun randomTransition(
    stateName: String = ESRestTestCase.randomAlphaOfLength(10),
    conditions: Conditions? = randomConditions()
): Transition {
    return Transition(stateName = stateName, conditions = conditions)
}

/**
 * TODO: Excluded randomCronSchedule being included in randomConditions as two issues need to be resolved first:
 *   1. Job Scheduler needs to be published to maven central as there is an issue retrieving dependencies for SPI
 *   2. CronSchedule in Job Scheduler needs to implement equals/hash methods so assertEquals compares two CronSchedule
 *      objects properly when doing roundtrip parsing tests
 */
fun randomConditions(
    condition: Pair<String, Any>? =
        ESRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize(), null))
): Conditions? {

    if (condition == null) return null

    val type = condition.first
    val value = condition.second

    return when (type) {
        Conditions.MIN_INDEX_AGE_FIELD -> Conditions(indexAge = value as TimeValue)
        Conditions.MIN_DOC_COUNT_FIELD -> Conditions(docCount = value as Long)
        Conditions.MIN_SIZE_FIELD -> Conditions(size = value as ByteSizeValue)
//        Conditions.CRON_FIELD -> Conditions(cron = value as CronSchedule) // TODO: Uncomment after issues are fixed
        else -> throw IllegalArgumentException("Invalid field: [$type] given for random Conditions.")
    }
}

fun nonNullRandomConditions(): Conditions =
    randomConditions(ESRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize())))!!

fun randomDeleteActionConfig(): DeleteActionConfig {
    return DeleteActionConfig(index = 0)
}

fun randomRolloverActionConfig(
    minSize: ByteSizeValue = randomByteSizeValue(),
    minDocs: Long = ESRestTestCase.randomLongBetween(1, 1000),
    minAge: TimeValue = randomTimeValueObject()
): RolloverActionConfig {
    return RolloverActionConfig(
        minSize = minSize,
        minDocs = minDocs,
        minAge = minAge,
        index = 0
    )
}

fun randomReadOnlyActionConfig(): ReadOnlyActionConfig {
    return ReadOnlyActionConfig(index = 0)
}

fun randomReadWriteActionConfig(): ReadWriteActionConfig {
    return ReadWriteActionConfig(index = 0)
}

fun randomReplicaCountActionConfig(numOfReplicas: Int = ESRestTestCase.randomIntBetween(0, 200)): ReplicaCountActionConfig {
    return ReplicaCountActionConfig(index = 0, numOfReplicas = numOfReplicas)
}

fun randomIndexPriorityActionConfig(indexPriority: Int = ESRestTestCase.randomIntBetween(0, 100)): IndexPriorityActionConfig {
    return IndexPriorityActionConfig(index = 0, indexPriority = indexPriority)
}

fun randomForceMergeActionConfig(
    maxNumSegments: Int = ESRestTestCase.randomIntBetween(1, 50)
): ForceMergeActionConfig {
    return ForceMergeActionConfig(maxNumSegments = maxNumSegments, index = 0)
}

fun randomNotificationActionConfig(
    destination: Destination = randomDestination(),
    messageTemplate: Script = randomTemplateScript("random message"),
    index: Int = 0
): NotificationActionConfig {
    return NotificationActionConfig(destination, messageTemplate, index)
}

fun randomAllocationActionConfig(require: Map<String, String> = emptyMap(), exclude: Map<String, String> = emptyMap(), include: Map<String, String> = emptyMap()): AllocationActionConfig {
    return AllocationActionConfig(require, include, exclude, index = 0)
}

fun randomDestination(type: DestinationType = randomDestinationType()): Destination {
    return Destination(
        type = type,
        chime = if (type == DestinationType.CHIME) randomChime() else null,
        slack = if (type == DestinationType.SLACK) randomSlack() else null,
        customWebhook = if (type == DestinationType.CUSTOM_WEBHOOK) randomCustomWebhook() else null
    )
}

fun randomDestinationType(): DestinationType {
    val types = listOf(DestinationType.SLACK, DestinationType.CHIME, DestinationType.CUSTOM_WEBHOOK)
    return ESRestTestCase.randomSubsetOf(1, types).first()
}

fun randomChime(): Chime {
    return Chime("https://www.amazon.com")
}

fun randomSlack(): Slack {
    return Slack("https://www.amazon.com")
}

fun randomCustomWebhook(): CustomWebhook {
    return CustomWebhook(
        url = "https://www.amazon.com",
        scheme = null,
        host = null,
        port = -1,
        path = null,
        queryParams = emptyMap(),
        headerParams = emptyMap(),
        username = null,
        password = null
    )
}

fun randomTemplateScript(
    source: String,
    params: Map<String, String> = emptyMap()
): Script = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, source, params)

fun randomSnapshotActionConfig(repository: String? = null, snapshot: String? = null): SnapshotActionConfig {
    return SnapshotActionConfig(repository, snapshot, index = 0)
}

/**
 * Helper functions for creating a random Conditions object
 */
fun randomIndexAge(indexAge: TimeValue = randomTimeValueObject()) = Conditions.MIN_INDEX_AGE_FIELD to indexAge

fun randomDocCount(docCount: Long = ESRestTestCase.randomLongBetween(1, 1000)) = Conditions.MIN_DOC_COUNT_FIELD to docCount

fun randomSize(size: ByteSizeValue = randomByteSizeValue()) = Conditions.MIN_SIZE_FIELD to size

fun randomCronSchedule(cron: CronSchedule = CronSchedule("0 * * * *", ZoneId.of("UTC"))) =
    Conditions.CRON_FIELD to cron

fun randomTimeValueObject() = TimeValue.parseTimeValue(ESRestTestCase.randomPositiveTimeValue(), "")

fun randomByteSizeValue() =
    ByteSizeValue.parseBytesSizeValue(
        ESRestTestCase.randomIntBetween(1, 1000).toString() + ESRestTestCase.randomFrom(listOf("b", "kb", "mb", "gb")),
        ""
    )
/**
 * End - Conditions helper functions
 */

fun randomChangePolicy(
    policyID: String = ESRestTestCase.randomAlphaOfLength(10),
    state: String? = if (ESRestTestCase.randomBoolean()) ESRestTestCase.randomAlphaOfLength(10) else null,
    include: List<StateFilter> = emptyList(),
    isSafe: Boolean = false
): ChangePolicy {
    return ChangePolicy(policyID, state, include, isSafe)
}

// will only return null since we dont want to send actual notifications during integ tests
fun randomErrorNotification(): ErrorNotification? = null

fun randomManagedIndexConfig(
    name: String = ESRestTestCase.randomAlphaOfLength(10),
    index: String = ESRestTestCase.randomAlphaOfLength(10),
    uuid: String = ESRestTestCase.randomAlphaOfLength(20),
    enabled: Boolean = ESRestTestCase.randomBoolean(),
    schedule: Schedule = IntervalSchedule(Instant.ofEpochMilli(Instant.now().toEpochMilli()), 5, ChronoUnit.MINUTES),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    policyID: String = ESRestTestCase.randomAlphaOfLength(10),
    policy: Policy? = randomPolicy(),
    changePolicy: ChangePolicy? = randomChangePolicy()
): ManagedIndexConfig {
    return ManagedIndexConfig(
        jobName = name,
        index = index,
        indexUuid = uuid,
        enabled = enabled,
        jobSchedule = schedule,
        jobLastUpdatedTime = lastUpdatedTime,
        jobEnabledTime = enabledTime,
        policyID = policy?.id ?: policyID,
        policySeqNo = policy?.seqNo,
        policyPrimaryTerm = policy?.primaryTerm,
        policy = policy?.copy(id = ManagedIndexConfig.NO_ID, seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO, primaryTerm = SequenceNumbers.UNASSIGNED_PRIMARY_TERM),
        changePolicy = changePolicy
    )
}

fun randomClusterStateManagedIndexConfig(
    index: String = ESRestTestCase.randomAlphaOfLength(10),
    uuid: String = ESRestTestCase.randomAlphaOfLength(20),
    policyID: String = ESRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM
): ClusterStateManagedIndexConfig {
    return ClusterStateManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm
    )
}

fun randomSweptManagedIndexConfig(
    index: String = ESRestTestCase.randomAlphaOfLength(10),
    uuid: String = ESRestTestCase.randomAlphaOfLength(20),
    policyID: String = ESRestTestCase.randomAlphaOfLength(10),
    seqNo: Long = SequenceNumbers.UNASSIGNED_SEQ_NO,
    primaryTerm: Long = SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
    changePolicy: ChangePolicy? = null,
    policy: Policy? = null
): SweptManagedIndexConfig {
    return SweptManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm,
        policy = policy,
        changePolicy = changePolicy
    )
}

fun Policy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder).string()
}

fun State.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Transition.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun Conditions.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun DeleteActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun RolloverActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadOnlyActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReadWriteActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ReplicaCountActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun IndexPriorityActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ForceMergeActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun NotificationActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun AllocationActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ChangePolicy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexMetaData.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder().startObject()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).endObject().string()
}

fun SnapshotActionConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

/**
* Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
* a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
* but that's an exercise for another day.
*/

fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    params: Map<String, String> = emptyMap(),
    entity: HttpEntity? = null,
    vararg headers: Header
): Response {
    val request = Request(method, endpoint)
    val options = RequestOptions.DEFAULT.toBuilder()
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    params.forEach { request.addParameter(it.key, it.value) }
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

/**
 * Wrapper for [RestClient.performRequest] which was deprecated in ES 6.5 and is used in tests. This provides
 * a single place to suppress deprecation warnings. This will probably need further work when the API is removed entirely
 * but that's an exercise for another day.
 */

fun RestClient.makeRequest(
    method: String,
    endpoint: String,
    entity: HttpEntity? = null,
    vararg headers: Header
): Response {
    val request = Request(method, endpoint)
    val options = RequestOptions.DEFAULT.toBuilder()
    headers.forEach { options.addHeader(it.name, it.value) }
    request.options = options.build()
    if (entity != null) {
        request.entity = entity
    }
    return performRequest(request)
}

fun <T> waitFor(
    timeout: Instant = Instant.ofEpochSecond(10),
    block: () -> T
): T {
    val startTime = Instant.now().toEpochMilli()
    do {
        try {
            return block()
        } catch (e: Throwable) {
            if ((Instant.now().toEpochMilli() - startTime) > timeout.toEpochMilli()) {
                throw e
            } else {
                Thread.sleep(100L)
            }
        }
    } while (true)
}
