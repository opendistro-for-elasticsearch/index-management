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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Transition
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionRetry
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionTimeout
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.DeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ReadWriteActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.RolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.ClusterStateManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.coordinator.SweptManagedIndexConfig
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
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentParser.Token
import org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken
import org.elasticsearch.index.seqno.SequenceNumbers
import org.elasticsearch.test.rest.ESRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

fun randomPolicy(
    id: String = ESRestTestCase.randomAlphaOfLength(10),
    schemaVersion: Long = ESRestTestCase.randomLong(),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    defaultNotification: Map<String, Any>? = randomDefaultNotification(), // TODO: DefaultNotification
    states: List<State> = List(ESRestTestCase.randomIntBetween(1, 10)) { randomState() }
): Policy {
    return Policy(id = id, schemaVersion = schemaVersion, lastUpdatedTime = lastUpdatedTime,
            defaultNotification = defaultNotification, defaultState = states[0].name, states = states, description = "random policy")
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
        Conditions.INDEX_AGE_FIELD -> Conditions(indexAge = value as TimeValue)
        Conditions.DOC_COUNT_FIELD -> Conditions(docCount = value as Long)
        Conditions.SIZE_FIELD -> Conditions(size = value as ByteSizeValue)
//        Conditions.CRON_FIELD -> Conditions(cron = value as CronSchedule) // TODO: Uncomment after issues are fixed
        else -> throw IllegalArgumentException("Invalid field: [$type] given for random Conditions.")
    }
}

fun nonNullRandomConditions(): Conditions =
    randomConditions(ESRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize())))!!

fun randomDeleteActionConfig(
    timeout: ActionTimeout = randomActionTimeout(),
    retry: ActionRetry = randomActionRetry()
): DeleteActionConfig {
    return DeleteActionConfig(timeout = timeout, retry = retry, index = 0)
}

fun randomRolloverActionConfig(
    minSize: ByteSizeValue = randomByteSizeValue(),
    minDocs: Long = ESRestTestCase.randomLongBetween(1, 1000),
    minAge: TimeValue = randomTimeValueObject(),
    timeout: ActionTimeout = randomActionTimeout(),
    retry: ActionRetry = randomActionRetry()
): RolloverActionConfig {
    return RolloverActionConfig(
        minSize = minSize,
        minDocs = minDocs,
        minAge = minAge,
        timeout = timeout,
        retry = retry,
        index = 0
    )
}

fun randomReadOnlyActionConfig(
    timeout: ActionTimeout = randomActionTimeout(),
    retry: ActionRetry = randomActionRetry()
): ReadOnlyActionConfig {
    return ReadOnlyActionConfig(timeout = timeout, retry = retry, index = 0)
}

fun randomReadWriteActionConfig(
    timeout: ActionTimeout = randomActionTimeout(),
    retry: ActionRetry = randomActionRetry()
): ReadWriteActionConfig {
    return ReadWriteActionConfig(timeout = timeout, retry = retry, index = 0)
}

fun randomActionTimeout() = ActionTimeout(randomTimeValueObject())

fun randomActionRetry() = ActionRetry(count = ESRestTestCase.randomLongBetween(1, 10), delay = randomTimeValueObject())

/**
 * Helper functions for creating a random Conditions object
 */
fun randomIndexAge(indexAge: TimeValue = randomTimeValueObject()) = Conditions.INDEX_AGE_FIELD to indexAge

fun randomDocCount(docCount: Long = ESRestTestCase.randomLongBetween(1, 1000)) = Conditions.DOC_COUNT_FIELD to docCount

fun randomSize(size: ByteSizeValue = randomByteSizeValue()) = Conditions.SIZE_FIELD to size

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
    state: String? = if (ESRestTestCase.randomBoolean()) ESRestTestCase.randomAlphaOfLength(10) else null
): ChangePolicy {
    return ChangePolicy(policyID, state)
}

fun randomDefaultNotification(): Map<String, Any>? { // TODO: DefaultNotification data class
    return null // TODO: random DefaultNotification
}

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
    changePolicy: ChangePolicy? = null
): SweptManagedIndexConfig {
    return SweptManagedIndexConfig(
        index = index,
        uuid = uuid,
        policyID = policyID,
        seqNo = seqNo,
        primaryTerm = primaryTerm,
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

fun ChangePolicy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndexConfig.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun parseDeleteActionWithType(xcp: XContentParser): DeleteActionConfig {
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    val deleteActionConfig = DeleteActionConfig.parse(xcp, 0)
    ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    return deleteActionConfig
}

fun parseRolloverActionWithType(xcp: XContentParser): RolloverActionConfig {
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    val rolloverActionConfig = RolloverActionConfig.parse(xcp, 0)
    ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    return rolloverActionConfig
}

fun parseReadOnlyActionWithType(xcp: XContentParser): ReadOnlyActionConfig {
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    val readOnlyActionConfig = ReadOnlyActionConfig.parse(xcp, 0)
    ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    return readOnlyActionConfig
}

fun parseReadWriteActionWithType(xcp: XContentParser): ReadWriteActionConfig {
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.FIELD_NAME, xcp.nextToken(), xcp::getTokenLocation)
    ensureExpectedToken(Token.START_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    val readWriteActionConfig = ReadWriteActionConfig.parse(xcp, 0)
    ensureExpectedToken(Token.END_OBJECT, xcp.nextToken(), xcp::getTokenLocation)
    return readWriteActionConfig
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
