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
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Conditions
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.ManagedIndex
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.models.Transition
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.test.rest.ESRestTestCase
import java.time.Instant
import java.time.ZoneId
import java.time.temporal.ChronoUnit

fun randomPolicy(
    name: String = ESRestTestCase.randomAlphaOfLength(10),
    schemaVersion: Long = ESRestTestCase.randomLong(),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    defaultNotification: Map<String, Any>? = randomDefaultNotification(), // TODO: DefaultNotification
    states: List<State> = List(ESRestTestCase.randomIntBetween(1, 10)) { randomState() }
): Policy {
    return Policy(name = name, schemaVersion = schemaVersion, lastUpdatedTime = lastUpdatedTime,
            defaultNotification = defaultNotification, defaultState = states[0].name, states = states)
}

fun randomState(
    name: String = ESRestTestCase.randomAlphaOfLength(10),
    actions: List<Map<String, Any>> = listOf(), // TODO: List<Action>
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
        Conditions.INDEX_AGE_FIELD -> Conditions(indexAge = value as String)
        Conditions.DOC_COUNT_FIELD -> Conditions(docCount = value as Long)
        Conditions.SIZE_FIELD -> Conditions(size = value as String)
//        Conditions.CRON_FIELD -> Conditions(cron = value as CronSchedule) // TODO: Uncomment after issues are fixed
        else -> throw IllegalArgumentException("Invalid field: [$type] given for random Conditions.")
    }
}

fun nonNullRandomConditions(): Conditions =
        randomConditions(ESRestTestCase.randomFrom(listOf(randomIndexAge(), randomDocCount(), randomSize())))!!

fun randomIndexAge(indexAge: String = randomAge()) = Conditions.INDEX_AGE_FIELD to indexAge

fun randomDocCount(docCount: Long = ESRestTestCase.randomLong()) = Conditions.DOC_COUNT_FIELD to docCount

fun randomSize(size: String = randomByteSizeValue()) = Conditions.SIZE_FIELD to size

fun randomCronSchedule(cron: CronSchedule = CronSchedule("0 * * * *", ZoneId.of("UTC"))) =
    Conditions.CRON_FIELD to cron

fun randomAge() =
    ESRestTestCase.randomIntBetween(1, 30).toString() + ESRestTestCase.randomFrom(listOf("s", "m", "h", "d"))

fun randomByteSizeValue() =
    ESRestTestCase.randomIntBetween(1, 1000).toString() + ESRestTestCase.randomFrom(listOf("b", "kb", "mb", "gb"))

fun randomChangePolicy(
    policyName: String = ESRestTestCase.randomAlphaOfLength(10),
    state: String? = if (ESRestTestCase.randomBoolean()) ESRestTestCase.randomAlphaOfLength(10) else null
): ChangePolicy {
    return ChangePolicy(policyName, state)
}

fun randomDefaultNotification(): Map<String, Any>? { // TODO: DefaultNotification data class
    return null // TODO: random DefaultNotification
}

fun randomManagedIndex(
    name: String = ESRestTestCase.randomAlphaOfLength(10),
    index: String = ESRestTestCase.randomAlphaOfLength(10),
    enabled: Boolean = ESRestTestCase.randomBoolean(),
    schedule: Schedule = IntervalSchedule(Instant.now(), 5, ChronoUnit.MINUTES),
    lastUpdatedTime: Instant = Instant.now().truncatedTo(ChronoUnit.MILLIS),
    enabledTime: Instant? = if (enabled) Instant.now().truncatedTo(ChronoUnit.MILLIS) else null,
    policyName: String = ESRestTestCase.randomAlphaOfLength(10),
    policy: Policy? = randomPolicy(),
    changePolicy: ChangePolicy? = null
): ManagedIndex {
    return ManagedIndex(
        jobName = name,
        index = index,
        enabled = enabled,
        jobSchedule = schedule,
        jobLastUpdatedTime = lastUpdatedTime,
        jobEnabledTime = enabledTime,
        policyName = policy?.name ?: policyName,
        policyVersion = policy?.version,
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

fun ChangePolicy.toJsonString(): String {
    val builder = XContentFactory.jsonBuilder()
    return this.toXContent(builder, ToXContent.EMPTY_PARAMS).string()
}

fun ManagedIndex.toJsonString(): String {
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
