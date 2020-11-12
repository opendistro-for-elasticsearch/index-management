/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.CronSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.Schedule
import org.apache.http.Header
import org.apache.http.HttpEntity
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestClient
import org.elasticsearch.test.rest.ESRestTestCase
import java.time.Instant
import java.time.temporal.ChronoUnit

fun randomDayOfWeekCronField(): String = if (ESRestTestCase.randomBoolean()) "*" else ESRestTestCase.randomIntBetween(0, 7).toString()

fun randomMonthCronField(): String = if (ESRestTestCase.randomBoolean()) "*" else ESRestTestCase.randomIntBetween(1, 12).toString()

fun randomDayOfMonthCronField(): String = if (ESRestTestCase.randomBoolean()) "*" else ESRestTestCase.randomIntBetween(1, 31).toString()

fun randomHourCronField(): String = if (ESRestTestCase.randomBoolean()) "*" else ESRestTestCase.randomIntBetween(0, 23).toString()

fun randomMinuteCronField(): String = if (ESRestTestCase.randomBoolean()) "*" else ESRestTestCase.randomIntBetween(0, 59).toString()

fun randomCronExpression(): String = "${randomMinuteCronField()} ${randomHourCronField()} ${randomDayOfMonthCronField()} ${randomMonthCronField()} ${randomDayOfWeekCronField()}"

val chronoUnits = listOf(ChronoUnit.MINUTES, ChronoUnit.HOURS, ChronoUnit.DAYS)

fun randomChronoUnit(): ChronoUnit = ESRestTestCase.randomSubsetOf(1, chronoUnits).first()

fun randomEpochMillis(): Long = ESRestTestCase.randomLongBetween(0, Instant.now().toEpochMilli())

fun randomInstant(): Instant = Instant.ofEpochMilli(randomEpochMillis())

fun randomCronSchedule(): CronSchedule = CronSchedule(randomCronExpression(), ESRestTestCase.randomZone())

fun randomIntervalSchedule(): IntervalSchedule = IntervalSchedule(randomInstant(), ESRestTestCase.randomIntBetween(1, 100), randomChronoUnit())

fun randomSchedule(): Schedule = if (ESRestTestCase.randomBoolean()) randomIntervalSchedule() else randomCronSchedule()

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