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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.WITH_TYPE
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupStats
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.DateHistogram
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.doAnswer
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import kotlinx.coroutines.runBlocking
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.common.bytes.BytesReference
import org.elasticsearch.common.document.DocumentField
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.elasticsearch.test.ESTestCase
import org.junit.Before
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

// TODO: Given the way these tests are mocking data, only entries that work with ZonedDateTime.parse
//   are being tested, should mock the data more generically to test all cases
class RollupMetadataServiceTests : ESTestCase() {

    private lateinit var xContentRegistry: NamedXContentRegistry

    @Before
    fun setup() {
        val namedXContentRegistryEntries = arrayListOf<NamedXContentRegistry.Entry>()
        xContentRegistry = NamedXContentRegistry(namedXContentRegistryEntries)
    }

    fun `test metadata for continuous rollup with minute calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1m",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-10-02T05:01:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-10-02T05:01:00Z")
        val expectedWindowEndTime = getInstant("2020-10-02T05:02:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with hour calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1h",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-10-02T05:35:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-10-02T05:00:00Z")
        val expectedWindowEndTime = getInstant("2020-10-02T06:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with hour calendar interval and daylight savings time`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1h",
            timezone = ZoneId.of("America/Los_Angeles")
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-03-08T01:35:15-08:00"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = localDateAtTimezone("2020-03-08T01:00:00", ZoneId.of("America/Los_Angeles"))
        // Should jump to March 3, 2020 at 3AM PST for end time due to DST
        val expectedWindowEndTime = localDateAtTimezone("2020-03-08T03:00:00", ZoneId.of("America/Los_Angeles"))

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with day calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "day",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-10-02T05:35:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-10-02T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-10-03T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with day calendar interval for leap year`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1d",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-02-28T08:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-02-28T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-02-29T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with week calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1w",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-03-22T08:40:15Z" // March 22, 2020 Sunday
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        // Since Monday is the beginning of the calendar week, the start time will be last Monday
        // given that the first document timestamp was on Sunday
        val expectedWindowStartTime = getInstant("2020-03-16T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-03-23T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with month calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1M",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2019-12-24T08:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2019-12-01T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-01-01T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with quarter calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1q",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-04-24T08:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-04-01T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-07-01T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with year calendar interval`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1y",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-04-24T08:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-01-01T00:00:00Z")
        val expectedWindowEndTime = getInstant("2021-01-01T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with time offset for document`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1h",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-04-24T08:40:15-07:00" // UTC-07:00 for America/Los_Angeles
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = localDateAtTimezone("2020-04-24T08:00:00", ZoneId.of("America/Los_Angeles"))
        val expectedWindowEndTime = localDateAtTimezone("2020-04-24T09:00:00", ZoneId.of("America/Los_Angeles"))

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with timezone for date histogram`() {
        val dimensions = listOf(randomCalendarDateHistogram().copy(
            calendarInterval = "1h",
            timezone = ZoneId.of("America/Los_Angeles")
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-04-24T08:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = localDateAtTimezone("2020-04-24T01:00:00", ZoneId.of("America/Los_Angeles"))
        val expectedWindowEndTime = localDateAtTimezone("2020-04-24T02:00:00", ZoneId.of("America/Los_Angeles"))

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with hour fixed interval`() {
        val dimensions = listOf(randomFixedDateHistogram().copy(
            fixedInterval = "3h",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-04-24T22:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = getInstant("2020-04-24T21:00:00Z")
        val expectedWindowEndTime = getInstant("2020-04-25T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with hour fixed interval and daylight savings time`() {
        val dimensions = listOf(randomFixedDateHistogram().copy(
            fixedInterval = "3h",
            timezone = ZoneId.of("America/Los_Angeles")
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-03-08T00:40:15-08:00"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        val expectedWindowStartTime = localDateAtTimezone("2020-03-08T00:00:00", ZoneId.of("America/Los_Angeles"))
        // Fixed interval does not understand daylight savings time so 3 hours (60 minutes * 3) is added to the start time
        // Resulting in 2020-03-08T03:00:00 (PST) which is now GMT-7 -> 2020-03-08T10:00:00Z
        val expectedWindowEndTime = localDateAtTimezone("2020-03-08T03:00:00", ZoneId.of("America/Los_Angeles"))

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata for continuous rollup with day fixed interval and leap year`() {
        val dimensions = listOf(randomFixedDateHistogram().copy(
            fixedInterval = "30d",
            timezone = ZoneId.of(DateHistogram.UTC)
        ))
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null,
            continuous = true,
            dimensions = dimensions
        )

        val firstDocTimestamp = "2020-02-01T22:40:15Z"
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = getIndexResponse(),
            indexException = null
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        // 30 days (24 hours * 30) increments since epoch will land us on 2020-01-09 as the nearest bucket
        val expectedWindowStartTime = getInstant("2020-01-09T00:00:00Z")
        val expectedWindowEndTime = getInstant("2020-02-08T00:00:00Z")

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Success) { "Init metadata returned unexpected results" }

            val metadata = initMetadataResult.metadata
            assertNotNull(metadata.continuous)
            assertEquals(expectedWindowStartTime, metadata.continuous!!.nextWindowStartTime)
            assertEquals(expectedWindowEndTime, metadata.continuous!!.nextWindowEndTime)
        }
    }

    fun `test metadata init when getting existing metadata fails`() {
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = randomAlphaOfLength(10)
        )

        val getException = Exception("Test failure")
        val client: Client = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetResponse>>(1)
                listener.onFailure(getException)
            }.whenever(this.mock).get(any(), any())
        }
        val metadataService = RollupMetadataService(client, xContentRegistry)

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Failure) { "Init metadata returned unexpected results" }

            assertEquals(getException, initMetadataResult.cause)
        }
    }

    fun `test metadata init when indexing new metadata fails`() {
        val rollup = randomRollup().copy(
            enabled = true,
            jobEnabledTime = Instant.now(),
            metadataID = null
        )

        val indexException = Exception("Test failure")
        val firstDocTimestamp = Instant.now().toString()
        val client = getClient(
            searchResponse = getSearchResponseForTimestamp(rollup, firstDocTimestamp),
            searchException = null,
            indexResponse = null,
            indexException = indexException
        )
        val metadataService = RollupMetadataService(client, xContentRegistry)

        runBlocking {
            val initMetadataResult = metadataService.init(rollup)
            require(initMetadataResult is MetadataResult.Failure) { "Init metadata returned unexpected results" }

            assertEquals(indexException, initMetadataResult.cause)
        }
    }

    // TODO: This test is failing with a thread leak error: "There are still zombie threads that couldn't be terminated"
    //   May be due to the use of BytesArray as it didn't start until after that was added
    fun `skip test get existing metadata`() {
        val metadata = RollupMetadata(
            id = randomAlphaOfLength(10),
            seqNo = 0,
            primaryTerm = 1,
            rollupID = randomAlphaOfLength(10),
            // Truncating to seconds since not doing so causes milliseconds mismatch when comparing results
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.SECONDS),
            status = RollupMetadata.Status.INIT,
            stats = RollupStats(0, 0, 0, 0, 0)
        )

        val getResponse: GetResponse = mock()
        val source: BytesReference = BytesArray(metadata.toJsonString(params = ToXContent.MapParams(mapOf(WITH_TYPE to "true"))))
        whenever(getResponse.isExists).doReturn(true)
        whenever(getResponse.seqNo).doReturn(metadata.seqNo)
        whenever(getResponse.primaryTerm).doReturn(metadata.primaryTerm)
        whenever(getResponse.id).doReturn(metadata.id)
        whenever(getResponse.sourceAsBytesRef).doReturn(source)

        val client: Client = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetResponse>>(1)
                listener.onResponse(getResponse)
            }.whenever(this.mock).get(any(), any())
        }
        val metadataService = RollupMetadataService(client, xContentRegistry)

//        runBlocking {
//            val getExistingMetadataResult = metadataService.getExistingMetadata(metadata.id)
//            require(getExistingMetadataResult is MetadataResult.Success) {
//                "Getting existing metadata returned unexpected results"
//            }
//
//            assertEquals(metadata, getExistingMetadataResult.metadata)
//        }
    }

    fun `test get existing metadata when metadata does not exist`() {
        val getResponse: GetResponse = mock()
        whenever(getResponse.isExists).doReturn(true)

        val client: Client = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetResponse>>(1)
                listener.onResponse(getResponse)
            }.whenever(this.mock).get(any(), any())
        }
        val metadataService = RollupMetadataService(client, xContentRegistry)

        runBlocking {
            val getExistingMetadataResult = metadataService.getExistingMetadata(randomRollup()
                .copy(id = randomAlphaOfLength(10), metadataID = randomAlphaOfLength(10)))
            require(getExistingMetadataResult is MetadataResult.NoMetadata) {
                "Getting existing metadata returned unexpected results"
            }
        }
    }

    // TODO: This can be split into multiple tests if the exceptions being caught are handled
    //   differently, for now just assuming a general exception is thrown
    fun `test get existing metadata fails`() {
        val getException = Exception("Test failure")

        val client: Client = mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<GetResponse>>(1)
                listener.onFailure(getException)
            }.whenever(this.mock).get(any(), any())
        }
        val metadataService = RollupMetadataService(client, xContentRegistry)

        runBlocking {
            val getExistingMetadataResult = metadataService.getExistingMetadata(randomRollup()
                .copy(id = randomAlphaOfLength(10), metadataID = randomAlphaOfLength(10)))
            require(getExistingMetadataResult is MetadataResult.Failure) {
                "Getting existing metadata returned unexpected results"
            }

            assertEquals(getException, getExistingMetadataResult.cause)
        }
    }

    // TODO: Test for a document timestamp before epoch
    // TODO: Test non-continuous metadata

    // Return a SearchResponse containing a single document with the given timestamp
    // Used to mock the search performed when initializing continuous rollup metadata
    private fun getSearchResponseForTimestamp(rollup: Rollup, timestamp: String): SearchResponse {
        val dateHistogram = rollup.dimensions.first() as DateHistogram

        // TODO: Mockito 2 supposedly should be able to mock final classes but there were errors when trying to do so
        //   Will need to check if there is a workaround or a better way to mock getting hits.hits since this current approach is verbose
        val docField = DocumentField(dateHistogram.sourceField, listOf(getInstant(timestamp).toEpochMilli().toString()))
        val searchHit = SearchHit(0)
        searchHit.setDocumentField(dateHistogram.sourceField, docField)
        val searchHits = SearchHits(arrayOf(searchHit), null, 0.0F)

        val searchResponse: SearchResponse = mock()
        whenever(searchResponse.hits).doReturn(searchHits)

        return searchResponse
    }

    private fun getIndexResponse(result: DocWriteResponse.Result = DocWriteResponse.Result.CREATED): IndexResponse {
        val indexResponse: IndexResponse = mock()
        whenever(indexResponse.result).doReturn(result)
        // TODO: Should change the following mock values if the result is a failed one
        whenever(indexResponse.id).doReturn("test")
        whenever(indexResponse.seqNo).doReturn(0L)
        whenever(indexResponse.primaryTerm).doReturn(1L)

        return indexResponse
    }

    private fun getClient(
        searchResponse: SearchResponse?,
        searchException: Exception?,
        indexResponse: IndexResponse?,
        indexException: Exception?
    ): Client {
        assertTrue("Must provide either a searchResponse or searchException", (searchResponse != null).xor(searchException != null))
        assertTrue("Must provide either an indexResponse or indexException", (indexResponse != null).xor(indexException != null))
        return mock {
            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<SearchResponse>>(1)
                if (searchResponse != null) listener.onResponse(searchResponse)
                else listener.onFailure(searchException)
            }.whenever(this.mock).search(any(), any())

            doAnswer { invocationOnMock ->
                val listener = invocationOnMock.getArgument<ActionListener<IndexResponse>>(1)
                if (indexResponse != null) listener.onResponse(indexResponse)
                else listener.onFailure(indexException)
            }.whenever(this.mock).index(any(), any())
        }
    }

    private fun getInstant(timestamp: String) = ZonedDateTime.parse(timestamp).toInstant()

    private fun localDateAtTimezone(localTime: String, timezone: ZoneId) =
        LocalDateTime.parse(localTime).atZone(timezone).toInstant()
}
