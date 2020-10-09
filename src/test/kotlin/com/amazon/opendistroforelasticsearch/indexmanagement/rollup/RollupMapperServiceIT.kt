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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule
import org.elasticsearch.client.Request
import org.elasticsearch.common.settings.Settings
import java.time.Instant
import java.time.temporal.ChronoUnit

class RollupMapperServiceIT : RollupRestTestCase() {

    fun `skip test rollup index is created`() {
        val rollup = createRollup(randomRollup().copy(jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES), enabled = true, jobEnabledTime = Instant.now()))

        updateRollupStartTime(rollup)

        waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
        }
    }

    fun `nope test some rollups`() {
        // insertSampleData("example_index", 100)
        // val rollup = createRollup(randomRollup().copy(jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES), enabled = true, jobEnabledTime = Instant.now()))
        val now = Instant.now().toEpochMilli()
        // TODO: this took 45min of time because MAPPINGS WERE WROOOONG, need checks in source code
        logger.info("now is $now")
        createIndex("source", Settings.builder().build(), "\"properties\": {\n" +
            "      \"timestamp\": {\n" +
            "        \"type\": \"date\" \n" +
            "      }\n" +
            "    }")
        var bulkJsonString = ""
        // 3 cases
        // now is the millisecond before new hour -> go back exactly
        // now is exactly the new hour
        // now is the millisecond after the new hour
        for (i in 1..60) {
            // a doc every (1 * i) minutes ago
            bulkJsonString += "{ \"index\" : {} }\n{ \"timestamp\" : \"${now - (i * 1000 * 60)}\" }\n"
        }
        // logger.info(bulkJsonString)
        insertSampleBulkData("source", bulkJsonString)
        val request = Request("GET", "/source/_search")
        val response = client().performRequest(request)
        logger.info("response rest status ${response.restStatus()}")
        logger.info("response is2 ${response.asMap()}")

        var rollup = Rollup(
            enabled = true,
            schemaVersion = 1L,
            jobSchedule = IntervalSchedule(Instant.now(), 1, ChronoUnit.MINUTES),
            jobLastUpdatedTime = Instant.now(),
            jobEnabledTime = Instant.now(),
            description = "test rollup",
            sourceIndex = "source",
            targetIndex = "target",
            metadataID = null,
            roles = emptyList(),
            pageSize = 10,
            delay = 0,
            continuous = false,
            dimensions = listOf(DateHistogram(sourceField = "timestamp", fixedInterval = "1h")),
            metrics = emptyList()
        )
        rollup = createRollup(rollup)

        updateRollupStartTime(rollup)

        waitFor {
            assertTrue("Target rollup index was not created", indexExists(rollup.targetIndex))
        }

        Thread.sleep(10_000)

        val request1 = Request("GET", "/target/_search")
        val response1 = client().performRequest(request1)
        logger.info("response target rest status ${response1.restStatus()}")
        logger.info("response target is2 ${response1.asMap()}")

        val sourceIndexMapping = client().getIndexMapping("source")
        logger.info("sourceIndexMapping $sourceIndexMapping")

        val targetIndexMapping = client().getIndexMapping("target")
        logger.info("targetIndexMapping $targetIndexMapping")

        fail("yo")
    }
}