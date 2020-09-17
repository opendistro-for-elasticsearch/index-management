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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionRetry
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionTimeout
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput
import org.elasticsearch.common.unit.ByteSizeValue
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.test.ESTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.assertFailsWith

class ActionConfigTests : ESTestCase() {

    fun `test invalid timeout for delete action config fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid timeout") {
            ActionTimeout(timeout = TimeValue.parseTimeValue("invalidTimeout", "timeout_test"))
        }
    }

    fun `test action retry count of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for retry count less than 1") {
            ActionRetry(count = 0)
        }
    }

    fun `test rollover action minimum size of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minSize less than 1") {
            randomRolloverActionConfig(minSize = ByteSizeValue.parseBytesSizeValue("0", "min_size_test"))
        }
    }

    fun `test rollover action minimum doc count of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for minDoc less than 1") {
            randomRolloverActionConfig(minDocs = 0)
        }
    }

    fun `test force merge action max num segments of zero fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for maxNumSegments less than 1") {
            randomForceMergeActionConfig(maxNumSegments = 0)
        }
    }

    fun `test snapshot action empty snapshot fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for snapshot equals to null") {
            randomSnapshotActionConfig(repository = "repository")
        }
    }

    fun `test snapshot action empty repository fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for repository equals to null") {
            randomSnapshotActionConfig(snapshot = "snapshot")
        }
    }

    fun `test allocation action empty parameters fails`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty parameters") {
            randomAllocationActionConfig()
        }
    }

    fun `test rollover action round trip`() {
        roundTripActionConfig(randomRolloverActionConfig())
    }

    fun `test replica count action round trip`() {
        roundTripActionConfig(randomReplicaCountActionConfig())
    }

    fun `test force merge action round trip`() {
        roundTripActionConfig(randomForceMergeActionConfig())
    }

    fun `test notification action round trip`() {
        roundTripActionConfig(randomNotificationActionConfig())
    }

    fun `test snapshot action round trip`() {
        roundTripActionConfig(randomSnapshotActionConfig(snapshot = "snapshot", repository = "repository"))
    }

    fun `test index priority action round trip`() {
        roundTripActionConfig(randomIndexPriorityActionConfig())
    }

    fun `test allocation action round trip`() {
        roundTripActionConfig(randomAllocationActionConfig(require = mapOf("box_type" to "hot")))
    }

    private fun roundTripActionConfig(expectedActionConfig: ActionConfig) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedActionConfig.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualActionConfig = ActionConfig.fromStreamInput(input)
        assertEquals(expectedActionConfig, actualActionConfig)
    }
}
