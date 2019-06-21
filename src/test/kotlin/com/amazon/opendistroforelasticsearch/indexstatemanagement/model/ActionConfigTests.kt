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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.actions.ActionRetry
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.actions.ActionTimeout
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class ActionConfigTests : ESTestCase() {

    fun `test invalid timeout for delete action config fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for invalid timeout") {
            ActionTimeout(timeout = TimeValue.parseTimeValue("invalidTimeout", "timeout_test"))
        }
    }

    fun `test action retry count of zero fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for retry count less than 1") {
            ActionRetry(count = 0)
        }
    }

    fun `test invalid action retry delay fails`() {
        assertFailsWith(
            IllegalArgumentException::class,
            "Expected IllegalArgumentException for invalid action retry delay") {
            ActionRetry(count = 3, delay = TimeValue.parseTimeValue("invalidDelay", "delay_test"))
        }
    }
}
