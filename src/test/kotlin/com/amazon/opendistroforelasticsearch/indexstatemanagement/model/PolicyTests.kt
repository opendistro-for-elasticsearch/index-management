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

import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomTransition
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class PolicyTests : ESTestCase() {

    fun `test invalid default state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for invalid default state") {
            randomPolicy().copy(defaultState = "definitely not this")
        }
    }

    fun `test empty states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for empty states") {
            randomPolicy().copy(states = emptyList())
        }
    }

    fun `test duplicate states`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for duplicate states") {
            val states = listOf(randomState(name = "duplicate"), randomState(), randomState(name = "duplicate"))
            randomPolicy(states = states)
        }
    }

    fun `test transition pointing to nonexistent state`() {
        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException for nonexistent transition state") {
            val states = listOf(randomState(transitions = listOf(randomTransition(stateName = "doesnt exist"))), randomState(), randomState())
            randomPolicy(states = states)
        }
    }
}
