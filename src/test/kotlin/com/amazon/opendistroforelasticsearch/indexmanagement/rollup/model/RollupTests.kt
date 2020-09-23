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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomDateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomTerms
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class RollupTests : ESTestCase() {
    fun `test rollup same indices`() {
        assertFailsWith(IllegalArgumentException::class, "Your source and target index cannot be the same") {
            randomRollup().copy(sourceIndex = "the_same", targetIndex = "the_same")
        }
    }

    fun `test rollup requires precisely one date histogram`() {
        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = listOf(randomTerms()))
        }

        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = emptyList())
        }

        assertFailsWith(IllegalArgumentException::class, "Must specify precisely one date histogram dimension") {
            randomRollup().copy(dimensions = listOf(randomDateHistogram(), randomDateHistogram()))
        }
    }

    fun `test rollup requires first dimension to be date histogram`() {
        assertFailsWith(IllegalArgumentException::class, "The first dimension must be a date histogram") {
            randomRollup().copy(dimensions = listOf(randomTerms(), randomDateHistogram()))
        }
    }

    fun `test rollup requires job enabled time if its enabled`() {
        assertFailsWith(IllegalArgumentException::class, "Job enabled time must be present if the job is enabled") {
            randomRollup().copy(enabled = true, jobEnabledTime = null)
        }
    }

    fun `test rollup requires no job enabled time if its disabled`() {
        assertFailsWith(IllegalArgumentException::class, "Job enabled time must not be present if the job is disabled") {
            randomRollup().copy(enabled = false, jobEnabledTime = randomInstant())
        }
    }
}