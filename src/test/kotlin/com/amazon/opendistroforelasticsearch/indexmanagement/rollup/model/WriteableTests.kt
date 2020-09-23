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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomAverage
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomMax
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomMin
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomSum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomValueCount
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class WriteableTests : ESTestCase() {

    fun `test average metric as stream`() {
        val avg = randomAverage()
        val out = BytesStreamOutput().also { avg.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedAvg = Average(sin)
        assertEquals("Round tripping Average stream doesn't work", avg, streamedAvg)
    }

    fun `test max metric as stream`() {
        val max = randomMax()
        val out = BytesStreamOutput().also { max.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedMax = Max(sin)
        assertEquals("Round tripping Max stream doesn't work", max, streamedMax)
    }

    fun `test min metric as stream`() {
        val min = randomMin()
        val out = BytesStreamOutput().also { min.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedMin = Min(sin)
        assertEquals("Round tripping Min stream doesn't work", min, streamedMin)
    }

    fun `test sum metric as stream`() {
        val sum = randomSum()
        val out = BytesStreamOutput().also { sum.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedSum = Sum(sin)
        assertEquals("Round tripping Sum stream doesn't work", sum, streamedSum)
    }

    fun `test value_count metric as stream`() {
        val valueCount = randomValueCount()
        val out = BytesStreamOutput().also { valueCount.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedValueCount = ValueCount(sin)
        assertEquals("Round tripping ValueCount stream doesn't work", valueCount, streamedValueCount)
    }
}