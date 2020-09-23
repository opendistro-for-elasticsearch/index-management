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

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Dimension
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Metric
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomAverage
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomDateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomMax
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomMin
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomSum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomTerms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.toJsonString
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class XContentTests : ESTestCase() {

    fun `test invalid dimension parsing`() {
        assertFailsWith(IllegalArgumentException::class, "Invalid dimension type [invalid_dimension] found in dimensions") {
            Dimension.parse(parser("{\"invalid_dimension\":{}}"))
        }
    }

    fun `test date histogram dimension parsing`() {
        val dateHistogram = randomDateHistogram()
        val dateHistogramString = dateHistogram.toJsonString()
        val parsedDateHistogram = Dimension.parse(parser(dateHistogramString))
        assertEquals("Round tripping Date Histogram doesn't work", dateHistogram, parsedDateHistogram)
    }

    fun `test histogram dimension parsing`() {
        val histogram = randomHistogram()
        val histogramString = histogram.toJsonString()
        val parsedHistogram = Dimension.parse(parser(histogramString))
        assertEquals("Round tripping Histogram doesn't work", histogram, parsedHistogram)
    }

    fun `test terms dimension parsing`() {
        val terms = randomTerms()
        val termsString = terms.toJsonString()
        val parsedTerms = Dimension.parse(parser(termsString))
        assertEquals("Round tripping Terms doesn't work", terms, parsedTerms)
    }

    fun `test invalid metric parsing`() {
        assertFailsWith(IllegalArgumentException::class, "Invalid metric type: [invalid_metric] found in rollup metrics") {
            Metric.parse(parser("{\"invalid_metric\":{}}"))
        }
    }

    fun `test average metric parsing`() {
        val avg = randomAverage()
        val avgString = avg.toJsonString()
        val parsedAvg = Metric.parse(parser(avgString))
        assertEquals("Round tripping Avg doesn't work", avg, parsedAvg)
    }

    fun `test max metric parsing`() {
        val max = randomMax()
        val maxString = max.toJsonString()
        val parsedMax = Metric.parse(parser(maxString))
        assertEquals("Round tripping Max doesn't work", max, parsedMax)
    }

    fun `test min metric parsing`() {
        val min = randomMin()
        val minString = min.toJsonString()
        val parsedMin = Metric.parse(parser(minString))
        assertEquals("Round tripping Min doesn't work", min, parsedMin)
    }

    fun `test sum metric parsing`() {
        val sum = randomSum()
        val sumString = sum.toJsonString()
        val parsedSum = Metric.parse(parser(sumString))
        assertEquals("Round tripping Sum doesn't work", sum, parsedSum)
    }

    fun `test value_count metric parsing`() {
        val valueCount = randomValueCount()
        val valueCountString = valueCount.toJsonString()
        val parsedValueCount = Metric.parse(parser(valueCountString))
        assertEquals("Round tripping ValueCount doesn't work", valueCount, parsedValueCount)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
