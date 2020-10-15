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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomTermQuery
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.ConstantScoreQueryBuilder
import org.elasticsearch.index.query.DisMaxQueryBuilder
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.RangeQueryBuilder
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.query.TermsQueryBuilder
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class RollupUtilsTests : ESTestCase() {

    fun `test rewriteQueryBuilder unsupported query`() {
        assertFailsWith(UnsupportedOperationException::class, "FunctionScoreQuery is not supported") {
            val filterFunctionBuilders = arrayOf<FunctionScoreQueryBuilder.FilterFunctionBuilder>()
            val queryBuilder = FunctionScoreQueryBuilder(filterFunctionBuilders)
            randomRollup().rewriteQueryBuilder(queryBuilder, mapOf())
        }
    }

    fun `test rewriteQueryBuilder term query`() {
        val termQuery = randomTermQuery()
        termQuery.queryName("dummy-query")
        termQuery.boost(0.4f)
        val actual = randomRollup().rewriteQueryBuilder(termQuery, mapOf()) as TermQueryBuilder
        assertEquals(termQuery.boost(), actual.boost())
        assertEquals(termQuery.queryName(), actual.queryName())
        assertEquals(termQuery.fieldName() + ".terms", actual.fieldName())
        assertEquals(termQuery.value(), actual.value())
    }

    fun `test rewriteQueryBuilder terms query`() {
        val termsQuery = TermsQueryBuilder("some-field", "some-value")
        termsQuery.queryName("dummy-query")
        termsQuery.boost(0.4f)
        val actual = randomRollup().rewriteQueryBuilder(termsQuery, mapOf()) as TermsQueryBuilder
        assertEquals(termsQuery.boost(), actual.boost())
        assertEquals(termsQuery.queryName(), actual.queryName())
        assertEquals(termsQuery.fieldName() + ".terms", actual.fieldName())
        assertEquals(termsQuery.values(), actual.values())
    }

    fun `test rewriteQueryBuilder range query`() {
        val rangeQuery = RangeQueryBuilder("some-field")
        rangeQuery.includeUpper(false)
        rangeQuery.includeLower(true)
        rangeQuery.to("2020-10-10")
        rangeQuery.from("2020-10-01")
        rangeQuery.timeZone("UTC")
        rangeQuery.format("YYYY-MM-DD")
        rangeQuery.queryName("some-query")
        rangeQuery.boost(0.5f)
        val fieldMapping = mapOf("some-field" to "dummy-mapping")
        val actual = randomRollup().rewriteQueryBuilder(rangeQuery, fieldMapping) as RangeQueryBuilder
        assertEquals(rangeQuery.queryName(), actual.queryName())
        assertEquals(rangeQuery.boost(), actual.boost())
        assertEquals(rangeQuery.includeLower(), actual.includeLower())
        assertEquals(rangeQuery.includeUpper(), actual.includeUpper())
        assertEquals(rangeQuery.timeZone(), actual.timeZone())
        assertEquals(rangeQuery.format(), actual.format())
        assertEquals(rangeQuery.fieldName() + ".dummy-mapping", actual.fieldName())
    }

    fun `test rewriteQueryBuilder matchAll query`() {
        val matchAllQuery = MatchAllQueryBuilder()
        val actual = randomRollup().rewriteQueryBuilder(matchAllQuery, mapOf()) as MatchAllQueryBuilder
        assertEquals(matchAllQuery, actual)
    }

    fun `test rewriteQueryBuilder bool query`() {
        val boolQuery = BoolQueryBuilder()
        val mustQuery = randomTermQuery()
        val mustNotQuery = randomTermQuery()
        val shouldQuery = randomTermQuery()
        val filterQuery = randomTermQuery()
        boolQuery.minimumShouldMatch(1)
        boolQuery.adjustPureNegative(false)
        boolQuery.queryName("some-query")
        boolQuery.boost(1f)
        boolQuery.must(mustQuery)
        boolQuery.mustNot(mustNotQuery)
        boolQuery.should(shouldQuery)
        boolQuery.filter(filterQuery)

        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(boolQuery, mapOf()) as BoolQueryBuilder
        assertEquals(boolQuery.queryName(), actual.queryName())
        assertEquals(boolQuery.boost(), actual.boost())
        assertEquals(boolQuery.adjustPureNegative(), actual.adjustPureNegative())
        assertEquals(boolQuery.minimumShouldMatch(), actual.minimumShouldMatch())
        assertEquals(boolQuery.filter().size, actual.filter().size)
        assertEquals(boolQuery.must().size, actual.must().size)
        assertEquals(boolQuery.mustNot().size, actual.mustNot().size)
        assertEquals(boolQuery.should().size, actual.should().size)
        assertEquals(rollup.rewriteQueryBuilder(filterQuery, mapOf()), actual.filter().first())
        assertEquals(rollup.rewriteQueryBuilder(mustQuery, mapOf()), actual.must().first())
        assertEquals(rollup.rewriteQueryBuilder(mustNotQuery, mapOf()), actual.mustNot().first())
        assertEquals(rollup.rewriteQueryBuilder(shouldQuery, mapOf()), actual.should().first())
    }

    fun `test rewriteQueryBuilder constant score query`() {
        val innerQuery = randomTermQuery()
        val constantQuery = ConstantScoreQueryBuilder(innerQuery)
        constantQuery.queryName("some-query")
        constantQuery.boost(0.2f)
        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(constantQuery, mapOf()) as ConstantScoreQueryBuilder
        assertEquals(constantQuery.queryName(), actual.queryName())
        assertEquals(constantQuery.boost(), actual.boost())
        assertEquals(rollup.rewriteQueryBuilder(innerQuery, mapOf()), actual.innerQuery())
    }

    fun `test rewriteQueryBuilder disMax query`() {
        val innerQuery = randomTermQuery()
        val disMaxQuery = DisMaxQueryBuilder()
        disMaxQuery.add(innerQuery)
        disMaxQuery.tieBreaker(0.3f)
        disMaxQuery.queryName("some-query")
        disMaxQuery.boost(0.1f)
        val rollup = randomRollup()
        val actual = rollup.rewriteQueryBuilder(disMaxQuery, mapOf()) as DisMaxQueryBuilder
        assertEquals(disMaxQuery.queryName(), actual.queryName())
        assertEquals(disMaxQuery.boost(), actual.boost())
        assertEquals(disMaxQuery.tieBreaker(), actual.tieBreaker())
        assertEquals(disMaxQuery.innerQueries().size, actual.innerQueries().size)
        assertEquals(rollup.rewriteQueryBuilder(innerQuery, mapOf()), actual.innerQueries().first())
    }

    fun `test populateFieldMappings`() {
        val rollup = randomRollup()
        val expected = mutableSetOf<RollupFieldMapping>()
        rollup.dimensions.forEach {
            expected.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, it.sourceField, it.type.type))
        }
        rollup.metrics.forEach { rollupMetric ->
            rollupMetric.metrics.forEach { metric ->
                expected.add(RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, rollupMetric.sourceField, metric.type.type))
            }
        }
        val actual = rollup.populateFieldMappings()
        assertEquals(expected, actual)
    }

    fun `test buildRollupQuery`() {
        val rollup = randomRollup()
        val queryBuilder = MatchAllQueryBuilder()
        val actual = rollup.buildRollupQuery(mapOf(), queryBuilder) as BoolQueryBuilder
        val expectedFilter = TermQueryBuilder("rollup._id", rollup.id)
        assertTrue(actual.should().isEmpty())
        assertTrue(actual.mustNot().isEmpty())
        assertFalse(actual.filter().isEmpty())
        assertFalse(actual.must().isEmpty())
        assertEquals(1, actual.must().size)
        assertEquals(rollup.rewriteQueryBuilder(queryBuilder, mapOf()), actual.must().first())
        assertEquals(1, actual.filter().size)
        assertEquals(expectedFilter, actual.filter().first())

    }
}
