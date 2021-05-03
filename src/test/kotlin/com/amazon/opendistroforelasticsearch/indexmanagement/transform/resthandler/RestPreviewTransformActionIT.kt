/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.TRANSFORM_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.common.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.TransformRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.client.ResponseException
import org.elasticsearch.index.IndexNotFoundException
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.junit.AfterClass
import org.junit.Before

@Suppress("UNCHECKED_CAST")
class RestPreviewTransformActionIT : TransformRestTestCase() {

    private val factories = AggregatorFactories.builder()
        .addAggregator(AggregationBuilders.sum("revenue").field("total_amount"))
        .addAggregator(AggregationBuilders.percentiles("passengerCount").field("passenger_count").percentiles(90.0, 95.0))
    private val transform = randomTransform().copy(
        sourceIndex = sourceIndex,
        groups = listOf(
            Terms(sourceField = "PULocationID", targetField = "location")
        ),
        aggregations = factories
    )

    @Before
    fun setupData() {
        var indexExists = false
        try {
            indexExists = indexExists(sourceIndex)
        } catch (e: IndexNotFoundException) {
        }
        if (!indexExists) {
            generateNYCTaxiData(sourceIndex)
        }
    }

    companion object {
        private const val sourceIndex = "transform-preview-api"

        @AfterClass
        @JvmStatic
        fun deleteData() {
            deleteIndex(sourceIndex)
        }
    }

    fun `test preview`() {
        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        val expectedKeys = setOf("revenue", "passengerCount", "location", "transform._doc_count")
        assertEquals("Preview transform failed", RestStatus.OK, response.restStatus())
        val transformedDocs = response.asMap()["documents"] as List<Map<String, Any>>
        assertEquals("Transformed docs have unexpected schema", expectedKeys, transformedDocs.first().keys)
    }

    // TODO: Not sure if we should validate on source indices instead of returning empty result.
    fun `test mismatched columns`() {
        val factories = AggregatorFactories.builder()
            .addAggregator(AggregationBuilders.sum("revenue").field("total_amountdzdfd"))
        val transform = transform.copy(
            groups = listOf(Terms(sourceField = "non-existent", targetField = "non-existent")),
            aggregations = factories
        )
        val response = client().makeRequest(
            "POST",
            "$TRANSFORM_BASE_URI/_preview",
            emptyMap(),
            transform.toHttpEntity()
        )
        assertEquals("Unexpected status", RestStatus.OK, response.restStatus())
    }

    fun `test nonexistent source index`() {
        val transform = transform.copy(sourceIndex = "non-existent-index")
        try {
            client().makeRequest(
                "POST",
                "$TRANSFORM_BASE_URI/_preview",
                emptyMap(),
                transform.toHttpEntity()
            )
            fail("expected exception")
        } catch (e: ResponseException) {
            assertEquals("Unexpected failure code", RestStatus.NOT_FOUND, e.response.restStatus())
        }
    }
}