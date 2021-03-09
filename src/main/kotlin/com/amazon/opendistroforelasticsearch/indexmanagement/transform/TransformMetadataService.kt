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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.exceptions.TransformMetadataException
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.Transform
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.transform.model.TransformStats
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import java.time.Instant
import org.elasticsearch.action.DocWriteResponse

@SuppressWarnings("ReturnCount")
class TransformMetadataService(private val esClient: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun getMetadata(transform: Transform): TransformMetadata {
        try {
            return if (transform.metadataId != null) {
                // update metadata
                val getRequest = GetRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX, transform.metadataId).routing(transform.id)
                val response: GetResponse = esClient.suspendUntil { get(getRequest, it) }
                val metadataSource = response.sourceAsBytesRef
                val transformMetadata = metadataSource?.let {
                    withContext(Dispatchers.IO) {
                        val xcp = XContentHelper.createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, metadataSource, XContentType.JSON)
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, TransformMetadata.Companion::parse)
                    }
                }
                // TODO: Should we attempt to create a new document instead if failed to parse?
                transformMetadata ?: throw Exception("Failed to parse the existing metadata document ${transform.metadataId}")
            } else {
                createMetadata(transform)
            }
        } catch (e: Exception) {
            logger.debug("Error when getting transform metadata [${transform.metadataId}]: $e")
            throw TransformMetadataException("Failed to get the metadata for the transform", e)
        }
    }

    private suspend fun createMetadata(transform: Transform): TransformMetadata {
        val metadata = TransformMetadata(transformId = transform.id, lastUpdatedAt = Instant.now(), status = TransformMetadata.Status.INIT, stats =
        TransformStats(0, 0, 0, 0, 0))
        writeMetadata(metadata)
        return metadata
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun writeMetadata(metadata: TransformMetadata, updating: Boolean = false) {
        val builder = XContentFactory.jsonBuilder().startObject()
            .field(TransformMetadata.TRANSFORM_METADATA_TYPE, metadata)
            .endObject()
        val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX).source(builder).routing(metadata.transformId)
        if (updating) {
            indexRequest.id(metadata.id).setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
        } else {
            indexRequest.opType(DocWriteRequest.OpType.CREATE)
        }

        val response: IndexResponse = esClient.suspendUntil { index(indexRequest, it) }
        when (response.result) {
            DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {}
            else -> {
                throw TransformMetadataException("Failed to write metadata, received ${response.result?.lowercase} status")
            }
        }
    }
}
