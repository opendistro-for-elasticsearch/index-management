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
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.hashToFixedSize
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
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.transport.RemoteTransportException

@SuppressWarnings("ReturnCount")
class TransformMetadataService(private val esClient: Client, val xContentRegistry: NamedXContentRegistry) {

    private val logger = LogManager.getLogger(javaClass)

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun getMetadata(transform: Transform): TransformMetadata {
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
            // TODO: Should we attempt to create a new document instead if failed to parse, the only reason this can happen is if someone deleted
            //  the metadata doc?
            transformMetadata ?: throw TransformMetadataException("Failed to parse the existing metadata document")
        } else {
            logger.debug("Creating metadata doc as none exists at the moment for transform job [${transform.id}]")
            createMetadata(transform)
        }
    }

    private suspend fun createMetadata(transform: Transform): TransformMetadata {
        val id = hashToFixedSize("TransformMetadata#${transform.id}")
        val metadata = TransformMetadata(
            id = id,
            transformId = transform.id,
            lastUpdatedAt = Instant.now(),
            status = TransformMetadata.Status.INIT,
            stats = TransformStats(0, 0, 0, 0, 0)
        )
        return writeMetadata(metadata)
    }

    @Suppress("BlockingMethodInNonBlockingContext")
    suspend fun writeMetadata(metadata: TransformMetadata, updating: Boolean = false): TransformMetadata {
        val errorMessage = "Failed to ${if (updating) "update" else "create"} metadata doc ${metadata.id} for transform job ${metadata.transformId}"
        try {
            val builder = metadata.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS)
            val indexRequest = IndexRequest(IndexManagementPlugin.INDEX_MANAGEMENT_INDEX)
                .source(builder)
                .id(metadata.id)
                .routing(metadata.transformId)
            if (updating) {
                indexRequest.setIfSeqNo(metadata.seqNo).setIfPrimaryTerm(metadata.primaryTerm)
            } else {
                indexRequest.opType(DocWriteRequest.OpType.CREATE)
            }

            val response: IndexResponse = esClient.suspendUntil { index(indexRequest, it) }
            return when (response.result) {
                DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED -> {
                    metadata.copy(seqNo = response.seqNo, primaryTerm = response.primaryTerm)
                }
                else -> {
                    logger.error(errorMessage)
                    throw TransformMetadataException("Failed to write metadata, received ${response.result?.lowercase} status")
                }
            }
        } catch (e: RemoteTransportException) {
            val unwrappedException = ExceptionsHelper.unwrapCause(e) as Exception
            logger.error(errorMessage, unwrappedException)
            throw TransformMetadataException(errorMessage, unwrappedException)
        } catch (e: Exception) {
            logger.error(errorMessage, e)
            throw TransformMetadataException(errorMessage, e)
        }
    }
}
