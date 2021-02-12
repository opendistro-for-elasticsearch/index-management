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

@file:Suppress("TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.elasticapi

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.contentParser
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.managedIndexMetadataID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.NoShardAvailableActionException
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentFragment
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.Index
import org.elasticsearch.index.IndexNotFoundException

private val log = LogManager.getLogger("Index Management Helper")

/**
 * Returns the current rollover_alias if it exists otherwise returns null.
 */
fun IndexMetadata.getRolloverAlias(): String? {
    if (this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key).isNullOrBlank()) return null

    return this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key)
}

fun IndexMetadata.getManagedIndexMetaData(): ManagedIndexMetaData? {
    val existingMetaDataMap = this.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA_TYPE)

    if (existingMetaDataMap != null) {
        return ManagedIndexMetaData.fromMap(existingMetaDataMap)
    }
    return null
}

fun getUuidsForClosedIndices(state: ClusterState): MutableList<String> {
    val indexMetadatas = state.metadata.indices
    val closeList = mutableListOf<String>()
    indexMetadatas.forEach {
        // it.key is index name
        if (it.value.state == IndexMetadata.State.CLOSE) {
            closeList.add(it.value.indexUUID)
        }
    }
    return closeList
}

/**
 * Do a exists search query to retrieve all policy with ism_template field
 * parse search response with this function
 *
 * @return map of policyID to ISMTemplate in this policy
 * @throws [IllegalArgumentException]
 */
@Throws(Exception::class)
fun getPolicyToTemplateMap(response: SearchResponse, xContentRegistry: NamedXContentRegistry = NamedXContentRegistry.EMPTY):
    Map<String, ISMTemplate?> {
    return response.hits.hits.map {
        val id = it.id
        val seqNo = it.seqNo
        val primaryTerm = it.primaryTerm
        val xcp = XContentFactory.xContent(XContentType.JSON)
            .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, it.sourceAsString)
        xcp.parseWithType(id, seqNo, primaryTerm, Policy.Companion::parse)
            .copy(id = id, seqNo = seqNo, primaryTerm = primaryTerm)
    }.map { it.id to it.ismTemplate }.toMap()
}

@Suppress("UNCHECKED_CAST")
fun <K, V> Map<K, V?>.filterNotNullValues(): Map<K, V> =
    filterValues { it != null } as Map<K, V>

// get metadata from config index using doc id
@Suppress("ReturnCount")
suspend fun IndexMetadata.getManagedIndexMetaData(client: Client): ManagedIndexMetaData? {
    try {
        val getRequest = GetRequest(INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(indexUUID))
            .routing(this.indexUUID)
        val getResponse: GetResponse = client.suspendUntil { get(getRequest, it) }
        if (!getResponse.isExists || getResponse.isSourceEmpty) {
            return null
        }

        return withContext(Dispatchers.IO) {
            val xcp = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                getResponse.sourceAsBytesRef, XContentType.JSON)
            ManagedIndexMetaData.parseWithType(xcp, getResponse.id, getResponse.seqNo, getResponse.primaryTerm)
        }
    } catch (e: Exception) {
        when (e) {
            is IndexNotFoundException, is NoShardAvailableActionException -> {
                log.error("Failed to get metadata because no index or shard not available")
            }
            else -> log.error("Failed to get metadata", e)
        }

        return null
    }
}

/**
 * multi-get metadata for indices
 *
 * @return list of metadata
 */
suspend fun Client.mgetManagedIndexMetadata(indices: List<Index>): List<ManagedIndexMetaData?> {
    log.debug("trying to get back metadata for indices ${indices.map { it.name }}")

    if (indices.isEmpty()) return emptyList()

    val mgetRequest = MultiGetRequest()
    indices.forEach {
        mgetRequest.add(MultiGetRequest.Item(
            INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it.uuid)).routing(it.uuid))
    }
    var mgetMetadataList = mutableListOf<ManagedIndexMetaData?>()
    try {
        val response: MultiGetResponse = this.suspendUntil { multiGet(mgetRequest, it) }
        mgetMetadataList = mgetResponseToList(response)
    } catch (e: ActionRequestValidationException) {
        log.info("No documents to get back metadata, ${e.message}")
    }
    return mgetMetadataList
}

/**
 * transform multi-get response to list for ManagedIndexMetaData
 */
fun mgetResponseToList(mgetResponse: MultiGetResponse): MutableList<ManagedIndexMetaData?> {
    val mgetList = mutableListOf<ManagedIndexMetaData?>()
    mgetResponse.responses.forEach {
        if (it.response != null && !it.response.isSourceEmpty) {
            val xcp = contentParser(it.response.sourceAsBytesRef)
            mgetList.add(ManagedIndexMetaData.parseWithType(
                    xcp, it.response.id, it.response.seqNo, it.response.primaryTerm))
        } else {
            mgetList.add(null)
        }
    }

    return mgetList
}

fun buildMgetMetadataRequest(clusterState: ClusterState): MultiGetRequest {
    val mgetMetadataRequest = MultiGetRequest()
    clusterState.metadata.indices.map { it.value.index }.forEach {
        mgetMetadataRequest.add(MultiGetRequest.Item(
            INDEX_MANAGEMENT_INDEX, managedIndexMetadataID(it.uuid)).routing(it.uuid))
    }
    return mgetMetadataRequest
}

// forIndex means saving to config index, distinguish from Explain and History,
// which only show meaningful partial metadata
@Suppress("ReturnCount")
fun XContentBuilder.addObject(name: String, metadata: ToXContentFragment?, params: ToXContent.Params, forIndex: Boolean = false): XContentBuilder {
    if (metadata != null) return this.buildMetadata(name, metadata, params)
    return if (forIndex) nullField(name) else this
}

fun XContentBuilder.buildMetadata(name: String, metadata: ToXContentFragment, params: ToXContent.Params): XContentBuilder {
    this.startObject(name)
    metadata.toXContent(this, params)
    this.endObject()
    return this
}
