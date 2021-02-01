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

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ManagedIndexMetaData
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType

/**
 * Returns the current rollover_alias if it exists otherwise returns null.
 */
fun IndexMetadata.getRolloverAlias(): String? {
    if (this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key).isNullOrBlank()) return null

    return this.settings.get(ManagedIndexSettings.ROLLOVER_ALIAS.key)
}

fun IndexMetadata.getManagedIndexMetaData(): ManagedIndexMetaData? {
    val existingMetaDataMap = this.getCustomData(ManagedIndexMetaData.MANAGED_INDEX_METADATA)

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
