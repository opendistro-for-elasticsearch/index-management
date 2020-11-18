/*
 *   Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.indexmanagement.util

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.IndicesAdminClient
import org.elasticsearch.cluster.ClusterState
import org.elasticsearch.cluster.metadata.IndexAbstraction
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType

class IndexUtils {
    companion object {
        @Suppress("ObjectPropertyNaming")
        const val _META = "_meta"
        const val PROPERTIES = "properties"
        const val FIELDS = "fields"
        const val SCHEMA_VERSION = "schema_version"
        const val DEFAULT_SCHEMA_VERSION = 1L
        val logger = LogManager.getLogger(IndexUtils::class.java)

        var indexManagementConfigSchemaVersion: Long
            private set
        var indexStateManagementHistorySchemaVersion: Long
            private set

        init {
            indexManagementConfigSchemaVersion = getSchemaVersion(IndexManagementIndices.indexManagementMappings)
            indexStateManagementHistorySchemaVersion = getSchemaVersion(IndexManagementIndices.indexStateManagementHistoryMappings)
        }

        @Suppress("NestedBlockDepth")
        fun getSchemaVersion(mapping: String): Long {
            val xcp = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE, mapping)

            while (!xcp.isClosed) {
                val token = xcp.currentToken()
                if (token != null && token != XContentParser.Token.END_OBJECT && token != XContentParser.Token.START_OBJECT) {
                    if (xcp.currentName() != _META) {
                        xcp.nextToken()
                        xcp.skipChildren()
                    } else {
                        while (xcp.nextToken() != XContentParser.Token.END_OBJECT) {
                            when (xcp.currentName()) {
                                SCHEMA_VERSION -> {
                                    val version = xcp.longValue()
                                    require(version > -1)
                                    return version
                                }
                                else -> xcp.nextToken()
                            }
                        }
                    }
                }
                xcp.nextToken()
            }
            return DEFAULT_SCHEMA_VERSION
        }

        fun shouldUpdateIndex(index: IndexMetadata, newVersion: Long): Boolean {
            var oldVersion = DEFAULT_SCHEMA_VERSION

            val indexMapping = index.mapping()?.sourceAsMap()
            if (indexMapping != null && indexMapping.containsKey(_META) && indexMapping[_META] is HashMap<*, *>) {
                val metaData = indexMapping[_META] as HashMap<*, *>
                if (metaData.containsKey(SCHEMA_VERSION)) {
                    oldVersion = (metaData[SCHEMA_VERSION] as Int).toLong()
                }
            }
            return newVersion > oldVersion
        }

        fun checkAndUpdateConfigIndexMapping(
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            checkAndUpdateIndexMapping(
                IndexManagementPlugin.INDEX_MANAGEMENT_INDEX,
                indexManagementConfigSchemaVersion,
                IndexManagementIndices.indexManagementMappings,
                clusterState,
                client,
                actionListener
            )
        }

        fun checkAndUpdateHistoryIndexMapping(
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            checkAndUpdateAliasMapping(
                IndexManagementIndices.HISTORY_WRITE_INDEX_ALIAS,
                indexStateManagementHistorySchemaVersion,
                IndexManagementIndices.indexStateManagementHistoryMappings,
                clusterState,
                client,
                actionListener
            )
        }

        @OpenForTesting
        @Suppress("LongParameterList")
        fun checkAndUpdateIndexMapping(
            index: String,
            schemaVersion: Long,
            mapping: String,
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            if (clusterState.metadata.indices.containsKey(index)) {
                if (shouldUpdateIndex(clusterState.metadata.indices[index], schemaVersion)) {
                    val putMappingRequest: PutMappingRequest = PutMappingRequest(index).type(_DOC).source(mapping, XContentType.JSON)
                    client.putMapping(putMappingRequest, actionListener)
                } else {
                    actionListener.onResponse(AcknowledgedResponse(true))
                }
            } else {
                logger.error("IndexMetaData does not exist for $index")
                actionListener.onResponse(AcknowledgedResponse(false))
            }
        }

        @OpenForTesting
        @Suppress("LongParameterList")
        fun checkAndUpdateAliasMapping(
            alias: String,
            schemaVersion: Long,
            mapping: String,
            clusterState: ClusterState,
            client: IndicesAdminClient,
            actionListener: ActionListener<AcknowledgedResponse>
        ) {
            val result = clusterState.metadata.indicesLookup[alias]
            if (result == null || result.type != IndexAbstraction.Type.ALIAS) {
                logger.error("There are no indices for alias $alias")
                actionListener.onResponse(AcknowledgedResponse(false))
            } else {
                val writeIndex = result.writeIndex
                if (writeIndex == null) {
                    logger.error("Concrete write index does not exist for alias $alias")
                    actionListener.onResponse(AcknowledgedResponse(false))
                } else {
                    if (shouldUpdateIndex(writeIndex, schemaVersion)) {
                        val putMappingRequest: PutMappingRequest = PutMappingRequest(writeIndex.index.name)
                            .type(_DOC).source(mapping, XContentType.JSON)
                        client.putMapping(putMappingRequest, actionListener)
                    } else {
                        actionListener.onResponse(AcknowledgedResponse(true))
                    }
                }
            }
        }
    }
}
