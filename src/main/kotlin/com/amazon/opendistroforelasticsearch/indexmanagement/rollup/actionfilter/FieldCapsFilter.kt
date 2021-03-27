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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter

import com.amazon.opendistroforelasticsearch.indexmanagement.GuiceHolder
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.getRollupJobs
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util.populateFieldMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexUtils.Companion.getFieldFromMappings
import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.fieldcaps.FieldCapabilities
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse
import org.elasticsearch.action.support.ActionFilter
import org.elasticsearch.action.support.ActionFilterChain
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.tasks.Task
import org.elasticsearch.transport.RemoteClusterAware

private val logger = LogManager.getLogger(FieldCapsFilter::class.java)

@Suppress("UNCHECKED_CAST", "SpreadOperator", "TooManyFunctions", "ComplexMethod", "NestedBlockDepth")
class FieldCapsFilter(
    val clusterService: ClusterService,
    val settings: Settings,
    private val indexNameExpressionResolver: IndexNameExpressionResolver
) : ActionFilter {

    @Volatile private var shouldIntercept = RollupSettings.ROLLUP_DASHBOARDS.get(settings)

    init {
        clusterService.clusterSettings.addSettingsUpdateConsumer(RollupSettings.ROLLUP_DASHBOARDS) {
            flag -> shouldIntercept = flag
        }
    }

    override fun <Request : ActionRequest?, Response : ActionResponse?> apply(
        task: Task,
        action: String,
        request: Request,
        listener: ActionListener<Response>,
        chain: ActionFilterChain<Request, Response>
    ) {
        if (request is FieldCapabilitiesRequest && shouldIntercept) {
            val indices = request.indices().map { it.toString() }.toTypedArray()
            val rollupIndices = mutableSetOf<String>()
            val nonRollupIndices = mutableSetOf<String>()
            val remoteClusterIndices = GuiceHolder.remoteClusterService.groupIndices(request.indicesOptions(), indices) {
                idx: String? -> indexNameExpressionResolver.hasIndexAbstraction(idx, clusterService.state())
            }
            val localIndices = remoteClusterIndices.remove(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)

            localIndices?.let {
                val concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), request.indicesOptions(), it)
                for (index in concreteIndices) {
                    val isRollupIndex = RollupSettings.ROLLUP_INDEX.get(clusterService.state().metadata.index(index).settings)
                    if (isRollupIndex) {
                        rollupIndices.add(index)
                    } else {
                        nonRollupIndices.add(index)
                    }
                }
            }

            remoteClusterIndices.entries.forEach {
                val cluster = it.key
                val clusterIndices = it.value
                clusterIndices.indices().forEach { index ->
                    nonRollupIndices.add("$cluster${RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR}$index")
                }
            }
            logger.debug("Resolved into rollup $rollupIndices and non rollup $nonRollupIndices indices")

            if (rollupIndices.isEmpty()) {
                return chain.proceed(task, action, request, listener)
            }

            if (nonRollupIndices.isNotEmpty()) {
                request.indices(*nonRollupIndices.toTypedArray())
            }

            chain.proceed(task, action, request, object : ActionListener<Response> {
                override fun onResponse(response: Response) {
                    logger.info("Has rollup indices will rewrite field caps response")
                    response as FieldCapabilitiesResponse
                    val rewrittenResponse = rewriteResponse(response, rollupIndices, nonRollupIndices.isEmpty())
                    listener.onResponse(rewrittenResponse as Response)
                }

                override fun onFailure(e: Exception) {
                    listener.onFailure(e)
                }
            })
        } else {
            chain.proceed(task, action, request, listener)
        }
    }

    /**
     * The FieldCapabilitiesResponse can contain merged or unmerged data. The response will hold unmerged data if its a cross cluster search.
     *
     * There is a boolean available in the FieldCapabilitiesRequest `isMergeResults` which indicates if the response is merged/unmerged.
     * Unfortunately this is package private and when rewriting we can't access it from request. Instead will be relying on the response.
     * If response has indexResponses then its unmerged else merged.
     */
    private fun rewriteResponse(response: FieldCapabilitiesResponse, rollupIndices: Set<String>, shouldDiscardResponse: Boolean): ActionResponse {
        val ismFieldCapabilitiesResponse = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(response)
        val isMergedResponse = ismFieldCapabilitiesResponse.indexResponses.isEmpty()

        // if original response contained only rollup indices we should discard it
        val fields = if (shouldDiscardResponse) mapOf() else response.get()
        val indices = if (shouldDiscardResponse) arrayOf() else response.indices
        val indexResponses = if (shouldDiscardResponse) listOf() else ismFieldCapabilitiesResponse.indexResponses

        return if (isMergedResponse) {
            rewriteResponse(indices, fields, rollupIndices)
        } else {
            val rollupIndexResponses = populateRollupIndexResponses(rollupIndices)
            val mergedIndexResponses = indexResponses + rollupIndexResponses

            val rewrittenISMResponse = ISMFieldCapabilitiesResponse(arrayOf(), mapOf(), mergedIndexResponses)
            rewrittenISMResponse.toFieldCapabilitiesResponse()
        }
    }

    private fun populateRollupIndexResponses(rollupIndices: Set<String>): List<ISMFieldCapabilitiesIndexResponse> {
        val indexResponses = mutableListOf<ISMFieldCapabilitiesIndexResponse>()
        rollupIndices.forEach { rollupIndex ->
            val rollupIsmFieldCapabilities = mutableMapOf<String, ISMIndexFieldCapabilities>()
            val rollupFieldMappings = populateSourceFieldMappingsForRollupIndex(rollupIndex)

            rollupFieldMappings.forEach { rollupFieldMapping ->
                val fieldName = rollupFieldMapping.fieldName
                val type = rollupFieldMapping.sourceType!!
                val isSearchable = rollupFieldMapping.fieldType == RollupFieldMapping.Companion.FieldType.DIMENSION
                rollupIsmFieldCapabilities[fieldName] = ISMIndexFieldCapabilities(fieldName, type, isSearchable, true, mapOf())
            }

            indexResponses.add(ISMFieldCapabilitiesIndexResponse(rollupIndex, rollupIsmFieldCapabilities, true))
        }

        return indexResponses
    }

    private fun rewriteResponse(
        indices: Array<String>,
        fields: Map<String, Map<String, FieldCapabilities>>,
        rollupIndices: Set<String>
    ): ActionResponse {
        val filteredIndicesFields = expandIndicesInFields(indices, fields)
        val rollupIndicesFields = populateRollupIndicesFields(rollupIndices)
        val mergedFields = mergeFields(filteredIndicesFields, rollupIndicesFields)
        val mergedIndices = indices + rollupIndices.toTypedArray()

        return FieldCapabilitiesResponse(mergedIndices, mergedFields)
    }

    private fun populateRollupIndicesFields(rollupIndices: Set<String>): Map<String, Map<String, FieldCapabilities>> {
        val fieldMappingIndexMap = populateSourceFieldMappingsForRollupIndices(rollupIndices)

        val response = mutableMapOf<String, MutableMap<String, FieldCapabilities>>()
        fieldMappingIndexMap.keys.forEach { fieldMapping ->
            val fieldName = fieldMapping.fieldName
            val type = fieldMapping.sourceType!!
            if (response[fieldName] == null) {
                response[fieldName] = mutableMapOf()
            }
            val isSearchable = fieldMapping.fieldType == RollupFieldMapping.Companion.FieldType.DIMENSION
            response[fieldName]!![type] = FieldCapabilities(fieldName, type, isSearchable, true, fieldMappingIndexMap.getValue(fieldMapping)
                .toTypedArray(), null, null, mapOf<String, Set<String>>())
        }

        return response
    }

    private fun populateSourceFieldMappingsForRollupJob(rollup: Rollup): Set<RollupFieldMapping> {
        val rollupFieldMappings = rollup.populateFieldMappings()
        val sourceIndices = indexNameExpressionResolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpand(), rollup.sourceIndex)
        sourceIndices.forEach {
            val mappings = clusterService.state().metadata.index(it).mapping()?.sourceAsMap ?: return rollupFieldMappings
            rollupFieldMappings.forEach { fieldMapping ->
                val fieldType = getFieldType(fieldMapping.fieldName, mappings)
                if (fieldType != null) {
                    fieldMapping.sourceType(fieldType)
                }
            }
        }

        return rollupFieldMappings
    }

    private fun populateSourceFieldMappingsForRollupIndex(rollupIndex: String): Set<RollupFieldMapping> {
        val fieldMappings = mutableSetOf<RollupFieldMapping>()
        val rollupJobs = clusterService.state().metadata.index(rollupIndex).getRollupJobs() ?: return fieldMappings
        rollupJobs.forEach { rollup ->
            fieldMappings.addAll(populateSourceFieldMappingsForRollupJob(rollup))
        }
        return fieldMappings
    }

    @Suppress("NestedBlockDepth")
    private fun populateSourceFieldMappingsForRollupIndices(rollupIndices: Set<String>): MutableMap<RollupFieldMapping, MutableSet<String>> {
        val fieldMappingsMap = mutableMapOf<RollupFieldMapping, MutableSet<String>>()

        rollupIndices.forEach { rollupIndex ->
            val fieldMappings = populateSourceFieldMappingsForRollupIndex(rollupIndex)
            fieldMappings.forEach { fieldMapping ->
                if (fieldMappingsMap[fieldMapping] == null) {
                    fieldMappingsMap[fieldMapping] = mutableSetOf()
                }
                fieldMappingsMap[fieldMapping]!!.add(rollupIndex)
            }
        }

        return fieldMappingsMap
    }

    private fun getFieldType(fieldName: String, mappings: Map<*, *>): String? {
        val field = getFieldFromMappings(fieldName, mappings)
        return if (field != null) field["type"]?.toString() else null
    }

    private fun expandIndicesInFields(
        indices: Array<String>,
        fields: Map<String, Map<String, FieldCapabilities>>
    ): Map<String, Map<String, FieldCapabilities>> {
        val expandedResponse = mutableMapOf<String, MutableMap<String, FieldCapabilities>>()
        fields.keys.forEach { field ->
            fields.getValue(field).keys.forEach { type ->
                if (expandedResponse[field] == null) {
                    expandedResponse[field] = mutableMapOf()
                }
                val fieldCaps = fields.getValue(field).getValue(type)
                val rewrittenIndices = if (fieldCaps.indices() != null && fieldCaps.indices().isNotEmpty()) fieldCaps.indices() else indices
                expandedResponse[field]!![type] = FieldCapabilities(fieldCaps.name, fieldCaps.type, fieldCaps.isSearchable, fieldCaps
                    .isAggregatable, rewrittenIndices, fieldCaps.nonSearchableIndices(), fieldCaps.nonAggregatableIndices(), fieldCaps.meta())
            }
        }

        return expandedResponse
    }

    private fun mergeFields(
        f1: Map<String, Map<String, FieldCapabilities>>,
        f2: Map<String, Map<String, FieldCapabilities>>
    ): Map<String, Map<String, FieldCapabilities>> {
        val mergedResponses = mutableMapOf<String, Map<String, FieldCapabilities>>()
        val fields = f1.keys.union(f2.keys)
        fields.forEach { field ->
            val mergedFields = mergeTypes(f1[field], f2[field])
            if (mergedFields != null) mergedResponses[field] = mergedFields
        }

        return mergedResponses
    }

    @Suppress("ReturnCount")
    private fun mergeTypes(t1: Map<String, FieldCapabilities>?, t2: Map<String, FieldCapabilities>?): Map<String, FieldCapabilities>? {
        if (t1 == null) return t2
        if (t2 == null) return t1
        val mergedFields = mutableMapOf<String, FieldCapabilities>()
        val types = t1.keys.union(t2.keys)
        types.forEach { type ->
            val mergedTypes = mergeFieldCaps(t1[type], t2[type])
            if (mergedTypes != null) mergedFields[type] = mergedTypes
        }

        return mergedFields
    }

    @Suppress("ReturnCount")
    private fun mergeFieldCaps(fc1: FieldCapabilities?, fc2: FieldCapabilities?): FieldCapabilities? {
        if (fc1 == null) return fc2
        if (fc2 == null) return fc1
        // TODO: Should we throw error instead?
        if (fc1.name != fc2.name && fc1.type != fc2.type) {
            logger.warn("cannot merge $fc1 and $fc2")
            return null
        }
        val isSearchable = fc1.isSearchable || fc2.isSearchable
        val isAggregatable = fc1.isAggregatable || fc2.isAggregatable
        val name = fc1.name
        val type = fc1.type
        val indices = fc1.indices() + fc2.indices()
        val nonAggregatableIndices = mergeNonAggregatableIndices(fc1, fc2)
        val nonSearchableIndices = mergeNonSearchableIndices(fc1, fc2)
        val meta = (fc1.meta().keys + fc2.meta().keys)
            .associateWith {
                val data = mutableSetOf<String>()
                data.addAll(fc1.meta().getOrDefault(it, mutableSetOf()))
                data.addAll(fc2.meta().getOrDefault(it, mutableSetOf()))
                data
            }

        return FieldCapabilities(name, type, isSearchable, isAggregatable, indices, nonSearchableIndices, nonAggregatableIndices, meta)
    }

    private fun mergeNonAggregatableIndices(fc1: FieldCapabilities, fc2: FieldCapabilities): Array<String>? {
        val response = mutableSetOf<String>()
        if (fc1.isAggregatable || fc2.isAggregatable) {
            if (!fc1.isAggregatable) response.addAll(fc1.indices())
            if (!fc2.isAggregatable) response.addAll(fc2.indices())
            if (fc1.nonAggregatableIndices() != null) response.addAll(fc1.nonAggregatableIndices())
            if (fc2.nonAggregatableIndices() != null) response.addAll(fc2.nonAggregatableIndices())
        }

        return if (response.isEmpty()) null else response.toTypedArray()
    }

    private fun mergeNonSearchableIndices(fc1: FieldCapabilities, fc2: FieldCapabilities): Array<String>? {
        val response = mutableSetOf<String>()
        if (fc1.isSearchable || fc2.isSearchable) {
            if (!fc1.isSearchable) response.addAll(fc1.indices())
            if (!fc2.isSearchable) response.addAll(fc2.indices())
            if (fc1.nonSearchableIndices() != null) response.addAll(fc1.nonSearchableIndices())
            if (fc2.nonSearchableIndices() != null) response.addAll(fc2.nonSearchableIndices())
        }

        return if (response.isEmpty()) null else response.toTypedArray()
    }

    override fun order(): Int {
        return Integer.MAX_VALUE
    }
}
