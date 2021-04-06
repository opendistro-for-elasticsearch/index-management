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

@file:Suppress("MagicNumber", "ComplexMethod", "NestedBlockDepth")

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.retry
import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.suspendUntil
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.updateISMTemplateRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.util.IndexManagementException
import com.amazon.opendistroforelasticsearch.indexmanagement.util.OpenForTesting
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.logging.log4j.LogManager
import org.apache.lucene.util.automaton.Operations
import org.elasticsearch.ElasticsearchException
import org.elasticsearch.ExceptionsHelper
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.template.post.SimulateIndexTemplateResponse
import org.elasticsearch.action.admin.indices.template.post.SimulateTemplateAction
import org.elasticsearch.action.bulk.BackoffPolicy
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.get.MultiGetRequest
import org.elasticsearch.action.get.MultiGetResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.cluster.metadata.Template
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.regex.Regex
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestStatus
import java.time.Instant

private val log = LogManager.getLogger("ISMTemplateService")

/**
 * find the matching policy based on ISM template field for the given index
 *
 * filter out hidden index
 * filter out older index than template lastUpdateTime
 *
 * @param ismTemplates current ISM templates saved in metadata
 * @param indexMetadata cluster state index metadata
 * @return policyID
 */
@Suppress("ReturnCount")
fun Map<String, ISMTemplate>.findMatchingPolicy(indexMetadata: IndexMetadata): String? {
    if (this.isEmpty()) return null

    val indexName = indexMetadata.index.name

    // don't include hidden index
    val isHidden = IndexMetadata.INDEX_HIDDEN_SETTING.get(indexMetadata.settings)
    if (isHidden) return null

    // only process indices created after template
    // traverse all ism templates for matching ones
    val patternMatchPredicate = { pattern: String -> Regex.simpleMatch(pattern, indexName) }
    var matchedPolicy: String? = null
    var highestPriority: Int = -1
    val matchedPolicies = mutableListOf<String>()
    this.filter { (_, template) ->
        template.lastUpdatedTime.toEpochMilli() < indexMetadata.creationDate
    }.forEach { (policyID, template) ->
        val matched = template.indexPatterns.stream().anyMatch(patternMatchPredicate)
        val negateIndexPatterns = template.indexPatterns.filter { it.startsWith("-") }.map { it.substring(1) }
        val negateMatch = negateIndexPatterns.stream().anyMatch(patternMatchPredicate)
        if (negateMatch) log.info("index [$indexName] negate matches [$negateIndexPatterns].")
        if (matched && !negateMatch) {
            if (highestPriority < template.priority) {
                highestPriority = template.priority
                matchedPolicy = policyID
                matchedPolicies.add(policyID)
            } else if (highestPriority == template.priority) {
                matchedPolicies.add(policyID)
            }
        }
    }

    if (matchedPolicies.size > 1) {
        log.warn("index [$indexName] matches multiple ISM templates [$matchedPolicies]")
    }

    return matchedPolicy
}

/**
 * validate the template Name and indexPattern provided in the template
 *
 * get the idea from ES validate function in MetadataIndexTemplateService
 * acknowledge https://github.com/a2lin who should be the first contributor
 */
@Suppress("ComplexMethod")
fun validateFormat(indexPatterns: List<String>): ElasticsearchException? {
    val indexPatternFormatErrors = mutableListOf<String>()
    for (indexPattern in indexPatterns) {
        if (indexPattern.contains("#")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a '#'")
        }
        if (indexPattern.contains(":")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not contain a ':'")
        }
        if (indexPattern.startsWith("_")) {
            indexPatternFormatErrors.add("index_pattern [$indexPattern] must not start with '_'")
        }
        if (!Strings.validFileNameExcludingAstrix(indexPattern)) {
            indexPatternFormatErrors.add("index_pattern [" + indexPattern + "] must not contain the following characters " +
                Strings.INVALID_FILENAME_CHARS)
        }
    }

    if (indexPatternFormatErrors.size > 0) {
        val validationException = ValidationException()
        validationException.addValidationErrors(indexPatternFormatErrors)
        return IndexManagementException.wrap(validationException)
    }
    return null
}

/**
 * find policy templates whose index patterns overlap with given template
 *
 * @return map of overlapping template name to its index patterns
 */
@Suppress("SpreadOperator")
fun Map<String, ISMTemplate>.findConflictingPolicyTemplates(
    candidate: String,
    indexPatterns: List<String>,
    priority: Int
): Map<String, List<String>> {
    val automaton1 = Regex.simpleMatchToAutomaton(*indexPatterns.toTypedArray())
    val overlappingTemplates = mutableMapOf<String, List<String>>()

    // focus on template with same priority
    this.filter { it.value.priority == priority }
        .forEach { (policyID, template) ->
        val automaton2 = Regex.simpleMatchToAutomaton(*template.indexPatterns.toTypedArray())
        if (!Operations.isEmpty(Operations.intersection(automaton1, automaton2))) {
            log.info("Existing ism_template for $policyID overlaps candidate $candidate")
            overlappingTemplates[policyID] = template.indexPatterns
        }
    }
    overlappingTemplates.remove(candidate)

    return overlappingTemplates
}

@OpenForTesting
class ISMTemplateService(
    private val client: Client,
    private val clusterService: ClusterService,
    private val xContentRegistry: NamedXContentRegistry,
    private val imIndices: IndexManagementIndices
) {

    private val logger = LogManager.getLogger(javaClass)

    @Volatile final var finishFlag = false
        private set
    fun reenableTemplateMigration() { finishFlag = false }

    private val ismTemplateMap = mutableMapOf<policyID, ISMTemplate>()
    // old v1 template may have negative priority that ISM template not allowed
    // use this map to cache the priority
    private val policyIDToNegOrder = mutableMapOf<policyID, Int>()
    private val negOrderToPriority = mutableMapOf<Int, Int>()
    private val policiesToUpdate = mutableMapOf<policyID, seqNoPrimaryTerm>()
    private val policiesFailedToUpdate = mutableMapOf<policyID, BulkItemResponse.Failure>()
    private lateinit var lastUpdatedTime: Instant

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    suspend fun doMigration(timeStamp: Instant) {
        lastUpdatedTime = timeStamp.minusSeconds(3600)

        getIndexTemplates()
        logger.info("index templates: $ismTemplateMap")

        getISMPolicies()
        logger.info("policies to update: ${policiesToUpdate.keys}")

        updateISMPolicies()

        if (policiesToUpdate.isEmpty())
            finishFlag = true
    }

    private suspend fun getIndexTemplates() {
        mapNegOrderToPriority()

        clusterService.state().metadata.templates.forEach {
            val template = it.value
            val indexPatterns = template.patterns()
            val priority = template.order
            val policyIDSetting = ManagedIndexSettings.POLICY_ID.get(template.settings())
            if (policyIDSetting != null) {
                processV1Template(policyIDSetting, indexPatterns, priority)
            }
        }

        clusterService.state().metadata.templatesV2().forEach {
            val template = it.value
            val indexPatterns = template.indexPatterns()
            val priority = template.priorityOrZero().toInt()
            val policyIDSetting = simulateTemplate(it.key)
            if (policyIDSetting != null) {
                processV2Template(policyIDSetting, indexPatterns, priority)
            }
        }
    }

    private suspend fun simulateTemplate(templateName: String): String? {
        val request = SimulateTemplateAction.Request(templateName)
        val response: SimulateIndexTemplateResponse =
            client.suspendUntil { execute(SimulateTemplateAction.INSTANCE, request, it) }

        var policyIDSetting: String? = null
        withContext(Dispatchers.IO) {
            val out = BytesStreamOutput().also { response.writeTo(it) }
            val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
            val resolvedTemplate = sin.readOptionalWriteable(::Template)
            if (resolvedTemplate != null) {
                policyIDSetting = ManagedIndexSettings.POLICY_ID.get(resolvedTemplate.settings())
            }
        }
        return policyIDSetting
    }

    private fun processV1Template(policyID: String, indexPatterns: List<String>, priority: Int) {
        val normalizedPriority = normTemplatePriority(priority)
        var indexPatterns = indexPatterns
        // find all existing templates with same priority
        // check if current index pattern matches with any existing template index pattern
        // and update the index pattern correspondingly
        val templates = ismTemplateMap.filter { it.value.priority == normalizedPriority }
        templates.forEach { (policyID, template) ->
            template.indexPatterns.forEachIndexed { _, exist ->
            val a1 = Regex.simpleMatchToAutomaton(exist)
            indexPatterns.forEachIndexed { i2, current ->
                val a2 = Regex.simpleMatchToAutomaton(current)
                if (!Operations.isEmpty(Operations.intersection(a1, a2))) {
                    if (current == exist) {
                        indexPatterns = indexPatterns.drop(i2)
                    } else if (current.length > exist.length) {
                        val existIndexPatterns = template.indexPatterns.toMutableSet()
                        existIndexPatterns.add("-$current")
                        ismTemplateMap[policyID] = template.copy(indexPatterns = existIndexPatterns.toList())
                    } else {
                        indexPatterns = indexPatterns.plus("-$exist")
                    }
                }
            }
        } }

        // if one policyID -> multiple templates
        //  if priority is same, merge index pattern
        //  if priority lower, discard
        //  if priority higher, override
        val existingPriority = policyIDToNegOrder[policyID] ?: priority
        val existingEntry = ismTemplateMap[policyID]
        if (existingEntry != null) {
            if (existingPriority == priority) {
                val existingIndexPatterns = existingEntry.indexPatterns
                val mergedIndexPatterns = mutableSetOf<String>()
                mergedIndexPatterns.addAll(existingIndexPatterns)
                mergedIndexPatterns.addAll(indexPatterns)
                ismTemplateMap[policyID] =
                    ISMTemplate(mergedIndexPatterns.toList(), normalizedPriority, lastUpdatedTime)
                return
            } else if (existingPriority < priority) {
                return
            }
        }

        ismTemplateMap[policyID] = ISMTemplate(indexPatterns,
            normTemplatePriority(priority), lastUpdatedTime)
    }

    private fun processV2Template(policyID: String, indexPatterns: List<String>, priority: Int) {
        // if current index pattern overlap with any v1 template
        // it will override it; if existing index pattern is shorter, minus
        // if existing index pattern is longer, discard
        val templates = ismTemplateMap.filter { it.value.priority == priority }
        templates.forEach { (policyID, template) ->
            template.indexPatterns.forEachIndexed { _, exist ->
                val a1 = Regex.simpleMatchToAutomaton(exist)
                indexPatterns.forEachIndexed { _, current ->
                    val a2 = Regex.simpleMatchToAutomaton(current)
                    if (!Operations.isEmpty(Operations.intersection(a1, a2))) {
                        if (current.length > exist.length) {
                            val existIndexPatterns = template.indexPatterns.toMutableSet()
                            existIndexPatterns.add("-$current")
                            ismTemplateMap[policyID] = template.copy(indexPatterns = existIndexPatterns.toList())
                        } else {
                            val existIndexPatterns = template.indexPatterns.toMutableSet()
                            existIndexPatterns.remove(exist)
                            ismTemplateMap[policyID] = template.copy(indexPatterns = existIndexPatterns.toList())
                        }
                    }
                }
            } }

        // if one policyID -> multiple templates
        //  if priority is same, merge index pattern
        //  if priority lower, discard
        //  if priority higher, override
        val existingPriority = policyIDToNegOrder[policyID] ?: priority
        val existingEntry = ismTemplateMap[policyID]
        if (existingEntry != null) {
            if (existingPriority == priority) {
                val existingIndexPatterns = existingEntry.indexPatterns
                val mergedIndexPatterns = mutableSetOf<String>()
                mergedIndexPatterns.addAll(existingIndexPatterns)
                mergedIndexPatterns.addAll(indexPatterns)
                ismTemplateMap[policyID] =
                    ISMTemplate(mergedIndexPatterns.toList(),
                        normTemplatePriority(priority), lastUpdatedTime)
                return
            } else if (existingPriority < priority) {
                return
            }
        }

        ismTemplateMap[policyID] = ISMTemplate(indexPatterns,
            normTemplatePriority(priority), lastUpdatedTime)
    }

    private suspend fun getISMPolicies() {
        if (ismTemplateMap.isEmpty()) return

        val mReq = MultiGetRequest()
        ismTemplateMap.keys.forEach { mReq.add(INDEX_MANAGEMENT_INDEX, it) }
        try {
            val mRes: MultiGetResponse = client.suspendUntil { multiGet(mReq, it) }
            policiesToUpdate.clear()
            mRes.forEach {
                if (it.response != null && !it.response.isSourceEmpty && !it.isFailed) {
                    val response = it.response

                    // TODO throw exception?
                    val policy = XContentHelper.createParser(
                        xContentRegistry,
                        LoggingDeprecationHandler.INSTANCE,
                        response.sourceAsBytesRef,
                        XContentType.JSON
                    ).use { xcp ->
                        xcp.parseWithType(response.id, response.seqNo, response.primaryTerm, Policy.Companion::parse)
                    }

                    if (policy.ismTemplate == null) {
                        policiesToUpdate[it.id] = Pair(response.seqNo, response.primaryTerm)
                    }
                }
            }
        } catch (e: ActionRequestValidationException) {
            logger.info("ISM config index not exists.")
        }
    }

    private suspend fun updateISMPolicies() {
        if (policiesToUpdate.isEmpty()) return

        if (!imIndices.attemptUpdateConfigIndexMapping()) {
            logger.error("Failed to update config index mapping.")
            return
        }

        val requests = mutableListOf<UpdateRequest>()
        policiesToUpdate.forEach { policyID, (seqNo, priTerm) ->
            val ismTemplate = ismTemplateMap[policyID]
            if (ismTemplate != null)
                requests.add(updateISMTemplateRequest(policyID, ismTemplate, seqNo, priTerm))
        }
        var requestsToRetry: List<DocWriteRequest<*>> = requests

        retryPolicy.retry(logger, listOf(RestStatus.TOO_MANY_REQUESTS)) {
            val bulkReq = BulkRequest().add(requestsToRetry)
            val bulkRes: BulkResponse = client.suspendUntil { bulk(bulkReq, it) }
            val failedResponses = (bulkRes.items ?: arrayOf()).filter { it.isFailed }

            val retryItems = mutableListOf<Int>()
            bulkRes.items.forEach {
                if (it.isFailed) {
                    if (it.status() == RestStatus.TOO_MANY_REQUESTS) {
                        retryItems.add(it.itemId)
                    } else {
                        logger.error("Failed to update template for policy [${it.id}], ${it.failureMessage}")
                        policiesFailedToUpdate[it.id] = it.failure
                    }
                } else {
                    policiesToUpdate.remove(it.id)
                }
            }
            requestsToRetry = retryItems.map { bulkReq.requests()[it] as UpdateRequest }

            if (requestsToRetry.isNotEmpty()) {
                val retryCause = failedResponses.first { it.status() == RestStatus.TOO_MANY_REQUESTS }.failure.cause
                throw ExceptionsHelper.convertToElastic(retryCause)
            }
        }
    }

    // old v1 template can have negative priority
    // map the negative priority to non-negative value
    private fun mapNegOrderToPriority() {
        val negOrderToPolicyIDs = mutableMapOf<Int, MutableList<policyID>>()
        clusterService.state().metadata.templates.forEach {
            val priority = it.value.order
            if (priority < 0) {
                if (negOrderToPolicyIDs[priority] != null) {
                    negOrderToPolicyIDs[priority]?.add(it.key)
                } else negOrderToPolicyIDs[priority] = mutableListOf(it.key)
            }
        }
        val sorted = negOrderToPolicyIDs.toSortedMap()
        var p = 0
        for (i in sorted.keys) {
            negOrderToPriority[i] = p++
        }
    }

    private fun normTemplatePriority(order: Int): Int {
        if (order < 0) return negOrderToPriority[order] ?: 0
        return order + (negOrderToPriority.size)
    }
}

typealias policyID = String
typealias seqNoPrimaryTerm = Pair<Long, Long>
