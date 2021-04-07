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
import org.apache.lucene.util.automaton.Automaton
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
import java.util.ArrayList

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

    private val v1ISMTemplateMap = mutableMapOf<policyID, ISMTemplate>()
    private val v2ISMTemplateMap = mutableMapOf<policyID, ISMTemplate>()
    private var ismTemplateMap = mutableMapOf<policyID, ISMTemplate>()

    private val v1CachedPriority = mutableMapOf<templateName, Int>()
    private val v1ProcessedPriority = mutableMapOf<templateName, Int>()
    private val v1ProcessedIndexPatterns = mutableMapOf<templateName, List<String>>()
    private val v1TemplateToPolicyIDs = mutableMapOf<templateName, policyID>()

    private val v1overlappingTemplatePairs = mutableSetOf<Pair<Set<templateName>, String>>()
    private val v1v2overlappingTemplatePairs = mutableSetOf<Pair<Set<templateName>, String>>()

    private val overlappingTemplateForSamePolicyID = mutableMapOf<policyID, MutableSet<templateName>>()

    // old v1 template may have negative priority that ISM template not allowed
    // use this map to cache the priority
    private val negOrderToPriority = mutableMapOf<Int, Int>()

    private val policiesToUpdate = mutableMapOf<policyID, seqNoPrimaryTerm>()
    private val policiesFailedToUpdate = mutableMapOf<policyID, BulkItemResponse.Failure>()
    private lateinit var lastUpdatedTime: Instant

    @Volatile private var retryPolicy =
        BackoffPolicy.constantBackoff(TimeValue.timeValueMillis(50), 3)

    suspend fun doMigration(timeStamp: Instant) {
        lastUpdatedTime = timeStamp.minusSeconds(3600)

        getIndexTemplates()
        logger.info("ism templates: $ismTemplateMap")
        logger.info("v1 processed index pattern: $v1ProcessedIndexPatterns")
        logger.info("v1 processed priority: $v1ProcessedPriority")
//        logger.info("v1 overlapping: $v1overlappingTemplatePairs")
//        logger.info("v1 v2 overlapping: $v1v2overlappingTemplatePairs")

        getISMPolicies()
        logger.info("policies to update: ${policiesToUpdate.keys}")

        updateISMPolicies()

        if (policiesToUpdate.isEmpty()) {
            finishFlag = true
            // TODO set template service enable cluster setting to -1 persistently
        }

        cleanCache()
    }

    private fun cleanCache() {
        v1ISMTemplateMap.clear()
        v2ISMTemplateMap.clear()
        ismTemplateMap.clear()
        v1CachedPriority.clear()
        v1ProcessedPriority.clear()
        v1ProcessedIndexPatterns.clear()
        v1TemplateToPolicyIDs.clear()
        v1overlappingTemplatePairs.clear()
        v1v2overlappingTemplatePairs.clear()
        negOrderToPriority.clear()
        policiesToUpdate.clear()
        policiesFailedToUpdate.clear()
    }

    private suspend fun getIndexTemplates() {
        cacheAndNormNegOrder()

        clusterService.state().metadata.templates.forEach {
            logger.info("v1 template: ${it.key}")
            val template = it.value
            val indexPatterns = template.patterns()
            val priority = template.order
            val policyIDSetting = ManagedIndexSettings.POLICY_ID.get(template.settings())
            if (policyIDSetting != null) {
                resolveOverlappingV1Templates(it.key, indexPatterns, priority, policyIDSetting)
            }
        }

        clusterService.state().metadata.templatesV2().forEach {
            val template = it.value
            val indexPatterns = template.indexPatterns()
            val priority = template.priorityOrZero().toInt()
            val policyIDSetting = simulateTemplate(it.key)
            if (policyIDSetting != null) {
                populateV2ISMTemplateMap(policyIDSetting, indexPatterns, priority, it.key)
            }
        }

        populateV1ISMTemplateMap()
        mergeV1V2ISMTemplateMap()
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

    private fun populateV1ISMTemplateMap() {
        // first populate overlappingTemplateForSamePolicyID
        // merge all index patterns, use the highest priority
        val allV1Templates = clusterService.state().metadata.templates
        overlappingTemplateForSamePolicyID.forEach { (policyID, templates) ->
            var highestPriority = 0
            val mergedIndexPatterns = mutableSetOf<String>()
            templates.forEach { tName ->
                val indexPatterns = allV1Templates[tName].patterns()
                val priority = v1ProcessedPriority[tName]
                if (highestPriority < priority!!) highestPriority = priority
                mergedIndexPatterns.addAll(indexPatterns)
            }
            v1ISMTemplateMap[policyID] = ISMTemplate(mergedIndexPatterns.toList(), highestPriority, lastUpdatedTime)
        }


        v1ProcessedIndexPatterns.forEach { (tName, indexPatterns) ->
            val policyID = v1TemplateToPolicyIDs[tName]
            val priority = v1ProcessedPriority[tName]
            if (policyID != null && priority != null && indexPatterns.isNotEmpty()) {
                val existingTemplate = v1ISMTemplateMap[policyID]
                if (existingTemplate != null) {
                    val a1 = Regex.simpleMatchToAutomaton(*indexPatterns.toTypedArray())
                    val a2 = Regex.simpleMatchToAutomaton(*existingTemplate.indexPatterns.toTypedArray())
                    if (!Operations.isEmpty(Operations.intersection(a1, a2))) {
                        // index pattern overlapping
                        if (existingTemplate.priority < priority) {
                            // override existing template if priority higher
                            v1ISMTemplateMap[policyID] = ISMTemplate(indexPatterns, priority, lastUpdatedTime)
                        }
                    } else {
                        // not overlapping -> merge together, use higher priority
                        val existingIndexPatterns = existingTemplate.indexPatterns
                        val mergedIndexPatterns = existingIndexPatterns + indexPatterns
                        logger.info("merged pattern: $mergedIndexPatterns")
                        v1ISMTemplateMap[policyID] = ISMTemplate(mergedIndexPatterns,
                             if (priority < existingTemplate.priority) existingTemplate.priority else priority,
                            lastUpdatedTime)
                    }
                } else {
                    v1ISMTemplateMap[policyID] = ISMTemplate(indexPatterns, priority, lastUpdatedTime)
                }
            }
        }
    }

    private fun mergeV1V2ISMTemplateMap() {
        ismTemplateMap = v1ISMTemplateMap
        v2ISMTemplateMap.forEach { (policyID, ismTemplate) ->
            val existingISMTemplate = ismTemplateMap[policyID]
            if (existingISMTemplate != null) {
                val existingPriority = existingISMTemplate.priority
                if (existingPriority == ismTemplate.priority) {
                    // merge index pattern
                    val mergedIndexPatterns = existingISMTemplate.indexPatterns + ismTemplate.indexPatterns
                    ismTemplateMap[policyID] = ismTemplate.copy(indexPatterns = mergedIndexPatterns)
                } else if (existingPriority < ismTemplate.priority) {
                    ismTemplateMap[policyID] = ismTemplate
                }
            } else {
                ismTemplateMap[policyID] = ismTemplate
            }
        }
    }

    private fun resolveOverlappingV1Templates(templateName: String, rawIndexPatterns: List<String>, rawPriority: Int, policyID: String) {
        val indexPatterns = rawIndexPatterns.toMutableList()
        val priority = normalizePriority(rawPriority)
        val allV1Templates = clusterService.state().metadata.templates

        // for all previous saved templates with the same priority
        // try to help resolving index pattern overlapping
        // based on how current index pattern overlaps previous index pattern
        // either increase priority or discard the subset index pattern
        var previousIncreaseFlag = false
        val previousTemplateNamesWithSamePriority = v1CachedPriority
            .filter { it.value == priority }.keys
        previousTemplateNamesWithSamePriority.forEach { tName ->
            logger.info("tName: $tName")
            val template = allV1Templates[tName]
            rawIndexPatterns.forEach { current ->
                template.patterns().forEach { previous ->
                    if (overlapping(current, previous)) {
                        val currentStarPos = current.indexOf("*")
                        val previousStarPos = previous.indexOf("*")
                        if (currentStarPos < current.length - 1 || previousStarPos < previous.length - 1) {
                            // star not in the end; a-b-*, a-*-c
                            // record overlapping Pair because this may not be what user expected
                            v1overlappingTemplatePairs.add(Pair(setOf(templateName, tName), current))
                            indexPatterns.remove(current)
                        } else if (currentStarPos < previousStarPos) {
                            // a* ab*, current contains previous index pattern
                            // previous template with index pattern ab* priority ++
                            previousOrderIncreaseHelper(tName)
                            previousIncreaseFlag = true

                            // if overlapping index pattern points to same policyID
                            // there could be unwanted result
                            if (v1TemplateToPolicyIDs[tName] == policyID) {
                                v1overlappingTemplatePairs.add(Pair(setOf(templateName, tName), current))
                                recordOverlappingTemplatesForSamePolicyID(tName, templateName, policyID)
                            }
                        } else {
                            // current contained by previous index pattern
                            // discard current index pattern because it wont be used
                            logger.info("before drop: $indexPatterns")
                            indexPatterns.remove(current)
                            logger.info("after drop: $indexPatterns")
                        }
                    }
                }
            }
        }

        // for all previous saved template with higher priority
        // if we increased previous order, pattern with higher priority also need to increase
        val previousTemplateNamesWithHigherPriority = v1CachedPriority
            .filter { it.value > priority }.keys
        previousTemplateNamesWithHigherPriority.forEach { tName ->
            val template = allV1Templates[tName]
            rawIndexPatterns.forEach { current ->
                template.patterns().forEach { previous ->
                    if (overlapping(current, previous)) {
                        if (previousIncreaseFlag) {
                            previousOrderIncreaseHelper(tName)
                        }
                        if (v1TemplateToPolicyIDs[tName] == policyID) {
                            v1overlappingTemplatePairs.add(Pair(setOf(templateName, tName), current))
                            recordOverlappingTemplatesForSamePolicyID(tName, templateName, policyID)
                        }
                    }
                }
            }
        }

        // for all previous saved template with lower priority
        // if previous increase, current need to increase same amount
        var priorityToIncrease = 0
        val previousTemplateNamesWithLowerPriority = v1CachedPriority.filter { it.value < priority }.keys
        previousTemplateNamesWithLowerPriority.forEach { tName ->
            val template = allV1Templates[tName]
            rawIndexPatterns.forEach { current ->
                template.patterns().forEach { previous ->
                    if (overlapping(current, previous)) {
                        val previousIncrease = v1ProcessedPriority[tName]!! - v1CachedPriority[tName]!!
                        if (previousIncrease > priorityToIncrease)
                            priorityToIncrease = previousIncrease

                        if (v1TemplateToPolicyIDs[tName] == policyID) {
                            v1overlappingTemplatePairs.add(Pair(setOf(templateName, tName), current))
                            recordOverlappingTemplatesForSamePolicyID(tName, templateName, policyID)
                        }
                    }
                }
            }
        }

        v1ProcessedPriority[templateName] = v1ProcessedPriority[templateName] ?: priority
        if (indexPatterns.isNotEmpty())
            v1ProcessedIndexPatterns[templateName] = indexPatterns

        v1CachedPriority[templateName] = priority + priorityToIncrease
        v1TemplateToPolicyIDs[templateName] = policyID
    }

    private fun recordOverlappingTemplatesForSamePolicyID(t1: templateName, t2: templateName, policyID: String) {
        val existingTemplateNames = overlappingTemplateForSamePolicyID[policyID]
        if (existingTemplateNames != null) {
            overlappingTemplateForSamePolicyID[policyID]!!.add(t1)
            overlappingTemplateForSamePolicyID[policyID]!!.add(t2)
        } else {
            overlappingTemplateForSamePolicyID[policyID] = mutableSetOf(t1, t2)
        }
    }

    private fun overlapping(currentPattern: String, previousPattern: String): Boolean {
        val a1 = Regex.simpleMatchToAutomaton(currentPattern)
        val a2 = Regex.simpleMatchToAutomaton(previousPattern)
        return !Operations.isEmpty(Operations.intersection(a1, a2))
    }

    private fun previousOrderIncreaseHelper(templateName: String) {
        val previousProcessedOrder = v1ProcessedPriority[templateName]
        if (previousProcessedOrder != null) {
            v1ProcessedPriority[templateName] = previousProcessedOrder + 1
        } else {
            // not possible to reach here
            logger.info("missing previous processed order")
        }
    }

    private fun populateV2ISMTemplateMap(policyID: String, indexPatterns: List<String>, rawPriority: Int, templateName: String) {
        var priority = normalizePriority(rawPriority)

        // check if indexPatterns matches any of v1 index pattern
        // v2 contains v1, discard v1
        // v2 subset of v1, v2 priority should be higher then corresponding v1
        // overlapping, record, v2 priority++
        indexPatterns.forEach { current ->
            val a1 = Regex.simpleMatchToAutomaton(current)
            v1ProcessedIndexPatterns.forEach { (v1TemplateName, v1IndexPatterns) ->
                v1IndexPatterns.forEach { previous ->
                    val a2 = Regex.simpleMatchToAutomaton(previous)
                    if (!Operations.isEmpty(Operations.intersection(a1, a2))) {
                        val currentStarPos = current.indexOf("*")
                        val previousStarPos = previous.indexOf("*")
                        if (currentStarPos < current.length - 1 || previousStarPos < previous.length - 1) {
                            // star not in the end; a-b-*, a-*-c
                            // record and discard v1 index pattern
                            v1v2overlappingTemplatePairs.add(Pair(setOf(templateName, v1TemplateName), previous))
                            val v1Priority = v1ProcessedPriority[v1TemplateName]
                            if (v1Priority != null && priority < v1Priority)
                                priority += v1Priority + 1
                            v1ProcessedIndexPatterns[v1TemplateName] =
                                v1IndexPatterns - previous
                        } else if (currentStarPos > previousStarPos) {
                            // current v2 index pattern is subset of v1 index pattern
                            val v1Priority = v1ProcessedPriority[v1TemplateName]
                            if (v1Priority != null && priority < v1Priority)
                                priority += v1Priority + 1
                        } else {
                            // current index pattern contains v1 index pattern
                            // discard v1 index pattern
                            v1ProcessedIndexPatterns[v1TemplateName] =
                                v1IndexPatterns - previous
                        }
                    }
                }
            }
        }

        v2ISMTemplateMap[policyID] = ISMTemplate(indexPatterns, priority, lastUpdatedTime)
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
    private fun cacheAndNormNegOrder() {
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

    // old v1 template can have negative priority
    // map the negative priority to non-negative value
    private fun normalizePriority(order: Int): Int {
        if (order < 0) return negOrderToPriority[order] ?: 0
        return order + (negOrderToPriority.size)
    }
}

typealias policyID = String
typealias templateName = String
typealias seqNoPrimaryTerm = Pair<Long, Long>
