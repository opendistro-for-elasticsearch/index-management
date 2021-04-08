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
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata
import org.elasticsearch.cluster.metadata.Template
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.Strings
import org.elasticsearch.common.ValidationException
import org.elasticsearch.common.collect.ImmutableOpenMap
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

    private val v2ISMTemplateMap = mutableMapOf<policyID, ISMTemplate>()
    private var ismTemplateMap = mutableMapOf<policyID, MutableList<ISMTemplate>>()

    private val v1CachedPriority = mutableMapOf<templateName, Int>()
    private val v1ProcessedPriority = mutableMapOf<templateName, Int>()
    private val v1ProcessedIndexPatterns = mutableMapOf<templateName, List<String>>()
    private val v1TemplateToPolicyIDs = mutableMapOf<templateName, policyID>()


    private val v1orderToTemplatesName = mutableMapOf<Int, MutableList<templateName>>()
    private val v1orderToPatterns = mutableMapOf<Int, MutableSet<String>>()
    private val v1orderToBucketIncrement = mutableMapOf<Int, Int>()
    private lateinit var allV1Templates: ImmutableOpenMap<String, IndexTemplateMetadata>

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
        ismTemplateMap.clear()
        v1CachedPriority.clear()
        v1ProcessedPriority.clear()
        v1ProcessedIndexPatterns.clear()
        v1TemplateToPolicyIDs.clear()
        negOrderToPriority.clear()
        policiesToUpdate.clear()
        policiesFailedToUpdate.clear()
    }

    private suspend fun getIndexTemplates() {
        allV1Templates = clusterService.state().metadata.templates
        cacheAndNormNegOrder()

        bucketizeV1Templates()
        populateBucketPriority()
        populateV1Template()

        clusterService.state().metadata.templatesV2().forEach {
            val template = it.value
            val indexPatterns = template.indexPatterns()
            val priority = template.priorityOrZero().toInt()
            val policyIDSetting = simulateTemplate(it.key)
            if (policyIDSetting != null) {
                populateV2ISMTemplateMap(policyIDSetting, indexPatterns, priority)
            }
        }
    }

    private fun bucketizeV1Templates() {
        allV1Templates.forEach {
            val policyIDSetting = ManagedIndexSettings.POLICY_ID.get(it.value.settings())
            if (policyIDSetting != null) {
                v1TemplateToPolicyIDs[it.key] = policyIDSetting
                val priority = normalizePriority(it.value.order)
                v1ProcessedPriority[it.key] = priority
                val bucket = v1orderToTemplatesName[priority]
                if (bucket == null) {
                    v1orderToTemplatesName[priority] = mutableListOf(it.key)
                } else {
                    v1orderToTemplatesName[priority]!!.add(0, it.key)
                }

                val patternBucket = v1orderToPatterns[priority]
                if (patternBucket == null) {
                    v1orderToPatterns[priority] = it.value.patterns().toMutableSet()
                } else {
                    v1orderToPatterns[priority]!!.addAll(it.value.patterns())
                }
            }
        }
    }

    private fun populateBucketPriority() {
        v1orderToTemplatesName.forEach { (order, templateNames) ->
            var bucketIncrement = 0
            logger.info("order $order, templateNames: $templateNames")
            // t1 t2 t3 | t4 t5
            templateNames.forEachIndexed { ind, current ->
                val templatesToIncreaseOrder = templateNames.subList(ind+1, templateNames.size)
                logger.info("current $current")
                logger.info("increase templates $templatesToIncreaseOrder")
                templatesToIncreaseOrder.forEach {
                    // initialize in bucketizeV1Templates
                    v1ProcessedPriority[it] = v1ProcessedPriority[it]!! + 1
                }
                bucketIncrement++
            }
            v1orderToBucketIncrement[order] = bucketIncrement
        }
    }

    private fun populateV1Template() {
        val allOrders = v1orderToTemplatesName.keys.toList().sorted()
        allOrders.forEachIndexed { ind, order ->
            val smallerOrders = allOrders.subList(0, ind)
            logger.info("order $order, smaller orders: $smallerOrders")
            val increments = smallerOrders.map { v1orderToBucketIncrement[it]!! }.sum()

            val templates = v1orderToTemplatesName[order]!!
            templates.forEach {
                // initialize in bucketizeV1Templates
                val priority = v1ProcessedPriority[it]!! + increments
                val policyID = v1TemplateToPolicyIDs[it]!!
                val indexPatterns = allV1Templates[it].patterns()
                logger.info("template $it, priority $priority, policy $policyID")
                saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, priority, lastUpdatedTime))
            }
        }
    }

    private fun saveISMTemplateToMap(policyID: String, ismTemplate: ISMTemplate) {
        val policyToISMTemplate = ismTemplateMap[policyID]
        if (policyToISMTemplate != null) {
            ismTemplateMap[policyID]!!.add(ismTemplate)
        } else {
            ismTemplateMap[policyID] = mutableListOf(ismTemplate)
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

    private fun populateV2ISMTemplateMap(policyID: String, indexPatterns: List<String>, priority: Int) {
        var v1Increment = 0
        if (v1orderToBucketIncrement.isNotEmpty()) {
            v1Increment = v1orderToBucketIncrement.keys.min()!! + v1orderToBucketIncrement.values.sum()
        }

        saveISMTemplateToMap(policyID, ISMTemplate(indexPatterns, priority+v1Increment, lastUpdatedTime))
    }

    private fun overlapping(patterns1: List<String>, patterns2: List<String>): Boolean {
        if (patterns1.isEmpty() || patterns2.isEmpty()) return false
        val a1 = Regex.simpleMatchToAutomaton(*patterns1.toTypedArray())
        val a2 = Regex.simpleMatchToAutomaton(*patterns2.toTypedArray())
        return !Operations.isEmpty(Operations.intersection(a1, a2))
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
        val negOrderSet = mutableSetOf<Int>()
        clusterService.state().metadata.templates.forEach {
            val priority = it.value.order
            if (priority < 0) {
                negOrderSet.add(priority)
            }
        }
        val sorted = negOrderSet.sorted()
        var p = 0
        for (i in sorted) {
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
