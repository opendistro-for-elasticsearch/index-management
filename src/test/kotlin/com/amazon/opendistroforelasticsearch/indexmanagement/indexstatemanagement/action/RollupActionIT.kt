package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.RollupActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.AttemptCreateRollupJobStep
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.step.rollup.WaitForRollupCompletionStep
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.ISMRollup
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetadata
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupMetrics
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.DateHistogram
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.dimension.Terms
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.toJsonString
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class RollupActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test rollup action`() {
        val indexName = "${testIndexName}_index_basic"
        val policyID = "${testIndexName}_policy_basic"
        val rollup = ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                pageSize = 100,
                dimensions = listOf(
                        DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID"),
                        Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(),
                                ValueCount(), Average())),
                        RollupMetrics(sourceField = "total_amount", targetField = "total_amount", metrics = listOf(Max(), Min()))
                )
        )
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupActionConfig(rollup, 0)
        val states = listOf(
                State("rollup", listOf(actionConfig), listOf())
        )
        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"PULocationID\": { \"type\": \"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
        val policy = Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    WaitForRollupCompletionStep.getJobCompletionMessage(rollupId),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        waitFor {
            val rollupJob = getRollup(rollupId = rollupId)
            assertNotNull("Rollup job doesn't have metadata set", rollupJob.metadataID)
            val rollupMetadata = getRollupMetadata(rollupJob.metadataID!!)
            assertEquals("Rollup is not finished", RollupMetadata.Status.FINISHED, rollupMetadata.status)
        }
    }

    fun `test rollup action failure`() {
        val indexName = "${testIndexName}_index_failure"
        val policyID = "${testIndexName}_policy_failure"
        val rollup = ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                pageSize = 100,
                dimensions = listOf(
                        DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID"),
                        Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(),
                                ValueCount(), Average()))
                )
        )
        val rollupId = rollup.toRollup(indexName).id
        val actionConfig = RollupActionConfig(rollup, 0)
        val states = listOf(
                State("rollup", listOf(actionConfig), listOf())
        )
        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
        val policy = Policy(
                id = policyID,
                description = "$testIndexName description",
                schemaVersion = 1L,
                lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
                errorNotification = randomErrorNotification(),
                defaultState = states[0].name,
                states = states
        )
        createPolicy(policy, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        Thread.sleep(60000)

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    WaitForRollupCompletionStep.getJobFailedMessage(rollupId),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }

    fun `test rollup action failure and retry failed step`() {
        val indexName = "${testIndexName}_index_retry"
        val policyID = "${testIndexName}_policy_retry"
        val rollup = ISMRollup(
                description = "basic search test",
                targetIndex = "target_rollup_search",
                pageSize = 100,
                dimensions = listOf(
                        DateHistogram(sourceField = "tpep_pickup_datetime", fixedInterval = "1h"),
                        Terms("RatecodeID", "RatecodeID"),
                        Terms("PULocationID", "PULocationID")
                ),
                metrics = listOf(
                        RollupMetrics(sourceField = "passenger_count", targetField = "passenger_count", metrics = listOf(Sum(), Min(), Max(),
                                ValueCount(), Average()))
                )
        )
        val rollupId = rollup.toRollup(indexName).id
        val policyString = "{\"policy\":{\"description\":\"$testIndexName description\",\"default_state\":\"rollup\",\"states\":[{\"name\":\"rollup\"," +
                "\"actions\":[{\"retry\":{\"count\":2,\"backoff\":\"constant\",\"delay\":\"10ms\"},\"rollup\":{\"ism_rollup\":" +
                "${rollup.toJsonString()}}}],\"transitions\":[]}]}}"

        val sourceIndexMappingString = "\"properties\": {\"tpep_pickup_datetime\": { \"type\": \"date\" }, \"RatecodeID\": { \"type\": " +
                "\"keyword\" }, \"passenger_count\": { \"type\": \"integer\" }, \"total_amount\": " +
                "{ \"type\": \"double\" }}"
        createPolicyJson(policyString, policyID)
        createIndex(indexName, policyID, mapping = sourceIndexMappingString)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will initialize the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // Change the start time so we attempt to create rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    AttemptCreateRollupJobStep.getSuccessMessage(rollupId, indexName),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Change the start time so wait for rollup step will execute
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    WaitForRollupCompletionStep.getJobProcessingMessage(rollupId),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }

        // Wait for few seconds and change start time so wait for rollup step will execute again - job will be failed
        Thread.sleep(60000)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor {
            assertEquals(
                    WaitForRollupCompletionStep.getJobFailedMessage(rollupId),
                    getExplainManagedIndexMetaData(indexName).info?.get("message")
            )
        }
    }
}