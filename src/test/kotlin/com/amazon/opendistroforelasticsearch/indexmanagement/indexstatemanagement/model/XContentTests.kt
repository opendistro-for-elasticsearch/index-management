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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.parseWithType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.nonNullRandomConditions
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomAllocationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomDeleteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomDestination
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomForceMergeActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomManagedIndexConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomNotificationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReadWriteActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomReplicaCountActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomIndexPriorityActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomRolloverActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomSnapshotActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomState
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomTransition
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.toJsonString
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler
import org.elasticsearch.common.xcontent.XContentParser
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase

class XContentTests : ESTestCase() {

    fun `test policy parsing`() {
        val policy = randomPolicy()

        val policyString = policy.toJsonString()
        val parsedPolicy = parserWithType(policyString).parseWithType(policy.id, policy.seqNo, policy.primaryTerm, Policy.Companion::parse)
        assertEquals("Round tripping Policy doesn't work", policy, parsedPolicy)
    }

    fun `test state parsing`() {
        val state = randomState()

        val stateString = state.toJsonString()
        val parsedState = State.parse(parser(stateString))
        assertEquals("Round tripping State doesn't work", state, parsedState)
    }

    fun `test transition parsing`() {
        val transition = randomTransition()

        val transitionString = transition.toJsonString()
        val parsedTransition = Transition.parse(parser(transitionString))
        assertEquals("Round tripping Transition doesn't work", transition, parsedTransition)
    }

    fun `test conditions parsing`() {
        val conditions = nonNullRandomConditions()

        val conditionsString = conditions.toJsonString()
        val parsedConditions = Conditions.parse(parser(conditionsString))
        assertEquals("Round tripping Conditions doesn't work", conditions, parsedConditions)
    }

    fun `test action config parsing`() {
        val deleteActionConfig = randomDeleteActionConfig()

        val deleteActionConfigString = deleteActionConfig.toJsonString()
        val parsedActionConfig = ActionConfig.parse((parser(deleteActionConfigString)), 0)
        assertEquals("Round tripping ActionConfig doesn't work", deleteActionConfig as ActionConfig, parsedActionConfig)
    }

    fun `test delete action config parsing`() {
        val deleteActionConfig = randomDeleteActionConfig()

        val deleteActionConfigString = deleteActionConfig.toJsonString()
        val parsedDeleteActionConfig = ActionConfig.parse(parser(deleteActionConfigString), 0)
        assertEquals("Round tripping DeleteActionConfig doesn't work", deleteActionConfig, parsedDeleteActionConfig)
    }

    fun `test rollover action config parsing`() {
        val rolloverActionConfig = randomRolloverActionConfig()

        val rolloverActionConfigString = rolloverActionConfig.toJsonString()
        val parsedRolloverActionConfig = ActionConfig.parse(parser(rolloverActionConfigString), 0)
        assertEquals("Round tripping RolloverActionConfig doesn't work", rolloverActionConfig, parsedRolloverActionConfig)
    }

    fun `test read_only action config parsing`() {
        val readOnlyActionConfig = randomReadOnlyActionConfig()

        val readOnlyActionConfigString = readOnlyActionConfig.toJsonString()
        val parsedReadOnlyActionConfig = ActionConfig.parse(parser(readOnlyActionConfigString), 0)
        assertEquals("Round tripping ReadOnlyActionConfig doesn't work", readOnlyActionConfig, parsedReadOnlyActionConfig)
    }

    fun `test read_write action config parsing`() {
        val readWriteActionConfig = randomReadWriteActionConfig()

        val readWriteActionConfigString = readWriteActionConfig.toJsonString()
        val parsedReadWriteActionConfig = ActionConfig.parse(parser(readWriteActionConfigString), 0)
        assertEquals("Round tripping ReadWriteActionConfig doesn't work", readWriteActionConfig, parsedReadWriteActionConfig)
    }

    fun `test replica_count action config parsing`() {
        val replicaCountActionConfig = randomReplicaCountActionConfig()

        val replicaCountActionConfigString = replicaCountActionConfig.toJsonString()
        val parsedReplicaCountActionConfig = ActionConfig.parse(parser(replicaCountActionConfigString), 0)
        assertEquals("Round tripping ReplicaCountActionConfig doesn't work", replicaCountActionConfig, parsedReplicaCountActionConfig)
    }

    fun `test set_index_priority action config parsing`() {
        val indexPriorityActionConfig = randomIndexPriorityActionConfig()

        val indexPriorityActionConfigString = indexPriorityActionConfig.toJsonString()
        val parsedIndexPriorityActionConfig = ActionConfig.parse(parser(indexPriorityActionConfigString), 0)
        assertEquals("Round tripping indexPriorityActionConfig doesn't work", indexPriorityActionConfig, parsedIndexPriorityActionConfig)
    }

    fun `test force_merge action config parsing`() {
        val forceMergeActionConfig = randomForceMergeActionConfig()

        val forceMergeActionConfigString = forceMergeActionConfig.toJsonString()
        val parsedForceMergeActionConfig = ActionConfig.parse(parser(forceMergeActionConfigString), 0)
        assertEquals("Round tripping ForceMergeActionConfig doesn't work", forceMergeActionConfig, parsedForceMergeActionConfig)
    }

    fun `test notification action config parsing`() {
        val chimeNotificationActionConfig = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.CHIME))
        val slackNotificationActionConfig = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.SLACK))
        val customNotificationActionConfig = randomNotificationActionConfig(destination = randomDestination(type = DestinationType.CUSTOM_WEBHOOK))

        val chimeNotificationActionConfigString = chimeNotificationActionConfig.toJsonString()
        val chimeParsedNotificationActionConfig = ActionConfig.parse(parser(chimeNotificationActionConfigString), 0)
        assertEquals("Round tripping chime NotificationActionConfig doesn't work",
            chimeNotificationActionConfig, chimeParsedNotificationActionConfig)

        val slackNotificationActionConfigString = slackNotificationActionConfig.toJsonString()
        val slackParsedNotificationActionConfig = ActionConfig.parse(parser(slackNotificationActionConfigString), 0)
        assertEquals("Round tripping slack NotificationActionConfig doesn't work",
            slackNotificationActionConfig, slackParsedNotificationActionConfig)

        val customNotificationActionConfigString = customNotificationActionConfig.toJsonString()
        val customParsedNotificationActionConfig = ActionConfig.parse(parser(customNotificationActionConfigString), 0)
        assertEquals("Round tripping custom webhook NotificationActionConfig doesn't work",
            customNotificationActionConfig, customParsedNotificationActionConfig)
    }

    fun `test snapshot action config parsing`() {
        val snapshotActionConfig = randomSnapshotActionConfig("repository", "snapshot")

        val snapshotActionConfigString = snapshotActionConfig.toJsonString()
        val parsedNotificationActionConfig = ActionConfig.parse(parser(snapshotActionConfigString), 0)
        assertEquals("Round tripping SnapshotActionConfig doesn't work", snapshotActionConfig, parsedNotificationActionConfig)
    }

    fun `test allocation action config parsing`() {
        val allocationActionConfig = randomAllocationActionConfig(require = mapOf("box_type" to "hot"))

        val allocationActionConfigString = allocationActionConfig.toJsonString()
        val parsedAllocationActionConfig = ActionConfig.parse(parser(allocationActionConfigString), 0)
        assertEquals("Round tripping AllocationActionConfig doesn't work", allocationActionConfig, parsedAllocationActionConfig)
    }

    fun `test managed index config parsing`() {
        val config = randomManagedIndexConfig()
        val configTwo = config.copy(changePolicy = null)
        var configThree = config.copy()

        val configString = config.toJsonString()
        val configTwoString = configTwo.toJsonString()
        val configThreeString = configThree.toJsonString()
        val parsedConfig =
            parserWithType(configString).parseWithType(config.id, config.seqNo, config.primaryTerm, ManagedIndexConfig.Companion::parse)
        val parsedConfigTwo =
            parserWithType(configTwoString).parseWithType(configTwo.id, configTwo.seqNo, configTwo.primaryTerm, ManagedIndexConfig.Companion::parse)
        configThree = configThree.copy(id = "some_doc_id", seqNo = 17, primaryTerm = 1)
        val parsedConfigThree =
            parserWithType(configThreeString).parseWithType(configThree.id, configThree.seqNo, configThree.primaryTerm, ManagedIndexConfig.Companion::parse)

        assertEquals("Round tripping ManagedIndexConfig doesn't work", config, parsedConfig)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with null change policy", configTwo, parsedConfigTwo)
        assertEquals("Round tripping ManagedIndexConfig doesn't work with id and version", configThree, parsedConfigThree)
    }

    fun `test managed index metadata parsing`() {
        val metadata = ManagedIndexMetaData(
            index = randomAlphaOfLength(10),
            indexUuid = randomAlphaOfLength(10),
            policyID = randomAlphaOfLength(10),
            policySeqNo = randomNonNegativeLong(),
            policyPrimaryTerm = randomNonNegativeLong(),
            policyCompleted = null,
            rolledOver = null,
            transitionTo = randomAlphaOfLength(10),
            stateMetaData = null,
            actionMetaData = null,
            stepMetaData = null,
            policyRetryInfo = null,
            info = null
        )
        val metadataString = metadata.toJsonString()
        val parsedMetaData = ManagedIndexMetaData.parse(parser(metadataString))
        assertEquals("Round tripping ManagedIndexMetaData doesn't work", metadata, parsedMetaData)
    }

    fun `test change policy parsing`() {
        val changePolicy = randomChangePolicy()

        val changePolicyString = changePolicy.toJsonString()
        val parsedChangePolicy = ChangePolicy.parse(parser(changePolicyString))
        assertEquals("Round tripping ChangePolicy doesn't work", changePolicy, parsedChangePolicy)
    }

    private fun parser(xc: String): XContentParser {
        val parser = XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
        parser.nextToken()
        return parser
    }

    private fun parserWithType(xc: String): XContentParser {
        return XContentType.JSON.xContent().createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, xc)
    }
}
