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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.NotificationActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.CustomWebhook
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.Destination
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.destination.DestinationType
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.elasticsearch.script.Script
import org.elasticsearch.script.ScriptType
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class NotificationActionIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    // cannot test chime/slack in integ tests, but can test a custom webhook by
    // using the POST call to write to the local integTest cluster and verify that index exists
    fun `test custom webhook notification`() {
        val indexName = "${testIndexName}_index"
        val policyID = "${testIndexName}_testPolicyName"
        val notificationIndex = "notification_index"
        val clusterUri = System.getProperty("tests.rest.cluster").split(",")[0]
        val destination = Destination(
            type = DestinationType.CUSTOM_WEBHOOK,
            chime = null,
            slack = null,
            customWebhook = CustomWebhook(
                url = "http://$clusterUri/$notificationIndex/_doc",
                scheme = null,
                host = null,
                port = -1,
                path = null,
                queryParams = emptyMap(),
                headerParams = mapOf("Content-Type" to "application/json"),
                username = null,
                password = null
            )
        )
        val messageTemplate = Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, "{ \"testing\": 5 }", emptyMap())
        val actionConfig = NotificationActionConfig(destination = destination, messageTemplate = messageTemplate, index = 0)
        val states = listOf(State(name = "NotificationState", actions = listOf(actionConfig), transitions = emptyList()))
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
        createIndex(indexName, policyID)

        val managedIndexConfig = getExistingManagedIndexConfig(indexName)

        // Change the start time so the job will trigger in 2 seconds, this will trigger the first initialization of the policy
        updateManagedIndexConfigStartTime(managedIndexConfig)

        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName).policyID) }

        // verify index does not exist
        assertFalse("Notification index exists before notification has been sent", indexExists(notificationIndex))

        // Speed up to second execution where it will trigger the first execution of the action which
        // should call notification custom webhook and create the doc in notification_index
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // verify index does exist
        waitFor { assertTrue("Notification index does not exist", indexExists(notificationIndex)) }
    }
}
