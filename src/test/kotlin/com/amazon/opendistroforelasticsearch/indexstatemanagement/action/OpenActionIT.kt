package com.amazon.opendistroforelasticsearch.indexstatemanagement.action

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.OpenActionConfig
import com.amazon.opendistroforelasticsearch.indexstatemanagement.randomDefaultNotification
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Locale

class OpenActionIT : IndexStateManagementRestTestCase() {

    fun `test basic`() {
        val indexName = "${javaClass.simpleName.toLowerCase(Locale.getDefault())}_index"
        val policyId = "${javaClass.simpleName.toLowerCase(Locale.getDefault())}_testPolicyName"
        val actionConfig = OpenActionConfig(null, null, 0)
        val states = listOf(
            State("OpenState", listOf(actionConfig), listOf())
        )

        val policy = Policy(id = policyId,
            name = "${javaClass.simpleName.toLowerCase(Locale.getDefault())}_testPolicyName",
            schemaVersion = 1L,
            lastUpdatedTime = Instant.now().truncatedTo(ChronoUnit.MILLIS),
            defaultNotification = randomDefaultNotification(),
            defaultState = states[0].name,
            states = states)
        createPolicy(policy, policyId)
        createIndex(indexName, null)
        closeIndex(indexName)

        assertEquals("close", getIndexState(indexName))

        addPolicyToIndex(indexName, policyId)
        Thread.sleep(2000)

        val managedIndexConfig = getManagedIndexConfig(indexName)
        assertNotNull("ManagedIndexConfig is null", managedIndexConfig)
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig!!, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        // Need to wait two cycles.
        // change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig, Instant.now().minusSeconds(58).toEpochMilli())

        Thread.sleep(3000)

        assertEquals("open", getIndexState(indexName))
    }
}
