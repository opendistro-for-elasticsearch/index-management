package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.ISM_TEMPLATE_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ISMTemplate
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.Policy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.State
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.action.ReadOnlyActionConfig
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomErrorNotification
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.settings.ManagedIndexSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.randomInstant
import com.amazon.opendistroforelasticsearch.indexmanagement.waitFor
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.rest.RestRequest.Method.GET
import org.junit.Assert
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.*

class ISMTemplateRestAPIIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test ISM template`() {
        val templateName = "t1"
        val ismTemp = ISMTemplate(listOf("log*"), "policy_1", 100, randomInstant())

        var res = createISMTemplate(templateName, ismTemp)
        assertEquals("Unable to create new ISM template", RestStatus.CREATED, res.restStatus())

        res = createISMTemplate(templateName, ismTemp)
        assertEquals("Unable to update new ISM template", RestStatus.OK, res.restStatus())

        var getRes = getISMTemplatesAsObject(templateName)
        assertISMTemplateEquals(ismTemp, getRes[templateName])

        val templateName2 = "t2"
        val ismTemp2 = ISMTemplate(listOf("trace*"), "policy_1", 100, randomInstant())
        createISMTemplate(templateName2, ismTemp2)
        getRes = getISMTemplatesAsObject("$templateName,$templateName2")
        val getRes2 = getISMTemplatesAsObject(null)
        assertEquals(getRes, getRes2)
        assertISMTemplateEquals(ismTemp, getRes[templateName])
        assertISMTemplateEquals(ismTemp2, getRes[templateName2])

    }

    fun `test get not exist template`() {
        val tn = "t1"
        try {
            client().makeRequest(GET.toString(), "${ISM_TEMPLATE_BASE_URI}/$tn")
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.NOT_FOUND, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectErrorMessage = mapOf(
                "error" to mapOf(
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("type" to "resource_not_found_exception", "reason" to "index template matching [$tn] not found")
                    ),
                    "type" to "resource_not_found_exception",
                    "reason" to "index template matching [$tn] not found"
                ),
                "status" to 404
            )
            assertEquals(expectErrorMessage, actualMessage)
        }
    }

    fun `test add template with invalid index pattern`() {
        val tn = "t1"
        try {
            val ismTemp = ISMTemplate(listOf(" "), "policy_1", 100, randomInstant())
            createISMTemplate(tn, ismTemp)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectErrorMessage = mapOf(
                "error" to mapOf(
                    "reason" to "index_template [$tn] invalid, cause [Validation Failed: 1: index_patterns [ ] must not contain a space;2: index_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];]",
                    "type" to "invalid_index_template_exception",
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("reason" to "index_template [$tn] invalid, cause [Validation Failed: 1: index_patterns [ ] must not contain a space;2: index_pattern [ ] must not contain the following characters [ , \", *, \\, <, |, ,, >, /, ?];]",
                            "type" to "invalid_index_template_exception")
                    )
                ),
                "status" to 400
            )
            assertEquals(expectErrorMessage, actualMessage)
        }
    }

    fun `test add template with overlapping index pattern`() {
        val tn = "t2"
        try {
            val ismTemp = ISMTemplate(listOf("log*"), "policy_1", 100, randomInstant())
            createISMTemplate("t1", ismTemp)
            createISMTemplate(tn, ismTemp)
            fail("Expect a failure")
        } catch (e: ResponseException) {
            assertEquals("Unexpected RestStatus", RestStatus.BAD_REQUEST, e.response.restStatus())
            val actualMessage = e.response.asMap()
            val expectErrorMessage = mapOf(
                "error" to mapOf(
                    "reason" to "new ism template $tn has index pattern [log*] matching existing templates t1 => [log*], please use a different priority than 100",
                    "type" to "illegal_argument_exception",
                    "root_cause" to listOf<Map<String, Any>>(
                        mapOf("reason" to "new ism template $tn has index pattern [log*] matching existing templates t1 => [log*], please use a different priority than 100",
                            "type" to "illegal_argument_exception")
                    )
                ),
                "status" to 400
            )
            assertEquals(expectErrorMessage, actualMessage)
        }
    }

    fun `test ism template managing index`() {
        val indexName1 = "log-000001"
        val indexName2 = "log-000002"
        val indexName3 = "log-000003"
        val policyID = "${testIndexName}_testPolicyName_1"

        // need to specify policyID null, can remove after policyID deprecated
        createIndex(indexName1, null)
        val templateName = "t1"
        val ismTemp = ISMTemplate(listOf("log*"), policyID, 100, randomInstant())
        createISMTemplate(templateName, ismTemp)

        println("ism template: ${getISMTemplatesAsObject(null)}")

        val actionConfig = ReadOnlyActionConfig(0)
        val states = listOf(
            State("ReadOnlyState", listOf(actionConfig), listOf())
        )
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
        createIndex(indexName2, null)
        createIndex(indexName3, Settings.builder().put("index.hidden", true).build())

        val managedIndexConfig = getExistingManagedIndexConfig(indexName2)
        updateManagedIndexConfigStartTime(managedIndexConfig)
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(indexName2).policyID) }

        // only index create after template can be managed
        assertPredicatesOnMetaData(
            listOf(indexName1 to listOf(ManagedIndexSettings.POLICY_ID.key to fun(policyID: Any?): Boolean = policyID == null)),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName1))


        // hidden index will not be manage
        assertPredicatesOnMetaData(
            listOf(indexName1 to listOf(ManagedIndexSettings.POLICY_ID.key to fun(policyID: Any?): Boolean = policyID == null)),
            getExplainMap(indexName1),
            true
        )
        assertNull(getManagedIndexConfig(indexName3))
    }

}