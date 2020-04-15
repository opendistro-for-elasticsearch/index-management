package com.amazon.opendistroforelasticsearch.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementIndices.Companion.indexStateManagementMappings
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.INDEX_STATE_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexstatemanagement.IndexStateManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexstatemanagement.util.UPDATED_INDICES
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.test.ESTestCase
import java.util.Locale

class IndexStateManagementIndicesIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    fun `test create index management`() {
        val policy = randomPolicy()
        val policyId = ESTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())
        assertIndexExists(INDEX_STATE_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_STATE_MANAGEMENT_INDEX, 2)
    }

    fun `test update management index mapping with new schema version`() {
        assertIndexDoesNotExist(INDEX_STATE_MANAGEMENT_INDEX)

        val mapping = indexStateManagementMappings.trimStart('{').trimEnd('}')
            .replace("\"schema_version\": 2", "\"schema_version\": 0")

        createIndex(INDEX_STATE_MANAGEMENT_INDEX, Settings.EMPTY, mapping)
        assertIndexExists(INDEX_STATE_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_STATE_MANAGEMENT_INDEX, 0)
        client().makeRequest("DELETE", "*")

        val policy = randomPolicy()
        val policyId = ESTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())

        assertIndexExists(INDEX_STATE_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_STATE_MANAGEMENT_INDEX, 2)
    }

    fun `test changing policy on an index that hasn't initialized yet check schema version`() {
        val policy = createRandomPolicy()
        val newPolicy = createPolicy(randomPolicy(), "new_policy", true)
        val indexName = "${testIndexName}_computer"
        val (index) = createIndex(indexName, policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)
        assertNull("Change policy is not null", managedIndexConfig.changePolicy)
        assertNull("Policy has already initialized", managedIndexConfig.policy)
        assertEquals("Policy id does not match", policy.id, managedIndexConfig.policyID)

        val mapping = "{" + indexStateManagementMappings.trimStart('{').trimEnd('}')
            .replace("\"schema_version\": 2", "\"schema_version\": 0")

        val entity = StringEntity(mapping, ContentType.APPLICATION_JSON)
        client().makeRequest(RestRequest.Method.PUT.toString(),
            "/$INDEX_STATE_MANAGEMENT_INDEX/_mapping", emptyMap(), entity)

        verifyIndexSchemaVersion(INDEX_STATE_MANAGEMENT_INDEX, 0)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should succeed
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList(), false)
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity())

        verifyIndexSchemaVersion(INDEX_STATE_MANAGEMENT_INDEX, 2)

        assertAffectedIndicesResponseIsEqual(mapOf(FAILURES to false, FAILED_INDICES to emptyList<Any>(), UPDATED_INDICES to 1), response.asMap())

        waitFor { assertEquals(newPolicy.id, getManagedIndexConfig(index)?.changePolicy?.policyID) }
    }
}