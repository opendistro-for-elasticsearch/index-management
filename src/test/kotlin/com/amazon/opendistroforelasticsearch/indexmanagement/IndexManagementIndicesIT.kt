package com.amazon.opendistroforelasticsearch.indexmanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices.Companion.HISTORY_INDEX_BASE
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices.Companion.HISTORY_WRITE_INDEX_ALIAS
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices.Companion.indexStateManagementHistoryMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices.Companion.indexManagementMappings
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices.Companion.indexManagementSettings
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.INDEX_MANAGEMENT_INDEX
import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementPlugin.Companion.POLICY_BASE_URI
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.IndexStateManagementRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model.ChangePolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomPolicy
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.resthandler.RestChangePolicyAction
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILED_INDICES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.FAILURES
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.util.UPDATED_INDICES
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.test.ESTestCase
import java.util.Locale

class IndexManagementIndicesIT : IndexStateManagementRestTestCase() {

    private val testIndexName = javaClass.simpleName.toLowerCase(Locale.ROOT)

    /*
    * If this test fails it means you changed the config mappings
    * This test is to ensure you did not forget to increase the schema_version in the mappings _meta object
    * The schema_version is used at runtime to check if the mappings need to be updated for the index
    * Once you are sure you increased the schema_version or know it is not needed you can update the cached mappings with the new values
    * */
    fun `test config mappings schema version number`() {
        val cachedMappings = javaClass.classLoader.getResource("mappings/cached-opendistro-ism-config.json")!!.readText()
        assertEquals("I see you updated the config mappings. Did you also update the schema_version?", cachedMappings, indexManagementMappings)
    }

    /*
    * If this test fails it means you changed the history mappings
    * This test is to ensure you did not forget to increase the schema_version in the mappings _meta object
    * The schema_version is used at runtime to check if the mappings need to be updated for the index
    * Once you are sure you increased the schema_version or know it is not needed you can update the cached mappings with the new values
    * */
    fun `test history mappings schema version number`() {
        val cachedMappings = javaClass.classLoader.getResource("mappings/cached-opendistro-ism-history.json")!!.readText()
        assertEquals("I see you updated the history mappings. Did you also update the schema_version?", cachedMappings, indexStateManagementHistoryMappings)
    }

    fun `test create index management`() {
        val policy = randomPolicy()
        val policyId = ESTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())
        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 5)
    }

    fun `test update management index mapping with new schema version`() {
        assertIndexDoesNotExist(INDEX_MANAGEMENT_INDEX)

        val mapping = indexManagementMappings.trim().trimStart('{').trimEnd('}')
            .replace("\"schema_version\": 5", "\"schema_version\": 0")

        createIndex(INDEX_MANAGEMENT_INDEX, Settings.builder().loadFromSource(indexManagementSettings, XContentType.JSON).build(), mapping)
        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 0)

        val policy = randomPolicy()
        val policyId = ESTestCase.randomAlphaOfLength(10)
        client().makeRequest("PUT", "$POLICY_BASE_URI/$policyId", emptyMap(), policy.toHttpEntity())

        assertIndexExists(INDEX_MANAGEMENT_INDEX)
        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 5)
    }

    fun `test update management index history mappings with new schema version`() {
        assertIndexDoesNotExist("$HISTORY_WRITE_INDEX_ALIAS?allow_no_indices=false")

        val mapping = indexStateManagementHistoryMappings.trim().trimStart('{').trimEnd('}')
                .replace("\"schema_version\": 2", "\"schema_version\": 0")

        val aliases = "\"$HISTORY_WRITE_INDEX_ALIAS\": { \"is_write_index\": true }"
        createIndex("$HISTORY_INDEX_BASE-1", Settings.builder().put("index.hidden", true).build(), mapping, aliases)
        assertIndexExists(HISTORY_WRITE_INDEX_ALIAS)
        verifyIndexSchemaVersion(HISTORY_WRITE_INDEX_ALIAS, 0)

        val policy = createRandomPolicy()
        val (index, policyID) = createIndex("history-schema", policy.id)

        val managedIndexConfig = getExistingManagedIndexConfig(index)

        // Change the start time so the job will trigger in 2 seconds.
        updateManagedIndexConfigStartTime(managedIndexConfig)

        // wait for the policy to initialize which will add 1 history document to the history index
        // this should update the history mappings to the new version
        waitFor { assertEquals(policyID, getExplainManagedIndexMetaData(index).policyID) }

        waitFor {
            assertIndexExists(HISTORY_WRITE_INDEX_ALIAS)
            verifyIndexSchemaVersion(HISTORY_WRITE_INDEX_ALIAS, 2)
        }
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

        val mapping = "{" + indexManagementMappings.trimStart('{').trimEnd('}')
            .replace("\"schema_version\": 5", "\"schema_version\": 0")

        val entity = StringEntity(mapping, ContentType.APPLICATION_JSON)
        client().makeRequest(RestRequest.Method.PUT.toString(),
            "/$INDEX_MANAGEMENT_INDEX/_mapping", emptyMap(), entity)

        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 0)

        // if we try to change policy now, it'll have no ManagedIndexMetaData yet and should succeed
        val changePolicy = ChangePolicy(newPolicy.id, null, emptyList(), false)
        val response = client().makeRequest(
            RestRequest.Method.POST.toString(),
            "${RestChangePolicyAction.CHANGE_POLICY_BASE_URI}/$index", emptyMap(), changePolicy.toHttpEntity())

        verifyIndexSchemaVersion(INDEX_MANAGEMENT_INDEX, 5)

        assertAffectedIndicesResponseIsEqual(mapOf(FAILURES to false, FAILED_INDICES to emptyList<Any>(), UPDATED_INDICES to 1), response.asMap())

        waitFor { assertEquals(newPolicy.id, getManagedIndexConfig(index)?.changePolicy?.policyID) }
    }
}