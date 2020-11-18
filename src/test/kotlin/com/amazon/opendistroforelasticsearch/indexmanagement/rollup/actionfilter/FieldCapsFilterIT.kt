package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter

import com.amazon.opendistroforelasticsearch.indexmanagement.makeRequest
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.RollupRestTestCase
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings.RollupSettings
import org.elasticsearch.client.ResponseException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestStatus

// TODO: Add assertions on fields
@Suppress("UNCHECKED_CAST")
class FieldCapsFilterIT : RollupRestTestCase() {

    fun `test field caps interception`() {
        createIndex("raw-data", Settings.EMPTY, """"properties":{"field-1":{"type":"boolean"},"field-2":{"type":"integer"},"field-3":{"type":"float"},"field-4":{"type":"keyword"},"field-5":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},"field-6":{"type":"text","fields":{"field-6-1":{"type":"keyword"}}},"field-7":{"properties":{"field-7-1":{"type":"geo_point"}}}}""")
        createIndex("rollup-data", Settings.builder().put(RollupSettings.ROLLUP_INDEX.key, true).build(), """"properties":{"field-1":{"type":"keyword"}}""")

        var response = client().makeRequest("GET", "/*-data/_field_caps?fields=*")
        assertTrue(response.restStatus() == RestStatus.OK)
        var data = response.asMap()
        var indices = data["indices"] as List<String>
        assertTrue(indices.containsAll(listOf("raw-data", "rollup-data")))

        // Request for all indices
        response = client().makeRequest("GET", "//_field_caps?fields=*")
        assertTrue(response.restStatus() == RestStatus.OK)
        data = response.asMap()
        indices = data["indices"] as List<String>
        assertTrue(indices.containsAll(listOf("raw-data", "rollup-data")))

        // Request for only rollup indices
        response = client().makeRequest("GET", "/rollup*/_field_caps?fields=*")
        assertTrue(response.restStatus() == RestStatus.OK)
        data = response.asMap()
        indices = data["indices"] as List<String>
        assertTrue(indices.containsAll(listOf("rollup-data")))

        // Request for only non-rollup indices
        response = client().makeRequest("GET", "/raw*/_field_caps?fields=*")
        assertTrue(response.restStatus() == RestStatus.OK)
        data = response.asMap()
        indices = data["indices"] as List<String>
        assertTrue(indices.containsAll(listOf("raw-data")))

        // Unknown index
        try {
            client().makeRequest("GET", "/unknown/_field_caps?fields=*")
            fail("Expected 404 not_found exception")
        } catch (e: ResponseException) {
            assertTrue(e.response.restStatus() == RestStatus.NOT_FOUND)
            val error = e.response.asMap()["error"] as Map<String, *>
            assertEquals("index_not_found_exception", error["type"])
        }
    }
}
