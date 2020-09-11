package com.amazon.opendistroforelasticsearch.indexmanagement.util

import org.elasticsearch.cluster.metadata.IndexMetadata
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class IndexUtilsTests : ESTestCase() {

    fun `test get schema version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": 1}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(1, schemaVersion)
    }

    fun `test get schema version without _meta`() {
        val message = "{\"user\":{ \"name\":\"test\"}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(1, schemaVersion)
    }

    fun `test get schema version without schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"test\": 1}}"

        val schemaVersion = IndexUtils.getSchemaVersion(message)
        assertEquals(1, schemaVersion)
    }

    fun `test get schema version with negative schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": -1}}"

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            IndexUtils.getSchemaVersion(message)
        }
    }

    fun `test get schema version with wrong schema_version`() {
        val message = "{\"user\":{ \"name\":\"test\"},\"_meta\":{\"schema_version\": \"wrong\"}}"

        assertFailsWith(IllegalArgumentException::class, "Expected IllegalArgumentException") {
            IndexUtils.getSchemaVersion(message)
        }
    }

    fun `test should update index without original version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
            "\"settings_version\":123,\"mappings\":{\"_doc\":{\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}"

        val parser = createParser(XContentType.JSON.xContent(), indexContent)
        val index: IndexMetadata = IndexMetadata.fromXContent(parser)

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, 10)
        assertTrue(shouldUpdateIndex)
    }

    fun `test should update index with lagged version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
            "\"settings_version\":123,\"mappings\":{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":" +
            "{\"name\":{\"type\":\"keyword\"}}}}}}"

        val parser = createParser(XContentType.JSON.xContent(), indexContent)
        val index: IndexMetadata = IndexMetadata.fromXContent(parser)

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, 10)
        assertTrue(shouldUpdateIndex)
    }

    fun `test should update index with same version`() {
        val indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
            "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
            "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mappings\":" +
            "{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":{\"name\":{\"type\":\"keyword\"}}}}}}"

        val parser = createParser(XContentType.JSON.xContent(), indexContent)
        val index: IndexMetadata = IndexMetadata.fromXContent(parser)

        val shouldUpdateIndex = IndexUtils.shouldUpdateIndex(index, 1)
        assertFalse(shouldUpdateIndex)
    }
}