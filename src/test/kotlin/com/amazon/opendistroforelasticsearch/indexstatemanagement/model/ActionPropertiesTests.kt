package com.amazon.opendistroforelasticsearch.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.managedindexmetadata.ActionProperties
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase

class ActionPropertiesTests : ESTestCase() {

    @Suppress("UNCHECKED_CAST")
    fun `test action properties exist in history index`() {
        // All properties inside the ActionProperties class need to be also added to the ism history mappings
        // This is to catch any commits/PRs that add to ActionProperties but forget to add to history mappings
        val expected = createParser(
            XContentType.JSON.xContent(),
            javaClass.classLoader.getResource("mappings/opendistro-ism-history.json")!!.readText())
        val expectedMap = expected.map() as Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Any>>>>>>>>
        val actionProperties = ActionProperties.Properties.values().map { it.key }
        val mappingActionProperties = expectedMap["properties"]!!["managed_index_meta_data"]!!["properties"]!!["action"]!!["properties"]!!["action_properties"]!!["properties"]
        assertNotNull("Could not get action properties from ism history mappings", mappingActionProperties)
        actionProperties.forEach { property ->
            assertTrue("The $property action property does not exist in the ism history mappings", mappingActionProperties!!.containsKey(property))
        }
    }
}