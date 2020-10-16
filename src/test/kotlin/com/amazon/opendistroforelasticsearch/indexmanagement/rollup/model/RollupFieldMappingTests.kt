package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.RollupFieldMapping.Companion.UNKNOWN_MAPPING
import org.elasticsearch.test.ESTestCase

class RollupFieldMappingTests : ESTestCase() {

    fun `test toIssue`() {
        var fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "dummy-field", "terms")
        var actual = fieldMapping.toIssue()
        assertEquals("missing terms grouping on dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)

        fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.METRIC, "dummy-field", "sum")
        actual = fieldMapping.toIssue()
        assertEquals("missing sum aggregation on dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)

        fieldMapping = RollupFieldMapping(RollupFieldMapping.Companion.FieldType.DIMENSION, "dummy-field", UNKNOWN_MAPPING)
        actual = fieldMapping.toIssue(false)
        assertEquals("missing field dummy-field", actual)

        actual = fieldMapping.toIssue(true)
        assertEquals("missing field dummy-field", actual)
    }
}