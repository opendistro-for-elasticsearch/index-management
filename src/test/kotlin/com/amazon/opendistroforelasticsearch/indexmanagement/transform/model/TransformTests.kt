package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransform
import org.elasticsearch.test.ESTestCase
import kotlin.test.assertFailsWith

class TransformTests : ESTestCase() {

    fun `test transform same indices`() {
        assertFailsWith(IllegalArgumentException::class, "Source and target index cannot be the same") {
            randomTransform().copy(sourceIndex = "dummy-index", targetIndex = "dummy-index")
        }
    }

    fun `test transform requires at least one grouping`() {
        assertFailsWith(IllegalArgumentException::class, "Must specify at least one grouping") {
            randomTransform().copy(groups = listOf())
        }
    }

    fun `test transform requires page size to be between 1 and 10K`() {
        assertFailsWith(IllegalArgumentException::class, "Page size was less than 1") {
            randomTransform().copy(pageSize = -1)
        }

        assertFailsWith(IllegalArgumentException::class, "Page size was greater than 10K") {
            randomTransform().copy(pageSize = 10001)
        }

        randomTransform().copy(pageSize = 1)
        randomTransform().copy(pageSize = 10000)
        randomTransform().copy(pageSize = 500)
    }
}