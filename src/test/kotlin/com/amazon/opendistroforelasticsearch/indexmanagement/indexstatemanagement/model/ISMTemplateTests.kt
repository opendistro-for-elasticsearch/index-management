package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomISMTemplate
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput
import org.elasticsearch.test.ESTestCase
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

class ISMTemplateTests : ESTestCase() {

    fun `test basic`() {
        val expectedISMTemplate = randomISMTemplate()

        roundTripISMTemplate(expectedISMTemplate)
    }

    private fun roundTripISMTemplate(expectedISMTemplate: ISMTemplate) {
        val baos = ByteArrayOutputStream()
        val osso = OutputStreamStreamOutput(baos)
        expectedISMTemplate.writeTo(osso)
        val input = InputStreamStreamInput(ByteArrayInputStream(baos.toByteArray()))

        val actualISMTemplate = ISMTemplate(input)
        assertEquals(expectedISMTemplate, actualISMTemplate)
    }
}