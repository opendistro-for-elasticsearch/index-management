package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.actionfilter

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.randomISMFieldCaps
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse
import org.elasticsearch.test.ESTestCase

class SerDeTests : ESTestCase() {

    fun `test round trip empty`() {
        val fieldCaps = FieldCapabilitiesResponse(arrayOf(), mapOf())
        val roundTripFromFieldCaps = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(fieldCaps).toFieldCapabilitiesResponse()
        assertEquals("Round tripping didn't work", fieldCaps, roundTripFromFieldCaps)
    }

    fun `test round trip nonempty`() {
        val ismFieldCaps = randomISMFieldCaps()
        val fieldCaps = ismFieldCaps.toFieldCapabilitiesResponse()
        val roundTrippedFieldCaps = ISMFieldCapabilitiesResponse.fromFieldCapabilitiesResponse(fieldCaps).toFieldCapabilitiesResponse()
        assertEquals("Round tripping didn't work", fieldCaps, roundTrippedFieldCaps)
        assertEquals("Expected indices are different", ismFieldCaps.indices.size, roundTrippedFieldCaps.indices.size)
        assertEquals("Expected response map is different", ismFieldCaps.responseMap.size, roundTrippedFieldCaps.get().size)
    }
}