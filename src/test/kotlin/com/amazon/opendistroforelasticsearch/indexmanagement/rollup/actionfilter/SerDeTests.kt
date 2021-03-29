/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
