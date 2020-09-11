<<<<<<< HEAD
/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

=======
>>>>>>> actionify change policy API
package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class RemovePolicyRequestTests : ESTestCase() {

    fun `test remove policy request`() {
        val indices = listOf("index1", "index2")
        val req = RemovePolicyRequest(indices)

        val out = BytesStreamOutput()
        req.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newReq = RemovePolicyRequest(sin)
        assertEquals(indices, newReq.indices)
    }
}
