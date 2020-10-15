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

package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.model

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.emptyUser
import com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.randomUser
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class WriteableTests : ESTestCase() {

    fun `test user as stream`() {
        val user = randomUser()
        val out = BytesStreamOutput()
        user.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newUser = User(sin)
        assertEquals("Round tripping User doesn't work", user, newUser)
    }

    fun `test empty user as stream`() {
        val user = emptyUser()
        val out = BytesStreamOutput()
        user.writeTo(out)
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val newUser = User(sin)
        assertEquals("Round tripping User doesn't work", user, newUser)
    }
}