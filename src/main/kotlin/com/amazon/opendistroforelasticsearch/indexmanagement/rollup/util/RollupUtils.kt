/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

@file:Suppress("TooManyFunctions")

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.util

import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.Rollup

fun Rollup.getInitialDocValues(docCount: Long): MutableMap<String, Any?> =
    mutableMapOf(
        Rollup.ROLLUP_DOC_ID_FIELD to this.id,
        Rollup.ROLLUP_DOC_COUNT_FIELD to docCount,
        Rollup.ROLLUP_DOC_SCHEMA_VERSION_FIELD to this.schemaVersion
    )
