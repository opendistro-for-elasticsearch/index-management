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

@file:Suppress("TopLevelPropertyNaming")

package com.amazon.opendistroforelasticsearch.indexstatemanagement.util

import org.elasticsearch.common.xcontent.ToXContent

const val _DOC = "_doc"
const val _ID = "_id"
const val _VERSION = "_version"
const val _SEQ_NO = "_seq_no"
const val IF_SEQ_NO = "if_seq_no"
const val _PRIMARY_TERM = "_primary_term"
const val IF_PRIMARY_TERM = "if_primary_term"
const val REFRESH = "refresh"

const val WITH_TYPE = "with_type"
val XCONTENT_WITHOUT_TYPE = ToXContent.MapParams(mapOf(WITH_TYPE to "false"))
