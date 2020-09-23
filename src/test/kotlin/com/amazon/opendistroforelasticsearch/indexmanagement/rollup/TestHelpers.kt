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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup

import com.amazon.opendistroforelasticsearch.indexmanagement.elasticapi.string
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Average
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Max
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Min
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.Sum
import com.amazon.opendistroforelasticsearch.indexmanagement.rollup.model.metric.ValueCount
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.XContentFactory

fun randomAverage(): Average = Average()

fun randomMax(): Max = Max()

fun randomMin(): Min = Min()

fun randomSum(): Sum = Sum()

fun randomValueCount(): ValueCount = ValueCount()

fun Average.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Max.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Min.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun Sum.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()

fun ValueCount.toJsonString(): String = this.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string()
