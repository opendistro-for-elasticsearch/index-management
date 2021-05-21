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

package com.amazon.opendistroforelasticsearch.indexmanagement.transform.settings

import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.unit.TimeValue

class TransformSettings {

    companion object {
        const val DEFAULT_RENEW_LOCK_RETRY_COUNT = 3
        const val DEFAULT_RENEW_LOCK_RETRY_DELAY = 1000L

        val TRANSFORM_JOB_SEARCH_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.transform.internal.search.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_SEARCH_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.transform.internal.search.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_INDEX_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.transform.internal.index.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_JOB_INDEX_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.transform.internal.index.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_CIRCUIT_BREAKER_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.transform.circuit_breaker.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val TRANSFORM_CIRCUIT_BREAKER_JVM_THRESHOLD: Setting<Int> = Setting.intSetting(
            "opendistro.transform.circuit_breaker.jvm.threshold",
            85,
            0,
            100,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
