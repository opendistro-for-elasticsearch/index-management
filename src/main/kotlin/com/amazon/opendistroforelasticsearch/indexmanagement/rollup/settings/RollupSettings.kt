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

package com.amazon.opendistroforelasticsearch.indexmanagement.rollup.settings

import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.unit.TimeValue

class RollupSettings {

    companion object {
        const val DEFAULT_ROLLUP_ENABLED = true
        const val DEFAULT_ACQUIRE_LOCK_RETRY_COUNT = 3
        const val DEFAULT_ACQUIRE_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_RENEW_LOCK_RETRY_COUNT = 3
        const val DEFAULT_RENEW_LOCK_RETRY_DELAY = 1000L
        const val DEFAULT_CLIENT_REQUEST_RETRY_COUNT = 3
        const val DEFAULT_CLIENT_REQUEST_RETRY_DELAY = 1000L

        val ROLLUP_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.enabled",
            DEFAULT_ROLLUP_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_ENABLED: Setting<Boolean> = Setting.boolSetting(
            "opendistro.rollup.search.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_INDEX: Setting<Boolean> = Setting.boolSetting(
            "index.opendistro.rollup_index",
            false,
            Setting.Property.IndexScope,
            Setting.Property.InternalIndex
        )

        val ROLLUP_INGEST_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.rollup.ingest.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_INGEST_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.rollup.ingest.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_BACKOFF_MILLIS: Setting<TimeValue> = Setting.positiveTimeSetting(
            "opendistro.rollup.search.backoff_millis",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ROLLUP_SEARCH_BACKOFF_COUNT: Setting<Int> = Setting.intSetting(
            "opendistro.rollup.search.backoff_count",
            5,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
