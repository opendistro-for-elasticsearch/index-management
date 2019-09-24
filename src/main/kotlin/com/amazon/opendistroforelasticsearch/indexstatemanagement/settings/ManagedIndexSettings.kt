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

package com.amazon.opendistroforelasticsearch.indexstatemanagement.settings

import com.amazon.opendistroforelasticsearch.indexstatemanagement.model.action.ActionConfig
import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.unit.TimeValue
import java.util.concurrent.TimeUnit
import java.util.function.Function

class ManagedIndexSettings {
    companion object {
        const val DEFAULT_ISM_ENABLED = true
        const val DEFAULT_JOB_INTERVAL = 5

        val INDEX_STATE_MANAGEMENT_ENABLED = Setting.boolSetting(
            "opendistro.index_state_management.enabled",
            DEFAULT_ISM_ENABLED,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val POLICY_ID = Setting.simpleString(
            "index.opendistro.index_state_management.policy_id",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val ROLLOVER_ALIAS = Setting.simpleString(
            "index.opendistro.index_state_management.rollover_alias",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val JOB_INTERVAL = Setting.intSetting(
            "opendistro.index_state_management.job_interval",
            DEFAULT_JOB_INTERVAL,
            1,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val SWEEP_PERIOD = Setting.timeSetting(
            "opendistro.index_state_management.coordinator.sweep_period",
            TimeValue.timeValueMinutes(10),
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val COORDINATOR_BACKOFF_MILLIS = Setting.positiveTimeSetting(
            "opendistro.index_state_management.coordinator.backoff_millis",
            TimeValue.timeValueMillis(50),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val COORDINATOR_BACKOFF_COUNT = Setting.intSetting(
            "opendistro.index_state_management.coordinator.backoff_count",
            2,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_ENABLED = Setting.boolSetting(
            "opendistro.index_state_management.history.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_MAX_DOCS = Setting.longSetting(
            "opendistro.index_state_management.history.max_docs",
            2500000L, // 1 doc is ~10kb or less. This many doc is roughly 25gb
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_INDEX_MAX_AGE = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.max_age",
            TimeValue.timeValueHours(24),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_ROLLOVER_CHECK_PERIOD = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.rollover_check_period",
            TimeValue.timeValueHours(8),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val HISTORY_RETENTION_PERIOD = Setting.positiveTimeSetting(
            "opendistro.index_state_management.history.rollover_retention_period",
            TimeValue(30, TimeUnit.DAYS),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val ALLOW_LIST = Setting.listSetting(
            "opendistro.index_state_management.allow_list",
            ActionConfig.ActionType.values().toList().map { it.type },
            Function.identity(),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
