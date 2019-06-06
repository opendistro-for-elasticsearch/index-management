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

import org.elasticsearch.common.settings.Setting
import org.elasticsearch.common.unit.TimeValue

class ManagedIndexSettings {
    companion object {
        val INDEX_STATE_MANAGEMENT_ENABLED = Setting.boolSetting(
            "opendistro.index_state_management.enabled",
            true,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )

        val POLICY_NAME = Setting.simpleString(
            "index.opendistro.index_state_management.policy_name",
            Setting.Property.IndexScope,
            Setting.Property.Dynamic
        )

        val SWEEP_PERIOD = Setting.positiveTimeSetting(
            "opendistro.index_state_management.sweeper.period",
            TimeValue.timeValueMinutes(1),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        )
    }
}
