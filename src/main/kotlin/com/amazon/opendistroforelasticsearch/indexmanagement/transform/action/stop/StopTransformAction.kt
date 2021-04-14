package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.stop

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class StopTransformAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StopTransformAction()
        val NAME = "cluster:admin/opendistro/transform/stop"
    }
}
