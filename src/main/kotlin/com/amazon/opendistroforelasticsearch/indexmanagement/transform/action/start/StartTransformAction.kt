package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.start

import org.elasticsearch.action.ActionType
import org.elasticsearch.action.support.master.AcknowledgedResponse

class StartTransformAction private constructor() : ActionType<AcknowledgedResponse>(NAME, ::AcknowledgedResponse) {
    companion object {
        val INSTANCE = StartTransformAction()
        val NAME = "cluster:admin/opendistro/transform/start"
    }
}
