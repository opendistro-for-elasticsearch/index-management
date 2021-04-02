package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.explain

import org.elasticsearch.action.ActionType

class ExplainTransformAction private constructor() : ActionType<ExplainTransformResponse>(NAME, ::ExplainTransformResponse) {
    companion object {
        val INSTANCE = ExplainTransformAction()
        val NAME = "cluster:admin/opendistro/transform/explain"
    }
}
