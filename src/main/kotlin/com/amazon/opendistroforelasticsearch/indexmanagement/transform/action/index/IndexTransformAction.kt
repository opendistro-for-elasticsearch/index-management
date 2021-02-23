package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.index

import org.elasticsearch.action.ActionType

class IndexTransformAction private constructor() : ActionType<IndexTransformResponse>(NAME, ::IndexTransformResponse) {
    companion object {
        val INSTANCE = IndexTransformAction()
        val NAME = "cluster:admin/opendistro/transform/index"
    }
}
