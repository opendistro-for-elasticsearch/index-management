package com.amazon.opendistroforelasticsearch.indexmanagement.transform.action.get

import org.elasticsearch.action.ActionType

class GetTransformAction private constructor() : ActionType<GetTransformResponse>(NAME, ::GetTransformResponse) {
    companion object {
        val INSTANCE = GetTransformAction()
        val NAME = "cluster:admin/opendistro/rollup/get"
    }
}
