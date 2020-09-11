package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.removepolicy

import org.elasticsearch.action.ActionType

class RemovePolicyAction private constructor() : ActionType<RemovePolicyResponse>(NAME, ::RemovePolicyResponse) {
    companion object {
        val INSTANCE = RemovePolicyAction()
        val NAME = "cluster:admin/ism/policies/delete"
    }
}
