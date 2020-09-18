package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicy

import org.elasticsearch.action.ActionType

class GetPolicyAction private constructor() : ActionType<GetPolicyResponse>(NAME, ::GetPolicyResponse) {
    companion object {
        val INSTANCE = GetPolicyAction()
        val NAME = "cluster:admin/opendistro/ism/policies/read"
    }
}