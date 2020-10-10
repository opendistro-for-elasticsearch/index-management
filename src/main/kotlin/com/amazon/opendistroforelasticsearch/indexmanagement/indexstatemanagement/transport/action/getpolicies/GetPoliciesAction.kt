package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement.transport.action.getpolicies

import org.elasticsearch.action.ActionType

class GetPoliciesAction private constructor() : ActionType<GetPoliciesResponse>(NAME, ::GetPoliciesResponse) {
    companion object {
        val INSTANCE = GetPoliciesAction()
        val NAME = "cluster:admin/opendistro/ism/managedindex/getpolicies"
    }
}
