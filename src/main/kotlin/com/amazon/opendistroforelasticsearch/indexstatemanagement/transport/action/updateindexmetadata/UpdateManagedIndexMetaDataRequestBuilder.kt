package com.amazon.opendistroforelasticsearch.indexstatemanagement.transport.action.updateindexmetadata

import org.elasticsearch.action.support.master.AcknowledgedRequestBuilder
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.ElasticsearchClient

class UpdateManagedIndexMetaDataRequestBuilder(
    client: ElasticsearchClient,
    action: UpdateManagedIndexMetaDataAction
) :
    AcknowledgedRequestBuilder<
        UpdateManagedIndexMetaDataRequest,
        AcknowledgedResponse,
        UpdateManagedIndexMetaDataRequestBuilder>
    (client, action, UpdateManagedIndexMetaDataRequest())
