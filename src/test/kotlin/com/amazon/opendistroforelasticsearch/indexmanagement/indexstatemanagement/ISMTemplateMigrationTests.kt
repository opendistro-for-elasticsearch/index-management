package com.amazon.opendistroforelasticsearch.indexmanagement.indexstatemanagement

import com.amazon.opendistroforelasticsearch.indexmanagement.IndexManagementIndices
import org.elasticsearch.client.Client
import org.elasticsearch.cluster.service.ClusterService
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.test.ESTestCase
import org.junit.Before
import org.mockito.Mockito

class ISMTemplateMigrationTests : ESTestCase() {

    private lateinit var client: Client
    private lateinit var clusterService: ClusterService
    private lateinit var xContentRegistry: NamedXContentRegistry
    private lateinit var imIndices: IndexManagementIndices

    @Before
    fun setup() {
        client = Mockito.mock(Client::class.java)
        xContentRegistry = Mockito.mock(NamedXContentRegistry::class.java)
        imIndices = Mockito.mock(IndexManagementIndices::class.java)
    }
}