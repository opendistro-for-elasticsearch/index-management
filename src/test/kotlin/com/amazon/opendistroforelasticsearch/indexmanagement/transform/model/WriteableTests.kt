package com.amazon.opendistroforelasticsearch.indexmanagement.transform.model

import com.amazon.opendistroforelasticsearch.indexmanagement.transform.randomTransformMetadata
import org.elasticsearch.common.io.stream.BytesStreamOutput
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.test.ESTestCase

class WriteableTests : ESTestCase() {

    fun `test transform metadata as stream`() {
        val transformMetadata = randomTransformMetadata()
        val out = BytesStreamOutput().also { transformMetadata.writeTo(it) }
        val sin = StreamInput.wrap(out.bytes().toBytesRef().bytes)
        val streamedTransformMetadata = TransformMetadata(sin)
        assertEquals("Round tripping TransformMetadata stream doesn't work", transformMetadata, streamedTransformMetadata)
    }
}
