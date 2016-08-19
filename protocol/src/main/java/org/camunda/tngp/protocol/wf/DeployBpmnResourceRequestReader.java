package org.camunda.tngp.protocol.wf;

import org.camunda.tngp.protocol.wf.repository.DeployBpmnResourceDecoder;
import org.camunda.tngp.protocol.wf.repository.MessageHeaderDecoder;
import org.camunda.tngp.util.buffer.BufferReader;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class DeployBpmnResourceRequestReader implements BufferReader
{
    protected final MessageHeaderDecoder sbeHeaderDecoder = new MessageHeaderDecoder();

    protected final DeployBpmnResourceDecoder sbeDecoder = new DeployBpmnResourceDecoder();

    protected final UnsafeBuffer resourceBuffer = new UnsafeBuffer(0, 0);

    @Override
    public void wrap(DirectBuffer buffer, int offset, int length)
    {
        sbeHeaderDecoder.wrap(buffer, offset);

        offset += sbeHeaderDecoder.encodedLength();

        sbeDecoder.wrap(buffer, offset, sbeHeaderDecoder.blockLength(), sbeHeaderDecoder.version());

        offset += sbeHeaderDecoder.blockLength();
        offset += DeployBpmnResourceDecoder.resourceHeaderLength();

        resourceBuffer.wrap(buffer, offset, sbeDecoder.resourceLength());
    }

    public DirectBuffer getResource()
    {
        return resourceBuffer;
    }

}
