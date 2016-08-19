package org.camunda.tngp.broker.util.mocks;


import org.camunda.tngp.log.LogWriter;
import org.camunda.tngp.util.buffer.BufferReader;
import org.camunda.tngp.util.buffer.BufferWriter;

// TODO: make LogWriter an interface
public class StubLogWriter extends LogWriter
{

    protected BufferWriterResultCollector collector = new BufferWriterResultCollector();
    protected long tailPosition = 0L;

    public StubLogWriter()
    {
        super(null);
    }

    @Override
    public long write(BufferWriter writer)
    {
        final int length = writer.getLength();
        collector.add(writer);

        final long result = tailPosition;
        tailPosition += length;
        return result;
    }

    public int size()
    {
        return collector.size();
    }

    public <T extends BufferReader> T getEntryAs(int index, Class<T> bufferReaderClass)
    {
        return collector.getEntryAs(index, bufferReaderClass);
    }
}
