package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Journal Record Format: 4 Bytes - checksum 8 Bytes - index 8 Bytes - asqn 4 Bytes - length length
 * Bytes - data
 */
public class JournalRecordImpl implements JournalRecord {

  private static final int METADATALENGTH = Long.BYTES + Integer.BYTES + Long.BYTES + Integer.BYTES;
  private final DirectBuffer data;
  private final long index;
  private final long asqn;
  private final int length;
  private int checksum;

  public JournalRecordImpl(
      final long index, final long asqn, final int checksum, final DirectBuffer data) {
    this.index = index;
    this.asqn = asqn;
    this.checksum = checksum;
    this.data = data;
    length = data.capacity();
  }

  public JournalRecordImpl(final ByteBuffer buffer, final int position) {
    buffer.position(position);
    checksum = buffer.getInt();
    index = buffer.getLong();
    asqn = buffer.getLong();
    length = buffer.getInt();
    data = new UnsafeBuffer();
    data.wrap(buffer, position + METADATALENGTH, length);
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long asqn() {
    return asqn;
  }

  @Override
  public int checksum() {
    return checksum;
  }

  @Override
  public DirectBuffer data() {
    return data;
  }

  public void writeTo(final ByteBuffer buffer) {
    final int position = buffer.position();

    if (position + METADATALENGTH + length > buffer.limit()) {
      throw new BufferOverflowException();
    }
    buffer.position(position + METADATALENGTH + length);

    // Write data to buffer
    data.getBytes(0, buffer, length);

    // Create a single byte[] in memory for the entire entry and write it as a batch to the
    // underlying buffer.
    buffer.position(position);
    // Write headers
    // buffer.putInt(checksum);
    buffer.position(buffer.position() + Integer.BYTES);
    buffer.putLong(index);
    buffer.putLong(asqn);
    buffer.putInt(length);
    buffer.position(position + METADATALENGTH + length);
  }

  public void writeChecksum(
      final ByteBuffer buffer, final BiFunction<ByteBuffer, Integer, Integer> checksumCalculator) {
    final int position = buffer.position();
    buffer.position(position + METADATALENGTH);
    checksum = checksumCalculator.apply(buffer, length);
    buffer.position(position);
    buffer.putInt(checksum);
    buffer.position(position);
  }

  public int size() {
    return METADATALENGTH + length;
  }
}
