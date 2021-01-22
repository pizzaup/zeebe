package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;
import java.nio.ByteBuffer;
import org.agrona.DirectBuffer;

public class JournalRecordImpl implements JournalRecord {

  public JournalRecordImpl(final ByteBuffer buffer, final int position) {}

  @Override
  public long index() {
    return 0;
  }

  @Override
  public int checksum() {
    return 0;
  }

  @Override
  public DirectBuffer data() {
    return null;
  }
}
