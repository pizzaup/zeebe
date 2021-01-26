package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;
import org.agrona.DirectBuffer;

/**
 * Journal Record
 */
public class PersistedJournalRecord implements JournalRecord {

  private final DirectBuffer data;
  private final long index;
  private final long asqn;
  private final int checksum;

  public PersistedJournalRecord(
      final long index, final long asqn, final int checksum, final DirectBuffer data) {
    this.index = index;
    this.asqn = asqn;
    this.checksum = checksum;
    this.data = data;
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
}
