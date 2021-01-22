package io.zeebe.journal.file;

import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;

public class SegmentedJournalReader implements JournalReader {

  @Override
  public boolean seek(final long index) {
    return false;
  }

  @Override
  public void seekToFirst() {

  }

  @Override
  public void seekToLast() {

  }

  @Override
  public boolean seekToApplicationSqNum(final long applicationSqNum) {
    return false;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public JournalRecord next() {
    return null;
  }
}
