package io.zeebe.journal;

import java.util.Iterator;

public interface JournalReader extends Iterator<JournalRecord> {

  /**
   * Seek to a record at the given index. if seek(index) return true, {@link JournalReader#next()}
   * should return a record at index.
   *
   * <p>if the index is less than {@link Journal#getFirstIndex()}, {@link JournalReader#next()}
   * should return a record at index {@link Journal#getFirstIndex()}. If the index is greater than
   * {@link Journal#getLastIndex()}, then it seeks to lastIndex + 1.
   *
   * @param index the index to seek to.
   * @return true if a record at the index exists, false otherwise.
   */
  boolean seek(long index);

  /**
   * Seek to the first index of the journal. Equivalent to calling seek(journal.getFirstIndex()).
   */
  void seekToFirst();

  /** Seek to the last index of the journal. Equivalent to calling seek(journal.getLastIndex()). */
  void seekToLast();

  /**
   * Seek to a record with the highest sequence number less than or equal to the given
   * applicationSqNum.
   *
   * @param applicationSqNum
   * @return true if such a record exists, false if there are no records with sequence number less
   *     than or equal to the given applicationSqNum.
   */
  boolean seekToApplicationSqNum(long applicationSqNum);

  void close();
}
