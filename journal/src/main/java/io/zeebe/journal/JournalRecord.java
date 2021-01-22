package io.zeebe.journal;

import org.agrona.DirectBuffer;

public interface JournalRecord {

  /**
   * index of the record
   *
   * @return index
   */
  long index();

  /**
   * Checksum of the data
   *
   * @return
   */
  int checksum();

  /**
   * Data in the record
   *
   * @return data
   */
  DirectBuffer data();
}
