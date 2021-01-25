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
   * application sequence number for the record
   *
   * @return
   */
  long asqn();

  /**
   * checksum of the data
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
