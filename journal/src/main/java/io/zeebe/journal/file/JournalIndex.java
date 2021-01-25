package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;

public interface JournalIndex {

  /**
   * Indexes the record and its position with in a segment
   *
   * @param record the record that should be indexed
   * @param position the position of the given index
   */
  void index(JournalRecord record, int position);

  /**
   * Looks up the position of the given index.
   *
   * @param index the index to lookup
   * @return the position of the given index or a lesser index
   */
  IndexInfo lookup(long index);

  /**
   * Looka up the index for the given application sequence number.
   *
   * @param asqn
   * @return the index of a record with asqn less than or equal to the given asqn.
   */
  Long lookupAsqn(long asqn);

  /**
   * Delete all entries after the given index.
   *
   * @param indexExclusive the index after which to be deleted
   */
  void deleteAfter(long indexExclusive);

  /**
   * Compacts the index until the next stored index (exclusively), which means everything lower then
   * the stored index will be removed.
   *
   * <p>Example Index: {5 -> 10; 10 -> 20; 15 -> 30}, when compact is called with index 11. The next
   * lower stored index is 10, everything lower then this index will be removed. This means the
   * mapping {5 -> 10}, should be removed.
   *
   * @param indexExclusive the index to which to compact the index
   */
  void deleteUntil(long indexExclusive);
}