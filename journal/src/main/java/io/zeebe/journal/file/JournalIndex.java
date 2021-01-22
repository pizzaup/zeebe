package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;

public interface JournalIndex {

  /**
   * Adds an entry for the given index at the given position.
   *
   * @param indexed the indexed entry for which to add the entry
   * @param position the position of the given index
   */
  void index(JournalRecord indexed, int position);

  /**
   * Looks up the position of the given index.
   *
   * @param index the index to lookup
   * @return the position of the given index or a lesser index
   */
  int lookup(long index);

  /**
   * Truncates the index to the given index, which means everything higher will be removed from the
   * index
   *
   * @param index the index to which to truncate the index
   */
  void truncate(long index);

  /**
   * Compacts the index until the next stored index (exclusively), which means everything lower then
   * the stored index will be removed.
   *
   * <p>Example Index: {5 -> 10; 10 -> 20; 15 -> 30}, when compact is called with index 11. The next
   * lower stored index is 10, everything lower then this index will be removed. This means the
   * mapping {5 -> 10}, should be removed.
   *
   * @param index the index to which to compact the index
   */
  void compact(long index);
}