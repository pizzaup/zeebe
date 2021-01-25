package io.zeebe.journal.file;

import io.zeebe.journal.Journal;
import io.zeebe.journal.JournalRecord;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

public class SparseJournalIndex implements JournalIndex {

  private final int density;
  private final TreeMap<Long, Integer> indexToPosition = new TreeMap<>();
  private final TreeMap<Long, Long> asqnToIndex = new TreeMap<>();

  public SparseJournalIndex(final int density) {
    this.density = density;
  }

  @Override
  public void index(final JournalRecord indexedEntry, final int position) {
    final long index = indexedEntry.index();
    if (index % density == 0) {
      indexToPosition.put(index, position);
      final long asqn = indexedEntry.asqn();
      if (asqn != Journal.ASQN_IGNORE) {
        asqnToIndex.put(asqn, index);
      }
    }
  }

  @Override
  public IndexInfo lookup(final long index) {
    final Map.Entry<Long, Integer> entry = indexToPosition.floorEntry(index);
    return entry != null ? new IndexInfo(entry.getKey(), entry.getValue()) : null;
  }

  @Override
  public Long lookupAsqn(final long asqn) {
    final Map.Entry<Long, Long> entry = asqnToIndex.floorEntry(asqn);
    return entry != null ? entry.getValue() : null;
  }

  @Override
  public void deleteAfter(final long index) {
    indexToPosition.tailMap(index, false).clear();
    // TODO: truncate asqnToIndex
  }

  @Override
  public void deleteUntil(final long index) {
    final Entry<Long, Integer> floorEntry = indexToPosition.floorEntry(index);

    if (floorEntry != null) {
      indexToPosition.headMap(floorEntry.getKey(), false).clear();
    }
  }

  // TODO: compact asqnToIndex
}
