package io.zeebe.journal.file;

import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import java.util.NoSuchElementException;

public class SegmentedJournalReader implements JournalReader {

  private final SegmentedJournal journal;
  private JournalSegment currentSegment;
  private JournalRecord previousEntry;
  private MappedJournalSegmentReader currentReader;

  SegmentedJournalReader(final SegmentedJournal journal) {
    this.journal = journal;
    initialize();
  }

  /** Initializes the reader to the given index. */
  private void initialize() {
    currentSegment = journal.getFirstSegment();
    currentReader = currentSegment.createReader();
    final long nextIndex = journal.getFirstIndex();
  }

  public long getCurrentIndex() {
    final long currentIndex = currentReader.getCurrentIndex();
    if (currentIndex != 0) {
      return currentIndex;
    }
    if (previousEntry != null) {
      return previousEntry.index();
    }
    return 0;
  }

  public JournalRecord getCurrentEntry() {
    final var currentEntry = currentReader.getCurrentEntry();
    if (currentEntry != null) {
      return currentEntry;
    }
    return previousEntry;
  }

  public long getNextIndex() {
    return currentReader.getNextIndex();
  }

  @Override
  public boolean hasNext() {
    return hasNextEntry();
  }

  @Override
  public JournalRecord next() {
    if (!currentReader.hasNext()) {
      final JournalSegment nextSegment = journal.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        replaceCurrentSegment(nextSegment);
        return currentReader.next();
      } else {
        throw new NoSuchElementException();
      }
    } else {
      previousEntry = currentReader.getCurrentEntry();
      return currentReader.next();
    }
  }

  @Override
  public boolean seek(final long index) {
    // If the current segment is not open, it has been replaced. Reset the segments.
    if (!currentSegment.isOpen()) {
      seekToFirst();
    }

    if (index < currentReader.getNextIndex()) {
      rewind(index);
    } else if (index > currentReader.getNextIndex()) {
      forward(index);
    } else {
      currentReader.seek(index);
    }
    return getNextIndex() == index;
  }

  @Override
  public void seekToFirst() {
    replaceCurrentSegment(journal.getFirstSegment());
    previousEntry = null;
  }

  @Override
  public void seekToLast() {
    replaceCurrentSegment(journal.getLastSegment());
    seek(journal.getLastIndex());
  }

  @Override
  public boolean seekToApplicationSqNum(final long applicationSqNum) {
    // TODO:
    return false;
  }

  @Override
  public void close() {
    currentReader.close();
    journal.closeReader(this);
  }

  /** Rewinds the journal to the given index. */
  private void rewind(final long index) {
    if (currentSegment.index() >= index) {
      final JournalSegment segment = journal.getSegment(index - 1);
      if (segment != null) {
        replaceCurrentSegment(segment);
      }
    }

    currentReader.seek(index);
    previousEntry = currentReader.getCurrentEntry();
  }

  /** Fast forwards the journal to the given index. */
  private void forward(final long index) {
    // skip to the correct segment if there is one
    if (!currentSegment.equals(journal.getLastSegment())) {
      final JournalSegment segment = journal.getSegment(index);
      if (segment != null && !segment.equals(currentSegment)) {
        replaceCurrentSegment(segment);
      }
    }

    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  private boolean hasNextEntry() {
    if (!currentReader.hasNext()) {
      final JournalSegment nextSegment = journal.getNextSegment(currentSegment.index());
      if (nextSegment != null && nextSegment.index() == getNextIndex()) {
        previousEntry = currentReader.getCurrentEntry();
        replaceCurrentSegment(nextSegment);
        return currentReader.hasNext();
      }
      return false;
    }
    return true;
  }

  private void replaceCurrentSegment(final JournalSegment nextSegment) {
    currentReader.close();
    currentSegment = nextSegment;
    currentReader = currentSegment.createReader();
  }
}
