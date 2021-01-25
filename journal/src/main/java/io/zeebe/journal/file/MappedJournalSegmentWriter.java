/*
 * Copyright 2017-present Open Networking Foundation
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.journal.file;

import io.zeebe.journal.JournalRecord;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.zip.CRC32;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;

/**
 * Segment writer.
 */
class MappedJournalSegmentWriter {

  private final MappedByteBuffer buffer;
  private final JournalSegment segment;
  private final int maxEntrySize;
  private final JournalIndex index;
  private final long firstIndex;
  private final CRC32 crc32 = new CRC32();
  private JournalRecord lastEntry;
  private boolean isOpen = true;

  MappedJournalSegmentWriter(
      final JournalSegmentFile file,
      final JournalSegment segment,
      final int maxEntrySize,
      final JournalIndex index) {
    this.segment = segment;
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    firstIndex = segment.index();
    buffer = mapFile(file, segment);
    reset(0);
  }

  private static MappedByteBuffer mapFile(
      final JournalSegmentFile file, final JournalSegment segment) {
    // map existing file, because file is already created by SegmentedJournal
    return IoUtil.mapExistingFile(
        file.file(), file.name(), 0, segment.descriptor().maxSegmentSize());
  }

  public long getLastIndex() {
    return lastEntry != null ? lastEntry.index() : segment.index() - 1;
  }

  public JournalRecord getLastEntry() {
    return lastEntry;
  }

  public long getNextIndex() {
    if (lastEntry != null) {
      return lastEntry.index() + 1;
    } else {
      return firstIndex;
    }
  }

  public JournalRecord append(final long asqn, final DirectBuffer data) {
    // Store the entry index.
    final long index = getNextIndex();
    final int length = data.capacity();

    // If the entry length exceeds the maximum entry size then throw an exception.
    if (length > maxEntrySize) {
      throw new StorageException.TooLarge(
          "Entry size " + length + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }

    // Update the last entry with the correct index/term/length.
    final JournalRecordImpl record = new JournalRecordImpl(index, asqn, -1, data);
    final int recordPosition = buffer.position();

    // write record
    record.writeTo(buffer);
    // write checksum
    buffer.position(recordPosition);
    record.writeChecksum(buffer, this::computeChecksum);
    // update position
    buffer.position(recordPosition + record.size());
    lastEntry = new JournalRecordImpl(buffer, recordPosition);
    this.index.index(lastEntry, recordPosition);
    buffer.position(recordPosition);
    return  lastEntry;
  }

  private int computeChecksum(final ByteBuffer buffer, final int length) {
    final ByteBuffer slice = buffer.slice();
    slice.limit(length);
    crc32.reset();
    crc32.update(slice);
    return (int) crc32.getValue();
  }

  public void append(final JournalRecord record) {
    final long nextIndex = getNextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (record.index() != nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // TODO: Validate checksum
    new JournalRecordImpl(record.index(), record.asqn(), record.checksum(), record.data())
        .writeTo(buffer);
  }

  private void reset(final long index) {
    buffer.position(JournalSegmentDescriptor.BYTES);
    // TODO
  }

  public void truncate(final long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= getLastIndex()) {
      return;
    }

    // Reset the last entry.
    lastEntry = null;

    // Truncate the index.
    this.index.deleteAfter(index);

    if (index < segment.index()) {
      buffer.position(JournalSegmentDescriptor.BYTES);
      buffer.putInt(0);
      buffer.putInt(0);
      buffer.position(JournalSegmentDescriptor.BYTES);
    } else {
      // Reset the writer to the given index.
      reset(index);

      // Zero entries after the given index.
      final int position = buffer.position();
      buffer.putInt(0);
      buffer.putInt(0);
      buffer.position(position);
    }
  }

  public void flush() {
    buffer.force();
  }

  public void close() {
    if (isOpen) {
      isOpen = false;
      flush();
      IoUtil.unmap(buffer);
    }
  }

  /**
   * Returns a boolean indicating whether the segment is empty.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return lastEntry == null;
  }
}
