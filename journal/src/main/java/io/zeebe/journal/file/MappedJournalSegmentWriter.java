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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.zip.CRC32;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;

/**
 * Segment writer.
 *
 * <p>The format of an entry in the log is as follows: TODO
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

  public JournalRecord append(final DirectBuffer data) {
    // Store the entry index.
    final long index = getNextIndex();

    // Serialize the entry.
    final int position = buffer.position();
    if (position + Integer.BYTES + Integer.BYTES > buffer.limit()) {
      throw new BufferOverflowException();
    }

    buffer.position(position + Integer.BYTES + Integer.BYTES);

    final int length = data.capacity();
    // If the entry length exceeds the maximum entry size then throw an exception.
    if (length > maxEntrySize) {
      // Just reset the buffer. There's no need to zero the bytes since we haven't written the
      // length or checksum.
      buffer.position(position);
      throw new StorageException.TooLarge(
          "Entry size " + length + " exceeds maximum allowed bytes (" + maxEntrySize + ")");
    }

    buffer.putInt(data.capacity());
    data.getBytes(0, buffer, data.capacity());

    // Compute the checksum for the entry.
    buffer.position(position + Integer.BYTES + Integer.BYTES);
    final long checksum = computeChecksum(length);

    // Create a single byte[] in memory for the entire entry and write it as a batch to the
    // underlying buffer.
    buffer.position(position);
    buffer.putInt(length);
    buffer.putInt((int) checksum);
    buffer.position(position + Integer.BYTES + Integer.BYTES + length);

    // Update the last entry with the correct index/term/length.
    final JournalRecord indexedEntry = new JournalRecordImpl(buffer, position);
    lastEntry = indexedEntry;
    this.index.index(lastEntry, position);
    return indexedEntry;
  }

  private long computeChecksum(final int length) {
    final ByteBuffer slice = buffer.slice();
    slice.limit(length);
    crc32.reset();
    crc32.update(slice);
    return crc32.getValue();
  }

  public void append(final JournalRecord record) {
    final long nextIndex = getNextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (record.index() != nextIndex) {
      throw new IndexOutOfBoundsException("Entry index is not sequential");
    }

    // TODO:
    // append(record)
  }

  private void reset(final long index) {
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
    this.index.truncate(index);

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
