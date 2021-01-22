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

import io.zeebe.journal.JournalReader;
import io.zeebe.journal.JournalRecord;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;
import org.agrona.IoUtil;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class MappedJournalSegmentReader implements JournalReader {
  private final MappedByteBuffer buffer;
  private final int maxEntrySize;
  private final JournalIndex index;
  private final JournalSegment segment;
  private JournalRecord currentEntry;
  private JournalRecord nextEntry;

  MappedJournalSegmentReader(
      final JournalSegmentFile file,
      final JournalSegment segment,
      final int maxEntrySize,
      final JournalIndex index) {
    this.maxEntrySize = maxEntrySize;
    this.index = index;
    this.segment = segment;
    buffer =
        IoUtil.mapExistingFile(
            file.file(), MapMode.READ_ONLY, file.name(), 0, segment.descriptor().maxSegmentSize());
    reset();
  }

  @Override
  public boolean hasNext() {
    // If the next entry is null, check whether a next entry exists.
    if (nextEntry == null) {
      readNext();
    }
    return nextEntry != null;
  }

  @Override
  public JournalRecord next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Set the current entry to the next entry.
    currentEntry = nextEntry;

    // Reset the next entry to null.
    nextEntry = null;

    // Read the next entry in the segment.
    readNext();

    // Return the current entry.
    return currentEntry;
  }

  public void reset() {
    buffer.position(JournalSegmentDescriptor.BYTES);
    currentEntry = null;
    nextEntry = null;
    readNext();
  }

  @Override
  public boolean seek(final long index) {
    final long firstIndex = segment.index();
    final long lastIndex = segment.lastIndex();

    reset();

    final int position = this.index.lookup(index - 1);
    if (position != null && position.index() >= firstIndex && position.index() <= lastIndex) {
      currentEntry = null;
      buffer.position(position.position());

      nextEntry = null;
      readNext();
    }

    while (getNextIndex() < index && hasNext()) {
      next();
    }
  }

  @Override
  public void seekToFirst() {}

  @Override
  public void seekToLast() {}

  @Override
  public boolean seekToApplicationSqNum(final long applicationSqNum) {
    return false;
  }

  private long getNextIndex() {
    return currentEntry == null ? segment.index() : currentEntry.index() + 1;
  }

  public void close() {
    IoUtil.unmap(buffer);
    segment.onReaderClosed(this);
  }

  /** Reads the next entry in the segment. */
  private void readNext() {
    // Compute the index of the next entry in the segment.
    final long index = getNextIndex();

    // Mark the buffer so it can be reset if necessary.
    buffer.mark();

    try {
      // Read the length of the entry.
      final int length = buffer.getInt();

      // If the buffer length is zero then return.
      if (length <= 0 || length > maxEntrySize) {
        buffer.reset();
        nextEntry = null;
        return;
      }

      // Read the checksum of the entry.
      final long checksum = buffer.getInt() & 0xFFFFFFFFL;

      // Compute the checksum for the entry bytes.
      final CRC32 crc32 = new CRC32();
      final ByteBuffer slice = buffer.slice();
      slice.limit(length);
      crc32.update(slice);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        slice.rewind();
        nextEntry = new JournalRecordImpl(buffer, buffer.position());
        buffer.position(buffer.position() + length);
      } else {
        buffer.reset();
        nextEntry = null;
      }
    } catch (final BufferUnderflowException e) {
      buffer.reset();
      nextEntry = null;
    }
  }
}
