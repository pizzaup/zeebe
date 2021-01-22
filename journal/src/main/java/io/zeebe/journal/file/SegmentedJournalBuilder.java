package io.zeebe.journal.file;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;

/**
 * Raft log builder.
 */
public class SegmentedJournalBuilder {

  private static final String DEFAULT_NAME = "atomix";
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = 1024 * 1024;
  private static final long DEFAULT_MIN_FREE_DISK_SPACE = 1024L * 1024 * 1024 * 1;
  protected String name = DEFAULT_NAME;
  protected File directory = new File(DEFAULT_DIRECTORY);
  protected int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  protected int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  protected int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;

  private long freeDiskSpace = DEFAULT_MIN_FREE_DISK_SPACE;

  protected SegmentedJournalBuilder() {
  }

  /**
   * Sets the storage name.
   *
   * @param name The storage name.
   * @return The storage builder.
   */
  public SegmentedJournalBuilder withName(final String name) {
    this.name = checkNotNull(name, "name cannot be null");
    return this;
  }

  /**
   * Sets the log directory, returning the builder for method chaining.
   *
   * <p>The log will write segment files into the provided directory.
   *
   * @param directory The log directory.
   * @return The storage builder.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public SegmentedJournalBuilder withDirectory(final String directory) {
    return withDirectory(new File(checkNotNull(directory, "directory cannot be null")));
  }

  /**
   * Sets the log directory, returning the builder for method chaining.
   *
   * <p>The log will write segment files into the provided directory.
   *
   * @param directory The log directory.
   * @return The storage builder.
   * @throws NullPointerException If the {@code directory} is {@code null}
   */
  public SegmentedJournalBuilder withDirectory(final File directory) {
    this.directory = checkNotNull(directory, "directory cannot be null");
    return this;
  }

  /**
   * Sets the maximum segment size in bytes, returning the builder for method chaining.
   *
   * <p>The maximum segment size dictates when logs should roll over to new segments. As entries
   * are written to a segment of the log, once the size of the segment surpasses the configured
   * maximum segment size, the log will create a new segment and append new entries to that
   * segment.
   *
   * <p>By default, the maximum segment size is {@code 1024 * 1024 * 32}.
   *
   * @param maxSegmentSize The maximum segment size in bytes.
   * @return The storage builder.
   * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
   */
  public SegmentedJournalBuilder withMaxSegmentSize(final int maxSegmentSize) {
    checkArgument(
        maxSegmentSize > JournalSegmentDescriptor.BYTES,
        "maxSegmentSize must be greater than " + JournalSegmentDescriptor.BYTES);
    this.maxSegmentSize = maxSegmentSize;
    return this;
  }

  /**
   * Sets the maximum entry size in bytes, returning the builder for method chaining.
   *
   * @param maxEntrySize the maximum entry size in bytes
   * @return the storage builder
   * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
   */
  public SegmentedJournalBuilder withMaxEntrySize(final int maxEntrySize) {
    checkArgument(maxEntrySize > 0, "maxEntrySize must be positive");
    this.maxEntrySize = maxEntrySize;
    return this;
  }

  /**
   * Sets the minimum free disk space to leave when allocating a new segment
   *
   * @param freeDiskSpace free disk space in bytes
   * @return the storage builder
   * @throws IllegalArgumentException if the {@code freeDiskSpace} is not positive
   */
  public SegmentedJournalBuilder withFreeDiskSpace(final long freeDiskSpace) {
    checkArgument(freeDiskSpace >= 0, "minFreeDiskSpace must be positive");
    this.freeDiskSpace = freeDiskSpace;
    return this;
  }

  public SegmentedJournal build() {
    return new SegmentedJournal(
        name,
        directory,
        maxSegmentSize,
        maxEntrySize,
        maxEntriesPerSegment,
        freeDiskSpace);
  }
}
