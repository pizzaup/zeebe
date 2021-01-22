package io.zeebe.journal.file;

public class StorageException extends RuntimeException {

  public StorageException() {}

  public StorageException(final String message) {
    super(message);
  }

  public StorageException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public StorageException(final Throwable cause) {
    super(cause);
  }

  /** Exception thrown when an entry being stored is too large. */
  public static class TooLarge extends StorageException {
    public TooLarge(final String message) {
      super(message);
    }
  }

  /** Exception thrown when storage runs out of disk space. */
  public static class OutOfDiskSpace extends StorageException {
    public OutOfDiskSpace(final String message) {
      super(message);
    }
  }

  /** Exception thrown when an entry has an invalid checksum. */
  public static class InvalidChecksum extends StorageException {
    public InvalidChecksum(final String message) {
      super(message);
    }
  }
}
