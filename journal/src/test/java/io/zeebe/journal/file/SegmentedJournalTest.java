package io.zeebe.journal.file;

import java.nio.file.Path;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SegmentedJournalTest {

  @TempDir Path directory;

  private SegmentedJournal journal;
  private final byte[] ENTRY = new byte[32];

  @BeforeEach
  public void setup() {
    journal = SegmentedJournal.builder().withDirectory(directory.resolve("data").toFile()).build();
  }

  @Test
  public void shouldAppendData() {
    final DirectBuffer data = new UnsafeBuffer();
    data.wrap(ENTRY);
    final var record  = journal.append(1, data);
  }
}
