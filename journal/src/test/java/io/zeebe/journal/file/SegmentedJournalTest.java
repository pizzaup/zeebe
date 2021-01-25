package io.zeebe.journal.file;

import static org.assertj.core.api.Assertions.assertThat;

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
    final var recordAppended = journal.append(1, data);
    assertThat(recordAppended.index()).isEqualTo(1);
    journal.flush();

    final var recordRead = journal.openReader().next();
    assertThat(recordAppended.index()).isEqualTo(recordRead.index());
    assertThat(recordAppended.asqn()).isEqualTo(recordRead.asqn());
    assertThat(recordAppended.checksum()).isEqualTo(recordRead.checksum());
  }
}
