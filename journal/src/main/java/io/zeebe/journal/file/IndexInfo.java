package io.zeebe.journal.file;

public class IndexInfo {
  private final long index;
  private final int position;

  public IndexInfo(final long index, final int position) {
    this.index = index;
    this.position = position;
  }

  public long index() {
    return index;
  }

  public int position() {
    return position;
  }
}
