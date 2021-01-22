package io.zeebe.journal.file;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class JournalMetrics {
  private static final String NAMESPACE = "atomix";
  private static final String PARTITION_LABEL = "partition";
  private static final Histogram SEGMENT_CREATION_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_creation_time")
          .help("Time spend to create a new segment")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Histogram SEGMENT_TRUNCATE_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_truncate_time")
          .help("Time spend to truncate a segment")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Histogram SEGMENT_FLUSH_TIME =
      Histogram.build()
          .namespace(NAMESPACE)
          .name("segment_flush_time")
          .help("Time spend to flush segment to disk")
          .labelNames(PARTITION_LABEL)
          .register();

  private static final Gauge SEGMENT_COUNT =
      Gauge.build()
          .namespace(NAMESPACE)
          .name("segment_count")
          .help("Number of segments")
          .labelNames(PARTITION_LABEL)
          .register();
  private static final Gauge JOURNAL_OPEN_DURATION =
      Gauge.build()
          .namespace(NAMESPACE)
          .name("journal_open_time")
          .help("Time taken to open the journal")
          .labelNames(PARTITION_LABEL)
          .register();

  private final String logName;

  public JournalMetrics(final String logName) {
    this.logName = logName;
  }

  public void observeSegmentCreation(final Runnable segmentCreation) {
    SEGMENT_CREATION_TIME.labels(logName).time(segmentCreation);
  }

  public void observeSegmentFlush(final Runnable segmentFlush) {
    SEGMENT_FLUSH_TIME.labels(logName).time(segmentFlush);
  }

  public void observeSegmentTruncation(final Runnable segmentTruncation) {
    SEGMENT_TRUNCATE_TIME.labels(logName).time(segmentTruncation);
  }

  public void observeJournalOpenDuration(final long durationMillis) {
    JOURNAL_OPEN_DURATION.labels(logName).set(durationMillis);
  }

  public void incSegmentCount() {
    SEGMENT_COUNT.labels(logName).inc();
  }

  public void decSegmentCount() {
    SEGMENT_COUNT.labels(logName).dec();
  }
}
