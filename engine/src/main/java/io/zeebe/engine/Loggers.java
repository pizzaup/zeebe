/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Loggers {
  public static final Logger STREAM_PROCESSING =
      LoggerFactory.getLogger("io.zeebe.broker.logstreams");
  public static final Logger WORKFLOW_REPOSITORY_LOGGER =
      LoggerFactory.getLogger("io.zeebe.broker.workflow.repository");

  public static final Logger WORKFLOW_PROCESSOR_LOGGER =
      LoggerFactory.getLogger("io.zeebe.broker.workflow");

  public static Logger getExporterLogger(final String exporterId) {
    final String loggerName = String.format("io.zeebe.broker.exporter.%s", exporterId);
    return LoggerFactory.getLogger(loggerName);
  }
}
