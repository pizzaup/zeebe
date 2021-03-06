/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.cluster.messaging.impl;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import io.atomix.cluster.messaging.ManagedUnicastService;
import io.atomix.cluster.messaging.MessagingConfig;
import io.atomix.utils.net.Address;
import io.zeebe.test.util.socket.SocketUtil;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

/** Netty unicast service test. */
public class NettyUnicastServiceTest extends ConcurrentTestCase {

  private static final Logger LOGGER = getLogger(NettyUnicastServiceTest.class);

  ManagedUnicastService service1;
  ManagedUnicastService service2;

  Address address1;
  Address address2;

  @Test
  public void testUnicast() throws Exception {
    service1.addListener(
        "test",
        (address, payload) -> {
          assertEquals(address2, address);
          assertArrayEquals("Hello world!".getBytes(), payload);
          resume();
        });

    service2.unicast(address1, "test", "Hello world!".getBytes());
    await(5000);
  }

  @Test
  public void shouldNotThrowExceptionWhenServiceStopped() throws Exception {
    // given
    service2.stop();

    // when - then
    assertThatCode(() -> service2.unicast(address1, "test", "Hello world!".getBytes()))
        .doesNotThrowAnyException();
  }

  @Before
  public void setUp() throws Exception {
    address1 = Address.from("127.0.0.1", SocketUtil.getNextAddress().getPort());
    address2 = Address.from("127.0.0.1", SocketUtil.getNextAddress().getPort());

    final String clusterId = "testClusterId";
    service1 = new NettyUnicastService(clusterId, address1, new MessagingConfig());
    service1.start().join();

    service2 = new NettyUnicastService(clusterId, address2, new MessagingConfig());
    service2.start().join();
  }

  @After
  public void tearDown() throws Exception {
    if (service1 != null) {
      try {
        service1.stop().join();
      } catch (final Exception e) {
        LOGGER.warn("Failed stopping netty1", e);
      }
    }

    if (service2 != null) {
      try {
        service2.stop().join();
      } catch (final Exception e) {
        LOGGER.warn("Failed stopping netty2", e);
      }
    }
  }
}
