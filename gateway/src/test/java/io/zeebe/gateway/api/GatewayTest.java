/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.gateway.api;

import io.zeebe.gateway.protocol.GatewayGrpc.GatewayBlockingStub;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;

public class GatewayTest {

  @Rule public StubbedGatewayRule gatewayRule = new StubbedGatewayRule();

  protected StubbedGateway gateway;
  protected GatewayBlockingStub client;

  @Before
  public void setUp() throws IOException {
    gateway = gatewayRule.getGateway();
    client = gatewayRule.getClient();
  }
}
