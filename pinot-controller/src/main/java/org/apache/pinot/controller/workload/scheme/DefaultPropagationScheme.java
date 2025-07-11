/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.controller.workload.scheme;

import java.util.HashSet;
import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


public class DefaultPropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public DefaultPropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig nodeConfig) {
    Set<String> instances;
    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    switch (nodeType) {
      case BROKER_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllBrokerInstances());
        break;
      case SERVER_NODE:
        instances = new HashSet<>(_pinotHelixResourceManager.getAllServerInstances());
        break;
      default:
        throw new IllegalArgumentException("Invalid node type: " + nodeType);
    }
    return instances;
  }
}
