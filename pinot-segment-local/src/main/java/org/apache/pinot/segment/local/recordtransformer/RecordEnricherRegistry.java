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
package org.apache.pinot.segment.local.recordtransformer;

import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordEnricherRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordEnricherRegistry.class);

  private RecordEnricherRegistry() {
  }

//  static {
//    for (RecordEnricherFactory recordEnricherFactory : ServiceLoader.load(RecordEnricherFactory.class)) {
//      LOGGER.info("Registered record enricher factory type: {}", recordEnricherFactory.getEnricherType());
//      RecordEnricherTransformer.RECORD_ENRICHER_FACTORY_MAP.put(recordEnricherFactory.getEnricherType(), recordEnricherFactory);
//    }
//  }
}
