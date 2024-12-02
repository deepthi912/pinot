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
package org.apache.pinot.plugin.record.enricher.clp;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import java.io.IOException;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerFactory;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerValidationConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.utils.JsonUtils;

@AutoService(RecordTransformerFactory.class)
public class CLPEncodingTransformerFactory implements RecordTransformerFactory {
  private static final String ENRICHER_TYPE = "clpEnricher";
  @Override
  public String getEnricherType() {
    return ENRICHER_TYPE;
  }

  @Override
  public RecordTransformer createTransformer(IngestionConfig ingestionConfig)
      throws IOException {
    return new CLPEncodingTransformer(ingestionConfig);
  }

  @Override
  public void validateEnrichmentConfig(JsonNode enricherProps, RecordTransformerValidationConfig validationConfig) {
    try {
      ClpTransformerConfig config = JsonUtils.jsonNodeToObject(enricherProps, ClpTransformerConfig.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse clp enricher config", e);
    }
  }
}
