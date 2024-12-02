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

package org.apache.pinot.plugin.record.enricher.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EncodedMessage;
import com.yscope.clp.compressorfrontend.MessageEncoder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformerValidationConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Enriches the record with custom functions.
 */
public class CustomFunctionTransformer implements RecordTransformer {
  public static final String ENRICHER_TYPE = "generateColumn";
  private final Map<String, FunctionEvaluator> _fieldToFunctionEvaluator;
  private final List<String> _fieldsToExtract;
  private final LinkedHashMap<String, String> _fieldToFunctionMap;

  public CustomFunctionTransformer(IngestionConfig ingestionConfig)
      throws IOException {
    if (ingestionConfig != null && ingestionConfig.getTransformConfigs() != null) {
      for (TransformConfig transformConfig : ingestionConfig.getTransformConfigs()) {
        if (transformConfig.getEnricherType() != null && transformConfig.getEnricherType().equals(ENRICHER_TYPE)) {
          _fieldToFunctionMap = JsonUtils.jsonNodeToObject(transformConfig.getProperties(), LinkedHashMap.class);
          _fieldToFunctionEvaluator = new LinkedHashMap<>();
          _fieldsToExtract = new ArrayList<>();
          for (Map.Entry<String, String> entry : _fieldToFunctionMap.entrySet()) {
            String column = entry.getKey();
            String function = entry.getValue();
            FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(function);
            _fieldToFunctionEvaluator.put(column, functionEvaluator);
            _fieldsToExtract.addAll(functionEvaluator.getArguments());
          }
          return;
        }
      }
    }
    _fieldToFunctionMap = new LinkedHashMap<>();
    _fieldToFunctionEvaluator = new LinkedHashMap<>();
    _fieldsToExtract = new ArrayList<>();
  }

  @Override
  public boolean isNoOp() {
    return _fieldToFunctionMap.isEmpty();
  }

  @Override
  public List<String> getInputColumns() {
    return _fieldsToExtract;
  }

  @Override
  public GenericRow transform(GenericRow record) {
    _fieldToFunctionEvaluator.forEach((field, evaluator) -> {
      record.putValue(field, evaluator.evaluate(record));
    });
    return record;
  }

  public static void validateEnrichmentConfig(JsonNode enricherProps) {
    LinkedHashMap<String, String> fieldToFunctionMap;
    try {
      fieldToFunctionMap = JsonUtils.jsonNodeToObject(enricherProps, LinkedHashMap.class);
      for (String function : fieldToFunctionMap.values()) {
        if (FunctionEvaluatorFactory.isGroovyExpression(function)) {
          throw new IllegalArgumentException("Groovy expression is not allowed for enrichment");
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse custom function enricher config", e);
    }
  }
}
