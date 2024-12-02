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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.record.enricher.clp.CLPEncodingTransformer;
import org.apache.pinot.plugin.record.enricher.clp.ClpTransformerConfig;
import org.apache.pinot.plugin.record.enricher.function.CustomFunctionTransformer;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * The {@code CompositeTransformer} class performs multiple transforms based on the inner {@link RecordTransformer}s.
 */
public class RecordEnricherTransformer implements RecordTransformer {
  private final List<RecordTransformer> _transformers;
  private final List<String> _columnsToExtract;

  /**
   * Returns a record transformer that performs null value handling, time/expression/data-type transformation and record
   * sanitization.
   * <p>NOTE: DO NOT CHANGE THE ORDER OF THE RECORD TRANSFORMERS
   * <ul>
   *   <li>
   *     Optional {@link ExpressionTransformer} before everyone else, so that we get the real columns for other
   *     transformers to work on
   *   </li>
   *   <li>
   *     Optional {@link FilterTransformer} after {@link ExpressionTransformer}, so that we have source as well as
   *     destination columns
   *   </li>
   *   <li>
   *     Optional {@link SchemaConformingTransformer} after {@link FilterTransformer}, so that we can transform input
   *     records that have varying fields to a fixed schema without dropping any fields
   *   </li>
   *   <li>
   *     Optional {@link SchemaConformingTransformerV2} after {@link FilterTransformer}, so that we can transform
   *     input records that have varying fields to a fixed schema and keep or drop other fields by configuration. We
   *     could also gain enhanced text search capabilities from it.
   *   </li>
   *   <li>
   *     {@link DataTypeTransformer} after {@link SchemaConformingTransformer} or {@link SchemaConformingTransformerV2}
   *     to convert values to comply with the schema
   *   </li>
   *   <li>
   *     Optional {@link TimeValidationTransformer} after {@link DataTypeTransformer} so that time value is converted to
   *     the correct type
   *   </li>
   *   <li>
   *     {@link NullValueTransformer} after {@link DataTypeTransformer} and {@link TimeValidationTransformer} because
   *     empty Collection/Map/Object[] and invalid values can be replaced with null
   *   </li>
   *   <li>
   *     Optional {@link SanitizationTransformer} after {@link NullValueTransformer} so that before sanitation, all
   *     values are non-null and follow the data types defined in the schema
   *   </li>
   *   <li>
   *     {@link SpecialValueTransformer} after {@link DataTypeTransformer} so that we already have the values complying
   *      with the schema before handling special values and before {@link NullValueTransformer} so that it transforms
   *      all the null values properly
   *   </li>
   * </ul>
   */
  public static List<RecordTransformer> getDefaultTransformers(TableConfig tableConfig)
      throws IOException {
    return Stream.of(new CLPEncodingTransformer(tableConfig.getIngestionConfig()),
        new CustomFunctionTransformer(tableConfig.getIngestionConfig())).collect(Collectors.toList());
  }

  /**
   * Returns a pass through record transformer that does not transform the record.
   */
  public static RecordEnricherTransformer getPassThroughTransformer() {
    return new RecordEnricherTransformer(Collections.emptyList(), Collections.emptyList());
  }

  public static RecordEnricherTransformer getDefaultTransformer(TableConfig tableConfig)
      throws IOException {
    List<RecordTransformer> recordTransformers = getDefaultTransformers(tableConfig);
    List<String> columnsToExtract = new ArrayList<>();
    for(RecordTransformer recordTransformer : recordTransformers) {
      columnsToExtract.addAll(recordTransformer.getInputColumns());
    }
    return new RecordEnricherTransformer(recordTransformers, columnsToExtract);
  }

  public RecordEnricherTransformer(List<RecordTransformer> transformers, List<String> columnsToExtract) {
    _transformers = transformers;
    _columnsToExtract = columnsToExtract;
  }

  public List<RecordTransformer> getTransformers() {
    return _transformers;
  }

  public List<String> getColumnsToExtract() {
    return _columnsToExtract;
  }

  public void add(RecordTransformer enricher) {
    _transformers.add(enricher);
    _columnsToExtract.addAll(enricher.getInputColumns());
  }

  @Nullable
  @Override
  public GenericRow transform(GenericRow record) {
    for (RecordTransformer transformer : _transformers) {
      if (!IngestionUtils.shouldIngestRow(record)) {
        return record;
      }
      record = transformer.transform(record);
      if (record == null) {
        return null;
      }
    }
    return record;
  }
}
