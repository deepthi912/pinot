package org.apache.pinot.segment.local.recordtransformer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.pinot.segment.local.segment.creator.TransformPipeline;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordEnricherTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordEnricherRegistry.class);
  private static final Map<String, RecordEnricherFactory> RECORD_ENRICHER_FACTORY_MAP = new HashMap<>();
  private final Set<String> _columnsToExtract = new HashSet<>();

  public RecordEnricherTransformer(TableConfig tableConfig) {
  }

  @Override
  public boolean isNoOp() {
    return RECORD_ENRICHER_FACTORY_MAP.isEmpty();
  }

  public static void validateEnrichmentConfig(TransformConfig enrichmentConfig,
      RecordEnricherValidationConfig config) {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }

    RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .validateEnrichmentConfig(enrichmentConfig.getProperties(), config);
  }

  public static RecordTransformer createRecordEnricher(TransformConfig enrichmentConfig)
      throws IOException {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }
    return RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .createEnricher(enrichmentConfig.getProperties());
  }

  public TransformPipeline fromIngestionConfig(IngestionConfig ingestionConfig) {
    TransformPipeline pipeline = new TransformPipeline();
    if (null == ingestionConfig || null == ingestionConfig.getEnrichmentConfigs()) {
      return pipeline;
    }
    List<TransformConfig> enrichmentConfigs = ingestionConfig.getEnrichmentConfigs();
    for (TransformConfig enrichmentConfig : enrichmentConfigs) {
      try {
        RecordTransformer enricher = createRecordEnricher(enrichmentConfig);
        _columnsToExtract.addAll(enricher.getInputColumns());
      } catch (IOException e) {
        throw new RuntimeException("Failed to instantiate record enricher " + enrichmentConfig.getEnricherType(), e);
      }
    }
    return pipeline;
  }

  static {
    for (RecordEnricherFactory recordEnricherFactory : ServiceLoader.load(RecordEnricherFactory.class)) {
      LOGGER.info("Registered record enricher factory type: {}", recordEnricherFactory.getEnricherType());
      RecordEnricherTransformer.RECORD_ENRICHER_FACTORY_MAP.put(recordEnricherFactory.getEnricherType(), recordEnricherFactory);
    }
  }
}
