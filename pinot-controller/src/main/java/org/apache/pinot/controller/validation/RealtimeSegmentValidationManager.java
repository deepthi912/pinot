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
package org.apache.pinot.controller.validation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerGauge;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.LeadControllerManager;
import org.apache.pinot.controller.api.resources.PauseStatusDetails;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.periodictask.ControllerPeriodicTask;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Validates realtime ideal states and segment metadata, fixing any partitions which have stopped consuming,
 * and uploading segments to deep store if segment download url is missing in the metadata.
 */
public class RealtimeSegmentValidationManager extends ControllerPeriodicTask<RealtimeSegmentValidationManager.Context> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RealtimeSegmentValidationManager.class);

  private final PinotLLCRealtimeSegmentManager _llcRealtimeSegmentManager;
  private final ValidationMetrics _validationMetrics;
  private final ControllerMetrics _controllerMetrics;
  private final StorageQuotaChecker _storageQuotaChecker;
  private final ResourceUtilizationManager _resourceUtilizationManager;

  private final int _segmentLevelValidationIntervalInSeconds;
  private long _lastSegmentLevelValidationRunTimeMs = 0L;
  private final boolean _segmentAutoResetOnErrorAtValidation;

  public static final String OFFSET_CRITERIA = "offsetCriteria";
  public static final String RUN_SEGMENT_LEVEL_VALIDATION = "runSegmentLevelValidation";
  public static final String REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP =
      "repairErrorSegmentsForPartialUpsertOrDedup";

  public RealtimeSegmentValidationManager(ControllerConf config, PinotHelixResourceManager pinotHelixResourceManager,
      LeadControllerManager leadControllerManager, PinotLLCRealtimeSegmentManager llcRealtimeSegmentManager,
      ValidationMetrics validationMetrics, ControllerMetrics controllerMetrics, StorageQuotaChecker quotaChecker,
      ResourceUtilizationManager resourceUtilizationManager) {
    super("RealtimeSegmentValidationManager", config.getRealtimeSegmentValidationFrequencyInSeconds(),
        config.getRealtimeSegmentValidationManagerInitialDelaySeconds(), pinotHelixResourceManager,
        leadControllerManager, controllerMetrics);
    _llcRealtimeSegmentManager = llcRealtimeSegmentManager;
    _validationMetrics = validationMetrics;
    _controllerMetrics = controllerMetrics;
    _storageQuotaChecker = quotaChecker;
    _resourceUtilizationManager = resourceUtilizationManager;

    _segmentLevelValidationIntervalInSeconds = config.getSegmentLevelValidationIntervalInSeconds();
    _segmentAutoResetOnErrorAtValidation = config.isAutoResetErrorSegmentsOnValidationEnabled();
    Preconditions.checkState(_segmentLevelValidationIntervalInSeconds > 0);
  }

  @Override
  protected Context preprocess(Properties periodicTaskProperties) {
    Context context = new Context();
    // Run segment level validation only if certain time has passed after previous run
    long currentTimeMs = System.currentTimeMillis();
    if (shouldRunSegmentValidation(periodicTaskProperties, currentTimeMs)) {
      LOGGER.info("Run segment-level validation");
      context._runSegmentLevelValidation = true;
      _lastSegmentLevelValidationRunTimeMs = currentTimeMs;
    }
    context._repairErrorSegmentsForPartialUpsertOrDedup =
        shouldRepairErrorSegmentsForPartialUpsertOrDedup(periodicTaskProperties);
    String offsetCriteriaStr = periodicTaskProperties.getProperty(OFFSET_CRITERIA);
    if (offsetCriteriaStr != null) {
      context._offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString(offsetCriteriaStr);
    }
    return context;
  }

  @Override
  protected void processTable(String tableNameWithType, Context context) {
    if (!TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
      return;
    }

    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    if (tableConfig == null) {
      LOGGER.warn("Failed to find table config for table: {}, skipping validation", tableNameWithType);
      return;
    }
    List<StreamConfig> streamConfigs = IngestionConfigUtils.getStreamConfigs(tableConfig);

    if (shouldEnsureConsuming(tableNameWithType)) {
      _llcRealtimeSegmentManager.ensureAllPartitionsConsuming(tableConfig, streamConfigs, context._offsetCriteria);
    }

    if (context._runSegmentLevelValidation) {
      runSegmentLevelValidation(tableConfig);
    } else {
      LOGGER.info("Skipping segment-level validation for table: {}", tableConfig.getTableName());
    }

    boolean isPauselessConsumptionEnabled = PauselessConsumptionUtils.isPauselessEnabled(tableConfig);
    if (isPauselessConsumptionEnabled) {
      // For pauseless tables without dedup or partial upsert, repair segments in error state
      _llcRealtimeSegmentManager.repairSegmentsInErrorStateForPauselessConsumption(tableConfig,
          context._repairErrorSegmentsForPartialUpsertOrDedup);
    } else if (_segmentAutoResetOnErrorAtValidation) {
      // Reset for pauseless tables is already handled in repairSegmentsInErrorStateForPauselessConsumption method with
      // additional checks for pauseless consumption
      _pinotHelixResourceManager.resetSegments(tableConfig.getTableName(), null, true);
    }
  }

  /**
   *
   * Updates the table paused state based on pause validations (e.g. storage quota being exceeded).
   * Skips updating the pause state if table is paused by admin.
   * Returns true if table is not paused
   */
  @VisibleForTesting
  boolean shouldEnsureConsuming(String tableNameWithType) {
    PauseStatusDetails pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
    boolean isTablePaused = pauseStatus.getPauseFlag();
    if (isTablePaused && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.ADMINISTRATIVE)) {
      return false; // if table is paused by admin, then skip subsequent checks
    }
    // Perform resource utilization checks.
    UtilizationChecker.CheckResult isResourceUtilizationWithinLimits =
        _resourceUtilizationManager.isResourceUtilizationWithinLimits(tableNameWithType,
            UtilizationChecker.CheckPurpose.REALTIME_INGESTION);
    if (isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.FAIL) {
      LOGGER.warn("Resource utilization limit exceeded for table: {}", tableNameWithType);
      _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
          1L);
      // update IS when table is not paused or paused with another reason code
      if (!isTablePaused || !pauseStatus.getReasonCode()
          .equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
        _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType,
            PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, "Resource utilization limit exceeded.");
      }
      return false; // if resource utilization check failed, then skip subsequent checks
    } else if ((isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.PASS) && isTablePaused
        && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
      // within limits and table previously paused by resource utilization --> unpause
      LOGGER.info("Resource utilization limit is back within limits for table: {}", tableNameWithType);
      // unset the pause state and allow consuming segment recreation.
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED, "Resource utilization within limits");
      pauseStatus = _llcRealtimeSegmentManager.getPauseStatusDetails(tableNameWithType);
      isTablePaused = pauseStatus.getPauseFlag();
    } else if ((isResourceUtilizationWithinLimits == UtilizationChecker.CheckResult.UNDETERMINED) && isTablePaused
        && pauseStatus.getReasonCode().equals(PauseState.ReasonCode.RESOURCE_UTILIZATION_LIMIT_EXCEEDED)) {
      // The table was previously paused due to exceeding resource utilization, but the current status cannot be
      // determined. To be safe, leave it as paused and once the status is available take the correct action
      LOGGER.warn("Resource utilization limit could not be determined for for table: {}, and it is paused, leave it as "
              + "paused", tableNameWithType);
      return false;
    }
    _controllerMetrics.setOrUpdateTableGauge(tableNameWithType, ControllerGauge.RESOURCE_UTILIZATION_LIMIT_EXCEEDED,
        0L);
    // Perform storage quota checks.
    TableConfig tableConfig = _pinotHelixResourceManager.getTableConfig(tableNameWithType);
    boolean isQuotaExceeded = _storageQuotaChecker.isTableStorageQuotaExceeded(tableConfig);
    if (isQuotaExceeded == isTablePaused) {
      return !isTablePaused;
    }
    // if quota breach and pause flag is not in sync, update the IS
    if (isQuotaExceeded) {
      String storageQuota = tableConfig.getQuotaConfig() != null ? tableConfig.getQuotaConfig().getStorage() : "NA";
      // as quota is breached pause the consumption right away
      _llcRealtimeSegmentManager.pauseConsumption(tableNameWithType, PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED,
          "Storage quota of " + storageQuota + " exceeded.");
    } else {
      // as quota limit is being honored, unset the pause state and allow consuming segment recreation.
      _llcRealtimeSegmentManager.updatePauseStateInIdealState(tableNameWithType, false,
          PauseState.ReasonCode.STORAGE_QUOTA_EXCEEDED, "Table storage within quota limits");
    }
    return !isQuotaExceeded;
  }

  private void runSegmentLevelValidation(TableConfig tableConfig) {
    String realtimeTableName = tableConfig.getTableName();

    List<SegmentZKMetadata> segmentsZKMetadata = _pinotHelixResourceManager.getSegmentsZKMetadata(realtimeTableName);

    // Delete tmp segments
    if (_llcRealtimeSegmentManager.isTmpSegmentAsyncDeletionEnabled()) {
      try {
        long startTimeMs = System.currentTimeMillis();
        int numDeletedTmpSegments = _llcRealtimeSegmentManager.deleteTmpSegments(realtimeTableName, segmentsZKMetadata);
        LOGGER.info("Deleted {} tmp segments for table: {} in {}ms", numDeletedTmpSegments, realtimeTableName,
            System.currentTimeMillis() - startTimeMs);
        _controllerMetrics.addMeteredTableValue(realtimeTableName, ControllerMeter.DELETED_TMP_SEGMENT_COUNT,
            numDeletedTmpSegments);
      } catch (Exception e) {
        LOGGER.error("Failed to delete tmp segments for table: {}", realtimeTableName, e);
      }
    }

    // Update the total document count gauge
    _validationMetrics.updateTotalDocumentCountGauge(realtimeTableName, computeTotalDocumentCount(segmentsZKMetadata));

    // Ensures all segments in COMMITTING state are properly tracked in ZooKeeper.
    // Acts as a recovery mechanism for segments that may have failed to register during start of commit protocol.
    if (PauselessConsumptionUtils.isPauselessEnabled(tableConfig)) {
      syncCommittingSegmentsFromMetadata(realtimeTableName, segmentsZKMetadata);
    }

    // Check missing segments and upload them to the deep store
    if (_llcRealtimeSegmentManager.isDeepStoreLLCSegmentUploadRetryEnabled()) {
      _llcRealtimeSegmentManager.uploadToDeepStoreIfMissing(tableConfig, segmentsZKMetadata);
    }
  }

  private void syncCommittingSegmentsFromMetadata(String realtimeTableName,
      List<SegmentZKMetadata> segmentsZKMetadata) {
    List<String> committingSegments = new ArrayList<>();
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      if (CommonConstants.Segment.Realtime.Status.COMMITTING.equals(segmentZKMetadata.getStatus())) {
        committingSegments.add(segmentZKMetadata.getSegmentName());
      }
    }
    LOGGER.info("Adding committing segments to ZK: {}", committingSegments);
    if (!_llcRealtimeSegmentManager.syncCommittingSegments(realtimeTableName, committingSegments)) {
      LOGGER.error("Failed to add committing segments for table: {}", realtimeTableName);
    }
  }

  private boolean shouldRunSegmentValidation(Properties periodicTaskProperties, long currentTimeMs) {
    boolean runValidation = Optional.ofNullable(
            periodicTaskProperties.getProperty(RUN_SEGMENT_LEVEL_VALIDATION))
        .map(value -> {
          try {
            return Boolean.parseBoolean(value);
          } catch (Exception e) {
            return false;
          }
        })
        .orElse(false);

    boolean timeThresholdMet = TimeUnit.MILLISECONDS.toSeconds(currentTimeMs - _lastSegmentLevelValidationRunTimeMs)
        >= _segmentLevelValidationIntervalInSeconds;

    return runValidation || timeThresholdMet;
  }

  private boolean shouldRepairErrorSegmentsForPartialUpsertOrDedup(Properties periodicTaskProperties) {
    return Optional.ofNullable(periodicTaskProperties.getProperty(REPAIR_ERROR_SEGMENTS_FOR_PARTIAL_UPSERT_OR_DEDUP))
        .map(value -> {
          try {
            return Boolean.parseBoolean(value);
          } catch (Exception e) {
            return false;
          }
        })
        .orElse(false);
  }

  @Override
  protected void nonLeaderCleanup(List<String> tableNamesWithType) {
    for (String tableNameWithType : tableNamesWithType) {
      if (TableNameBuilder.isRealtimeTableResource(tableNameWithType)) {
        _validationMetrics.cleanupTotalDocumentCountGauge(tableNameWithType);
        _controllerMetrics.removeTableMeter(tableNameWithType, ControllerMeter.DELETED_TMP_SEGMENT_COUNT);
      }
    }
  }

  @VisibleForTesting
  static long computeTotalDocumentCount(List<SegmentZKMetadata> segmentsZKMetadata) {
    long numTotalDocs = 0;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      numTotalDocs += segmentZKMetadata.getTotalDocs();
    }
    return numTotalDocs;
  }

  @Override
  public void cleanUpTask() {
    LOGGER.info("Unregister all the validation metrics.");
    _validationMetrics.unregisterAllMetrics();
  }

  public static final class Context {
    private boolean _runSegmentLevelValidation;
    private boolean _repairErrorSegmentsForPartialUpsertOrDedup;
    private OffsetCriteria _offsetCriteria;
  }

  @VisibleForTesting
  public ValidationMetrics getValidationMetrics() {
    return _validationMetrics;
  }
}
