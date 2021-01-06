/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor;

import com.amazonaws.services.batch.model.*;
import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.service.AwsBatchExecutorService;
import pipelite.executor.service.ExecutorServiceFactory;
import pipelite.log.LogKey;
import pipelite.stage.Stage;
import pipelite.stage.executor.InternalError;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

/** Executes a command using AWSBatch. */
@Flogger
@Getter
@Setter
public class AwsBatchExecutor extends AbstractExecutor<AwsBatchExecutorParameters>
    implements JsonSerializableExecutor {

  // TODO: capture logs

  private SubmitJobResult submitJobResult;
  private ZonedDateTime startTime;

  @Override
  public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
    if (submitJobResult == null) {
      return submit(pipelineName, processId, stage);
    }

    return poll(pipelineName, processId, stage);
  }

  private StageExecutorResult submit(String pipelineName, String processId, Stage stage) {
    startTime = ZonedDateTime.now();

    AwsBatchExecutorService.Client client = getClient();

    AwsBatchExecutorParameters params = getExecutorParams();

    SubmitJobRequest request =
        new SubmitJobRequest()
            .withJobName(getJobName(pipelineName, processId, stage))
            .withJobQueue(params.getQueue())
            .withJobDefinition(params.getDefinition())
            .withParameters(params.getParameters())
            .withTimeout(
                new JobTimeout()
                    .withAttemptDurationSeconds((int) params.getTimeout().getSeconds()));
    // TODO: .withContainerOverrides()

    try {
      submitJobResult = client.submitJob(request);
    } catch (Exception ex) {
      logContext(log.atSevere().withCause(ex), pipelineName, processId, stage)
          .log("Unexpected exception from AWSBatch submit job.");
      return StageExecutorResult.internalError(InternalError.SUBMIT);
    }

    if (submitJobResult == null || submitJobResult.getJobId() == null) {
      logContext(log.atSevere(), pipelineName, processId, stage)
          .log("Missing AWSBatch submit job id.");
      return StageExecutorResult.internalError(InternalError.SUBMIT);
    }
    return new StageExecutorResult(StageExecutorResultType.ACTIVE);
  }

  private StageExecutorResult poll(String pipelineName, String processId, Stage stage) {

    AwsBatchExecutorService.Client client = getClient();

    Duration timeout = getExecutorParams().getTimeout();

    if (timeout != null && ZonedDateTime.now().isAfter(startTime.plus(timeout))) {
      logContext(log.atSevere(), pipelineName, processId, stage)
          .log("Maximum run time exceeded. Killing AwsBatch job.");

      try {
        client.terminateJob(getJobId());
      } catch (Exception ex) {
        logContext(log.atSevere().withCause(ex), pipelineName, processId, stage)
            .log("Unexpected exception from AWSBatch terminate for job id %s", getJobId());
        return StageExecutorResult.internalError(InternalError.TERMINATE);
      }
      return StageExecutorResult.internalError(InternalError.TIMEOUT);
    }

    logContext(log.atFine(), pipelineName, processId, stage).log("Checking AWSBatch job result.");

    JobDetail jobDetail;
    try {
      jobDetail = client.describeJob(getJobId());

      if (jobDetail == null || jobDetail.getStatus() == null) {
        logContext(log.atSevere(), pipelineName, processId, stage)
            .log("Missing AWSBatch job details for job id %s", getJobId());
        return StageExecutorResult.internalError(InternalError.POLL);
      }
    } catch (Exception ex) {
      logContext(log.atSevere().withCause(ex), pipelineName, processId, stage)
          .log("Unexpected exception from AWSBatch terminate.");
      return StageExecutorResult.internalError(InternalError.POLL);
    }

    String status = jobDetail.getStatus();
    switch (status) {
      case "SUCCEEDED":
        return StageExecutorResult.success();
      case "FAILED":
        return StageExecutorResult.error();
      default:
        return StageExecutorResult.active();
    }
  }

  private String getJobName(String pipelineName, String processId, Stage stage) {
    String jobName = "pipelite" + '_' + pipelineName + '_' + processId + '_' + stage;
    // The first character must be alphanumeric. Up to 128 letters (uppercase
    // and lowercase), numbers, hyphens, and underscores are allowed.
    if (jobName.matches(".*[^a-zA-Z0-9\\-_].*") || jobName.length() > 128) {
      jobName = "pipelite" + "_" + UUID.randomUUID();
    }
    return jobName;
  }

  private String getJobId() {
    if (submitJobResult == null) {
      return null;
    }
    return submitJobResult.getJobId();
  }

  private AwsBatchExecutorService.Client getClient() {
    return ExecutorServiceFactory.service(AwsBatchExecutorService.class)
        .client(getExecutorParams().getRegion());
  }

  private FluentLogger.Api logContext(
      FluentLogger.Api log, String pipelineName, String processId, Stage stage) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stage.getStageName());
  }
}
