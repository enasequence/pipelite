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

import com.amazonaws.services.batch.AWSBatch;
import com.amazonaws.services.batch.AWSBatchClientBuilder;
import com.amazonaws.services.batch.model.*;
import com.google.common.flogger.FluentLogger;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultType;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

/** Executes a command using AWSBatch. */
@Flogger
@Getter
@Setter
public class AwsBatchExecutor extends AbstractExecutor<AwsBatchExecutorParameters>
    implements JsonSerializableExecutor {

  // TODO: batch poll requests
  // TODO: capture lobs
  // TODO: error handling

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

    AWSBatch client = getClient();

    AwsBatchExecutorParameters params = getExecutorParams();

    SubmitJobRequest request =
        new SubmitJobRequest()
            .withJobName(getJobName(pipelineName, processId, stage))
            .withJobQueue(params.getQueue())
            .withJobDefinition(params.getJobDefinition())
            .withParameters(params.getJobParameters())
            .withTimeout(
                new JobTimeout()
                    .withAttemptDurationSeconds((int) params.getTimeout().getSeconds()));
    // TODO: .withContainerOverrides()

    submitJobResult = client.submitJob(request);

    //    TerminateJobResult terminateJob(TerminateJobRequest terminateJobRequest);

    if (submitJobResult == null || submitJobResult.getJobId() == null) {
      logContext(log.atSevere(), pipelineName, processId, stage).log("No AWSBatch submit job id.");
      StageExecutorResult result = new StageExecutorResult(StageExecutorResultType.ERROR);
      result.setInternalError(StageExecutorResult.InternalError.CMD_SUBMIT);
      return result;
    }
    return new StageExecutorResult(StageExecutorResultType.ACTIVE);
  }

  private StageExecutorResult poll(String pipelineName, String processId, Stage stage) {

    AWSBatch client = getClient();

    Duration timeout = getExecutorParams().getTimeout();

    if (timeout != null && ZonedDateTime.now().isAfter(startTime.plus(timeout))) {
      logContext(log.atSevere(), pipelineName, processId, stage)
          .log("Maximum run time exceeded. Killing AwsBatch job.");

      client.terminateJob(new TerminateJobRequest().withJobId(submitJobResult.getJobId()));

      StageExecutorResult result = StageExecutorResult.error();
      result.setInternalError(StageExecutorResult.InternalError.CMD_TIMEOUT);
      return result;
    }

    logContext(log.atFine(), pipelineName, processId, stage).log("Checking AWSBatch job result.");

    DescribeJobsResult jobResult =
        client.describeJobs(new DescribeJobsRequest().withJobs(getJobId()));
    Optional<JobDetail> jobDetail =
        jobResult.getJobs().stream().filter(j -> j.getJobId().equals(getJobId())).findFirst();

    String status = jobDetail.get().getStatus();
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

  private AWSBatch getClient() {
    AWSBatchClientBuilder builder = AWSBatchClientBuilder.standard();
    String region = getExecutorParams().getRegion();
    if (region != null) {
      builder.setRegion(region);
    }
    return builder.build();
  }

  private FluentLogger.Api logContext(
      FluentLogger.Api log, String pipelineName, String processId, Stage stage) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName)
        .with(LogKey.PROCESS_ID, processId)
        .with(LogKey.STAGE_NAME, stage.getStageName());
  }
}
