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
import com.amazonaws.services.batch.model.*;
import com.google.common.flogger.FluentLogger;
import java.util.*;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.context.AwsBatchContextCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.stage.StageState;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

/** Executes a command using AWSBatch. */
@Flogger
@Getter
@Setter
public class AwsBatchExecutor extends AbstractExecutor<AwsBatchExecutorParameters>
    implements JsonSerializableExecutor {

  // TODO: capture logs

  private SubmitJobResult submitJobResult;

  private static final AwsBatchContextCache sharedContextCache = new AwsBatchContextCache();

  private AwsBatchContextCache.Context getSharedContext() {
    return sharedContextCache.getContext(this);
  }

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (submitJobResult == null) {
      return submit(request);
    }

    return poll(request);
  }

  @Override
  public void terminate() {
    TerminateJobRequest terminateJobRequest =
        new TerminateJobRequest().withJobId(getJobId()).withReason("Job terminated by pipelite");
    RetryTask.DEFAULT_FIXED.execute(
        r -> getSharedContext().get().terminateJob(terminateJobRequest));
  }

  private StageExecutorResult submit(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Submitting AWSBatch job.");

    AwsBatchExecutorParameters params = getExecutorParams();
    SubmitJobRequest submitJobRequest =
        new SubmitJobRequest()
            .withJobName(getJobName(request))
            .withJobQueue(params.getQueue())
            .withJobDefinition(params.getDefinition())
            .withParameters(params.getParameters())
            .withTimeout(
                new JobTimeout()
                    .withAttemptDurationSeconds((int) params.getTimeout().getSeconds()));
    // TODO: .withContainerOverrides()

    submitJobResult =
        RetryTask.DEFAULT_FIXED.execute(r -> getSharedContext().get().submitJob(submitJobRequest));

    if (submitJobResult == null || submitJobResult.getJobId() == null) {
      throw new PipeliteException("Missing AWSBatch submit job id.");
    }
    return new StageExecutorResult(StageState.ACTIVE);
  }

  private StageExecutorResult poll(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Checking AWSBatch job result.");
    Optional<StageExecutorResult> result = getSharedContext().describeJobs.getResult(getJobId());
    if (result == null || !result.isPresent()) {
      return StageExecutorResult.active();
    }
    return result.get();
  }

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> jobIds, AWSBatch awsBatch) {
    Map<String, StageExecutorResult> results = new HashMap<>();
    DescribeJobsResult jobResult =
        awsBatch.describeJobs(new DescribeJobsRequest().withJobs(jobIds));
    jobResult
        .getJobs()
        .forEach(
            j -> {
              StageExecutorResult result;
              switch (j.getStatus()) {
                case "SUCCEEDED":
                  result = StageExecutorResult.success();
                  break;
                case "FAILED":
                  result = StageExecutorResult.error();
                  break;
                default:
                  result = StageExecutorResult.active();
              }
              results.put(j.getJobId(), result);
            });
    return results;
  }

  private String getJobName(StageExecutorRequest request) {
    String jobName =
        "pipelite"
            + '_'
            + request.getPipelineName()
            + '_'
            + request.getProcessId()
            + '_'
            + request.getStage().getStageName();
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

  private FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
