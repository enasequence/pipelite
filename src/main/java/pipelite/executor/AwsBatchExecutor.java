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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.AwsBatchDescribeJobsCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Flogger
@Getter
@Setter
public class AwsBatchExecutor extends AbstractAsyncExecutor<AwsBatchExecutorParameters>
    implements JsonSerializableExecutor {

  // Json deserialization requires a no argument constructor.
  public AwsBatchExecutor() {}

  /** The AWSBatch region. Set during submit. */
  private String region;

  /** The AWSBatch job id. Set during submit. */
  private String jobId;

  private DescribeJobs<String, AwsBatchDescribeJobsCache.ExecutorContext> describeJobs() {
    return describeJobsCache.awsBatch.getDescribeJobs(this);
  }

  @Override
  protected void prepareSubmit(StageExecutorRequest request) {
    region = null;
    jobId = null;
  }

  @Override
  protected StageExecutorResult submit(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Submitting AWSBatch job.");

    AwsBatchExecutorParameters params = getExecutorParams();
    region = params.getRegion();

    SubmitJobRequest submitJobRequest =
        new SubmitJobRequest()
            .withJobName(awsBatchJobName(request))
            .withJobQueue(params.getQueue())
            .withJobDefinition(params.getDefinition())
            .withParameters(params.getParameters())
            .withTimeout(
                new JobTimeout()
                    .withAttemptDurationSeconds((int) params.getTimeout().getSeconds()));
    // TODO: .withContainerOverrides()

    AWSBatch awsBatch = awsBatchClient(region);
    SubmitJobResult submitJobResult =
        RetryTask.DEFAULT.execute(r -> awsBatch.submitJob(submitJobRequest));

    if (submitJobResult == null || submitJobResult.getJobId() == null) {
      throw new PipeliteException("Missing AWSBatch submit job id.");
    }
    jobId = submitJobResult.getJobId();
    return StageExecutorResult.submitted();
  }

  @Override
  protected StageExecutorResult poll(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Checking AWSBatch job result.");
    StageExecutorResult result =
        describeJobs().getResult(jobId, getExecutorParams().getPermanentErrors());
    if (result.isActive()) {
      return StageExecutorResult.active();
    }
    if (getExecutorParams().isSaveLog()) {
      // TODO: save log
    }
    return result;
  }

  @Override
  public void terminate() {
    if (jobId == null) {
      return;
    }
    TerminateJobRequest terminateJobRequest =
        new TerminateJobRequest().withJobId(jobId).withReason("Job terminated by pipelite");
    RetryTask.DEFAULT.execute(
        r -> describeJobs().getExecutorContext().getAwsBatch().terminateJob(terminateJobRequest));
    // Remove request because the execution is being terminated.
    describeJobs().removeRequest(jobId);
  }

  public static Map<String, StageExecutorResult> describeJobs(
      List<String> requests, AwsBatchDescribeJobsCache.ExecutorContext executorContext) {

    Map<String, StageExecutorResult> results = new HashMap<>();
    DescribeJobsResult jobResult =
        RetryTask.DEFAULT.execute(
            r ->
                executorContext
                    .getAwsBatch()
                    .describeJobs(new DescribeJobsRequest().withJobs(requests)));
    jobResult.getJobs().forEach(j -> results.put(j.getJobId(), describeJobsResult(j)));
    return results;
  }

  // TOOO: exit codes
  protected static StageExecutorResult describeJobsResult(JobDetail jobDetail) {
    switch (jobDetail.getStatus()) {
      case "SUCCEEDED":
        return StageExecutorResult.success();
      case "FAILED":
        return StageExecutorResult.error();
    }
    return StageExecutorResult.active();
  }

  public static AWSBatch awsBatchClient(String region) {
    AWSBatchClientBuilder awsBuilder = AWSBatchClientBuilder.standard();
    if (region != null) {
      awsBuilder.setRegion(region);
    }
    return awsBuilder.build();
  }

  private String awsBatchJobName(StageExecutorRequest request) {
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

  private FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
