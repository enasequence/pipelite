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
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.AwsBatchExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.log.LogKey;
import pipelite.retryable.RetryableExternalAction;
import pipelite.service.PipeliteServices;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.AwsBatchExecutorParameters;

import java.util.UUID;

@Flogger
@Getter
@Setter
public class AwsBatchExecutor
    extends AsyncExecutor<
        AwsBatchExecutorParameters, DefaultRequestContext, AwsBatchExecutorContext>
    implements JsonSerializableExecutor {

  /**
   * The AWSBatch region. Set during submit. Serialize in database to continue execution after
   * service restart.
   */
  private String region;

  @Override
  protected DefaultRequestContext prepareRequestContext() {
    return new DefaultRequestContext(getJobId());
  }

  @Override
  protected DescribeJobs<DefaultRequestContext, AwsBatchExecutorContext> prepareDescribeJobs(
      PipeliteServices pipeliteServices) {
    return pipeliteServices.jobs().awsBatch().getDescribeJobs(this);
  }

  @Override
  protected SubmitJobResult submitJob() {
    StageExecutorRequest request = getRequest();
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

    AWSBatch awsBatch = client(region);
    com.amazonaws.services.batch.model.SubmitJobResult submitJobResult =
        RetryableExternalAction.execute(() -> awsBatch.submitJob(submitJobRequest));

    if (submitJobResult == null || submitJobResult.getJobId() == null) {
      throw new PipeliteException("Missing AWSBatch submit job id.");
    }
    String jobId = submitJobResult.getJobId();
    logContext(log.atInfo(), request).log("Submitted AWSBatch job " + getJobId());
    return new SubmitJobResult(jobId);
  }

  @Override
  public void terminateJob() {
    String jobId = getJobId();
    if (jobId == null) {
      return;
    }
    TerminateJobRequest terminateJobRequest =
        new TerminateJobRequest().withJobId(jobId).withReason("Job terminated by pipelite");
    RetryableExternalAction.execute(() -> client(region).terminateJob(terminateJobRequest));
  }

  /** Polls job execution results. */
  public static DescribeJobsResults<DefaultRequestContext> pollJobs(
      AWSBatch awsBatch, DescribeJobsPollRequests<DefaultRequestContext> requests) {
    DescribeJobsResults<DefaultRequestContext> results = new DescribeJobsResults<>();
    com.amazonaws.services.batch.model.DescribeJobsResult jobResult =
        RetryableExternalAction.execute(
            () -> awsBatch.describeJobs(new DescribeJobsRequest().withJobs(requests.jobIds)));
    jobResult
        .getJobs()
        .forEach(
            j ->
                results.add(DescribeJobsResult.create(requests, j.getJobId(), extractJobResult(j))));
    return results;
  }

  // TOOO: exit codes
  protected static StageExecutorResult extractJobResult(JobDetail jobDetail) {
    switch (jobDetail.getStatus()) {
      case "SUCCEEDED":
        return StageExecutorResult.success();
      case "FAILED":
        return StageExecutorResult.error();
    }
    return StageExecutorResult.active();
  }

  public static AWSBatch client(String region) {
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
