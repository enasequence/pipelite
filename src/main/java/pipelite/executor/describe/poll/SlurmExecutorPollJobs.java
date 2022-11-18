/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.executor.describe.poll;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.exception.PipeliteException;
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.SlurmExecutorContext;
import pipelite.executor.describe.context.request.SlurmRequestContext;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

@Component
@Flogger
public class SlurmExecutorPollJobs implements PollJobs<SlurmExecutorContext, SlurmRequestContext> {

  private static final String SQUEUE_CMD =
      "squeue --me --states=all -O \"JobID:|,State:|,exit_code:|,AllocNodes:|\"";

  private static final String SQUEUE_HEADER = "JOBID|STATE|EXIT_CODE|ALLOC_NODES|";
  private static final String SQUEUE_STATUS_COMPLETED = "COMPLETED";
  private static final String SQUEUE_STATUS_TIMEOUT = "TIMEOUT";
  private static final List<String> SQUEUE_STATUS_FAILED =
      Arrays.asList(
          "FAILED",
          "BOOT_FAIL",
          "CANCELLED",
          "DEADLINE",
          "FAILED",
          "NODE_FAIL",
          "OUT_OF_MEMORY",
          "PREEMPTED",
          "REVOKED",
          SQUEUE_STATUS_TIMEOUT);

  private static final int SQUEUE_COLUMNS = 4;
  private static final int SQUEUE_COLUMN_JOB_ID = 0;
  private static final int SQUEUE_COLUMN_STATUS = 1;
  private static final int SQUEUE_COLUMN_EXIT_CODE = 2;
  private static final int SQUEUE_COLUMN_HOST = 3;

  @Override
  public DescribeJobsResults<SlurmRequestContext> pollJobs(
      SlurmExecutorContext executorContext,
      DescribeJobsPollRequests<SlurmRequestContext> requests) {

    StageExecutorResult result = executorContext.cmdRunner().execute(SQUEUE_CMD);
    if (result.isError()) {
      throw new PipeliteException("Failed to poll SLURM jobs: " + result.stageLog());
    }
    return extractJobResults(requests, result.stageLog());
  }

  public static DescribeJobsResults<SlurmRequestContext> extractJobResults(
      DescribeJobsPollRequests<SlurmRequestContext> requests, String str) {
    DescribeJobsResults<SlurmRequestContext> results = new DescribeJobsResults<>();
    Set<String> jobIds = new HashSet<>();

    int lineNumber = 0;
    for (String line : str.split("\\r?\\n")) {
      lineNumber++;
      if (lineNumber == 1) {
        if (!SQUEUE_HEADER.equals(line)) {
          throw new PipeliteException("Unexpected SLURM squeue header line: " + line);
        }
      } else {
        DescribeJobsResult<SlurmRequestContext> result = extractJobResult(requests, line);
        if (result != null) {
          results.add(result);
          jobIds.add(result.jobId());
        }
      }
    }

    for (String jobId : requests.jobIds) {
      if (!jobIds.contains(jobId)) {
        // The job has been lost.
        results.add(DescribeJobsResult.builder(requests, jobId).lostError().build());
      }
    }

    return results;
  }

  public static DescribeJobsResult<SlurmRequestContext> extractJobResult(
      DescribeJobsPollRequests<SlurmRequestContext> requests, String line) {
    String[] column = line.trim().split("\\|");
    if (column.length != SQUEUE_COLUMNS) {
      throw new PipeliteException("Unexpected SLURM squeue output line: " + line);
    }

    String jobId = column[SQUEUE_COLUMN_JOB_ID];

    if (requests.request(jobId) == null) {
      // Result is for an unrelated job.
      return null;
    }

    DescribeJobsResult.Builder result = DescribeJobsResult.builder(requests, jobId);

    if (column[SQUEUE_COLUMN_STATUS].equals(SQUEUE_STATUS_COMPLETED)) {
      result.success();
    } else if (SQUEUE_STATUS_FAILED.contains(column[SQUEUE_COLUMN_STATUS])) {
      if (column[SQUEUE_COLUMN_STATUS].equals(SQUEUE_STATUS_TIMEOUT)) {
        result.timeoutError();
      } else {
        String exitCode = column[SQUEUE_COLUMN_EXIT_CODE];
        result.executionError(exitCode);
      }
    } else {
      result.active();
    }

    if (result.isCompleted()) {
      result.attribute(StageExecutorResultAttribute.EXEC_HOST, column[SQUEUE_COLUMN_HOST]);
    }

    return result.build();
  }
}
