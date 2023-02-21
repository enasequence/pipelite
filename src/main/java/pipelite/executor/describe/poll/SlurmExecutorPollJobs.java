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

  // Job steps https://slurm.schedmd.com/job_launch.html#job_records

  private static final String SQUEUE_CMD =
      "squeue --me --states=all -O \"JobID:|,State:|,exit_code:|\"";

  private static final String SQUEUE_HEADER = "JOBID|STATE|EXIT_CODE|";
  private static final int SQUEUE_COLUMNS = 3;
  private static final int SQUEUE_COLUMN_JOB_ID = 0;
  private static final int SQUEUE_COLUMN_STATE = 1;
  // private static final int SQUEUE_COLUMN_EXIT_CODE = 2;

  private static final String SACCT_CMD =
      "sacct -p --format JobId,State,ExitCode,TotalCPU,MaxRSS,Elapsed,NodeList -j ";

  // sacct -p --units=K --format JobId,State,ExitCode,TotalCPU,MaxRSS,NodeList,Elapsed -j 9332
  // JobID|State|ExitCode|TotalCPU|MaxRSS|NodeList|Elapsed|
  // 9332|COMPLETED|0:0|00:00.004||hl-codon-100-01|00:00:05|
  // 9332.batch|COMPLETED|0:0|00:00.003|132K|hl-codon-100-01|00:00:05|
  // 9332.extern|COMPLETED|0:0|00:00:00|0|hl-codon-100-01|00:00:05|
  private static final String SACCT_HEADER =
      "JobID|State|ExitCode|TotalCPU|MaxRSS|Elapsed|NodeList|";

  private static final int SACCT_COLUMNS = 7;
  private static final int SACCT_COLUMN_JOB_ID = 0;
  private static final int SACCT_COLUMN_STATE = 1;
  private static final int SACCT_COLUMN_EXIT_CODE = 2;
  private static final int SACCT_COLUMN_CPU_TIME = 3;
  private static final int SACCT_COLUMN_MAX_MEMORY = 4;
  private static final int SACCT_COLUMN_ELAPSED = 5;
  private static final int SACCT_COLUMN_HOST = 6;

  /** State, exit code and elapsed time are extracted from the output line without step. */
  private static final String SACCT_NO_STEP = "";

  /** Cpu time, max memory and host are extracted from the output line with batch step. */
  private static final String SACCT_BATCH_STEP = "batch";

  private static final String JOB_STATE_COMPLETED = "COMPLETED";
  private static final String JOB_STATE_TIMEOUT = "TIMEOUT";
  private static final List<String> JOB_STATE_FAILED =
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
          JOB_STATE_TIMEOUT);

  @Override
  public DescribeJobsResults<SlurmRequestContext> pollJobs(
      SlurmExecutorContext executorContext,
      DescribeJobsPollRequests<SlurmRequestContext> requests) {

    StageExecutorResult result = executorContext.cmdRunner().execute(SQUEUE_CMD);
    if (result.isError()) {
      throw new PipeliteException("Failed to poll SLURM jobs using squeue: " + result.stageLog());
    }

    String str = result.stageLog();
    return extractJobResultsUsingSqueue(executorContext, requests, str);
  }

  public static DescribeJobsResults<SlurmRequestContext> extractJobResultsUsingSqueue(
      SlurmExecutorContext executorContext,
      DescribeJobsPollRequests<SlurmRequestContext> requests,
      String str) {
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
        DescribeJobsResult<SlurmRequestContext> result =
            extractJobResultUsingSqueue(executorContext, requests, line);
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

  public static DescribeJobsResult<SlurmRequestContext> extractJobResultUsingSqueue(
      SlurmExecutorContext executorContext,
      DescribeJobsPollRequests<SlurmRequestContext> requests,
      String line) {
    String[] column = line.trim().split("\\|");
    if (column.length != SQUEUE_COLUMNS) {
      throw new PipeliteException("Unexpected SLURM squeue output line: " + line);
    }

    String jobId = column[SQUEUE_COLUMN_JOB_ID];

    if (requests.request(jobId) == null) {
      // Result is for an unrelated job.
      return null;
    }

    DescribeJobsResult.Builder resultBuilder = DescribeJobsResult.builder(requests, jobId);

    String state = column[SQUEUE_COLUMN_STATE];
    if (JOB_STATE_COMPLETED.equals(state)) {
      resultBuilder.success();
    } else if (JOB_STATE_FAILED.contains(state)) {
      if (JOB_STATE_TIMEOUT.equals(state)) {
        resultBuilder.timeoutError();
      } else {
        // TODO: SLURM exit code is not correctly reported by squeue.
        // https://bugs.schedmd.com/show_bug.cgi?id=15462
        // String exitCode = column[SQUEUE_COLUMN_EXIT_CODE];
        // result.executionError(exitCode);
        resultBuilder.executionError();
      }
    } else {
      resultBuilder.active();
    }

    if (resultBuilder.isCompleted()) {
      extractJobResultUsingSacct(executorContext, resultBuilder);
    }

    return resultBuilder.build();
  }

  public static DescribeJobsResult<SlurmRequestContext> extractJobResultUsingSacct(
      SlurmExecutorContext executorContext, DescribeJobsResult.Builder resultBuilder) {
    StageExecutorResult result =
        executorContext.cmdRunner().execute(SACCT_CMD + resultBuilder.jobId());
    if (result.isError()) {
      throw new PipeliteException(
          "Failed to get SLURM job status using sacct: " + result.stageLog());
    }

    String str = result.stageLog();

    int lineNumber = 0;
    for (String line : str.split("\\r?\\n")) {
      lineNumber++;
      if (lineNumber == 1) {
        if (!SACCT_HEADER.equals(line)) {
          throw new PipeliteException("Unexpected SLURM sacct header line: " + line);
        }
      } else {
        extractJobResultUsingSacct(resultBuilder, line);
      }
    }

    return resultBuilder.build();
  }

  public static void extractJobResultUsingSacct(
      DescribeJobsResult.Builder resultBuilder, String line) {
    String[] column = line.trim().split("\\|");
    if (column.length != SACCT_COLUMNS) {
      throw new PipeliteException("Unexpected SLURM sacct output line: " + line);
    }
    String jobIdWithStep = column[SACCT_COLUMN_JOB_ID];
    String jobId = extractSacctJobId(jobIdWithStep);

    if (!resultBuilder.jobId().equals(jobId)) {
      throw new PipeliteException("Unexpected SLURM sacct job id: " + jobId);
    }

    String step = extractSacctStep(jobIdWithStep);
    if (step.equals(SACCT_NO_STEP)) {
      String state = column[SACCT_COLUMN_STATE];
      if (JOB_STATE_COMPLETED.equals(state)) {
        resultBuilder.success();
      } else if (JOB_STATE_FAILED.contains(state)) {
        if (JOB_STATE_TIMEOUT.equals(state)) {
          resultBuilder.timeoutError();
        } else {
          String exitCodeWithSignal = column[SACCT_COLUMN_EXIT_CODE];
          String exitCode = extractSacctExitCode(exitCodeWithSignal);
          resultBuilder.executionError(exitCode);
        }
      } else {
        resultBuilder.active();
      }
      if (resultBuilder.isCompleted()) {
        resultBuilder.attribute(
            StageExecutorResultAttribute.ELAPSED_TIME, column[SACCT_COLUMN_ELAPSED]);
      }
    }

    if (step.equals(SACCT_BATCH_STEP)) {
      resultBuilder.attribute(StageExecutorResultAttribute.EXEC_HOST, column[SACCT_COLUMN_HOST]);
      resultBuilder.attribute(StageExecutorResultAttribute.CPU_TIME, column[SACCT_COLUMN_CPU_TIME]);
      resultBuilder.attribute(
          StageExecutorResultAttribute.MAX_MEM, column[SACCT_COLUMN_MAX_MEMORY]);
    }
  }

  public static String extractSacctJobId(String jobIdWithStep) {
    return jobIdWithStep.replaceAll("\\..*$", "");
  }

  public static String extractSacctStep(String jobIdWithStep) {
    if (jobIdWithStep.contains(".")) {
      try {
        return jobIdWithStep.split("\\.")[1];
      } catch (Exception ex) {
        throw new PipeliteException("Unexpected sacct job id with step: " + jobIdWithStep);
      }
    } else {
      return "";
    }
  }

  public static String extractSacctExitCode(String exitCodeWithSignal) {
    try {
      return exitCodeWithSignal.split(":")[0];
    } catch (Exception ex) {
      throw new PipeliteException("Unexpected sacct exit code with signal: " + exitCodeWithSignal);
    }
  }
}
