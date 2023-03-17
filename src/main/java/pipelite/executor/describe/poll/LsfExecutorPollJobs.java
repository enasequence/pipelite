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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.executor.describe.DescribeJobsRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

@Component
@Flogger
public class LsfExecutorPollJobs implements PollJobs<LsfExecutorContext, LsfRequestContext> {
  private static final String BJOBS_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host exit_reason delimiter='|'\" -noheader ";

  private static final Pattern BJOBS_LOST_JOB_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");

  private static final String BJOBS_EXIT_REASON_TIMEOUT =
      "TERM_RUNLIMIT: job killed after reaching LSF run time limit";

  private static final String BJOBS_STATUS_DONE = "DONE";
  private static final String BJOBS_STATUS_EXIT = "EXIT";

  private static final int BJOBS_COLUMNS = 8;
  private static final int BJOBS_COLUMN_JOB_ID = 0;
  private static final int BJOBS_COLUMN_STATUS = 1;
  private static final int BJOBS_COLUMN_EXIT_CODE = 2;
  private static final int BJOBS_COLUMN_CPU_TIME = 3;
  private static final int BJOBS_COLUMN_MAX_MEM = 4;
  private static final int BJOBS_COLUMN_AVG_MEM = 5;
  private static final int BJOBS_COLUMN_HOST = 6;
  private static final int BJOBS_COLUMN_EXIT_REASON = 7;

  @Override
  public DescribeJobsResults<LsfRequestContext> pollJobs(
      LsfExecutorContext executorContext, DescribeJobsRequests<LsfRequestContext> requests) {
    // Ignore exit code as bjobs returns 255 if some jobs are not found.
    StageExecutorResult result =
        executorContext.cmdRunner().execute(BJOBS_CMD + String.join(" ", requests.jobIds()));
    if (result.isError()) {
      log.atSevere().log("Failed to poll LSF jobs using bjobs: " + result.stageLog());
      return new DescribeJobsResults<>();
    }
    return extractJobResults(requests, result.stdOut());
  }

  public static DescribeJobsResults<LsfRequestContext> extractJobResults(
      DescribeJobsRequests<LsfRequestContext> requests, String str) {
    DescribeJobsResults<LsfRequestContext> results = new DescribeJobsResults<>();
    for (String line : str.split("\\r?\\n")) {
      DescribeJobsResult<LsfRequestContext> result = extractJobResult(requests, line);
      if (result != null) {
        results.add(result);
      }
    }
    return results;
  }

  public static DescribeJobsResult<LsfRequestContext> extractJobResult(
      DescribeJobsRequests<LsfRequestContext> requests, String line) {
    DescribeJobsResult<LsfRequestContext> result = extractLostJobResult(requests, line);
    if (result == null) {
      result = extractFoundJobResult(requests, line);
    }
    return result;
  }

  public static DescribeJobsResult<LsfRequestContext> extractLostJobResult(
      DescribeJobsRequests<LsfRequestContext> requests, String line) {
    try {
      Matcher m = BJOBS_LOST_JOB_PATTERN.matcher(line);
      if (m.find()) {
        // The job has been lost.
        String jobId = m.group(1);
        return DescribeJobsResult.builder(requests, jobId).lostError().build();
      }
      return null;
    } catch (Exception ex) {
      return null;
    }
  }

  public static DescribeJobsResult<LsfRequestContext> extractFoundJobResult(
      DescribeJobsRequests<LsfRequestContext> requests, String line) {
    String[] column = line.trim().split("\\|");
    if (column.length != BJOBS_COLUMNS) {
      log.atWarning().log("Unexpected LSF bjobs output line: " + line);
      return null;
    }

    String jobId = column[BJOBS_COLUMN_JOB_ID];

    DescribeJobsResult.Builder result = DescribeJobsResult.builder(requests, jobId);

    if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_DONE)) {
      result.success();
    } else if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_EXIT)) {
      if (column[BJOBS_COLUMN_EXIT_REASON].startsWith(BJOBS_EXIT_REASON_TIMEOUT)) {
        result.timeoutError();
      } else {
        String exitCode = column[BJOBS_COLUMN_EXIT_CODE];
        result.executionError(exitCode);
      }
    } else {
      result.active();
    }

    if (result.isCompleted()) {
      result.attribute(StageExecutorResultAttribute.EXEC_HOST, column[BJOBS_COLUMN_HOST]);
      result.attribute(StageExecutorResultAttribute.CPU_TIME, column[BJOBS_COLUMN_CPU_TIME]);
      result.attribute(StageExecutorResultAttribute.MAX_MEM, column[BJOBS_COLUMN_MAX_MEM]);
      result.attribute(StageExecutorResultAttribute.AVG_MEM, column[BJOBS_COLUMN_AVG_MEM]);
    }

    return result.build();
  }
}
