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
import pipelite.executor.describe.DescribeJobsPollRequests;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.DescribeJobsResults;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

@Component
@Flogger
public class LsfExecutorPollJobs implements PollJobs<LsfExecutorContext, LsfRequestContext> {
  private static final String POLL_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";

  private static final Pattern POLL_JOB_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");

  private static final String POLL_JOB_STATUS_SUCCESS = "DONE";
  private static final String POLL_JOB_STATUS_ERROR = "EXIT";

  private static final int POLL_COLUMN_JOB_ID = 0;
  private static final int POLL_COLUMN_STATUS = 1;
  private static final int POLL_COLUMN_EXIT_CODE = 2;
  private static final int POLL_COLUMN_CPU_TIME = 3;
  private static final int POLL_COLUMN_MAX_MEM = 4;
  private static final int POLL_COLUMN_AVG_MEM = 5;
  private static final int POLL_COLUMN_HOST = 6;

  @Override
  public DescribeJobsResults<LsfRequestContext> pollJobs(
      LsfExecutorContext executorContext, DescribeJobsPollRequests<LsfRequestContext> requests) {
    // Ignore exit code as bjobs returns 255 if some jobs are not found.
    StageExecutorResult result =
        executorContext.cmdRunner().execute(POLL_CMD + String.join(" ", requests.jobIds));
    return extractJobResultsFromPollOutput(result.stageLog(), requests);
  }

  // TODO: move LSF stageExecutorResult creations in one place
  public static StageExecutorResult stageExecutorResultNotFound() {
    return null;
  }

  // TODO: move LSF stageExecutorResult creations in one place
  public static StageExecutorResult stageExecutorResultSuccess() {
    StageExecutorResult result = StageExecutorResult.success();
    result.attribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(0));
    return result;
  }

  // TODO: move LSF stageExecutorResult creations in one place
  public static StageExecutorResult stageExecutorResultError(String exitCode) {
    StageExecutorResult result = StageExecutorResult.executionError();
    result.attribute(StageExecutorResultAttribute.EXIT_CODE, exitCode);
    return result;
  }

  public static String extractNotFoundJobIdFromPollOutput(String str) {
    try {
      Matcher m = POLL_JOB_NOT_FOUND_PATTERN.matcher(str);
      if (m.find()) {
        return m.group(1);
      }
      return null;
    } catch (Exception ex) {
      return null;
    }
  }

  /** Extracts job results from bjobs output. */
  public static DescribeJobsResults<LsfRequestContext> extractJobResultsFromPollOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    DescribeJobsResults<LsfRequestContext> results = new DescribeJobsResults<>();
    for (String line : str.split("\\r?\\n")) {
      String notFoundJobId = extractNotFoundJobIdFromPollOutput(line);
      if (notFoundJobId != null) {
        log.atWarning().log("LSF job " + notFoundJobId + " was not found");
        DescribeJobsResult<LsfRequestContext> result =
            DescribeJobsResult.create(requests, notFoundJobId, stageExecutorResultNotFound());
        results.add(result);
      } else {
        DescribeJobsResult<LsfRequestContext> result =
            extractJobResultFromPollOutput(line, requests);
        if (result != null) {
          results.add(result);
        }
      }
    }
    return results;
  }

  /** Extracts job result from a single line of bjobs output. */
  public static DescribeJobsResult<LsfRequestContext> extractJobResultFromPollOutput(
      String str, DescribeJobsPollRequests<LsfRequestContext> requests) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      log.atWarning().log("Unexpected LSF jobs output line: " + str);
      return null;
    }

    StageExecutorResult result;
    if (column[POLL_COLUMN_STATUS].equals(POLL_JOB_STATUS_SUCCESS)) {
      result = stageExecutorResultSuccess();
    } else if (column[POLL_COLUMN_STATUS].equals(POLL_JOB_STATUS_ERROR)) {
      result = stageExecutorResultError(column[POLL_COLUMN_EXIT_CODE]);
    } else {
      result = StageExecutorResult.active();
    }

    String jobId = column[POLL_COLUMN_JOB_ID];
    result.attribute(StageExecutorResultAttribute.JOB_ID, jobId);

    if (result.isSuccess() || result.isError()) {
      result.attribute(StageExecutorResultAttribute.EXEC_HOST, column[POLL_COLUMN_HOST]);
      result.attribute(StageExecutorResultAttribute.CPU_TIME, column[POLL_COLUMN_CPU_TIME]);
      result.attribute(StageExecutorResultAttribute.MAX_MEM, column[POLL_COLUMN_MAX_MEM]);
      result.attribute(StageExecutorResultAttribute.AVG_MEM, column[POLL_COLUMN_AVG_MEM]);
    }

    return DescribeJobsResult.create(requests, jobId, result);
  }
}
