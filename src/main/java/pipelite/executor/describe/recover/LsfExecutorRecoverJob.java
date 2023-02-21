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
package pipelite.executor.describe.recover;

import static pipelite.executor.AsyncCmdExecutor.readOutFile;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.flogger.Flogger;
import org.springframework.stereotype.Component;
import pipelite.executor.describe.DescribeJobsResult;
import pipelite.executor.describe.context.executor.LsfExecutorContext;
import pipelite.executor.describe.context.request.LsfRequestContext;

@Component
@Flogger
public class LsfExecutorRecoverJob implements RecoverJob<LsfExecutorContext, LsfRequestContext> {

  private static final int JOB_RECOVERY_LINES = 1000;
  private static final String JOB_RECOVERY_LAST_LINE = "The output (if any) follows";

  private static final Pattern JOB_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");

  private static final String JOB_COMPLETED = "Successfully completed";
  private static final String JOB_DONE = "Done successfully"; // bhist -f result
  private static final String JOB_TIMEOUT =
      "TERM_RUNLIMIT: job killed after reaching LSF run time limit";

  @Override
  public DescribeJobsResult<LsfRequestContext> recoverJob(
      LsfExecutorContext executorContext, LsfRequestContext request) {
    String outFile = request.getOutFile();
    log.atInfo().log(
        "Recovering LSF job " + request.getJobId() + " result from output file: " + outFile);

    // Recover by reading lines from the end of the output file.
    try {
      String lines = readOutFile(executorContext.cmdRunner(), outFile, JOB_RECOVERY_LINES);
      return recoverJobFromOutFile(request, lines);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log(
          "Failed to recover LSF job " + request.getJobId() + " from output file: " + outFile);
      return DescribeJobsResult.builder(request).lostError().build();
    }
  }

  /** Remove job output lines. */
  public static String filterOutFile(String lines) {
    if (lines == null) {
      return null;
    }
    int i = lines.indexOf(JOB_RECOVERY_LAST_LINE);
    if (i > -1) {
      lines = lines.substring(0, i);
    }
    return lines;
  }

  /**
   * Extract job result from the output file. Return lost error if the output file does not exist or
   * can't be parsed.
   */
  public static DescribeJobsResult<LsfRequestContext> recoverJobFromOutFile(
      LsfRequestContext request, String lines) {
    lines = filterOutFile(lines);
    DescribeJobsResult.Builder<LsfRequestContext> result = DescribeJobsResult.builder(request);
    if (lines == null || lines.isEmpty()) {
      result.lostError();
    } else if (isRecoverJobSuccess(lines)) {
      result.success();
    } else if (isRecoverJobTimeoutError(lines)) {
      result.timeoutError();
    } else {
      Integer exitCode = recoverExitCode(lines);
      if (exitCode != null) {
        result.executionError(exitCode);
      } else {
        result.lostError();
      }
    }
    return result.build();
  }

  public static boolean isRecoverJobSuccess(String lines) {
    return lines.contains(JOB_COMPLETED) || lines.contains(JOB_DONE);
  }

  public static boolean isRecoverJobTimeoutError(String lines) {
    return lines.contains(JOB_TIMEOUT);
  }

  /**
   * Recover exit code from the output file. Returns null if the exit code could not be extracted.
   */
  public static Integer recoverExitCode(String lines) {
    try {
      Matcher m = JOB_EXIT_CODE_PATTERN.matcher(lines);
      return m.find() ? Integer.valueOf(m.group(1)) : null;
    } catch (Exception ex) {
      return null;
    }
  }
}
