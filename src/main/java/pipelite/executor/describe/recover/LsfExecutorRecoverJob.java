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
    log.atWarning().log(
        "Recovering LSF job " + request.getJobId() + " result from output file: " + outFile);

    // The recovery is done by reading lines from the beginning of the output file.
    String lines = readOutFile(executorContext.cmdRunner(), outFile, JOB_RECOVERY_LINES);
    lines = getRecoveryLines(lines);
    return extractJobResult(request, lines);
  }

  public static String getRecoveryLines(String lines) {
    if (lines == null) {
      return null;
    }
    int i = lines.indexOf(JOB_RECOVERY_LAST_LINE);
    if (i > -1) {
      lines = lines.substring(0, i);
    }
    return lines;
  }

  /** Extract job result from the output file. Returns null if the result could not be extracted. */
  public static DescribeJobsResult<LsfRequestContext> extractJobResult(
      LsfRequestContext request, String lines) {
    if (lines == null) {
      return null;
    }

    DescribeJobsResult.Builder<LsfRequestContext> result = DescribeJobsResult.builder(request);

    if (extractSuccess(lines)) {
      return result.success().build();
    }

    if (extractTimeout(lines)) {
      return result.timeoutError().build();
    }

    Integer exitCode = extractExitCode(lines);
    if (exitCode != null) {
      return result.executionError(exitCode).build();
    }

    return null;
  }

  public static boolean extractSuccess(String lines) {
    return lines.contains(JOB_COMPLETED) || lines.contains(JOB_DONE);
  }

  public static boolean extractTimeout(String lines) {
    return lines.contains(JOB_TIMEOUT);
  }

  /**
   * Extracts exit code from the output file. Returns null if the exit code could not be extracted.
   */
  public static Integer extractExitCode(String lines) {
    try {
      Matcher m = JOB_EXIT_CODE_PATTERN.matcher(lines);
      return m.find() ? Integer.valueOf(m.group(1)) : null;
    } catch (Exception ex) {
      return null;
    }
  }
}
