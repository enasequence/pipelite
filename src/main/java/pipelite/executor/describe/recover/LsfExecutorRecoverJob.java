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
  private static final Pattern JOB_RECOVERY_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");

  @Override
  public DescribeJobsResult<LsfRequestContext> recoverJob(
      LsfExecutorContext executorContext, LsfRequestContext request) {
    String outFile = request.getOutFile();
    log.atWarning().log(
        "Recovering LSF job " + request.getJobId() + " result from output file: " + outFile);
    String str = readOutFile(executorContext.cmdRunner(), outFile, JOB_RECOVERY_LINES);
    return extractJobResult(request, str);
  }

  /** Extract job result from the output file. Returns null if the result could not be extracted. */
  public static DescribeJobsResult<LsfRequestContext> extractJobResult(
      LsfRequestContext request, String str) {
    if (str == null) {
      return null;
    }

    DescribeJobsResult.Builder<LsfRequestContext> result = DescribeJobsResult.builder(request);

    if (str.contains("Done successfully") // bhist -f result
        || str.contains("Successfully completed") // output file result
    ) {
      return result.success().build();
    }

    Integer exitCode = extractExitCode(str);
    if (exitCode != null) {
      return result.executionError(exitCode).build();
    }

    return null;
  }

  /**
   * Extracts exit code from the output file. Returns null if the exit code could not be extracted.
   */
  public static Integer extractExitCode(String str) {
    try {
      Matcher m = JOB_RECOVERY_EXIT_CODE_PATTERN.matcher(str);
      return m.find() ? Integer.valueOf(m.group(1)) : null;
    } catch (Exception ex) {
      return null;
    }
  }
}
