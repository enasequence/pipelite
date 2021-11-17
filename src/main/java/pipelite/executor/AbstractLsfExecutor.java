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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.flogger.FluentLogger;
import com.google.common.primitives.Ints;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.exception.PipeliteException;
import pipelite.exception.PipeliteTimeoutException;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.context.LsfContextCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.ExecutorParameters;
import pipelite.stage.parameters.SharedLsfExecutorParameters;
import pipelite.time.Time;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner", "pollResult", "pollTimeout"})
public abstract class AbstractLsfExecutor<T extends SharedLsfExecutorParameters>
    extends AbstractExecutor<T> implements JsonSerializableExecutor {

  private static final int JOB_RECOVERY_PARALLELISM = 10;
  private static final int JOB_RECOVERY_LOG_BYTES = 5 * 1024;
  private static final Duration JOB_RECOVERY_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration JOB_RECOVERY_POLL_FREQUENCY = Duration.ofSeconds(5);

  /** The command to be executed. */
  private String cmd;

  /** The JSF job id. */
  private String jobId;

  /** The LSF output file. */
  private String outFile;

  private StageExecutorResult pollResult;

  /* Log file timeout. */
  private ZonedDateTime pollTimeout;

  protected LsfContextCache.Context getSharedContext() {
    return getExecutorContextCache()
        .lsf
        .getContext((AbstractLsfExecutor<SharedLsfExecutorParameters>) this);
  }

  protected static class JobResult {
    public String jobId;
    public StageExecutorResult result;
  }

  private static final String OUT_FILE_SUFFIX = ".out";

  private static final String MKDIR_CMD = "mkdir -p ";

  protected static final String BSUB_CMD = "bsub";

  private static final String BKILL_CMD = "bkill ";

  private static final String BJOBS_CMD =
      "bjobs -o \"jobid stat exit_code cpu_used max_mem avg_mem exec_host delimiter='|'\" -noheader ";
  private static final Pattern BSUB_JOB_ID_SUBMITTED_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is submitted");
  private static final Pattern BJOBS_JOB_ID_NOT_FOUND_PATTERN =
      Pattern.compile("Job <(\\d+)\\> is not found");
  private static final String BJOBS_STATUS_DONE = "DONE";
  private static final String BJOBS_STATUS_EXIT = "EXIT";
  private static final int BJOBS_COLUMN_JOB_ID = 0;
  private static final int BJOBS_COLUMN_STATUS = 1;
  private static final int BJOBS_COLUMN_EXIT_CODE = 2;
  private static final int BJOBS_COLUMN_CPU_TIME = 3;
  private static final int BJOBS_COLUMN_MAX_MEM = 4;
  private static final int BJOBS_COLUMN_AVG_MEM = 5;
  private static final int BJOBS_COLUMN_HOST = 6;

  private static final String BHIST_CMD = "bhist -l ";

  /** Exit code pattern from bhist -f or output file result. */
  private static final Pattern LSF_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");

  /**
   * Returns the submit command.
   *
   * @return the submit command.
   */
  public abstract String getSubmitCmd(StageExecutorRequest request);

  /**
   * Returns the command runner.
   *
   * @return the command runner.
   */
  public CmdRunner getCmdRunner() {
    return CmdRunner.create(getExecutorParams());
  }

  protected void beforeSubmit(StageExecutorRequest request) {}

  @Override
  public StageExecutorResult execute(StageExecutorRequest request) {
    if (jobId == null) {
      StageExecutorResult result = createWorkDir(request);
      if (result.isError()) {
        return result;
      }
      beforeSubmit(request);
      outFile = getOutFile(request, getExecutorParams());
      return submit(request);
    }
    return poll(request);
  }

  @Override
  public void terminate() {
    RetryTask.DEFAULT.execute(r -> bkill(getCmdRunner(), jobId));
    // Remove request because the execution is being terminated.
    getSharedContext().describeJobs.removeRequest(getDescribeJobsRequest());
  }

  private StageExecutorResult submit(StageExecutorRequest request) {
    StageExecutorResult submitResult =
        RetryTask.DEFAULT.execute(r -> getCmdRunner().execute(getSubmitCmd(request)));
    if (submitResult.isError()) {
      return submitResult;
    }

    jobId = extractSubmittedJobIdFromBsubOutput(submitResult.getStageLog());

    logContext(log.atInfo(), request).log("LSF submit job id: %s", jobId);
    submitResult.setSubmitted();
    return submitResult;
  }

  private StageExecutorResult poll(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Checking LSF job result using bjobs.");

    if (pollResult == null) {
      Optional<StageExecutorResult> result =
          getSharedContext().describeJobs.getResult(getDescribeJobsRequest());
      if (result == null || !result.isPresent()) {
        return StageExecutorResult.active();
      }
      pollResult = result.get();
      if (!pollResult.isSuccess() && !pollResult.isError()) {
        throw new PipeliteException(
            "Unexpected executor state: " + pollResult.getExecutorState().name());
      }
      setPermanentError();
      pollTimeout = ZonedDateTime.now().plus(getExecutorParams().getLogTimeout());
    }

    if (!getExecutorParams().isSaveLog() || readOutFile(request)) {
      return pollResult;
    }
    return StageExecutorResult.active();
  }

  private void setPermanentError() {
    T executorParams = getExecutorParams();
    if (executorParams.getPermanentErrors() != null) {
      String exitCode = pollResult.getAttribute(StageExecutorResultAttribute.EXIT_CODE);
      if (exitCode == null || Ints.tryParse(exitCode) == null) {
        throw new PipeliteException("Missing or invalid LSF exit code: " + exitCode);
      }
      if (executorParams.getPermanentErrors().contains(Ints.tryParse(exitCode))) {
        pollResult.setPermanentError();
      }
    }
  }

  private static String bjobs(CmdRunner cmdRunner, List<String> jobIds) {
    String str = String.join(" ", jobIds);
    log.atFine().log("Checking LSF job results using bjobs: " + str);
    // Ignore exit code as bjobs returns 255 if some of the jobs are not found.
    StageExecutorResult result = cmdRunner.execute(BJOBS_CMD + str);
    return result.getStageLog();
  }

  private static String bhist(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Checking LSF job result using bhist: " + jobId);
    StageExecutorResult bhistResult = cmdRunner.execute(BHIST_CMD + jobId);
    return bhistResult.getStageLog();
  }

  private static StageExecutorResult bkill(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Terminating LSF job using bkill: " + jobId);
    return cmdRunner.execute(BKILL_CMD + jobId);
  }

  public static Map<LsfContextCache.Request, StageExecutorResult> describeJobs(
      List<LsfContextCache.Request> requests, CmdRunner cmdRunner) {
    log.atFine().log("Checking LSF job results.");

    // Create a map for job id -> LsfContextCache.Request.
    Map<String, LsfContextCache.Request> requestMap = new HashMap<>();
    requests.forEach(request -> requestMap.put(request.getJobId(), request));

    // Get job results using bjobs.
    List<String> jobIds = requests.stream().map(r -> r.getJobId()).collect(Collectors.toList());
    String str = bjobs(cmdRunner, jobIds);
    List<JobResult> jobResults = extractResultsFromBjobsOutput(str);

    // Recover job results using bhist and out file.
    if (jobResults.stream().anyMatch(r -> r.jobId != null && r.result == null)) {
      recoverJobs(cmdRunner, requestMap, jobResults);
    }

    Map<LsfContextCache.Request, StageExecutorResult> result = new HashMap<>();
    jobResults.stream()
        .filter(r -> r.jobId != null && r.result != null)
        .forEach(r -> result.put(requestMap.get(r.jobId), r.result));
    return result;
  }

  private LsfContextCache.Request getDescribeJobsRequest() {
    return new LsfContextCache.Request(jobId, outFile);
  }

  /**
   * Attempt to recover missing job results. This will only ever be done once and if it fails the
   * job is considered failed as well.
   */
  private static void recoverJobs(
      CmdRunner cmdRunner,
      Map<String, LsfContextCache.Request> requestMap,
      List<JobResult> jobResults) {
    log.atInfo().log("Recovering LSF job results.");

    AtomicInteger remainingCount = new AtomicInteger();
    AtomicInteger attemptedCount = new AtomicInteger();
    AtomicInteger recoveredCount = new AtomicInteger();
    ZonedDateTime start = ZonedDateTime.now();
    ZonedDateTime until = start.plus(JOB_RECOVERY_TIMEOUT);
    ExecutorService executorService = Executors.newFixedThreadPool(JOB_RECOVERY_PARALLELISM);
    try {
      jobResults.stream()
          .filter(r -> r.jobId != null && r.result == null)
          .forEach(
              r -> {
                attemptedCount.incrementAndGet();
                remainingCount.incrementAndGet();
                executorService.submit(
                    () -> {
                      try {
                        // Attempt to recover missing job result using bhist.
                        if (recoverJobUsingBhist(cmdRunner, r)) {
                          recoveredCount.incrementAndGet();
                        } else {
                          // Attempt to recover missing job result using output file.
                          if (recoverJobUsingOutFile(cmdRunner, r, requestMap)) {
                            recoveredCount.incrementAndGet();
                          }
                        }
                      } finally {
                        remainingCount.decrementAndGet();
                      }
                    });
              });

      try {
        while (remainingCount.get() > 0) {
          Time.waitUntil(JOB_RECOVERY_POLL_FREQUENCY, until);
        }
      } catch (PipeliteTimeoutException ex) {
        log.atWarning().log("LSF job recovery timeout exceeded.");
      }
    } finally {
      executorService.shutdownNow();
    }

    log.atInfo().log(
        "Finished recovering LSF job results in "
            + (Duration.between(ZonedDateTime.now(), start).abs().toMillis() / 1000)
            + " seconds. Recovered "
            + recoveredCount.get()
            + " out of "
            + attemptedCount.get()
            + " jobs.");
  }

  private static boolean recoverJobUsingBhist(CmdRunner cmdRunner, JobResult jobResult) {
    StageExecutorResult executorResult =
        extractResultFromBhistOutputOrOutFile(bhist(cmdRunner, jobResult.jobId));

    if (executorResult != null) {
      log.atInfo().log("Recovered job result using LSF bhist for job: " + jobResult.jobId);
      jobResult.result = executorResult;
      return true;
    } else {
      log.atWarning().log(
          "Could not recover job result using LSF bhist for job: " + jobResult.jobId);
      return false;
    }
  }

  private static boolean recoverJobUsingOutFile(
      CmdRunner cmdRunner, JobResult jobResult, Map<String, LsfContextCache.Request> requestMap) {
    String outFile = requestMap.get(jobResult.jobId).getOutFile();
    StageExecutorResult executorResult = extractResultFromOutFile(cmdRunner, outFile);
    if (executorResult != null) {
      log.atInfo().log(
          "Recovered job result from LSF output file " + outFile + " for job: " + jobResult.jobId);
      jobResult.result = executorResult;
      return true;
    } else {
      log.atWarning().log(
          "Could not recover job result from LSF output file "
              + outFile
              + "  for job: "
              + jobResult.jobId);
      log.atWarning().log(
          "Considering LSF job failed as could not extract result for job: " + jobResult.jobId);
      jobResult.result = StageExecutorResult.error();
      return false;
    }
  }

  private StageExecutorResult createWorkDir(StageExecutorRequest request) {
    return RetryTask.DEFAULT.execute(
        r -> getCmdRunner().execute(MKDIR_CMD + getWorkDir(request, getExecutorParams())));
  }

  /**
   * Read the output file at end job execution.
   *
   * @param request the stage executor request
   * @return true if the output file was available or if the LSF output file poll timeout was
   *     exceeded
   */
  protected boolean readOutFile(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Reading LSF out file: %s", outFile);

    // The LSF output file may not be immediately available after the job execution finishes.

    if (pollTimeout.isBefore(ZonedDateTime.now())) {
      // LSF output file poll timeout was exceeded
      pollResult.setStageLog(
          "Missing LSF output file. Not available within "
              + (getExecutorParams().getLogTimeout().toMillis() / 1000)
              + " seconds.");
      return true;
    }

    // Check if the out file exists.
    if (!outFileExists(outFile)) {
      // LSF output file is not available yet
      return false;
    }

    // LSF output file is available
    pollResult.setStageLog(readOutFile(getCmdRunner(), outFile, getExecutorParams()));
    return true;
  }

  private static String readOutFile(
      CmdRunner cmdRunner, String outFile, CmdExecutorParameters executorParams) {
    try {
      StageExecutorResult result = writeFileToStdout(cmdRunner, outFile, executorParams);
      return result.getStageLog();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read LSF out file: %s", outFile);
      return null;
    }
  }

  private boolean outFileExists(String outFile) {
    // Check if the out file exists. The file may not be immediately available after the job
    // execution finishes.
    StageExecutorResult result =
        RetryTask.DEFAULT.execute(r -> getCmdRunner().execute("sh -c 'test -f " + outFile + "'"));
    return result.isSuccess();
  }

  public static StageExecutorResult writeFileToStdout(
      CmdRunner cmdRunner, String stdoutFile, CmdExecutorParameters executorParams) {
    // Execute through sh required by LocalRunner to direct output to stdout/err.
    int logBytes =
        (executorParams.getLogBytes() > 0)
            ? executorParams.getLogBytes()
            : CmdExecutorParameters.DEFAULT_LOG_BYTES;
    return RetryTask.DEFAULT.execute(
        r -> cmdRunner.execute("sh -c 'tail -c " + logBytes + " " + stdoutFile + "'"));
  }

  public static String extractSubmittedJobIdFromBsubOutput(String str) {
    try {
      Matcher m = BSUB_JOB_ID_SUBMITTED_PATTERN.matcher(str);
      if (!m.find()) {
        throw new PipeliteException("No LSF submit job id.");
      }
      return m.group(1);
    } catch (Exception ex) {
      throw new PipeliteException("No LSF submit job id.");
    }
  }

  public static String extractNotFoundJobIdFromBjobsOutput(String str) {
    try {
      Matcher m = BJOBS_JOB_ID_NOT_FOUND_PATTERN.matcher(str);
      return m.find() ? m.group(1) : null;
    } catch (Exception ex) {
      return null;
    }
  }

  /**
   * Extracts exit code from bhist output or out file. Returns null if the exit code could not be
   * extracted.
   */
  public static Integer extractExitCodeFromBhistOutputOrOutFile(String str) {
    try {
      Matcher m = LSF_EXIT_CODE_PATTERN.matcher(str);
      return m.find() ? Integer.valueOf(m.group(1)) : null;
    } catch (Exception ex) {
      return null;
    }
  }

  /** Extracts job results from bjobs output. */
  public static List<JobResult> extractResultsFromBjobsOutput(String str) {
    List<JobResult> results = new ArrayList<>();
    for (String line : str.split("\\r?\\n")) {
      String jobId = extractNotFoundJobIdFromBjobsOutput(line);
      if (jobId != null) {
        log.atWarning().log("LSF bsubs job not found: " + jobId);
        JobResult jobResult = new JobResult();
        jobResult.jobId = jobId;
        results.add(jobResult);
      } else {
        JobResult jobResult = extractResultFromBjobsOutput(line);
        if (jobResult != null) {
          results.add(jobResult);
        }
      }
    }
    return results;
  }

  /** Extracts job result from a single line of bjobs output. */
  public static JobResult extractResultFromBjobsOutput(String str) {
    String[] column = str.split("\\|");
    if (column.length != 7) {
      log.atWarning().log("Unexpected bjobs output line: " + str);
      return null;
    }

    StageExecutorResult result;
    if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_DONE)) {
      result = StageExecutorResult.success();
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(0));
    } else if (column[BJOBS_COLUMN_STATUS].equals(BJOBS_STATUS_EXIT)) {
      result = StageExecutorResult.error();
      result.addAttribute(
          StageExecutorResultAttribute.EXIT_CODE, String.valueOf(column[BJOBS_COLUMN_EXIT_CODE]));
    } else {
      result = StageExecutorResult.active();
    }

    String jobId = column[BJOBS_COLUMN_JOB_ID];
    result.addAttribute(StageExecutorResultAttribute.JOB_ID, jobId);

    if (result.isSuccess() || result.isError()) {
      result.addAttribute(StageExecutorResultAttribute.EXEC_HOST, column[BJOBS_COLUMN_HOST]);
      result.addAttribute(StageExecutorResultAttribute.CPU_TIME, column[BJOBS_COLUMN_CPU_TIME]);
      result.addAttribute(StageExecutorResultAttribute.MAX_MEM, column[BJOBS_COLUMN_MAX_MEM]);
      result.addAttribute(StageExecutorResultAttribute.AVG_MEM, column[BJOBS_COLUMN_AVG_MEM]);
    }

    JobResult jobResult = new JobResult();
    jobResult.jobId = jobId;
    jobResult.result = result;
    return jobResult;
  }

  /**
   * Extracts stage execution result from bhist output or out file. Returns null if the result could
   * not be extracted.
   */
  public static StageExecutorResult extractResultFromBhistOutputOrOutFile(String str) {
    if (str == null) {
      return null;
    }

    if (str.contains("Done successfully") // bhist -f result
        || str.contains("Successfully completed") // output file result
    ) {
      StageExecutorResult result = StageExecutorResult.success();
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, "0");
      return result;
    }

    Integer exitCode = extractExitCodeFromBhistOutputOrOutFile(str);
    if (exitCode != null) {
      StageExecutorResult result = StageExecutorResult.error();
      result.addAttribute(StageExecutorResultAttribute.EXIT_CODE, String.valueOf(exitCode));
      return result;
    }
    return null;
  }

  /**
   * Extracts stage execution result from out file. Returns null if the result could not be
   * extracted.
   */
  public static StageExecutorResult extractResultFromOutFile(CmdRunner cmdRunner, String outFile) {
    log.atWarning().log("Checking LSF job result from out file: " + outFile);

    // Extract the job execution result from the out file. The format
    // is the same as in the bhist result.
    CmdExecutorParameters executorParams =
        CmdExecutorParameters.builder().logBytes(JOB_RECOVERY_LOG_BYTES).build();
    String str = readOutFile(cmdRunner, outFile, executorParams);
    return extractResultFromBhistOutputOrOutFile(str);
  }

  protected static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }

  public static <T extends CmdExecutorParameters> Path getWorkDir(
      StageExecutorRequest request, T params) {
    Path path;
    if (params != null && params.getWorkDir() != null) {
      path =
          ExecutorParameters.validatePath(params.getWorkDir(), "workDir")
              .resolve("pipelite")
              .normalize();
    } else {
      path = Paths.get("pipelite");
    }
    return path.resolve(request.getPipelineName()).resolve(request.getProcessId());
  }

  public void setOutFile(String outFile) {
    this.outFile = outFile;
  }

  /**
   * Returns the output file in the working directory for stdout and stderr.
   *
   * @param request the pipeline name
   * @param params the execution parameters
   * @return the output file path
   */
  public static <T extends CmdExecutorParameters> String getOutFile(
      StageExecutorRequest request, T params) {
    return getWorkDir(request, params)
        .resolve(request.getStage().getStageName() + OUT_FILE_SUFFIX)
        .toString();
  }

  /**
   * Returns the output file.
   *
   * @return the output file
   */
  public String getOutFile() {
    return outFile;
  }

  protected FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
