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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.flogger.FluentLogger;
import java.io.File;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import pipelite.executor.describe.DescribeJobs;
import pipelite.executor.describe.cache.LsfDescribeJobsCache;
import pipelite.executor.task.RetryTask;
import pipelite.log.LogKey;
import pipelite.service.StageService;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.stage.parameters.SharedLsfExecutorParameters;
import pipelite.time.Time;

/** Executes a command using LSF. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner"})
public abstract class AbstractLsfExecutor<T extends SharedLsfExecutorParameters>
    extends AbstractAsyncExecutor<T, LsfDescribeJobsCache> implements JsonSerializableExecutor {

  private static final int JOB_RECOVERY_PARALLELISM = 10;
  private static final int JOB_RECOVERY_LOG_LINES = 1000;
  private static final Duration JOB_RECOVERY_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration JOB_RECOVERY_POLL_FREQUENCY = Duration.ofSeconds(5);

  private static final String BSUB_CMD = "bsub";
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
  private static final Pattern BHIST_EXIT_CODE_PATTERN =
      Pattern.compile("Exited with exit code (\\d+)");

  /**
   * The command to execute using LSF. Set during executor creation. Serialize in database to
   * continue execution after service restart.
   */
  private String cmd;

  /**
   * The file containing the concatenated stdout and stderr output of the stage execution. Set
   * during submit. Serialize in database to continue execution after service restart.
   */
  private String outFile;

  /** Set during poll. */
  @JsonIgnore private StageExecutorResult pollResult;

  /** Set during poll. */
  @JsonIgnore private ZonedDateTime pollTimeout;

  @Override
  protected LsfDescribeJobsCache initDescribeJobsCache(StageService stageService) {
    return stageService.getLsfDescribeJobsCache();
  }

  protected DescribeJobs<LsfDescribeJobsCache.RequestContext, LsfDescribeJobsCache.ExecutorContext>
      describeJobs() {
    return getDescribeJobsCache()
        .getDescribeJobs((AbstractLsfExecutor<SharedLsfExecutorParameters>) this);
  }

  protected static class JobResult {
    public String jobId;
    public StageExecutorResult result;
  }

  /**
   * Returns the submit command.
   *
   * @return the submit command.
   */
  public abstract String getSubmitCmd(StageExecutorRequest request);

  protected StringBuilder createSubmitCmdBuilder() {
    StringBuilder cmd = new StringBuilder();
    cmd.append(BSUB_CMD);

    String outDir = AbstractLsfExecutor.getOutDir(outFile);
    if (outDir != null) {
      addArgument(cmd, "-outdir");
      addArgument(cmd, outDir);
      addArgument(cmd, "-cwd");
      addArgument(cmd, outDir);
    }

    addArgument(cmd, "-oo");
    addArgument(cmd, AbstractLsfExecutor.getOutFileName(outFile));

    return cmd;
  }

  /**
   * Returns the command runner.
   *
   * @return the command runner.
   */
  protected CmdRunner getCmdRunner() {
    return CmdRunner.create(getExecutorParams());
  }

  @Override
  protected void prepareAsyncSubmit(StageExecutorRequest request) {
    // Reset to allow execution retry.
    outFile = null;
    pollResult = null;
    pollTimeout = null;
  }

  @Override
  protected StageExecutorResult submit(StageExecutorRequest request) {
    outFile = CmdExecutorParameters.getLogFile(request, getExecutorParams()).toString();
    StageExecutorResult submitResult =
        RetryTask.DEFAULT.execute(r -> getCmdRunner().execute(getSubmitCmd(request)));
    if (submitResult.isError()) {
      return submitResult;
    }

    setJobId(extractSubmittedJobIdFromBsubOutput(submitResult.getStageLog()));
    logContext(log.atInfo(), request).log("Submitted LSF job " + getJobId());
    submitResult.setSubmitted();
    return submitResult;
  }

  @Override
  protected StageExecutorResult poll(StageExecutorRequest request) {
    String jobId = getJobId();
    logContext(log.atFine(), request).log("Polling LSF job result " + jobId);

    if (pollResult == null) {
      StageExecutorResult result =
          describeJobs()
              .getResult(describeJobsRequestContext(), getExecutorParams().getPermanentErrors());
      if (result.isActive()) {
        return result;
      }
      pollResult = result;
      pollTimeout = ZonedDateTime.now().plus(getExecutorParams().getLogTimeout());
    }

    if (!isSaveLogFile(pollResult) || readOutFile(request)) {
      if (isDeleteLogFile(pollResult)) {
        logContext(log.atFine(), request).log("Deleting output file: " + outFile);
        getCmdRunner().deleteFile(Paths.get(outFile));
      }
      return pollResult;
    }
    return StageExecutorResult.active();
  }

  @Override
  public void terminate() {
    String jobId = getJobId();
    if (jobId == null) {
      return;
    }
    RetryTask.DEFAULT.execute(r -> bkill(getCmdRunner(), jobId));
    describeJobs().removeRequest(describeJobsRequestContext());
  }

  private static String bjobs(CmdRunner cmdRunner, List<String> jobIds) {
    String str = String.join(" ", jobIds);
    log.atFine().log("Checking LSF job results using bjobs: " + str);
    // Ignore exit code as bjobs returns 255 if some of the jobs are not found.
    StageExecutorResult result = cmdRunner.execute(BJOBS_CMD + str);
    return result.getStageLog();
  }

  private static String bhist(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Checking LSF job results using bhist: " + jobId);
    StageExecutorResult bhistResult = cmdRunner.execute(BHIST_CMD + jobId);
    return bhistResult.getStageLog();
  }

  private static StageExecutorResult bkill(CmdRunner cmdRunner, String jobId) {
    log.atWarning().log("Terminating LSF job using bkill: " + jobId);
    return cmdRunner.execute(BKILL_CMD + jobId);
  }

  public static Map<LsfDescribeJobsCache.RequestContext, StageExecutorResult> describeJobs(
      List<LsfDescribeJobsCache.RequestContext> requests,
      LsfDescribeJobsCache.ExecutorContext executorContext) {
    log.atFine().log("Describing LSF job results");

    // Create a map for job id -> LsfContextCache.Request.
    Map<String, LsfDescribeJobsCache.RequestContext> requestMap = new HashMap<>();
    requests.forEach(request -> requestMap.put(request.getJobId(), request));

    // Get job results using bjobs.
    List<String> jobIds = requests.stream().map(r -> r.getJobId()).collect(Collectors.toList());
    String str = bjobs(executorContext.getCmdRunner(), jobIds);
    List<JobResult> jobResults = extractResultsFromBjobsOutput(str);

    // Recover job results using bhist and out file.
    if (jobResults.stream().anyMatch(r -> r.jobId != null && r.result == null)) {
      recoverJobs(executorContext.getCmdRunner(), requestMap, jobResults);
    }

    Map<LsfDescribeJobsCache.RequestContext, StageExecutorResult> result = new HashMap<>();
    jobResults.stream()
        .filter(r -> r.jobId != null && r.result != null)
        .forEach(r -> result.put(requestMap.get(r.jobId), r.result));
    return result;
  }

  private LsfDescribeJobsCache.RequestContext describeJobsRequestContext() {
    return new LsfDescribeJobsCache.RequestContext(getJobId(), outFile);
  }

  /**
   * Attempt to recover missing job results. This will only ever be done once and if it fails the
   * job is considered failed.
   */
  private static void recoverJobs(
      CmdRunner cmdRunner,
      Map<String, LsfDescribeJobsCache.RequestContext> requestMap,
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
      CmdRunner cmdRunner,
      JobResult jobResult,
      Map<String, LsfDescribeJobsCache.RequestContext> requestMap) {
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

  /**
   * Read the output file at end job execution.
   *
   * @param request the stage executor request
   * @return true if the output file was available or if the LSF output file poll timeout was
   *     exceeded
   */
  protected boolean readOutFile(StageExecutorRequest request) {
    logContext(log.atFine(), request).log("Reading LSF output file: %s", outFile);

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
    if (!getCmdRunner().fileExists(Paths.get(outFile))) {
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
      return cmdRunner.readFile(Paths.get(outFile), executorParams.getLogLines());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read LSF out file: %s", outFile);
      return null;
    }
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
      Matcher m = BHIST_EXIT_CODE_PATTERN.matcher(str);
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
    log.atWarning().log("Checking LSF job result from output file: " + outFile);

    // Extract the job execution result from the out file. The format
    // is the same as in the bhist result.
    CmdExecutorParameters executorParams =
        CmdExecutorParameters.builder().logLines(JOB_RECOVERY_LOG_LINES).build();
    String str = readOutFile(cmdRunner, outFile, executorParams);
    return extractResultFromBhistOutputOrOutFile(str);
  }

  protected static void addArgument(StringBuilder cmd, String argument) {
    cmd.append(" ");
    cmd.append(argument);
  }

  public void setOutFile(String outFile) {
    this.outFile = outFile;
  }

  /**
   * Returns the output file.
   *
   * @return the output file
   */
  public String getOutFile() {
    return outFile;
  }

  public static String getOutDir(String outFile) {
    File file = new File(outFile);
    return file.getParent();
  }

  public static String getOutFileName(String outFile) {
    File file = new File(outFile);
    return file.getName();
  }

  protected FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
