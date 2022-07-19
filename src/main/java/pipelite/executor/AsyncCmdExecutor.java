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
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.describe.context.DefaultExecutorContext;
import pipelite.executor.describe.context.DefaultRequestContext;
import pipelite.log.LogKey;
import pipelite.service.PipeliteServices;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorRequest;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.AsyncCmdExecutorParameters;
import pipelite.stage.path.AsyncCmdLogFilePathResolver;

import java.nio.file.Paths;
import java.time.ZonedDateTime;

/** Executes a command asynchronously. */
@Flogger
@Getter
@Setter
@JsonIgnoreProperties({"cmdRunner"})
public abstract class AsyncCmdExecutor<
        T extends AsyncCmdExecutorParameters,
        RequestContext extends DefaultRequestContext,
        ExecutorContext extends DefaultExecutorContext<RequestContext>>
    extends AsyncExecutor<T, RequestContext, ExecutorContext> implements JsonSerializableExecutor {

  @JsonIgnore protected final AsyncCmdLogFilePathResolver logFilePathResolver;

  /**
   * The command to execute. Set during executor creation. Serialize in database to continue
   * execution after service restart.
   */
  protected String cmd;

  /**
   * The file containing the concatenated stdout and stderr output of the stage execution. Set
   * during submit. Serialize in database to continue execution after service restart.
   */
  protected String outFile;

  public AsyncCmdExecutor(AsyncCmdLogFilePathResolver logFilePathResolver) {
    this.logFilePathResolver = logFilePathResolver;
  }

  public final void setOutFile(String outFile) {
    this.outFile = outFile;
  }

  public final String getOutFile() {
    return outFile;
  }

  protected final CmdRunner getCmdRunner() {
    return CmdRunner.create(getExecutorParams());
  }

  @Override
  public final void prepareExecution(
      PipeliteServices pipeliteServices, String pipelineName, String processId, Stage stage) {
    super.prepareExecution(pipeliteServices, pipelineName, processId, stage);
    this.outFile =
        logFilePathResolver
            .resolvedPath()
            .file(new StageExecutorRequest(pipelineName, processId, stage));
  }

  @Override
  protected final void endJob() {
    ZonedDateTime outFileTimeout =
        this.getJobCompletedTime().plus(getExecutorParams().getLogTimeout());
    StageExecutorResult result = this.getJobCompletedResult();
    if (isSaveLogFile(result)) {
      readOutFile(getRequest(), result, outFileTimeout);
    }
  }

  /**
   * Read the output file at end job execution.
   *
   * @param request the stage executor request
   * @param outFileTimeout the output file read timeout
   * @return true if the output file was available or if the output file poll timeout was exceeded
   */
  protected boolean readOutFile(
      StageExecutorRequest request, StageExecutorResult result, ZonedDateTime outFileTimeout) {
    logContext(log.atFine(), request).log("Reading output file: " + outFile);

    // The output file may not be immediately available after the job execution finishes.

    if (outFileTimeout.isBefore(ZonedDateTime.now())) {
      // The output file poll timeout was exceeded
      result.setStageLog(
          "Missing output file. Not available within "
              + (getExecutorParams().getLogTimeout().toMillis() / 1000)
              + " seconds.");
      return true;
    }

    // Check if the out file exists.
    if (!getCmdRunner().fileExists(Paths.get(outFile))) {
      // The output file is not available yet
      return false;
    }

    // The output file is available
    result.setStageLog(readOutFile(getCmdRunner(), outFile, getExecutorParams().getLogLines()));
    return true;
  }

  protected static String readOutFile(CmdRunner cmdRunner, String outFile, int logLines) {
    try {
      return cmdRunner.readFile(Paths.get(outFile), logLines);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed to read output file: " + outFile);
      return null;
    }
  }

  protected static FluentLogger.Api logContext(FluentLogger.Api log, StageExecutorRequest request) {
    return log.with(LogKey.PIPELINE_NAME, request.getPipelineName())
        .with(LogKey.PROCESS_ID, request.getProcessId())
        .with(LogKey.STAGE_NAME, request.getStage().getStageName());
  }
}
