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
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
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
import pipelite.stage.parameters.AsyncCmdExecutorParameters;
import pipelite.stage.path.LogFilePathResolver;
import pipelite.time.Time;

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

  @JsonIgnore protected final LogFilePathResolver logFilePathResolver;

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

  public AsyncCmdExecutor(LogFilePathResolver logFilePathResolver) {
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
    // Read the output file.
    logContext(log.atInfo(), getRequest())
        .log("Attempting to read async job " + getJobId() + " output file: " + outFile);

    // Wait no longer than log timeout for the output file.
    ZonedDateTime endTime = this.getJobCompletedTime().plus(getExecutorParams().getLogTimeout());
    long logTimeoutSeconds = getExecutorParams().getLogTimeout().toMillis() / 1000;
    if (isSaveLogFile(getJobCompletedResult())) {
      while (ZonedDateTime.now().isBefore(endTime)) {
        if (getCmdRunner().fileExists(Paths.get(outFile))) {
          getJobCompletedResult()
              .setStageLog(readOutFile(getCmdRunner(), outFile, getExecutorParams().getLogLines()));
          return;
        }
        Time.wait(Duration.ofSeconds(Math.min(5, logTimeoutSeconds / 3)));
      }
      getJobCompletedResult()
          .setStageLog(
              "The output file was not available within " + logTimeoutSeconds + " seconds.");
    }
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
