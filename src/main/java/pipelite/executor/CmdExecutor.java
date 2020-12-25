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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.*;
import pipelite.stage.executor.StageExecutor;
import pipelite.stage.Stage;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;

import java.io.IOException;

/** Executes a command. Must be serializable to json. */
@Flogger
@Getter
@Setter
public class CmdExecutor implements StageExecutor {

  /** The command string to be executed. */
  private String cmd;

  /** The command runner used to execute the command. */
  @JsonSerialize(using = CmdRunnerSerializer.class)
  @JsonDeserialize(using = CmdRunnerDeserializer.class)
  protected CmdRunner cmdRunner;

  public String getDispatcherCmd(String pipelineName, String processId, Stage stage) {
    return null;
  }

  public void getDispatcherJobId(StageExecutorResult stageExecutorResult) {}

  public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
    String singularityImage = stage.getExecutorParams().getSingularityImage();

    String execCmd = cmd;

    String dispatchCommand = getDispatcherCmd(pipelineName, processId, stage);
    if (dispatchCommand != null) {
      String cmdPrefix = dispatchCommand + " ";
      if (singularityImage != null) {
        cmdPrefix += "singularity run " + singularityImage + " ";
      }
      execCmd = cmdPrefix + execCmd;
    } else {
      if (singularityImage != null) {
        execCmd = "singularity run " + singularityImage + " " + execCmd;
      }
    }

    try {
      CmdRunnerResult result = cmdRunner.execute(execCmd, stage.getExecutorParams());
      return result.getStageExecutorResult(execCmd, stage.getExecutorParams().getHost());
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", execCmd);
      StageExecutorResult result = StageExecutorResult.error(ex);
      result.addAttribute(StageExecutorResultAttribute.COMMAND, execCmd);
      getDispatcherJobId(result);
      return result;
    }
  }

  public static String getWorkDir(String pipelineName, String processId, Stage stage) {
    if (stage.getExecutorParams().getWorkDir() != null) {
      String workDir = stage.getExecutorParams().getWorkDir();
      workDir = workDir.replace('\\', '/');
      workDir = workDir.trim();
      if (!workDir.endsWith("/")) {
        workDir = workDir + "/";
      }
      return workDir + "pipelite/" + pipelineName + "/" + processId;
    } else {
      return "pipelite/" + pipelineName + "/" + processId;
    }
  }

  public static String getOutFile(
      String pipelineName, String processId, Stage stage, String suffix) {
    String workDir = getWorkDir(pipelineName, processId, stage);
    if (!workDir.endsWith("/")) {
      workDir = workDir + "/";
    }
    return workDir + pipelineName + "_" + processId + "_" + stage.getStageName() + "." + suffix;
  }

  private static class CmdRunnerSerializer extends StdSerializer<CmdRunner> {
    public CmdRunnerSerializer() {
      this(null);
    }

    public CmdRunnerSerializer(Class<CmdRunner> t) {
      super(t);
    }

    @Override
    public void serialize(CmdRunner value, JsonGenerator generator, SerializerProvider provider)
        throws IOException {
      generator.writeString(value.getClass().getName());
    }
  }

  private static class CmdRunnerDeserializer extends StdDeserializer<CmdRunner> {
    public CmdRunnerDeserializer() {
      this(null);
    }

    public CmdRunnerDeserializer(Class<CmdRunner> vc) {
      super(vc);
    }

    @Override
    public CmdRunner deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
      try {
        JsonNode node = jp.getCodec().readTree(jp);
        String className = node.asText();
        return (CmdRunner) Class.forName(className).newInstance();
      } catch (Exception ex) {
        throw new IOException(ex);
      }
    }
  }

  @Override
  public String toString() {
    return serialize();
  }
}
