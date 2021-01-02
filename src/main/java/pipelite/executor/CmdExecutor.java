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
import java.io.IOException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.flogger.Flogger;
import pipelite.executor.cmd.*;
import pipelite.stage.Stage;
import pipelite.stage.executor.AbstractExecutor;
import pipelite.stage.executor.JsonSerializableExecutor;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;

/** Executes a command. Must be serializable to json. */
@Flogger
@Getter
@Setter
public class CmdExecutor<T extends CmdExecutorParameters> extends AbstractExecutor<T>
    implements JsonSerializableExecutor {

  /** The command to be executed. */
  private String cmd;

  /** The runner to execute the command. */
  @JsonSerialize(using = CmdRunnerSerializer.class)
  @JsonDeserialize(using = CmdRunnerDeserializer.class)
  protected CmdRunner cmdRunner;

  /**
   * Returns an optional prefix command.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the stage
   * @return an optional prefix command
   */
  public String getPrefixCmd(String pipelineName, String processId, Stage stage) {
    return null;
  }

  /**
   * Returns the full command after all modifications including the optional prefix command.
   *
   * @param pipelineName the pipeline name
   * @param processId the process id
   * @param stage the stage
   * @return the full command after all modifications including the optional prefix command
   */
  public String getFullCmd(String pipelineName, String processId, Stage stage) {
    String prefixCmd = getPrefixCmd(pipelineName, processId, stage);
    if (prefixCmd != null) {
      prefixCmd = prefixCmd + " ";
    } else {
      prefixCmd = "";
    }
    String singularityImage = getExecutorParams().getSingularityImage();
    if (singularityImage != null) {
      prefixCmd += "singularity run " + singularityImage + " ";
    }
    return prefixCmd + cmd;
  }

  public StageExecutorResult execute(String pipelineName, String processId, Stage stage) {
    String fullCmd = getFullCmd(pipelineName, processId, stage);
    try {
      CmdRunnerResult result = cmdRunner.execute(fullCmd, getExecutorParams());
      return result.getStageExecutorResult(fullCmd);
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Failed call: %s", fullCmd);
      StageExecutorResult result = StageExecutorResult.error(ex);
      result.addAttribute(StageExecutorResultAttribute.COMMAND, fullCmd);
      return result;
    }
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
    public CmdRunner deserialize(JsonParser parser, DeserializationContext context)
        throws IOException {
      try {
        JsonNode node = parser.getCodec().readTree(parser);
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
