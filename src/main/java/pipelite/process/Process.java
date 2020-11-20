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
package pipelite.process;

import com.google.common.flogger.FluentLogger;
import java.util.HashSet;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.log.LogKey;
import pipelite.stage.Stage;

@Flogger
@Value
@Builder
public class Process {
  private final String pipelineName;
  private final String processId;
  @EqualsAndHashCode.Exclude private final List<Stage> stages;

  public boolean validate() {
    boolean isSuccess = true;
    if (pipelineName == null || pipelineName.isEmpty()) {
      logContext(log.atSevere()).log("Pipeline name is missing");
      isSuccess = false;
    }
    if (processId == null || processId.isEmpty()) {
      logContext(log.atSevere()).log("Process id is missing");
      isSuccess = false;
    }

    if (stages == null || stages.isEmpty()) {
      logContext(log.atSevere()).log("No stages");
      isSuccess = false;

    } else {
      HashSet<String> stageNames = new HashSet<>();

      for (Stage stage : stages) {
        if (!stage.validate()) {
          isSuccess = false;
        } else {
          if (stageNames.contains(stage.getStageName())) {
            stage.logContext(log.atSevere()).log("Duplicate stage name: %s", stage.getStageName());
            isSuccess = false;
          }
          stageNames.add(stage.getStageName());

          if (!stage.getPipelineName().equals(pipelineName)) {
            logContext(log.atSevere())
                .log(
                    "Conflicting pipeline name %s in stage %s",
                    stage.getPipelineName(), stage.getStageName());
            isSuccess = false;
          }
        }
      }
    }
    return isSuccess;
  }

  public FluentLogger.Api logContext(FluentLogger.Api log) {
    return log.with(LogKey.PIPELINE_NAME, pipelineName).with(LogKey.PROCESS_ID, processId);
  }
}
