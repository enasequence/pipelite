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
package pipelite.process.builder;

import java.util.ArrayList;
import java.util.List;
import pipelite.stage.Stage;
import pipelite.stage.StageParameters;

public class ProcessBuilder {

  final String pipelineName;
  final String processId;
  final int priority;
  final List<Stage> stages = new ArrayList<>();

  public static final int DEFAULT_PRIORITY = 0;

  public ProcessBuilder(String pipelineName, String processId) {
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.priority = DEFAULT_PRIORITY;
  }

  public ProcessBuilder(String pipelineName, String processId, int priority) {
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.priority = priority;
  }

  public StageBuilder execute(String stageName) {
    return new StageBuilder(this, stageName, null, StageParameters.builder().build());
  }

  public StageBuilder execute(String stageName, StageParameters stageParameters) {
    return new StageBuilder(this, stageName, null, stageParameters);
  }
}