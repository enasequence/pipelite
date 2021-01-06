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
package pipelite.stage.executor;

import lombok.Builder;
import lombok.Value;
import org.springframework.util.Assert;
import pipelite.stage.Stage;

@Value
@Builder
public class StageExecutorRequest {
  private final String pipelineName;
  private final String processId;
  private final Stage stage;

  public StageExecutorRequest(String pipelineName, String processId, Stage stage) {
    Assert.notNull(pipelineName, "Missing pipeline name");
    Assert.notNull(processId, "Missing process id");
    Assert.notNull(stage, "Missing stage");
    Assert.notNull(stage.getStageName(), "Missing stage name");
    this.pipelineName = pipelineName;
    this.processId = processId;
    this.stage = stage;
  }
}
