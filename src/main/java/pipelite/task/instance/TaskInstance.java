/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.task.instance;

import com.google.common.base.Verify;
import lombok.Data;
import pipelite.entity.PipeliteStage;
import pipelite.stage.Stage;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;

@Data
public class TaskInstance {

  private final Stage stage;

  public TaskInstance(Stage stage) {
    Verify.verifyNotNull(stage);
    this.stage = stage;
  }

  private PipeliteStage pipeliteStage = new PipeliteStage();

  private int memory;
  private int cores;

  private String[] javaSystemProperties;
  private ExecutorConfig[] taskExecutorConfig;
}
