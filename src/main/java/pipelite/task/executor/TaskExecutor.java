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
package pipelite.task.executor;

import pipelite.task.state.TaskExecutionState;
import uk.ac.ebi.ena.sra.pipeline.executors.ExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.ExecutionInfo;
import uk.ac.ebi.ena.sra.pipeline.launcher.StageInstance;

public interface TaskExecutor {

  void reset(StageInstance instance);

  void execute(StageInstance instance);

  <T extends ExecutorConfig> void configure(T params);

  TaskExecutionState can_execute(StageInstance instance);
  //    boolean       was_error();
  ExecutionInfo get_info();

  Class<? extends ExecutorConfig> getConfigClass();
}