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
package pipelite;

import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;

public interface RegisteredPipeline {

  /**
   * A unique name for the pipeline.
   *
   * @return a unique name for the pipeline
   */
  String getPipelineName();

  /**
   * A process to be executed. Executable pipeline instances are called processes. A process is
   * identified by a unique process id. Processes are composed of stages that are defined using the
   * provided process builder. The process builder has been initialised with the unique process id.
   *
   * @param builder the process builder
   * @return a process to be executed
   */
  Process createProcess(ProcessBuilder builder);
}
