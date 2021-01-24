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

/** Implement this interface to register a pipelite pipeline in this service. */
public interface Pipeline {

  /**
   * Returns the pipeline name. The name must be unique.
   *
   * @return the pipeline name
   */
  String getPipelineName();

  /**
   * Returns the maximum number of parallel process executions.
   *
   * @return the maximum number of parallel process executions
   */
  int getPipelineParallelism();

  /**
   * Creates a process to be executed by pipelite.
   *
   * @param builder the process builder
   * @return the process
   */
  Process createProcess(ProcessBuilder builder);
}
