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
package pipelite.runner.process.creator;

import pipelite.PrioritizedPipeline;
import pipelite.entity.ProcessEntity;

public interface PrioritizedProcessCreator {

  /**
   * Creates and saves prioritized processes.
   *
   * @param processCnt the number of requested processes to create
   * @return the number of created processes
   */
  int createProcesses(int processCnt);

  /**
   * Creates and saves one prioritized process.
   *
   * @param prioritizedProcess the next process
   * @return the created process or null if it could not be created
   */
  ProcessEntity createProcess(PrioritizedPipeline.PrioritizedProcess prioritizedProcess);
}
