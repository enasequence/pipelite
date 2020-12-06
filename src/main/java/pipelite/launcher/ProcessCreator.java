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
package pipelite.launcher;

import pipelite.entity.ProcessEntity;
import pipelite.process.ProcessSource;

public interface ProcessCreator {

  /**
   * Creates and saves new processes using a process source.
   *
   * @param processCnt the number or requested processes
   * @return the number of created processes
   */
  int createProcesses(int processCnt);

  /**
   * Creates and saves a new process.
   *
   * @param newProcess the new process from process source
   * @return the created process or null if it could not be created
   */
  ProcessEntity createProcess(ProcessSource.NewProcess newProcess);
}
