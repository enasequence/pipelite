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
package pipelite.runner.process.queue;

import pipelite.entity.ProcessEntity;

public interface ProcessQueue {

  /**
   * Returns the pipeline name.
   *
   * @return the pipeline name
   */
  String getPipelineName();

  /**
   * Returns true if more processes can be queued.
   *
   * @return true if more processes can be queued
   */
  boolean isFillQueue();

  /**
   * Queues processes and returns the number of processes queued.
   *
   * @return the number of processes queued
   */
  int fillQueue();

  /**
   * Returns true if there are available processes in the queue taking into account the maximum
   * number of active processes.
   *
   * @param activeProcesses the number of currently executing processes
   * @return true if there are available processes in the queue
   */
  boolean isAvailableProcesses(int activeProcesses);

  /**
   * Returns the number of available processes in the queue.
   *
   * @return the number of available processes in the queue
   */
  int getQueuedProcessCount();

  /**
   * Returns the next available process in the queue.
   *
   * @return the next available process in the queue
   */
  ProcessEntity nextAvailableProcess();
}
