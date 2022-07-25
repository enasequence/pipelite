/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite;

import java.time.Duration;
import lombok.Data;
import lombok.Value;
import lombok.experimental.Accessors;

/**
 * A pipeline to be executed by Pipelite. A pipeline has a unique name that is defined by
 * implementing the pipelineName method. The pipeline consists of one or more stages executed using
 * any of the available executors (e.g Kubernetes). Stage dependencies and stage executor parameters
 * are configured by implementing the configureProcess method and using the ProcessBuilder class.
 * New processes can be directly inserted to the PIPELITE2_PROCESS table or they can be created and
 * prioritised using the nextProcess method. If the nextProcess method is implemented then Pipelite
 * will call the confirmProcess method when he process execution has completed.
 */
public interface Pipeline extends RegisteredPipeline {

  @Data
  @Accessors(fluent = true)
  class Options {
    /** The maximum number of parallel process executions. Default value is 1. */
    private int pipelineParallelism = 1;

    /**
     * The minimum duration to refresh the process queue. Pipelite keeps processes in an in-memory
     * queue. The queue is refreshed to keep its size above the pipeline parallelism. By default
     * this is not done more often than once every 10 minutes. The default value can be overridden
     * here or by using pipelite.advanced.processQueueMinRefreshFrequency property.
     */
    private Duration processQueueMinRefreshFrequency;

    /**
     * The maximum duration to refresh the process queue. Pipelite keeps processes in an in-memory
     * queue. The queue is refreshed to keep its size above the pipeline parallelism. By default
     * this is done at least once every four hours. The default value can be overridden here or by
     * using pipelite.advanced.processQueueMaxRefreshFrequency property.
     */
    private Duration processQueueMaxRefreshFrequency;
  }

  /**
   * Configures the pipeline by returning the configuration options.
   *
   * @return the pipeline configuration options
   */
  Pipeline.Options configurePipeline();

  /** The process execution priority. */
  enum Priority {
    LOWEST(1),
    LOW(3),
    DEFAULT(5),
    HIGH(7),
    HIGHEST(9);

    Priority(int priority) {
      this.priority = priority;
    }

    final int priority;

    public int getInt() {
      return priority;
    }
  }

  /**
   * The process to execute. A process is identified by a unique processId and prioritised using
   * Priority.
   */
  @Value
  class Process {
    /** The unique process id. */
    private final String processId;
    /** The process priority. */
    private final Priority priority;

    public Process(String processId, Priority priority) {
      this.processId = processId;
      this.priority = priority;
    }

    public Process(String processId) {
      this.processId = processId;
      this.priority = Priority.DEFAULT;
    }
  }

  /**
   * Return the next process to be executed or null if there are no processes to execute. If the
   * same process id is returned more than once all but the first are ignored.
   *
   * @return the next process to be executed or null if there are no processes to execute
   */
  default Process nextProcess() {
    return null;
  }

  /**
   * Called when the Process returned by {@link Pipeline#nextProcess} has been successfully
   * executed.
   *
   * @param processId the process id that has been successfully executed
   */
  default void confirmProcess(String processId) {}
}
