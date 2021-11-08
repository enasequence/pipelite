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

import java.time.Duration;
import lombok.Data;
import lombok.Value;
import lombok.experimental.Accessors;

/** A pipeline to be executed by pipelite. */
public interface Pipeline extends RegisteredPipeline {

  @Data
  @Accessors(fluent = true)
  class Options {
    /** The maximum number of parallel process executions. Default value is 1. */
    private int pipelineParallelism = 1;

    private Duration processQueueMinRefreshFrequency;
    private Duration processQueueMaxRefreshFrequency;
  }

  /**
   * Configures the pipeline by returning pipeline specific configuration options.
   *
   * @return the pipeline configuration options
   */
  Pipeline.Options configurePipeline();

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

  @Value
  class Process {
    private final String processId;
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
   * Return the next process to be executed or null if there are no more processes to execute. If
   * the same process id is returned more than once all but the first are ignored.
   *
   * @return the next process to be executed or null if there are no more processes to execute
   */
  default Process nextProcess() {
    return null;
  }

  /**
   * A confirmation that the {@link Pipeline#nextProcess} has been successful.
   *
   * @param processId the process id of the process for which {@link Pipeline#nextProcess} has been
   *     successful
   */
  default void confirmProcess(String processId) {}
}
