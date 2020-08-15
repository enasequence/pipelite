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
package uk.ac.ebi.ena.sra.pipeline.configuration;

import pipelite.task.result.resolver.TaskExecutionResultExceptionResolver;
import uk.ac.ebi.ena.sra.pipeline.base.external.LSFClusterCall.LSFQueue;
import uk.ac.ebi.ena.sra.pipeline.executors.LSFExecutorConfig;
import uk.ac.ebi.ena.sra.pipeline.launcher.LSFStageExecutor;
import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.StageExecutorFactory;
import pipelite.task.executor.TaskExecutor;

public class LSFExecutorFactory implements StageExecutorFactory {
  private final String pipeline_name;
  private final TaskExecutionResultExceptionResolver resolver;
  private final String queue;
  private final int memory_limit;
  private final int cpu_cores;
  private final int lsf_mem_timeout;

    public LSFExecutorFactory(
            String pipeline_name,
            TaskExecutionResultExceptionResolver resolver,
            String queue,
            int memory_limit,
            int cpu_cores,
            int lsf_mem_timeout) {
    LSFQueue.findByName(queue);

    this.pipeline_name = pipeline_name;
    this.resolver = resolver;
    this.queue = queue;
    this.memory_limit = memory_limit;
    this.cpu_cores = cpu_cores;
    this.lsf_mem_timeout = lsf_mem_timeout;
    }

  public TaskExecutor getExecutor() {
    LSFExecutorConfig cfg_def =
        new LSFExecutorConfig() {
          @Override
          public int getLSFMemoryReservationTimeout() {
            return lsf_mem_timeout;
          }

          @Override
          public String getLsfQueue() {
            return queue;
          }
        };

    TaskExecutor executor =
        new LSFStageExecutor(pipeline_name, resolver, memory_limit, cpu_cores, cfg_def);

    return executor;
  }
}
