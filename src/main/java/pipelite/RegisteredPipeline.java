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

import pipelite.process.builder.ProcessBuilder;

/**
 * Configures a pipeline to be executed by Pipelite. Pipelines are identified by a unique name and
 * configured using {@link RegisteredPipeline#configurePipeline}. Pipeline executions are called
 * processes and are configured using {@link RegisteredPipeline#configureProcess}.
 */
public interface RegisteredPipeline<PipelineOptions> {

  /**
   * A unique name for the pipeline.
   *
   * @return a unique name for the pipeline
   */
  String pipelineName();

  /**
   * Configures the pipeline by returning pipeline type specific configuration options.
   *
   * @return the pipeline configuration options
   */
  PipelineOptions configurePipeline();

  /**
   * Configures the processes to be executed for the pipeline. Processes are identified by a unique
   * process id and are configured using the provided {@link ProcessBuilder}. The {@link
   * ProcessBuilder} has been initialised with the process id. A process consists of one or more
   * stages. When a stage depends on others then it is executed after the stages it depends on have
   * been executed successfully. Each stage is configured to use one of the available execution
   * backends. A process execution is considered completed after all stages have been successfully
   * executed.
   *
   * @param builder a builder to configure the processes for the pipeline
   */
  void configureProcess(ProcessBuilder builder);
}
