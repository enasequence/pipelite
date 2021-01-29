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

import lombok.Data;
import lombok.experimental.Accessors;

/** A parallel pipeline to be executed by pipelite. */
public interface Pipeline extends RegisteredPipeline<Pipeline.Options> {

  @Data
  @Accessors(fluent = true)
  class Options {
    /** The maximum number of parallel process executions. Default value is 1. */
    private int pipelineParallelism = 1;
  }
}
