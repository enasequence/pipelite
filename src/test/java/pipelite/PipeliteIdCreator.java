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

import java.util.UUID;

public class PipeliteIdCreator {

  public static <T> String serviceName() {
    return "service-" + id();
  }

  public static <T> String pipelineName() {
    return "pipeline-" + id();
  }

  public static <T> String processRunnerPoolName() {
    return "pool-" + id();
  }

  public static <T> String processId() {
    return "process-" + id();
  }

  public static <T> String stageName() {
    return "stage-" + id();
  }

  public static String id() {
    return UUID.randomUUID().toString();
  }
}