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

public class UniqueStringGenerator {

  public static <T> String randomServiceName(Class<T> cls) {
    return cls.getSimpleName() + "_service_" + id();
  }

  public static <T> String randomPipelineName(Class<T> cls) {
    return cls.getSimpleName() + "_pipeline_" + id();
  }

  public static <T> String randomProcessRunnerPoolName(Class<T> cls) {
    return cls.getSimpleName() + "process_runner_pool_" + id();
  }

  public static <T> String randomProcessId(Class<T> cls) {
    return cls.getSimpleName() + "_process_" + id();
  }

  public static <T> String randomStageName(Class<T> cls) {
    return cls.getSimpleName() + "_stage_" + id();
  }

  private static String id() {
    return UUID.randomUUID().toString();
  }
}
