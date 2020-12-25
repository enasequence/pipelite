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
package pipelite.stage.executor;

import com.google.common.base.Supplier;
import java.time.Duration;
import java.util.*;
import lombok.Value;
import pipelite.configuration.StageConfiguration;

@Value
public class StageExecutorParametersUtils {

  public static String getHost(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getHost, stageConfiguration::getHost);
  }

  public static Duration getTimeout(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getTimeout, stageConfiguration::getTimeout);
  }

  public static Integer getMaximumRetries(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getMaximumRetries, stageConfiguration::getMaximumRetries);
  }

  public static Integer getImmediateRetries(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getImmediateRetries, stageConfiguration::getImmediateRetries);
  }

  public static String getTempDir(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getWorkDir, stageConfiguration::getWorkDir);
  }

  public static Map<String, String> getEnv(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return mergeEnv(executorParams.getEnv(), stageConfiguration.getEnv());
  }

  public static Integer getMemory(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getMemory, stageConfiguration::getMemory);
  }

  public static Duration getMemoryTimeout(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getMemoryTimeout, stageConfiguration::getMemoryTimeout);
  }

  public static Integer getCores(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getCores, stageConfiguration::getCores);
  }

  public static String getQueue(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getQueue, stageConfiguration::getQueue);
  }

  public static String getSingularityImage(
      StageExecutorParameters executorParams, StageConfiguration stageConfiguration) {
    return getValue(executorParams::getSingularityImage, stageConfiguration::getSingularityImage);
  }

  private static <T> T getValue(Supplier<T> stage, Supplier<T> stageConfiguration) {
    T value = stage.get();
    if (value == null) {
      value = stageConfiguration.get();
    }
    return value;
  }

  private static Map<String, String> mergeEnv(
      Map<String, String> executorParams, Map<String, String> stageConfiguration) {
    Map<String, String> merge = new HashMap<>();
    if (stageConfiguration != null) {
      for (String key : stageConfiguration.keySet()) {
        merge.put(key, stageConfiguration.get(key));
      }
    }
    if (executorParams != null) {
      for (String key : executorParams.keySet()) {
        merge.put(key, executorParams.get(key));
      }
    }
    return merge;
  }
}
