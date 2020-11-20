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
package pipelite.stage;

import com.google.common.base.Supplier;
import java.time.Duration;
import java.util.*;
import lombok.Value;
import pipelite.configuration.StageConfiguration;

@Value
public class StageParametersUtils {

  public static String getHost(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getHost, stageConfiguration::getHost);
  }

  public static Duration getTimeout(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getTimeout, stageConfiguration::getTimeout);
  }

  public static Integer getMaximumRetries(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getMaximumRetries, stageConfiguration::getMaximumRetries);
  }

  public static Integer getImmediateRetries(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getImmediateRetries, stageConfiguration::getImmediateRetries);
  }

  public static String getTempDir(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getWorkDir, stageConfiguration::getWorkDir);
  }

  public static Map<String, String> getEnv(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return mergeEnv(stageParameters.getEnv(), stageConfiguration.getEnv());
  }

  public static Integer getMemory(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getMemory, stageConfiguration::getMemory);
  }

  public static Duration getMemoryTimeout(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getMemoryTimeout, stageConfiguration::getMemoryTimeout);
  }

  public static Integer getCores(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getCores, stageConfiguration::getCores);
  }

  public static String getQueue(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getQueue, stageConfiguration::getQueue);
  }

  public static String getSingularityImage(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getSingularityImage, stageConfiguration::getSingularityImage);
  }

  private static <T> T getValue(Supplier<T> stage, Supplier<T> stageConfiguration) {
    T value = stage.get();
    if (value == null) {
      value = stageConfiguration.get();
    }
    return value;
  }

  private static Map<String, String> mergeEnv(
      Map<String, String> stageParameters, Map<String, String> stageConfiguration) {
    Map<String, String> merge = new HashMap<>();
    if (stageConfiguration != null) {
      for (String key : stageConfiguration.keySet()) {
        merge.put(key, stageConfiguration.get(key));
      }
    }
    if (stageParameters != null) {
      for (String key : stageParameters.keySet()) {
        merge.put(key, stageParameters.get(key));
      }
    }
    return merge;
  }
}
