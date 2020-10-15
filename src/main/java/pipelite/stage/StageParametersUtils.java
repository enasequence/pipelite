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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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

  public static Integer getRetries(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getRetries, stageConfiguration::getRetries);
  }

  public static String getTempDir(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getWorkDir, stageConfiguration::getWorkDir);
  }

  public static String[] getEnv(
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

  public static Duration getPollDelay(
      StageParameters stageParameters, StageConfiguration stageConfiguration) {
    return getValue(stageParameters::getPollDelay, stageConfiguration::getPollDelay);
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

  private static String[] mergeEnv(String[] stage, String[] stageConfiguration) {
    if (stage == null) {
      stage = new String[0];
    }
    if (stageConfiguration == null) {
      stageConfiguration = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(stage));
    Set<String> set2 = new HashSet<>(Arrays.asList(stageConfiguration));
    set1.addAll(set2);
    return set1.toArray(new String[0]);
  }
}