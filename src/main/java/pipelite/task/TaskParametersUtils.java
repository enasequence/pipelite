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
package pipelite.task;

import com.google.common.base.Supplier;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.Value;
import pipelite.configuration.TaskConfiguration;

@Value
public class TaskParametersUtils {

  public static String getHost(TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getHost, taskConfiguration::getHost);
  }

  public static Duration getTimeout(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getTimeout, taskConfiguration::getTimeout);
  }

  public static Integer getRetries(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getRetries, taskConfiguration::getRetries);
  }

  public static String getTempDir(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getWorkDir, taskConfiguration::getWorkDir);
  }

  public static String[] getEnv(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return mergeEnv(taskParameters.getEnv(), taskConfiguration.getEnv());
  }

  public static Integer getMemory(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getMemory, taskConfiguration::getMemory);
  }

  public static Duration getMemoryTimeout(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getMemoryTimeout, taskConfiguration::getMemoryTimeout);
  }

  public static Integer getCores(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getCores, taskConfiguration::getCores);
  }

  public static String getQueue(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getQueue, taskConfiguration::getQueue);
  }

  public static Duration getPollDelay(
      TaskParameters taskParameters, TaskConfiguration taskConfiguration) {
    return getValue(taskParameters::getPollDelay, taskConfiguration::getPollDelay);
  }

  private static <T> T getValue(Supplier<T> taskInstance, Supplier<T> taskConfiguration) {
    T value = taskInstance.get();
    if (value == null) {
      value = taskConfiguration.get();
    }
    return value;
  }

  private static String[] mergeEnv(String[] taskInstance, String[] taskConfiguration) {
    if (taskInstance == null) {
      taskInstance = new String[0];
    }
    if (taskConfiguration == null) {
      taskConfiguration = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(taskInstance));
    Set<String> set2 = new HashSet<>(Arrays.asList(taskConfiguration));
    set1.addAll(set2);
    return set1.toArray(new String[0]);
  }
}
