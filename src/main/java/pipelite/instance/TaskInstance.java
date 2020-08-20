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
package pipelite.instance;

import com.google.common.base.Supplier;
import com.google.common.base.Verify;
import lombok.Data;
import pipelite.configuration.TaskConfiguration;
import pipelite.configuration.TaskParameters;
import pipelite.entity.PipeliteProcess;
import pipelite.entity.PipeliteStage;
import pipelite.stage.Stage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Data
public class TaskInstance implements TaskParameters {

  private final PipeliteProcess pipeliteProcess;
  private final PipeliteStage pipeliteStage;
  private final TaskConfiguration taskConfiguration;
  private final Stage stage;

  public TaskInstance(
      PipeliteProcess pipeliteProcess,
      PipeliteStage pipeliteStage,
      TaskConfiguration taskConfiguration,
      Stage stage) {
    Verify.verifyNotNull(pipeliteProcess);
    Verify.verifyNotNull(pipeliteStage);
    Verify.verifyNotNull(taskConfiguration);
    Verify.verifyNotNull(stage);
    Verify.verifyNotNull(stage.getTaskConfiguration());

    this.pipeliteProcess = pipeliteProcess;
    this.pipeliteStage = pipeliteStage;
    this.stage = stage;
    this.taskConfiguration = taskConfiguration;
  }

  @Override
  public Integer getMemory() {
    return getValue(stage.getTaskConfiguration()::getMemory, taskConfiguration::getMemory);
  }

  @Override
  public Integer getMemoryTimeout() {
    return getValue(
        stage.getTaskConfiguration()::getMemoryTimeout, taskConfiguration::getMemoryTimeout);
  }

  @Override
  public Integer getCores() {
    return getValue(stage.getTaskConfiguration()::getCores, taskConfiguration::getCores);
  }

  @Override
  public String getQueue() {
    return getValue(stage.getTaskConfiguration()::getQueue, taskConfiguration::getQueue);
  }

  @Override
  public int getRetries() {
    return getValue(stage.getTaskConfiguration()::getRetries, taskConfiguration::getRetries);
  }

  @Override
  public String getTempDir() {
    return getValue(stage.getTaskConfiguration()::getTempDir, taskConfiguration::getTempDir);
  }

  @Override
  public String[] getEnv() {
    return mergeEnv(stage.getTaskConfiguration().getEnv(), taskConfiguration.getEnv());
  }

  private <T> T getValue(Supplier<T> stage, Supplier<T> taskConfiguration) {
    T value = stage.get();
    if (value == null) {
      value = taskConfiguration.get();
    }
    return value;
  }

  private String[] mergeEnv(String[] stage, String[] taskConfiguration) {
    if (stage == null) {
      stage = new String[0];
    }
    if (taskConfiguration == null) {
      taskConfiguration = new String[0];
    }
    Set<String> set1 = new HashSet<>(Arrays.asList(stage));
    Set<String> set2 = new HashSet<>(Arrays.asList(taskConfiguration));
    set1.addAll(set2);
    return set1.toArray(new String[0]);
  }
}
