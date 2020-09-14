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
package pipelite.process.builder;

import java.util.ArrayList;
import java.util.List;
import pipelite.task.Task;
import pipelite.task.TaskParameters;

public class ProcessBuilder {

  final String processName;
  final String processId;
  final int priority;
  final List<Task> tasks = new ArrayList<>();

  public static final int DEFAULT_PRIORITY = 0;

  public ProcessBuilder(String processName, String processId) {
    this.processName = processName;
    this.processId = processId;
    this.priority = DEFAULT_PRIORITY;
  }

  public ProcessBuilder(String processName, String processId, int priority) {
    this.processName = processName;
    this.processId = processId;
    this.priority = priority;
  }

  public TaskBuilder task(String taskName) {
    return new TaskBuilder(this, taskName, null, TaskParameters.builder().build());
  }

  public TaskBuilder task(String taskName, TaskParameters taskParameters) {
    return new TaskBuilder(this, taskName, null, taskParameters);
  }
}
