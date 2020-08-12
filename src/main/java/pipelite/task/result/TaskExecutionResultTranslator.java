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
package pipelite.task.result;

public class TaskExecutionResultTranslator {

  private final TaskExecutionResult[] results;

  public TaskExecutionResultTranslator(TaskExecutionResult[] results) {
    this.results = results;
  }

  private TaskExecutionResult getCommitStatusOK() {
    return results[0];
  }

  public TaskExecutionResult getCommitStatusDefaultFailure() {
    return results[results.length - 1];
  }

  public TaskExecutionResult getCommitStatus(Throwable t) {
    Class<?> klass = null == t ? null : t.getClass();

    for (TaskExecutionResult csd : results) {
      Class<? extends Throwable> cause = csd.getCause();
      if (klass == cause || (null != cause && cause.isInstance(t))) return csd;
    }

    return getCommitStatusDefaultFailure();
  }

  public TaskExecutionResult getCommitStatus(int exitCode) {
    if (0 == exitCode) {
      return getCommitStatusOK();
    }

    for (TaskExecutionResult csd : results) {
      if (exitCode == csd.getExitCode()) {
        return csd;
      }
    }

    return getCommitStatusDefaultFailure();
  }
}
