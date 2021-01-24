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

import lombok.Value;

/** Implement this interface to register a process source to be used by pipelite. */
public interface ProcessSource {

  String getPipelineName();

  @Value
  class NewProcess {
    private final String processId;
    private final Integer priority;

    public NewProcess(String processId, Integer priority) {
      this.processId = processId;
      this.priority = priority;
    }

    public NewProcess(String processId) {
      this.processId = processId;
      this.priority = null;
    }
  }

  /** Returns the next new process to be executed. */
  NewProcess next();

  /** Confirms that the new process has been accepted. */
  void accept(String processId);
}
