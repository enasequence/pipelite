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

import java.util.*;
import pipelite.process.ProcessFactory;
import pipelite.process.ProcessInstance;

public class TestInMemoryProcessFactory implements ProcessFactory {

  private final Map<String, ProcessInstance> processInstances = new HashMap<>();

  public TestInMemoryProcessFactory(Collection<ProcessInstance> processInstances) {
    processInstances.stream()
        .forEach(
            processInstance ->
                this.processInstances.put(processInstance.getProcessId(), processInstance));
  }

  @Override
  public ProcessInstance create(String processId) {
    return processInstances.get(processId);
  }

  public int getProcessInstances() {
    return processInstances.size();
  }
}
