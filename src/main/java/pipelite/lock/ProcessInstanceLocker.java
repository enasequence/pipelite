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
package pipelite.lock;

import pipelite.entity.PipeliteProcess;

public interface ProcessInstanceLocker {

  boolean lock(String launcherName, PipeliteProcess pipeliteProcess);

  boolean unlock(String launcherName, PipeliteProcess pipeliteProcess);

  boolean isLocked(PipeliteProcess pipeliteProcess);

  void purge(String launcherName, String processName);

}
