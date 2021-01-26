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
package pipelite.launcher;

import com.google.common.util.concurrent.AbstractScheduledService;

public abstract class PipeliteService extends AbstractScheduledService {

  /** AbstractScheduledService service name. */
  @Override
  protected final String serviceName() {
    return getLauncherName();
  }

  /**
   * Returns the process launcher name.
   *
   * @return the process launcher name.
   */
  public abstract String getLauncherName();

  /** Terminates all running processes. */
  public abstract void terminate();
}
