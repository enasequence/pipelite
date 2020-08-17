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
package uk.ac.ebi.ena.sra.pipeline.launcher;

import uk.ac.ebi.ena.sra.pipeline.launcher.PipeliteLauncher.ProcessLauncherInterface;

public abstract class ProcessPoolExecutor extends TaggedPoolExecutor {
  public ProcessPoolExecutor(int corePoolSize) {
    super(corePoolSize);
  }

  @Override
  @Deprecated
  public void execute(Object id, Runnable runnable) {
    super.execute(id, runnable);
  }

  @Override
  public void execute(Runnable process) {
    execute(((ProcessLauncherInterface) process).getProcessId(), process);
  }

  public abstract void unwind(ProcessLauncherInterface process);

  public abstract void init(ProcessLauncherInterface r);

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    unwind((ProcessLauncherInterface) r);
    super.afterExecute(r, t);
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    init((ProcessLauncherInterface) r);
  }
}
