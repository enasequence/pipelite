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

import java.util.function.Supplier;
import lombok.extern.flogger.Flogger;
import org.springframework.util.Assert;
import pipelite.configuration.LauncherConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.lock.PipeliteLocker;
import pipelite.process.Process;
import pipelite.process.ProcessFactory;

@Flogger
public class PipeliteLauncher extends ProcessRunnerPoolService {

  private final String pipelineName;
  private final ProcessFactory processFactory;
  private final ProcessCreator processCreator;
  private final ProcessQueue processQueue;
  private final int processCreateMaxSize;
  private final boolean shutdownIfIdle;
  private final ProcessLauncherStats stats = new ProcessLauncherStats();

  public PipeliteLauncher(
      LauncherConfiguration launcherConfiguration,
      PipeliteLocker pipeliteLocker,
      ProcessFactory processFactory,
      ProcessCreator processCreator,
      ProcessQueue processQueue,
      Supplier<ProcessRunnerPool> processRunnerPoolSupplier) {
    super(
        launcherConfiguration,
        pipeliteLocker,
        processQueue.getLauncherName(),
        processRunnerPoolSupplier);
    Assert.notNull(launcherConfiguration, "Missing launcher configuration");
    Assert.notNull(processFactory, "Missing process factory");
    Assert.notNull(processCreator, "Missing process creator");
    Assert.notNull(processQueue, "Missing process queue");
    this.processFactory = processFactory;
    this.processCreator = processCreator;
    this.processQueue = processQueue;
    this.pipelineName = processQueue.getPipelineName();
    this.processCreateMaxSize = launcherConfiguration.getProcessCreateMaxSize();
    this.shutdownIfIdle = launcherConfiguration.isShutdownIfIdle();
  }

  @Override
  protected void run() {
    if (processQueue.isFillQueue()) {
      processCreator.createProcesses(processCreateMaxSize);
      processQueue.fillQueue();
    }
    while (processQueue.isAvailableProcesses(getActiveProcessRunnerCount())) {
      runProcess(processQueue.nextAvailableProcess());
    }
  }

  @Override
  protected boolean shutdownIfIdle() {
    return !processQueue.isAvailableProcesses(0) && getActiveProcessRunnerCount() == 0 && shutdownIfIdle;
  }

  protected void runProcess(ProcessEntity processEntity) {
    Process process = ProcessFactory.create(processEntity, processFactory);
    if (process != null) {
      runProcess(pipelineName, process, (p, r) -> stats.add(p, r));
    } else {
      stats.addProcessCreationFailedCount(1);
    }
  }

  public String getPipelineName() {
    return pipelineName;
  }

  public ProcessLauncherStats getStats() {
    return stats;
  }
}
