/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.manager;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.PreDestroy;
import lombok.extern.flogger.Flogger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import pipelite.Pipeline;
import pipelite.Schedule;
import pipelite.configuration.PipeliteConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.metrics.PipeliteMetrics;
import pipelite.runner.pipeline.PipelineRunner;
import pipelite.runner.pipeline.PipelineRunnerFactory;
import pipelite.runner.process.ProcessRunnerPool;
import pipelite.runner.schedule.ScheduleRunner;
import pipelite.runner.schedule.ScheduleRunnerFactory;
import pipelite.service.PipeliteServices;

@Flogger
@Component
@DependsOn({"registeredPipelineService", "registeredScheduleService"})
public class ProcessRunnerPoolManager {

  private final PipeliteConfiguration pipeliteConfiguration;
  private final PipeliteServices pipeliteServices;
  private final PipeliteMetrics pipeliteMetrics;
  private final List<ProcessRunnerPool> pools = new ArrayList<>();
  private ServiceManager serviceManager;
  private State state;

  private enum State {
    STOPPED,
    INITIALISED,
    RUNNING
  }

  public ProcessRunnerPoolManager(
      @Autowired PipeliteConfiguration pipeliteConfiguration,
      @Autowired PipeliteServices pipeliteServices,
      @Autowired PipeliteMetrics pipeliteMetrics) {
    this.pipeliteConfiguration = pipeliteConfiguration;
    this.pipeliteServices = pipeliteServices;
    this.pipeliteMetrics = pipeliteMetrics;
    this.state = State.STOPPED;
  }

  public Collection<ProcessRunnerPool> getPools() {
    return pools;
  }

  private void clear() {
    pipeliteServices.runner().clearScheduleRunner();
    pipeliteServices.runner().clearPipelineRunners();
    pools.clear();
    serviceManager = null;
  }

  public synchronized void createPools() {
    log.atInfo().log("Creating process runner pools");

    try {
      _createPipelineRunners();
      _createScheduleRunner();

      log.atInfo().log("Created process runner pools");

      _createServiceManager();
    } catch (Exception ex) {
      log.atSevere().withCause(ex).log("Unexpected exception when creating process runner pools");
      clear();
      throw new PipeliteException(ex);
    }
  }

  /** Should not be called directly. Called by {@link #createPools()}. */
  public void _createPipelineRunners() {
    if (state != State.STOPPED) {
      log.atWarning().log("Failed to create pipeline runners manager state is not stopped");
      return;
    }

    for (Pipeline pipeline :
        pipeliteServices.registeredPipeline().getRegisteredPipelines(Pipeline.class)) {
      PipelineRunner pipelineRunner = createPipelineRunner(pipeline.pipelineName());
      log.atInfo().log("Creating pipeline runner: " + pipeline.pipelineName());
      pipeliteServices.runner().addPipelineRunner(pipelineRunner);
      pools.add(pipelineRunner);
    }
    log.atInfo().log("Created pipeline runners");
  }

  /** Should not be called directly. Called by {@link #createPools()}. */
  public void _createScheduleRunner() {
    if (state != State.STOPPED) {
      log.atWarning().log("Failed to create schedule runners manager state is not stopped");
      return;
    }

    if (pipeliteServices.registeredPipeline().isSchedules()) {
      ScheduleRunner scheduleRunner =
          createScheduler(
              pipeliteServices.registeredPipeline().getRegisteredPipelines(Schedule.class));
      log.atInfo().log("Creating schedule runner");
      pipeliteServices.runner().setScheduleRunner(scheduleRunner);
      pools.add(scheduleRunner);
      log.atInfo().log("Created schedule runner");
    }
  }

  /** Should not be called directly. Called by {@link #createPools()}. */
  public synchronized void _createServiceManager() {
    log.atInfo().log("Creating service manager for process runner pools");

    if (pools.isEmpty()) {
      log.atSevere().log(
          "Failed to create service manager for process runner pools because none exist");
      return;
    }

    try {
      serviceManager = new ServiceManager(pools);
      serviceManager.addListener(
          new ServiceManager.Listener() {
            public void stopped() {}

            public void healthy() {}

            public void failure(Service service) {
              log.atSevere().withCause(service.failureCause()).log(
                  "Process runner pool has failed");
              pipeliteServices
                  .internalError()
                  .saveInternalError(this.getClass(), service.failureCause());
              stopPools();
            }
          },
          MoreExecutors.directExecutor());

    } catch (Exception ex) {
      log.atSevere().withCause(ex).log(
          "Unexpected exception when creating service manager for process runner pools");
      clear();
      throw new PipeliteException(ex);
    }

    state = State.INITIALISED;
    log.atInfo().log("Created service manager for process runner pools");
  }

  /** Starts process runner pools. */
  public synchronized void startPools() {
    log.atInfo().log("Starting process runner pools");

    if (state != State.INITIALISED) {
      log.atWarning().log(
          "Failed to start process runner pools because manager state is not initialised");
      return;
    }

    serviceManager.startAsync();

    state = State.RUNNING;
    log.atInfo().log("Started process runner pools");
  }

  /** Waits until process runner pools have stopped. */
  public synchronized void waitPoolsToStop() {
    if (state != State.RUNNING) {
      log.atWarning().log(
          "Failed to wait process runner pools to stop because manager state is not running");
      return;
    }
    serviceManager.awaitStopped();
    state = State.STOPPED;
  }

  @PreDestroy
  /** Stops process runner pools. */
  public synchronized void stopPools() {
    if (state != State.RUNNING) {
      return;
    }

    log.atInfo().log(
        "Stopping process runner pools. Shutdown period is + "
            + pipeliteConfiguration.service().getShutdownPeriod().getSeconds()
            + " seconds.");

    try {
      serviceManager
          .stopAsync()
          .awaitStopped(
              pipeliteConfiguration.service().getShutdownPeriod().getSeconds(), TimeUnit.SECONDS);
    } catch (TimeoutException ignored) {
    }
    clear();
    log.atInfo().log("Stopped process runner pools");
    state = State.STOPPED;
  }

  /** Terminates all running processes. */
  public synchronized void terminateProcesses() {
    log.atInfo().log("Terminating all running processes");
    pools.forEach(service -> service.terminate());
  }

  private ScheduleRunner createScheduler(List<Schedule> schedules) {
    return ScheduleRunnerFactory.create(
        pipeliteConfiguration, pipeliteServices, pipeliteMetrics, schedules);
  }

  private PipelineRunner createPipelineRunner(String pipelineName) {
    return PipelineRunnerFactory.create(
        pipeliteConfiguration, pipeliteServices, pipeliteMetrics, pipelineName);
  }
}
