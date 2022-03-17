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
package pipelite.service;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.configuration.PipeliteConfiguration;

@Service
public class PipeliteExecutorService {

  private final PipeliteConfiguration pipeliteConfiguration;
  private final InternalErrorService internalErrorService;

  public PipeliteExecutorService(
      @Autowired PipeliteConfiguration pipeliteConfiguration,
      @Autowired InternalErrorService internalErrorService) {
    this.pipeliteConfiguration = pipeliteConfiguration;
    this.internalErrorService = internalErrorService;
  }

  private AtomicReference<ExecutorService> runProcessExecutorService = new AtomicReference<>();
  private AtomicReference<ExecutorService> submitStageExecutorService = new AtomicReference<>();
  private AtomicReference<ExecutorService> refreshQueueExecutorService = new AtomicReference<>();
  private AtomicReference<ExecutorService> replenishQueueExecutorService = new AtomicReference<>();

  @PostConstruct
  private void initExecutorServices() {
    String serviceName = pipeliteConfiguration.service().getName();
    runProcessExecutorService.set(
        createExecutorService(
            serviceName,
            "pipelite-process-%d",
            pipeliteConfiguration.advanced().getProcessRunnerWorkers(),
            internalErrorService));
    submitStageExecutorService.set(
        createExecutorService(
            serviceName,
            "pipelite-submit-%d",
            pipeliteConfiguration.advanced().getStageSubmitWorkers(),
            internalErrorService));
    refreshQueueExecutorService.set(
        createExecutorService(serviceName, "pipelite-refresh-%d", 5, internalErrorService));
    replenishQueueExecutorService.set(
        createExecutorService(serviceName, "pipelite-replenish-%d", 5, internalErrorService));
  }

  public static ExecutorService createExecutorService(
      String serviceName,
      String nameFormat,
      int workers,
      InternalErrorService internalErrorService) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setUncaughtExceptionHandler(
                (thread, throwable) -> {
                  if (internalErrorService != null) {
                    internalErrorService.saveInternalError(
                        serviceName, PipeliteExecutorService.class, throwable);
                  }
                })
            .build();
    return Executors.newFixedThreadPool(workers, threadFactory);
  }

  /** Used in ProcessRunnerPool.runOneIteration to run processes. */
  public ExecutorService process() {
    return runProcessExecutorService.get();
  }

  /** Used AbstractAsyncExecutor.execute to submit stages. */
  public ExecutorService submit() {
    return submitStageExecutorService.get();
  }

  /** Used in PipelineRunner runOneIteration to refresh process queue. */
  public ExecutorService refreshQueue() {
    return refreshQueueExecutorService.get();
  }

  /** Used in PipelineRunner runOneIteration to replenish process queue. */
  public ExecutorService replenishQueue() {
    return replenishQueueExecutorService.get();
  }
}
