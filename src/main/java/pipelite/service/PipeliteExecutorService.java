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

  private AtomicReference<ExecutorService> processExecutorService = new AtomicReference<>();
  public AtomicReference<ExecutorService> stageExecutorService = new AtomicReference<>();

  @PostConstruct
  private void initExecutorServices() {
    initExecutorService(
        "pipelite-process-%d",
        pipeliteConfiguration.advanced().getProcessRunnerWorkers(), processExecutorService);
    initExecutorService(
        "pipelite-stage-%d",
        pipeliteConfiguration.advanced().getStageRunnerWorkers(), stageExecutorService);
    ;
  }

  private void initExecutorService(
      String nameFormat, int workers, AtomicReference<ExecutorService> executorService) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat(nameFormat)
            .setUncaughtExceptionHandler(
                (thread, throwable) ->
                    internalErrorService.saveInternalError(
                        pipeliteConfiguration.service().getName(), this.getClass(), throwable))
            .build();
    executorService.set(Executors.newFixedThreadPool(workers, threadFactory));
  }

  public ExecutorService process() {
    return processExecutorService.get();
  }

  public ExecutorService stage() {
    return stageExecutorService.get();
  }
}