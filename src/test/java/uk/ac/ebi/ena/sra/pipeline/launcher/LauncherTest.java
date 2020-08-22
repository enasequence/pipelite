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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import pipelite.RandomStringGenerator;
import pipelite.configuration.LauncherConfiguration;
import pipelite.configuration.ProcessConfiguration;
import pipelite.entity.PipeliteProcess;
import pipelite.resolver.DefaultExceptionResolver;
import pipelite.service.PipeliteProcessService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@Slf4j
public class LauncherTest {
  static final long delay = 60;

  static final int PIPELITE_PROCESS_LIST_COUNT = 10;
  static final int PIPELITE_PROCESS_LIST_SIZE = 100;

  static int pipeliteProcessCreationCount = 0;
  static AtomicInteger pipeliteProcessExecutionCount = new AtomicInteger(0);

  private LauncherConfiguration defaultLauncherConfiguration() {
    return LauncherConfiguration.builder().build();
  }

  private ProcessConfiguration defaultProcessConfiguration() {
    return ProcessConfiguration.builder()
        .processName(RandomStringGenerator.randomProcessName())
        .resolver(DefaultExceptionResolver.NAME)
        .build();
  }

  @Test
  public void test() {

    LauncherConfiguration launcherConfiguration = defaultLauncherConfiguration();
    ProcessConfiguration processConfiguration = defaultProcessConfiguration();

    PipeliteProcessService pipeliteProcessService = mock(PipeliteProcessService.class);

    doAnswer(
            new Answer<Object>() {
              int pipeliteProcessListCount = 0;

              public Object answer(InvocationOnMock invocation) {

                while (++pipeliteProcessListCount <= PIPELITE_PROCESS_LIST_COUNT) {
                  return IntStream.range(0, PIPELITE_PROCESS_LIST_SIZE)
                      .mapToObj(
                          i -> {
                            PipeliteProcess pipeliteProcess = new PipeliteProcess();
                            pipeliteProcess.setProcessId(
                                "Process" + pipeliteProcessCreationCount++);
                            pipeliteProcess.setProcessName(processConfiguration.getProcessName());
                            return pipeliteProcess;
                          })
                      .collect(Collectors.toList());
                }
                return Collections.emptyList();
              }
            })
        .when(pipeliteProcessService)
        .getActiveProcesses(any());

    ProcessLauncherFactory processLauncherFactory =
        pipeliteProcess ->
            new ProcessLauncher() {
              @Override
              public String getProcessId() {
                return pipeliteProcess.getProcessId();
              }

              @Override
              public void run() {
                pipeliteProcessExecutionCount.incrementAndGet();
                log.debug("Launching process {}", pipeliteProcess.getProcessId());
                try {
                  Thread.sleep(delay);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
              }

              @Override
              public void stop() {}
            };

    PipeliteLauncher pipeliteLauncher =
        new PipeliteLauncher(
            launcherConfiguration,
            processConfiguration,
            pipeliteProcessService,
            processLauncherFactory);
    pipeliteLauncher.stopIfEmpty();
    pipeliteLauncher.execute();

    assertThat(pipeliteProcessExecutionCount.get())
        .isEqualTo(PIPELITE_PROCESS_LIST_COUNT * PIPELITE_PROCESS_LIST_SIZE);

    assertThat(pipeliteLauncher.getSubmittedProcessCount())
        .isEqualTo(PIPELITE_PROCESS_LIST_COUNT * PIPELITE_PROCESS_LIST_SIZE);

    assertThat(pipeliteLauncher.getActiveProcessCount()).isEqualTo(0);
  }
}
