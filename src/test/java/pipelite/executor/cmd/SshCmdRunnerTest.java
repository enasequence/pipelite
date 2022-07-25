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
package pipelite.executor.cmd;

import static org.assertj.core.api.Assertions.assertThat;
import static pipelite.service.PipeliteExecutorService.createExecutorService;

import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteIdCreator;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.configuration.properties.SshTestConfiguration;
import pipelite.exception.PipeliteException;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.executor.StageExecutorResultAttribute;
import pipelite.stage.parameters.CmdExecutorParameters;
import pipelite.time.Time;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=SshCmdRunnerTest"})
@ActiveProfiles("test")
public class SshCmdRunnerTest {

  private static final String TMP_DIR = "/tmp";

  @Autowired SshTestConfiguration sshTestConfiguration;

  private SshCmdRunner cmdRunner() {
    return new SshCmdRunner(
        CmdExecutorParameters.builder()
            .host(sshTestConfiguration.getHost())
            .user(sshTestConfiguration.getUser())
            .build());
  }

  @Test
  public void createTempFileAndFileExistsAndDeleteFile() {
    SshCmdRunner cmdRunner = cmdRunner();

    // Create file in temp dir
    String tempFile = cmdRunner.createTempFile();
    assertThat(cmdRunner.fileExists(Path.of(tempFile))).isTrue();

    // Delete file
    cmdRunner.deleteFile(Path.of(tempFile));
    assertThat(cmdRunner.fileExists(Path.of(tempFile))).isFalse();
  }

  @Test
  public void echo() {
    SshCmdRunner cmdRunner = cmdRunner();
    StageExecutorResult result = cmdRunner.execute("echo test");
    assertThat(result.isError()).isFalse();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isEqualTo("0");
    assertThat(result.stageLog()).startsWith("test");
  }

  @Test
  public void multiThreadedEcho() {
    ExecutorService executorService = createExecutorService("multiThreadedEcho", 5, null);

    int cnt = 20;
    AtomicInteger successCnt = new AtomicInteger();
    AtomicInteger totalCnt = new AtomicInteger();
    ZonedDateTime startTime = ZonedDateTime.now();

    for (int i = 0; i < cnt; ++i) {
      final int j = i;
      executorService.submit(
          () -> {
            SshCmdRunner cmdRunner = cmdRunner();
            try {
              StageExecutorResult result = cmdRunner.execute("echo test" + j);
              String exitCode = result.attribute(StageExecutorResultAttribute.EXIT_CODE);
              if (!exitCode.equals("0")) {
                throw new PipeliteException("Unexpected exit code: " + exitCode);
              }
              if (!result.stageLog().contains("test" + j)) {
                throw new PipeliteException("Unexpected log: " + result.stageLog());
              }
              successCnt.incrementAndGet();

            } catch (Exception ex) {
              ex.printStackTrace();
            } finally {
              totalCnt.incrementAndGet();
            }
          });
    }

    while (totalCnt.get() < cnt) {
      Time.wait(Duration.ofSeconds(1));
    }
    executorService.shutdownNow();

    System.out.println(
        "Total time: " + Duration.between(startTime, ZonedDateTime.now()).toSeconds() + " seconds");

    assertThat(successCnt.get()).isEqualTo(cnt);
  }

  @Test
  public void unknownCommand() {
    SshCmdRunner cmdRunner = cmdRunner();
    StageExecutorResult result = cmdRunner.execute(PipeliteIdCreator.id());
    assertThat(result.isError()).isTrue();
    assertThat(result.attribute(StageExecutorResultAttribute.EXIT_CODE)).isNotEqualTo("0");
  }
}
