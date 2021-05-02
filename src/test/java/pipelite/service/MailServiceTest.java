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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=MailServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
public class MailServiceTest {

  @Autowired MailService mailService;

  @Test
  public void sendProcessExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID").execute("STAGE1").withSyncTestExecutor().build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getProcessExecutionSubject(process))
        .isEqualTo("Pipelite process (PENDING): PIPELINE_NAME/PROCESS_ID");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
  }

  @Test
  public void sendStageExecutionMessage() {
    Process process =
        new ProcessBuilder("PROCESS_ID").execute("STAGE1").withSyncTestExecutor().build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setProcessState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, process.getStages().get(0)))
        .isEqualTo("Pipelite stage (PENDING): PIPELINE_NAME/PROCESS_ID/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"processState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0,\n"
                + "  \"priority\" : 5\n"
                + "}\n"
                + "\n"
                + "Stages:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"stageName\" : \"STAGE1\",\n"
                + "  \"stageState\" : \"PENDING\",\n"
                + "  \"executionCount\" : 0\n"
                + "}\n");
  }
}
