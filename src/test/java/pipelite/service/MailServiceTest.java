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
import pipelite.PipeliteTestConfiguration;
import pipelite.entity.ProcessEntity;
import pipelite.entity.StageEntity;
import pipelite.process.Process;
import pipelite.process.ProcessState;
import pipelite.process.builder.ProcessBuilder;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = PipeliteTestConfiguration.class,
    properties = {
      "pipelite.mail.host=outgoing.ebi.ac.uk",
      "pipelite.mail.port=587",
      "pipelite.mail.from=pipelite-noreply@ebi.ac.uk",
      "pipelite.mail.to=rasko@ebi.ac.uk"
    })
public class MailServiceTest {

  @Autowired MailService mailService;

  @Test
  public void sendProcessExecutionMessage() {
    Process process = new ProcessBuilder("PROCESS_ID").execute("STAGE1").withCallExecutor().build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getProcessExecutionSubject(process))
        .isEqualTo("pipelite process (PENDING): PIPELINE_NAME/PROCESS_ID");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"state\" : \"PENDING\",\n"
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
                + "  \"executionCount\" : 0,\n"
                + "  \"resultType\" : \"PENDING\"\n"
                + "}\n");
    // mailService.sendProcessExecutionMessage("PIPELINE_NAME", process);
  }

  @Test
  public void sendStageExecutionMessage() {
    Process process = new ProcessBuilder("PROCESS_ID").execute("STAGE1").withCallExecutor().build();
    StageEntity stageEntity =
        StageEntity.createExecution("PIPELINE_NAME", "PROCESS_ID", process.getStages().get(0));
    process.getStages().get(0).setStageEntity(stageEntity);
    ProcessEntity processEntity = new ProcessEntity();
    processEntity.setPipelineName("PIPELINE_NAME");
    processEntity.setProcessId("PROCESS_ID");
    processEntity.setState(ProcessState.PENDING);
    processEntity.setPriority(5);
    process.setProcessEntity(processEntity);
    process.setProcessEntity(processEntity);
    assertThat(mailService.getStageExecutionSubject(process, process.getStages().get(0)))
        .isEqualTo("pipelite stage (PENDING): PIPELINE_NAME/PROCESS_ID/STAGE1");
    assertThat(mailService.getExecutionBody(process, "SUBJECT"))
        .isEqualTo(
            "SUBJECT\n"
                + "\n"
                + "Process:\n"
                + "---------------\n"
                + "{\n"
                + "  \"processId\" : \"PROCESS_ID\",\n"
                + "  \"pipelineName\" : \"PIPELINE_NAME\",\n"
                + "  \"state\" : \"PENDING\",\n"
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
                + "  \"executionCount\" : 0,\n"
                + "  \"resultType\" : \"PENDING\"\n"
                + "}\n");
    // mailService.sendStageExecutionMessage("PIPELINE_NAME", process, process.getStages().get(0));
  }
}
