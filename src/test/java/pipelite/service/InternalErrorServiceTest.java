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

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;
import pipelite.PipeliteIdCreator;
import pipelite.PipeliteTestConfigWithServices;
import pipelite.entity.InternalErrorEntity;

@SpringBootTest(
    classes = PipeliteTestConfigWithServices.class,
    properties = {"pipelite.service.force=true", "pipelite.service.name=InternalErrorServiceTest"})
@DirtiesContext
@ActiveProfiles("test")
@Transactional
class InternalErrorServiceTest {

  @Autowired InternalErrorService service;

  @Test
  public void withServiceName() {

    try {
      throw new RuntimeException("Test");
    } catch (Exception ex) {
      InternalErrorEntity error = service.saveInternalError(this.getClass(), ex);
      assertThat(error.getErrorId()).isNotNull();
      assertThat(error.getErrorTime()).isNotNull();
      assertThat(error.getServiceName()).isEqualTo("InternalErrorServiceTest");
      assertThat(error.getErrorMessage()).isEqualTo(ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      assertThat(error.getErrorLog()).isEqualTo(sw.toString());
    }
  }

  @Test
  public void withStageName() {
    String serviceName = PipeliteIdCreator.serviceName();
    String pipelineName = PipeliteIdCreator.pipelineName();
    String processId = PipeliteIdCreator.processId();
    String stageName = PipeliteIdCreator.stageName();

    try {
      throw new RuntimeException("Test");
    } catch (Exception ex) {
      InternalErrorEntity error =
          service.saveInternalError(pipelineName, processId, stageName, this.getClass(), ex);
      assertThat(error.getErrorId()).isNotNull();
      assertThat(error.getErrorTime()).isNotNull();
      assertThat(error.getServiceName()).isEqualTo("InternalErrorServiceTest");
      assertThat(error.getPipelineName()).isEqualTo(pipelineName);
      assertThat(error.getProcessId()).isEqualTo(processId);
      assertThat(error.getStageName()).isEqualTo(stageName);
      assertThat(error.getErrorMessage()).isEqualTo(ex.getMessage());
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      ex.printStackTrace(pw);
      assertThat(error.getErrorLog()).isEqualTo(sw.toString());
    }
  }
}
