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
package pipelite.stage.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;

public class ExecutorParametersValidatorTest {

  @Test
  public void validateNotNull() {
    assertThat(ExecutorParametersValidator.validateNotNull("test", "test")).isEqualTo("test");
    assertThrows(
        PipeliteException.class, () -> ExecutorParametersValidator.validateNotNull(null, "test"));
  }

  @Test
  public void validatePath() {
    assertThat(ExecutorParametersValidator.validatePath("/a/b", "test"))
        .isEqualTo(Paths.get("/a/b"));
    assertThrows(
        PipeliteException.class, () -> ExecutorParametersValidator.validatePath(null, "test"));
  }

  @Test
  public void validateUrl() throws Exception {

    // https
    assertThat(ExecutorParametersValidator.validateUrl("https://www.ebi.ac.uk", "test"))
        .isEqualTo(new URL("https://www.ebi.ac.uk"));

    // resource

    String resource = "pipelite/executor/lsf.yaml";
    assertThat(ExecutorParametersValidator.validateUrl(resource, "test"))
        .isEqualTo(ExecutorParametersValidatorTest.class.getClassLoader().getResource(resource));
    assertThat(ExecutorParametersValidator.validateUrl(resource, "test"))
        .toString()
        .startsWith("file:/");
  }
}
