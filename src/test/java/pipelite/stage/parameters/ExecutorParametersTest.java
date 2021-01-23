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
package pipelite.stage.parameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;
import pipelite.exception.PipeliteException;

public class ExecutorParametersTest {

  @Test
  public void validate() {
    new ExecutorParameters().validate();
    ExecutorParameters.builder().build().validate();
    assertThrows(
        PipeliteException.class,
        () -> ExecutorParameters.builder().immediateRetries(null).build().validate());
    assertThrows(
        PipeliteException.class,
        () -> ExecutorParameters.builder().maximumRetries(null).build().validate());
    assertThrows(
        PipeliteException.class,
        () -> ExecutorParameters.builder().timeout(null).build().validate());
  }

  @Test
  public void validateNotNull() {
    assertThat(ExecutorParameters.validateNotNull("test", "test")).isEqualTo("test");
    assertThrows(PipeliteException.class, () -> ExecutorParameters.validateNotNull(null, "test"));
  }

  @Test
  public void validatePath() {
    assertThat(ExecutorParameters.validatePath("/a/b", "test")).isEqualTo(Paths.get("/a/b"));
    assertThrows(PipeliteException.class, () -> ExecutorParameters.validatePath(null, "test"));
  }

  @Test
  public void validateUrl() throws Exception {

    // https
    assertThat(ExecutorParameters.validateUrl("https://www.ebi.ac.uk", "test"))
        .isEqualTo(new URL("https://www.ebi.ac.uk"));

    // resource

    String resource = "pipelite/executor/lsf.yaml";
    assertThat(ExecutorParameters.validateUrl(resource, "test"))
        .isEqualTo(ExecutorParametersTest.class.getClassLoader().getResource(resource));
    assertThat(ExecutorParameters.validateUrl(resource, "test")).toString().startsWith("file:/");
  }
}