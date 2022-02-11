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
package pipelite;

import java.util.ArrayList;
import java.util.List;
import lombok.Value;
import pipelite.exception.PipeliteException;
import pipelite.process.builder.ProcessBuilder;

/** Validates stage execution graphs created by configureProcess. */
public class ConfigureProcessValidator {

  @Value
  public static class ValidatorError {
    private final String pipelineName;
    private final String message;
  }

  /**
   * Validates stage execution graph. Uses 'VALIDATE' as the processId.
   *
   * @param pipeline the schedule or pipeline to validate
   * @return the validation errors
   */
  public static List<ValidatorError> validate(RegisteredPipeline pipeline) {
    List<ValidatorError> errors = new ArrayList<>();
    try {
      ProcessBuilder processBuilder = new ProcessBuilder("VALIDATE");
      pipeline.configureProcess(processBuilder);
      processBuilder.build();
    } catch (PipeliteException ex) {
      errors.add(new ValidatorError(pipeline.pipelineName(), ex.getMessage()));
    }
    return errors;
  }
}
