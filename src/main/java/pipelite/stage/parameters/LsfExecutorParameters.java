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

import java.util.Map;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import pipelite.configuration.ExecutorConfiguration;

/** LSF executor parameters. For more information please refer to LSF documentation. */
@Data
@NoArgsConstructor
@SuperBuilder
@EqualsAndHashCode(callSuper = true)
public class LsfExecutorParameters extends CmdExecutorParameters {
  public enum Format {
    YAML,
    JSON,
    JSDL
  };

  /** The LSF job definition file. */
  private String definition;

  /** The LSF job definition file format. */
  private Format format;

  /**
   * The LSF job definition parameters applied to the job definition file. The key is the parameter
   * placeholder that if found in the job definition file will be replaced with the corresponding
   * value.
   */
  private Map<String, String> parameters;

  @Override
  public void applyDefaults(ExecutorConfiguration executorConfiguration) {
    LsfExecutorParameters defaultParams = executorConfiguration.getLsf();
    super.applyDefaults(defaultParams);
    applyDefault(this::getDefinition, this::setDefinition, defaultParams::getDefinition);
    applyDefault(this::getFormat, this::setFormat, defaultParams::getFormat);
    applyMapDefaults(parameters, defaultParams.parameters);
  }
}
