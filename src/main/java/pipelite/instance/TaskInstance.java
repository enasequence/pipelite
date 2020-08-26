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
package pipelite.instance;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Value;
import pipelite.stage.Stage;

@Value
@Builder
public class TaskInstance {
  private final String processName;
  private final String processId;
  private final Stage stage;
  @EqualsAndHashCode.Exclude private final TaskParameters taskParameters;
}
