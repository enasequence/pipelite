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
package pipelite.executor.async;

import java.time.ZonedDateTime;
import lombok.Value;

@Value
public class SubmitResult {
  private final ZonedDateTime time = ZonedDateTime.now();
  private final String jobId;

  private SubmitResult(String jobId) {
    this.jobId = jobId;
  }

  public static SubmitResult valueOf(String jobId) {
    return new SubmitResult(jobId);
  }
}
