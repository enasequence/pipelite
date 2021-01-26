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
package pipelite.controller.info;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ScheduleInfo {
  private String serviceName;
  private String pipelineName;
  private String cron;
  private String description;
  private String processId;
  private String summary;
  private String startTime;
  private String endTime;
  private String nextTime;
  private String lastCompleted;
  private String lastFailed;
  private Integer completedStreak;
  private Integer failedStreak;
}
