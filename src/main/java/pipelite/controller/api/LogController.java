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
package pipelite.controller.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.controller.api.info.LogInfo;
import pipelite.entity.StageLogEntity;
import pipelite.service.StageService;

@RestController
@RequestMapping(value = "/api/log")
@Tag(name = "LogAPI", description = "Process logs")
public class LogController {
  @Autowired StageService stageService;

  @GetMapping("/{pipelineName}/{processId}/{stageName}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Process logs")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public LogInfo logs(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processId") String processId,
      @PathVariable(value = "stageName") String stageName) {
    LogInfo logInfo = new LogInfo();
    Optional<StageLogEntity> stageLogEntity =
        stageService.getSavedStageLog(pipelineName, processId, stageName);
    if (stageLogEntity.isPresent()) {
      logInfo.setLog(stageLogEntity.get().getStageLog());
    }
    return logInfo;
  }
}
