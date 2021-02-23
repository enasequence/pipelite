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
package pipelite.controller.api;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pipelite.RegisteredPipeline;
import pipelite.process.Process;
import pipelite.process.builder.ProcessBuilder;
import pipelite.service.ProcessService;
import pipelite.service.RegisteredPipelineService;

@RestController
@RequestMapping(value = {"/api/action"})
@Tag(name = "ActionAPI", description = "Processing actions")
public class ActionController {
  @Autowired RegisteredPipelineService registeredPipelineService;
  @Autowired private ProcessService processService;

  @Value
  @Builder
  public static class ProcessStateChangeResult {
    private final String pipelineName;
    private final String processId;
    private final boolean success;
    private final String message;
  }

  @PutMapping("/process/retry/{pipelineName}/{processIds}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Retry permanently failed stages")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "400", description = "Error"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public ResponseEntity<List<ProcessStateChangeResult>> retry(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processIds") List<String> processIds) {
    List<ProcessStateChangeResult> result =
        changeProcessState(
            pipelineName, processIds, (process) -> processService.retry(pipelineName, process));
    boolean isError = result.stream().anyMatch(s -> !s.isSuccess());
    return new ResponseEntity<>(result, isError ? HttpStatus.BAD_REQUEST : HttpStatus.OK);
  }

  @PutMapping("/process/rerun/{pipelineName}/{processIds}/{stageName}")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Rerun previously executed stages")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "400", description = "Error"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public ResponseEntity<List<ProcessStateChangeResult>> rerun(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processIds") List<String> processIds,
      @PathVariable(value = "stageName") String stageName) {
    List<ProcessStateChangeResult> result =
        changeProcessState(
            pipelineName,
            processIds,
            (process) -> processService.rerun(pipelineName, stageName, process));
    boolean isError = result.stream().anyMatch(s -> !s.isSuccess());
    return new ResponseEntity<>(result, isError ? HttpStatus.BAD_REQUEST : HttpStatus.OK);
  }

  private List<ProcessStateChangeResult> changeProcessState(
      String pipelineName, List<String> processIds, Consumer<Process> action) {
    List<ProcessStateChangeResult> result = new ArrayList<>();

    RegisteredPipeline registeredPipeline;
    try {
      registeredPipeline = registeredPipelineService.getRegisteredPipeline(pipelineName);
    } catch (Exception ex) {
      for (String processId : processIds) {
        ProcessStateChangeResult.ProcessStateChangeResultBuilder resultBuilder =
            ProcessStateChangeResult.builder().pipelineName(pipelineName).processId(processId);
        result.add(resultBuilder.success(false).message(ex.getMessage()).build());
      }
      return result;
    }

    for (String processId : processIds) {
      ProcessBuilder processBuilder = new ProcessBuilder(processId);
      registeredPipeline.configureProcess(processBuilder);
      Process process = processBuilder.build();
      ProcessStateChangeResult.ProcessStateChangeResultBuilder resultBuilder =
          ProcessStateChangeResult.builder().pipelineName(pipelineName).processId(processId);
      try {
        action.accept(process);
        result.add(resultBuilder.success(true).message("").build());
      } catch (Exception ex) {
        result.add(resultBuilder.success(false).message(ex.getMessage()).build());
      }
    }
    return result;
  }
}
