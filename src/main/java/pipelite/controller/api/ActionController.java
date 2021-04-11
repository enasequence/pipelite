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
import pipelite.service.RetryService;

@RestController
@RequestMapping(value = {"/api/action"})
@Tag(name = "ActionAPI", description = "Processing actions")
public class ActionController {

  @Autowired private RetryService retryService;

  @Value
  @Builder
  public static class RetryResult {
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
  public ResponseEntity<List<RetryResult>> retry(
      @PathVariable(value = "pipelineName") String pipelineName,
      @PathVariable(value = "processIds") List<String> processIds) {
    List<RetryResult> result =
        retry(pipelineName, processIds, (processId) -> retryService.retry(pipelineName, processId));
    boolean isError = result.stream().anyMatch(s -> !s.isSuccess());
    return new ResponseEntity<>(result, isError ? HttpStatus.BAD_REQUEST : HttpStatus.OK);
  }

  private List<RetryResult> retry(
      String pipelineName, List<String> processIds, Consumer<String> action) {
    List<RetryResult> result = new ArrayList<>();

    for (String processId : processIds) {
      RetryResult.RetryResultBuilder resultBuilder =
          RetryResult.builder().pipelineName(pipelineName).processId(processId);
      try {
        action.accept(processId);
        result.add(resultBuilder.success(true).message("").build());
      } catch (Exception ex) {
        result.add(resultBuilder.success(false).message(ex.getMessage()).build());
      }
    }
    return result;
  }
}
