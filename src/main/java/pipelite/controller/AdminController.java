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
package pipelite.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;

@RestController
@RequestMapping(value = "/admin")
@Tag(name = "AdministrationAPI", description = "Administration of pipelite services")
public class AdminController {

  @Autowired Application application;

  @PutMapping("/stop")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Stop all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public void stop() {
    application.stop();
  }

  @PutMapping("/restart")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Restarts all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public void restart() {
    application.restart();
  }

  @PutMapping("/shutdown")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Shuts down all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public void shutDown() {
    new Thread(() -> application.shutDown()).start();
  }
}
