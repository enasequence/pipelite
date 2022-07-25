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
import java.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.MetricsEndpoint;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import pipelite.controller.api.info.AdminInfo;
import pipelite.controller.utils.TimeUtils;
import pipelite.manager.ProcessRunnerPoolManager;

@RestController
@RequestMapping(value = {"/api/admin"})
@Tag(name = "AdministrationAPI", description = "Administration of pipelite services")
public class AdminController {

  @Autowired ProcessRunnerPoolManager processRunnerPoolManager;
  @Autowired ConfigurableApplicationContext applicationContext;
  @Autowired MetricsEndpoint metricsEndpoint;

  @GetMapping("/uptime")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Show the server uptime")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public String uptime() {
    long seconds =
        (metricsEndpoint.metric("process.uptime", null).getMeasurements().get(0))
            .getValue()
            .longValue();
    return TimeUtils.humanReadableDuration(Duration.ofSeconds(seconds));
  }

  @GetMapping("/stop")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Stop all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public AdminInfo stop() {
    new Thread(() -> processRunnerPoolManager.stopPools()).start();
    return new AdminInfo("Stopping all pipelite services");
  }

  @GetMapping("/kill")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Stop all pipelite services and terminate all running processes")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public AdminInfo kill() {
    new Thread(
            () -> {
              processRunnerPoolManager.terminateProcesses();
              processRunnerPoolManager.stopPools();
            })
        .start();
    return new AdminInfo("Stopping all pipelite services and terminating all running processes");
  }

  @GetMapping("/restart")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Restarts all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public AdminInfo restart() {
    new Thread(
            () -> {
              processRunnerPoolManager.stopPools();
              processRunnerPoolManager.createPools();
              processRunnerPoolManager.startPools();
            })
        .start();
    return new AdminInfo("Restarting all pipelite services");
  }

  @GetMapping("/shutdown")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Shuts down all pipelite services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public AdminInfo shutDown() {
    new Thread(
            () -> {
              processRunnerPoolManager.stopPools();
              applicationContext.close();
            })
        .start();
    return new AdminInfo("Shutting down all pipelite services");
  }
}
