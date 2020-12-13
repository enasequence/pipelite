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

import com.thedeanda.lorem.Lorem;
import com.thedeanda.lorem.LoremIpsum;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.*;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import pipelite.Application;
import pipelite.controller.info.ServiceInfo;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.PipeliteScheduler;
import pipelite.launcher.PipeliteService;
import pipelite.launcher.PipeliteUnlocker;

@RestController
@RequestMapping(value = "/service")
@Tag(name = "ProcessAPI", description = "Pipelite services")
public class ServiceController {

  @Autowired Application application;
  @Autowired Environment environment;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running services")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ServiceInfo> runningServices() {
    List<ServiceInfo> list = new ArrayList<>();
    application.getRunningServices().stream()
        .forEach(launcher -> list.addAll(getServices(launcher)));
    getLoremIpsumProcesses(list);
    return list;
  }

  public static List<ServiceInfo> getServices(PipeliteService service) {
    List<ServiceInfo> services = new ArrayList<>();
    String serviceType = "";
    String description = "";
    String pipelineName = "";
    if (service instanceof PipeliteLauncher) {
      serviceType = "PipeliteLauncher";
      description = "Runs processes for one pipeline";
      pipelineName = ((PipeliteLauncher) service).getPipelineName();
    }
    if (service instanceof PipeliteScheduler) {
      serviceType = "PipeliteScheduler";
      description = "Runs processes for one or more schedules";
      pipelineName = "";
    }
    if (service instanceof PipeliteUnlocker) {
      serviceType = "PipeliteUnlocker";
      description = "Removes expired locks";
      pipelineName = "";
    }
    services.add(
        ServiceInfo.builder()
            .serviceName(service.serviceName())
            .pipelineName(pipelineName)
            .serviceType(serviceType)
            .description(description)
            .build());
    return services;
  }

  public void getLoremIpsumProcesses(List<ServiceInfo> list) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      Random random = new Random();
      IntStream.range(1, 10)
          .forEach(
              i ->
                  list.add(
                      ServiceInfo.builder()
                          .serviceName(lorem.getFirstNameFemale())
                          .pipelineName(lorem.getCountry())
                          .serviceType(lorem.getWords(1))
                          .description(lorem.getWords(5))
                          .build()));
    }
  }
}
