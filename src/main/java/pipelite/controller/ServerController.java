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

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import pipelite.Application;
import pipelite.configuration.WebSecurityConfiguration;
import pipelite.controller.info.ServerInfo;
import pipelite.service.ServerService;

@RestController
@RequestMapping(value = "/server")
@Tag(name = "ServerAPI", description = "Pipelite servers")
public class ServerController {

  @Autowired private Application application;
  @Autowired private ServerService serverService;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "Pipelite servers")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public List<ServerInfo> servers() {
    List<ServerInfo> list = new ArrayList<>();
    serverService
        .getServers()
        .forEach(
            server -> {
              String serviceUrl =
                  getUrl(server.getHost(), server.getPort(), server.getContextPath());
              list.add(
                  new ServerInfo(
                      serviceUrl,
                      isHealthy(server.getHost(), server.getPort(), server.getContextPath())));
            });
    return list;
  }

  private String getUrl(String host, Integer port, String path) {
    return UriComponentsBuilder.newInstance()
        .scheme("http")
        .host(host)
        .port(port)
        .path(path)
        .build()
        .toUriString();
  }

  private boolean isHealthy(String host, Integer port, String path) {
    try {
      RestTemplate restTemplate = new RestTemplate();
      String healthUrl = getUrl(host, port, path + "/" + WebSecurityConfiguration.HEALTH_ENDPOINT);
      JsonNode resp = restTemplate.getForObject(healthUrl, JsonNode.class);
      if (resp.get("status").asText().equalsIgnoreCase("UP")) {
        return true;
      }
    } catch (Exception ex) {
      return false;
    }
    return false;
  }
}
