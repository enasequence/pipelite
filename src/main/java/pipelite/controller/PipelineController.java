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
import pipelite.controller.info.PipelineInfo;
import pipelite.launcher.process.runner.ProcessRunner;
import pipelite.launcher.process.runner.ProcessRunnerPoolService;

@RestController
@RequestMapping(value = "/pipeline")
@Tag(name = "PipelineAPI", description = "Pipelite pipelines")
public class PipelineController {

  @Autowired Application application;
  @Autowired Environment environment;

  @GetMapping("/")
  @ResponseStatus(HttpStatus.OK)
  @Operation(description = "All running pipelines")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public Collection<PipelineInfo> runningPipelines() {
    Set<PipelineInfo> set = new HashSet<>();
    application.getRunningLaunchers().stream()
        .forEach(launcher -> set.addAll(getPipelines(launcher)));
    application.getRunningSchedulers().stream()
        .forEach(launcher -> set.addAll(getPipelines(launcher)));
    getLoremIpsumPipelines(set);
    return set;
  }

  public static List<PipelineInfo> getPipelines(ProcessRunnerPoolService service) {
    Collection<ProcessRunner> processRunners = service.getActiveProcessRunners();
    List<PipelineInfo> pipelines = new ArrayList<>();
    for (ProcessRunner processRunner : processRunners) {
      pipelines.add(PipelineInfo.builder().pipelineName(processRunner.getPipelineName()).build());
    }
    return pipelines;
  }

  public void getLoremIpsumPipelines(Set<PipelineInfo> set) {
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> "LoremIpsum".equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      Random random = new Random();
      IntStream.range(1, 100)
          .forEach(i -> set.add(PipelineInfo.builder().pipelineName(lorem.getCountry()).build()));
    }
  }
}
