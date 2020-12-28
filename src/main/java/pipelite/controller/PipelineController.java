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
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import pipelite.Application;
import pipelite.controller.info.PipelineInfo;
import pipelite.controller.utils.LoremUtils;
import pipelite.controller.utils.TimeUtils;
import pipelite.launcher.PipeliteLauncher;
import pipelite.launcher.process.runner.ProcessRunnerMetrics;

@RestController
@RequestMapping(value = "/pipeline")
@Tag(name = "PipelineAPI", description = "Pipelines")
public class PipelineController {

  @Autowired Application application;
  @Autowired Environment environment;

  @GetMapping("/local")
  @ResponseStatus(HttpStatus.OK)
  @Operation(
      description =
          "Pipelines running in this server including statistics within the given number of minutes")
  @ApiResponses(
      value = {
        @ApiResponse(responseCode = "200", description = "OK"),
        @ApiResponse(responseCode = "500", description = "Internal Server error")
      })
  public Collection<PipelineInfo> localPipelines(@RequestParam(defaultValue = "5") int since) {
    List<PipelineInfo> list =
        getLocalPipelines(application.getRunningLaunchers(), Duration.ofMinutes(since));
    getLoremIpsumPipelines(list, Duration.ofMinutes(since));
    return list;
  }

  public static List<PipelineInfo> getLocalPipelines(
      Collection<PipeliteLauncher> pipeliteLaunchers, Duration since) {
    List<PipelineInfo> pipelines = new ArrayList<>();
    for (PipeliteLauncher pipeliteLauncher : pipeliteLaunchers) {
      ProcessRunnerMetrics metrics = pipeliteLauncher.getMetrics();
      pipelines.add(
          PipelineInfo.builder()
              .pipelineName(pipeliteLauncher.getPipelineName())
              .maxRunningProcessCount(pipeliteLauncher.getProcessParallelism())
              .runningProcessCount(pipeliteLauncher.getActiveProcessCount())
              .queuedProcessCount(pipeliteLauncher.getQueuedProcessCount())
              .completedProcessCount(getMetrics(metrics::getProcessCompletedCount, since))
              .failedProcessCount(getMetrics(metrics::getProcessFailedCount, since))
              .successfulStageCount(getMetrics(metrics::getStageSuccessCount, since))
              .failedStageCount(getMetrics(metrics::getStageFailedCount, since))
              .internalErrorCount(getMetrics(metrics::getInternalErrorCount, since))
              .uptime(TimeUtils.humanReadableDuration(pipeliteLauncher.getStartTime()))
              .build());
    }
    return pipelines;
  }

  public static String getMetrics(Function<Duration, Double> func, Duration since) {
    DecimalFormat format = new DecimalFormat("#");
    return format.format(func.apply(since));
  }

  private void getLoremIpsumPipelines(List<PipelineInfo> list, Duration since) {
    Random random = new Random();
    Function<Duration, Double> randomCount = duration -> Double.valueOf(random.nextInt(100));
    if (Arrays.stream(environment.getActiveProfiles())
        .anyMatch(profile -> LoremUtils.PROFILE_NAME.equals(profile))) {
      Lorem lorem = LoremIpsum.getInstance();
      IntStream.range(1, 100)
          .forEach(
              i ->
                  list.add(
                      PipelineInfo.builder()
                          .pipelineName(lorem.getCountry())
                          .maxRunningProcessCount(random.nextInt(50))
                          .runningProcessCount(random.nextInt(10))
                          .queuedProcessCount(random.nextInt(100))
                          .completedProcessCount(getMetrics(randomCount, since))
                          .failedProcessCount(getMetrics(randomCount, since))
                          .successfulStageCount(getMetrics(randomCount, since))
                          .failedStageCount(getMetrics(randomCount, since))
                          .internalErrorCount(getMetrics(randomCount, since))
                          .uptime(
                              TimeUtils.humanReadableDuration(ZonedDateTime.now().minusHours(1)))
                          .build()));
    }
  }
}
