/*
 * Copyright 2018-2019 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package uk.ac.ebi.ena.sra.pipeline.configuration;

import com.beust.jcommander.Parameter;

public class DefaultLauncherParams {
  @Parameter(names = "--workers", description = "number of simultaniously working processes")
  public int workers = 2;

  @Parameter(names = "--lock", description = "lock file path")
  public String lock = "/var/tmp/.launcher.lock";

  @Parameter(names = "--memory-limit", description = "memory per stage")
  public int lsf_mem = DefaultConfiguration.currentSet().getDefaultLSFMem();

  @Parameter(names = "--cpu-cores-limit", description = "CPU cores per stage")
  public int lsf_cpu_cores = DefaultConfiguration.currentSet().getDefaultLSFCpuCores();

  @Parameter(names = "--queue", description = "LSF queue name")
  public String queue_name = DefaultConfiguration.currentSet().getDefaultLSFQueue();

  @Parameter(names = "--log-file", description = "log file")
  public String log_file = "/var/tmp/launcher.log";

  @Parameter(
      names = "--lsf-mem-timeout",
      description = "timeout in minutes for lsf memory reservation")
  public int lsf_mem_timeout = DefaultConfiguration.currentSet().getDefaultLSFMemTimeout();
}
