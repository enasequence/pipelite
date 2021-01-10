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
package pipelite.executor.context;

import lombok.extern.flogger.Flogger;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.task.RetryTask;
import pipelite.executor.task.RetryTaskAggregator;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.time.Time;

import java.time.Duration;
import java.util.Optional;

/** Contexts for operations that can be shared between LSF executors. */
@Flogger
public class AbstractLsfContext extends SharedExecutorContext<CmdRunner> implements Runnable {
  private static final Duration DESCRIBE_JOB_REQUEST_TIMEOUT = Duration.ofMinutes(10);
  private static final Duration DESCRIBE_JOB_REQUEST_FREQUENCY = Duration.ofSeconds(15);
  private static final int DESCRIBE_JOB_REQUEST_LIMIT = 100;

  private final DescribeJobs describeJobs;

  public static class DescribeJobs
      extends RetryTaskAggregator<String, StageExecutorResult, CmdRunner> {
    public DescribeJobs(CmdRunner cmdRunner) {
      super(
          RetryTask.DEFAULT_FIXED,
          DESCRIBE_JOB_REQUEST_TIMEOUT,
          DESCRIBE_JOB_REQUEST_LIMIT,
          cmdRunner,
          AbstractLsfExecutor::describeJobs);
    }
  }

  public AbstractLsfContext(CmdRunner cmdRunner) {
    super(cmdRunner);
    describeJobs = new DescribeJobs(cmdRunner);
    new Thread(this).start();
  }

  public Optional<StageExecutorResult> describeJob(String jobId) {
    return describeJobs.getResult(jobId);
  }

  @Override
  public void run() {
    while (true) {
      Time.wait(DESCRIBE_JOB_REQUEST_FREQUENCY);
      describeJobs.makeRequests();
    }
  }
}
