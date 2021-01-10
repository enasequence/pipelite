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

import lombok.Value;
import lombok.extern.flogger.Flogger;
import pipelite.executor.AbstractLsfExecutor;
import pipelite.executor.cmd.CmdRunner;
import pipelite.executor.task.DefaultFixedRetryTaskAggregator;
import pipelite.stage.executor.StageExecutorResult;
import pipelite.stage.parameters.CmdExecutorParameters;

@Flogger
public class LsfContextCache
    extends SharedContextCache<
        AbstractLsfExecutor<CmdExecutorParameters>,
        LsfContextCache.ContextId,
        LsfContextCache.Context> {

  @Value
  public static final class ContextId {
    private final String host;
  }

  public static final class Context extends SharedContextCache.Context<CmdRunner> {
    public final DefaultFixedRetryTaskAggregator<String, StageExecutorResult, CmdRunner>
        describeJobs;

    public Context(CmdRunner cmdRunner) {
      super(cmdRunner);
      describeJobs =
          new DefaultFixedRetryTaskAggregator<>(100, cmdRunner, AbstractLsfExecutor::describeJobs);
    }
  }

  public LsfContextCache() {
    super(
        e -> new LsfContextCache.Context(CmdRunner.create(e.getExecutorParams())),
        e -> new LsfContextCache.ContextId(e.getExecutorParams().getHost()));
    registerMakeRequests(
        () -> getContexts().forEach(context -> context.describeJobs.makeRequests()));
  }
}
