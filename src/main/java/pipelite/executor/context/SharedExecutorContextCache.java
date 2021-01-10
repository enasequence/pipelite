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

import pipelite.stage.executor.StageExecutor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache of {@link SharedExecutorContext}.
 *
 * @param <Executor> the executor
 * @param <SharedContextId>> the shared context id
 * @param <SharedContext> the shared context
 */
public abstract class SharedExecutorContextCache<
    Executor extends StageExecutor, SharedContextId, SharedContext extends SharedExecutorContext> {

  private final Map<SharedContextId, SharedContext> contextCache = new ConcurrentHashMap<>();

  /**
   * Returns a cached shared context. Creates a new one if one does not already exist.
   *
   * @executor the executor
   * @return the shared context
   */
  public SharedContext getSharedContext(Executor executor) {
    SharedContextId sharedContextId = createSharedContextId(executor);
    return contextCache.computeIfAbsent(sharedContextId, k -> createSharedContext(executor));
  }

  /**
   * Creates a new shared context id.
   *
   * @executor the executor parameters
   * @return the shared context
   */
  protected abstract SharedContextId createSharedContextId(Executor executor);

  /**
   * Creates a new shared context.
   *
   * @executor the executor parameters
   * @return the shared context
   */
  protected abstract SharedContext createSharedContext(Executor executor);
}
