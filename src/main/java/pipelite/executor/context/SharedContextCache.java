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
import pipelite.time.Time;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Cache for shared execution contexts. Returns cached context or creates new one if a context does
 * not already exist.
 *
 * @param <Executor> the executor
 * @param <SharedContextId>> the shared context id
 * @param <SharedContext> the shared context
 */
public abstract class SharedContextCache<
    Executor extends StageExecutor, SharedContextId, SharedContext> {

  private static final Duration POLL_FREQUENCY = Duration.ofSeconds(10);

  private final Map<SharedContextId, SharedContext> cache = new ConcurrentHashMap<>();
  private final Function<Executor, SharedContext> contextFactory;
  private final Function<Executor, SharedContextId> contextIdFactory;
  private Runnable makeRequests;

  /** Shared context. */
  public abstract static class Context<SharedContext> {
    private final SharedContext context;

    public Context(SharedContext context) {
      this.context = context;
    }

    /**
     * Returns the shared context.
     *
     * @return the shared context
     */
    public SharedContext get() {
      return context;
    }
  }

  public SharedContextCache(
      Function<Executor, SharedContext> contextFactory,
      Function<Executor, SharedContextId> contextIdFactory) {
    this.contextFactory = contextFactory;
    this.contextIdFactory = contextIdFactory;
    new Thread(
        () -> {
          while (true) {
            Time.wait(POLL_FREQUENCY);
            if (makeRequests != null) {
              makeRequests.run();
            }
          }
        }).start();
  }

  protected void registerMakeRequests(Runnable makeRequests) {
    this.makeRequests = makeRequests;
  }

  /**
   * Returns the cached context or creates new one if a context does not already exist.
   *
   * @executor the executor
   * @return the cached context or creates new one if a context does not already exist
   */
  public SharedContext getContext(Executor executor) {
    SharedContextId sharedContextId = contextIdFactory.apply(executor);
    return cache.computeIfAbsent(sharedContextId, k -> contextFactory.apply(executor));
  }

  /**
   * Returns all contexts.
   *
   * @return all contexts
   */
  public Collection<SharedContext> getContexts() {
    return cache.values();
  }
}
