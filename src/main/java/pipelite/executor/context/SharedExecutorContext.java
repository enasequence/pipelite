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

/**
 * Contexts for operations that can be shared between executors.
 *
 * @param <SharedContext> the shared context.
 */
public abstract class SharedExecutorContext<SharedContext> {

  /** The context required to execute tasks. */
  private final SharedContext context;

  /** @param context the context required to execute tasks */
  public SharedExecutorContext(SharedContext context) {
    this.context = context;
  }

  public SharedContext get() {
    return context;
  }
}
