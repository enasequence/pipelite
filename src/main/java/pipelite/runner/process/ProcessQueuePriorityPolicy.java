/*
 * Copyright 2020-2022 EMBL - European Bioinformatics Institute
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package pipelite.runner.process;

public enum ProcessQueuePriorityPolicy {
  /**
   * Process prioritisation is done strictly in priority order, highest priority first. Low priority
   * processes may never become active.
   */
  PRIORITY,
  /** Process prioritisation is done strictly in creation order, oldest processes first. */
  FIFO,
  /**
   * Process prioritisation is done by selecting 25% oldest processes first, the rest highest
   * priority first.
   */
  PREFER_PRIORITY,
  /**
   * Process prioritisation is done by selecting 50% oldest processes first, the rest highest
   * priority first.
   */
  BALANCED,
  /**
   * Process prioritisation is done by selecting 75% oldest processes first, the rest highest
   * priority first.
   */
  PREFER_FIFO;
}
