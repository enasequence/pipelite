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
package uk.ac.ebi.ena.sra.pipeline.launcher;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

public class TaggedPoolExecutor extends ThreadPoolExecutor {
  final Logger log = Logger.getLogger(this.getClass());

  final Map<Object, Runnable> running = Collections.synchronizedMap(new WeakHashMap<>());

  public TaggedPoolExecutor(int corePoolSize) {
    super(corePoolSize, corePoolSize, 0, TimeUnit.DAYS, new SynchronousQueue<>());
  }

  public void execute(Object id, Runnable runnable) {
    synchronized (running) {
      if (running.containsKey(id)) return;
      running.put(id, runnable);
    }
    try {
      super.execute(runnable);
    } catch (RejectedExecutionException ree) {
      synchronized (running) {
        running.remove(id, runnable);
      }
      throw ree;
    }
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    synchronized (running) {
      if (!running.containsValue(r)) log.warn("Attempted to remove not existing value");

      for (Iterator<Entry<Object, Runnable>> i = running.entrySet().iterator(); i.hasNext(); ) {
        Entry<?, ?> e = i.next();
        if (e.getValue() == r) {
          i.remove();
          break;
        }
      }
    }
  }

  @Override
  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
  }
}
