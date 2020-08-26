package pipelite.instance;

import pipelite.entity.PipeliteProcess;

import java.util.List;

public interface TaskInstanceFactory {
    List<TaskInstance> create(PipeliteProcess pipeliteProcess);
}
