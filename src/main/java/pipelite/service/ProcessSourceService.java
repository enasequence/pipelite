package pipelite.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import pipelite.process.ProcessSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ProcessSourceService {

  private final Map<String, ProcessSource> map = new HashMap<>();

  public ProcessSourceService(@Autowired List<ProcessSource> sources) {
    for (ProcessSource source : sources) {
      if (map.containsKey(source.getPipelineName())) {
        throw new ProcessSourceServiceException("Non-unique pipeline: " + source.getPipelineName());
      }
      map.put(source.getPipelineName(), source);
    }
  }

  /**
   * Creates a process source.
   *
   * @param pipelineName the pipeline name. A pipeline is identified by its name.
   * @return the process source for the pipeline.
   * @throws ProcessFactoryServiceException if the pipeline name is null or empty.
   */
  public ProcessSource create(String pipelineName) {
    if (pipelineName == null || pipelineName.trim().isEmpty()) {
      throw new ProcessSourceServiceException("Missing pipeline name");
    }
    return map.get(pipelineName);
  }
}
