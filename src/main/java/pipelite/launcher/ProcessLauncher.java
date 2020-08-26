package pipelite.launcher;

import com.google.common.util.concurrent.Service;
import pipelite.instance.ProcessInstance;

public interface ProcessLauncher extends Service {
  String serviceName();

  void init(ProcessInstance processInstance);

  int getTaskFailedCount();

  int getTaskSkippedCount();

  int getTaskCompletedCount();
}
