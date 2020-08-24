package pipelite.launcher;

import com.google.common.util.concurrent.Service;

public interface PipeliteLauncher extends Service {
  String serviceName();
}
