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
package uk.ac.ebi.ena.sra.pipeline.dblock;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;
import uk.ac.ebi.ena.sra.pipeline.filelock.FileLockInfo;

public abstract class AbstractPingPong implements Runnable {
  ServerSocket server;
  int port;
  private FileLockInfo my_info;
  CountDownLatch l = new CountDownLatch(1);
  final String machine;
  final int pid;
  private Logger log = Logger.getLogger(this.getClass());
  volatile boolean stop = false;

  public abstract FileLockInfo parseFileLock(String request_line);

  public abstract String formFileLock(FileLockInfo info);

  public AbstractPingPong(int port, String machine, int pid) {
    this.port = port;
    this.machine = machine;
    this.pid = pid;
    this.my_info = new FileLockInfo(null, pid, machine, port);
  }

  public int getPort() throws InterruptedException {
    l.await();
    return this.port;
  }

  public FileLockInfo getLockInfo() throws InterruptedException {
    l.await();
    return this.my_info;
  }

  public void stop() {
    this.stop = true;
    try {
      this.server.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    try {
      this.server = new ServerSocket(this.port);
      this.port = server.getLocalPort();
      this.my_info = new FileLockInfo(null, pid, machine, port);
      l.countDown();

      while (!stop) {
        Socket client = server.accept();
        // TODO multi-thread?
        try (BufferedReader input_reader =
                new BufferedReader(new InputStreamReader(client.getInputStream()));
            DataOutputStream client_stream = new DataOutputStream(client.getOutputStream())) {
          String request_line = input_reader.readLine();
          FileLockInfo info = parseFileLock(request_line);
          String reply =
              String.valueOf(
                      null == info
                          ? Boolean.FALSE
                          : (this.pid == info.pid && this.machine.equals(info.machine))
                              ? Boolean.TRUE
                              : Boolean.FALSE)
                  + "\n";
          client_stream.writeBytes(reply);
          log.info("recv: " + request_line + ", resp: " + reply);
        } catch (Throwable t) {
          log.error("Pong", t);
        } finally {
          client.close();
        }
      }
    } catch (SocketException e) {
      if (stop) log.info("exit requested");
      else e.printStackTrace();
    } catch (IOException e) {

      e.printStackTrace();
    }
  }

  public boolean pingLockOwner(FileLockInfo info) throws UnknownHostException, IOException {
    try (Socket kkSocket = new Socket(info.machine, info.port);
        PrintWriter out = new PrintWriter(kkSocket.getOutputStream(), true);
        BufferedReader in =
            new BufferedReader(new InputStreamReader(kkSocket.getInputStream())); ) {
      out.println(formFileLock(info));
      return Boolean.parseBoolean(in.readLine());

    } catch (ConnectException e) {
      log.info("cannot ping " + info);
      return false;
    }
  }
}
