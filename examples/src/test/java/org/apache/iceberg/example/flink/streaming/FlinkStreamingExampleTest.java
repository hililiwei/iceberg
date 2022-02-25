/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.example.flink.streaming;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.iceberg.example.WordData;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.apache.flink.configuration.ConfigConstants;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.Assert.fail;

public class FlinkStreamingExampleTest {

  @ClassRule
  public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();
  private static String warehouse;

  @BeforeClass
  public static void createWarehouse() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue("The warehouse should be deleted", warehouseFile.delete());
    // before variables
    warehouse = "file:" + warehouseFile;
  }

  @Test
  public void testExample() throws Exception {
    InetAddress localhost = InetAddress.getByName("localhost");

    // suppress sysout messages from this example
    final PrintStream originalSysout = System.out;
    final PrintStream originalSyserr = System.err;

    final ByteArrayOutputStream errorMessages = new ByteArrayOutputStream();

    System.setOut(new PrintStream(new NullStream()));
    System.setErr(new PrintStream(errorMessages));

    try {
      try (ServerSocket server = new ServerSocket(0, 10, localhost)) {

        final ServerThread serverThread = new ServerThread(server);
        serverThread.setDaemon(true);
        serverThread.start();

        final int serverPort = server.getLocalPort();

        SocketWindowWordCount.main(new String[] {"--port", String.valueOf(serverPort),
                                                "--warehouse", warehouse});

        if (errorMessages.size() != 0) {
          fail(
              "Found error message: "
                  + new String(
                  errorMessages.toByteArray(),
                  ConfigConstants.DEFAULT_CHARSET));
        }

        serverThread.join();
        serverThread.checkError();
      }
    } finally {
      System.setOut(originalSysout);
      System.setErr(originalSyserr);
    }
  }

  // ------------------------------------------------------------------------

  private static class ServerThread extends Thread {

    private final ServerSocket serverSocket;

    private volatile Throwable error;

    public ServerThread(ServerSocket serverSocket) {
      super("Socket Server Thread");

      this.serverSocket = serverSocket;
    }

    @Override
    public void run() {
      try {
        try (Socket socket = acceptWithoutTimeout(serverSocket);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

          writer.println(WordData.TEXT);
        }
      } catch (Throwable t) {
        this.error = t;
      }
    }

    public void checkError() throws IOException {
      if (error != null) {
        throw new IOException("Error in server thread: " + error.getMessage(), error);
      }
    }
  }

  private static final class NullStream extends OutputStream {
    @Override
    public void write(int b) {}
  }

  public static Socket acceptWithoutTimeout(ServerSocket serverSocket) throws IOException {
    Preconditions.checkArgument(
        serverSocket.getSoTimeout() == 0, "serverSocket SO_TIMEOUT option must be 0");
    while (true) {
      try {
        return serverSocket.accept();
      } catch (SocketTimeoutException exception) {
        // This should be impossible given that the socket timeout is set to zero
        // which indicates an infinite timeout. This is due to the underlying JDK-8237858
        // bug. We retry the accept call indefinitely to replicate the expected behavior.
      }
    }
  }
}
