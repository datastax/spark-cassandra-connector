/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.spark.connector.ccm;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class CcmBridge implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(CcmBridge.class);

  private final int[] nodes;

  private final int jmxPortOffset;

  private final Path configDirectory;

  private final AtomicBoolean started = new AtomicBoolean();

  private final AtomicBoolean created = new AtomicBoolean();

  private final String ipPrefix;

  private final Map<String, Object> cassandraConfiguration;
  private final Map<String, Object> dseConfiguration;
  private final List<String> rawDseYaml;
  private final List<String> createOptions;
  private final List<String> dseWorkloads;

  private final String jvmArgs;

  public static final Version VERSION = Version.parse(System.getProperty("ccm.version", "3.11.0"));

  public static final String INSTALL_DIRECTORY = System.getProperty("ccm.directory");

  public static final String BRANCH = System.getProperty("ccm.branch");

  public static final Boolean DSE_ENABLEMENT = Boolean.getBoolean("ccm.dse");

  public static final String DEFAULT_CLIENT_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_CLIENT_TRUSTSTORE_PATH = "/client.truststore";

  public static final File DEFAULT_CLIENT_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_TRUSTSTORE_PATH);

  public static final String DEFAULT_CLIENT_KEYSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_CLIENT_KEYSTORE_PATH = "/client.keystore";

  public static final File DEFAULT_CLIENT_KEYSTORE_FILE =
      createTempStore(DEFAULT_CLIENT_KEYSTORE_PATH);

  // Contains the same keypair as the client keystore, but in format usable by OpenSSL
  public static final File DEFAULT_CLIENT_PRIVATE_KEY_FILE = createTempStore("/client.key");
  public static final File DEFAULT_CLIENT_CERT_CHAIN_FILE = createTempStore("/client.crt");

  public static final String DEFAULT_SERVER_TRUSTSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_SERVER_TRUSTSTORE_PATH = "/server.truststore";

  private static final File DEFAULT_SERVER_TRUSTSTORE_FILE =
      createTempStore(DEFAULT_SERVER_TRUSTSTORE_PATH);

  public static final String DEFAULT_SERVER_KEYSTORE_PASSWORD = "cassandra1sfun";
  public static final String DEFAULT_SERVER_KEYSTORE_PATH = "/server.keystore";

  private static final File DEFAULT_SERVER_KEYSTORE_FILE =
      createTempStore(DEFAULT_SERVER_KEYSTORE_PATH);

  // A separate keystore where the certificate has a CN of localhost, used for hostname
  // validation testing.
  public static final String DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH = "/server_localhost.keystore";

  private static final File DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE =
      createTempStore(DEFAULT_SERVER_LOCALHOST_KEYSTORE_PATH);

  // major DSE versions
  private static final Version V6_0_0 = Version.parse("6.0.0");
  private static final Version V5_1_0 = Version.parse("5.1.0");
  private static final Version V5_0_0 = Version.parse("5.0.0");

  // mapped C* versions from DSE versions
  private static final Version V4_0_0 = Version.parse("4.0.0");
  private static final Version V3_10 = Version.parse("3.10");
  private static final Version V3_0_15 = Version.parse("3.0.15");
  private static final Version V2_1_19 = Version.parse("2.1.19");

  // artificial estimation of maximum number of nodes for this cluster, may be bumped anytime.
  public static final Integer MAX_NUMBER_OF_NODES = 4;

  private CcmBridge(
      Path configDirectory,
      int[] nodes,
      String ipPrefix,
      Map<String, Object> cassandraConfiguration,
      Map<String, Object> dseConfiguration,
      List<String> dseConfigurationRawYaml,
      List<String> createOptions,
      Collection<String> jvmArgs,
      List<String> dseWorkloads,
      Integer jmxPortOffset) {
    this.configDirectory = configDirectory;
    this.nodes = nodes;
    this.ipPrefix = ipPrefix;
    this.cassandraConfiguration = cassandraConfiguration;
    this.dseConfiguration = dseConfiguration;
    this.rawDseYaml = dseConfigurationRawYaml;
    this.createOptions = createOptions;

    StringBuilder allJvmArgs = new StringBuilder("");
    for (String jvmArg : jvmArgs) {
      allJvmArgs.append(" ");
      allJvmArgs.append("--jvm_arg=");
      allJvmArgs.append(jvmArg);
    }
    this.jvmArgs = allJvmArgs.toString();
    this.dseWorkloads = dseWorkloads;
    this.jmxPortOffset = jmxPortOffset;
  }

  public Optional<Version> getDseVersion() {
    return DSE_ENABLEMENT ? Optional.of(VERSION) : Optional.empty();
  }

  public Version getCassandraVersion() {
    if (!DSE_ENABLEMENT) {
      return VERSION;
    } else {
      Version stableVersion = VERSION.nextStable();
      if (stableVersion.compareTo(V6_0_0) >= 0) {
        return V4_0_0;
      } else if (stableVersion.compareTo(V5_1_0) >= 0) {
        return V3_10;
      } else if (stableVersion.compareTo(V5_0_0) >= 0) {
        return V3_0_15;
      } else {
        return V2_1_19;
      }
    }
  }

  public void create(String clusterName) {
    if (created.compareAndSet(false, true)) {
      if (INSTALL_DIRECTORY != null) {
        createOptions.add("--install-dir=" + new File(INSTALL_DIRECTORY).getAbsolutePath());
      } else if (BRANCH != null) {
        createOptions.add("-v git:" + BRANCH.trim().replaceAll("\"", ""));

      } else {
        createOptions.add("-v " + VERSION.toString());
      }
      if (DSE_ENABLEMENT) {
        createOptions.add("--dse");
      }
      execute(
          "create",
          clusterName,
          "-i",
          ipPrefix,
          createOptions.stream().collect(Collectors.joining(" ")));

      for (int i: nodes) {
        execute("add",
                "-s",
                "-j", jmxPort(i).toString(),
                "--dse",
                "-i", ipOfNode(i),
                "--remote-debug-port=0",
                "node" + i);
      }

      for (Map.Entry<String, Object> conf : cassandraConfiguration.entrySet()) {
        execute("updateconf", String.format("%s:%s", conf.getKey(), conf.getValue()));
      }
      if (getCassandraVersion().compareTo(Version.V2_2_0) >= 0) {
        execute("updateconf", "enable_user_defined_functions:true");
      }
      if (DSE_ENABLEMENT) {
        for (Map.Entry<String, Object> conf : dseConfiguration.entrySet()) {
          execute("updatedseconf", String.format("%s:%s", conf.getKey(), conf.getValue()));
        }
        for (String yaml : rawDseYaml) {
          executeUnsanitized("updatedseconf", "-y", yaml);
        }
        if (!dseWorkloads.isEmpty()) {
          execute("setworkload", String.join(",", dseWorkloads));
        }
      }
    }
  }

  public void dsetool(int node, String... args) {
    execute(String.format("node%d dsetool %s", node, Joiner.on(" ").join(args)));
  }

  public void reloadCore(int node, String keyspace, String table, boolean reindex) {
    dsetool(node, "reload_core", keyspace + "." + table, "reindex=" + reindex);
  }

  public void start() {
    if (started.compareAndSet(false, true)) {
      for (int i: nodes) {
        start(i);
      }
    }
  }

  public void stop() {
    if (started.compareAndSet(true, false)) {
      execute("stop");
    }
  }

  public void remove() {
    execute("remove");
  }

  public void pause(int n) {
    execute("node" + n, "pause");
  }

  public void resume(int n) {
    execute("node" + n, "resume");
  }

  public void start(int n) {
    execute("node" + n, "start", jvmArgs + "--wait-for-binary-proto");
  }

  public void nodetool(int n, String... args) {
      execute(String.format("node%d nodetool %s", n, Joiner.on(" ").join(args)));
  }

  public void refreshSizeEstimates(int n) {
    nodetool(n, "refreshsizeestimates");
  }

  public void flush(int n) {
    execute("node" + n, "flush");
  }

  public String ipOfNode(int n) {
    return ipPrefix + n;
  }

  public Integer jmxPort(int n) {
    return 7108 + jmxPortOffset + n;
  }

  public int[] getNodes() {
    return nodes;
  }

  public InetSocketAddress addressOfNode(int n) {
    return new InetSocketAddress(ipOfNode(n), 9042);
  }

  public List<InetSocketAddress> nodeAddresses() {
    return Arrays.stream(nodes).mapToObj(this::addressOfNode).collect(Collectors.toList());
  }

  public void stop(int n) {
    execute("node" + n, "stop");
  }

  synchronized void execute(String... args) {
    String command =
        "ccm "
            + String.join(" ", args)
            + " --config-dir="
            + configDirectory.toFile().getAbsolutePath();

    execute(CommandLine.parse(command));
  }

  synchronized void executeUnsanitized(String... args) {
    String command = "ccm ";

    CommandLine cli = CommandLine.parse(command);
    for (String arg : args) {
      cli.addArgument(arg, false);
    }
    cli.addArgument("--config-dir=" + configDirectory.toFile().getAbsolutePath());

    execute(cli);
  }

  private void execute(CommandLine cli) {
    logger.debug("Executing: " + cli);
    ExecuteWatchdog watchDog = new ExecuteWatchdog(TimeUnit.MINUTES.toMillis(10));
    try (LogOutputStream outStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                logger.debug("ccmout> {}", line);
              }
            };
        LogOutputStream errStream =
            new LogOutputStream() {
              @Override
              protected void processLine(String line, int logLevel) {
                logger.error("ccmerr> {}", line);
              }
            }) {
      Executor executor = new DefaultExecutor();
      ExecuteStreamHandler streamHandler = new PumpStreamHandler(outStream, errStream);
      executor.setStreamHandler(streamHandler);
      executor.setWatchdog(watchDog);

      int retValue = executor.execute(cli);
      if (retValue != 0) {
        logger.error(
            "Non-zero exit code ({}) returned from executing ccm command: {}", retValue, cli);
      }
    } catch (IOException ex) {
      if (watchDog.killedProcess()) {
        throw new RuntimeException("The command '" + cli + "' was killed after 10 minutes");
      } else {
        throw new RuntimeException("The command '" + cli + "' failed to execute", ex);
      }
    }
  }

  @Override
  public void close() {
    remove();
  }

  /**
   * Extracts a keystore from the classpath into a temporary file.
   *
   * <p>This is needed as the keystore could be part of a built test jar used by other projects, and
   * they need to be extracted to a file system so cassandra may use them.
   *
   * @param storePath Path in classpath where the keystore exists.
   * @return The generated File.
   */
  private static File createTempStore(String storePath) {
    File f = null;
    try (OutputStream os = new FileOutputStream(f = File.createTempFile("server", ".store"))) {
      f.deleteOnExit();
      URL resource = CcmBridge.class.getResource(storePath);
      if (resource != null) {
        Resources.copy(resource, os);
      } else {
        throw new IllegalStateException("Store path not found: " + storePath);
      }
    } catch (IOException e) {
      logger.warn("Failure to write keystore, SSL-enabled servers may fail to start.", e);
    }
    return f;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int[] nodes = {1};
    private final Map<String, Object> cassandraConfiguration = new LinkedHashMap<>();
    private final Map<String, Object> dseConfiguration = new LinkedHashMap<>();
    private final List<String> dseRawYaml = new ArrayList<>();
    private final List<String> jvmArgs = new ArrayList<>();
    private String ipPrefix = "127.0.0.";
    private final List<String> createOptions = new ArrayList<>();
    private final List<String> dseWorkloads = new ArrayList<>();
    private int jmxPortOffset = 0;

    private final Path configDirectory;

    private Builder() {
      try {
        this.configDirectory = Files.createTempDirectory("ccm");
        // mark the ccm temp directories for deletion when the JVM exits
//        this.configDirectory.toFile().deleteOnExit();
      } catch (IOException e) {
        // change to unchecked for now.
        throw new RuntimeException(e);
      }
      // disable auto_snapshot by default to reduce disk usage when destroying schema.
      withCassandraConfiguration("auto_snapshot", "false");
    }

    public Builder withCassandraConfiguration(String key, Object value) {
      cassandraConfiguration.put(key, value);
      return this;
    }

    public Builder withDseConfiguration(String key, Object value) {
      dseConfiguration.put(key, value);
      return this;
    }

    public Builder withDseConfiguration(String rawYaml) {
      dseRawYaml.add(rawYaml);
      return this;
    }

    public Builder withJvmArgs(String... jvmArgs) {
      Collections.addAll(this.jvmArgs, jvmArgs);
      return this;
    }

    public Builder withNodes(int... nodes) {
      this.nodes = nodes;
      return this;
    }

    public Builder withIpPrefix(String ipPrefix) {
      this.ipPrefix = ipPrefix;
      return this;
    }

    /** Adds an option to the {@code ccm create} command. */
    public Builder withCreateOption(String option) {
      this.createOptions.add(option);
      return this;
    }

    /** Enables SSL encryption. */
    public Builder withSsl() {
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      cassandraConfiguration.put(
          "client_encryption_options.keystore", DEFAULT_SERVER_KEYSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      return this;
    }

    public Builder withSslLocalhostCn() {
      cassandraConfiguration.put("client_encryption_options.enabled", "true");
      cassandraConfiguration.put(
          "client_encryption_options.keystore",
          DEFAULT_SERVER_LOCALHOST_KEYSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.keystore_password", DEFAULT_SERVER_KEYSTORE_PASSWORD);
      return this;
    }

    /** Enables client authentication. This also enables encryption ({@link #withSsl()}. */
    public Builder withSslAuth() {
      withSsl();
      cassandraConfiguration.put("client_encryption_options.require_client_auth", "true");
      cassandraConfiguration.put(
          "client_encryption_options.truststore", DEFAULT_SERVER_TRUSTSTORE_FILE.getAbsolutePath());
      cassandraConfiguration.put(
          "client_encryption_options.truststore_password", DEFAULT_SERVER_TRUSTSTORE_PASSWORD);
      return this;
    }

    public Builder withDseWorkloads(String... workloads) {
      this.dseWorkloads.addAll(Arrays.asList(workloads));
      return this;
    }

    /** Sets JMX port for nodes to default JMX port + offset + node id number */
    public Builder withJMXPortOffset(Integer offset) {
      this.jmxPortOffset = offset;
      return this;
    }

    public CcmBridge build() {
      return new CcmBridge(
          configDirectory,
          nodes,
          ipPrefix,
          cassandraConfiguration,
          dseConfiguration,
          dseRawYaml,
          createOptions,
          jvmArgs,
          dseWorkloads,
          jmxPortOffset);
    }
  }
}
