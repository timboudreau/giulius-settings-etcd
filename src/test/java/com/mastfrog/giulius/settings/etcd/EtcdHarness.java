/*
 * The MIT License
 *
 * Copyright 2019 Mastfrog Technologies.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package com.mastfrog.giulius.settings.etcd;

import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import com.mastfrog.shutdown.hooks.DeploymentMode;
import com.mastfrog.giulius.settings.etcd.EtcdModule.EtcdClientSupplier;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT;
import com.mastfrog.giulius.settings.etcd.EtcdModule.TrustManagerSupplier;
import com.mastfrog.settings.Settings;
import com.mastfrog.shutdown.hooks.ShutdownHookRegistry;
import static com.mastfrog.util.collections.CollectionUtils.setOf;
import com.mastfrog.util.net.PortFinder;
import com.mastfrog.util.preconditions.ConfigurationError;
import com.mastfrog.util.preconditions.Exceptions;
import com.mastfrog.util.strings.Strings;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.inject.Inject;

/**
 *
 * @author Tim Boudreau
 */
public class EtcdHarness {

    public static final String SYS_PROP_ETCD_BINARY = "etcd.binary";
    private static final PortFinder portFinder = new PortFinder();
    private final ShutdownHookRegistry reg;

    @Inject
    EtcdHarness(ShutdownHookRegistry reg) {
        this.reg = reg;
    }

    public static final class HarnessModule extends AbstractModule implements Settings, Thread.UncaughtExceptionHandler, Rethrow {

        private final AtomicBoolean initialized = new AtomicBoolean();
        private static final String baseId
                = Long.toString(ThreadLocalRandom.current().nextLong(), 36) + ".";

        private final Settings settings;

        HarnessModule(Settings settings) {
            this.settings = settings;
        }

        private String name;

        synchronized String name() {
            if (name == null) {
                name = baseId + Long.toString(System.currentTimeMillis(), 36);
            }
            return name;
        }

        Path etcdBinary() {
            String binPath = System.getProperty(SYS_PROP_ETCD_BINARY);
            if (binPath != null) {
                Path p = Paths.get(binPath);
                if (Files.exists(p) && p.toFile().canExecute()) {
                    return p;
                }
            }
            String path = System.getenv("PATH");
            if (path != null) {
                for (String p : Strings.split(File.pathSeparatorChar, path)) {
                    Path dir = Paths.get(p);
                    if (Files.exists(dir) && Files.isDirectory(dir)) {
                        Path test = dir.resolve("etcd");
                        if (Files.exists(test) && test.toFile().canExecute()) {
                            return test;
                        }
                    }
                }
            }
            Path result = Paths.get("/opt/local/bin/etcd");
            if (Files.exists(result) && result.toFile().canExecute()) {
                return result;
            }
            return null;
        }

        private final AtomicReference<Process> process = new AtomicReference<>();

        @Override
        protected void configure() {
            install(new EtcdModule(this));
            bind(String.class).annotatedWith(Names.named("etcdName")).toInstance(name());
            bind(new TypeLiteral<AtomicReference<Process>>() {
            }).toInstance(process);
            bind(Shutdown.class).asEagerSingleton();
            bind(Thread.UncaughtExceptionHandler.class).toInstance(this);
            bind(Rethrow.class).toInstance(this);
            bind(EtcdClientSupplier.class).toInstance(
                    new EtcdClientSupplier(binder().getProvider(
                            Key.get(Executor.class, Names.named(EtcdModule.ETCD_THREADPOOL_BINDING))),
                            Providers.of(this), binder().getProvider(DeploymentMode.class),
                            binder().getProvider(TrustManagerSupplier.class),
                            binder().getProvider(ShutdownHookRegistry.class)));
        }

        private Throwable thrown;

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace();
            synchronized (this) {
                if (thrown == null) {
                    thrown = e;
                } else {
                    thrown.addSuppressed(e);
                }
            }
        }

        public void rethrow() throws Throwable {
            if (thrown != null) {
                throw thrown;
            }
        }

        static final class Shutdown implements Runnable {

            private final AtomicReference<Process> ref;

            @Inject
            Shutdown(ShutdownHookRegistry reg, AtomicReference<Process> ref) {
                this.ref = ref;
                reg.add(this);
            }

            @Override
            public synchronized void run() {
                Process p = ref.get();
                if (p != null) {
                    if (p.isAlive()) {
                        p.destroy();
                    }
                    int ix = 0;
                    while (p.isAlive()) {
                        try {
                            Thread.sleep(50);
                        } catch (InterruptedException ex) {

                        }
                        if (ix++ > 1000) {
                            break;
                        }
                    }
                }
            }
        }

        private int port = -1;
        private int port() {
            if (port == -1) {
                port = portFinder.findAvailableServerPort();
            }
            return port;
        }

        private String authority;

        synchronized String authority() {
            if (authority == null) {
                int thePort = port();
                authority = "0.0.0.0:" + thePort;
            }
            return authority;
        }

        void init() {
            if (initialized.compareAndSet(false, true)) {
                reallyInit();
            }
        }

        Path dir() {
            Path tmp = Paths.get(System.getProperty("java.io.tmpdir"));
            return tmp.resolve("etcdHarness_" + name());
        }

        void reallyInit() {
            List<String> cmdline = new ArrayList<>();
            Path etcdPath = etcdBinary();
            if (etcdPath == null) {
                throw new ConfigurationError("No etcd binary");
            }
            String name = name();
            cmdline.add(etcdPath.toString());
            cmdline.add("--force-new-cluster");
            cmdline.add("--name");
            cmdline.add(name);
            cmdline.add("--heartbeat-interval");
            cmdline.add("100");
            cmdline.add("--listen-client-urls");
            cmdline.add("http://" + authority());
            cmdline.add("--advertise-client-urls");
            cmdline.add("http://localhost:" + port());
            cmdline.add("--initial-cluster-state");
            cmdline.add("new");
            cmdline.add("--max-txn-ops");
            cmdline.add("10");
            cmdline.add("--max-request-bytes");
            cmdline.add("12384");
            cmdline.add("--grpc-keepalive-min-time");
            cmdline.add("1s");
            cmdline.add("--grpc-keepalive-timeout");
            cmdline.add("7s");
            try {
                Path dir = dir();
                if (!Files.exists(dir)) {
                    Files.createDirectory(dir);
                }
                Path data = dir.resolve("data");
                Path wal = dir.resolve("wal");
                Files.createDirectory(data);
                Files.createDirectory(wal);
                cmdline.add("--data-dir");
                cmdline.add(data.toString());
                cmdline.add("--wal-dir");
                cmdline.add(wal.toString());

                System.out.println("WILL RUN:\n");
                System.out.println(Strings.join(' ', cmdline));

                ProcessBuilder pb = new ProcessBuilder(cmdline);
                pb.directory(dir.toFile());
                pb.inheritIO();
                process.set(pb.start());
                Thread.sleep(1000);
            } catch (Exception ex) {
                Exceptions.chuck(ex);
            }
        }

        @Override
        public String getString(String string) {
            return getString(string, null);
        }

        @Override
        public String getString(String string, String string1) {
            switch (string) {
                // This can be called before the injector is created,
                // so handle it without creating a process
                case SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT:
                    return "3";
                default:
                    init();
                    switch (string) {
                        case EtcdModule.SETTINGS_KEY_ETCD_ENDPOINTS:
                            return authority();
                        case EtcdModule.SETTINGS_KEY_ETCD_PLAIN_TEXT:
                            return "true";
                        case EtcdModule.SETTINGS_KEY_ETCD_SESSION_TIMEOUT_SECONDS:
                            return "10";
                        case "etcdName":
                            return name();
                        default:
                            return settings.getString(string, string1);
                    }
            }
        }

        @Override
        public Set<String> allKeys() {
            HashSet<String> result = new HashSet<>(setOf(
                    EtcdModule.DEFAULT_ETCD_ENDPOINT,
                    EtcdModule.SETTINGS_KEY_ETCD_PLAIN_TEXT,
                    EtcdModule.SETTINGS_KEY_ETCD_SESSION_TIMEOUT_SECONDS,
                    EtcdModule.SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT,
                    "etcdName"));
            result.addAll(settings.allKeys());
            return result;
        }

        @Override
        public Properties toProperties() {
            Properties props = new Properties();
            for (String k : settings.allKeys()) {
                props.setProperty(k, settings.getString(k));
            }
            for (String k : allKeys()) {
                props.setProperty(k, getString(k));
            }
            return props;
        }

        @Override
        public Iterator<String> iterator() {
            return allKeys().iterator();
        }
    }
}
