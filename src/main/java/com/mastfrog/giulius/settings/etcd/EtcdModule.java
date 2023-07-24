/*
 * The MIT License
 *
 * Copyright 2014 Tim Boudreau.
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

import com.google.common.io.ByteSource;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.lease.LeaseClient;
import com.ibm.etcd.client.lease.PersistentLease;
import com.ibm.etcd.client.lock.LockClient;
import com.mastfrog.function.throwing.ThrowingSupplier;
import com.mastfrog.giulius.thread.ThreadModule;
import com.mastfrog.settings.Settings;
import com.mastfrog.shutdown.hooks.DeploymentMode;
import com.mastfrog.shutdown.hooks.ShutdownHookRegistry;
import com.mastfrog.util.preconditions.ConfigurationError;
import com.mastfrog.util.preconditions.Exceptions;
import com.mastfrog.util.strings.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.inject.Singleton;
import javax.net.ssl.TrustManagerFactory;

/**
 * Sets up bindings for Etcd
 *
 * @author Tim Boudreau
 */
public class EtcdModule extends AbstractModule {

    private final Settings settings;
    /**
     * Settings key for the etcd client url; may be a comma-delimited list of
     * clients, in which case calls will attempt to use all the specified
     * clients before failing. Default is to use the local machine.
     */
    public static final String SETTINGS_KEY_ETCD_ENDPOINTS = "etcd.endpoints";
    /**
     * Value of the default etcd url
     */
    public static final String DEFAULT_ETCD_ENDPOINT = "0.0.0.0:2379";

    public static final String SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT = "etcd.network.threads";
    private static final int DEFAULT_ETCD_NETWORK_THREAD_COUNT = 3;

    static final String ETCD_THREADPOOL_BINDING = "_etcdThreads";

    public static final String SETTINGS_KEY_ETCD_SEND_VIA_EVENT_LOOP = "etcd.send.via.event.loop";
    static final boolean DEFAULT_ETCD_SEND_VIA_EVENT_LOOP = true;

    public static final String SETTINGS_KEY_ETCD_IMMEDIATE_AUTH = "etcd.immediate.auth";
    static final boolean DEFAULT_ETCD_IMMEDIATE_AUTH = false;

    public static final String SETTINGS_KEY_ETCD_MAX_INBOUND_MESSAGE_SIZE = "etcd.max.inbound.message.size";
    static final int DEFAULT_ETCD_MAX_INBOUND_MESSAGE_SIZE = 2048;

    public static final String SETTINGS_KEY_ETCD_DEFAULT_TIMEOUT_MS = "etcd.default.timeout.ms";

    public static final String SETTINGS_KEY_ETCD_USER = "etcd.user";
    public static final String SETTINGS_KEY_ETCD_PASSWORD = "etcd.password";
    public static final String ENV_KEY_ETCD_USER = "ETCD_USER";
    public static final String ENV_KEY_ETCD_PASSWORD = "ETCD_PASSWORD";

    public static final String SETTINGS_KEY_ETCD_SESSION_TIMEOUT_SECONDS = "etc.dsession.timeout.seconds";
    public static final String SETTINGS_KEY_ETCD_PLAIN_TEXT = "etcd.plain.text";
    public static final String SETTINGS_KEY_ETCD_CERT = "etcd.cert.file";

    public EtcdModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        ThreadModule m = new ThreadModule().builder(ETCD_THREADPOOL_BINDING).daemon().forkJoin()
                .withExplicitThreadCount(settings.getInt(SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT, DEFAULT_ETCD_NETWORK_THREAD_COUNT))
                .bind();

        install(m);
        bind(EtcdClient.class).toProvider(EtcdClientProvider.class);
        bind(TrustManagerSupplier.class).toInstance(new TrustManagerSupplier(trust));

        Provider<EtcdClient> etcdClientProvider = binder().getProvider(EtcdClient.class);
        bind(KvClient.class).toProvider(new XformProvider<>(etcdClientProvider,
                client -> client.getKvClient()));
        bind(LeaseClient.class).toProvider(new XformProvider<>(etcdClientProvider,
                client -> client.getLeaseClient()));
        bind(LockClient.class).toProvider(new XformProvider<>(etcdClientProvider,
                client -> client.getLockClient()));
        bind(PersistentLease.class).toProvider(new XformProvider<>(etcdClientProvider,
                client -> client.getSessionLease()));
    }

    private TrustManagerFactory trust;

    public EtcdModule setTrustManager(TrustManagerFactory trust) {
        if (this.trust != null) {
            throw new ConfigurationError("Trust manager already set");
        }
        this.trust = trust;
        return this;
    }

    static final class XformProvider<X, T> implements Provider<T> {

        private final Provider<X> orig;
        private final Function<X, T> xform;
        private volatile T cached;

        public XformProvider(Provider<X> orig, Function<X, T> xform) {
            this.orig = orig;
            this.xform = xform;
        }

        @Override
        public T get() {
            T cached = this.cached;
            if (cached == null) {
                synchronized (this) {
                    cached = this.cached;
                    if (cached == null) {
                        this.cached = cached = xform.apply(orig.get());
                    }
                }
            }
            return cached;
        }
    }

    static final class TrustManagerSupplier implements Supplier<TrustManagerFactory> {

        private final TrustManagerFactory factory;

        public TrustManagerSupplier(TrustManagerFactory factory) {
            this.factory = factory;
        }

        @Override
        public TrustManagerFactory get() {
            return factory;
        }

    }

    @Singleton
    static class EtcdClientSupplier implements ThrowingSupplier<EtcdClient> {

        private final Provider<Executor> netExecutor;
        private final Provider<Settings> settings;
        private final Provider<DeploymentMode> mode;
        private final Provider<TrustManagerSupplier> trust;
        private final Provider<ShutdownHookRegistry> reg;

        @Inject
        EtcdClientSupplier(@Named(ETCD_THREADPOOL_BINDING) Provider<Executor> netExecutor,
                Provider<Settings> settings, Provider<DeploymentMode> mode,
                Provider<TrustManagerSupplier> trust, Provider<ShutdownHookRegistry> reg) {
            this.netExecutor = netExecutor;
            this.settings = settings;
            this.mode = mode;
            this.trust = trust;
            this.reg = reg;
        }

        String settingsOrEnv(String envKey, String settingsKey, String defaultValue) {
            String result = System.getenv(envKey);
            if (result == null) {
                result = settings.get().getString(settingsKey, defaultValue);
            }
            return result;
        }

        @Override
        public EtcdClient get() throws Exception {
            List<String> l = Arrays.asList(Strings.split(',',
                    settings.get().getString(SETTINGS_KEY_ETCD_ENDPOINTS, DEFAULT_ETCD_ENDPOINT)));
            EtcdClient.Builder bldr = EtcdClient.forEndpoints(l)
                    .withUserExecutor(netExecutor.get())
                    .withThreadCount(settings.get().getInt(SETTINGS_KEY_ETCD_NETWORK_THREAD_COUNT, DEFAULT_ETCD_NETWORK_THREAD_COUNT))
                    .sendViaEventLoop(settings.get().getBoolean(SETTINGS_KEY_ETCD_SEND_VIA_EVENT_LOOP, DEFAULT_ETCD_SEND_VIA_EVENT_LOOP))
                    .withMaxInboundMessageSize(settings.get().getInt(SETTINGS_KEY_ETCD_MAX_INBOUND_MESSAGE_SIZE, DEFAULT_ETCD_MAX_INBOUND_MESSAGE_SIZE));
            Long timeout = settings.get().getLong(SETTINGS_KEY_ETCD_DEFAULT_TIMEOUT_MS);
            if (timeout != null) {
                bldr = bldr.withDefaultTimeout(timeout, TimeUnit.MILLISECONDS);
            }
            if (settings.get().getBoolean(SETTINGS_KEY_ETCD_IMMEDIATE_AUTH, DEFAULT_ETCD_IMMEDIATE_AUTH)) {
                bldr = bldr.withImmediateAuth();
            }
            String user = settingsOrEnv(ENV_KEY_ETCD_USER, SETTINGS_KEY_ETCD_USER, null);
            String password = settingsOrEnv(ENV_KEY_ETCD_PASSWORD, SETTINGS_KEY_ETCD_PASSWORD, null);
            if ((user == null) != (password == null)) {
                throw new ConfigurationError("Etcd user and password must either BOTH be null or both be set, but got " + user + ":" + password);
            }
            if (user != null) {
                bldr = bldr.withCredentials(user, password);
            }
            boolean plainText = settings.get().getBoolean(SETTINGS_KEY_ETCD_PLAIN_TEXT, !mode.get().isProduction());
            if (plainText) {
                bldr = bldr.withPlainText();
            }
            String certFile = settings.get().getString(SETTINGS_KEY_ETCD_CERT);
            if (certFile != null) {
                bldr = bldr.withCaCert(new FileByteSource(certFile));
            }
            if (trust.get().get() != null) {
                bldr.withTrustManager(trust.get().get());
            }
            Integer sessionTimeoutSeconds = settings.get().getInt(SETTINGS_KEY_ETCD_SESSION_TIMEOUT_SECONDS);
            if (sessionTimeoutSeconds != null) {
                bldr = bldr.withSessionTimeoutSecs(sessionTimeoutSeconds);
            }
            EtcdClient result = bldr.build();
            reg.get().addResource(result);
            return result;
        }
    }

    static final class FileByteSource extends ByteSource {

        private final String file;

        FileByteSource(String file) {
            this.file = file;
        }

        @Override
        public InputStream openStream() throws IOException {
            Path path = Paths.get(file);
            if (!Files.exists(path)) {
                throw new ConfigurationError("File does not exist: " + file);
            }
            if (Files.isDirectory(path)) {
                throw new ConfigurationError("File is a directory: " + file);
            }
            return Files.newInputStream(path, StandardOpenOption.READ);
        }
    }

    static class EtcdClientProvider extends ReplaceableProvider<EtcdClient> {

        @Inject
        public EtcdClientProvider(EtcdClientSupplier supp) {
            super(supp);
        }

    }

    static class ReplaceableProvider<T> implements Provider<T> {

        private final AtomicReference<Provider<T>> ref = new AtomicReference<>();

        public ReplaceableProvider(ThrowingSupplier<T> supp) {
            ref.set(new LazyProvider<>(supp, ref));
        }

        @Override
        public T get() {
            return ref.get().get();
        }
    }

    static final class LazyProvider<T> implements Provider<T> {

        private final ThrowingSupplier<T> supp;
        private final AtomicReference<Provider<T>> ref;

        public LazyProvider(ThrowingSupplier<T> supp, AtomicReference<Provider<T>> ref) {
            this.supp = supp;
            this.ref = ref;
        }

        @Override
        public synchronized T get() {
            try {
                T result = supp.get();
                ref.set(new FixedProvider<>(result));
                return result;
            } catch (Exception ex) {
                return Exceptions.chuck(ex);
            }
        }
    }

    static final class FixedProvider<T> implements Provider<T> {

        private final T obj;

        public FixedProvider(T obj) {
            this.obj = obj;
        }

        @Override
        public T get() {
            return obj;
        }
    }
}
