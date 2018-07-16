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

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.justinsb.etcd.EtcdClient;
import com.mastfrog.giulius.Dependencies;
import com.mastfrog.giulius.ShutdownHookRegistry;
import com.mastfrog.netty.http.client.HttpClient;
import com.mastfrog.settings.MutableSettings;
import com.mastfrog.settings.Settings;
import com.mastfrog.settings.SettingsBuilder;
import com.mastfrog.util.preconditions.Exceptions;
import com.mastfrog.util.thread.Receiver;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.joda.time.Duration;

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
    public static final String SETTINGS_KEY_ETCD_URL = "etcd.url";
    /**
     * Value of the default etcd url
     */
    public static final String DEFAULT_ETCD_URL = "http://127.0.0.1:4001";

    /**
     * Settings key for the interval in seconds between settings refreshes
     */
    public static final String SETTINGS_KEY_ETCD_REFRESH_INTERVAL_SECONDS = "etcd.settings.refresh.seconds";
    public static final long DEFAULT_ETCD_REFRESH_INTERVAL_SECONDS = 23;

    /**
     * Guice &#064;Named injection key for a Joda-Time Duration with the refresh
     * interval
     */
    public static final String GUICE_BINDING_ETCD_REFRESH_INTERVAL = "etcd.settings.refresh.interval";
    /**
     * Guice &#064;Named injection key for the ScheduledExecutorService which
     * runs refresh
     */
    public static final String GUICE_BINDING_ETCD_REFRESH_THREAD_POOL = "etcd.settings.refresh.threadpool";

    public static final String GUICE_BINDING_HTTP_CLIENT = "etcd.http.client";

    /**
     * The namespace or keyspace to look in within etcd for settings. The
     * default is /, meaning everything. If a one etcd cluser may serve multiple
     * applications, it is a good idea to use a namespace.
     */
        public static final String SETTINGS_KEY_ETCD_NAMESPACE = "etcd.namespace";
    /**
     * The default namespace
     */
    public static final String DEFAULT_ETCD_NAMESPACE = "/";

    /**
     * The number of times to try all the known etcd servers to answer a call to
     * MetaEtcdClient before raising an error to the caller.
     */
    public static final String SETTINGS_KEY_MAX_RETRIES = "etcd.max.retries";
    public static final int DEFAULT_MAX_RETRIES = 10;
    /**
     * The amount of time after too many failures, during which an etcd client
     * should not be in the list of first-choice ones to query. In seconds.
     */
    public static final String SETTINGS_KEY_FAIL_WINDOW = "etcd.fail.window.seconds";
    public static final int DEFAULT_FAIL_WINDOW = 30;
    /**
     * The maximum number of times a call to a single etcd server should fail
     * before it is taken out of the list of preferred ones, and calling it
     * becomes a last resort if all others fail.
     */
    public static final String SETTINGS_KEY_MAX_FAILS_TO_DISABLE = "etcd.max.fails.to.disable";
    /**
     * The default number of fails needed to consider an etcd server
     * out-of-business
     */
    public static final int DEFAULT_MAX_FAILS_TO_DISABLE = 3;

    public EtcdModule(Settings settings) {
        this.settings = settings;
    }

    @Override
    protected void configure() {
        try {
            String url = settings.getString(SETTINGS_KEY_ETCD_URL, DEFAULT_ETCD_URL);

            Duration refresh = Duration.standardSeconds(settings.getLong(SETTINGS_KEY_ETCD_REFRESH_INTERVAL_SECONDS, DEFAULT_ETCD_REFRESH_INTERVAL_SECONDS));
            bind(Duration.class).annotatedWith(Names.named(GUICE_BINDING_ETCD_REFRESH_INTERVAL)).toInstance(refresh);
            bind(ScheduledExecutorService.class).annotatedWith(Names.named(GUICE_BINDING_ETCD_REFRESH_THREAD_POOL)).toInstance(Executors.newSingleThreadScheduledExecutor());
            bind(ThreadPoolAndHttpClientShutdown.class).asEagerSingleton();
//            CloseableHttpAsyncClient httpclient = buildDefaultHttpClient();
//            bind(CloseableHttpAsyncClient.class).toInstance(httpclient);

            HttpClient httpClient = HttpClient.builder()
                            .maxChunkSize(512)
                            .noCompression()
                            .maxInitialLineLength(255)
                            .followRedirects()
                            .threadCount(1)
                            .dontSend100Continue()
                            .build();
            bind(HttpClient.class).annotatedWith(Names.named(GUICE_BINDING_HTTP_CLIENT))
                    .toInstance(httpClient);

            List<EtcdClient> clients = new ArrayList<>();
            for (String u : url.split(",")) {
                clients.add(new EtcdClient(new URI(u), httpClient));
            }
            EtcdClient[] arr = clients.toArray(new EtcdClient[0]);
            MetaEtcdClient meta = new MetaEtcdClient(settings, arr);
            bind(EtcdClient.class).toProvider(meta);
            bind(MetaEtcdClient.class).toInstance(meta);
            bind(EtcdClient[].class).toInstance(arr);
        } catch (URISyntaxException ex) {
            Exceptions.chuck(ex);
        }
    }

//    static CloseableHttpAsyncClient buildDefaultHttpClient() {
//        // TODO: Increase timeout??
//        RequestConfig requestConfig = RequestConfig.custom().build();
//        CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom().setDefaultRequestConfig(requestConfig).build();
//        httpClient.start();
//        return httpClient;
//    }
    public static Settings forNamespace(String namespace, Receiver<?>... deps) throws IOException {
        return create(DEFAULT_ETCD_URL, namespace, deps);
    }

    public static Settings create(String url, Receiver<?>... deps) throws IOException {
        return create(url, "/", deps);
    }

    public static Settings create(String url, String namespace, Receiver<?>... deps) throws IOException {
        SettingsBuilder sb = new SettingsBuilder(namespace).addDefaultLocations()
                .add(SETTINGS_KEY_ETCD_NAMESPACE, namespace)
                .add(SETTINGS_KEY_ETCD_URL, url);
        Settings s = sb.build();
        return create(s, deps);
    }

    @SuppressWarnings("unchecked")
    public static Settings create(Settings s, Receiver<?>... dep) throws IOException {
        Dependencies deps = new Dependencies(s, new EtcdModule(s));
        for (Receiver r : dep) {
            r.receive(deps);
        }
        return deps.getInstance(EtcdSettings.class);
    }

    public static MutableSettings forNamespaceMutable(String namespace, Receiver<?>... deps) throws IOException {
        return createMutable(DEFAULT_ETCD_URL, namespace, deps);
    }

    public static MutableSettings createMutable(String url, Receiver<?>... deps) throws IOException {
        return createMutable(url, "/", deps);
    }

    public static MutableSettings createMutable(String url, String namespace, Receiver<?>... deps) throws IOException {
        SettingsBuilder sb = new SettingsBuilder(namespace).addDefaultLocations()
                .add(SETTINGS_KEY_ETCD_NAMESPACE, namespace)
                .add(SETTINGS_KEY_ETCD_URL, url);
        Settings s = sb.build();
        return createMutable(s, deps);
    }

    @SuppressWarnings("unchecked")
    public static MutableSettings createMutable(Settings s, Receiver<?>... dep) throws IOException {
        Dependencies deps = new Dependencies(s, new EtcdModule(s));
        for (Receiver r : dep) {
            r.receive(deps);
        }
        return deps.getInstance(MutableEtcdSettings.class);
    }

    private static class ThreadPoolAndHttpClientShutdown implements Runnable {

        private final HttpClient httpClient;
        private final EtcdErrorHandler handler;

//        private final CloseableHttpAsyncClient httpClient;
        @Inject
        ThreadPoolAndHttpClientShutdown(@Named(GUICE_BINDING_ETCD_REFRESH_THREAD_POOL) ScheduledExecutorService svc, ShutdownHookRegistry reg, /*CloseableHttpAsyncClient httpClient*/ HttpClient httpClient, EtcdErrorHandler handler) {
            reg.add(svc);
            this.httpClient = httpClient;
            reg.add(this);
            this.handler = handler;
        }

        public void run() {
            try {
//                httpClient.close();
                httpClient.shutdown();
            } catch (Exception ex) {
                handler.onException(ex);
            }
        }
    }
}
