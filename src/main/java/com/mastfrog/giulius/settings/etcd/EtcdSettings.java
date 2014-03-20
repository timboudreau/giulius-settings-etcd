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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.justinsb.etcd.EtcdNode;
import com.justinsb.etcd.EtcdResult;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.GUICE_BINDING_ETCD_REFRESH_INTERVAL;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.GUICE_BINDING_ETCD_REFRESH_THREAD_POOL;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_ETCD_NAMESPACE;
import com.mastfrog.settings.Settings;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.joda.time.Duration;

/**
 * Implementation of settings over etcd. Use static methods on EtcdModule to
 * create these simply.
 * <p/>
 * The contents of this Settings are loaded and cached - a simple call to get a
 * value will not go over the wire. A refresh interval is specified to
 * periodically re-get the settings.
 *
 * @author Tim Boudreau
 */
@Singleton
public class EtcdSettings implements Settings {

    protected final String namespace;
    protected final AtomicReference<Map<String, String>> pairs = new AtomicReference<>();

    /**
     * Create a Settings which takes its values from an etcd subnode.
     *
     * @param client An etcd client
     * @param namespace The subnode under which to find settings, like /mystuff
     * @param svc A thread pool to use for refreshing data
     * @param interval The frequency with which to refresh data
     * @param errs An error handler to log exceptions - don't want to dictate
     * how that is handled here
     */
    @Inject
    public EtcdSettings(MetaEtcdClient client,
            @Named(SETTINGS_KEY_ETCD_NAMESPACE) String namespace,
            @Named(GUICE_BINDING_ETCD_REFRESH_THREAD_POOL) ScheduledExecutorService svc,
            @Named(GUICE_BINDING_ETCD_REFRESH_INTERVAL) Duration interval,
            EtcdErrorHandler errs) {
        this.namespace = namespace.startsWith("/") ? namespace : '/' + namespace;
        pairs.set(new HashMap<String, String>());
        Refresher r = new Refresher(pairs, namespace, client, errs);
        r.run();
        svc.scheduleAtFixedRate(r, interval.getMillis(), interval.getMillis(), TimeUnit.MILLISECONDS);
    }

    private String prop(String s) {
        String result = pairs.get().get(s);
        return result == null ? null : result.trim();
    }

    @Override
    public Set<String> allKeys() {
        return pairs.get().keySet();
    }

    @Override
    public Integer getInt(String name) {
        String prop = prop(name);
        return prop == null ? null : Integer.parseInt(prop);
    }

    @Override
    public int getInt(String name, int defaultValue) {
        String prop = prop(name);
        return prop == null ? defaultValue : Integer.parseInt(prop);
    }

    @Override
    public Long getLong(String name) {
        String prop = prop(name);
        return prop == null ? null : Long.parseLong(prop);
    }

    @Override
    public long getLong(String name, long defaultValue) {
        String prop = prop(name);
        return prop == null ? defaultValue : Long.parseLong(prop);
    }

    @Override
    public String getString(String name) {
        return prop(name);
    }

    @Override
    public Boolean getBoolean(String name) {
        String prop = prop(name);
        return prop == null ? null : Boolean.parseBoolean(prop);
    }

    @Override
    public boolean getBoolean(String name, boolean defaultValue) {
        String prop = prop(name);
        return prop == null ? defaultValue : Boolean.parseBoolean(prop);
    }

    @Override
    public Double getDouble(String name) {
        String prop = prop(name);
        return prop == null ? null : Double.parseDouble(prop);
    }

    @Override
    public double getDouble(String name, double defaultValue) {
        String prop = prop(name);
        return prop == null ? defaultValue : Double.parseDouble(prop);
    }

    @Override
    public String getString(String name, String defaultValue) {
        String result = prop(name);
        return result == null ? defaultValue : result;
    }

    @Override
    public Properties toProperties() {
        Properties p = new Properties();
        p.putAll(pairs.get());
        return p;
    }

    @Override
    public Iterator<String> iterator() {
        return pairs.get().keySet().iterator();
    }

    public String toString() {
        return "etcd{" + pairs.get() + "}";
    }

    private static class Refresher implements Runnable {

        private final AtomicReference<Map<String, String>> ref;
        private final String namespace;
        private final MetaEtcdClient client;
        private final EtcdErrorHandler errs;
        private int rcount;

        public Refresher(AtomicReference<Map<String, String>> ref, String namespace, MetaEtcdClient client, EtcdErrorHandler errs) {
            this.ref = ref;
            this.namespace = namespace;
            this.client = client;
            this.errs = errs;
        }

        @Override
        public void run() {
            try {
                Thread.currentThread().setDaemon(true);
                Thread.currentThread().setName("Etcd settings refresh thread");
            } catch (IllegalThreadStateException ex) {
                // will happen on the first call which is done sync on the main
                // thread
            }
            try {
                Map<String, String> pairs = new HashMap<>();
                EtcdResult res = client.listChildren(namespace);
                if (res.errorCode != null) {
                    System.out.println("ERROR CODE " + res.errorCode + " INDEX " + res.errorIndex);
                }
                for (EtcdNode nd : res.node.nodes) {
                    String key = nd.key;
                    String val = nd.value;
                    if (val != null) {
                        if (key.startsWith("/")) {
                            key = key.substring(1);
                        }
                        pairs.put(key, val);
                    }
                }
                ref.set(pairs);
            } catch (Exception ex) {
                errs.onException(ex);
            }
        }
    }
}
