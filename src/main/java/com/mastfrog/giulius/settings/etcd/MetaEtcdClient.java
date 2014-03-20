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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdResult;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.DEFAULT_FAIL_WINDOW;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.DEFAULT_MAX_FAILS_TO_DISABLE;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.DEFAULT_MAX_RETRIES;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_FAIL_WINDOW;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_MAX_FAILS_TO_DISABLE;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_MAX_RETRIES;
import com.mastfrog.settings.Settings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.joda.time.DateTimeUtils;
import org.joda.time.Duration;

/**
 * A wrapper for a cluster of etcd clients to try as failovers.
 *
 * @author Tim Boudreau
 */
public class MetaEtcdClient implements Provider<EtcdClient> {

    private final List<Entry> entries;
    private final int maxRetries;

    @Inject
    public MetaEtcdClient(Settings settings, EtcdClient... clients) {
        if (clients == null || clients.length == 0) {
            throw new IllegalArgumentException("No clients: " + clients);
        }
        maxRetries = settings.getInt(SETTINGS_KEY_MAX_RETRIES, DEFAULT_MAX_RETRIES);
        Duration failWindow = Duration.standardSeconds(settings.getInt(SETTINGS_KEY_FAIL_WINDOW, DEFAULT_FAIL_WINDOW));
        List<Entry> e = new ArrayList<>();
        int maxFails = settings.getInt(SETTINGS_KEY_MAX_FAILS_TO_DISABLE, DEFAULT_MAX_FAILS_TO_DISABLE);

        for (EtcdClient client : clients) {
            e.add(new Entry(client, failWindow, maxFails));
        }
        entries = ImmutableList.copyOf(e);
    }

    private Entry client(boolean unavailableOk) {
        List<Entry> ens = entries;
        if (unavailableOk) {
            ens = new LinkedList<Entry>(ens);
            Collections.shuffle(ens);
        }
        for (Entry e : entries) {
            if (e.available || unavailableOk) {
                return e;
            }
        }
        return null;
    }

    private EtcdResult tryClients(EtcdTask task) throws EtcdClientException {
        for (int i = 0; i < maxRetries; i++) {
            Entry e = client(false);
            if (e == null) {
                e = client(true);
            }
            try {
                final Entry e1 = e;
                return e1.run(task);
            } catch (Exception ex) {
                if (i == maxRetries - 1) {
                    throw ex;
                }
            }
        }
        throw new AssertionError("Should not get here");
    }

    public EtcdResult get(final String key) throws EtcdClientException {
        EtcdTask task = new EtcdTask() {

            @Override
            public EtcdResult run(EtcdClient client) throws EtcdClientException {
                return client.get(key);
            }
        };
        return tryClients(task);
    }

    public EtcdResult delete(final String key) throws EtcdClientException {
        EtcdTask task = new EtcdTask() {

            @Override
            public EtcdResult run(EtcdClient client) throws EtcdClientException {
                return client.delete(key);
            }
        };
        return tryClients(task);
    }

    public EtcdResult set(final String key, final String value) throws EtcdClientException {
        EtcdTask task = new EtcdTask() {

            @Override
            public EtcdResult run(EtcdClient client) throws EtcdClientException {
                return client.set(key, value);
            }

        };
        return tryClients(task);
    }

    public EtcdResult set(final String key, final String value, final int ttl) throws EtcdClientException {
        EtcdTask task = new EtcdTask() {

            @Override
            public EtcdResult run(EtcdClient client) throws EtcdClientException {
                return client.set(key, value, ttl);
            }

        };
        return tryClients(task);
    }

    public EtcdResult listChildren(final String key) throws EtcdClientException {
        EtcdTask task = new EtcdTask() {

            @Override
            public EtcdResult run(EtcdClient client) throws EtcdClientException {
                return client.listChildren(key);
            }

        };
        return tryClients(task);
    }

    @Override
    public EtcdClient get() {
        Entry e = client(false);
        if (e == null) {
            e = client(true);
        }
        return e.get();
    }

    private static final class Entry {

        private final EtcdClient client;
        private final AtomicLong lastFailed = new AtomicLong();
        private final AtomicInteger failCount = new AtomicInteger();
        private volatile boolean available;
        private final int maxFails;
        private final Duration failWindow;

        public Entry(EtcdClient client, Duration failWindow, int maxFails) {
            this.client = client;
            this.failWindow = failWindow;
            this.maxFails = maxFails;
        }

        EtcdClient get() {
            return client;
        }

        EtcdResult run(EtcdTask task) throws EtcdClientException {
            try {
                EtcdResult res = task.run(client);
                onSuccess();
                return res;
            } catch (EtcdClientException ex) {
                onFail(ex);
                throw ex;
            }
        }

        boolean inFailWindow() {
            Duration timeSinceLastFail = new Duration(DateTimeUtils.currentTimeMillis() - lastFailed.get());
            boolean result = timeSinceLastFail.isShorterThan(failWindow);
            if (!result) {
                failCount.set(0);
            }
            return result;
        }

        void onSuccess() {
            failCount.set(0);
            available = true;
        }

        void onFail(EtcdClientException ex) {
            boolean inFailWindow = inFailWindow();
            System.out.println("onFail in window " + inFailWindow);
            lastFailed.set(DateTimeUtils.currentTimeMillis());
            if (failCount.incrementAndGet() > maxFails && inFailWindow) {
                System.out.println("Set available to false");
                available = false;
            } else {
                System.out.println("setAvailable to true");
                available = true;
            }
        }

        public boolean isAvailable() {
            return available || !inFailWindow();
        }
    }

    interface EtcdTask {

        EtcdResult run(EtcdClient client) throws EtcdClientException;
    }
}
