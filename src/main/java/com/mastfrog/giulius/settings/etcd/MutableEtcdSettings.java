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
import com.justinsb.etcd.EtcdClientException;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.GUICE_BINDING_ETCD_REFRESH_INTERVAL;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.GUICE_BINDING_ETCD_REFRESH_THREAD_POOL;
import static com.mastfrog.giulius.settings.etcd.EtcdModule.SETTINGS_KEY_ETCD_NAMESPACE;
import com.mastfrog.settings.MutableSettings;
import java.util.concurrent.ScheduledExecutorService;
import org.joda.time.Duration;

/**
 * Mutable implementation of Settings. The mutator methods <i>do</i> make calls
 * over the wire.
 *
 * @author Tim Boudreau
 */
@Singleton
public final class MutableEtcdSettings extends EtcdSettings implements MutableSettings {

    private final MetaEtcdClient client;
    private final EtcdErrorHandler errs;

    @Inject
    public MutableEtcdSettings(MetaEtcdClient client,
            @Named(SETTINGS_KEY_ETCD_NAMESPACE) String namespace,
            @Named(GUICE_BINDING_ETCD_REFRESH_THREAD_POOL) ScheduledExecutorService svc,
            @Named(GUICE_BINDING_ETCD_REFRESH_INTERVAL) Duration interval,
            EtcdErrorHandler errs) {
        super(client, namespace, svc, interval, errs);
        this.client = client;
        this.errs = errs;
    }

    @Override
    public void setInt(String name, int value) {
        setString(name, Integer.toString(value));
    }

    @Override
    public void setBoolean(String name, boolean val) {
        setString(name, Boolean.toString(val));
    }

    @Override
    public void setDouble(String name, double val) {
        setString(name, Double.toString(val));
    }

    @Override
    public void setLong(String name, long val) {
        setString(name, Long.toString(val));
    }

    @Override
    public void setString(String name, String value) {
        try {
            client.set(namespace + '/' + name, value);
            pairs.get().put(name, value);
        } catch (EtcdClientException ex) {
            errs.onException(ex);
        }
    }

    @Override
    public void clear(String name) {
        try {
            client.delete(namespace + '/' + name);
            pairs.get().remove(name);
        } catch (EtcdClientException ex) {
            errs.onException(ex);
        }
    }
}
