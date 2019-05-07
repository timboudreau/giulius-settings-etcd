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

import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.RangeResponse;
import com.ibm.etcd.client.kv.KvClient;
import com.mastfrog.settings.Settings;
import static java.nio.charset.StandardCharsets.UTF_8;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 *
 * @author Tim Boudreau
 */
class KvSettings implements Settings {

    private final Provider<KvClient> clientProvider;
    private final Set<String> knownKeys = Sets.newConcurrentHashSet();

    @Inject
    KvSettings(Provider<KvClient> clientProvider) {
        this.clientProvider = clientProvider;
    }

    KvClient client() {
        return clientProvider.get();
    }

    @Override
    public String getString(String string) {
        return getString(string, null);
    }

    @Override
    public String getString(String string, String string1) {
        RangeResponse rr = client().get(ByteString.copyFrom(string, UTF_8)).sync();
        if (rr.getKvsCount() <= 0) {
            return string1;
        }
        ByteString result = rr.getKvs(0).getValue();
        String res = result.toString(UTF_8);
        knownKeys.add(string);
        return res;
    }

    @Override
    public Set<String> allKeys() {
        return knownKeys;
    }

    @Override
    public Properties toProperties() {
        Properties result = new Properties();
        for (String key : knownKeys) {
            String val = getString(key);
            if (val != null) {
                result.setProperty(key, val);
            }
        }
        return result;
    }

    @Override
    public Iterator<String> iterator() {
        return knownKeys.iterator();
    }
}
