package com.mastfrog.giulius.settings.etcd;

import com.mastfrog.giulius.Dependencies;
import com.mastfrog.giulius.ShutdownHookRegistry;
import com.mastfrog.settings.MutableSettings;
import com.mastfrog.util.thread.Receiver;
import java.io.FileDescriptor;
import java.io.IOException;
import java.net.InetAddress;
import java.security.Permission;

/**
 *
 * @author Tim Boudreau
 */
public class Test {

    public static void main(String[] args) throws InterruptedException, IOException {

//        System.setSecurityManager(new SM());
        System.setProperty("productionMode", "true");

        final Dependencies[] d = new Dependencies[1];
        Receiver<Dependencies> r = new Receiver<Dependencies>() {
            @Override
            public void receive(Dependencies object) {
                d[0] = object;
            }
        };
        MutableSettings s = EtcdModule.forNamespaceMutable("/", r);
        Thread.sleep(4000);

        System.out.println("KEYS: " + s);

        s.setString("funky", "hoodge-" + System.currentTimeMillis());

        System.out.println("VAL " + s.getString("funky"));

        s = EtcdModule.forNamespaceMutable("/");
        System.out.println("NUE VAL " + s.getString("funky"));

        d[0].shutdown();
        System.out.println("Deps shutdown");
    }

    static class SM extends SecurityManager {

        @Override
        public void checkAccess(Thread t) {
            Class top = null;
            StringBuilder sb = new StringBuilder();
            for (Class c : getClassContext()) {
                if (c == ShutdownHookRegistry.class) {
                    return;
                }
                if ("com.mastfrog.settings.SettingsRefreshInterval".equals(c.getName())) {
                    return;
                }
                if ("com.mastfrog.giulius.ShutdownHookRegistry$VMShutdownHookRegistry".equals(c.getName())) {
                    return;
                }
                if ("io.netty.util.concurrent.DefaultThreadFactor".equals(c.getName())) {
                    return;
                }
                if ("com.mastfrog.giulius.settings.etcd.EtcdSettings".equals(c.getName())) {
                    return;
                }
                if ("com.mastfrog.giulius.settings.etcd.EtcdSettings$Refresher".equals(c.getName())) {
                    return;
                }
                if ("java.util.TimerThread".equals(c.getName())) {
                    return;
                }
                if ("com.mastfrog.netty.http.client.HttpClient$TF".equals(c.getName())) {
                    return;
                }
                if ("java.util.logging.LogManager$Cleaner".equals(c.getName())) {
                    return;
                }
                if ("com.google.inject.internal.Finalizer".equals(c.getName())) {
                    return;
                }
                if ("com.sun.grizzly.http.SelectorThread".equals(c.getName())) {
                    return;
                }
                if ("com.sun.grizzly.tcp.http11.GrizzlyRequest".equals(c.getName())) {
                    return;
                }
                if ("sun.net.www.protocol.http.HttpURLConnection".equals(c.getName())) {
                    return;
                }
                if ("sun.net.www.protocol.http.HttpURLConnection$HttpInputStream".equals(c.getName())) {
                    return;
                }
                sb.append(c.getName()).append('\n');
            }
            throw new SecurityException("Random thread creation prohibited: \n" + sb);
        }

        @Override
        public void checkAccept(String host, int port) {

        }

        @Override
        public void checkConnect(String host, int port) {
        }

        @Override
        public void checkConnect(String host, int port, Object context) {
        }

        @Override
        public void checkCreateClassLoader() {
        }

        @Override
        public void checkDelete(String file) {
        }

        @Override
        public void checkExec(String cmd) {
        }

        @Override
        public void checkExit(int status) {
        }

        @Override
        public void checkLink(String lib) {
        }

        @Override
        public void checkListen(int port) {
        }

        @Override
        public void checkMemberAccess(Class<?> clazz, int which) {
        }

        @Override
        public void checkMulticast(InetAddress maddr) {
        }

        @Override
        public void checkMulticast(InetAddress maddr, byte ttl) {
        }

        @Override
        public void checkPackageAccess(String pkg) {
        }

        @Override
        public void checkPackageDefinition(String pkg) {
        }

        @Override
        public void checkPrintJobAccess() {
        }

        @Override
        public void checkPropertiesAccess() {
        }

        @Override
        public void checkPropertyAccess(String key) {
        }

        @Override
        public void checkRead(FileDescriptor fd) {
        }

        @Override
        public void checkRead(String file) {
        }

        @Override
        public void checkRead(String file, Object context) {
        }

        @Override
        public void checkSecurityAccess(String target) {
        }

        @Override
        public void checkSetFactory() {
        }

        @Override
        public void checkSystemClipboardAccess() {
        }

        @Override
        public void checkWrite(FileDescriptor fd) {
        }

        @Override
        public void checkWrite(String file) {
        }

        @Override
        public void checkAccess(ThreadGroup g) {
        }

        @Override
        public void checkPermission(Permission perm) {
        }

        @Override
        public void checkPermission(Permission perm, Object context) {
        }

    }
}
