/*-
 * #%L
 * Kafka Adapter Plus
 * %%
 * Copyright (C) 2026 Florian Kube
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */
package com.finkeflo.cpi.kafka;

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;

/**
 * Reflective, dependency-free view on the OSGi {@code Bundle} that loaded a given class.
 *
 * <p>Reflection is deliberate: the adapter only carries an unversioned, optional
 * {@code org.osgi.framework} import. Compiling against {@code osgi.core} would make bnd emit a
 * versioned import that may not resolve on the tenant's Equinox and would then break bundle
 * resolution outright. Outside OSGi (unit tests, plain JVM) every accessor degrades to
 * {@code null}/empty and {@link #isOsgi()} is {@code false}.
 */
final class OsgiBundleInfo {

    /** {@code org.osgi.framework.Bundle#UNINSTALLED}. */
    private static final int STATE_UNINSTALLED = 0x00000001;

    private final Object bundle;
    /** {@code org.osgi.framework.Bundle}; methods are looked up on the interface, never on the impl. */
    private final Class<?> bundleType;

    private OsgiBundleInfo(Object bundle, Class<?> bundleType) {
        this.bundle = bundle;
        this.bundleType = bundleType;
    }

    static OsgiBundleInfo of(Class<?> anchor) {
        try {
            ClassLoader loader = anchor.getClassLoader();
            Class<?> frameworkUtil = Class.forName("org.osgi.framework.FrameworkUtil", false, loader);
            Class<?> bundleType = Class.forName("org.osgi.framework.Bundle", false, loader);
            Object bundle = frameworkUtil.getMethod("getBundle", Class.class).invoke(null, anchor);
            if (bundle != null) {
                return new OsgiBundleInfo(bundle, bundleType);
            }
        } catch (Throwable ignored) {
            // Not running inside an OSGi framework, or org.osgi.framework is not wired.
        }
        return new OsgiBundleInfo(null, null);
    }

    boolean isOsgi() {
        return bundle != null;
    }

    Long getBundleId() {
        return (Long) call("getBundleId");
    }

    String getVersion() {
        Object v = call("getVersion");
        return v == null ? null : v.toString();
    }

    Long getLastModified() {
        return (Long) call("getLastModified");
    }

    /** {@code true} when the bundle object is known to be uninstalled (its class space is gone). */
    boolean isUninstalled() {
        Object state = call("getState");
        return state instanceof Integer && ((Integer) state) == STATE_UNINSTALLED;
    }

    /**
     * Number of revisions the framework still keeps for this bundle. {@code >1} means an older
     * revision is still wired (pending refresh). Uses the framework's own class loader for the
     * {@code org.osgi.framework.wiring} types, which the adapter does not import.
     */
    Integer getRevisionCount() {
        if (bundle == null) {
            return null;
        }
        try {
            Class<?> revisionsType = Class.forName("org.osgi.framework.wiring.BundleRevisions", false,
                    bundleType.getClassLoader());
            Object revisions = bundleType.getMethod("adapt", Class.class).invoke(bundle, revisionsType);
            if (revisions == null) {
                return null;
            }
            Object list = revisionsType.getMethod("getRevisions").invoke(revisions);
            return list instanceof List ? ((List<?>) list).size() : null;
        } catch (Throwable ignored) {
            return null;
        }
    }

    /**
     * {@code true} when the class loader of {@code anchor} is <em>not</em> the one behind the
     * bundle's current wiring — i.e. the anchor lives in a revision that an adapter update has
     * already replaced. Resource and native-library lookups over such a loader fail even after a
     * complete class warm-up (issue #148). {@code null} when it cannot be determined (outside OSGi
     * or wiring API not reachable).
     */
    static Boolean isClassSpaceStale(Class<?> anchor) {
        OsgiBundleInfo info = of(anchor);
        if (info.bundle == null) {
            return null;
        }
        try {
            Class<?> wiringType = Class.forName("org.osgi.framework.wiring.BundleWiring", false,
                    info.bundleType.getClassLoader());
            Object wiring = info.bundleType.getMethod("adapt", Class.class).invoke(info.bundle, wiringType);
            if (wiring == null) {
                // No current wiring at all: the bundle is uninstalled/unresolved — nothing is live.
                return Boolean.TRUE;
            }
            Object currentLoader = wiringType.getMethod("getClassLoader").invoke(wiring);
            return currentLoader != anchor.getClassLoader();
        } catch (Throwable ignored) {
            return null;
        }
    }

    String getHeader(String name) {
        if (bundle == null) {
            return null;
        }
        try {
            Object headers = bundleType.getMethod("getHeaders", String.class).invoke(bundle, "");
            if (headers instanceof Dictionary) {
                Object v = ((Dictionary<?, ?>) headers).get(name);
                return v == null ? null : v.toString();
            }
        } catch (Throwable ignored) {
            // fall through
        }
        return null;
    }

    /** Recursive {@code Bundle.findEntries(path, filePattern, true)}; empty outside OSGi. */
    List<URL> findEntries(String path, String filePattern) {
        if (bundle == null) {
            return Collections.emptyList();
        }
        try {
            Method m = bundleType.getMethod("findEntries", String.class, String.class, boolean.class);
            Object result = m.invoke(bundle, path, filePattern, true);
            List<URL> urls = new ArrayList<>();
            if (result instanceof Enumeration) {
                Enumeration<?> e = (Enumeration<?>) result;
                while (e.hasMoreElements()) {
                    Object o = e.nextElement();
                    if (o instanceof URL) {
                        urls.add((URL) o);
                    }
                }
            }
            return urls;
        } catch (Throwable ignored) {
            return Collections.emptyList();
        }
    }

    /** {@code Bundle.getEntry(path)}; {@code null} outside OSGi or when absent. */
    URL getEntry(String path) {
        Object url = call("getEntry", path);
        return url instanceof URL ? (URL) url : null;
    }

    /**
     * One-line class-space fingerprint for start-up diagnostics: which bundle revision a route is
     * wired to. After an adapter update, routes still showing the <em>old</em>
     * {@code bundleLastModified}/{@code loader} are the ones at risk (issue #148).
     */
    static String describeClassSpace(Class<?> anchor) {
        OsgiBundleInfo info = of(anchor);
        return "bundleVersion=" + info.getVersion()
                + " bundleId=" + info.getBundleId()
                + " bundleLastModified=" + info.getLastModified()
                + " revisions=" + info.getRevisionCount()
                + " stale=" + isClassSpaceStale(anchor)
                + " loader=" + System.identityHashCode(anchor.getClassLoader());
    }

    private Object call(String method, String arg) {
        if (bundle == null) {
            return null;
        }
        try {
            return bundleType.getMethod(method, String.class).invoke(bundle, arg);
        } catch (Throwable ignored) {
            return null;
        }
    }

    private Object call(String method) {
        if (bundle == null) {
            return null;
        }
        try {
            return bundleType.getMethod(method).invoke(bundle);
        } catch (Throwable ignored) {
            return null;
        }
    }
}
