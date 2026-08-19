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

import java.util.concurrent.Callable;

/**
 * Composite ClassLoader that resolves classes from the OSGi bundle first,
 * then falls back to the system/platform ClassLoader for JDK classes
 * that are not imported by the bundle (e.g. org.ietf.jgss, javax.security.*).
 *
 * This is needed because Kafka uses Class.forName() internally for SASL/SSL
 * login modules and security classes, which the OSGi bundle ClassLoader
 * cannot resolve when these packages are marked as optional imports.
 */
public class BundleBackedClassLoader extends ClassLoader {

    private final ClassLoader bundleClassLoader;
    private final ClassLoader fallbackClassLoader;

    public BundleBackedClassLoader(ClassLoader bundleClassLoader) {
        this(bundleClassLoader, null);
    }

    public BundleBackedClassLoader(ClassLoader bundleClassLoader, ClassLoader fallbackClassLoader) {
        super(bundleClassLoader);
        this.bundleClassLoader = bundleClassLoader;
        this.fallbackClassLoader = fallbackClassLoader;
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            return bundleClassLoader.loadClass(name);
        } catch (ClassNotFoundException e) {
            // During hot bundle updates CPI may temporarily keep running threads on a stale
            // bundle class loader. Fall back to the thread's original TCCL (if distinct) so the
            // call can continue with the freshest class space available.
            if (fallbackClassLoader != null
                    && fallbackClassLoader != bundleClassLoader
                    && fallbackClassLoader != this) {
                try {
                    return fallbackClassLoader.loadClass(name);
                } catch (ClassNotFoundException ignored) {
                    // Continue to system fallback.
                }
            }
            // Fallback to system classloader for JDK classes not wired by OSGi
            ClassLoader systemCl = ClassLoader.getSystemClassLoader();
            if (systemCl != null && systemCl != bundleClassLoader) {
                return systemCl.loadClass(name);
            }
            throw e;
        }
    }

    /**
     * Execute a callable with the BundleBackedClassLoader set as context ClassLoader.
     * Restores the original ClassLoader in a finally block.
     */
    public static <T> T withBundleClassLoader(Class<?> bundleClass, Callable<T> callable) throws Exception {
        ClassLoader originalCl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    new BundleBackedClassLoader(bundleClass.getClassLoader(), originalCl));
            return callable.call();
        } finally {
            Thread.currentThread().setContextClassLoader(originalCl);
        }
    }

    /**
     * Execute a runnable with the BundleBackedClassLoader set as context ClassLoader.
     * Restores the original ClassLoader in a finally block.
     */
    public static void runWithBundleClassLoader(Class<?> bundleClass, Runnable action) {
        ClassLoader originalCl = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    new BundleBackedClassLoader(bundleClass.getClassLoader(), originalCl));
            action.run();
        } finally {
            Thread.currentThread().setContextClassLoader(originalCl);
        }
    }
}
