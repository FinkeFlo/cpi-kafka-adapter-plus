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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

public class ConstantPoolScannerTest {

    @Test
    public void findsSuperTypesFieldTypesAndInvokedSignatures() throws IOException {
        TreeSet<String> types = scan(Sample.class);

        // super class and interface (CONSTANT_Class)
        Assert.assertTrue(types.toString(), types.contains("java.io.ByteArrayInputStream"));
        Assert.assertTrue(types.toString(), types.contains("java.lang.Runnable"));
        // referenced field/method signatures (CONSTANT_NameAndType descriptors)
        Assert.assertTrue(types.toString(), types.contains("java.util.TreeSet"));
        Assert.assertTrue(types.toString(), types.contains("java.lang.String"));
        // caught exception type and array element type
        Assert.assertTrue(types.toString(), types.contains("java.io.IOException"));
        Assert.assertTrue(types.toString(), types.contains("java.util.Map$Entry"));
    }

    @Test
    public void scannerHandlesEveryClassFileOfThisBuild() throws IOException {
        for (Class<?> c : Arrays.asList(ConstantPoolScanner.class, BundleClassWarmup.class,
                ConstantPoolScannerTest.class, org.apache.kafka.clients.consumer.KafkaConsumer.class)) {
            TreeSet<String> types = new TreeSet<>();
            try (InputStream in = open(c)) {
                Assert.assertTrue("constant pool of " + c.getName() + " must be readable",
                        ConstantPoolScanner.collectReferencedTypes(in, types));
            }
            Assert.assertFalse(c.getName(), types.isEmpty());
        }
    }

    @Test
    public void nonClassInputIsRejectedWithoutThrowing() throws IOException {
        TreeSet<String> types = new TreeSet<>();
        Assert.assertFalse(ConstantPoolScanner.collectReferencedTypes(
                new ByteArrayInputStream("not a class file at all".getBytes("UTF-8")), types));
        Assert.assertTrue(types.isEmpty());
    }

    @Test
    public void classEntriesHandleArraysAndPrimitives() {
        TreeSet<String> types = new TreeSet<>();
        ConstantPoolScanner.addClassEntry("java/lang/String", types);
        ConstantPoolScanner.addClassEntry("[Ljava/lang/Thread;", types);
        ConstantPoolScanner.addClassEntry("[[B", types);
        ConstantPoolScanner.addClassEntry("[I", types);
        ConstantPoolScanner.addClassEntry("DefaultPackageType", types);
        ConstantPoolScanner.addClassEntry("", types);
        ConstantPoolScanner.addClassEntry(null, types);
        Assert.assertEquals(new TreeSet<>(Arrays.asList("java.lang.String", "java.lang.Thread")), types);
    }

    @Test
    public void descriptorsYieldEveryObjectTypeAndIgnorePrimitives() {
        TreeSet<String> types = new TreeSet<>();
        ConstantPoolScanner.addDescriptorTypes("(Ljava/lang/String;I[Lorg/apache/camel/Exchange;)Ljava/util/Map;", types);
        ConstantPoolScanner.addDescriptorTypes("J", types);
        ConstantPoolScanner.addDescriptorTypes("()V", types);
        ConstantPoolScanner.addDescriptorTypes("(Ljava/lang/String", types); // malformed: no terminator
        ConstantPoolScanner.addDescriptorTypes(null, types);
        Assert.assertEquals(new TreeSet<>(Arrays.asList(
                "java.lang.String", "org.apache.camel.Exchange", "java.util.Map")), types);
    }

    private static TreeSet<String> scan(Class<?> c) throws IOException {
        TreeSet<String> types = new TreeSet<>();
        try (InputStream in = open(c)) {
            Assert.assertTrue(ConstantPoolScanner.collectReferencedTypes(in, types));
        }
        return types;
    }

    private static InputStream open(Class<?> c) {
        InputStream in = c.getClassLoader().getResourceAsStream(c.getName().replace('.', '/') + ".class");
        Assert.assertNotNull("class file not on the class path: " + c.getName(), in);
        return in;
    }

    /** Fixture with one reference of each interesting constant-pool shape. */
    @SuppressWarnings("unused")
    static final class Sample extends ByteArrayInputStream implements Runnable {

        private final TreeSet<String> names = new TreeSet<>();

        Sample() {
            super(new byte[0]);
        }

        @Override
        public void run() {
            InputStream self = this;
            try {
                names.add(String.valueOf(self.available()));
            } catch (IOException e) {
                names.add(e.getMessage());
            }
            for (java.util.Map.Entry<String, String> e : new java.util.HashMap<String, String>().entrySet()) {
                names.add(e.getKey());
            }
        }
    }
}
