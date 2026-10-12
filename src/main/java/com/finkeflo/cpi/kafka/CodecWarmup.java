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

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * Exercises every Kafka compression codec once, in the calling class space, so that their native
 * libraries are extracted and linked <em>before</em> an adapter update can purge the bundle
 * revision (issue #148).
 *
 * <p>snappy-java and zstd-jni locate their {@code .so} with {@code getResourceAsStream} on their
 * own class loader when first used. On a route that is still running on a purged revision that
 * lookup returns {@code null} and the codec dies for good ({@code SnappyError
 * FAILED_TO_LOAD_NATIVE_LIBRARY}, then {@code NoClassDefFoundError: Could not initialize class …}).
 * Pre-loading the classes does not help — the resource, not the class, is missing — so the only
 * defence is to trigger the native load while the revision is still live. A real
 * compress/decompress round trip through Kafka's own {@link Compression} API does exactly that and
 * additionally initialises Kafka's codec-side statics.
 *
 * <p>lz4-java never loads its native library inside the bundle: {@code LZ4Factory.fastestInstance()},
 * which Kafka's LZ4 streams use, only tries it when lz4-java was loaded by the system class loader
 * or the library is already loaded. In the bundle lz4 therefore runs in pure Java
 * ({@link #lz4Implementation}), and its round trip only loads and initialises its classes.
 */
final class CodecWarmup {

    private static final byte[] SAMPLE =
            "cpi-kafka-plus codec warm-up sample — repeat repeat repeat repeat repeat".getBytes(StandardCharsets.UTF_8);

    /** {@code LZ4Factory.toString()} is this prefix followed by the implementation's name. */
    private static final String LZ4_FACTORY_PREFIX = "LZ4Factory:";

    private CodecWarmup() {}

    static final class Result {
        final Map<String, String> outcomes = new LinkedHashMap<>();
        /** See {@link CodecWarmup#lz4Implementation()}. */
        String lz4Implementation;
        long durationMs;

        int failed() {
            int n = 0;
            for (String v : outcomes.values()) {
                if (!"ok".equals(v)) {
                    n++;
                }
            }
            return n;
        }

        String summary() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : outcomes.entrySet()) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append(e.getKey()).append(':').append(e.getValue());
            }
            return sb.toString();
        }
    }

    /** Round-trips {@link #SAMPLE} through every codec except {@code NONE}; never throws. */
    static Result warmAll() {
        Result result = new Result();
        long t0 = System.nanoTime();
        for (CompressionType type : CompressionType.values()) {
            if (type == CompressionType.NONE) {
                continue;
            }
            result.outcomes.put(type.name, roundTrip(type));
        }
        result.lz4Implementation = lz4Implementation();
        result.durationMs = (System.nanoTime() - t0) / 1_000_000L;
        return result;
    }

    /**
     * The LZ4 implementation that Kafka's LZ4 streams use, from {@code LZ4Factory.fastestInstance()}:
     * {@code JNI}, {@code JavaUnsafe} or {@code JavaSafe}. If it cannot be determined, the failure's
     * class name. Read by reflection because lz4-java is a runtime-only dependency.
     */
    static String lz4Implementation() {
        try {
            Class<?> factory = Class.forName("net.jpountz.lz4.LZ4Factory", true, CodecWarmup.class.getClassLoader());
            String name = String.valueOf(factory.getMethod("fastestInstance").invoke(null));
            return name.startsWith(LZ4_FACTORY_PREFIX) ? name.substring(LZ4_FACTORY_PREFIX.length()) : name;
        } catch (InvocationTargetException e) {
            return (e.getCause() != null ? e.getCause() : e).getClass().getName();
        } catch (Throwable t) {
            return t.getClass().getName();
        }
    }

    /** @return {@code "ok"} or {@code "<ExceptionClass>: <message>"} */
    static String roundTrip(CompressionType type) {
        try {
            Compression compression = Compression.of(type).build();
            ByteBufferOutputStream sink = new ByteBufferOutputStream(512);
            try (OutputStream out = compression.wrapForOutput(sink, RecordBatch.MAGIC_VALUE_V2)) {
                out.write(SAMPLE);
            }
            ByteBuffer compressed = sink.buffer().duplicate();
            compressed.flip();
            byte[] restored = new byte[SAMPLE.length];
            try (InputStream in = compression.wrapForInput(compressed, RecordBatch.MAGIC_VALUE_V2,
                    BufferSupplier.NO_CACHING)) {
                int off = 0;
                while (off < restored.length) {
                    int n = in.read(restored, off, restored.length - off);
                    if (n < 0) {
                        break;
                    }
                    off += n;
                }
            }
            if (!Arrays.equals(SAMPLE, restored)) {
                return "mismatch";
            }
            return "ok";
        } catch (Throwable t) {
            return t.getClass().getName() + ": " + t.getMessage();
        }
    }
}
