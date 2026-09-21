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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

/**
 * Reads the constant pool of a class file and reports the types that class can make the JVM
 * resolve at runtime (issue #154).
 *
 * <p>Only two constant kinds are relevant, and together they are exactly the set the JVM resolves
 * lazily on first use:
 * <ul>
 *   <li>{@code CONSTANT_Class} — super class, interfaces, {@code new}/{@code checkcast}/
 *       {@code instanceof} operands, exception handler types, array types;</li>
 *   <li>the descriptor of {@code CONSTANT_NameAndType} and {@code CONSTANT_MethodType} — the
 *       parameter and return types of every field and method the class actually references.</li>
 * </ul>
 *
 * <p>Deliberately <em>not</em> scanned: arbitrary {@code CONSTANT_Utf8} entries. Those also hold
 * string literals and generic signatures; harvesting them would invent class names that do not
 * exist and turn the warm-up log into noise.
 *
 * <p>Parsing stops silently on a class file the reader does not understand (truncated entry,
 * constant tag from a newer class file version). A skipped class only means a few types are not
 * pre-loaded — never a failed warm-up.
 */
final class ConstantPoolScanner {

    private static final int MAGIC = 0xCAFEBABE;

    private static final int TAG_UTF8 = 1;
    private static final int TAG_INTEGER = 3;
    private static final int TAG_FLOAT = 4;
    private static final int TAG_LONG = 5;
    private static final int TAG_DOUBLE = 6;
    private static final int TAG_CLASS = 7;
    private static final int TAG_STRING = 8;
    private static final int TAG_FIELDREF = 9;
    private static final int TAG_METHODREF = 10;
    private static final int TAG_INTERFACE_METHODREF = 11;
    private static final int TAG_NAME_AND_TYPE = 12;
    private static final int TAG_METHOD_HANDLE = 15;
    private static final int TAG_METHOD_TYPE = 16;
    private static final int TAG_DYNAMIC = 17;
    private static final int TAG_INVOKE_DYNAMIC = 18;
    private static final int TAG_MODULE = 19;
    private static final int TAG_PACKAGE = 20;

    private ConstantPoolScanner() {}

    /**
     * Adds the binary names of every type {@code classFile} references to {@code sink}. The stream
     * is read but not closed; the caller owns it.
     *
     * @return {@code true} if the constant pool was fully understood, {@code false} if parsing was
     *         abandoned (the types found up to that point are still in {@code sink})
     */
    static boolean collectReferencedTypes(InputStream classFile, Set<String> sink) throws IOException {
        DataInputStream in = new DataInputStream(new BufferedInputStream(classFile, 8192));
        if (in.readInt() != MAGIC) {
            return false;
        }
        in.readUnsignedShort(); // minor version
        in.readUnsignedShort(); // major version
        int count = in.readUnsignedShort();
        if (count < 1) {
            return false;
        }

        String[] utf8 = new String[count];
        int[] classNames = new int[count];
        int[] descriptors = new int[count];
        int classCount = 0;
        int descriptorCount = 0;

        for (int i = 1; i < count; i++) {
            int tag = in.readUnsignedByte();
            switch (tag) {
                case TAG_UTF8:
                    // readUTF consumes the u2 length and decodes modified UTF-8 — the class file
                    // encoding, byte for byte.
                    utf8[i] = in.readUTF();
                    break;
                case TAG_CLASS:
                    classNames[classCount++] = in.readUnsignedShort();
                    break;
                case TAG_METHOD_TYPE:
                    descriptors[descriptorCount++] = in.readUnsignedShort();
                    break;
                case TAG_NAME_AND_TYPE:
                    in.readUnsignedShort(); // name
                    descriptors[descriptorCount++] = in.readUnsignedShort();
                    break;
                case TAG_STRING:
                case TAG_MODULE:
                case TAG_PACKAGE:
                    in.readUnsignedShort();
                    break;
                case TAG_METHOD_HANDLE:
                    in.readUnsignedByte();
                    in.readUnsignedShort();
                    break;
                case TAG_INTEGER:
                case TAG_FLOAT:
                case TAG_FIELDREF:
                case TAG_METHODREF:
                case TAG_INTERFACE_METHODREF:
                case TAG_DYNAMIC:
                case TAG_INVOKE_DYNAMIC:
                    in.readInt();
                    break;
                case TAG_LONG:
                case TAG_DOUBLE:
                    in.readLong();
                    i++; // 8-byte constants occupy two pool slots (JVMS 4.4.5)
                    break;
                default:
                    return false; // constant kind from a newer class file version
            }
        }

        for (int i = 0; i < classCount; i++) {
            addClassEntry(resolve(utf8, classNames[i]), sink);
        }
        for (int i = 0; i < descriptorCount; i++) {
            addDescriptorTypes(resolve(utf8, descriptors[i]), sink);
        }
        return true;
    }

    private static String resolve(String[] utf8, int index) {
        return index > 0 && index < utf8.length ? utf8[index] : null;
    }

    /**
     * A {@code CONSTANT_Class} name is either an internal name ({@code java/lang/String}) or, for
     * array types, a descriptor ({@code [Ljava/lang/String;}, {@code [[I}).
     */
    static void addClassEntry(String name, Set<String> sink) {
        if (name == null || name.isEmpty()) {
            return;
        }
        int dims = 0;
        while (dims < name.length() && name.charAt(dims) == '[') {
            dims++;
        }
        if (dims == 0) {
            addBinaryName(name, sink);
            return;
        }
        String element = name.substring(dims);
        if (element.length() > 2 && element.charAt(0) == 'L' && element.endsWith(";")) {
            addBinaryName(element.substring(1, element.length() - 1), sink);
        }
        // else: array of a primitive — nothing to load.
    }

    /** Extracts every {@code L…;} object type from a field or method descriptor. */
    static void addDescriptorTypes(String descriptor, Set<String> sink) {
        if (descriptor == null) {
            return;
        }
        for (int i = 0; i < descriptor.length(); i++) {
            if (descriptor.charAt(i) != 'L') {
                continue;
            }
            int end = descriptor.indexOf(';', i + 1);
            if (end < 0) {
                return; // malformed descriptor
            }
            addBinaryName(descriptor.substring(i + 1, end), sink);
            i = end;
        }
    }

    private static void addBinaryName(String internalName, Set<String> sink) {
        if (internalName.isEmpty() || internalName.indexOf('/') < 0 && internalName.indexOf('.') < 0) {
            // Default-package types cannot be imported from another bundle; skipping them also
            // filters the primitive leftovers of malformed entries.
            return;
        }
        sink.add(internalName.replace('/', '.'));
    }
}
