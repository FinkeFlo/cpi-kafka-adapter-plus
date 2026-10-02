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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.NewClassTree;
import com.sun.source.tree.Tree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;

/**
 * A name-based call graph over the adapter's main sources, used to answer one question: which
 * endpoint options does a channel direction read at all?
 *
 * <p>The sources are parsed, not compiled, so there is no type resolution. A call {@code x.m()} is
 * resolved to every method named {@code m} in the calling class and in each adapter class whose
 * simple name the calling file mentions. That over-approximates what is reachable, which can only
 * hide a violation, never invent one: an option reported as unread is truly read nowhere on that
 * path. Lambdas and anonymous classes count as part of the method that contains them.
 */
final class AdapterSourceIndex {

    private static final Path MAIN_SOURCES = Paths.get("src/main/java/com/finkeflo/cpi/kafka");
    private static final String ENDPOINT = "CpiKafkaPlusEndpoint";
    private static final String CONSTRUCTOR = "<init>";

    /** A method of a top-level class, keyed by class and name (overloads are merged). */
    private static final class Node {
        final String owner;
        final String name;
        final Set<String> calls = new LinkedHashSet<>();
        final Set<String> identifiers = new LinkedHashSet<>();

        Node(String owner, String name) {
            this.owner = owner;
            this.name = name;
        }
    }

    private final Map<String, List<Node>> nodesByName = new HashMap<>();
    private final Map<String, List<Node>> nodesByOwner = new HashMap<>();
    private final Map<String, Set<String>> mentionedTypes = new HashMap<>();

    private AdapterSourceIndex() {
    }

    static AdapterSourceIndex parseMainSources() throws IOException {
        List<File> files;
        try (Stream<Path> paths = Files.list(MAIN_SOURCES)) {
            files = paths.filter(p -> p.toString().endsWith(".java")).map(Path::toFile).collect(Collectors.toList());
        }
        JavaCompiler javac = ToolProvider.getSystemJavaCompiler();
        if (javac == null) {
            throw new IllegalStateException("No system Java compiler available; the build runs on a JDK, not a JRE");
        }
        AdapterSourceIndex index = new AdapterSourceIndex();
        try (StandardJavaFileManager fileManager = javac.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            Iterable<? extends JavaFileObject> units = fileManager.getJavaFileObjectsFromFiles(files);
            JavacTask task = (JavacTask) javac.getTask(null, fileManager, diagnostic -> { },
                    Arrays.asList("-proc:none"), null, units);
            Set<String> classNames = new HashSet<>();
            List<CompilationUnitTree> trees = new ArrayList<>();
            for (CompilationUnitTree unit : task.parse()) {
                trees.add(unit);
                for (Tree type : unit.getTypeDecls()) {
                    if (type instanceof ClassTree) {
                        classNames.add(((ClassTree) type).getSimpleName().toString());
                    }
                }
            }
            for (CompilationUnitTree unit : trees) {
                index.scan(unit, classNames);
            }
        }
        return index;
    }

    private void scan(CompilationUnitTree unit, Set<String> classNames) {
        for (Tree type : unit.getTypeDecls()) {
            if (!(type instanceof ClassTree)) {
                continue;
            }
            String owner = ((ClassTree) type).getSimpleName().toString();
            Set<String> mentioned = mentionedTypes.computeIfAbsent(owner, k -> new HashSet<>());
            new TreeScanner<Void, Void>() {
                private Node current;

                @Override
                public Void visitMethod(MethodTree method, Void unused) {
                    if (current != null) {
                        return super.visitMethod(method, unused);   // anonymous class: part of the enclosing method
                    }
                    String name = method.getName().toString();
                    current = node(owner, name.equals("<init>") ? CONSTRUCTOR : name);
                    try {
                        return super.visitMethod(method, unused);
                    } finally {
                        current = null;
                    }
                }

                @Override
                public Void visitMethodInvocation(MethodInvocationTree invocation, Void unused) {
                    Tree select = invocation.getMethodSelect();
                    if (select instanceof IdentifierTree) {
                        record(((IdentifierTree) select).getName().toString(), true);
                    } else if (select instanceof MemberSelectTree) {
                        record(((MemberSelectTree) select).getIdentifier().toString(), true);
                    }
                    return super.visitMethodInvocation(invocation, unused);
                }

                @Override
                public Void visitNewClass(NewClassTree creation, Void unused) {
                    String created = creation.getIdentifier().toString();
                    created = created.substring(created.lastIndexOf('.') + 1);
                    if (classNames.contains(created)) {
                        mentioned.add(created);
                        record("new " + created, true);
                    }
                    return super.visitNewClass(creation, unused);
                }

                @Override
                public Void visitIdentifier(IdentifierTree identifier, Void unused) {
                    String name = identifier.getName().toString();
                    if (classNames.contains(name)) {
                        mentioned.add(name);
                    }
                    record(name, false);
                    return super.visitIdentifier(identifier, unused);
                }

                private void record(String name, boolean call) {
                    Node target = current != null ? current : node(owner, CONSTRUCTOR);  // field initialisers
                    (call ? target.calls : target.identifiers).add(name);
                }
            }.scan(type, null);
        }
    }

    private Node node(String owner, String name) {
        for (Node existing : nodesByOwner.getOrDefault(owner, Collections.emptyList())) {
            if (existing.name.equals(name)) {
                return existing;
            }
        }
        Node created = new Node(owner, name);
        nodesByOwner.computeIfAbsent(owner, k -> new ArrayList<>()).add(created);
        nodesByName.computeIfAbsent(name, k -> new ArrayList<>()).add(created);
        return created;
    }

    /**
     * The options read on the path that starts in {@code entryClass} (all of its methods) and in
     * the named endpoint factory method ({@code createConsumer} or {@code createProducer}).
     *
     * @param options the endpoint option names, i.e. its {@code @UriParam} fields
     */
    Set<String> optionsReadFrom(String entryClass, String endpointFactoryMethod, Collection<String> options) {
        Deque<Node> work = new ArrayDeque<>(nodesByOwner.getOrDefault(entryClass, Collections.emptyList()));
        for (Node n : nodesByOwner.getOrDefault(ENDPOINT, Collections.emptyList())) {
            if (n.name.equals(endpointFactoryMethod)) {
                work.add(n);
            }
        }
        Set<Node> seen = new HashSet<>(work);
        Set<String> read = new LinkedHashSet<>();
        while (!work.isEmpty()) {
            Node n = work.poll();
            for (String option : options) {
                String accessor = Character.toUpperCase(option.charAt(0)) + option.substring(1);
                if (n.calls.contains("get" + accessor) || n.calls.contains("is" + accessor)
                        || (ENDPOINT.equals(n.owner) && n.identifiers.contains(option))) {
                    read.add(option);
                }
            }
            for (String call : n.calls) {
                for (Node target : targets(n.owner, call)) {
                    if (seen.add(target)) {
                        work.add(target);
                    }
                }
            }
        }
        return read;
    }

    private List<Node> targets(String caller, String call) {
        List<Node> out = new ArrayList<>();
        if (call.startsWith("new ")) {
            String created = call.substring(4);
            for (Node n : nodesByOwner.getOrDefault(created, Collections.emptyList())) {
                if (n.name.equals(CONSTRUCTOR)) {
                    out.add(n);
                }
            }
            return out;
        }
        Set<String> visible = mentionedTypes.getOrDefault(caller, Collections.emptySet());
        for (Node n : nodesByName.getOrDefault(call, Collections.emptyList())) {
            if (n.owner.equals(caller) || visible.contains(n.owner)) {
                out.add(n);
            }
        }
        return out;
    }
}
