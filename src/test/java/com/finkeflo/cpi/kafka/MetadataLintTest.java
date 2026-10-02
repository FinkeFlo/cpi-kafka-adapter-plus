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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.UriParam;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Lints the metadata of the current line against the runtime and the documentation (#178).
 *
 * <p>The current line is the highest {@code version::} per direction, so the test follows every
 * minor bump without being edited. Frozen lines are not linted: they can no longer be changed, and
 * the runtime keeps serving them as they are.
 *
 * <p>Known violations are listed in {@link #KNOWN_VIOLATIONS}, each with the open issue that
 * decides it. An entry that no longer matches a violation fails the test too, so the list cannot
 * outlive its fix.
 */
public class MetadataLintTest {

    private static final File METADATA_DIR = new File("src/main/resources/metadata");
    private static final File CONFIGURATION_DOC = new File("docs/configuration.md");
    private static final Pattern VERSION_MARKER = Pattern.compile("version::(\\d+)\\.(\\d+)\\.(\\d+)");
    private static final Pattern REGEX_RESTRICTION = Pattern.compile("^Constraint\\.isValidRegex\\((.*)\\)$", Pattern.DOTALL);
    private static final String PARAMETER_PLACEHOLDER = "{{x}}";

    /** Rule, direction and attribute of a violation, mapped to the open issue that decides it. */
    private static final Map<String, String> KNOWN_VIOLATIONS = new LinkedHashMap<>();
    static {
        // An ErrorMessage is only ever shown by a Restriction; the five fields whose start check has
        // a fixed range get one in #186, the remaining ones wait for the tenant tests of #206.
        for (String field : Arrays.asList("maxPartitionFetchSizeKb", "retryDelaySeconds")) {
            KNOWN_VIOLATIONS.put("error-message-without-restriction sender." + field, "#186");
        }
        for (String field : Arrays.asList("producerRetryMaxAttempts", "producerRetryDelaySeconds",
                "producerRetryTotalBudgetSeconds")) {
            KNOWN_VIOLATIONS.put("error-message-without-restriction receiver." + field, "#186");
        }
        for (String field : Arrays.asList("maxPollRecords", "batchTimeout", "fetchMinBytes", "fetchMaxWaitMs",
                "minBacklogToDrain", "batchSize", "schemaRegistryUrl", "dlqTopic", "dlqMaxRetries",
                "autoPauseErrorThreshold", "autoPauseCooldownSeconds")) {
            KNOWN_VIOLATIONS.put("error-message-without-restriction sender." + field, "#206");
        }
        for (String field : Arrays.asList("deliveryTimeoutSeconds", "transactionalIdPrefix",
                "maxConcurrentTransactions", "maxRequestSizeKb", "producerBatchSizeKb", "bufferMemoryKb",
                "schemaRegistryUrl")) {
            KNOWN_VIOLATIONS.put("error-message-without-restriction receiver." + field, "#206");
        }
        // Whether the UI checks a Restriction against an externalized {{parameter}} literally is a
        // tenant question (#206 c); this pattern predates the question.
        KNOWN_VIOLATIONS.put("restriction-rejects-parameter sender.pollingIntervalSeconds", "#206");
        // Every message fails with these strategies; whether to drop them or fail at start is #183.
        KNOWN_VIOLATIONS.put("fixed-value-rejected receiver.subjectNameStrategy=RecordNameStrategy", "#183");
        KNOWN_VIOLATIONS.put("fixed-value-rejected receiver.subjectNameStrategy=TopicRecordNameStrategy", "#183");
        // The producer never writes the MPL attachment; implement or hide is #201.
        KNOWN_VIOLATIONS.put("option-ignored receiver.writeMplErrorAttachment", "#201");
    }

    /** Numeric Restrictions: the range the pattern must express, and what activates the start check. */
    private static final Map<String, RangeSpec> RANGES = new LinkedHashMap<>();
    static {
        RANGES.put("sender.pollingIntervalSeconds", new RangeSpec(1, 21600));
    }

    /** Text Restrictions: one value the pattern must accept and one it must reject. */
    private static final Map<String, String[]> TEXT_RESTRICTIONS = new LinkedHashMap<>();
    static {
        TEXT_RESTRICTIONS.put("sender.credentialAlias", new String[] {"kafka-credentials", ""});
        TEXT_RESTRICTIONS.put("receiver.credentialAlias", new String[] {"kafka-credentials", ""});
    }

    /** Settings without which a dropdown value is never evaluated, per attribute. */
    private static final Map<String, Map<String, String>> ACTIVATION = new LinkedHashMap<>();
    static {
        ACTIVATION.put("saslMechanism", params("securityProtocol", "SASL_SSL"));
        ACTIVATION.put("subjectNameStrategy", params("schemaRegistryEnabled", "true",
                "schemaRegistryUrl", "http://localhost:1", "avroValueSerialization", "true"));
        ACTIVATION.put("avroOutputFormat", params("schemaRegistryEnabled", "true",
                "schemaRegistryUrl", "http://localhost:1"));
        ACTIVATION.put("batchOutputFormat", params("batchMode", "true"));
        ACTIVATION.put("acks", params("enableIdempotence", "false"));
    }

    private static Map<Direction, Metadata> currentLine;

    private enum Direction {
        SENDER("sender", "CpiKafkaPlusConsumer", "createConsumer", "## Sender"),
        RECEIVER("receiver", "CpiKafkaPlusProducer", "createProducer", "## Receiver");

        final String id;
        final String runtimeClass;
        final String endpointFactory;
        final String docHeading;

        Direction(String id, String runtimeClass, String endpointFactory, String docHeading) {
            this.id = id;
            this.runtimeClass = runtimeClass;
            this.endpointFactory = endpointFactory;
            this.docHeading = docHeading;
        }
    }

    private static final class RangeSpec {
        final long lo;
        final long hi;
        final Map<String, String> activation;

        RangeSpec(long lo, long hi, String... activation) {
            this.lo = lo;
            this.hi = hi;
            this.activation = params(activation);
        }
    }

    private static final class Attribute {
        final String name;
        boolean usage;
        String defaultValue = "";
        final List<String> fixedValues = new ArrayList<>();
        boolean offered;
        String errorMessage;
        final List<String> restrictions = new ArrayList<>();

        Attribute(String name) {
            this.name = name;
        }
    }

    private static final class Metadata {
        final File file;
        final Map<String, Attribute> attributes = new LinkedHashMap<>();

        Metadata(File file) {
            this.file = file;
        }

        List<Attribute> offered() {
            List<Attribute> out = new ArrayList<>();
            for (Attribute a : attributes.values()) {
                if (a.offered) {
                    out.add(a);
                }
            }
            return out;
        }
    }

    @BeforeClass
    public static void loadCurrentLine() throws Exception {
        currentLine = new LinkedHashMap<>();
        for (Direction direction : Direction.values()) {
            currentLine.put(direction, parse(currentLineFile(direction)));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Rules
    // ---------------------------------------------------------------------------------------------

    /** An ErrorMessage is displayed by a failing Restriction, or for an empty mandatory field. */
    @Test
    public void everyErrorMessageHasARestrictionOrBelongsToAMandatoryField() {
        Set<String> violations = new TreeSet<>();
        forEachOffered((direction, a) -> {
            if (a.errorMessage != null && a.restrictions.isEmpty() && !a.usage) {
                violations.add("error-message-without-restriction " + direction.id + "." + a.name);
            }
        });
        assertOnlyKnownViolations("error-message-without-restriction", violations);
    }

    /**
     * The metadata default is what a new channel gets, the Java initializer what a channel without
     * the field gets (an older line, or an unreferenced definition), and the {@code @UriParam}
     * default what the Camel tooling reports. All three must agree, also for definitions no tab
     * references, which CPI may still emit into the endpoint URI.
     */
    @Test
    public void metadataDefaultsMatchTheJavaDefaults() throws Exception {
        CpiKafkaPlusEndpoint fresh = new CpiKafkaPlusEndpoint();
        Set<String> violations = new TreeSet<>();
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            for (Attribute a : line.getValue().attributes.values()) {
                String key = line.getKey().id + "." + a.name;
                Field field = endpointOption(a.name);
                if (field == null) {
                    violations.add("default-mismatch " + key + " (no endpoint option of that name)");
                    continue;
                }
                field.setAccessible(true);
                Object initial = field.get(fresh);
                String initializer = initial == null ? "" : String.valueOf(initial);
                String annotated = field.getAnnotation(UriParam.class).defaultValue();
                if (!a.defaultValue.equals(initializer) || !a.defaultValue.equals(annotated)) {
                    violations.add("default-mismatch " + key + " (metadata '" + a.defaultValue
                            + "', Java initializer '" + initializer + "', @UriParam '" + annotated + "')");
                }
            }
        }
        assertOnlyKnownViolations("default-mismatch", violations);
    }

    /**
     * Every Restriction compiles and accepts its own default. A numeric one agrees with the start
     * check at lo−1, lo, hi and hi+1, and lets an externalized {@code {{parameter}}} through.
     */
    @Test
    public void everyRestrictionCompilesAcceptsItsDefaultAndAgreesWithTheStartCheck() throws Exception {
        Set<String> violations = new TreeSet<>();
        Set<String> specified = new LinkedHashSet<>(RANGES.keySet());
        specified.addAll(TEXT_RESTRICTIONS.keySet());
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            Direction direction = line.getKey();
            for (Attribute a : line.getValue().offered()) {
                String key = direction.id + "." + a.name;
                for (String restriction : a.restrictions) {
                    specified.remove(key);
                    Pattern pattern = compileRestriction(restriction);
                    if (pattern == null) {
                        violations.add("restriction-invalid " + key + " (" + restriction + ")");
                        continue;
                    }
                    if (!a.defaultValue.isEmpty() && !pattern.matcher(a.defaultValue).find()) {
                        violations.add("restriction-rejects-default " + key + "=" + a.defaultValue);
                    }
                    RangeSpec range = RANGES.get(key);
                    String[] text = TEXT_RESTRICTIONS.get(key);
                    if (range != null) {
                        checkRange(direction, a.name, pattern, range, violations);
                    } else if (text != null) {
                        if (!pattern.matcher(text[0]).find() || pattern.matcher(text[1]).find()) {
                            violations.add("restriction-text-mismatch " + key + " (accepts '" + text[0]
                                    + "', rejects '" + text[1] + "' expected)");
                        }
                    } else {
                        violations.add("restriction-unspecified " + key
                                + " (add it to RANGES or TEXT_RESTRICTIONS in this test)");
                    }
                }
            }
        }
        for (String stale : specified) {
            violations.add("restriction-unspecified " + stale + " (listed in this test, but has no Restriction)");
        }
        assertOnlyKnownViolations("restriction-", violations);
    }

    /** A dropdown that offers a value the runtime rejects fails a channel the UI accepted. */
    @Test
    public void everyFixedValueIsAcceptedByTheRuntime() throws Exception {
        Set<String> violations = new TreeSet<>();
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            Direction direction = line.getKey();
            for (Attribute a : line.getValue().offered()) {
                for (String value : a.fixedValues) {
                    Map<String, String> params = new LinkedHashMap<>(ACTIVATION.getOrDefault(a.name, Collections.emptyMap()));
                    params.put(a.name, value);
                    String rejection = runtimeRejection(direction, a.name, params);
                    if (rejection != null) {
                        violations.add("fixed-value-rejected " + direction.id + "." + a.name + "=" + value
                                + " (" + rejection + ")");
                    }
                }
            }
        }
        assertOnlyKnownViolations("fixed-value-rejected", violations);
    }

    /**
     * An option offered on a channel whose runtime never reads it is a control that lies: the
     * operator changes it and nothing happens. Generalises the diagnosticsLevel rule of
     * {@link DiagnosticContractTest}.
     */
    @Test
    public void noOptionIsOfferedOnADirectionThatIgnoresIt() throws Exception {
        List<String> options = new ArrayList<>();
        for (Field field : CpiKafkaPlusEndpoint.class.getDeclaredFields()) {
            if (field.isAnnotationPresent(UriParam.class)) {
                options.add(field.getName());
            }
        }
        AdapterSourceIndex index = AdapterSourceIndex.parseMainSources();
        Set<String> violations = new TreeSet<>();
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            Direction direction = line.getKey();
            Set<String> read = index.optionsReadFrom(direction.runtimeClass, direction.endpointFactory, options);
            Assert.assertFalse("the source index found no option read by " + direction.runtimeClass
                    + "; the parser or the entry point has drifted", read.isEmpty());
            for (Attribute a : line.getValue().offered()) {
                if (!read.contains(a.name)) {
                    violations.add("option-ignored " + direction.id + "." + a.name);
                }
            }
        }
        assertOnlyKnownViolations("option-ignored", violations);
    }

    /** {@code docs/configuration.md} lists exactly the offered options per direction, with their defaults. */
    @Test
    public void configurationDocMatchesTheMetadata() throws IOException {
        Map<Direction, Map<String, String>> documented = documentedDefaults();
        Set<String> violations = new TreeSet<>();
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            Direction direction = line.getKey();
            Map<String, String> rows = documented.get(direction);
            Set<String> offered = new LinkedHashSet<>();
            for (Attribute a : line.getValue().offered()) {
                offered.add(a.name);
                String key = direction.id + "." + a.name;
                if (!rows.containsKey(a.name)) {
                    violations.add("doc-missing " + key);
                } else if (!rows.get(a.name).equals(a.defaultValue)) {
                    violations.add("doc-default-mismatch " + key + " (doc '" + rows.get(a.name)
                            + "', metadata '" + a.defaultValue + "')");
                }
            }
            for (String name : rows.keySet()) {
                if (!offered.contains(name)) {
                    violations.add("doc-not-offered " + direction.id + "." + name);
                }
            }
        }
        assertOnlyKnownViolations("doc-", violations);
    }

    @Test
    public void everyKnownViolationNamesAnIssue() {
        for (Map.Entry<String, String> entry : KNOWN_VIOLATIONS.entrySet()) {
            Assert.assertTrue("allow-list entry '" + entry.getKey() + "' must reference an open issue as #<n>",
                    entry.getValue().matches("#\\d+"));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Restriction ranges and runtime acceptance
    // ---------------------------------------------------------------------------------------------

    private static void checkRange(Direction direction, String name, Pattern pattern, RangeSpec range,
                                   Set<String> violations) throws Exception {
        String key = direction.id + "." + name;
        for (long value : new long[] {range.lo - 1, range.lo, range.hi, range.hi + 1}) {
            boolean inRange = value >= range.lo && value <= range.hi;
            if (pattern.matcher(String.valueOf(value)).find() != inRange) {
                violations.add("restriction-range-mismatch " + key + "=" + value + " (pattern "
                        + (inRange ? "rejects" : "accepts") + " it)");
            }
            Map<String, String> params = new LinkedHashMap<>(range.activation);
            params.put(name, String.valueOf(value));
            boolean started = startRejection(direction, params) == null;
            if (started != inRange) {
                violations.add("restriction-start-mismatch " + key + "=" + value + " (start check "
                        + (started ? "accepts" : "rejects") + " it)");
            }
        }
        if (!pattern.matcher(PARAMETER_PLACEHOLDER).find()) {
            violations.add("restriction-rejects-parameter " + key);
        }
    }

    /** Why the runtime of the direction refuses the settings, or {@code null} if it takes them. */
    private static String runtimeRejection(Direction direction, String attribute, Map<String, String> params)
            throws Exception {
        String rejection = startRejection(direction, params);
        if (rejection != null) {
            return "start: " + rejection;
        }
        CpiKafkaPlusEndpoint endpoint = endpoint(direction, params);
        try {
            if (direction == Direction.SENDER) {
                Properties props = new CpiKafkaPlusConsumer(endpoint, exchange -> { }).buildConsumerProperties();
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
                new ConsumerConfig(props);
            } else {
                Properties props = ProducerConfigFactory.buildProducerProperties(endpoint);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
                new ProducerConfig(props);
            }
        } catch (RuntimeException e) {
            return "Kafka client config: " + e.getMessage();
        }
        if ("subjectNameStrategy".equals(attribute) && direction == Direction.RECEIVER) {
            // Resolved per message, not at start: the subject is the first thing serialize() needs.
            try (AvroSerializerHelper helper = new AvroSerializerHelper(endpoint)) {
                Method resolveSubject = AvroSerializerHelper.class.getDeclaredMethod("resolveSubject", String.class);
                resolveSubject.setAccessible(true);
                resolveSubject.invoke(helper, "orders");
            } catch (InvocationTargetException e) {
                return "per message: " + e.getCause().getMessage();
            }
        }
        return null;
    }

    /** Why the direction's start checks refuse the settings, or {@code null} if it starts. */
    private static String startRejection(Direction direction, Map<String, String> params) throws Exception {
        CpiKafkaPlusEndpoint endpoint = endpoint(direction, params);
        if (direction == Direction.SENDER) {
            CpiKafkaPlusConsumer consumer = new CpiKafkaPlusConsumer(endpoint, exchange -> { });
            try {
                consumer.doStart();
                return null;
            } catch (IllegalArgumentException e) {
                return e.getMessage();
            } finally {
                stopQuietly(consumer::doStop);
            }
        }
        CpiKafkaPlusProducer producer = new CpiKafkaPlusProducer(endpoint);
        try {
            producer.doStart();
            return null;
        } catch (IllegalArgumentException e) {
            return e.getMessage();
        } finally {
            stopQuietly(producer::doStop);
        }
    }

    private interface Stop {
        void run() throws Exception;
    }

    private static void stopQuietly(Stop stop) {
        try {
            stop.run();
        } catch (Exception ignored) {
            // a failed start may also fail to stop; not what is under test
        }
    }

    private static CpiKafkaPlusEndpoint endpoint(Direction direction, Map<String, String> overrides) throws Exception {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("bootstrapServers", "localhost:9999");
        params.put("securityProtocol", "PLAINTEXT");
        params.put("credentialAlias", "kafka-credentials");
        if (direction == Direction.SENDER) {
            params.put("groupId", "lint");
        }
        params.putAll(overrides);
        StringBuilder uri = new StringBuilder("cpi-kafka-plus:orders?");
        for (Map.Entry<String, String> p : params.entrySet()) {
            uri.append(p.getKey()).append('=').append(p.getValue()).append('&');
        }
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
            ctx.start();
            return (CpiKafkaPlusEndpoint) ctx.getEndpoint(uri.substring(0, uri.length() - 1));
        }
    }

    private static Pattern compileRestriction(String restriction) {
        Matcher m = REGEX_RESTRICTION.matcher(restriction.trim());
        if (!m.matches()) {
            return null;
        }
        try {
            return Pattern.compile(m.group(1));
        } catch (PatternSyntaxException e) {
            return null;
        }
    }

    private static Field endpointOption(String name) {
        try {
            Field field = CpiKafkaPlusEndpoint.class.getDeclaredField(name);
            return field.isAnnotationPresent(UriParam.class) ? field : null;
        } catch (NoSuchFieldException e) {
            return null;
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Allow-list
    // ---------------------------------------------------------------------------------------------

    private static void assertOnlyKnownViolations(String rulePrefix, Set<String> violations) {
        Set<String> unexpected = new TreeSet<>();
        Set<String> matched = new TreeSet<>();
        for (String violation : violations) {
            String key = violation.contains(" (") ? violation.substring(0, violation.indexOf(" (")) : violation;
            if (KNOWN_VIOLATIONS.containsKey(key)) {
                matched.add(key);
            } else {
                unexpected.add(violation);
            }
        }
        Set<String> stale = new TreeSet<>();
        for (String known : KNOWN_VIOLATIONS.keySet()) {
            if (known.startsWith(rulePrefix) && !matched.contains(known)) {
                stale.add(known + " " + KNOWN_VIOLATIONS.get(known));
            }
        }
        Assert.assertTrue("metadata lint violations in " + currentLineNames() + ":\n  "
                + String.join("\n  ", unexpected), unexpected.isEmpty());
        Assert.assertTrue("allow-list entries that no longer match a violation (remove them):\n  "
                + String.join("\n  ", stale), stale.isEmpty());
    }

    private interface AttributeVisitor {
        void visit(Direction direction, Attribute attribute);
    }

    private static void forEachOffered(AttributeVisitor visitor) {
        for (Map.Entry<Direction, Metadata> line : currentLine.entrySet()) {
            for (Attribute a : line.getValue().offered()) {
                visitor.visit(line.getKey(), a);
            }
        }
    }

    private static String currentLineNames() {
        List<String> names = new ArrayList<>();
        for (Metadata m : currentLine.values()) {
            names.add(m.file.getName());
        }
        return names.toString();
    }

    // ---------------------------------------------------------------------------------------------
    // Parsing
    // ---------------------------------------------------------------------------------------------

    /** The file of the direction with the highest {@code version::}, whatever it is named. */
    private static File currentLineFile(Direction direction) throws IOException {
        File[] files = METADATA_DIR.listFiles((dir, name) ->
                name.startsWith("metadata-" + direction.id + "-") && name.endsWith(".xml"));
        Assert.assertNotNull("metadata dir not found at " + METADATA_DIR.getAbsolutePath(), files);
        File best = null;
        int[] bestVersion = null;
        for (File f : files) {
            Matcher m = VERSION_MARKER.matcher(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8));
            Assert.assertTrue("no version:: marker in " + f.getName(), m.find());
            int[] version = {Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3))};
            if (bestVersion == null || compare(version, bestVersion) > 0) {
                best = f;
                bestVersion = version;
            }
        }
        Assert.assertNotNull("no " + direction.id + " metadata file", best);
        return best;
    }

    private static int compare(int[] a, int[] b) {
        for (int i = 0; i < 3; i++) {
            if (a[i] != b[i]) {
                return Integer.compare(a[i], b[i]);
            }
        }
        return 0;
    }

    private static Metadata parse(File file) throws Exception {
        Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(file);
        Metadata metadata = new Metadata(file);
        NodeList definitions = doc.getElementsByTagName("AttributeMetadata");
        for (int i = 0; i < definitions.getLength(); i++) {
            Element definition = (Element) definitions.item(i);
            Attribute a = new Attribute(childText(definition, "Name"));
            a.usage = "true".equals(childText(definition, "Usage"));
            String def = childText(definition, "Default");
            a.defaultValue = def == null ? "" : def;
            NodeList fixed = definition.getElementsByTagName("FixedValue");
            for (int j = 0; j < fixed.getLength(); j++) {
                a.fixedValues.add(childText((Element) fixed.item(j), "Value"));
            }
            a.restrictions.addAll(childTexts(definition, "Restriction"));
            metadata.attributes.put(a.name, a);
        }
        NodeList references = doc.getElementsByTagName("AttributeReference");
        for (int i = 0; i < references.getLength(); i++) {
            Element reference = (Element) references.item(i);
            String name = childText(reference, "ReferenceName");
            Attribute a = metadata.attributes.get(name);
            Assert.assertNotNull(file.getName() + " references '" + name + "' without an AttributeMetadata", a);
            a.offered = true;
            a.errorMessage = childText(reference, "ErrorMessage");
            a.restrictions.addAll(childTexts(reference, "Restriction"));
        }
        return metadata;
    }

    private static String childText(Element parent, String tag) {
        List<String> texts = childTexts(parent, tag);
        return texts.isEmpty() ? null : texts.get(0);
    }

    private static List<String> childTexts(Element parent, String tag) {
        List<String> out = new ArrayList<>();
        for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child instanceof Element && tag.equals(((Element) child).getTagName())) {
                out.add(child.getTextContent().trim());
            }
        }
        return out;
    }

    /** Parameter rows of the Sender and Receiver sections: name to documented default. */
    private static Map<Direction, Map<String, String>> documentedDefaults() throws IOException {
        Pattern row = Pattern.compile("^\\|\\s*`([A-Za-z0-9]+)`\\s*\\|\\s*([^|]*?)\\s*\\|");
        Map<Direction, Map<String, String>> out = new LinkedHashMap<>();
        Direction section = null;
        for (String line : Files.readAllLines(CONFIGURATION_DOC.toPath(), StandardCharsets.UTF_8)) {
            for (Direction d : Direction.values()) {
                if (line.startsWith(d.docHeading)) {
                    section = d;
                    out.put(d, new TreeMap<>());
                }
            }
            Matcher m = row.matcher(line);
            if (section != null && m.find()) {
                String documented = m.group(2).replace("`", "").trim();
                if (documented.equals("_(required)_") || documented.equals("—") || documented.equals("-")) {
                    documented = "";
                }
                out.get(section).put(m.group(1), documented);
            }
        }
        for (Direction d : Direction.values()) {
            Assert.assertTrue("no '" + d.docHeading + "' section in " + CONFIGURATION_DOC, out.containsKey(d));
        }
        return out;
    }

    private static Map<String, String> params(String... keyValues) {
        Map<String, String> out = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            out.put(keyValues[i], keyValues[i + 1]);
        }
        return out;
    }
}
