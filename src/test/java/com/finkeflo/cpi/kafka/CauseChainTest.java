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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Assert;
import org.junit.Test;

/**
 * A cause chain can be cyclic: {@code initCause} allows A → B → A, and a subclass can return itself
 * from {@code getCause()}. Every walk over a cause chain must still end (issue #241). Each walk test
 * has a timeout because the failure mode is a walk that never returns.
 */
public class CauseChainTest {

    private static final long TIMEOUT_MS = 2000L;

    // --- the chain itself ---

    @Test
    public void walksFromTheThrowableToItsDeepestCause() {
        IllegalStateException deepest = new IllegalStateException("deepest");
        RuntimeException middle = new RuntimeException("middle", deepest);
        Exception top = new Exception("top", middle);

        Assert.assertEquals(Arrays.<Throwable>asList(top, middle, deepest), links(top));
        Assert.assertSame(deepest, CauseChain.root(top));
    }

    @Test
    public void aThrowableWithoutCauseIsItsOwnRoot() {
        IllegalStateException alone = new IllegalStateException("alone");

        Assert.assertEquals(Arrays.<Throwable>asList(alone), links(alone));
        Assert.assertSame(alone, CauseChain.root(alone));
    }

    @Test
    public void nullHasNoLinks() {
        Assert.assertTrue(links(null).isEmpty());
        Assert.assertNull(CauseChain.root(null));
    }

    @Test(timeout = TIMEOUT_MS)
    public void aCycleEndsBeforeTheFirstRepeat() {
        Throwable a = cycle();
        Throwable b = a.getCause();

        Assert.assertEquals(Arrays.asList(a, b), links(a));
        Assert.assertSame(b, CauseChain.root(a));
    }

    @Test(timeout = TIMEOUT_MS)
    public void aSelfReferenceIsWalkedOnce() {
        Throwable self = new SelfCaused();

        Assert.assertEquals(Arrays.asList(self), links(self));
        Assert.assertSame(self, CauseChain.root(self));
    }

    @Test(timeout = TIMEOUT_MS)
    public void aChainOfEverNewCausesEndsAtTheDepthLimit() {
        Assert.assertEquals(CauseChain.MAX_DEPTH, links(new EndlessCauses(0)).size());
    }

    @Test
    public void aLongChainIsWalkedToItsEnd() {
        IllegalStateException deepest = new IllegalStateException("deepest");
        Exception e = deepest;
        for (int i = 0; i < 40; i++) {
            e = new RuntimeException("wrapper " + i, e);
        }

        Assert.assertEquals(41, links(e).size());
        Assert.assertSame(deepest, CauseChain.root(e));
    }

    // --- every walk ends on a cyclic or self-referencing chain ---

    @Test(timeout = TIMEOUT_MS)
    public void isTransientForRedeliveryEnds() {
        Assert.assertFalse(KafkaErrorHelper.isTransientForRedelivery((Exception) cycle()));
        Assert.assertFalse(KafkaErrorHelper.isTransientForRedelivery(new SelfCaused()));

        // a transient link inside the cycle is still found
        IllegalStateException a = new IllegalStateException("route failed");
        java.net.ConnectException b = new java.net.ConnectException("Connection refused");
        a.initCause(b);
        b.initCause(a);
        Assert.assertTrue(KafkaErrorHelper.isTransientForRedelivery(a));
    }

    @Test(timeout = TIMEOUT_MS)
    public void extractKafkaCauseEnds() {
        Throwable a = cycle();
        Assert.assertNotNull(KafkaErrorHelper.extractKafkaCause((Exception) a));
        Exception wrapper = new RuntimeException("wrapper", new SelfCaused());
        Assert.assertTrue(KafkaErrorHelper.extractKafkaCause(wrapper) instanceof SelfCaused);
    }

    @Test(timeout = TIMEOUT_MS)
    public void errorCodeEnds() {
        Assert.assertEquals(CpiKafkaPlusErrorCode.KP_GEN_001, CpiKafkaPlusErrorCode.fromThrowable(cycle()));
        Assert.assertEquals(CpiKafkaPlusErrorCode.KP_GEN_001,
                CpiKafkaPlusErrorCode.fromThrowable(new SelfCaused()));
    }

    @Test(timeout = TIMEOUT_MS)
    public void pollFailureClassifierEnds() {
        Assert.assertNull(PollFailureClassifier.findNonRetryablePollFailureCause(cycle()));
        Assert.assertNull(PollFailureClassifier.findNonRetryablePollFailureCause(new SelfCaused()));
        Assert.assertFalse(PollFailureClassifier.isFencedInstanceIdFailure(cycle()));
        Assert.assertFalse(PollFailureClassifier.isFencedInstanceIdFailure(new SelfCaused()));
    }

    @Test(timeout = TIMEOUT_MS)
    public void classSpaceFaultsEnd() {
        Assert.assertFalse(ClassSpaceFaults.isWiringInvalid(cycle()));
        Assert.assertFalse(ClassSpaceFaults.isWiringInvalid(new SelfCaused()));
        Assert.assertFalse(ClassSpaceFaults.hasFaultSignature(cycle()));
        Assert.assertFalse(ClassSpaceFaults.hasFaultSignature(new SelfCaused()));
    }

    @Test(timeout = TIMEOUT_MS)
    public void bundleWarmupWiringCheckEnds() {
        Assert.assertFalse(BundleClassWarmup.isWiringGone(cycle()));
        Assert.assertFalse(BundleClassWarmup.isWiringGone(new SelfCaused()));
    }

    @Test(timeout = TIMEOUT_MS)
    public void nodeFaultIdentityEnds() {
        NodeFaultEscalation escalation = new NodeFaultEscalation(() -> "test-topic", () -> false,
                () -> "client-1", System::currentTimeMillis);

        // An Error takes the escalation's own walk, an Exception KafkaErrorHelper.extractKafkaCause.
        Error first = new LinkageError("first");
        Error second = new NoClassDefFoundError("second");
        first.initCause(second);
        second.initCause(first);
        Assert.assertSame(second, escalation.resolveNodeFaultIdentityCause(first));
        Assert.assertNotNull(escalation.resolveNodeFaultIdentityCause(cycle()));
    }

    @Test(timeout = TIMEOUT_MS)
    public void deadLetterOfADeserializationFailureEnds() {
        MockProducer<byte[], byte[]> producer =
                new MockProducer<>(true, null, new ByteArraySerializer(), new ByteArraySerializer());
        DlqProducerHelper helper = new DlqProducerHelper("orders-dlq", producer,
                ProducerSendGuard.of(500L, "PLAINTEXT"));

        helper.sendDeserializationFailure(new TopicPartition("orders", 0), 42L, null,
                "bad".getBytes(StandardCharsets.UTF_8), new RecordHeaders(), -1L, cycle());

        Assert.assertEquals(1, producer.history().size());
        Assert.assertEquals(IllegalArgumentException.class.getName(),
                header(producer.history().get(0), "CpiKafkaPlusDlqCauseClass"));
    }

    // --- helpers ---

    /** A → B → A, with A an {@link IllegalStateException} and B an {@link IllegalArgumentException}. */
    private static Throwable cycle() {
        IllegalStateException a = new IllegalStateException("a");
        IllegalArgumentException b = new IllegalArgumentException("b", a);
        a.initCause(b);
        return a;
    }

    private static List<Throwable> links(Throwable t) {
        List<Throwable> links = new ArrayList<>();
        for (Throwable link : CauseChain.of(t)) {
            links.add(link);
        }
        return links;
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    /** {@code initCause(this)} is refused, but a subclass can still name itself as its cause. */
    private static final class SelfCaused extends RuntimeException {
        private static final long serialVersionUID = 1L;

        SelfCaused() {
            super("self-caused");
        }

        @Override
        public synchronized Throwable getCause() {
            return this;
        }
    }

    /** A cause chain without end and without a repeat: every {@code getCause()} is a new object. */
    private static final class EndlessCauses extends RuntimeException {
        private static final long serialVersionUID = 1L;
        private final int depth;

        EndlessCauses(int depth) {
            super("depth " + depth);
            this.depth = depth;
        }

        @Override
        public synchronized Throwable getCause() {
            return new EndlessCauses(depth + 1);
        }
    }
}
