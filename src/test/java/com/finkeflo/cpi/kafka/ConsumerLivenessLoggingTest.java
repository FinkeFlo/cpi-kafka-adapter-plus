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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.camel.impl.DefaultCamelContext;
import org.junit.Assert;
import org.junit.Test;

/**
 * Guards what the consumer writes into the CPI tenant trace file while it is running.
 *
 * <p>The tenant trace file only receives ERROR, which makes the level a statement about whether an
 * operator should ever see the line — not about severity. The liveness heartbeat got that wrong in
 * both directions in turn: unthrottled at ERROR it was every line the adapter contributed to a
 * shared tenant, and throttled it still cost roughly 288 lines per day and endpoint to report that
 * nothing had happened. What an operator actually needs is the opposite — a line when something
 * changes, and silence while it does not.
 */
public class ConsumerLivenessLoggingTest {

    private static final Path SOURCE = Paths.get("src/main/java/com/finkeflo/cpi/kafka")
            .resolve("CpiKafkaPlusConsumer.java");

    private static String source() throws Exception {
        return new String(Files.readAllBytes(SOURCE), StandardCharsets.UTF_8);
    }

    private static String bodyOf(String source, String signature) {
        int start = source.indexOf(signature);
        Assert.assertTrue("Method '" + signature + "' was not found — this test has drifted from "
                + "the code it guards.", start > 0);
        int end = source.indexOf("\n    }", start);
        Assert.assertTrue("Could not delimit the body of '" + signature + "'.", end > start);
        return source.substring(start + signature.length(), end);
    }

    @Test
    public void theRecurringLivenessTickDoesNotReachTheTenantTraceFile() throws Exception {
        String body = bodyOf(source(), "private void logEmitCycleHeartbeat() {");
        int interval = body.indexOf("reason=INTERVAL");
        Assert.assertTrue("The periodic heartbeat branch disappeared.", interval > 0);

        String statement = body.substring(body.lastIndexOf("LOG.", interval), interval);
        Assert.assertTrue("The recurring 'alive' tick must not be logged at ERROR. It reports that "
                        + "the scheduler called poll(), which is not a failure anyone investigates, "
                        + "and it keeps reporting 'alive' while a partition stands still — so it "
                        + "cannot be absent when it matters. At ERROR it competes with real "
                        + "failures for attention in a file shared with every other integration "
                        + "flow on the tenant. Found: " + statement.trim(),
                statement.startsWith("LOG.info("));
    }

    @Test
    public void aChangeOfTheInitialisedStateStillReachesTheTenantTraceFile() throws Exception {
        String body = bodyOf(source(), "private void logEmitCycleHeartbeat() {");
        int stateChange = body.indexOf("reason=STATE_CHANGE");
        Assert.assertTrue("The state-change heartbeat branch disappeared.", stateChange > 0);

        String statement = body.substring(body.lastIndexOf("LOG.", stateChange), stateChange);
        Assert.assertTrue("A change of 'initialized' must stay at ERROR: it is an event, not a "
                        + "tick, it is bounded, and it is the corroborating record that a consumer "
                        + "was torn down. Downgrading it would make a reconnect invisible. "
                        + "Found: " + statement.trim(),
                statement.startsWith("LOG.error("));
    }

    @Test
    public void closingTheConsumerForReconnectReachesTheTenantTraceFile() throws Exception {
        String body = bodyOf(source(), "private void maybeReconnectAfterPollFailure() {");
        int line = body.indexOf("closing consumer for reconnect");
        Assert.assertTrue("The reconnect log line disappeared.", line > 0);

        String statement = body.substring(body.lastIndexOf("LOG.", line), line);
        Assert.assertTrue("Closing the consumer for reconnect must be logged at ERROR. WARN does "
                        + "not reach the tenant trace file, and the rebuild that follows logs at "
                        + "INFO, so at WARN a consumer being destroyed and recreated left no "
                        + "visible record at all. Found: " + statement.trim(),
                statement.startsWith("LOG.error("));
    }

    private CpiKafkaPlusConsumer consumer(DefaultCamelContext ctx) throws Exception {
        ctx.addComponent("cpi-kafka-plus", new CpiKafkaPlusComponent());
        ctx.start();
        CpiKafkaPlusEndpoint ep = (CpiKafkaPlusEndpoint) ctx.getEndpoint(
                "cpi-kafka-plus:t?bootstrapServers=localhost:9092&groupId=g1");
        return (CpiKafkaPlusConsumer) ep.createConsumer(exchange -> { });
    }

    private static int intField(Object target, String name) throws Exception {
        Field f = CpiKafkaPlusConsumer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getInt(target);
    }

    private static boolean booleanField(Object target, String name) throws Exception {
        Field f = CpiKafkaPlusConsumer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getBoolean(target);
    }

    @Test
    public void aContinuingOutageDoesNotRebuildTheConsumerOnEveryFailedPoll() throws Exception {
        try (DefaultCamelContext ctx = new DefaultCamelContext()) {
            CpiKafkaPlusConsumer consumer = consumer(ctx);
            Field initialized = CpiKafkaPlusConsumer.class.getDeclaredField("initialized");
            initialized.setAccessible(true);
            initialized.setBoolean(consumer, true);

            Method reconnect = CpiKafkaPlusConsumer.class
                    .getDeclaredMethod("maybeReconnectAfterPollFailure");
            reconnect.setAccessible(true);

            Field maxField = CpiKafkaPlusConsumer.class
                    .getDeclaredField("MAX_CONSECUTIVE_POLL_FAILURES");
            maxField.setAccessible(true);
            int max = maxField.getInt(null);

            for (int i = 1; i < max; i++) {
                reconnect.invoke(consumer);
                Assert.assertTrue("Failure " + i + " of " + max + " must not reconnect yet.",
                        booleanField(consumer, "initialized"));
            }

            reconnect.invoke(consumer);
            Assert.assertFalse("The threshold failure must trigger the reconnect.",
                    booleanField(consumer, "initialized"));
            Assert.assertEquals("The counter must be reset once the reconnect is triggered, so it "
                            + "measures failures since the last reconnect. Left standing it stays "
                            + "above the threshold for the whole outage, and the consumer is then "
                            + "destroyed and rebuilt on every single failed poll — a rebuild storm "
                            + "of the shape the shared producer was rate-limited against, only "
                            + "unbounded, and a second ERROR line on every failed poll.",
                    0, intField(consumer, "consecutivePollFailures"));
            Assert.assertEquals("The duration window must restart with the counter, otherwise the "
                            + "duration threshold alone keeps triggering a reconnect per poll.",
                    0L, longField(consumer, "firstPollFailureMs"));

            initialized.setBoolean(consumer, true);
            for (int i = 1; i < max; i++) {
                reconnect.invoke(consumer);
                Assert.assertTrue("Failure " + i + " after a reconnect must not reconnect again — "
                                + "the outage continues, but the consumer was just rebuilt.",
                        booleanField(consumer, "initialized"));
            }

            ctx.stop();
        }
    }

    private static long longField(Object target, String name) throws Exception {
        Field f = CpiKafkaPlusConsumer.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.getLong(target);
    }
}
