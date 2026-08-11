#!/usr/bin/env python3
"""Consumer E2E test driver for the Kafka Adapter Plus consumer iFlows.

Produces test message(s) directly to Kafka (bypassing the CPI producer adapter,
since the consumer path is what's under test here), then verifies correct
processing via the SAP CPI Message Processing Log (MPL) API and the
"consumertest" persist step, as documented in `.tmp/e2e_test_plan.md`.

Scenarios:
  json-single  - single JSON message, correlated via SAP_ApplicationID
                 (MPL ApplicationMessageId exact-match filter)
  avro-json    - single Avro-serialized message (Schema Registry), JSON
                 consumer output, correlated via SAP_ApplicationID
  batch-json   - N JSON messages pinned to one partition, verified via an
                 MPL time-window query; batches parsed from the JSON Array
                 persist-step payload
  batch-xml    - like batch-json, but the persist-step payload is XML List
  dlq          - poison-pill XML message with forceError=true; verifies the
                 primary iFlow's MPL entry is FAILED and that the message
                 was routed to the configured dead-letter topic unchanged

Required credentials (CLI flags or environment variables of the same name
in upper case, see --help):
  Kafka:            E2E_KAFKA_BOOTSTRAP_SERVERS, E2E_KAFKA_SASL_USERNAME,
                     E2E_KAFKA_SASL_PASSWORD
  CPI API (MPL):     E2E_CPI_API_BASE_URL, E2E_CPI_API_TOKEN_URL,
                     E2E_CPI_API_CLIENT_ID, E2E_CPI_API_CLIENT_SECRET
  Schema Registry:   E2E_SCHEMA_REGISTRY_URL, E2E_SCHEMA_REGISTRY_USERNAME,
                     E2E_SCHEMA_REGISTRY_PASSWORD (only for avro-json)
"""

import argparse
import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, Producer


# --- Kafka helpers ---

def kafka_common_config(args):
    return {
        "bootstrap.servers": args.kafka_bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": args.kafka_username,
        "sasl.password": args.kafka_password,
    }


def produce(args, topic, key, value, headers=None):
    producer = Producer(kafka_common_config(args))
    errors = []

    def on_delivery(err, _msg):
        if err:
            errors.append(err)

    producer.produce(
        topic=topic,
        key=key.encode("utf-8") if isinstance(key, str) else key,
        value=value.encode("utf-8") if isinstance(value, str) else value,
        partition=args.partition if args.partition is not None else -1,
        headers=headers or [],
        on_delivery=on_delivery,
    )
    producer.flush(10)
    if errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}")


# --- SAP CPI API helpers ---

def get_oauth_token(token_url, client_id, client_secret):
    data = urllib.parse.urlencode({"grant_type": "client_credentials"}).encode()
    auth = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    req = urllib.request.Request(
        token_url,
        data=data,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())["access_token"]


class CpiApiClient:
    def __init__(self, base_url, token):
        self.base_url = base_url
        self.token = token

    def _get(self, path):
        req = urllib.request.Request(
            self.base_url + path, headers={"Authorization": f"Bearer {self.token}"}
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read()

    def _get_json(self, path):
        return json.loads(self._get(path))

    def mpl_query(self, filt, orderby=None):
        params = {"$filter": filt, "$format": "json"}
        if orderby:
            params["$orderby"] = orderby
        qs = urllib.parse.urlencode(params)
        return self._get_json(f"/api/v1/MessageProcessingLogs?{qs}")["d"]["results"]

    def mpl_wait_for(self, filt, timeout_s, poll_interval_s=3, min_count=1, orderby=None):
        deadline = time.monotonic() + timeout_s
        entries = []
        while True:
            entries = self.mpl_query(filt, orderby=orderby)
            if len(entries) >= min_count or time.monotonic() >= deadline:
                return entries
            time.sleep(poll_interval_s)

    def persist_step_payload(self, message_guid, store_id="consumertest"):
        entries = self._get_json(
            f"/api/v1/MessageProcessingLogs('{message_guid}')/MessageStoreEntries?$format=json"
        )["d"]["results"]
        match = [e for e in entries if e.get("MessageStoreId") == store_id]
        if not match:
            available = [e.get("MessageStoreId") for e in entries]
            raise AssertionError(
                f"no '{store_id}' persist-step entry found for MPL {message_guid} "
                f"(available store ids: {available})"
            )
        return self._get(f"/api/v1/MessageStoreEntries('{match[0]['Id']}')/$value")


# --- Batch payload parsing (JSON Array / XML List persist-step formats) ---

def parse_batch_keys(raw, xml):
    if xml:
        root = ET.fromstring(raw)
        records = root.findall("record")
        count_attr = int(root.attrib["count"])
        if count_attr != len(records):
            raise AssertionError(
                f"XML 'count' attribute ({count_attr}) does not match actual record count ({len(records)})"
            )
        return [r.findtext("key") for r in records]

    data = json.loads(raw)
    records = data["kafkaRecords"]["record"]
    if isinstance(records, dict):  # single-record batches unwrap to a bare object, not a list
        records = [records]
    return [r["key"] for r in records]


# --- Scenarios ---

def run_single(args, api, avro):
    test_id = str(uuid.uuid4())

    if avro:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        sr_client = SchemaRegistryClient(
            {
                "url": args.schema_registry_url,
                "basic.auth.user.info": f"{args.schema_registry_username}:{args.schema_registry_password}",
            }
        )
        subject = args.topic_subject or f"{args.topic}-value"
        schema_str = sr_client.get_latest_version(subject).schema.schema_str
        serializer = AvroSerializer(sr_client, schema_str)
        expected = {
            "testId": test_id,
            "message": "avro-consumer-e2e-test",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        payload = serializer(expected, SerializationContext(args.topic, MessageField.VALUE))
    else:
        expected = {"testId": test_id, "message": "consumer-e2e-test"}
        payload = json.dumps(expected).encode("utf-8")

    produce(
        args,
        args.topic,
        test_id,
        payload,
        headers=[("X-Kafka-Adapter-Plus-Trace-Uuid", test_id.encode("utf-8"))],
    )

    entries = api.mpl_wait_for(f"ApplicationMessageId eq '{test_id}'", timeout_s=args.timeout)
    if len(entries) != 1:
        raise AssertionError(f"expected exactly 1 MPL entry for testId {test_id}, got {len(entries)}")
    entry = entries[0]
    if entry["Status"] != "COMPLETED":
        raise AssertionError(f"expected MPL Status COMPLETED, got {entry['Status']}")

    raw = api.persist_step_payload(entry["MessageGuid"])
    actual = json.loads(raw)
    for field, expected_value in expected.items():
        if actual.get(field) != expected_value:
            raise AssertionError(
                f"field '{field}' mismatch: expected {expected_value!r}, got {actual.get(field)!r}"
            )

    print(f"OK: {args.scenario} testId={test_id} verified (MPL={entry['MessageGuid']})")


def run_batch(args, api, xml):
    count = args.message_count
    batch_size = args.batch_size
    # Small buffer to tolerate clock skew between the runner and the CPI tenant.
    start_dt = (datetime.now(timezone.utc) - timedelta(seconds=5)).strftime("%Y-%m-%dT%H:%M:%S")

    test_ids = []
    for i in range(count):
        test_id = str(uuid.uuid4())
        test_ids.append(test_id)
        value = json.dumps({"testId": test_id, "seq": i, "testCase": args.scenario})
        produce(args, args.topic, test_id, value)

    filt = f"IntegrationFlowName eq '{args.iflow_name}' and LogEnd ge datetime'{start_dt}'"

    deadline = time.monotonic() + args.timeout
    entries = []
    parsed_batches = []
    found_ids = set()
    expected_ids = set(test_ids)
    while True:
        entries = api.mpl_query(filt, orderby="LogEnd asc")
        parsed_batches = [parse_batch_keys(api.persist_step_payload(e["MessageGuid"]), xml) for e in entries]
        found_ids = {k for batch in parsed_batches for k in batch}
        if expected_ids.issubset(found_ids) or time.monotonic() >= deadline:
            break
        time.sleep(args.poll_interval)

    if not expected_ids.issubset(found_ids):
        missing = expected_ids - found_ids
        raise AssertionError(f"missing {len(missing)} testId(s) after {args.timeout}s timeout: {missing}")

    # A previous run's MPL entries can fall inside the LogEnd window and the consumer can
    # replay foreign records; only this run's ids are in scope for the assertions below.
    parsed_batches = [batch for batch in parsed_batches if expected_ids.intersection(batch)]

    batch_sizes = [len(b) for b in parsed_batches]
    if any(size > batch_size for size in batch_sizes):
        raise AssertionError(f"a batch exceeded configured batchSize={batch_size}: sizes={batch_sizes}")

    flat_order = [k for batch in parsed_batches for k in batch if k in expected_ids]
    if flat_order != test_ids:
        raise AssertionError("record order across batches does not match send order")

    print(
        f"OK: {args.scenario}: {count} messages split into {len(parsed_batches)} batch(es) {batch_sizes}, "
        f"all present, no duplicates, correct order"
    )
    # The exact split is timing-dependent (see .tmp/e2e_test_plan.md); only warn, don't fail,
    # but a single all-in-one-batch result is worth flagging as it previously indicated a
    # stale/misconfigured deployment.
    if len(parsed_batches) == 1 and count > batch_size:
        print(
            f"::warning::all {count} records landed in a single MPL entry despite batchSize={batch_size} "
            "- this can indicate a stale/misconfigured deployment; verify manually if unexpected"
        )


def run_dlq(args, api):
    test_id = str(uuid.uuid4())
    xml_payload = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f"<TestMessage><testId>{test_id}</testId><forceError>true</forceError>"
        "<note>dlq-poison-pill-e2e-test</note></TestMessage>"
    )
    produce(
        args,
        args.topic,
        test_id,
        xml_payload,
        headers=[("X-Kafka-Adapter-Plus-Trace-Uuid", test_id.encode("utf-8"))],
    )

    entries = api.mpl_wait_for(f"ApplicationMessageId eq '{test_id}'", timeout_s=args.timeout)
    if len(entries) != 1:
        raise AssertionError(f"expected exactly 1 MPL entry for testId {test_id}, got {len(entries)}")
    if entries[0]["Status"] != "FAILED":
        raise AssertionError(
            f"expected MPL Status FAILED for poison-pill message, got {entries[0]['Status']}"
        )

    consumer = Consumer(
        {
            **kafka_common_config(args),
            "group.id": f"e2e-dlq-check-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([args.dlq_topic])
    found = None
    deadline = time.monotonic() + args.timeout
    try:
        while time.monotonic() < deadline and found is None:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode("utf-8", errors="replace") if msg.key() else None
            if key == test_id:
                found = msg
    finally:
        consumer.close()

    if found is None:
        raise AssertionError(f"testId {test_id} not found in dead-letter topic {args.dlq_topic} within timeout")

    headers = {h[0]: h[1].decode("utf-8", errors="replace") for h in (found.headers() or [])}
    if headers.get("X-Kafka-Adapter-Plus-Trace-Uuid") != test_id:
        raise AssertionError("trace header not preserved in dead-letter message")
    if headers.get("CpiKafkaPlusDlqErrorType") != "PERMANENT":
        raise AssertionError(
            f"expected CpiKafkaPlusDlqErrorType=PERMANENT, got {headers.get('CpiKafkaPlusDlqErrorType')!r}"
        )
    if test_id not in found.value().decode("utf-8", errors="replace"):
        raise AssertionError("dead-letter payload does not contain original testId")

    print(
        f"OK: dlq testId={test_id} correctly routed to {args.dlq_topic} with preserved header/payload "
        f"(errorType={headers.get('CpiKafkaPlusDlqErrorType')}, retryCount={headers.get('CpiKafkaPlusDlqRetryCount')})"
    )


# --- CLI ---

def build_arg_parser():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--scenario", required=True,
                   choices=["json-single", "avro-json", "batch-json", "batch-xml", "dlq"])
    p.add_argument("--topic", required=True, help="Kafka topic the consumer iFlow reads from")
    p.add_argument("--iflow-name", required=True, help="IntegrationFlowName as reported in the MPL")
    p.add_argument("--dlq-topic", help="Dead-letter topic (required for --scenario dlq)")
    p.add_argument("--partition", type=int, default=0,
                   help="Partition to pin all produced messages to (deterministic batch splitting)")
    p.add_argument("--message-count", type=int, default=7, help="batch-* scenarios: number of messages to send")
    p.add_argument("--batch-size", type=int, default=3, help="batch-* scenarios: configured adapter Batch Size")
    p.add_argument("--timeout", type=int, default=60, help="Seconds to wait for MPL/DLQ results")
    p.add_argument("--poll-interval", type=int, default=3, help="Seconds between MPL poll retries")
    p.add_argument("--topic-subject", help="avro-json: Schema Registry subject to read the schema from")

    p.add_argument("--kafka-bootstrap", default=os.environ.get("E2E_KAFKA_BOOTSTRAP_SERVERS"))
    p.add_argument("--kafka-username", default=os.environ.get("E2E_KAFKA_SASL_USERNAME"))
    p.add_argument("--kafka-password", default=os.environ.get("E2E_KAFKA_SASL_PASSWORD"))

    p.add_argument("--cpi-api-base-url", default=os.environ.get("E2E_CPI_API_BASE_URL"))
    p.add_argument("--cpi-api-token-url", default=os.environ.get("E2E_CPI_API_TOKEN_URL"))
    p.add_argument("--cpi-api-client-id", default=os.environ.get("E2E_CPI_API_CLIENT_ID"))
    p.add_argument("--cpi-api-client-secret", default=os.environ.get("E2E_CPI_API_CLIENT_SECRET"))

    p.add_argument("--schema-registry-url", default=os.environ.get("E2E_SCHEMA_REGISTRY_URL"))
    p.add_argument("--schema-registry-username", default=os.environ.get("E2E_SCHEMA_REGISTRY_USERNAME"))
    p.add_argument("--schema-registry-password", default=os.environ.get("E2E_SCHEMA_REGISTRY_PASSWORD"))
    return p


def main():
    args = build_arg_parser().parse_args()

    required = ["kafka_bootstrap", "kafka_username", "kafka_password",
                "cpi_api_base_url", "cpi_api_token_url", "cpi_api_client_id", "cpi_api_client_secret"]
    if args.scenario == "dlq" and not args.dlq_topic:
        print("::error::--dlq-topic is required for --scenario dlq", file=sys.stderr)
        sys.exit(2)
    if args.scenario == "avro-json":
        required += ["schema_registry_url", "schema_registry_username", "schema_registry_password"]

    missing = [name for name in required if not getattr(args, name)]
    if missing:
        print(f"::error::missing required credentials: {missing}", file=sys.stderr)
        sys.exit(2)

    token = get_oauth_token(args.cpi_api_token_url, args.cpi_api_client_id, args.cpi_api_client_secret)
    api = CpiApiClient(args.cpi_api_base_url, token)

    try:
        if args.scenario == "json-single":
            run_single(args, api, avro=False)
        elif args.scenario == "avro-json":
            run_single(args, api, avro=True)
        elif args.scenario == "batch-json":
            run_batch(args, api, xml=False)
        elif args.scenario == "batch-xml":
            run_batch(args, api, xml=True)
        elif args.scenario == "dlq":
            run_dlq(args, api)
    except AssertionError as e:
        print(f"::error::{args.scenario} FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
