#!/usr/bin/env python3
"""Upload and deploy the Kafka Adapter Plus custom adapter (.esa) to a SAP CPI tenant.

Intended for the maintainer-only "Deploy to CPI (E2E tenant)" GitHub Actions workflow
(workflow_dispatch, environment-protected). Not part of the adapter runtime.

Because the custom-adapter designtime artifact is keyed only by its "Id" (no version in
the key), updating it means: delete the existing artifact (if any), upload the new one,
then trigger the async deploy and poll until it finishes. See VERSIONING.md for why the
CPI-visible adapter version is never a preview/qualifier string - this script deploys
whatever version is currently set in config.adk on the checked-out ref, unchanged.

Required environment variables:
  CPI_API_TOKEN_URL      OAuth2 token endpoint (client_credentials grant)
  CPI_API_CLIENT_ID      OAuth2 client id (Integration Content / Design API scope)
  CPI_API_CLIENT_SECRET  OAuth2 client secret
  CPI_API_BASE_URL       CPI tenant base URL (e.g. https://<tenant>.cfapps.<region>.hana.ondemand.com)
  ADAPTER_ID             Custom adapter "Id" (e.g. kafkaAdapterPlus)
  ADAPTER_PACKAGE_ID     Integration Package technical id the adapter belongs to
  ADAPTER_NAME           Display name for the adapter (e.g. Kafka Adapter Plus)
  ESA_PATH               Path to the built .esa file
  ADAPTER_VERSION        Version string to record on the artifact (informational only)
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request


def env(name):
    value = os.environ.get(name)
    if not value:
        print(f"::error::Missing required environment variable {name}", file=sys.stderr)
        sys.exit(1)
    return value


def get_token(token_url, client_id, client_secret):
    creds = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    req = urllib.request.Request(f"{token_url}?grant_type=client_credentials", method="POST")
    req.add_header("Authorization", f"Basic {creds}")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())["access_token"]


def api_request(base_url, token, method, path, body=None, accept_statuses=(200, 201, 202, 204)):
    url = f"{base_url}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read()
            status = resp.status
    except urllib.error.HTTPError as e:
        raw = e.read()
        status = e.code
    if status not in accept_statuses:
        print(f"::error::{method} {path} -> HTTP {status}: {raw.decode(errors='replace')}", file=sys.stderr)
        sys.exit(1)
    return status, raw


def delete_existing(base_url, token, adapter_id):
    print(f"Deleting existing adapter artifact '{adapter_id}' (if present)...")
    status, raw = api_request(
        base_url, token, "DELETE", f"/api/v1/IntegrationAdapterDesigntimeArtifacts('{adapter_id}')",
        accept_statuses=(200, 202, 204, 404),
    )
    if status == 404:
        print("No existing artifact found - nothing to delete.")
    else:
        print(f"Existing artifact deleted (HTTP {status}).")


def upload_artifact(base_url, token, adapter_id, package_id, name, version, esa_path):
    print(f"Uploading '{esa_path}' as adapter '{adapter_id}' (version {version}) into package '{package_id}'...")
    with open(esa_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()
    body = {
        "Id": adapter_id,
        "Version": version,
        "PackageId": package_id,
        "Name": name,
        "ArtifactContent": content_b64,
    }
    status, raw = api_request(base_url, token, "POST", "/api/v1/IntegrationAdapterDesigntimeArtifacts", body=body)
    print(f"Upload accepted (HTTP {status}).")


def deploy_and_wait(base_url, token, adapter_id, timeout_seconds=600, poll_interval=10):
    print(f"Triggering deploy for adapter '{adapter_id}'...")
    status, raw = api_request(
        base_url, token, "POST", f"/api/v1/DeployIntegrationAdapterDesigntimeArtifact?Id='{adapter_id}'",
    )
    task_id = None
    try:
        parsed = json.loads(raw)
        task_id = parsed.get("d", parsed).get("DeployIntegrationAdapterDesigntimeArtifact") if isinstance(parsed, dict) else None
    except (json.JSONDecodeError, AttributeError):
        pass
    if not task_id:
        print("Deploy triggered but no task id returned - assuming synchronous success.")
        return

    print(f"Polling deploy status for task '{task_id}'...")
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        status, raw = api_request(base_url, token, "GET", f"/api/v1/BuildAndDeployStatus('{task_id}')",
                                   accept_statuses=(200, 501))
        if status == 501:
            print("Status polling not available on this tenant - assuming deploy is proceeding asynchronously.")
            return
        try:
            task_status = json.loads(raw)["d"]["Status"]
        except (json.JSONDecodeError, KeyError):
            task_status = None
        print(f"  status: {task_status}")
        if task_status in ("SUCCESS", "FAILED", "ERROR"):
            if task_status != "SUCCESS":
                print(f"::error::Deploy task '{task_id}' ended with status {task_status}", file=sys.stderr)
                sys.exit(1)
            print("Deploy finished successfully.")
            return
        time.sleep(poll_interval)
    print(f"::error::Timed out waiting for deploy task '{task_id}' to finish", file=sys.stderr)
    sys.exit(1)


def main():
    token_url = env("CPI_API_TOKEN_URL")
    client_id = env("CPI_API_CLIENT_ID")
    client_secret = env("CPI_API_CLIENT_SECRET")
    base_url = env("CPI_API_BASE_URL").rstrip("/")
    adapter_id = env("ADAPTER_ID")
    package_id = env("ADAPTER_PACKAGE_ID")
    name = env("ADAPTER_NAME")
    esa_path = env("ESA_PATH")
    version = env("ADAPTER_VERSION")

    if not os.path.isfile(esa_path):
        print(f"::error::ESA file not found at {esa_path}", file=sys.stderr)
        sys.exit(1)

    token = get_token(token_url, client_id, client_secret)
    delete_existing(base_url, token, adapter_id)
    upload_artifact(base_url, token, adapter_id, package_id, name, version, esa_path)
    deploy_and_wait(base_url, token, adapter_id)
    print("Done.")


if __name__ == "__main__":
    main()
