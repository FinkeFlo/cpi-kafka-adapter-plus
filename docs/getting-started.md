# Getting Started

## 1. Download the latest release

Go to the [GitHub Releases page](https://github.com/FinkeFlo/cpi-kafka-adapter-plus/releases) and download the `.esa` file from the latest release (e.g. `cpi-kafka-adapter-plus-1.2.3.esa`).

No build step required — the ESA is a ready-to-deploy artifact.

## 2. Upload to SAP Integration Suite

1. Open your SAP Integration Suite tenant and navigate to **Design**
2. Open the Integration Package you want to add the adapter to (or create a new one)
3. Switch the package to edit mode and click **Add → Integration Adapter**
4. Select the downloaded `.esa` file and confirm the upload

The adapter now appears as an artifact inside the package.

## 3. Deploy the adapter

Select the adapter artifact in the package and click **Deploy**. The adapter runtime becomes available on the tenant once the deployment completes.

!!! note "Verify availability"
    After deployment, open any IFlow and add a Sender or Receiver channel — **CPI Kafka Plus** should appear in the adapter type list.

## Updating to a newer version

1. Download the new `.esa` from the [Releases page](https://github.com/FinkeFlo/cpi-kafka-adapter-plus/releases)
2. Open the existing adapter artifact in the package and upload the new ESA
3. **Before deploying**, open the artifact and click **View Metadata** — this refreshes the adapter metadata in the package and prevents transport issues with SAP Cloud Transport Management
4. Deploy the updated artifact

!!! warning "Always refresh metadata before transport"
    Skipping the **View Metadata** step can cause inconsistencies when transporting the package via SAP Cloud Transport Management.

## What's Next

- [Configuration Reference](configuration.md) — All connection, security, batch, and Avro settings
- [Batch Processing](features/batch-processing.md) — Set up high-throughput batch consumption
- [Authentication](security/authentication.md) — Configure SASL/SSL security
