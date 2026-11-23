# Google Earth Engine

COGs are on AWS S3. Trends.Earth needs them on a GCP bucket. `gee/copy_aws_to_gcp.py` copies 3 test tiles for 3 years from S3 to GCP. STAC items are copied unchanged, so their hrefs still point at the S3 source.

`register_gee_assets.py` registers each STAC item found in GCS as a multi-band image in Google Earth Engine (no pixel ingestion), with one ImageCollection per region: `projects/{GEE_PROJECT}/assets/dep_ls_lulc_0-2-1` (Pacific) and `ci_ls_lulc_0-2-1` (non-Pacific). GEE users can instead read the COGs directly with `ee.Image.loadGeoTIFF`.

Here is an example to visualise LULC:
```javascript
var lulc = ee.Image.loadGeoTIFF(
  "gs://ldn-lulc-public-dev/ci_ls_lulc/0-2-1/141/128/2020/ci_ls_lulc_141_128_2020_classification.tif"
);
Map.centerObject(lulc);
Map.addLayer(lulc.updateMask(lulc.neq(255)), {min: 1, max: 7}, "LULC 2020");  // 255 is nodata
```

## Setup

All steps are once only.

1. Install gcloud and authenticate. `gcloud auth login` is for gcloud commands; application default credentials are for code (obstore and the Earth Engine API).

    ```
    brew install --cask google-cloud-sdk
    gcloud auth login
    gcloud config set project auspatious-earth-engine
    gcloud auth application-default login
    ```

2. Create the bucket (US-CENTRAL1 is required for COG-backed assets):

    ```
    gcloud storage buckets create gs://ldn-lulc-public-dev \
        --project=auspatious-earth-engine \
        --default-storage-class=STANDARD \
        --location=US-CENTRAL1 \
        --uniform-bucket-level-access
    ```

4. Make the bucket publicly readable so anyone, including GEE users in other projects, can read the COGs:

    ```
    gcloud storage buckets add-iam-policy-binding gs://ldn-lulc-public-dev \
        --member=allUsers --role=roles/storage.objectViewer
    ```

    Anyone can read and list objects anonymously, e.g. https://storage.googleapis.com/ldn-lulc-public-dev/ci_ls_lulc/0-2-1/141/128/2000/ci_ls_lulc_141_128_2000_classification.tif. Reads by GEE in US-CENTRAL1 are free, but internet downloads are billed to the `auspatious-earth-engine` project (about $0.12/GB). STAC hrefs still point at S3, so most downloads go there instead.


## Run

```
make gee-copy
make gee-register
```
