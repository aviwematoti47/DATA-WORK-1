"""
Spotfire Python Data Function — BigQuery Crop/Region Query
===========================================================

PURPOSE
-------
Queries a BigQuery table for agricultural data filtered by crop type,
region, and a date range.  The result is returned as a Spotfire
DataFrame output.

SPOTFIRE REGISTRATION (Tools ▶ Register Data Functions…)
---------------------------------------------------------
Script language : Python
Script text     : (paste the entire contents of this file)

Input parameters
~~~~~~~~~~~~~~~~
Name        | Type    | Value / Column
----------- | ------- | -----------------------------------------
crop        | String  | a document property or a table column
region      | String  | a document property or a table column
start_date  | String  | a document property (ISO-8601: YYYY-MM-DD)
end_date    | String  | a document property (ISO-8601: YYYY-MM-DD)

Output parameters
~~~~~~~~~~~~~~~~~
Name        | Type
----------- | -------
result_df   | Table

ENVIRONMENT VARIABLE
--------------------
Set  BQ_SERVICE_ACCOUNT_JSON  to the **absolute path** of the Google
Cloud service-account JSON key file before launching Spotfire Analyst,
for example:

  Windows  :  set BQ_SERVICE_ACCOUNT_JSON=C:/keys/my-sa-key.json
  Linux/Mac:  export BQ_SERVICE_ACCOUNT_JSON=/home/user/keys/my-sa-key.json

DEPENDENCIES
------------
  pip install google-cloud-bigquery pandas pyarrow db-dtypes
"""

import os

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Constants — adjust to match your BigQuery project / dataset / table
# ---------------------------------------------------------------------------
_BQ_PROJECT = os.environ.get("BQ_PROJECT", "your-gcp-project-id")
_BQ_DATASET = os.environ.get("BQ_DATASET", "your_dataset")
_BQ_TABLE = os.environ.get("BQ_TABLE", "your_table")

_ENV_KEY = "BQ_SERVICE_ACCOUNT_JSON"

# ---------------------------------------------------------------------------
# SQL template
# ---------------------------------------------------------------------------
_QUERY_TEMPLATE = """
SELECT *
FROM `{project}.{dataset}.{table}`
WHERE crop   = @crop
  AND region = @region
  AND date  >= @start_date
  AND date  <= @end_date
ORDER BY date
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_inputs(crop, region, start_date, end_date):
    """Raise ValueError if any required input is absent or blank."""
    missing = [
        name
        for name, value in [
            ("crop", crop),
            ("region", region),
            ("start_date", start_date),
            ("end_date", end_date),
        ]
        if not value or not str(value).strip()
    ]
    if missing:
        raise ValueError(
            "The following required inputs are missing or empty: "
            + ", ".join(missing)
        )


def _build_client():
    """
    Return an authenticated BigQuery client.

    Reads the service-account key path from the BQ_SERVICE_ACCOUNT_JSON
    environment variable.  Raises EnvironmentError when the variable is
    not set or the file cannot be found, and google.auth.exceptions.*
    when the credentials are invalid.
    """
    key_path = os.environ.get(_ENV_KEY, "").strip()
    if not key_path:
        raise EnvironmentError(
            f"Environment variable '{_ENV_KEY}' is not set.  "
            "Set it to the path of your Google Cloud service-account JSON key file."
        )
    if not os.path.isfile(key_path):
        raise EnvironmentError(
            f"Service-account key file not found: {key_path!r}.  "
            f"Verify the path stored in the '{_ENV_KEY}' environment variable."
        )

    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
    )
    return bigquery.Client(project=_BQ_PROJECT, credentials=credentials)


def _run_query(client, crop, region, start_date, end_date):
    """Execute the parameterized query and return a pandas DataFrame."""
    sql = _QUERY_TEMPLATE.format(
        project=_BQ_PROJECT,
        dataset=_BQ_DATASET,
        table=_BQ_TABLE,
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("crop", "STRING", str(crop).strip()),
            bigquery.ScalarQueryParameter("region", "STRING", str(region).strip()),
            bigquery.ScalarQueryParameter("start_date", "DATE", str(start_date).strip()),
            bigquery.ScalarQueryParameter("end_date", "DATE", str(end_date).strip()),
        ]
    )

    query_job = client.query(sql, job_config=job_config)
    return query_job.result().to_dataframe()


# ---------------------------------------------------------------------------
# Main entry point — Spotfire calls the top-level scope of this script
# ---------------------------------------------------------------------------
# Spotfire injects these names into the script's global scope before
# execution; the script must assign to  result_df  (the output name).

try:
    # 1. Validate all inputs are present
    _validate_inputs(crop, region, start_date, end_date)  # noqa: F821 — injected by Spotfire

    # 2. Build an authenticated BigQuery client
    _client = _build_client()

    # 3. Run the parameterized query
    result_df = _run_query(_client, crop, region, start_date, end_date)

except EnvironmentError as _env_err:
    # Missing or invalid service-account file — surface a clear message
    result_df = pd.DataFrame(
        {"Error": ["Authentication configuration error"], "Details": [str(_env_err)]}
    )

except ValueError as _val_err:
    # Missing input parameters
    result_df = pd.DataFrame(
        {"Error": ["Missing input parameters"], "Details": [str(_val_err)]}
    )

except GoogleAPIError as _api_err:
    # BigQuery / Google API errors (quota, permissions, bad SQL, …)
    result_df = pd.DataFrame(
        {"Error": ["BigQuery API error"], "Details": [str(_api_err)]}
    )

except Exception as _err:  # noqa: BLE001
    # Catch-all for unexpected errors so Spotfire still receives a table
    result_df = pd.DataFrame(
        {"Error": ["Unexpected error"], "Details": [str(_err)]}
    )
