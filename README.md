# DATA-WORK-1

## Spotfire Python Data Function — BigQuery Crop/Region Query

`bigquery_data_function.py` is a **Spotfire Python Data Function** that queries a
Google BigQuery table for agricultural data, filtered by crop type, region, and a
date range.

---

### Prerequisites

| Requirement | Notes |
|---|---|
| Spotfire Analyst (10.x or later) | Python Data Functions must be enabled |
| Python 3.8+ | Installed and configured in Spotfire |
| Google Cloud SDK / service-account key | A JSON key file with BigQuery read access |
| Python packages | `google-cloud-bigquery pandas pyarrow db-dtypes` |

Install the required packages in the Python environment used by Spotfire:

```bash
pip install google-cloud-bigquery pandas pyarrow db-dtypes
```

---

### Environment Variable

Before launching Spotfire Analyst, set the `BQ_SERVICE_ACCOUNT_JSON` environment
variable to the **absolute path** of your Google Cloud service-account JSON key file:

**Windows**
```cmd
set BQ_SERVICE_ACCOUNT_JSON=C:\keys\my-sa-key.json
```

**Linux / macOS**
```bash
export BQ_SERVICE_ACCOUNT_JSON=/home/user/keys/my-sa-key.json
```

Optional overrides (defaults are set inside the script):

| Variable | Default | Description |
|---|---|---|
| `BQ_PROJECT` | `your-gcp-project-id` | GCP project that contains the dataset |
| `BQ_DATASET` | `your_dataset` | BigQuery dataset name |
| `BQ_TABLE` | `your_table` | BigQuery table name |

---

### Registering the Data Function in Spotfire

1. Open Spotfire Analyst.
2. Go to **Tools ▶ Register Data Functions…**
3. Click **New** and choose **Python Script** as the script language.
4. Paste the entire contents of `bigquery_data_function.py` into the script editor.
5. Set the **Name** (e.g. `BigQuery Crop Query`).

#### Input Parameters

| Name | Type | Recommended source |
|---|---|---|
| `crop` | String | Document Property or Table Column |
| `region` | String | Document Property or Table Column |
| `start_date` | String | Document Property (ISO-8601 format: `YYYY-MM-DD`) |
| `end_date` | String | Document Property (ISO-8601 format: `YYYY-MM-DD`) |

#### Output Parameters

| Name | Type |
|---|---|
| `result_df` | Table |

6. Click **OK** to save the registration.
7. Insert the Data Function into your analysis via
   **Insert ▶ Data Function…**, select the registered function,
   map the inputs and output, then click **Run**.

---

### Error Handling

If an error occurs the function will **not** raise an exception.
Instead, `result_df` will contain a two-column table with the columns
`Error` and `Details` so that Spotfire can still display a result:

| Scenario | `Error` column value |
|---|---|
| `BQ_SERVICE_ACCOUNT_JSON` not set or file missing | `Authentication configuration error` |
| One or more inputs are blank | `Missing input parameters` |
| BigQuery / Google API error | `BigQuery API error` |
| Any other unexpected error | `Unexpected error` |

---

### SQL Query

The data function executes the following parameterized query (adjust the
`_BQ_PROJECT`, `_BQ_DATASET`, and `_BQ_TABLE` constants or the corresponding
environment variables to match your schema):

```sql
SELECT *
FROM `<project>.<dataset>.<table>`
WHERE crop   = @crop
  AND region = @region
  AND date  >= @start_date
  AND date  <= @end_date
ORDER BY date
```

All parameters are passed as `ScalarQueryParameter` values to prevent
SQL injection.
