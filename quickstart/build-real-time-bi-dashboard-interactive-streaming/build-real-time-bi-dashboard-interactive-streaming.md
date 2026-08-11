author: Chanin Nantasenamat
id: build-real-time-bi-dashboard-interactive-streaming
summary: Build a live e-commerce BI dashboard on Snowflake using interactive tables, an interactive warehouse, a Snowpark Container Services simulation service, and Streamlit in Snowflake — no external message bus required.
categories: snowflake-site:taxonomy/solution-center/certification/quickstart, snowflake-site:taxonomy/product/analytics, snowflake-site:taxonomy/product/applications-and-collaboration, snowflake-site:taxonomy/snowflake-feature/interactive-tables, snowflake-site:taxonomy/snowflake-feature/interactive-warehouse
environments: web
language: en
status: Published
feedback link: https://github.com/Snowflake-Labs/sfguides/issues

# Build a Real-Time BI Dashboard with Interactive Streaming
<!-- ------------------------ -->
## Overview

Traditional analytical warehouses are optimized for large batch scans. When a dashboard needs to refresh every few seconds and answer sub-second queries at high concurrency, you need a different architecture. Snowflake's **interactive tables** and **interactive warehouses** close that gap: they maintain a warm in-memory cache tuned for selective, low-latency reads.

In this quickstart, you will build a complete real-time analytics system for a synthetic e-commerce store. A **Snowpark Container Services (SPCS)** container generates orders server-side at a configurable rate, writing each tick into an interactive table through a stored procedure. An **interactive warehouse** keeps that table's cache warm and serves millisecond-latency queries to a **Streamlit in Snowflake** dashboard that refreshes automatically every few seconds.

Everything runs inside a single Snowflake account — no Kafka, no external message bus, no managed services outside Snowflake.

![Architecture diagram showing the data flow between the SPCS simulation service, stored procedure, interactive table, interactive warehouse, history and control tables, maintenance tasks, and the Streamlit dashboard.](assets/architecture-diagram.png)

<!-- Editable source: https://excalidraw.com/#json=bQ6oGHlYPPAosL35KrNzf,QPKcLrtlIZzJUk9AVuRhbg -->
<!-- Local source: assets/architecture-diagram.excalidraw -->

### What You'll Learn
- How to create an interactive table with a clustering key tuned for dashboard queries
- How to attach an interactive table to an interactive warehouse and query it at sub-second latency
- How to build and deploy a long-running Python service on Snowpark Container Services
- How to use `EXECUTE AS OWNER` in a stored procedure to bridge permission boundaries
- How to build an auto-refreshing dashboard with `st.fragment` in Streamlit in Snowflake

### What You'll Build
You will assemble five tightly integrated Snowflake objects into a single, self-contained real-time analytics system:
- An SPCS simulation service continuously generates synthetic e-commerce orders and writes them — via a stored procedure — into an interactive table
- An interactive warehouse keeps that table warm for sub-second dashboard queries
- A history table accumulates every tick for trend charts and heatmaps
- A control table acts as the command channel between the dashboard and the simulation service
- A Streamlit in Snowflake app ties everything together, giving you live KPI tiles, auto-refreshing charts, and full simulation controls in one interface

### Prerequisites
- Access to a [Snowflake account](https://signup.snowflake.com/?utm_source=snowflake-devrel&utm_medium=developer-guides&utm_cta=developer-guides) with `ACCOUNTADMIN`. The account must be in a region that supports both [interactive tables and interactive warehouses](https://docs.snowflake.com/en/user-guide/interactive#region-availability) and [Snowpark Container Services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview#available-regions).
- [Docker Desktop](https://docs.docker.com/get-docker/) installed and running locally.
- [Git](https://git-scm.com/) installed.
- Basic familiarity with SQL and Python.

<!-- ------------------------ -->
## Clone the Repository

Fork and clone the companion repository:

```bash
git clone https://github.com/Snowflake-Labs/sfguide-build-real-time-bi-dashboard-interactive-streaming.git
cd sfguide-build-real-time-bi-dashboard-interactive-streaming
```

The repository has the following structure:

```
├── scripts/
│   └── setup.sql             # Creates all Snowflake objects in one pass
├── spcs/
│   ├── Dockerfile            # Container definition for the simulation service
│   ├── requirements.txt      # Python dependencies (snowflake-connector-python)
│   ├── simulation_service.py # Long-running service loop
│   └── simulation_spec.yaml  # SPCS service specification
└── streamlit/
    ├── streamlit_app.py      # Live dashboard application
    └── environment.yml       # Streamlit in Snowflake Python environment
```

<!-- ------------------------ -->
## Create Snowflake Objects

All setup SQL is in `scripts/setup.sql`. Open a Snowsight worksheet and paste the blocks below, or run the full file in one shot. The script is idempotent — safe to re-run.

Here is a summary of the Snowflake objects you will create:

| Object | Type | Purpose |
|---|---|---|
| `ORDERS` | Interactive Table | Current tick snapshot of live orders |
| `ORDERS_HISTORY` | Table | Accumulates all historical ticks for charts and heatmaps |
| `SIMULATION_CONTROL` | Table | Command channel between the dashboard and the simulation service |
| `SP_SIMULATE_TICK` | Stored Procedure | Generates synthetic orders each tick |
| `INTERACTIVE_ANALYTICS_WH` | Virtual Warehouse | DDL, task execution, and general queries |
| `INTERACTIVE_ANALYTICS_IWH` | Interactive Warehouse | Sub-second query cache for the ORDERS table |
| `INTERACTIVE_ANALYTICS_POOL` | Compute Pool | SPCS node that runs the simulation container |
| `INTERACTIVE_ANALYTICS_SVC` | SPCS Service | Long-running simulation service |
| `INTERACTIVE_ANALYTICS_AUTOSTOP` | Task (serverless) | Stops the simulation after 1 hour of inactivity |
| `INTERACTIVE_ANALYTICS_HISTORY_CLEANUP` | Task (warehouse) | Purges ORDERS_HISTORY rows older than 1 day |
| `LIVE_ORDERS_DASHBOARD` | Streamlit in Snowflake | Live dashboard with KPI tiles, charts, and simulation controls |

### Database and schema

```sql
USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO;
CREATE SCHEMA   IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC;
```

### Standard warehouse

The standard warehouse handles DDL, stored procedure execution, and queries against regular tables. Interactive warehouses can only query interactive tables, so a separate standard warehouse is needed for everything else.

```sql
CREATE WAREHOUSE IF NOT EXISTS INTERACTIVE_ANALYTICS_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;
```

### Interactive table

`ORDERS` holds the current tick snapshot — only the most recently generated set of orders. The `CLUSTER BY` key matches the dashboard's `WHERE` clause: a recent time window filtered by region. Truncating the timestamp to the minute keeps the clustering key low-cardinality and prunes efficiently for "last N minutes" queries.

```sql
CREATE OR REPLACE INTERACTIVE TABLE INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS (
  ORDER_ID          STRING,
  ORDER_TS          TIMESTAMP_NTZ,
  REGION            STRING,
  PRODUCT_CATEGORY  STRING,
  QUANTITY          INT,
  AMOUNT            NUMBER(10,2)
)
CLUSTER BY (DATE_TRUNC('MINUTE', ORDER_TS), REGION);
```

### Interactive warehouse

Create the interactive warehouse and attach `ORDERS` to it so its data cache stays warm for the dashboard.

```sql
CREATE INTERACTIVE WAREHOUSE IF NOT EXISTS INTERACTIVE_ANALYTICS_IWH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

ALTER WAREHOUSE INTERACTIVE_ANALYTICS_IWH RESUME IF SUSPENDED;

ALTER WAREHOUSE INTERACTIVE_ANALYTICS_IWH
  ADD TABLES (INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS);
```

Positive
: Interactive warehouses enforce a **5-second query timeout** and a **24-hour minimum auto-suspend**. Keep dashboard queries selective — narrow projections, filters aligned with the clustering key — for consistent sub-second latency.

### History and control tables

`ORDERS_HISTORY` accumulates every tick so the dashboard can display multi-minute time-series charts and a Region x Category heatmap. `SIMULATION_CONTROL` is the command channel between the dashboard and the SPCS service.

```sql
CREATE TABLE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY (
  ORDER_ID          STRING,
  ORDER_TS          TIMESTAMP_NTZ,
  REGION            STRING,
  PRODUCT_CATEGORY  STRING,
  QUANTITY          NUMBER,
  AMOUNT            NUMBER(10,2),
  TICK_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL (
  is_running      BOOLEAN       NOT NULL DEFAULT FALSE,
  mode            VARCHAR       NOT NULL DEFAULT 'normal',
  flash_category  VARCHAR,
  rate_mult       FLOAT         NOT NULL DEFAULT 1.0,
  last_active_at  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  total_orders    NUMBER        NOT NULL DEFAULT 0
);

INSERT INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL
  (is_running, mode, flash_category, rate_mult, last_active_at, total_orders)
VALUES (FALSE, 'normal', NULL, 1.0, CURRENT_TIMESTAMP(), 0);
```

### Simulation stored procedure

`SP_SIMULATE_TICK` is the heart of the simulation. The SPCS service calls it every second. In a single round-trip it:

1. Replaces the `ORDERS` snapshot with `INSERT OVERWRITE`.
2. Appends the new rows to `ORDERS_HISTORY`.
3. Increments the `TOTAL_ORDERS` counter in `SIMULATION_CONTROL`.

The `EXECUTE AS OWNER` clause lets the service call the procedure without needing direct `INSERT OVERWRITE` permission on the interactive table, which is restricted to the owner role.

```sql
CREATE OR REPLACE PROCEDURE INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(
    MODE           VARCHAR,
    FLASH_CATEGORY VARCHAR,
    NUM_ROWS       FLOAT,
    DOMINANT       VARCHAR,
    DOM_PCT        FLOAT
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var flash   = FLASH_CATEGORY || null;
    var numRows = Math.floor(NUM_ROWS);

    // ---- Tick categories ----
    // Pick 2-5 categories randomly for this tick.
    // If a Flash Sale is active, the promoted category is always included.
    var all_cats = ['Electronics','Apparel','Home','Beauty','Sports','Toys'];
    for (var i = all_cats.length - 1; i > 0; i--) {   // Fisher-Yates shuffle
        var j = Math.floor(Math.random() * (i + 1));
        var tmp = all_cats[i]; all_cats[i] = all_cats[j]; all_cats[j] = tmp;
    }
    var n_cats    = Math.floor(Math.random() * 4) + 2;
    var tick_cats = all_cats.slice(0, n_cats);
    if (MODE === 'flash_sale' && flash && tick_cats.indexOf(flash) === -1) {
        tick_cats[tick_cats.length - 1] = flash;
    }

    var n         = tick_cats.length;
    var quoted    = tick_cats.map(function(c) { return "'" + c + "'"; });
    var cat_array = 'ARRAY_CONSTRUCT(' + quoted.join(',') + ')';
    var cat_idx   = 'UNIFORM(0,' + (n - 1) + ',RANDOM())';

    var cat_sql, amount_sql;

    if (MODE === 'flash_sale' && flash) {
        // Flash Sale: 50 % of orders go to the promoted category; prices elevated.
        cat_sql    = "CASE WHEN UNIFORM(0,99,RANDOM()) < 50 THEN '" + flash + "'"
                   + " ELSE " + cat_array + "[" + cat_idx + "]::STRING END";
        amount_sql = "CASE WHEN PRODUCT_CATEGORY = '" + flash + "'"
                   + " THEN UNIFORM(50,800,RANDOM()) ELSE UNIFORM(5,400,RANDOM()) END";
    } else {
        // Each region has a preferred product category with a graduated
        // probability spectrum (28 % down to 8 %), creating a visible
        // diagonal gradient in the Region x Category heatmap.
        var rw = {
            'North America': {'Electronics':28,'Sports':22,'Beauty':18,'Home':14,'Toys':10,'Apparel':8},
            'Europe':        {'Apparel':28,'Electronics':22,'Beauty':20,'Toys':14,'Home':8,'Sports':8},
            'APAC':          {'Toys':28,'Home':22,'Electronics':18,'Beauty':14,'Apparel':10,'Sports':8},
            'LATAM':         {'Home':28,'Apparel':22,'Beauty':18,'Sports':14,'Toys':10,'Electronics':8},
            'MEA':           {'Beauty':28,'Toys':22,'Home':18,'Apparel':14,'Electronics':10,'Sports':8}
        };
        var regs   = ['North America','Europe','APAC','LATAM','MEA'];
        var rcases = [];
        for (var ri = 0; ri < regs.length; ri++) {
            var reg = regs[ri];
            var w   = rw[reg];
            var wl  = tick_cats.map(function(c) { return {cat: c, w: w[c] || 8}; });
            wl.sort(function(a, b) { return b.w - a.w; });
            var tot = 0;
            for (var wi = 0; wi < wl.length; wi++) { tot += wl[wi].w; }
            var cum = 0;
            var inn = [];
            for (var k = 0; k < wl.length - 1; k++) {
                cum += Math.round(wl[k].w * 100 / tot);
                inn.push('WHEN rv < ' + cum + " THEN '" + wl[k].cat + "'");
            }
            inn.push("ELSE '" + wl[wl.length - 1].cat + "'");
            // One UNIFORM draw per row via scalar subquery for accurate distribution.
            rcases.push(
                "WHEN REGION = '" + reg + "' THEN "
                + '(SELECT CASE ' + inn.join(' ') + ' END'
                + ' FROM (SELECT UNIFORM(0,99,RANDOM()) AS rv) _t)'
            );
        }
        rcases.push('ELSE ' + cat_array + '[' + cat_idx + ']::STRING');
        cat_sql    = 'CASE ' + rcases.join(' ') + ' END';
        amount_sql = 'UNIFORM(5,400,RANDOM())';
    }

    var sql = [
        'INSERT OVERWRITE INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS',
        'SELECT ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY,',
        '       ROUND(UNIT_PRICE * QUANTITY, 2) AS AMOUNT',
        'FROM (',
        '  SELECT UUID_STRING() AS ORDER_ID,',
        "    DATEADD('second',-UNIFORM(0,300,RANDOM()),CURRENT_TIMESTAMP())",
        '      ::TIMESTAMP_NTZ AS ORDER_TS,',
        "    ARRAY_CONSTRUCT('North America','Europe','APAC','LATAM','MEA')",
        '      [UNIFORM(0,4,RANDOM())]::STRING AS REGION,',
        '    ' + cat_sql + ' AS PRODUCT_CATEGORY,',
        '    UNIFORM(1,5,RANDOM()) AS QUANTITY,',
        '    ' + amount_sql + ' AS UNIT_PRICE',
        '  FROM TABLE(GENERATOR(ROWCOUNT => ' + numRows + '))',
        ')'
    ];
    snowflake.execute({sqlText: sql.join(' ')});

    snowflake.execute({sqlText:
        'INSERT INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY'
        + ' (ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY, AMOUNT, TICK_AT)'
        + ' SELECT ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY, AMOUNT,'
        + ' CURRENT_TIMESTAMP() FROM INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS'
    });

    snowflake.execute({sqlText:
        'UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL'
        + ' SET TOTAL_ORDERS = COALESCE(TOTAL_ORDERS, 0) + ' + numRows
        + ',    LAST_ACTIVE_AT = CURRENT_TIMESTAMP()'
    });
    return 'OK';
$$;
```

### Compute pool and image repository

The SPCS service runs on a compute pool. The image repository is a Snowflake-managed private Docker registry.

```sql
CREATE COMPUTE POOL IF NOT EXISTS INTERACTIVE_ANALYTICS_POOL
  MIN_NODES         = 1
  MAX_NODES         = 1
  INSTANCE_FAMILY   = CPU_X64_XS
  AUTO_RESUME       = TRUE
  AUTO_SUSPEND_SECS = 3600;

CREATE IMAGE REPOSITORY IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.SIM_REPO;

-- Note the repository_url in the output — you will use it in the next step.
SHOW IMAGE REPOSITORIES LIKE 'SIM_REPO'
  IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC;
```

### App stage

```sql
CREATE STAGE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE;
```

### Maintenance tasks

These tasks run in the background to manage costs. The autostop task turns off the simulation after one hour of inactivity. The cleanup task deletes history rows older than one day to keep storage costs low.

```sql
CREATE OR REPLACE TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP
  SCHEDULE                                 = '15 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  COMMENT                                  = 'Auto-stop simulation when idle > 1 h'
AS
  UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL
  SET is_running = FALSE
  WHERE is_running = TRUE
    AND last_active_at < DATEADD('hour', -1, CURRENT_TIMESTAMP());

CREATE OR REPLACE TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP
  WAREHOUSE = INTERACTIVE_ANALYTICS_WH
  SCHEDULE  = 'USING CRON 0 2 * * * UTC'
  COMMENT   = 'Delete ORDERS_HISTORY rows older than 1 day'
AS
  DELETE FROM INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY
  WHERE TICK_AT < DATEADD('day', -1, CURRENT_TIMESTAMP());

ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP RESUME;
ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP RESUME;
```

### Seed initial orders

Seed `ORDERS` with 2,000 rows so the dashboard renders immediately before the simulation starts.

```sql
INSERT OVERWRITE INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS
SELECT
  UUID_STRING()                                                       AS ORDER_ID,
  DATEADD('second', -UNIFORM(0, 1800, RANDOM()), CURRENT_TIMESTAMP())
    ::TIMESTAMP_NTZ                                                   AS ORDER_TS,
  ARRAY_CONSTRUCT('North America','Europe','APAC','LATAM','MEA')
    [UNIFORM(0, 4, RANDOM())]::STRING                                 AS REGION,
  ARRAY_CONSTRUCT('Electronics','Apparel','Home','Beauty','Sports','Toys')
    [UNIFORM(0, 5, RANDOM())]::STRING                                 AS PRODUCT_CATEGORY,
  UNIFORM(1, 5, RANDOM())                                             AS QUANTITY,
  ROUND(UNIFORM(5, 400, RANDOM()) * UNIFORM(1, 5, RANDOM()), 2)      AS AMOUNT
FROM TABLE(GENERATOR(ROWCOUNT => 2000));
```

<!-- ------------------------ -->
## Build the Container

The simulation service is a lightweight Python container. It reads `SIMULATION_CONTROL` every few ticks and calls `SP_SIMULATE_TICK` in a tight loop, generating orders every second.

### How the service authenticates

When running inside SPCS, the container automatically receives an OAuth token from Snowflake at `/snowflake/session/token`. The service reads this token and uses it to connect — no password or key-pair is needed.

```python
# spcs/simulation_service.py (key excerpt)
TOKEN_FILE = "/snowflake/session/token"

def connect():
    with open(TOKEN_FILE) as f:
        token = f.read().strip()
    return snowflake.connector.connect(
        host=os.environ["SNOWFLAKE_HOST"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        authenticator="oauth",
        token=token,
        role="ACCOUNTADMIN",
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INTERACTIVE_ANALYTICS_WH"),
    )
```

### Get the registry URL

Run the following query and copy the `repository_url` value. It looks like:
`<orgname>-<accountname>.registry.snowflakecomputing.com/interactive_streaming_demo/public/sim_repo`

```sql
SHOW IMAGE REPOSITORIES LIKE 'SIM_REPO'
  IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC;
```

### Log in to the registry

```bash
docker login <your_registry_url>
```

When prompted, enter your Snowflake username and password (or a [programmatic access token](https://docs.snowflake.com/en/user-guide/programmatic-access/programmatic-access-tokens-intro)).

### Build, tag, and push the image

From the root of the cloned repository:

```bash
docker build -t simulation ./spcs/

docker tag simulation \
  <your_registry_url>/simulation:latest

docker push <your_registry_url>/simulation:latest
```

Positive
: Building on Apple Silicon (arm64)? Add `--platform linux/amd64` to `docker build` so the image runs correctly on Snowflake's x86_64 compute pool nodes.

<!-- ------------------------ -->
## Deploy Everything

### Create the SPCS simulation service

The `simulation_spec.yaml` file in the repository defines the container environment. Create the service from the inline specification below (the image path uses the path relative to your registry, which Snowflake resolves automatically):

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;

CREATE SERVICE IF NOT EXISTS
  INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC
  IN COMPUTE POOL INTERACTIVE_ANALYTICS_POOL
  FROM SPECIFICATION $$
  spec:
    container:
    - name: simulator
      image: /interactive_streaming_demo/public/sim_repo/simulation:latest
      env:
        SNOWFLAKE_WAREHOUSE: INTERACTIVE_ANALYTICS_WH
        SIM_INTERVAL: "1"
        IDLE_SUSPEND_SECS: "3600"
        ROWS_BASE: "100"
        CTRL_CACHE_TICKS: "5"
  $$
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1;
```

Wait until the status column shows `RUNNING` (typically 30–60 seconds):

```sql
SHOW SERVICES LIKE 'INTERACTIVE_ANALYTICS_SVC'
  IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC;
```

If the service takes longer or shows errors, inspect the container logs:

```sql
SELECT SYSTEM$GET_SERVICE_LOGS(
  'INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC',
  '0', 'simulator', 50
);
```

### Upload the Streamlit dashboard

Run these commands from the `streamlit/` directory using **SnowSQL** or the **Snowflake Python connector**:

```sql
PUT file://streamlit_app.py
    @INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit/
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://environment.yml
    @INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit/
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

CREATE OR REPLACE STREAMLIT INTERACTIVE_STREAMING_DEMO.PUBLIC.LIVE_ORDERS_DASHBOARD
  ROOT_LOCATION   = '@INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit'
  MAIN_FILE       = 'streamlit_app.py'
  QUERY_WAREHOUSE = INTERACTIVE_ANALYTICS_WH
  TITLE           = 'Live Orders Dashboard';
```

Open the app in Snowsight under **Projects > Streamlit > LIVE_ORDERS_DASHBOARD**.

<!-- ------------------------ -->
## Explore the Dashboard

### Start the simulation

In the sidebar, click **Start** in the **Simulation** section. If the SPCS service is suspended, the app resumes it automatically before enabling the data feed. The **Orders over time** chart begins updating every few seconds as new ticks arrive.

### KPI tiles

The top row shows four live metrics:

- **Total Orders** — cumulative order count since the last session reset.
- **Orders (last window)** — order count within the selected time window.
- **Revenue (last window)** — total revenue within the time window.
- **Avg order value** — average order amount within the time window.

### Adjust settings

The **Settings** sidebar section exposes three controls:

- **Time window** — how far back the charts look (5 min, 15 min, 30 min, 60 min, or All time).
- **Tick interval** — how often the dashboard re-queries Snowflake (1–30 seconds).
- **Rate multiplier** — scales the order volume per tick (0.5× to 3×). Changes take effect within the next tick cycle.

### Flash Sale

Select a product category from the **Flash Sale category** dropdown and click **Start**. Orders for the selected category immediately spike to 50 % of all ticks, and their unit prices are elevated to simulate promotional pricing. Click **Stop** to return to normal mode.

### Region x Category heatmap

The heatmap shows order volume for every region-category pair. Each region has a preferred product category with a graduated probability spectrum — the preferred category is roughly 3.5× more common than the least-preferred one, creating a visible diagonal:

| Region | Top category |
|---|---|
| North America | Electronics |
| Europe | Apparel |
| APAC | Toys |
| LATAM | Home |
| MEA | Beauty |

### New session

Click **New session** at the bottom of the **Simulation** sidebar section to clear all order history and reset the total counter to zero. Confirm the prompt to start a clean demo — useful when switching audiences or rerunning the demo from scratch.

<!-- ------------------------ -->
## Clean Up

Remove all resources when finished to stop incurring costs.

Negative
: Interactive warehouses have a **24-hour minimum billing period** once created. Drop the warehouse as soon as you are done to avoid unexpected charges.

```sql
USE ROLE ACCOUNTADMIN;

-- Stop the data feed before dropping objects.
UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL
  SET is_running = FALSE;

ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP SUSPEND;
ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP SUSPEND;

DROP SERVICE      IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC;
DROP STREAMLIT    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.LIVE_ORDERS_DASHBOARD;
DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP;
DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP;
DROP PROCEDURE    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(VARCHAR, VARCHAR, FLOAT, VARCHAR, FLOAT);
DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_IWH;
DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_WH;
DROP COMPUTE POOL IF EXISTS INTERACTIVE_ANALYTICS_POOL;
DROP DATABASE     IF EXISTS INTERACTIVE_STREAMING_DEMO;
```

<!-- ------------------------ -->
## Conclusion And Resources

Congratulations! You built a complete real-time BI dashboard on Snowflake without any external infrastructure. You created an interactive table clustered for fast time-window queries, attached it to an interactive warehouse for sub-second read latency, deployed a containerized simulation service on Snowpark Container Services, and wired everything together through a live Streamlit in Snowflake dashboard with full simulation controls.

### What You Learned
- Creating an interactive table clustered on `(DATE_TRUNC('MINUTE', ORDER_TS), REGION)` for fast "last N minutes by region" aggregations
- Attaching an interactive table to an interactive warehouse to keep the data cache warm and serve millisecond-latency queries
- Deploying a long-running Python service on SPCS with internal OAuth authentication — no keys or passwords in the container
- Using `EXECUTE AS OWNER` in a stored procedure to allow an SPCS service to write to an interactive table it does not own directly
- Building an auto-refreshing dashboard with `st.fragment(run_every=...)` that re-queries Snowflake on a configurable timer

### Related Resources

Documentation:
- [Interactive tables and interactive warehouses](https://docs.snowflake.com/en/user-guide/interactive)
- [Snowpark Container Services overview](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [CREATE INTERACTIVE TABLE](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-table)
- [CREATE INTERACTIVE WAREHOUSE](https://docs.snowflake.com/en/sql-reference/sql/create-interactive-warehouse)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [st.fragment — Streamlit docs](https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment)
- [Snowflake Programmatic Access Tokens](https://docs.snowflake.com/en/user-guide/programmatic-access/programmatic-access-tokens-intro)

Additional Reading:
- [Introducing Interactive Tables and Interactive Warehouses — Snowflake Engineering Blog](https://www.snowflake.com/en/engineering-blog/)
- [Snowpark Container Services — Build and Deploy guide](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/tutorials/tutorial-1)
