# Cost-Based Join Optimizer for GoDB

Final project for MIT 6.5830 Database Systems.

This project extends GoDB with a cost-based join optimizer that replaces the original rule-based planner with a dynamic-programming optimizer for left-deep join trees. The optimizer selects join orders and physical join algorithms using estimated table cardinalities, join predicates, buffer availability, and index metadata.

## Features

- Bottom-up dynamic programming join enumeration
- Left-deep join tree optimization
- Physical join selection across:
  - Index Nested Loop Join (INLJ)
  - Hash Join (HJ)
  - Sort-Merge Join (SMJ)
  - Block Nested Loop Join (BNLJ)
- Runtime cardinality estimation from loaded tables
- Integration into the GoDB shell through `cbo` and `joinopt`

## Repository

```bash
git clone https://github.com/mar258/6.5830_proj.git
cd 6.5830_proj
```

## Project Structure

Important files for the cost-based optimizer:

```text
main.go
planner/
├── cost_based_optimizer.go
├── cost_based_optimizer_explain.go
├── cost_based_physical_reorder.go
├── cost_based_explain.go
├── cost_based_optimizer_test.go
├── cost_based_optimizer_eval_test.go
├── join_optimizer_shell.go
├── physical_plan_builder.go
└── sql_planner.go
synthetic_data/
```

Key components:

- `cost_based_optimizer.go`: dynamic programming join enumeration and cost-based plan selection
- `cost_based_physical_reorder.go`: converts optimized CBO plans into executable GoDB physical plan nodes
- `cost_based_explain.go`: explain/debug output for optimizer decisions
- `cost_based_optimizer_test.go`: optimizer tests
- `cost_based_optimizer_eval_test.go`: evaluation test benchmarks
- `physical_plan_builder.go`: integration point for CBO join reordering during physical planning
- `sql_planner.go`: adds the `PlanWithCBO` planning path
- `main.go`: shell commands for running normal queries, `cbo` queries, and `joinopt` explanations

## Running GoDB

Start the GoDB shell:

```bash
go run main.go shell
```

Example using the MBTA catalog and database configuration:

```bash
go run main.go shell -catalog mbta/mbta-catalog.json -db godb_data -buffer 1000 -explain
```

Flags:

- `-catalog`: path to catalog JSON file
- `-db`: database storage directory
- `-buffer`: buffer pool size
- `-explain`: print physical execution plans

## Using the Cost-Based Optimizer

Normal SQL queries use the original rule-based planner:

```sql
SELECT COUNT(*)
FROM stations, lines
WHERE stations.line_id = lines.line_id;
```

To execute a query using the cost-based optimizer, prefix the query with `cbo`:

```sql
cbo SELECT COUNT(*)
FROM stations, lines
WHERE stations.line_id = lines.line_id;
```

To print the optimizer’s chosen join order and estimated costs without executing the query, use `joinopt`:

```sql
joinopt SELECT COUNT(*)
FROM stations, lines
WHERE stations.line_id = lines.line_id;
```

## MBTA Dataset Setup

Cleaned MBTA CSV files can be found in the drive. Place the cleaned CSV files along with the MBTA catalog inside an `mbta/` directory:

```text
mbta/
├── mbta-catalog.json
├── routes.csv
├── lines.csv
├── time_periods.csv
├── stations.csv
├── station_orders.csv
├── rail_ridership.csv
└── gated_station_entries.csv
```

The CSV files should not include header rows.

Load the dataset with:

```bash
go run main.go load -catalog mbta/mbta-catalog.json mbta/*.csv
```

This creates the generated GoDB data files in the configured database directory

## Running evaluations 

```bash
go test ./planner -run '^$' -bench BenchmarkEvalJoinTwoTableOuter1kInner10k -benchmem -v
go test ./planner -run '^$' -bench 'BenchmarkEvalJoinChain150kTablesIOCost/tables_07/(Rule|CBO)' -benchmem
go test ./planner -run '^$' -bench BenchmarkEvalJoinChainSkewedSizesIOCost -benchmem -v
go test ./planner -run '^$' -bench BenchmarkEvalJoinCBOMixesJoinAlgorithms -benchmem -v
```

## Running the Tests

You can run the provided tests by running the following commands

```text
go test ./planner -run 'TestCBO' -v
go test ./planner -run 'TestCBOReorder' -v
```
