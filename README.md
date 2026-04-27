# pipewatch

A lightweight CLI tool for monitoring and alerting on data pipeline health metrics across multiple sources.

---

## Installation

```bash
pip install pipewatch
```

Or install from source:

```bash
git clone https://github.com/yourname/pipewatch.git && cd pipewatch && pip install .
```

---

## Usage

Define your pipeline sources in a config file and run:

```bash
pipewatch monitor --config pipelines.yaml
```

Example `pipelines.yaml`:

```yaml
pipelines:
  - name: daily-etl
    source: postgres
    check_interval: 60
    alert_on:
      - row_count_drop: 20%
      - last_run_delay: 15m
  - name: event-stream
    source: kafka
    check_interval: 30
    alert_on:
      - lag_threshold: 5000
```

Run a one-time health check:

```bash
pipewatch check --pipeline daily-etl
```

Send alerts to Slack or email by adding a `notifications` block to your config. See the [docs](https://github.com/yourname/pipewatch/wiki) for the full configuration reference.

---

## License

This project is licensed under the [MIT License](LICENSE).