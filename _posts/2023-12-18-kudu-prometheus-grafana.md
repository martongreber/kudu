---
layout: post
title: Integrating Kudu Prometheus Metrics with Grafana, A Step-by-Step Guide
author: Marton Greber
---

## Introduction
The addition of Prometheus metrics support in Apache Kudu
([KUDU-3375](https://issues.apache.org/jira/browse/KUDU-3375)) enables users to integrate Kudu
metrics into Grafana for enhanced monitoring. This guide covers the basics, from installing
Prometheus and Grafana to configuring them. It provides a practical approach to crafting Grafana
dashboards for a better understanding of Apache Kudu.

<!--more-->

## Integration Benefits
* With this improvement of directly exposing metrics in a prometheus format, there is no need to add
custom jars like https://github.com/prometheus/jmx_exporter to convert metrics from JMX format to
Prometheus format.
* Prometheus and Grafana are ideal tools for long term tracking of Kudu metrics and have the ability
to scale up to Kudu clusters of 100+ nodes and track Kudu Master and Tablet server health.
* For production grade deployments we recommend using Thanos for Prometheus high availability and a
highly available storage setup.

## Prerequisites
Ensure that you have the following installed:
1. Prometheus: Follow the
[official Prometheus installation guide](https://prometheus.io/docs/prometheus/latest/getting_started)
to set up Prometheus on your system.
2. Grafana: Install Grafana by following the steps outlined in the
[official Grafana documentation](https://grafana.com/docs/grafana/latest/setup-grafana/installation).

## Prometheus Configuration for Kudu
1. Open the Prometheus configuration file, typically located at /etc/prometheus/prometheus.yml. If
using a custom location, specify it accordingly.
2. Add the following snippet to the file to configure Prometheus to scrape Kudu metrics:
    ```yaml
    # Define scrape configurations for various jobs
    scrape_configs:

    # Configuration for the 'kudu' job
    - job_name: 'kudu'

        # Set the interval at which Prometheus will scrape metrics for this job (5 seconds in this case)
        scrape_interval: 5s

        # Specify the metrics endpoint path for the 'kudu' job
        metrics_path: '/metrics_prometheus'

        # Define static targets for this job
        static_configs:

        # Targets for the 'masters' group
        # These are the web server addresses
        - targets: ['localhost:8765', 'localhost:8767', 'localhost:8769']

            # Assign labels to the targets for better identification and grouping
            labels:
            group: 'masters'

        # Targets for the 'tservers' group
        # These are the web server addresses
        - targets: ['localhost:9871', 'localhost:9873', 'localhost:9875']

            # Assign labels to the targets for better identification and grouping
            labels:
            group: 'tservers'
    ```

3. Adjust the targets to match the Kudu instances in your environment. For more configuration
options, refer to the
[Prometheus documentation](https://prometheus.io/docs/prometheus/latest/configuration/configuration).
4. Start Prometheus, if using a custom config location, use the config.file command line flag, for
example:
    ```
    $ prometheus --config.file="<custom_path>/prometheus.yml"
    ```
5. Verify that Prometheus is running on localhost:9090 and check the /targets endpoint to ensure all
Kudu services are up.
    <div align="center">
        <img src="{{ site.github.url }}/img/2023-12-18-kudu-prometheus-grafana/prometheus-targets.png"
        alt="Kudu Masters and TServers in prometheus."
        class="img-responsive">
        <em>Figure 1. Kudu Masters and TServers in prometheus.</em>
    </div>

## Grafana Setup
1. Start Grafana, which runs by default on port 3000.
2. Configure Prometheus as a data source in Grafana:
    * Follow the steps outlined in the Grafana documentation to add Prometheus as a data source.
    * Set the Prometheus URL to the Prometheus server (e.g., http://localhost:9090).
    * Save and test the connection.
    <div align="center">
        <img src="{{ site.github.url }}/img/2023-12-18-kudu-prometheus-grafana/metrics-explorer.png"
        alt="Kudu metrics in Grafana metrics explorer."
        class="img-responsive">
        <em>Figure 2: Kudu metrics in Grafana metrics explorer.</em>
    </div>

3. For a comprehensive list of available metrics in Kudu, please refer to the [official metrics
reference](https://kudu.apache.org/docs/metrics_reference.html). Kudu Prometheus metrics all have
the "kudu_" prefix. Please find more info in the original
[commit message](https://github.com/apache/kudu/commit/00efc6826ac9a1f5d10750296c7357790a041fec)
about naming and details.
4. Create a Kudu Dashboard:
    * Navigate to the "+" menu on the left sidebar and select "Dashboard".
    * Add a new panel, select Prometheus as the data source, and use PromQL
    queries to create visualizations based on Kudu metrics.
    <div align="center">
        <img src="{{ site.github.url }}/img/2023-12-18-kudu-prometheus-grafana/master-tserver-memory.png"
        alt="Kudu Master and TServer memory dashboard." class="img-responsive">
        <em>Figure 3: Kudu Master and TServer memory dashboard.</em>
    </div>

## Working with a large number of TServers
For users with a large number of TServers the above mentioned config is really tedious to edit. Not
to mention that in case of TServer addition or removal, the config has to be edited. Prometheus
offers file-based service discovery. In case you are interested, please check the official guide for
more details. In essence the main Prometheus config yml file can be split up to contain different
target configs in json format. The above mentioned configuration can be split up in the following
way:
1. config.yml:
    ```yaml
    scrape_configs:
        - job_name: 'kudu'

            scrape_interval: 5s
            metrics_path: '/metrics_prometheus'

            file_sd_configs:
            - files:
                - 'master_targets.json'
                - 'tserver_targets.json'
    ```

2. masters_target.json:
    ```json
    [
        {
            "labels": {
                "job": "kudu",
                "group": "masters"
            },
            "targets": [
                "localhost:8765",
                "localhost:8767",
                "localhost:8769"
            ]
        }
    ]
    ```

3. tservers_target.json:
    ```json
    [
        {
            "labels": {
                "job": "kudu",
                "group": "tservers"
            },
            "targets": [
                "localhost:9871",
                "localhost:9873",
                "localhost:9875"
            ]
        }
    ]
    ```

While Prometheus is running and file-based service discovery is configured, it will automatically
listen for changes on the configured files. Using the above mentioned modular config approach one
can create a script which parses the TServer addresses from ```kudu tserver list``` for example, and
those addresses can be plugged into the "targets" array of the TServer_target.json file. Obviously
this script needs to be executed, each time TServer hosts are changed. A possible implementation of
such script, and example sample config is available in
[this repository](https://github.com/martongreber/kudu-prometheus).
