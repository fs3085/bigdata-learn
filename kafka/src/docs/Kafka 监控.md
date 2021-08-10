基于 Prometheus 和 Grafana 的 Kafka 集群监控方案

Kafka 监控现状

0.9.0 时，broker 端主要使用 kafka-manager 作为监控工具，能看到实时的 metrics，并做了一些聚合。但数据不落地，无法进行历史数据回放。同时缺少 size 指标，无法查看 topic、partition、broker 的数据量情况。其它的工具如 Kafka Offset Monitor 和 trifecta，在数据量大时查询缓慢，基本不可用。



可用的开源组件

Prometheus：Prometheus 是一个云原生的 metrics 时间序列数据库，具备 metrics 采集、查询、告警能力。

Grafana： metrics 可视化系统。可对接多种 metrics 数据源。

JMX Exporter：把 JMX metrics 导出为 Promethues 格式。https://github.com/prometheus/jmx_exporter

Kafka Exporter：汇聚常见的 Kafka metrics，导出为 Prometheus 格式。https://github.com/danielqsj/kafka_exporter

监控方案

JMX Exporter：部署到每台 broker 上。

Kafka Exporter：部署到任意 1 台 broker 上。

Prometheus：部署到 1 台独立机器上，采集 JMX Exporter 和 Kafka Exporter 上的数据。

Grafana：部署到 1 台独立机器上，可视化 Prometheus Kafka metrics 数据。对于 cluster、broker、topic、consumer 4 个角色，分别制作了 dashboard。





JMX

https://help.aliyun.com/document_detail/141108.html?spm=a2c4g.11186623.6.621.12bb4dea7EyM9F