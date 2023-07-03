#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
import yaml
import re
import time


# from prometheus_client import start_http_server
# from prometheus_client.core import REGISTRY
# import utils

from prometheus_client.core import GaugeMetricFamily

from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics


logger = get_module_logger(__name__)



class HiveServerMetricCollector(MetricCollector):
    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hive", "hiveserver2")
        self.target = "-"
        self.urls = urls
        self.dns = set()

        #print("cluster, urls, dns: ", cluster, self.urls, self.dns)

        self.hadoop_hiveserver2_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_hiveserver2_metrics.setdefault(self.file_list[i], {})
        #print("hadoop_hiveserver2_metrics: ", self.hadoop_hiveserver2_metrics)

        self.common_metric_collector = CommonMetricCollector(cluster, "hive", "hiveserver2")
        #print("common_metric_collector ", self.common_metric_collector.cluster,self.common_metric_collector.componet,self.common_metric_collector.service,self.common_metric_collector.prefix,self.common_metric_collector.common_metrics,self.common_metric_collector.tmp_metrics)

        self.scrape_metrics = ScrapeMetrics(urls)
        #print("scrape_metrics ", self.scrape_metrics.urls)

    def collect(self):
        isSetup = False
        beans_list = self.scrape_metrics.scrape()
        for beans in beans_list:
            if not isSetup:
                self.common_metric_collector.setup_labels(beans)
                self.setup_metrics_labels(beans)
                isSetup = True

            for i in range(len(beans)):
                if 'tag.Hostname' in beans[i]:
                    self.target = beans[i]["tag.Hostname"]
                    break
            self.hadoop_hiveserver2_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))

            self.get_metrics(beans)

        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_hiveserver2_metrics:
                for metric in self.hadoop_hiveserver2_metrics[service]:
                    yield self.hadoop_hiveserver2_metrics[service][metric]

    def setup_hiveserver2_labels(self, beans):
        for metric in self.metrics['hiveserver2']:
            label = ["cluster", "state", "_target"]
            #snake_case = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            #name = "_".join([self.prefix, snake_case])#
            name = "_".join([self.prefix, "hs2_jvm_method_last_gc"])
            self.hadoop_hiveserver2_metrics['hiveserver2'][metric] = GaugeMetricFamily(name, self.metrics['hiveserver2'][metric], labels=label)



    def setup_metrics_labels(self, beans):
        # print(print(self.metrics))

        #for service in self.metrics['hiveserver2']:
        for service in self.metrics:
            for i in range(len(beans)):
                if 'producer-node-metrics' == service and 'type=producer-node-metrics' in beans[i]['name']:
                    self.setup_node_labels(beans[i], service)
                elif 'producer-topic-metrics' == service and 'type=producer-topic-metrics' in beans[i]['name']:
                    self.setup_topic_labels(beans[i], service)
                elif 'producer-metrics' == service and 'type=producer-metrics' in beans[i]['name'] or 'kafka-metrics-count' == service and 'type=kafka-metrics-count' in beans[i]['name']:
                    self.setup_producer_labels(beans[i], service)
                elif service in beans[i]['name']:
                    self.setup_other_labels(beans[i], service)
                elif 'hiveserver2' == service and 'GarbageCollector' in beans[i]['name']:
                    self.setup_hiveserver2_labels(beans[i])
                else:
                    continue

    def get_hiveserver2_labels(self, bean, service):

        for metric in self.metrics['hiveserver2']:
            if "LastGcInfo" in bean:
                for each in bean["LastGcInfo"]:
                    if "duration" in each:
                        method = "duration"
                        key = metric
                        label = [self.cluster, method, "LastGcInfo", self.target]
                        #self.hadoop_hiveserver2_metrics[service][metric].add_metric([self.cluster], bean[metric])
                        self.hadoop_hiveserver2_metrics['hiveserver2'][key].add_metric(label, bean[metric][
                            "duration"] if metric in bean else 0)
                    if "memoryUsageAfterGc" in each:
                        for each_part in bean[metric]["memoryUsageAfterGc"]:
                            method = each_part["key"]
                            key = metric
                            label = [self.cluster, method, "memoryUsageAfterGc", self.target]
                            self.hadoop_hiveserver2_metrics['hiveserver2'][key].add_metric(label, each_part['value'][
                                'used'] if metric in bean else 0)
                    if "memoryUsageBeforeGc" in each:
                        for each_part in bean[metric]["memoryUsageBeforeGc"]:
                            method = each_part["key"]
                            key = metric
                            label = [self.cluster, method, "memoryUsageBeforeGc", self.target]
                            self.hadoop_hiveserver2_metrics['hiveserver2'][key].add_metric(label, each_part['value'][
                                'used'] if metric in bean else 0)


    def get_metrics(self, beans):
        # bean is a type of <Dict>
        # status is a type of <Str>
        for i in range(len(beans)):
            if 'tag.Hostname' in beans[i]:
                host = beans[i]['tag.Hostname']
                break
            else:
                continue
        for i in range(len(beans)):
            for service in self.metrics:
                if 'producer-node-metrics' == service and 'type=producer-node-metrics' in beans[i]['name']:
                    self.get_node_metrics(beans[i], service, host)
                elif 'producer-topic-metrics' == service and 'type=producer-topic-metrics' in beans[i]['name']:
                    self.get_topic_metrics(beans[i], service, host)
                elif 'producer-metrics' == service and 'type=producer-metrics' in beans[i]['name'] or 'kafka-metrics-count' == service and 'type=kafka-metrics-count' in beans[i]['name']:
                    self.get_producer_metrics(beans[i], service, host)
                elif 'hiveserver2' == service:
                    self.get_hiveserver2_labels(beans[i], service)
                elif service in beans[i]['name']:
                    self.get_other_metrics(beans[i], service, host)
                else:
                    continue