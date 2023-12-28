import yaml
import re
from prometheus_client.core import GaugeMetricFamily
from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)

class HBaseRegionServerMetricCollector(MetricCollector):
    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hbase", "regionserver")
        self.target = "-"
        self.urls = urls
        self.dns = set()
        self.hadoop_regionserver_metrics  = {}
        for i in range(len(self.file_list)):
            self.hadoop_regionserver_metrics.setdefault(self.file_list[i], {})
        self.common_metric_collector = CommonMetricCollector(cluster, "hbase", "regionserver")
        self.scrape_metrics = ScrapeMetrics(urls)

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
            self.hadoop_regionserver_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)
        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_regionserver_metrics:
                for metric in self.hadoop_regionserver_metrics[service]:
                    yield self.hadoop_regionserver_metrics[service][metric]

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            for service in self.metrics:
                if service in beans[i]['name']:
                    for metric in self.metrics[service]:
                        name = re.sub('[^a-z0-9A-Z]', '_', metric).lower()
                        if 'region_metric' in metric:
                            label = ['cluster', 'host', 'region']
                        elif 'table_metric' in metric:
                            label = ['cluster', 'host', 'table']
                        elif 'User_metric' in metric:
                            label = ['cluster', 'host', 'user']
                        else:
                            label = ['cluster', 'host']
                        self.hadoop_regionserver_metrics[service][metric] = GaugeMetricFamily("_".join([self.prefix, service.lower(), name]), self.metrics[service][metric], labels=label)


    def get_regions_metrics(self, beans, service):
        host = beans['tag.Hostname']
        for metric in beans:
           # if metric in self.metrics['Regions']:
                if "_region_" in metric and 'metric' in metric:
                    region = metric.split("_metric_")[0].split("region_")[1]
                    key = "".join(["region", metric.split(region)[-1]])
                    self.hadoop_regionserver_metrics[service][key].add_metric([self.cluster, host, region], beans[metric])
                elif metric in self.metrics[service]:
                    self.hadoop_regionserver_metrics[service][metric].add_metric([self.cluster, host], beans[metric])
                else:
                    continue
            #else:
             #   continue

    #
    # def get_regions_metrics(self, beans, service):
    #     host = beans['tag.Hostname']
    #     for metric in beans:
    #         if metric in self.metrics[service]:
    #             if "_region_" in metric and 'metric' in metric:
    #                 region = metric.split("_metric_")[0].split("region_")[1]
    #                 key = "".join(["region", metric.split(region)[-1]])
    #                 self.hadoop_regionserver_metrics[service][key].add_metric([self.cluster, host, region], beans[metric])
    #             elif metric in self.metrics[service]:
    #                 self.hadoop_regionserver_metrics[service][metric].add_metric([self.cluster, host], beans[metric])
    #             else:
    #                 continue
    #         else:
    #             continue

    # def get_tables_metrics(self, beans, service):
    #     host = beans['tag.Hostname']
    #     for metric in beans:
    #         if "percentile" in self.metrics[service]:
    #             if "_table_" in metric and 'metric' in metric:
    #                 table = metric.split("_metric_")[0].split("table_")[1]
    #                 key = "".join(["table", metric.split(table)[-1]])
    #                 self.hadoop_regionserver_metrics[service][key].add_metric([self.cluster, host, table], beans[metric])
    #             elif metric in self.metrics[service]:
    #                 self.hadoop_regionserver_metrics[service][metric].add_metric([self.cluster, host], beans[metric])
    #             else:
    #                 continue
    #         else:
    #             continue


    def get_users_metrics(self, beans, service):
        host = beans['tag.Hostname']
        for metric in beans:
            if metric in self.metrics[service]:
                if "User_" in metric and '_metric_' in metric:
                    user = metric.split("_metric_")[0].split("User_")[1]
                    key = "".join(["User", metric.split(user)[-1]])
                    self.hadoop_regionserver_metrics[service][key].add_metric([self.cluster, host, user], beans[metric])
                elif metric in self.metrics[service]:
                    self.hadoop_regionserver_metrics[service][metric].add_metric([self.cluster, host], beans[metric])
                else:
                    continue
            else:
                continue

    def get_other_metrics(self, beans, service):
        host = beans['tag.Hostname']
        for metric in beans:
            if metric in self.metrics[service]:
                self.hadoop_regionserver_metrics[service][metric].add_metric([self.cluster, host], beans[metric])
            else:
                continue

    def get_metrics(self, beans):
        for i in range(len(beans)):
            for service in self.metrics:
                if 'Regions' == service and 'sub={0}'.format(service) in beans[i]['name']:
                    self.get_regions_metrics(beans[i], service)
                # elif 'Tables' == service and 'sub={0}'.format(service) in beans[i]['name']:
                #     self.get_tables_metrics(beans[i], service)
                elif 'Users' == service and 'sub={0}'.format(service) in beans[i]['name']:
                    self.get_users_metrics(beans[i], service)
                elif 'sub={0}'.format(service) in beans[i]['name']:
                    self.get_other_metrics(beans[i], service)
            else:
                continue




