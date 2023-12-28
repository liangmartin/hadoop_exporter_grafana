import yaml
import re
from prometheus_client.core import GaugeMetricFamily,HistogramMetricFamily
from utils import get_module_logger
from common import MetricCollector, CommonMetricCollector
from scraper import ScrapeMetrics

logger = get_module_logger(__name__)

class HBaseMasterMetricCollector(MetricCollector):
    def __init__(self, cluster, urls):
        MetricCollector.__init__(self, cluster, "hbase", "master")
        self.target = "-"
        self.urls = urls
        self.dns = set()
        self.hadoop_hbase_metrics = {}
        for i in range(len(self.file_list)):
            self.hadoop_hbase_metrics.setdefault(self.file_list[i], {})
        self.common_metric_collector = CommonMetricCollector(cluster, "hbase", "master")
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
            self.hadoop_hbase_metrics.update(self.common_metric_collector.get_metrics(beans, self.target))
            self.get_metrics(beans)
        for i in range(len(self.merge_list)):
            service = self.merge_list[i]
            if service in self.hadoop_hbase_metrics:
                for metric in self.hadoop_hbase_metrics[service]:
                    yield self.hadoop_hbase_metrics[service][metric]

    def setup_metrics_labels(self, beans):
        for i in range(len(beans)):
            if 'Server' in beans[i]['name']:
                self.setup_hbase_server_labels()
            elif 'Balancer' in beans[i]['name']:
                self.setup_hbase_balancer_labels()
            elif 'AssignmentManger' in beans[i]['name']:
                self.setup_hbase_assignmentmanger_labels()
            elif 'IPC' in beans[i]['name']:
                self.setup_hbase_ipc_labels()
            elif 'FileSystem' in beans[i]['name']:
                self.setup_hbase_filesystem_labels()
            else:
                continue


    def setup_hbase_server_labels(self):
        for metric in self.metrics['Server']:
            label = ["cluster", "host"]
            name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if 'RegionServersState' in metric:
                label.append('server')
            elif 'numRegionServers' in metric:
                name = 'live_region'
            elif 'numDeadRegionServers' in metric:
                name = 'dead_region'
            else:
                pass
            self.hadoop_hbase_metrics['Server'][metric] = GaugeMetricFamily("_".join([self.prefix, 'server', name]), self.metrics['Server'][metric], labels=label)

    def setup_hbase_balancer_labels(self):
        balancer_flag = 1
        for metric in self.metrics['Balancer']:
            label = ["cluster", "host"]
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self.hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily("_".join([self.prefix, name]),self.metrics['Balancer'][metric],labels=label)
            elif 'BalancerCluster' in metric:
                if balancer_flag:
                    balancer_flag = 0
                    name = 'balancer_cluster_latency_microseconds'
                    key = 'BalancerCluster'
                    self.hadoop_hbase_metrics['Balancer'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of balancer cluster latency in microseconds",labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join(['balancer', snake_case])
                self.hadoop_hbase_metrics['Balancer'][metric] = GaugeMetricFamily("_".join([self.prefix, name]),self.metrics['Balancer'][metric],labels=label)

    def setup_hbase_assignmentmanger_labels(self):
        bulkassign_flag, assign_flag = 1, 1
        for metric in self.metrics['AssignmentManger']:
            label = ["cluster", "host"]
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or '_num_ops' in metric:
                name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                self.hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily("_".join([self.prefix, name]), self.metrics['AssignmentManger'][metric], labels=label)
            elif 'BulkAssign' in metric:
                if bulkassign_flag:
                    bulkassign_flag = 0
                    name = 'bulkassign_latency_microseconds'
                    key = 'BulkAssign'
                    self.hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of bulkassign latency in microseconds",labels=label)
                else:
                    continue
            elif 'Assign' in metric:
                if assign_flag:
                    assign_flag = 0
                    name = 'assign_latency_microseconds'
                    key = 'Assign'
                    self.hadoop_hbase_metrics['AssignmentManger'][key] = HistogramMetricFamily(
                        "_".join([self.prefix, name]),
                        "The percentile of assign latency in microseconds",
                        labels=label)
                else:
                    continue
            else:
                snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
                name = "_".join(['assignmentmanger', snake_case])
                self.hadoop_hbase_metrics['AssignmentManger'][metric] = GaugeMetricFamily("_".join([self.prefix, name]), self.metrics['AssignmentManger'][metric], labels=label)


    def setup_hbase_ipc_labels(self):
        total_calltime_flag, response_size_flag, process_calltime_flag, queue_calltime_flag, request_size_flag, exception_flag = 1, 1, 1, 1, 1, 1
        for metric in self.metrics['IPC']:
            label = ["cluster", "host"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = "_".join(['ipc', snake_case])
                self.hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self.prefix, name]),self.metrics['IPC'][metric], labels=label)
            elif 'RangeCount_' in metric:
                name = metric.replace("-", "_").lower()
                self.hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self.prefix, 'ipc', name]),self.metrics['IPC'][metric], labels=label)
            elif 'TotalCallTime' in metric:
                if total_calltime_flag:
                    total_calltime_flag = 0
                    name = 'ipc_total_calltime_latency_microseconds'
                    key = 'TotalCallTime'
                    self.hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self.prefix, name]), "The percentile of total calltime latency in microseconds", labels=label)
                else:
                    continue
            elif 'ResponseSize' in metric:
                if response_size_flag:
                    response_size_flag = 0
                    name = 'ipc_response_size_bytes'
                    key = 'ResponseSize'
                    self.hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self.prefix, name]), "The percentile of response size in bytes", labels=label)
                else:
                    continue
            elif 'ProcessCallTime' in metric:
                if process_calltime_flag:
                    process_calltime_flag = 0
                    name = 'ipc_prcess_calltime_latency_microseconds'
                    key = 'ProcessCallTime'
                    self.hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self.prefix, name]), "The percentile of process calltime latency in microseconds", labels=label)
                else:
                    continue
            elif 'RequestSize' in metric:
                if request_size_flag:
                    request_size_flag = 0
                    name = 'ipc_request_size_bytes'
                    key = 'RequestSize'
                    self.hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of request size in bytes", labels=label)

                else:
                    continue
            elif 'QueueCallTime' in metric:
                if queue_calltime_flag:
                    queue_calltime_flag = 0
                    name = 'ipc_queue_calltime_latency_microseconds'
                    key = 'QueueCallTime'
                    self.hadoop_hbase_metrics['IPC'][key] = HistogramMetricFamily("_".join([self.prefix, name]), "The percentile of queue calltime latency in microseconds", labels=label)
            elif 'exceptions' in metric:
                if exception_flag:
                    exception_flag = 0
                    name = 'ipc_exceptions_total'
                    key = 'exceptions'
                    label.append("type")
                    self.hadoop_hbase_metrics['IPC'][key] = GaugeMetricFamily("_".join([self.prefix, name]), "Exceptions caused by requests", labels=label)
                else:
                    continue
            else:
                name = "_".join(['ipc', snake_case])
                self.hadoop_hbase_metrics['IPC'][metric] = GaugeMetricFamily("_".join([self.prefix, name]), self.metrics['IPC'][metric], labels=label)


    def setup_hbase_filesystem_labels(self):
        hlog_split_time_flag, metahlog_split_time_flag, hlog_split_size_flag, metahlog_split_size_flag = 1, 1, 1, 1
        for metric in self.metrics['FileSystem']:
            label = ["cluster", "host"]
            snake_case = re.sub('([a-z0-9])([A-Z])', r'\1_\2', metric).lower()
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                name = snake_case
                self.hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily("_".join([self.prefix, name]),self.metrics['FileSystem'][metric],labels=label)
            elif 'MetaHlogSplitTime' in metric:
                if metahlog_split_time_flag:
                    metahlog_split_time_flag = 0
                    name = 'metahlog_split_time_latency_microseconds'
                    key = 'MetaHlogSplitTime'
                    self.hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of time latency it takes to finish splitMetaLog()",labels=label)
                else:
                    continue
            elif 'HlogSplitTime' in metric:
                if hlog_split_time_flag:
                    hlog_split_time_flag = 0
                    name = 'hlog_split_time_latency_microseconds'
                    key = 'HlogSplitTime'
                    self.hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of time latency it takes to finish WAL.splitLog()",labels=label)
                else:
                    continue
            elif 'MetaHlogSplitSize' in metric:
                if metahlog_split_size_flag:
                    metahlog_split_size_flag = 0
                    name = 'metahlog_split_size_bytes'
                    key = 'MetaHlogSplitSize'
                    self.hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of hbase:meta WAL files size being split",labels=label)
                else:
                    continue
            elif 'HlogSplitSize' in metric:
                if hlog_split_size_flag:
                    hlog_split_size_flag = 0
                    name = 'hlog_split_size_bytes'
                    key = 'HlogSplitSize'
                    self.hadoop_hbase_metrics['FileSystem'][key] = HistogramMetricFamily("_".join([self.prefix, name]),"The percentile of WAL files size being split",labels=label)
                else:
                    continue
            else:
                name = snake_case
                self.hadoop_hbase_metrics['FileSystem'][metric] = GaugeMetricFamily("_".join([self.prefix, name]),self._metrics['FileSystem'][metric],labels=label)


    def get_hbase_server_labels(self, beans):
        host = beans['tag.Hostname']
        label = [self.cluster, host]
        for metric in self.metrics['Server']:
            if 'RegionServersState' in metric:
                if 'tag.liveRegionServers' in beans and beans['tag.liveRegionServers']:
                    live_region_servers = yaml.safe_load(beans['tag.liveRegionServers'])
                    live_region_list = live_region_servers.split(';')
                    for j in range(len(live_region_list)):
                        server = live_region_list[j].split(',')[0]
                        label = [self.cluster, host, server]
                        self.hadoop_hbase_metrics['Server'][metric].add_metric(label, 1.0)
                elif 'tag.deadRegionServers' in beans and beans['tag.deadRegionServers']:
                    dead_region_servers = yaml.safe_load(beans['tag.deadRegionServers'])
                    dead_region_list = dead_region_servers.split(';')
                    for j in range(len(dead_region_list)):
                        server = dead_region_list[j].split(',')[0]
                        label = [self.cluster, host, server]
                        self.hadoop_hbase_metrics['Server'][metric].add_metric(label + [server], 0.0)
            elif 'ActiveMaster' in metric:
                if 'tag.isActiveMaster' in beans:
                    state = beans['tag.isActiveMaster']
                    value = float(bool(state))
                    self.hadoop_hbase_metrics['Server'][metric].add_metric(label, value)
            else:
                self.hadoop_hbase_metrics['Server'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)


    def get_hbase_balbancer_labels(self,beans):
        host = beans['tag.Hostname']
        label = [self.cluster, host]
        balancer_sum = 0.0
        balancer_value, balancer_percentile = [], []

        for metric in self.metrics['Balancer']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                self.hadoop_hbase_metrics['Balancer'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)
            elif 'BalancerCluster' in metric:
                if '_num_ops' in metric:
                    balancer_count = beans[metric]
                    key = 'BalancerCluster'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    balancer_percentile.append(str(float(per) / 100.0))
                    balancer_value.append(beans[metric])
                    balancer_sum += int(beans[metric])
            else:
                self.hadoop_hbase_metrics['Balancer'][metric].add_metric(label,beans[metric] if metric in beans and beans[metric] else 0)

        balancer_bucket = sorted(zip(balancer_percentile, balancer_value))
        balancer_bucket.append(("+Inf", balancer_count))
        self.hadoop_hbase_metrics['Balancer'][key].add_metric(label, buckets=balancer_bucket, sum_value=balancer_sum)

    def get_assignmentmanger_metrics(self, beans):
        host = beans['tag.Hostname']
        label = [self.cluster, host]

        bulkassign_sum, assign_sum = 0.0, 0.0
        bulkassign_value, bulkassign_percentile, assign_value, assign_percentile = [], [], [], []

        for metric in self.metrics['AssignmentManger']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric:
                self.hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)
            elif 'BulkAssign' in metric:
                if '_num_ops' in metric:
                    bulkassign_count = beans[metric]
                    bulkassign_key = 'BulkAssign'
                    self.hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)
                else:
                    per = metric.split("_")[1].split("th")[0]
                    bulkassign_percentile.append(str(float(per) / 100.0))
                    bulkassign_value.append(beans[metric])
                    bulkassign_sum += int(beans[metric])
            elif 'Assign' in metric:
                if '_num_ops' in metric:
                    assign_count = beans[metric]
                    assign_key = 'Assign'
                    self.hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)
                else:
                    per = metric.split("_")[1].split("th")[0]
                    assign_percentile.append(str(float(per) / 100.0))
                    assign_value.append(beans[metric])
                    assign_sum += int(beans[metric])
            else:
                self.hadoop_hbase_metrics['AssignmentManger'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)
        bulkassign_bucket = sorted(zip(bulkassign_percentile, bulkassign_value))
        bulkassign_bucket.append(("+Inf", bulkassign_count))
        assign_bucket = sorted(zip(assign_percentile, assign_value))
        assign_bucket.append(("+Inf", assign_count))
        self.hadoop_hbase_metrics['AssignmentManger'][bulkassign_key].add_metric(label, buckets = bulkassign_bucket, sum_value = bulkassign_sum)
        self.hadoop_hbase_metrics['AssignmentManger'][assign_key].add_metric(label, buckets = assign_bucket, sum_value = assign_sum)



    def get_ipc_metrics(self, beans):
        host = beans['tag.Hostname']
        label = [self.cluster, host]

        total_calltime_sum, process_calltime_sum, queue_calltime_sum, response_sum, request_sum = 0.0, 0.0, 0.0, 0.0, 0.0
        total_calltime_value, process_calltime_value, queue_calltime_value, response_value, request_value = [], [], [], [], []
        total_calltime_percentile, process_calltime_percentile, queue_calltime_percentile, response_percentile, request_percentile = [], [], [], [], []

        for metric in self.metrics['IPC']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                self.hadoop_hbase_metrics['IPC'][metric].add_metric(label, beans[metric] if metric in beans and beans[
                    metric] else 0)
            elif 'TotalCallTime' in metric:
                if '_num_ops' in metric:
                    total_calltime_count = beans[metric]
                    total_calltime_key = 'TotalCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    total_calltime_percentile.append(str(float(per) / 100.0))
                    total_calltime_value.append(beans[metric])
                    total_calltime_sum += int(beans[metric])
            elif 'ProcessCallTime' in metric:
                if '_num_ops' in metric:
                    process_calltime_count = beans[metric]
                    process_calltime_key = 'ProcessCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    process_calltime_percentile.append(str(float(per) / 100.0))
                    process_calltime_value.append(beans[metric])
                    process_calltime_sum += int(beans[metric])
            elif 'QueueCallTime' in metric:
                if '_num_ops' in metric:
                    queue_calltime_count = beans[metric]
                    queue_calltime_key = 'QueueCallTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    queue_calltime_percentile.append(str(float(per) / 100.0))
                    queue_calltime_value.append(beans[metric])
                    queue_calltime_sum += int(beans[metric])
            elif 'ResponseSize' in metric:
                if '_num_ops' in metric:
                    response_count = beans[metric]
                    response_key = 'ResponseSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    response_percentile.append(str(float(per) / 100.0))
                    response_value.append(beans[metric])
                    response_sum += int(beans[metric])
            elif 'RequestSize' in metric:
                if '_num_ops' in metric:
                    request_count = beans[metric]
                    request_key = 'RequestSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    request_percentile.append(str(float(per) / 100.0))
                    request_value.append(beans[metric])
                    request_sum += int(beans[metric])
            elif 'exceptions' in metric:
                key = 'exceptions'
                new_label = label
                if 'exceptions' == metric:
                    type = "sum"
                else:
                    type = metric.split(".")[1]
                self.hadoop_hbase_metrics['IPC'][key].add_metric(new_label + [type],beans[metric] if metric in beans and beans[metric] else 0)
            else:
                self.hadoop_hbase_metrics['IPC'][metric].add_metric(label, beans[metric] if metric in beans and beans[metric] else 0)

        total_calltime_bucket = sorted(zip(total_calltime_percentile, total_calltime_value))
        total_calltime_bucket.append(("+Inf", total_calltime_count))
        process_calltime_bucket = sorted(zip(process_calltime_percentile, process_calltime_value))
        process_calltime_bucket.append(("+Inf", process_calltime_count))
        queue_calltime_bucket = sorted(zip(queue_calltime_percentile, queue_calltime_value))
        queue_calltime_bucket.append(("+Inf", queue_calltime_count))
        response_bucket = sorted(zip(response_percentile, response_value))
        response_bucket.append(("+Inf", response_count))
        request_bucket = sorted(zip(request_percentile, request_value))
        request_bucket.append(("+Inf", request_count))

        self.hadoop_hbase_metrics['IPC'][total_calltime_key].add_metric(label, buckets=total_calltime_bucket, sum_value=total_calltime_sum)
        self.hadoop_hbase_metrics['IPC'][process_calltime_key].add_metric(label, buckets=process_calltime_bucket, sum_value=process_calltime_sum)
        self.hadoop_hbase_metrics['IPC'][queue_calltime_key].add_metric(label, buckets=queue_calltime_bucket, sum_value=queue_calltime_sum)
        self.hadoop_hbase_metrics['IPC'][response_key].add_metric(label, buckets=response_bucket, sum_value=response_sum)
        self.hadoop_hbase_metrics['IPC'][request_key].add_metric(label, buckets=request_bucket, sum_value=request_sum)

    def get_filesystem_metrics(self, beans):
        host = beans['tag.Hostname']
        label = [self.cluster, host]

        hlog_split_time_sum, metahlog_split_time_sum, hlog_split_size_sum, metahlog_split_size_sum = 0.0, 0.0, 0.0, 0.0
        hlog_split_time_value, metahlog_split_time_value, hlog_split_size_value, metahlog_split_size_value = [], [], [], []
        hlog_split_time_percentile, metahlog_split_time_percentile, hlog_split_size_percentile, metahlog_split_size_percentile = [], [], [], []

        for metric in self.metrics['FileSystem']:
            if '_min' in metric or '_max' in metric or '_mean' in metric or 'median' in metric or 'RangeCount_' in metric:
                self.hadoop_hbase_metrics['FileSystem'][metric].add_metric(label,beans[metric] if metric in beans and beans[metric] else 0)
            elif 'MetaHlogSplitTime' in metric:
                if '_num_ops' in metric:
                    metahlog_split_time_count = beans[metric]
                    metahlog_split_time_key = 'MetaHlogSplitTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    metahlog_split_time_percentile.append(str(float(per) / 100.0))
                    metahlog_split_time_value.append(beans[metric])
                    metahlog_split_time_sum += int(beans[metric])
            elif 'HlogSplitTime' in metric:
                if '_num_ops' in metric:
                    hlog_split_time_count = beans[metric]
                    hlog_split_time_key = 'HlogSplitTime'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    hlog_split_time_percentile.append(str(float(per) / 100.0))
                    hlog_split_time_value.append(beans[metric])
                    hlog_split_time_sum += int(beans[metric])
            elif 'MetaHlogSplitSize' in metric:
                if '_num_ops' in metric:
                    metahlog_split_size_count = beans[metric]
                    metahlog_split_size_key = 'MetaHlogSplitSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    metahlog_split_size_percentile.append(str(float(per) / 100.0))
                    metahlog_split_size_value.append(beans[metric])
                    metahlog_split_size_sum += int(beans[metric])
            elif 'HlogSplitSize' in metric:
                if '_num_ops' in metric:
                    hlog_split_size_count = beans[metric]
                    hlog_split_size_key = 'HlogSplitSize'
                else:
                    per = metric.split("_")[1].split("th")[0]
                    hlog_split_size_percentile.append(str(float(per) / 100.0))
                    hlog_split_size_value.append(beans[metric])
                    hlog_split_size_sum += int(beans[metric])
            else:
                self.hadoop_hbase_metrics['FileSystem'][metric].add_metric(label,beans[metric] if metric in beans and beans[metric] else 0)

        hlog_split_time_bucket = sorted(zip(hlog_split_time_percentile, hlog_split_time_value))
        hlog_split_time_bucket.append(("+Inf", hlog_split_time_count))
        metahlog_split_time_bucket = sorted(zip(metahlog_split_time_percentile, metahlog_split_time_value))
        metahlog_split_time_bucket.append(("+Inf", metahlog_split_time_count))
        hlog_split_size_bucket = sorted(zip(hlog_split_size_percentile, hlog_split_size_value))
        hlog_split_size_bucket.append(("+Inf", hlog_split_size_count))
        metahlog_split_size_bucket = sorted(zip(metahlog_split_size_percentile, metahlog_split_size_value))
        metahlog_split_size_bucket.append(("+Inf", metahlog_split_size_count))

        self.hadoop_hbase_metrics['FileSystem'][hlog_split_time_key].add_metric(label, buckets=hlog_split_time_bucket,sum_value=hlog_split_time_sum)
        self.hadoop_hbase_metrics['FileSystem'][metahlog_split_time_key].add_metric(label,buckets=metahlog_split_time_bucket,sum_value=metahlog_split_time_sum)
        self.hadoop_hbase_metrics['FileSystem'][hlog_split_size_key].add_metric(label, buckets=hlog_split_size_bucket,sum_value=hlog_split_size_sum)
        self.hadoop_hbase_metrics['FileSystem'][metahlog_split_size_key].add_metric(label,buckets=metahlog_split_size_bucket,sum_value=metahlog_split_size_sum)

    def get_metrics(self, beans):
        for i in range(len(beans)):
            if 'Server' in beans[i]['name'] and 'Master' in beans[i]['name']:
                self.get_hbase_server_labels(beans[i])
            elif 'Balancer' in beans[i]['name']:
                self.get_hbase_balbancer_labels(beans[i])
            elif 'AssignmentManger' in beans[i]['name']:
                self.get_assignmentmanger_metrics(beans[i])
            elif 'IPC' in beans[i]['name']:
                self.get_ipc_metrics(beans[i])
            elif 'FileSystem' in beans[i]['name']:
                self.get_filesystem_metrics(beans[i])
            else:
                continue
