# hadoop_exporter-grafana

# 如何使用？

```
python3 hadoop_jmx_exporter.py -h
```

```

usage: hadoop_jmx_exporter.py [-h] -cluster cluster_name
                              [-queue yarn_queue_regexp]
                              [-nns [namenode_jmx_url [namenode_jmx_url ...]]]
                              [-rms [resourcemanager_jmx_url [resourcemanager_jmx_url ...]]]
                              [-hss [hiveserver_jmx_url [hiveserver_jmx_url ...]]]
                              [-jns [journalnode_jmx_url [journalnode_jmx_url ...]]]
                              [-hbs [hbase_server_url [hbase_server_url ...]]]
                              [-hrs [hbase_region_url [hbase_region_url ...]]]
                              [-host host] [-port port]

hadoop jmx metric prometheus exporter

optional arguments:
  -h, --help            show this help message and exit
  -cluster cluster_name
                        Hadoop cluster name (maybe HA name)
  -queue yarn_queue_regexp
                        Regular expression of queue name. default: root.*
  -nns [namenode_jmx_url [namenode_jmx_url ...]]
                        Hadoop hdfs namenode jmx metrics URL.
  -rms [resourcemanager_jmx_url [resourcemanager_jmx_url ...]]
                        Hadoop resourcemanager metrics jmx URL.
  -hss [hiveserver_jmx_url [hiveserver_jmx_url ...]]
                        Hive Hiveserver2 jmx metrics URL.
  -hbs [hbase_server_url [hbase_server_url ...]]
                        Hive Hbase Server jmx metrics URL.
  -hrs [hbase_region_url [hbase_region_url ...]]
                        Hive Hbase regionServer jmx metrics URL.
  -jns [journalnode_jmx_url [journalnode_jmx_url ...]]
                        Hadoop journalnode jmx metrics URL.
  -host host            Listen on this address. default: 0.0.0.0
  -port port            Listen to this port. default: 6688
```
