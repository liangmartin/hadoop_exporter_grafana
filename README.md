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
                              [-jns [journalnode_jmx_url [journalnode_jmx_url ...]]]
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
  -jns [journalnode_jmx_url [journalnode_jmx_url ...]]
                        Hadoop journalnode jmx metrics URL.
  -host host            Listen on this address. default: 0.0.0.0
  -port port            Listen to this port. default: 6688
```
