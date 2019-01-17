Volume Manager for MapR-FS
==========================

Purpose
-------

Volume manager package is used to automatically create and delete volumes according to specified configuration.

Build
-----

1. $ mvn clean package
2. $ mvn rpm:rpm
3. Install RPM on the node(s) where you'd like volume manager to run. 
4. Restart Warden on the node(s) in step 3.
5. Place volume group configuration files in /opt/mapr/volume-manager/conf/vg.d

Sample configuration files
--------------------------

Below are sample configuration files

<pre></code>
pathformat=\'/projects/silog/data/phase=DEV/app=sialt/year=\'yyyy/\'month=\'MM
volumename=silog_data_sialt
minreplication=2
replication=3
replicationtype=high_throughput
owner=silog
group=silog
permission=770
ae=silog
aetype=1
topology=/data/lbox
aceEnabled=yes
readAce="g:silog|u:pdtrump"
writeAce="u:silog"
sched=0
interval=month
ahead=1
retention=3

pathformat=\'/projects/iss/auto/userlogs/\'yyyyMMdd
volumename=iss_auto_userlogs
minreplication=2
replication=3
replicationtype=high_throughput
owner=iss
group=iss
permission=770
ae=iss
aetype=1
topology=/data/iss
sched=0
interval=day
ahead=2
retention=14
</code></pre>
