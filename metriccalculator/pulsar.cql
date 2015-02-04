CREATE KEYSPACE IF NOT EXISTS PULSAR WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

CREATE TABLE IF NOT EXISTS pulsar.mc_mctimegroupmetric(
  metricname text,
  groupid text,
  metrictime timestamp,
  tag_metrictime text,
  "value" counter,
  PRIMARY KEY ((metricname, groupid), metrictime, tag_metrictime)
) WITH CLUSTERING ORDER BY (metrictime ASC, tag_metrictime ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
  
CREATE TABLE IF NOT EXISTS pulsar.mc_groupmetric (
  metricname text,
  groupid text,
  metrictime timestamp,
  "value" int,
  PRIMARY KEY (metricname, metrictime, groupid)
) WITH CLUSTERING ORDER BY (metrictime ASC, groupid ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
  
 
CREATE TABLE IF NOT EXISTS pulsar.mc_countrygroupmetric (
  metricname text,
  groupid text,
  tag_value text,
  metrictime timestamp,
  "value" int,
  PRIMARY KEY ((metricname, groupid), metrictime, tag_value)
) WITH CLUSTERING ORDER BY (metrictime ASC,tag_value ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
  
 
CREATE TABLE IF NOT EXISTS pulsar.mc_topgroupmetric (
  metricname text,
  metrictime timestamp,
  "value" int,
  groupid text,
  PRIMARY KEY (metricname, metrictime, value, groupid)
) WITH CLUSTERING ORDER BY (metrictime ASC, value DESC, groupid ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};
  
CREATE TABLE pulsar.mc_geodevicegroupmetric (
  metricname text,
  groupid text,
  metrictime timestamp,
  tag_con text,
  tag_cty text,
  tag_device text,
  tag_os text,
  tag_bf text,
  "value" int,
  PRIMARY KEY ((metricname, groupid), metrictime, tag_con,tag_cty,tag_device,tag_os,tag_bf)
) WITH CLUSTERING ORDER BY (metrictime ASC,tag_con ASC,tag_cty ASC,tag_device ASC,tag_os ASC,tag_bf ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};