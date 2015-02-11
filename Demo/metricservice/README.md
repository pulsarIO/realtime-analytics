# Metricservice

Metricservice provides a restful interface to query Cassandra table to visualize the metrics and draw some graphs. This is just for demo purpose.

Sample urls:

http://HOST:PORT/pulsar/metric?columnFamilyName=mc_groupmetric&metricname=visitorsperbrowser

http://HOST:PORT/pulsar/counter?metricname=MCSessionCount&groupid=2015-01-08