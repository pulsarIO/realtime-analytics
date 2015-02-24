/*
Pulsar
Copyright (C) 2013-2015 eBay Software Foundation
Licensed under the GPL v2 license.  See LICENSE for full terms.
*/
package com.ebay.pulsar.metriccalculator.cassandra;

import java.util.List;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.ebay.jetstream.config.AbstractNamedBean;
import com.ebay.jetstream.xmlser.XSerializable;

@ManagedResource(objectName = "CassandraConfig", description = "Configuration for cassandra")
public class CassandraConfig extends AbstractNamedBean implements
        XSerializable, InitializingBean {
    private boolean enableCassandra = false;
    private String keySpace;
    private int batchSize = 100;
    private boolean loggedBatch = true;

    private int throttlingCount = 10;
    private int sleepTime = 1; // 1 MS

    // Copy from driver.
    private static final int DEFAULT_MIN_REQUESTS = 25;
    private static final int DEFAULT_MAX_REQUESTS = 100;
    private static final int DEFAULT_CORE_POOL_LOCAL = 2;
    private static final int DEFAULT_CORE_POOL_REMOTE = 1;
    private static final int DEFAULT_MAX_POOL_LOCAL = 8;
    private static final int DEFAULT_MAX_POOL_REMOTE = 2;
    private int remoteCoreConnectionsPerHost = DEFAULT_CORE_POOL_REMOTE;
    private int localCoreConnectionsPerHost = DEFAULT_CORE_POOL_LOCAL;
    private int remoteMaxConnectionsPerHost = DEFAULT_MAX_POOL_REMOTE;
    private int localMaxConnectionsPerHost = DEFAULT_MAX_POOL_LOCAL;
    private int remoteMaxSimultaneousRequestsPerConnectionThreshold = DEFAULT_MAX_REQUESTS;
    private int localMaxSimultaneousRequestsPerConnectionThreshold = DEFAULT_MAX_REQUESTS;
    private int remoteMinSimultaneousRequestsPerConnectionThreshold = DEFAULT_MIN_REQUESTS;
    private int localMinSimultaneousRequestsPerConnectionThreshold = DEFAULT_MIN_REQUESTS;
    private SSLOptions sslOptions;
    private List<String> contactPoints;
    private int port = ProtocolOptions.DEFAULT_PORT;
    private String username;
    private String password;
    private int usedHostsPerRemoteDc = 4;
    private ReconnectionPolicy reconnectionPolicy;
    private RetryPolicy retryPolicy;
    private ProtocolOptions.Compression compression = ProtocolOptions.Compression.NONE;
    private boolean metricsEnabled = true;
    private boolean jmxEnabled = true;

    private int connectTimeoutMillis = SocketOptions.DEFAULT_CONNECT_TIMEOUT_MILLIS;
    private int readTimeoutMillis = SocketOptions.DEFAULT_READ_TIMEOUT_MILLIS;
    private Boolean keepAlive;
    private Boolean reuseAddress;
    private Integer soLinger;
    private Boolean tcpNoDelay;
    private Integer receiveBufferSize;
    private Integer sendBufferSize;
    private boolean rotateSessionTable = true;

    private void copyPoolingOptions(Builder builder) {
        PoolingOptions opts = new PoolingOptions();

        opts.setCoreConnectionsPerHost(HostDistance.REMOTE,
                remoteCoreConnectionsPerHost);
        opts.setCoreConnectionsPerHost(HostDistance.LOCAL,
                localCoreConnectionsPerHost);
        opts.setMaxConnectionsPerHost(HostDistance.REMOTE,
                remoteMaxConnectionsPerHost);
        opts.setMaxConnectionsPerHost(HostDistance.LOCAL,
                localMaxConnectionsPerHost);
        opts.setMaxSimultaneousRequestsPerConnectionThreshold(
                HostDistance.REMOTE,
                remoteMaxSimultaneousRequestsPerConnectionThreshold);
        opts.setMaxSimultaneousRequestsPerConnectionThreshold(
                HostDistance.LOCAL,
                localMaxSimultaneousRequestsPerConnectionThreshold);
        opts.setMinSimultaneousRequestsPerConnectionThreshold(
                HostDistance.REMOTE,
                remoteMinSimultaneousRequestsPerConnectionThreshold);
        opts.setMinSimultaneousRequestsPerConnectionThreshold(
                HostDistance.LOCAL,
                localMinSimultaneousRequestsPerConnectionThreshold);

        builder.withPoolingOptions(opts);
    }

    public Builder createBuilder() {
        Builder builder = Cluster.builder();
        for (String address : contactPoints) {
            builder.addContactPoint(address);
        }
        builder.withCompression(compression);
        if (username != null && password != null) {
            builder.withCredentials(username, password);
        }
     
        if (reconnectionPolicy != null) {
            builder.withReconnectionPolicy(reconnectionPolicy);
        }

        if (retryPolicy != null) {
            builder.withRetryPolicy(retryPolicy);
        }
        builder.withPort(port);

        if (!jmxEnabled) {
            builder.withoutJMXReporting();
        }

        if (!metricsEnabled) {
            builder.withoutMetrics();
        }

        if (sslOptions != null) {
            builder.withSSL(sslOptions);
        }

        copyPoolingOptions(builder);

        SocketOptions opts = new SocketOptions();
        opts.setConnectTimeoutMillis(connectTimeoutMillis);
        opts.setReadTimeoutMillis(readTimeoutMillis);

        if (receiveBufferSize != null) {
            opts.setReceiveBufferSize(receiveBufferSize);
        }
        if (sendBufferSize != null) {
            opts.setSendBufferSize(sendBufferSize);
        }
        if (soLinger != null) {
            opts.setSoLinger(soLinger);
        }
        if (keepAlive != null) {
            opts.setKeepAlive(keepAlive);
        }
        if (reuseAddress != null) {
            opts.setReuseAddress(reuseAddress);
        }
        if (tcpNoDelay != null) {
            opts.setTcpNoDelay(tcpNoDelay);
        }

        builder.withSocketOptions(opts);
        return builder;
    }

    public boolean getEnableCassandra() {
        return enableCassandra;
    }

    public void setEnableCassandra(boolean enableCassandra) {
        this.enableCassandra = enableCassandra;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public ProtocolOptions.Compression getCompression() {
        return compression;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public List<String> getContactPoints() {
        return contactPoints;
    }

    public boolean getJmxEnabled() {
        return jmxEnabled;
    }

    public Boolean getKeepAlive() {
        return keepAlive;
    }

    public String getKeySpace() {
        return keySpace;
    }

    public int getLocalCoreConnectionsPerHost() {
        return localCoreConnectionsPerHost;
    }

    public int getLocalMaxConnectionsPerHost() {
        return localMaxConnectionsPerHost;
    }

    public int getLocalMaxSimultaneousRequestsPerConnectionThreshold() {
        return localMaxSimultaneousRequestsPerConnectionThreshold;
    }

    public int getLocalMinSimultaneousRequestsPerConnectionThreshold() {
        return localMinSimultaneousRequestsPerConnectionThreshold;
    }

    public boolean getLoggedBatch() {
        return loggedBatch;
    }

    public boolean getMetricsEnabled() {
        return metricsEnabled;
    }

    public String getPassword() {
        return password;
    }

    public int getPort() {
        return port;
    }

    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public Integer getReceiveBufferSize() {
        return receiveBufferSize;
    }

    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    public int getRemoteCoreConnectionsPerHost() {
        return remoteCoreConnectionsPerHost;
    }

    public int getRemoteMaxConnectionsPerHost() {
        return remoteMaxConnectionsPerHost;
    }

    public int getRemoteMaxSimultaneousRequestsPerConnectionThreshold() {
        return remoteMaxSimultaneousRequestsPerConnectionThreshold;
    }

    public int getRemoteMinSimultaneousRequestsPerConnectionThreshold() {
        return remoteMinSimultaneousRequestsPerConnectionThreshold;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    public Boolean getReuseAddress() {
        return reuseAddress;
    }

    public boolean getRotateSessionTable() {
        return rotateSessionTable;
    }

    public Integer getSendBufferSize() {
        return sendBufferSize;
    }

    public Integer getSoLinger() {
        return soLinger;
    }

    public SSLOptions getSslOptions() {
        return sslOptions;
    }

    public Boolean getTcpNoDelay() {
        return tcpNoDelay;
    }

    public int getUsedHostsPerRemoteDc() {
        return usedHostsPerRemoteDc;
    }

    public String getUsername() {
        return username;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public void setCompression(ProtocolOptions.Compression compression) {
        this.compression = compression;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void setContactPoints(List<String> contactPoints) {
        this.contactPoints = contactPoints;
    }

    public void setJmxEnabled(boolean jmxEnabled) {
        this.jmxEnabled = jmxEnabled;
    }

    public void setKeepAlive(Boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public void setKeySpace(String keySpace) {
        this.keySpace = keySpace;
    }

    public void setLocalCoreConnectionsPerHost(int localCoreConnectionsPerHost) {
        this.localCoreConnectionsPerHost = localCoreConnectionsPerHost;
    }

    public void setLocalMaxConnectionsPerHost(int localMaxConnectionsPerHost) {
        this.localMaxConnectionsPerHost = localMaxConnectionsPerHost;
    }

    public void setLocalMaxSimultaneousRequestsPerConnectionThreshold(
            int localMaxSimultaneousRequestsPerConnectionThreshold) {
        this.localMaxSimultaneousRequestsPerConnectionThreshold = localMaxSimultaneousRequestsPerConnectionThreshold;
    }

    public void setLocalMinSimultaneousRequestsPerConnectionThreshold(
            int localMinSimultaneousRequestsPerConnectionThreshold) {
        this.localMinSimultaneousRequestsPerConnectionThreshold = localMinSimultaneousRequestsPerConnectionThreshold;
    }

    public void setLoggedBatch(boolean loggedBatch) {
        this.loggedBatch = loggedBatch;
    }

    public void setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public void setReceiveBufferSize(Integer receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
    }

    public void setReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
        this.reconnectionPolicy = reconnectionPolicy;
    }

    public void setRemoteCoreConnectionsPerHost(int remoteCoreConnectionsPerHost) {
        this.remoteCoreConnectionsPerHost = remoteCoreConnectionsPerHost;
    }

    public void setRemoteMaxConnectionsPerHost(int remoteMaxConnectionsPerHost) {
        this.remoteMaxConnectionsPerHost = remoteMaxConnectionsPerHost;
    }

    public void setRemoteMaxSimultaneousRequestsPerConnectionThreshold(
            int remoteMaxSimultaneousRequestsPerConnectionThreshold) {
        this.remoteMaxSimultaneousRequestsPerConnectionThreshold = remoteMaxSimultaneousRequestsPerConnectionThreshold;
    }

    public void setRemoteMinSimultaneousRequestsPerConnectionThreshold(
            int remoteMinSimultaneousRequestsPerConnectionThreshold) {
        this.remoteMinSimultaneousRequestsPerConnectionThreshold = remoteMinSimultaneousRequestsPerConnectionThreshold;
    }

    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    public void setReuseAddress(Boolean reuseAddress) {
        this.reuseAddress = reuseAddress;
    }

    public void setRotateSessionTable(boolean rotateSessionTable) {
        this.rotateSessionTable = rotateSessionTable;
    }

    public void setSendBufferSize(Integer sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
    }

    public void setSoLinger(Integer soLinger) {
        this.soLinger = soLinger;
    }

    public void setSslOptions(SSLOptions sslOptions) {
        this.sslOptions = sslOptions;
    }

    public void setTcpNoDelay(Boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
    }

    public void setUsedHostsPerRemoteDc(int usedHostsPerRemoteDc) {
        this.usedHostsPerRemoteDc = usedHostsPerRemoteDc;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public int getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }

    public int getThrottlingCount() {
        return throttlingCount;
    }

    public void setThrottlingCount(int throttlingCount) {
        this.throttlingCount = throttlingCount;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        // TODO Auto-generated method stub
    }
}
