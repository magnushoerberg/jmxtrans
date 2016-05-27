package com.googlecode.jmxtrans.cluster.zookeeper;

import com.googlecode.jmxtrans.cluster.ClusterService;
import lombok.Getter;
import lombok.Setter;

/**
 * ZookeeperConfig. This class stores the Zookeeper related configuration parts.
 *
 * @author Tibor Kulcsar
 * @since <pre>May 17, 2016</pre>
 */
public class ZookeeperConfig {
    public static final String OWNER_NODE_NAME  = "owner";
    public static final String AFFINITY_NODE_NAME = "affinity";
    public static final String CONFIG_NODE_NAME = "config";
    public static final String REQUEST_NODE_NAME = "request";

    @Getter @Setter private String connectionString;
    @Getter @Setter private String workerAlias;
    @Getter @Setter private String heartBeatPath;
    @Getter @Setter private String configPath;
    @Getter @Setter private int connectTimeout;
    @Getter @Setter private int connectRetry;

    protected ZookeeperConfig(String workerAlias,
                              String connectionString,
                              String heartBeatPath,
                              String configPath,
                              int connectTimeout,
                              int connectRetry) {
        this.workerAlias = workerAlias;
        this.connectionString = connectionString;
        this.heartBeatPath = heartBeatPath;
        this.configPath = configPath;
        this.connectTimeout = connectTimeout;
        this.connectRetry = connectRetry;
    }

    public String getWorkerNodePath(){
        return this.heartBeatPath + "/" + workerAlias;
    }

    public String getJvmPath(String jvmAlias){
        return configPath + "/" + jvmAlias;
    }

    public String getOwnerNodePath(String jvmAlias){
        return configPath + "/" + jvmAlias + "/" + OWNER_NODE_NAME;
    }

    public String getJvmAffinityNodePath(String jvmAlias){
        return configPath + "/" + jvmAlias + "/" + AFFINITY_NODE_NAME;
    }

    public String getConfigNodePath(String jvmAlias){
        return configPath + "/" + jvmAlias + "/" + CONFIG_NODE_NAME;
    }

    public String getRequestNodePath(String jvmAlias){
        return configPath + "/" + jvmAlias + "/" + REQUEST_NODE_NAME;
    }

    public String getAffinityWorkerPath(String affinity){
        return this.heartBeatPath + "/" + affinity;
    }


}