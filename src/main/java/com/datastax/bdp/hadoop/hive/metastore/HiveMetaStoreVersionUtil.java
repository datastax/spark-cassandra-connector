/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore;

import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.VersionNumber;

public class HiveMetaStoreVersionUtil
{
    private final static Logger logger = LoggerFactory.getLogger(HiveMetaStoreVersionUtil.class);

    // Only add version if there is metadata format change.
    public static final TreeMap<VersionNumber, Integer> hiveMetaStoreVerions = new TreeMap<VersionNumber, Integer>()
    {{
        put(VersionNumber.parse("4.5.5"), 1);
        put(VersionNumber.parse("5.1.0"), 2);
    }};

    public static final Integer nonHiveMetastoreVersion = -1;

    /**
     * @param dseVersion is expected to be parsed by the pattern: (\d+)\.(\d+)\.(\d+)(\-[.\w]+)?(\+[.\w]+)?
     *
     * @return -1 for 4.6.0 and versions less then 4.5.5, 1 otherwise.
     */
    public static int getHiveMetastoreVersion(String dseVersion)
    {
        // 4.6.0 release still uses the dse version as prefix
        if ("4.6.0".equals(dseVersion))
            return nonHiveMetastoreVersion;

        try
        {
            VersionNumber version = VersionNumber.parse(dseVersion);
            Map.Entry<VersionNumber, Integer> closeDseVersionEntry = hiveMetaStoreVerions.floorEntry(version);
            return closeDseVersionEntry == null ? nonHiveMetastoreVersion : closeDseVersionEntry.getValue();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Wrong dse version : " + dseVersion, e);
            return nonHiveMetastoreVersion;
        }
    }

    public static VersionNumber getDSEVersion(Cluster cluster)
    {
        return cluster.getMetadata().getAllHosts().stream()
                .map(Host::getDseVersion)
                .min(VersionNumber::compareTo)
                .orElseThrow(() -> new RuntimeException("No host to get the current DSE version"));
    }
}
