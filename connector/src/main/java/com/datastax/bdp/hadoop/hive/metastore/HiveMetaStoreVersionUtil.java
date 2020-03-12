/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.hadoop.hive.metastore;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TreeMap;

public class HiveMetaStoreVersionUtil
{
    private final static Logger logger = LoggerFactory.getLogger(HiveMetaStoreVersionUtil.class);

    // Only add version if there is metadata format change.
    public static final TreeMap<Version, Integer> hiveMetaStoreVerions = new TreeMap<Version, Integer>()
    {{
        put(Version.parse("4.5.5"), 1);
        put(Version.parse("5.1.0"), 2);
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
            Version version = Version.parse(dseVersion);
            Map.Entry<Version, Integer> closeDseVersionEntry = hiveMetaStoreVerions.floorEntry(version);
            return closeDseVersionEntry == null ? nonHiveMetastoreVersion : closeDseVersionEntry.getValue();
        }
        catch (IllegalArgumentException e)
        {
            logger.error("Wrong dse version : " + dseVersion, e);
            return nonHiveMetastoreVersion;
        }
    }

    public static Version getDSEVersion(CqlSession session)
    {
        return session.getMetadata().getNodes().values().stream()
                .map(node -> (Version)node.getExtras().get(DseNodeProperties.DSE_VERSION))
                .min(Version::compareTo)
                .orElseThrow(() -> new RuntimeException("No host to get the current DSE version"));
    }
}
