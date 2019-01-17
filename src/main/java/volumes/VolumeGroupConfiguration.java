package volumes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * VolumeGroupConfiguration class contains parameters of the MapR volume
 * properties, as well as volume manager specific properties like retention and
 * ahead factor.
 */
class VolumeGroupConfiguration {

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG =
            LogFactory.getLog(VolumeGroupConfiguration.class);

    /**
     * MapR cluster name as security latch, currently unused
     */
    private String cluster;

    /**
     * volume mount path template in StringDateFormat compatible format
     */
    private String pathformat;

    /**
     * volume name prefix
     */
    private String name;

    /**
     * MapR minimum replication factor
     */
    private int minReplication;

    /**
     * MapR replication factor
     */
    private int replication;

    /**
     * MapR replication type
     */
    private String replicationType;

    /**
     * MapR volume owner
     *
     * this property is used both as MapR volume owner as well as MapR-FS
     * mount directory owner, in order to provide FS operation access to the
     * actual owner of the volume.
     */
    private String owner;

    /**
     * MapR volume group
     *
     * this property is used both as MapR volume owner group as well as MapR-FS
     * mount directory owner group, in order to provide FS operation access to
     * the actual owner group of the volume.
     */
    private String group;

    /**
     * MapR accounting entity
     */
    private String ae;

    /**
     * MapR accounting entity type
     */
    private int aeType;

    /**
     * enabled/disable MapR ACE
     */
    private boolean isAceEnabled;

    /**
     * read ACE
     */
    private String readAce;

    /**
     * write ACE
     */
    private String writeAce;

    /**
     * MapR volume cluster topology
     */
    private String topology;

    /**
     * MapR volume snapshot schedule id
     */
    private int schedule;

    /**
     * volume retention indicating how many full intervals the volume should
     * exist in the cluster before it will be automatically purged
     *
     * special value 0 will enforce skipping purge actions, i.e. retention is
     * 'forever'
     */
    private int retention;

    /**
     * volume creation interval, aka 'partition interval'
     * this can be 'day', 'month' or 'year'
     */
    private String interval;

    /**
     * ahead factor - how many volumes of this group has to be pre-created in
     * advance
     */
    private int aheadFactor;

    /**
     * FS permission to be set on the volume mount directory
     */
    private String permission;

    /**
     * set cluster name
     */
    public void setCluster(String cl) {
        cluster = cl;
    }

    /**
     * retrieve cluster name
     */
    public String getCluster() {
        return cluster;
    }

    /**
     * set path format
     */
    public void setPathFormat(String p) {
        pathformat = p;
    }

    /**
     * retrieve path format
     */
    public String getPathFormat() {
        return pathformat;
    }

    /**
     * set volume name prefix
     */
    public void setName(String n) {
        name = n;
    }

    /**
     * retrieve volume name prefix
     */
    public String getName() {
        return name;
    }

    /**
     * set minimum replication factor
     */
    public void setMinReplication(int mr) {
        minReplication = mr;
    }

    /**
     * retrieve minimum replication factor
     */
    public int getMinReplication() {
        return minReplication;
    }

    /**
     * set replication factor
     */
    public void setReplication(int r) {
        replication = r;
    }

    /**
     * retrieve replication factor
     */
    public int getReplication() {
        return replication;
    }

    /**
     * set replication type
     */
    public void setReplicationType(String rt) {
        replicationType = rt;
    }

    /**
     * retrieve replication type
     */
    public String getReplicationType() {
        return replicationType;
    }

    /**
     * set owner
     */
    public void setOwner(String o) {
        owner = o;
    }

    /**
     * retrieve owner
     */
    public String getOwner() {
        return owner;
    }

    /**
     * set owner group
     */
    public void setGroup(String g) {
        group = g;
    }

    /**
     * retrieve owner group
     */
    public String getGroup() {
        return group;
    }

    /**
     * set accounting entity
     */
    public void setAe(String a) {
        ae = a;
    }

    /**
     * retrieve accounting entity
     */
    public String getAe() {
        return ae;
    }

    /**
     * set accounting entity type
     */
    public void setAeType(int type) {
        aeType = type;
    }

    /**
     * retrieve accounting entity type
     */
    public int getAeType() {
        return aeType;
    }

    /**
     * set ACE enabled/disabled indicator
     */
    public void setAceEnabled(boolean enabled) {
        isAceEnabled = enabled;
    }

    /**
     * get ACE enabled/disabled indicator
     */
    public boolean isAceEnabled() {
        return isAceEnabled;
    }

    /**
     * set read ACE
     */
    public void setReadAce(String ace) {
        readAce = ace;
    }

    /**
     * retrieve read ACE
     */
    public String getReadAce() {
        return readAce;
    }

    /**
     * set write ACE
     */
    public void setWriteAce(String ace) {
        writeAce = ace;
    }

    /**
     * retrieve write ACE
     */
    public String getWriteAce() {
        return writeAce;
    }

    /**
     * set volume cluster topology
     */
    public void setTopology(String t) {
        topology = t;
    }

    /**
     * retrieve volume cluster topology
     */
    public String getTopology() {
        return topology;
    }

    /**
     * set snapshot schedule id
     */
    public void setSchedule(int id) {
        schedule = id;
    }

    /**
     * retrieve snapshot schedule id
     */
    public int getSchedule() {
        return schedule;
    }

    /**
     * set retention
     */
    public void setRetention(int ret) {
        retention = ret;
    }

    /**
     * retrieve retention
     */
    public int getRetention() {
        return retention;
    }

    /**
     * set creation interval
     */
    public void setInterval(String i) {
        interval = i;
    }

    /**
     * retrieve creation interval
     */
    public String getInterval() {
        return interval;
    }

    /**
     * set ahead factor
     */
    public void setAheadFactor(int af) {
        aheadFactor = af;
    }

    /**
     * retrieve ahead factor
     */
    public int getAheadFactor() {
        return aheadFactor;
    }

    /**
     * set FS mount directory permission
     */
    public void setPermission(String p) {
        permission = p;
    }

    /**
     * retrieve FS mount directory permission
     */
    public String getPermission() {
        return permission;
    }
}
