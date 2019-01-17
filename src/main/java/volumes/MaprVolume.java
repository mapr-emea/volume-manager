package volumes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.text.ParseException;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import com.mapr.fs.MapRFileAce;
import com.mapr.fs.MapRFileSystem;
import java.net.URI;
import java.util.ArrayList;

// class representing MapR volume item
class MaprVolume {

    // logger
    private static final Log LOG = LogFactory.getLog(MaprVolume.class);

    // name
    private String name = null;

    // mount directory
    private String mountdir = null;

    // VG configuration item for reference
    private VolumeGroupConfiguration vgc = null;

    // mount path
    private String path = null;

    // owner of the mount directory
    private String owner = null;

    // group of the mount directory
    private String group = null;

    // permission to set on mount directory
    private String permission = null;

    // accounting entity
    private String ae = null;

    // accounting entity type
    private int aeType;

    // topology
    private String topology = new String("/data");

    // snapshot schedule
    private int schedule = 0;

    // minimum replication factor
    private int minReplication = 2;

    // replication factor
    private int replication = 3;

    // replication type
    private String replicationType;

    // ACE enabled/disabled
    private boolean isAceEnabled;

    // read ACE
    private String readAce;

    // write ACE
    private String writeAce;

    // internal array containing ACEs
    private ArrayList<MapRFileAce> aces = new ArrayList<MapRFileAce>();

    /**
     * This constructor creates dummy MapR volume item,
     * only name will be set as a property.
     * This constructor is used for purge handling, since
     * in order to purge the volume it is sufficient to 
     * know its name.
     */
    MaprVolume(String name) {
        setName(name);
    }

    /**
     * This constructor creates full volume descriptor,
     * used by VolumeActionManager to create the volume in
     * the cluster.
     */
    MaprVolume(VolumeGroupConfiguration vgc, String appender) {
        StringBuilder sb = new StringBuilder();

        sb.append(vgc.getName());

        if (!appender.isEmpty()) {
            sb.append("_");
            sb.append(appender);
        }

        setName(sb.toString());
        setVolumeGroupConfiguration(vgc);
        setPath(computePath(appender));
        setMountDir(computePath(appender));
        setOwner(vgc.getOwner());
        setGroup(vgc.getGroup());
        setPermission(vgc.getPermission());
        setAe(vgc.getAe());
        setAeType(vgc.getAeType());
        setTopology(vgc.getTopology());
        setSchedule(vgc.getSchedule());
        setMinReplication(vgc.getMinReplication());
        setReplication(vgc.getReplication());
        setReplicationType(vgc.getReplicationType());
        setAceEnabled(vgc.isAceEnabled());
        setReadAce(vgc.getReadAce());
        setWriteAce(vgc.getWriteAce());
    }

    /**
     * set name of the volume
     */
    public void setName(String n) {
        this.name = n;
    }

    /**
     * retrieve name of the volume
     */
    public String getName() {
        return name;
    }

    /**
     * set mount directory
     */
    public void setMountDir(String m) {
        this.mountdir = m;
    }

    /**
     * retrieve mount directory
     */
    public String getMountDir() {
        return mountdir;
    }

    /**
     * set owner of the volume.
     */
    public void setOwner(String o) {
        this.owner = o;
    }

    /**
     * retrieve owner of the volume
     */
    public String getOwner() {
        return owner;
    }

    /**
     * set owner group of the volume
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * retrieve owner group of the volume
     */
    public String getGroup() {
        return group;
    }

    /**
     * set DFS permissions to be set on the volume directory
     */
    public void setPermission(String permission) {
        this.permission = permission;
    }

    /**
     * retrieve DFS permissions to be set on the volume directory
     */
    public String getPermission() {
        return permission;
    }

    /**
     * set accounting entity to be used on the volume
     */
    public void setAe(String ae) {
        this.ae = ae;
    }

    /**
     * retrieve accounting entity used on the volume
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
     * set minimum replication factor of the volume
     */
    public void setMinReplication(int mr) {
        this.minReplication = mr;
    }

    /**
     * retrieve minimum replication factor of the volume
     */
    public int getMinReplication() {
        return minReplication;
    }

    /**
     * set replication factor of the volume
     */
    public void setReplication(int r) {
        this.replication = r;
    }

    /**
     * retrieve replication factor of the volume
     */
    public int getReplication() {
        return replication;
    }

    /**
     * set replication type of the volume
     */
    public void setReplicationType(String rt) {
        this.replicationType = rt;
    }

    /**
     * retrieve replication type of the volume
     */
    public String getReplicationType() {
        return replicationType;
    }

    /**
     * set ACE enabled/disabled indicator
     */
    public void setAceEnabled(boolean enabled) {
        this.isAceEnabled = enabled;
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
        this.readAce = ace;
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
        this.writeAce = ace;
    }

    /**
     * retrieve write ACE
     */
    public String getWriteAce() {
        return writeAce;
    }

    /**
     * retrieve ACE array
     */
    public ArrayList<MapRFileAce> getAces() {
        return aces;
    }

    public void setVolumeGroupConfiguration(VolumeGroupConfiguration vgc) {
        this.vgc = vgc;
    }


    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    private String computePath(String appender) {

        // if appender is empty string, simply return path
        if (appender.isEmpty()) {
            return new SimpleDateFormat(vgc.getPathFormat()).format(new Date());
        }

        // in all other cases we expect appender to be formatted date
        final DateFormat df = new SimpleDateFormat("yyyyMMdd");

        Date date = null;
        try {
            date = (Date)df.parse(appender);
        } catch (ParseException pe) {
            LOG.error("date '" + appender + "' parsing error: " + pe.getMessage());
            return null;
        }

        final SimpleDateFormat sdf = new SimpleDateFormat(vgc.getPathFormat());
        return sdf.format(date.getTime());
    }
}
