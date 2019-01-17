package volumes;

import getent.Getent;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FilenameFilter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import java.util.Calendar;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.hadoop.fs.permission.FsPermission;

import com.mapr.fs.MapRFileAce;

/**
* VolumeManagerConfiguration - configuration class for volume manager
* application
*/
class VolumeManagerConfiguration {

    /**
     * XML 'Hadoop-style' configuration helper
     */
    private volatile Configuration conf = null;

    /**
     * logger for troubleshooting and debugging
     */
    private static final Log LOG =
            LogFactory.getLog(VolumeManagerConfiguration.class);

    /**
     * don't allow main loop interval below 10 sec
     */
    private static final long minLoopInterval = 10000;

    /**
     * XML configuraiton keys
     */
    public static final String MAPR_REST_PRINCIPAL_KEY =
            "volume.mapr.rest.principal";
    public static final String MAPR_REST_KEYTAB_KEY =
            "volume.mapr.rest.keytab";
    public static final String MAPR_REST_NODES_KEY =
            "volume.mapr.rest.nodes";
    public static final String MAPR_REST_PORT_KEY =
            "volume.mapr.rest.port";
    public static final String MAPR_LOOP_INTERVAL_KEY =
            "volume.loop.interval";
    public static final String VG_CONFIG_DIR_KEY =
            "volume.groups.config.dir";
    public static final String FS_ACTION_ATTEMPTS =
            "volume.fs.action.attempts";
    public static final String REST_THROTTLING_INTERVAL =
            "volume.rest.throttling.interval";

    /**
     * Volume group configuration properties
     */
    private static final String VG_PROP_CLUSTER = "cluster";
    private static final String VG_PROP_PATHFORMAT = "pathformat";
    private static final String VG_PROP_NAME = "name";
    private static final String VG_PROP_MINREPLICATION = "minreplication";
    private static final String VG_PROP_REPLICATION = "replication";
    private static final String VG_PROP_REPLICATIONTYPE = "replicationtype";
    private static final String VG_PROP_OWNER = "owner";
    private static final String VG_PROP_GROUP = "group";
    private static final String VG_PROP_PERMISSION = "permission";
    private static final String VG_PROP_AE = "ae";
    private static final String VG_PROP_AETYPE = "aetype";
    private static final String VG_PROP_IS_ACE_ENABLED = "aceEnabled";
    private static final String VG_PROP_READ_ACE = "readAce";
    private static final String VG_PROP_WRITE_ACE = "writeAce";
    private static final String VG_PROP_TOPOLOGY = "topology";
    private static final String VG_PROP_SCHEDULE = "schedule";
    private static final String VG_PROP_INTERVAL = "interval";
    private static final String VG_PROP_RETENTION = "retention";
    private static final String VG_PROP_AHEAD = "ahead";

    /**
     * MapR kerberos principal
     */
    private String maprPrincipal;

    /**
     * MapR kerberos keytab file path (including the filename)
     */
    private String maprKeytabPath;

    /**
     * MapR REST nodes
     */
    private List<String> restNodes;

    /**
     * MapR REST port number
     */
    private String maprRestPort;

    /**
     * main loop sleep interval
     */
    private long loopInterval;

    /**
     * configuration directory from where configuration has been loaded
     */
    private static File configDir;

    /**
     * configuration directory for volume groups
     */
    private File vgConfigDir;

    /**
     * number of FS action attempts for each volume
     */
    private long fsActionAttempts;

    /**
     * Time to sleep between looping REST calls
     */
    private long restThrottlingInterval;

    /**
     * map of volume group configuration elements
     */
    private Map<String, VolumeGroupConfiguration> vgMap;

    /**
     * map of supported calendar intervals
     */
    private static Map<String, Integer> calIntervalMap =
            new HashMap<String, Integer>();

    /**
     * validity indicator
     */
    private boolean isValid;

    /**
     * last modified timestamp
     */
    private static long lastModified = 0;

    /**
     * The Java wrapper for the Unix getent command.
     */
    private final Getent getent = new Getent(120);

    /**
     * config reload indicator
     */
    private boolean hasConfigReloaded;

    /**
     * Constructor
     */
    VolumeManagerConfiguration(File confDir) {

        this.isValid = true;
        populateCalendarIntervalMap();
        this.conf = VolumeManagerConfiguration.loadConfiguration(confDir);
        parseConfiguration(this.conf);
        // load all VGs to be managed
        loadVolumeGroups();
        this.hasConfigReloaded = true;
    }

    /**
     * set MapR principal name
     */
    public void setMaprPrincipal(String principal) {
        this.maprPrincipal = principal;
    }

    /**
     * retrieve MapR principal name
     */
    public String getMaprPrincipal() {
        return maprPrincipal;
    }

    /**
     * set path to MapR principal keytab file (including filename)
     */
    public void setMaprKeytabPath(String path) {
        this.maprKeytabPath = path;
    }

    /**
     * retrieve path to MapR principal keytab file (including filename)
     */
    public String getMaprKeytabPath() {
        return maprKeytabPath;
    }

    /**
     * set MapR cluster REST node list
     */
    public void setRestNodes(List<String> array) {
        this.restNodes = array;
    }

    /**
     * retrieve MapR cluster REST node list
     */
    public List<String> getRestNodes() {
        return restNodes;
    }

    /**
     * set MapR REST port number
     */
    public void setMaprRestPort(String port) {
        this.maprRestPort = port;
    }

    /**
     * retrieve MapR REST port number
     */
    public String getMaprRestPort() {
        return maprRestPort;
    }

    /**
     * set applicaiton main loop interval
     */
    public void setLoopInterval(long msec) {
        this.loopInterval = msec;
    }

    /**
     * retrieve application main loop interval
     */
    public long getLoopInterval() {
        return loopInterval;
    }

    /**
     * set directory containing volume manager configuration
     */
    private static void setConfigDir(File dir) {
        configDir = dir;
    }

    /**
     * retrieve directory containing volume manager configuration
     */
    private static File getConfigDir() {
        return configDir;
    }

    /**
     * set directory containing volume group configuration files
     */
    public void setVgConfigDir(File dir) {
        this.vgConfigDir = dir;
    }

    /**
     * retrieve directory containing volume group configuration files
     */
    public File getVgConfigDir() {
        return vgConfigDir;
    }

    /**
     * set number of FS action attempts for a single volume
     */
    public void setFsActionAttempts(long a) {
        this.fsActionAttempts = a;
    }

    /**
     * get number of FS action attempts for a single volume
     */
    public long getFsActionAttempts() {
        return fsActionAttempts;
    }

    /**
     * set throttling interval (for querying ACEs)
     */
    public void setRestThrottlingInterval(long i) {
        this.restThrottlingInterval = i;
    }

    /**
     * get throttling interval (for querying ACEs)
     */
    public long getRestThrottlingInterval() {
        return restThrottlingInterval;
    }

    /**
     * retrieve the map of volume group configration elements
     */
    public Map<String, VolumeGroupConfiguration> getVgMap() {
        return vgMap;
    }

    /**
     * get indicator of config reload event
     */
    public boolean hasConfigReloaded() {
        return this.hasConfigReloaded;
    }

    /**
     * set indicator of config reload event
     */
    public void clearConfigReloaded() {
        this.hasConfigReloaded = false;
    }

    /**
     * check if configuration is valid
     */
    public boolean isValid() {
        return isValid;
    }

    /**
     * Reads all the XML files from the given directory and attempts to load
     * them into a {@link Configuration object}.
     *
     * @param configDir
     *            the configuration directory to search for XML files
     * @return
     */
    private static Configuration loadConfiguration(final File configDir) {

        final Configuration conf = new Configuration();

        setConfigDir(configDir);

        final File[] xmlFiles = configDir.listFiles(new FilenameFilter() {

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean accept(final File dir, final String name) {
                return name.endsWith(".xml");
            }
        });

        if (xmlFiles != null) {
            for (File xmlFile : xmlFiles) {
                conf.addResource(xmlFile);
                // preserve time of last modification
                if (xmlFile.lastModified() > lastModified) {
                    lastModified = xmlFile.lastModified();
                }
            }
        }

        return conf;
    }

    /**
     * parses XML Hadoop-style configuration
     */
    private void parseConfiguration(Configuration conf) {

        // read REST nodes and populate the set in configuration
        final String restNodesStr = conf.get(MAPR_REST_NODES_KEY, null);
        if (restNodesStr == null) {
            LOG.error(buildConfigErrorMessage(MAPR_REST_NODES_KEY));
            this.isValid = false;
            return;
        }

        final String[] n = restNodesStr.split(",");
        List<String> nodes = new ArrayList<String>();
        for (String s : n) {
            nodes.add(s);
        }

        setRestNodes(nodes);

        // kerberos principal
        final String principal = conf.get(MAPR_REST_PRINCIPAL_KEY, null);
        if (principal == null) {
            LOG.error(buildConfigErrorMessage(MAPR_REST_PRINCIPAL_KEY));
            this.isValid = false;
            return;
        } else {
            setMaprPrincipal(principal);
        }

        // kerberos keytab file path
        final String keytab = conf.get(MAPR_REST_KEYTAB_KEY, null);
        if (keytab == null) {
            LOG.error(buildConfigErrorMessage(MAPR_REST_KEYTAB_KEY));
            this.isValid = false;
            return;
        } else {
            setMaprKeytabPath(keytab);
        }

        // REST port
        setMaprRestPort(conf.get(MAPR_REST_PORT_KEY, "8443"));

        // main loop sleep interval
        final String interval = conf.get(MAPR_LOOP_INTERVAL_KEY, "60000");
        long i = Long.parseLong(interval);
        if (i < minLoopInterval) {
            LOG.info("sleep interval configured too small, setting minimum of "
                    + minLoopInterval);
            i = minLoopInterval;
        }
        setLoopInterval(i);

        // volume groups config directory
        final String vgconfigdir = conf.get(VG_CONFIG_DIR_KEY,
                "/opt/mapr/volume-manager/conf/vg.d");
        setVgConfigDir(new File(vgconfigdir));

        // FS action attempt number
        final String attempts = conf.get(FS_ACTION_ATTEMPTS, "3");
        long a = Long.parseLong(attempts);
        if (a < 1) {
            LOG.info("number of FS action attempts configured too small, setting minimum of 1");
            a = 1;
        }
        setFsActionAttempts(a);

        // REST throttling interval
        final String rti = conf.get(REST_THROTTLING_INTERVAL, "0");
        long intvl = Long.parseLong(rti);
        if (intvl < 0) {
            LOG.warn(REST_THROTTLING_INTERVAL + " can't be negative number. Throttling will be disabled.");
            intvl = 0;
        }
        setRestThrottlingInterval(intvl);
    }

    /**
     * loads volumes groups by iterating through each file of respective
     * configuration directory
     */
    public void loadVolumeGroups() {

        LOG.info("loading volume groups from directory " +
                getVgConfigDir().getPath());

        // retrieve list of all files in configuration directory
        final File[] files = getVgConfigDir().listFiles();

        if (files == null) {
            LOG.warn("vg config directory " + getVgConfigDir().getPath() +
                    " doesn't exist");
            this.isValid = false;
            return;
        } else {
            LOG.info("a total of " + files.length +
                    " files to load in VG config directory");
        }

        // iterate through each file and load volume group configuration from it
        vgMap = new HashMap<String, VolumeGroupConfiguration>();

        for (File file : files) {

            LOG.info("loading volume from from file " + file.getPath());

            // preserve last modification time
            if (file.lastModified() > lastModified) {
                this.lastModified = file.lastModified();
            }

            try {
                if (file.isFile()) {
                    VolumeGroupConfiguration vgc = null;
                    vgc = loadVolumeGroupConfiguration(file);
                    if (vgc != null) {
                        if (vgMap.get(vgc.getName()) != null) {
                            LOG.warn("duplicate vg name in file " +
                                    file.getPath() + ", discarding this vg");
                        } else {
                            vgMap.put(vgc.getName(), vgc);
                            LOG.info("loaded vg '" + vgc.getName() +
                                    "' from file " + file.getPath());
                        }
                    }
                } else {
                    LOG.warn("skipping " + file.getPath() +
                            " as it is not a file");
                }
            } catch (Exception se) {
                LOG.warn("unable to load file " + file.getPath() + " : " + se);
            }
        }

        LOG.info("loaded " + vgMap.size() + " volume groups from " +
                getVgConfigDir().getPath());
    }

    /**
     * load volume group configuration from properties file
     */
    private VolumeGroupConfiguration loadVolumeGroupConfiguration(File file) {

        LOG.info("loading VG properties from file : " + file);

        // load properties from the config file
        Properties prop = new Properties();
        try {
            FileInputStream fis = new FileInputStream(file);
            prop.load(fis);
            fis.close();
        } catch (IOException e) {
            LOG.error("error when loading properties: " + e);
            return null;
        }

        /**
         * following section validates and sets configuration properties based
         * on what's been loaded from properties file
         */
        VolumeGroupConfiguration vgc = new VolumeGroupConfiguration();

        // cluster name
        vgc.setCluster(prop.getProperty(VG_PROP_CLUSTER, null));
        if (vgc.getCluster() == null) {
            logMissingProperty(VG_PROP_CLUSTER);
            return null;
        }

        // path format
        vgc.setPathFormat(prop.getProperty(VG_PROP_PATHFORMAT));
        if (vgc.getPathFormat() == null) {
            logMissingProperty(VG_PROP_PATHFORMAT);
            return null;
        }

        // check validity of path format
        try {
            DateFormat sdf = new SimpleDateFormat(vgc.getPathFormat());
            sdf.format(System.currentTimeMillis());
            LOG.debug("pathformat validated");
        } catch (IllegalArgumentException iae) {
            logInvalidPropertyWithException(VG_PROP_PATHFORMAT,
                    vgc.getPathFormat(), iae);
            return null;
        }

        vgc.setName(prop.getProperty(VG_PROP_NAME));
        if (vgc.getName() == null) {
            logMissingProperty(VG_PROP_NAME);
            return null;
        }

        // minimum replication factor
        final String mr = prop.getProperty(VG_PROP_MINREPLICATION, "2");

        final int minrep = Integer.valueOf(mr);
        if (minrep <= 0) {
            logInvalidProperty(VG_PROP_MINREPLICATION, mr);
            return null;
        } else {
            vgc.setMinReplication(minrep);
        }

        final String r = prop.getProperty(VG_PROP_REPLICATION, "3");

        // replication factor
        final int rep = Integer.valueOf(r);
        if (rep <= 0) {
            logInvalidProperty(VG_PROP_REPLICATION, r);
            return null;
        } else {
            vgc.setReplication(rep);
        }

        // replication type
        final String reptype = prop.getProperty(VG_PROP_REPLICATIONTYPE,
                "high_throughput");
        if (!"high_throughput".equals(reptype) &&
                !"low_latency".equals(reptype)) {
            logInvalidProperty(VG_PROP_REPLICATIONTYPE, reptype);
            return null;
        } else {
            vgc.setReplicationType(reptype);
        }

        // volume and mount directory owner
        final String owner = prop.getProperty(VG_PROP_OWNER);
        if (owner == null) {
            logMissingProperty(VG_PROP_OWNER);
            return null;
        }

        if (this.getent.getUserByName(owner) == null) {
            LOG.error("unable to retrieve user '" + owner +
                    "' using getent - does user exist?");
            return null;
        } else {
            vgc.setOwner(owner);
        }

        // volume and mount directory group owner
        final String group = prop.getProperty(VG_PROP_GROUP);
        if (group == null) {
            logMissingProperty(VG_PROP_GROUP);
            return null;
        }

        if (this.getent.getGroupByName(group) == null) {
            LOG.error("unable to retrieve group '" + group +
                    "' using getent - does group exist?");
            return null;
        } else {
            vgc.setGroup(group);
        }

        // FS permissiong of the volume mount directory
        vgc.setPermission(prop.getProperty(VG_PROP_PERMISSION, "755"));

        try {
            FsPermission permission = new FsPermission(vgc.getPermission());
            LOG.debug(VG_PROP_PERMISSION + " property validated : " +
                    permission.toString());
        } catch (Exception e) {
            logInvalidPropertyWithException(VG_PROP_PERMISSION,
                    vgc.getPermission(), e);
            return null;
        }

        // accounting entity
        final String ae = prop.getProperty(VG_PROP_AE);
        if (ae == null) {
            logMissingProperty(VG_PROP_AE);
            return null;
        } else {
            vgc.setAe(ae);
        }

        // accounting entity type
        final String aetype = prop.getProperty(VG_PROP_AETYPE, "0");
        final int aeType = Integer.valueOf(aetype);
        if (aeType != 0 && aeType != 1) {
            logInvalidProperty(VG_PROP_AETYPE, aetype);
            return null;
        } else {
            vgc.setAeType(aeType);
        }

        // enable ACE
        final String aceEnabled = prop.getProperty(VG_PROP_IS_ACE_ENABLED, "false");
        if ("true".equals(aceEnabled) || "yes".equals(aceEnabled)) {
            vgc.setAceEnabled(true);   
        } else {
            vgc.setAceEnabled(false);
        }

        // read ACEs
        final String readAce = prop.getProperty(VG_PROP_READ_ACE, "");
        if ((vgc.isAceEnabled() && readAce.isEmpty())) {
            logInvalidProperty(VG_PROP_READ_ACE, readAce);
            return null;
        } else {
            vgc.setReadAce(readAce);
        }

        // write ACEs
        final String writeAce = prop.getProperty(VG_PROP_WRITE_ACE, "");
        if ((vgc.isAceEnabled() && writeAce.isEmpty())) {
            logInvalidProperty(VG_PROP_READ_ACE, writeAce);
            return null;
        } else {
            vgc.setWriteAce(writeAce);
        }

        // volume cluster topology
        final String topology = prop.getProperty(VG_PROP_TOPOLOGY);
        if (topology == null) {
            logMissingProperty(VG_PROP_TOPOLOGY);
            return null;
        } else {
            vgc.setTopology(topology);
        }

        // snapshot schedule
        final String schedule = prop.getProperty(VG_PROP_SCHEDULE, "0");
        final int scheduleId = Integer.valueOf(schedule);
        if (scheduleId < 0) {
            logInvalidProperty(VG_PROP_AETYPE, schedule);
            return null;
        } else {
            vgc.setSchedule(scheduleId);
        }

        // volume creation interval
        final String interval = prop.getProperty(VG_PROP_INTERVAL);
        if (interval == null) {
            logMissingProperty(VG_PROP_INTERVAL);
            return null;
        }

        // validate if configured interval is supported
        if (calIntervalMap.get(interval) == null) {
            logInvalidProperty(VG_PROP_INTERVAL, interval);
            return null;
        } else {
            vgc.setInterval(interval);
        }

        // retention
        final String retention = prop.getProperty(VG_PROP_RETENTION);
        if (retention == null) {
            logMissingProperty(VG_PROP_RETENTION);
            return null;
        }

        // reject negative retention; retention 0 means 'keep forever'
        final int ret = Integer.valueOf(retention);
        if (ret < 0) {
            logInvalidProperty(VG_PROP_RETENTION, retention);
            return null;
        } else {
            vgc.setRetention(ret);
        }

        // ahead factor
        final String ahead = prop.getProperty(VG_PROP_AHEAD);
        if (ahead == null) {
            logMissingProperty(VG_PROP_AHEAD);
            return null;
        }

        // reject negative ahead factor
        int af = Integer.valueOf(ahead);
        if (af < 0) {
            logInvalidProperty(VG_PROP_AHEAD, ahead);
            return null;
        } else {
            vgc.setAheadFactor(af);
        }

        // reject non-zero retention/ahead factor on static volumes
        if (calIntervalMap.get(interval) == Integer.valueOf(Calendar.ERA) && (ret != 0 || af != 0)) {
            LOG.error("VG " + vgc.getName() + " : static volume must have retention and ahead properties set to 0");
            return null;
        }

        return vgc;
    }

    /**
     * populate creational intervals supported by the application
     */
    private void populateCalendarIntervalMap() {

        calIntervalMap.put("day", Integer.valueOf(Calendar.DATE));
        calIntervalMap.put("month", Integer.valueOf(Calendar.MONTH));
        calIntervalMap.put("year", Integer.valueOf(Calendar.YEAR));
        calIntervalMap.put("none", Integer.valueOf(Calendar.ERA));
    }

    /**
     * retrieve creation interval enumeration element
     * returns null is interval is not supported by the application
     */
    public static Integer getCalInterval(String interval) {

        return calIntervalMap.get(interval);
    }

    /**
     * build fatal configuration error message for logging
     */
    private String buildConfigErrorMessage(String key) {

        StringBuilder sb = new StringBuilder();
        sb.append("could not read '");
        sb.append(key);
        sb.append("' from configuration, exiting.");

        return sb.toString();
    }

    /**
     * build missing property message for logging
     */
    private void logMissingProperty(final String prop) {

        LOG.error("missing property: '" + prop + "'");
    }

    /**
     * build invalid property message for logging
     */
    private void logInvalidProperty(final String prop, final String value) {

        LOG.error("invalid value of property '" + prop + "' : " + value);
    }

    /**
     * build invalid property message for logging, including exception data
     */
    private void logInvalidPropertyWithException(final String prop,
            final String value, final Exception e) {

        LOG.error("invalid value of property '" + prop + "' : " + value +
                ", exception : " + e);
    }

    /**
     * check if configuration has been modified since last configuration load
     */
    public boolean hasConfigurationExpired() {

        if (configDir == null) {
            LOG.warn("configuration directory has not been set - application not yet initialized?");
            return false;
        }

        final File[] xmlFiles = configDir.listFiles(new FilenameFilter() {

            /**
             * {@inheritDoc}
             */
            @Override
            public boolean accept(final File dir, final String name) {
                return name.endsWith(".xml");
            }
        });

        if (xmlFiles != null) {
            for (File xmlFile : xmlFiles) {
                // preserve time of last modification
                if (xmlFile.lastModified() > lastModified) {
                    LOG.info("Configuration file is new or updated: " + xmlFile.getPath());
                    return true;
                }
            }
        }

        // retrieve list of all files in configuration directory
        final File[] files = getVgConfigDir().listFiles();
        
        for (File file : files) {
            if (file.lastModified() > lastModified) {
                LOG.info("Configuration file is new or updated: " + file.getPath());
                return true;
            }
        }

        return false;
    }
}
