package volumes;

import javax.security.auth.Subject;
import java.security.PrivilegedAction;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.URL;
import java.net.URLEncoder;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.InputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;
import java.util.Iterator;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.mapr.fs.MapRFileSystem;
import org.apache.hadoop.fs.FileSystem;
import java.net.URI;

/**
 * VolumeManager main application class
 * Logic flow:
 * (1) read options, load and validate configuration
 * (2) enter main loop, authenticate through kerberos
 * (3)   retrieve current volume list using MapR REST API and convert it to
 *       the list of MapR volumes
 * (4)   build volume actions (create/purge/...) by applying configuration rules
 *       upon the list obtained in step (3)
 * (5)   execute volume actions one by one
 * (6)   check if kerberos ticket expired, goto step (2) if the case
 * (7)   sleep and go to step (3)
 */
class VolumeManager {

    /**
     * Volume Manager configuration
     */
    private static volatile VolumeManagerConfiguration vmconf = null;

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(VolumeManager.class);

    /**
     * Configuration file name
     */
    private static volatile File configDir = null;

    /**
     * local hostname
     */
    private static volatile String localHostName = null;

    /**
     * REST node index for internal array
     */
    private static int restNodeIndex = 0;

    /**
     * REST failure counter
     */
    private static int restFailCount = 0;

   /**
    * maximum repeating REST errors before alarm is raised
    */
    private static int MAX_REPEATING_REST_ERRORS = 3;

    /**
     * shutdown flag
     */
    private static boolean shutdown = false;

    /**
     * volume action manager
     */
    private static VolumeActionManager vam = new VolumeActionManager();

    /**
     * volume list retrieval alarm
     */
    private static final String MAPR_ALARM_KEY = "NODE_ALARM_SERVICE_VOLUME-MANAGER_DOWN";

    /**
     * main application function
     */
    public static void main(final String[] args) {

        LOG.info("Starting Volume Manager");

        // setting up hostname for MapR alarms
        setHostName();

        // process command line options
        LOG.info("processing command line options ...");
        if (!processCommandLineOptions(args)) {

            LOG.fatal("failure in processing command line options, exiting.");
            return;
        }

        // initialize application configuration
        initConf(configDir);

        if (!vmconf.isValid()) {
            LOG.fatal("configuration is invalid, check errors. exiting.");
            return;
        }

        // set reference to applicaiton config for action manager
        vam.setVMConf(vmconf);

        // enter main loop
        runMainLoop();

        // cleanup and exit
        cleanup();

        return;
    }

    /**
     * set local hostname, used for MapR alarms
     */
    private static void setHostName() {

        try {
            InetAddress addr = InetAddress.getLocalHost();
            localHostName = addr.getCanonicalHostName();
            LOG.info("retrieved local hostname : " + localHostName);
        } catch (UnknownHostException e) {
            LOG.error("unable to set local hostname, MapR alarms will be set cluster-wide : "
                    + e.getMessage());
        }
    }

    /**
     * get local hostname
     */
    public static String getHostName() {
        return localHostName;
    }

    /**
     * initializes configuration from a given directory
     */
    public static void initConf(File configDir) {

        LOG.info("loading configuration from " + configDir.getAbsolutePath() +
                " ...");
        vmconf = new VolumeManagerConfiguration(configDir);
    }

    /**
     * reload configuration
     */
    public static void reloadConf() {
        LOG.info("re-loading configuration");
        initConf(configDir);
    }

    /**
     * sets configuration
     */
    public static void setConf(VolumeManagerConfiguration conf) {
        vmconf = conf;
    }

    /**
     * processes command line options
     */
    private static boolean processCommandLineOptions(final String[] args) {

        final Options options = new Options();
        final Option confDirOpt = Option.builder("c").longOpt("configDir")
                    .hasArg(true).desc("Specifies the configuration directory")
                    .required(true).build();

        options.addOption(confDirOpt);

        final CommandLineParser parser = new DefaultParser();
        final CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException pe) {
            LOG.error(pe.getMessage());
            return false;
        }

        // Check if the configured configuration directory exists
        configDir = new File(cmd.getOptionValue("c"));
        if (!configDir.exists() || !configDir.isDirectory()) {
                LOG.error("Configuration directory "
                        + configDir.getAbsolutePath()
                        + " does not exist or is not a directory, exiting...");
                return false;
       }
       LOG.info("specified configuration directory : " +
               configDir.getAbsolutePath());
       return true;
    }

    /**
     * create login context using kerberos principal name and keytab file from
     * the configuration
     *
     * returns null if authentication failed
     */
    private LoginContext loginWithKerberos() {

        LOG.info("attempting authentication with principal="
                + vmconf.getMaprPrincipal()
                + ", keytab=" + vmconf.getMaprKeytabPath());

        // create security configuration
        final SecurityConfiguration sc =
                new SecurityConfiguration(vmconf.getMaprPrincipal(),
                vmconf.getMaprKeytabPath(), true);

        // try login
        LoginContext lc = null;
        try {
            lc = new LoginContext("VolumeManager", null, null, sc);
            lc.login();
        } catch (LoginException e) {
            LOG.fatal("authentication failed : " + e);
            return null;
        }

        LOG.info("authentication successful");
        return lc;
    }

    /**
     * this method is used to switch target REST node in case of failure for
     * fault tolerance
     */
    public static void failoverRestNode() {
        final String currentNode = getRestNode();
        restNodeIndex = (restNodeIndex + 1) % vmconf.getRestNodes().size();
        LOG.info("failed over REST node from " + currentNode + " to "
                + getRestNode());
    }

    /**
     * retrieve REST node name to target
     */
    private static String getRestNode() {
        return vmconf.getRestNodes().get(restNodeIndex);
    }

    /**
     * retrieve REST endpoint to target, in format 'https://host:port'
     */
    public static String getRestEndPoint() {
        StringBuilder sb = new StringBuilder();
        sb.append("https://");
        sb.append(getRestNode());
        sb.append(":");
        sb.append(vmconf.getMaprRestPort());

        return sb.toString();
    }

    /**
     * main application loop
     */
    private static void runMainLoop() {

        LOG.info("running main application loop");

        while (shutdown != true) {

            // instantiate volume manager and login
            LOG.info("attempting kerberos login");
            final VolumeManager volumeManager = new VolumeManager();
            LoginContext lc = volumeManager.loginWithKerberos();

            // sleep and re-iterate the loop if login failed
            if (lc == null) {
                doSleep();
                continue;
            }

            // enter main loop
            final Subject subject = lc.getSubject();
            runPrivileged(subject);
        }
    }

    /**
     * retrieve volume data using MapR REST interface
     */
    private static InputStream retrieveVolumeData() {

        LOG.info("retrieving MapR volume data");

        // build REST URL
        InputStream is = null;
        URL maprUrl = null;
        try {
            maprUrl = new URL(VolumeManager.getRestEndPoint() +
                    "/rest/volume/list");
        } catch (MalformedURLException e) {
            LOG.error("malformed URL generated internally: " +
                    VolumeManager.getRestEndPoint() + "/rest/volume/list");
            return is;
        }

        // execute REST call
        try {
            LOG.info("calling URL " + maprUrl.toString());
            is = maprUrl.openConnection().getInputStream();
            // reset REST failure counter
            restFailCount = 0;

        } catch (IOException e) {
            LOG.error("REST call error: " + e);
            // switch target REST node for next attempt
            failoverRestNode();
            restFailCount++;
            if (restFailCount >= MAX_REPEATING_REST_ERRORS) {
                raiseAlarm("Volume Manager failed to retrieve volume list more than "
                        + MAX_REPEATING_REST_ERRORS + " times");
            }
        }

        return is;
    }

    /**
     * convert REST input JSON data into list of volumes currently listed on
     * the MapR cluster
     */
    private static void processVolumeData(InputStream is) {

        LOG.info("processing MapR volume data");

        try {
            List<MaprVolume> volumes = MaprVolumeParser.parse(is);
            LOG.info("retrieved " + volumes.size() +
                    " volume items from REST input stream");
            if (volumes.size() != 0) {
                vam.prepare(vmconf, volumes);
                vam.execute();
            }
        } catch (IOException e) {
            LOG.error("error when reading from input stream: " + e);
        }
    }

    /**
     * Checks if at least one of the subject's Kerberos tickets have expired.
     * 
     * @param subject
     *            the subject whose tickets shall be checked
     * @param now
     *            the current timestamp
     * @return <code>true</code> if at least one of the subject's Kerberos
     *         tickets have expired, <code>false</code> otherwise
     */
    private static boolean hasTicketExpired(final Subject subject,
            final long now) {

        final Set<Object> creds = subject.getPrivateCredentials();
        for (final Iterator<Object> it = creds.iterator(); it.hasNext();) {
            final Object cred = it.next();
            if (cred instanceof KerberosTicket) {
                final KerberosTicket ticket = (KerberosTicket) cred;
                final long endTime = ticket.getEndTime().getTime();
                if (endTime < now) {
                    LOG.info("Kerberos ticket expired");
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * cleanup function
     */
    private static void cleanup() {

        LOG.info("cleaning up ...");
    }

    /**
     * helper function to sleep for configured period of time
     */
    private static void doSleep(Date start) {

        Date now = new Date(System.currentTimeMillis());

        try {
            long tts = vmconf.getLoopInterval() - (now.getTime() -
                    start.getTime());
            LOG.info("sleeping " + tts/1000 + " sec ...");
            if (tts > 0) {
                Thread.sleep(vmconf.getLoopInterval() );
            }
        } catch (InterruptedException ie) {
            LOG.error("error in Thread.sleep() : " + ie.getMessage());
        }
    }

    /**
     * helper function to sleep for configured period of time
     */
    private static void doSleep() {

        try {
            long tts = vmconf.getLoopInterval();
            LOG.info("sleeping " + tts/1000 + " sec ...");
            if (tts > 0) {
                Thread.sleep(vmconf.getLoopInterval() );
            }
        } catch (InterruptedException ie) {
            LOG.error("error in Thread.sleep() : " + ie.getMessage());
        }
    }

    /**
     * main loop function executed in kerberos login context
     */
    private static void runPrivileged(final Subject subject) {

        Subject.doAs(subject, new PrivilegedAction<Void>() {

            @Override
            public Void run() {

                runPrivilegedLoop(subject);
                return null;

            }
        });
    }

    /**
     * main loop body executed in kerberos login context
     */
    private static void runPrivilegedLoop(Subject subject) {

        while (true) {
            if (shutdown) {
                return;
            }

            // check if kerberos ticket has expired
            if (hasTicketExpired(subject, System.currentTimeMillis())) {
                return;
            }

            // check if configuration has expired
            if (vmconf.hasConfigurationExpired()) {
                LOG.info("configuration will be reloaded");
                reloadConf();
                vam.setVMConf(vmconf);
            }

            Date start = new Date(System.currentTimeMillis());
            InputStream is = retrieveVolumeData();

            if (is != null) {
                processVolumeData(is);
            } else {
                LOG.error("error while retrieving cluster volume data");
            }

            // clear config reload flag
            if (vmconf.hasConfigReloaded()) {
                vmconf.clearConfigReloaded();
            }

            doSleep(start);
        }
    }
    /**
     * raise cluster alarm
     */
    public static void raiseAlarm(String description) {

        LOG.info("raising MapR alarm with REST call: " + MAPR_ALARM_KEY + ", description: [" + description + "]");

        // build REST URL
        URL restUrl = null;
        StringBuilder url = new StringBuilder();

        try {
            url.append(VolumeManager.getRestEndPoint());
            url.append("/rest/alarm/raise?alarm=");
            url.append(MAPR_ALARM_KEY);
            url.append("&entity=");
            url.append(getHostName());
            url.append("&description=");
            url.append(URLEncoder.encode(description, "UTF-8"));
            restUrl = new URL(url.toString());
        } catch (MalformedURLException e) {
            LOG.error("malformed URL generated internally: " + url.toString());
            return;
        } catch (UnsupportedEncodingException uee) {
            LOG.error("Can not encode plain URL '" + url.toString() + "' into REST");
            return;
        }

        // execute REST call
        try {
            LOG.info("calling URL " + restUrl.toString());
            restUrl.openConnection().getInputStream();
        } catch (IOException e) {
            LOG.error("REST call error: " + e);
            // switch target REST node for next attempt
            failoverRestNode();
        }

        return;
    }
}
