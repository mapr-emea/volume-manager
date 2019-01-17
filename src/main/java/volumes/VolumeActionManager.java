package volumes;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Calendar;
import java.util.Date;
import java.text.SimpleDateFormat;

import java.net.URL;
import java.net.URI;
import java.io.InputStream;
import java.net.MalformedURLException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import java.io.IOException;
import java.io.File;

import com.mapr.fs.MapRFileAce;
import com.mapr.fs.MapRFileSystem;

/** 
 * Basic idea is to generate set of volumes that should be in place according to
 * configuration. By comparing target set with an actual set of volumes it is
 * then possible to create lists of volumes that are to be created or purged.
 */
class VolumeActionManager {

    /**
     * logger for troublsehooting and debugging
     */
    private static final Log LOG = LogFactory.getLog(VolumeActionManager.class);

    /**
     * FS action retry delay
     */
    private final long FS_ACTION_RETRY_DELAY = 5000;

    /**
     * reference to application configuration
     */
    private VolumeManagerConfiguration vmConf;

    /**
     * target map of volumes, regenerated on each main loop iteration
     */
    HashMap<String,MaprVolume> targetVolMap = new HashMap<String,MaprVolume>();

    /**
     * lists of volumes that are to be created or purged
     */
    ArrayList<MaprVolume> createList = new ArrayList<MaprVolume>();
    ArrayList<MaprVolume> purgeList = new ArrayList<MaprVolume>();
    ArrayList<MaprVolume> aceModList = new ArrayList<MaprVolume>();

    /**
     * public ACE string
     */
    final String publicAce = new String("p");

    /**
     * Array containing ACEs to be set for the volume
     */
    ArrayList<MapRFileAce> publicAces = null;

    /**
     * Native MapR FS for ACE operations
     */
    FileSystem fs = null;

    /**
     * MapR-FS designator
     */
    private final String MAPRFS_URI = "maprfs:///";

    /**
     * retrieve list of volumes to be created
     */
    public final List<MaprVolume> getCreateList() {
        return createList;
    }

    /**
     * retrieve list of volumes to be purged
     */
    public final List<MaprVolume> getPurgeList() {
        return purgeList;
    }

    /**
     * retrieve list of volumes which are subject for ACE modification
     */
    public final List<MaprVolume> getAceModList() {
        return aceModList;
    }

    /**
     * set reference to volume manager configuration
     */
    public void setVMConf(VolumeManagerConfiguration vmconf) {
        this.vmConf = vmconf;
    }

    /**
     * get reference to volume manager configuration
     */
    public VolumeManagerConfiguration getVMConf() {
        return vmConf;
    }

    /**
     * Contructor
     */
    VolumeActionManager() {
        try {
            publicAces = buildPublicAces();
        } catch (IOException ioe) {
            LOG.error("Failed to build array of public ACEs, will keep null value");
        }
    }

    /**
     * loops through configured VGs to populate target map of volumes
     */
    public void prepare(VolumeManagerConfiguration vmconf, 
            List<MaprVolume> volumes) {

        LOG.info("preparing volume actions");

        // clear data from previous iteration
        targetVolMap.clear();
        createList.clear();
        purgeList.clear();
        aceModList.clear();

        final Map<String, VolumeGroupConfiguration> vgMap = vmconf.getVgMap();

        for (VolumeGroupConfiguration vgc : vgMap.values()) {
            populateTargetMapFromVg(vgc);
        }

        LOG.info("generated target volume map, size=" + targetVolMap.size());

        // initialize the FileSystem if not yet done
        if (fs == null) {
            fs = getMapRFS();

            if (fs == null) {
                LOG.error("Can't obtain MapRFS handle, preparing volume actions aborted.");
                return;
            }
        }

        genDeltaLists(vgMap, volumes);

        LOG.info("finished preparing volume actions : purge=" + 
                purgeList.size() + " create=" + createList.size() +
                " aceMod=" + aceModList.size());
    }

    /**
     * populates target map of volumes based on VG configuration
     */
    private void populateTargetMapFromVg(VolumeGroupConfiguration vgc) {

        final Integer interval =
                VolumeManagerConfiguration.getCalInterval(vgc.getInterval());
        final Date now = new Date();

        // generate the list of volumes (retent, current and ahead)
        for (int i=-vgc.getRetention(); i<=vgc.getAheadFactor(); i++) {

            String appender = this.getVolumeNameAppender(now, interval.intValue(), i);

            StringBuilder sb = new StringBuilder();
            sb.append(vgc.getName());

            if (!appender.isEmpty()) {
                sb.append("_");
                sb.append(appender);
            }

            LOG.info("adding volume to target map: " + sb.toString());
            targetVolMap.put(sb.toString(), new MaprVolume(vgc, appender));
        }
    }

    /**
     * Helper function that retrieves the date in the desired format to be used
     * in the actual volume name:
     * - the date of the day for daily volumes
     * - first day of the month for monthly volumes
     * - first day of the year for yearly volumes
     */
    private String getVolumeNameAppender(Date now, int interval, int offset) {

        SimpleDateFormat sdf = null;

        switch (interval) {
            case Calendar.MONTH:
                sdf = new SimpleDateFormat("yyyyMM01");
                break;
            case Calendar.YEAR:
                sdf = new SimpleDateFormat("yyyy0101");
                break;
            case Calendar.ERA:
                return new String();
            default:
                sdf = new SimpleDateFormat("yyyyMMdd");
                break;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(interval, offset);
        return sdf.format(cal.getTime());
    }

    /**
     * generate create/purge lists based on actual volume list
     */
    public void genDeltaLists(Map<String, VolumeGroupConfiguration> vgMap, 
            List<MaprVolume> volumes) {

        LOG.info("generating delta lists");

        // list of volumes will be converted into the map for create lookups
        HashMap<String, MaprVolume> volMap = new HashMap<String, MaprVolume>();

        // purge and ACE modification lists generation loop
        for (MaprVolume vol : volumes) {

            volMap.put(vol.getName(), vol);

            // check if the volume is relevant for automation
            final String name = vol.getName().replaceAll("_\\d*$", "");
            final VolumeGroupConfiguration vgc = vgMap.get(name);
            if (vgc == null) {
                LOG.debug("volume " + name + " / " + vol.getName() + " is not relevant for automation");
                continue;
            }

            LOG.info("volume " + vol.getName() + " is relevant for automation");

            // retrieve configured volume instance for ACE mod check
            MaprVolume configuredVol = targetVolMap.get(vol.getName());

            // enforce ACE mod check on retention=0 volume
            if (configuredVol == null && vgc.getRetention() == 0) {
                String[] splits = vol.getName().split("_");
                configuredVol = new MaprVolume(vgc, splits[splits.length - 1]);
            }

            // this volume is either in target map or retention=0, hence doesn't need to be purged
            // only check ACE mod and skip over
            if (configuredVol != null) {
                // only check ACE mods on startup / config modification event
                if (vgc.isAceEnabled() && vmConf.hasConfigReloaded()) {
                    if (isAceToBeModified(configuredVol, vgc)) {
                        aceModList.add(configuredVol);
                    }
                }
                continue;
            }

            if (vgc.getRetention() == 0) {
                LOG.info("retention of " + vol.getName() + 
                        " is forever, skipping");
                continue;
            }

            // check if volume is to be purged
            if (isToBePurged(vol)) {
                purgeList.add(vol);
                LOG.info("added volume to purge list: " + vol.getName());
            } else {
                LOG.info("volume " + vol.getName() + " is ahead, skipping");
            }
        }

        // create list generation loop
        for (Map.Entry<String, MaprVolume> entry : targetVolMap.entrySet()) {
            final MaprVolume v = volMap.get(entry.getKey());
            if (v == null) {
                createList.add(entry.getValue());
                LOG.info("added volume to create list: " + entry.getKey());
            }
        }
    }

    /**
     * function to check if volume is to be purged
     */
    private boolean isToBePurged(MaprVolume vol) {

        final String[] splits = vol.getName().split("_");
        final String date = splits[splits.length-1];
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(new Date());
        final String now = sdf.format(cal.getTime());
        return (Integer.valueOf(now) > Integer.valueOf(date));
    }

    /**
     * executes actions on purge/create lists
     */
    public void execute() {

        // return immediately if nothing to do
        if (purgeList.size() + createList.size() + aceModList.size() == 0) {
            LOG.info("no pending volume actions");
            return;
        }

        LOG.info("executing volume actions");

        // purge
        for (MaprVolume vol : purgeList) {
            this.purge(vol);
        }

        // create
        for (MaprVolume vol : createList) {
            this.create(vol);
        }
        
        // ACE modification
        for (MaprVolume vol : aceModList) {
            try {
                this.setAces(vol, true);
            } catch (IOException ioe) {
                LOG.error("Error when modifying ACE of volume " + vol.getName() + " " + ioe);
            }
        }

        LOG.info("finished executing volume actions");
    }

    /**
     * volume action : purge one volume
     */
    private void purge(MaprVolume volume) {

        LOG.info("purging volume " + volume.getName());

        final String rep = VolumeManager.getRestEndPoint();

        // build REST URL
        StringBuilder sb = new StringBuilder();
        sb.append(rep);
        sb.append("/rest/volume/remove?name=");
        sb.append(volume.getName());

        if (callRest(sb.toString())) {
            LOG.info("purged volume " + volume.getName());
        } else {
            LOG.error("error when purging volume " + volume.getName());
        }
    }

    /**
     * volume action : create one volume
     */
    private void create(MaprVolume volume) {

        LOG.info("creating volume " + volume.getName() + " on path " + 
                volume.getPath());

        // ensure base directory for mounting the volume
        this.ensureParentDirectory(volume.getPath());

        // build and call REST URL
        final String vcUrl = buildVolumeCreateURL(volume);

        if (callRest(vcUrl)) {
            // set FS ownership and permission on success
            setOwnershipAndPerm(volume);
        }
    }

    /**
     * function implementing REST call
     */
    private boolean callRest(String surl) {

        URL url = null;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            LOG.error("malformed URL : " + surl + " : " + e);
            return false;
        }

        // execute REST and validate the response
        InputStream is = null;
        try {
            LOG.info("calling URL " + url.toString());
            is = url.openConnection().getInputStream();
            if (MaprRestParser.getResponseStatus(is)) {
                LOG.info("REST call successful");
                return true;
            } else {
                VolumeManager.raiseAlarm("REST error response for URL " + surl);
            }
        } catch (Exception e) {
            LOG.error("exception when calling URL " + url.toString() + " : " 
                    + e);
            // switch target REST node for next attempt
            VolumeManager.failoverRestNode();
            VolumeManager.raiseAlarm("REST error when calling URL " + surl);
        }
        return false;
    }

    /**
     * function implementing REST call
     */
    private List<String> getAcesWithRest(String surl) {

        if (vmConf.getRestThrottlingInterval() != 0) {
            try {
                Thread.sleep(vmConf.getRestThrottlingInterval());
            } catch (InterruptedException ie) {
                LOG.warn("Thread.sleep() interrupted: " + ie);
            }
        }

        URL url = null;
        try {
            url = new URL(surl);
        } catch (MalformedURLException e) {
            LOG.error("malformed URL : " + surl + " : " + e);
            return null;
        }

        // execute REST and validate the response
        InputStream is = null;
        List<String> aces = null;
        try {
            LOG.info("calling URL " + url.toString());
            is = url.openConnection().getInputStream();
            aces = MaprAceParser.parse(is);
        } catch (Exception e) {
            LOG.error("exception when calling URL " + url.toString() + " : "
                    + e);
            // switch target REST node for next attempt
            VolumeManager.failoverRestNode();
            VolumeManager.raiseAlarm("REST error when calling URL " + surl);
        }

        return aces;
    }

    /**
     * ensures volume mount base directory
     */
    private boolean ensureParentDirectory(String path) {

        LOG.info("ensuring volume mount base directory");

        boolean parentDirectoryStatus = false;
        long maxAttempts = vmConf.getFsActionAttempts();

        // execute FS actions
        for (long a = 1; a <= maxAttempts; a++) {
            LOG.info("DFS operation batch attempt " + a + " of " + maxAttempts);
            try {
                final File file = new File(path);
                final String dir = file.getParent();

                if (!fs.exists(new Path(dir))) {
                    LOG.info("creating directory " + dir);
                    parentDirectoryStatus = fs.mkdirs(new Path(dir), 
                            new FsPermission("755"));
                } else {
                    LOG.info("parent directory " + dir + " exists");
                    parentDirectoryStatus = true;
                }
                LOG.info("ensured volume mount base directory");
                break;
            } catch (IOException ie) {
                LOG.error("failure performing MapR-FS operation: " + ie);
                if (a == maxAttempts) {
                    VolumeManager.raiseAlarm("FS operation failure on " + path);
                } else {
                    LOG.info("Sleeping 5 msec before next attempt ...");
                    try {
                        Thread.sleep(FS_ACTION_RETRY_DELAY);
                    } catch (InterruptedException inte) {
                        LOG.info("caught interrupted exception" + inte);
                    }
                }
            }
        }

        return parentDirectoryStatus;
    }

    /**
     * sets directory ownership and permissions
     */
    private void setOwnershipAndPerm(MaprVolume volume) {

        LOG.info("changing ownership of " + volume.getPath() + " to " + 
                volume.getOwner() + ":" + volume.getGroup() + 
                " and permission to " + volume.getPermission());

        long maxAttempts = vmConf.getFsActionAttempts();

        // execute FS actions
        for (long a = 1; a <= maxAttempts; a++) {
            LOG.info("DFS operation batch attempt " + a + " of " + maxAttempts);
            try {
                fs.setOwner(new Path(volume.getPath()), volume.getOwner(), 
                        volume.getGroup());
                LOG.info("changed ownership of " + volume.getPath() + " to " + 
                        volume.getOwner() + ":" + volume.getGroup());
                fs.setPermission(new Path(volume.getPath()), 
                        new FsPermission(volume.getPermission()));
                LOG.info("changed permission of " + volume.getPath() + " to " + 
                        volume.getPermission());
                if (volume.isAceEnabled()) {
                    setAces(volume, false);
                }
                break;
            } catch (Exception ie) {
                LOG.error("failure performing MapR-FS operation: " + ie);
                if (a == maxAttempts) {
                    VolumeManager.raiseAlarm("FS operation failure on " + volume.getPath());
                } else {
                    LOG.info("Sleeping 5 sec before next attempt ...");
                    try {
                        Thread.sleep(FS_ACTION_RETRY_DELAY);
                    } catch (InterruptedException inte) {
                        LOG.info("caught interrupted exception" + inte);
                    }
                }
            }
        }
    }

    /**
     * set MapR ACE on the volume
     */
    private void setAces(MaprVolume volume, boolean withRest) throws IOException {

        if (!volume.isAceEnabled()) {
            LOG.warn("ACE setting attempt on a volume with no ACE enabled. Ignoring.");
            return;
        }

        boolean result = true;

        LOG.info("Setting ACEs on " + volume.getPath() + " [readAce='" + volume.getReadAce() +
                "' writeAce='" + volume.getWriteAce() + "']");

        // modify volume via REST for whole volume ACEs if requested
        if (withRest) {
            String url = buildVolumeAceModURL(volume);
            result = callRest(url);
        }

        // set public ACEs on FS level only if whole volume ACEs setting was OK
        if (result) {
            try {
                ((MapRFileSystem)fs).setAces(new Path(volume.getPath()), this.publicAces);
                LOG.info("Setting public ACEs on FS level successful");
            } catch (IOException ioe){
                LOG.error("Error when setting MapR ACEs");
                throw ioe;
            }
        } else {
            LOG.info("FS level ACE setting skipped due to whole volume REST setting failure.");
        }
    }

    /**
     * builds URL string for ACE modification of the whole volume
     */
    public String buildVolumeAceModURL(MaprVolume volume) {
    
        // build REST URL
        StringBuilder sb = new StringBuilder();
        sb.append(VolumeManager.getRestEndPoint());
        sb.append("/rest/volume/modify?");

        sb.append("name=");
        sb.append(volume.getName());

        sb.append("&readAce=");
        sb.append(volume.getReadAce().replaceAll("&", "%26").replaceAll(" ", "%20"));
        
        sb.append("&writeAce=");
        sb.append(volume.getWriteAce().replaceAll("&", "%26").replaceAll(" ", "%20"));

        return sb.toString();
    }

    /** 
     * builds URL string for volume creation
     */
    public String buildVolumeCreateURL(MaprVolume volume) {

        // make slashes HTTP-compatible
        final String restPath = volume.getPath().replaceAll("/", "%2F");

        // build REST URL
        StringBuilder sb = new StringBuilder();
        sb.append(VolumeManager.getRestEndPoint());
        sb.append("/rest/volume/create?");

        sb.append("name=");
        sb.append(volume.getName());

        sb.append("&path=");
        sb.append(restPath);

        sb.append("&user=");
        sb.append(volume.getOwner());

        sb.append("&group=");
        sb.append(volume.getGroup());

        sb.append("&ae=");
        sb.append(volume.getAe());

        sb.append("&aetype=");
        sb.append(volume.getAeType());

        sb.append("&topology=");
        sb.append(volume.getTopology());

        if (volume.getSchedule() > 0) {
            sb.append("&schedule=");
            sb.append(volume.getSchedule());
        }

        sb.append("&minreplication=");
        sb.append(volume.getMinReplication());

        sb.append("&replication=");
        sb.append(volume.getReplication());

        sb.append("&replicationtype=");
        sb.append(volume.getReplicationType());

        // use whole volume ACEs is enabled
        if (volume.isAceEnabled()) {
            sb.append("&readAce=");
            sb.append(volume.getReadAce().replaceAll("&", "%26").replaceAll(" ","%20"));
            sb.append("&writeAce=");
            sb.append(volume.getWriteAce().replaceAll("&","%26").replaceAll(" ","%20"));
        }

        return sb.toString();
    }

    /**
     * initialize MapR FS
     */
    private FileSystem getMapRFS() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", MAPRFS_URI);
        conf.set("fs.maprfs.impl", "com.mapr.fs.MapRFileSystem");

        try {
            LOG.info("Initializing MapR Native FS ...");
            FileSystem filesys = FileSystem.get(URI.create(MAPRFS_URI), conf);
            LOG.info("MapR Native FS initialization successful");
            return filesys;
        } catch (IOException ioe){
            LOG.error("Failure when initializing MapR FS : " + ioe);
            return null;
        }
    }

   /**
     * initialize ACE array
     */
    private ArrayList<MapRFileAce> buildPublicAces() throws IOException {

        ArrayList<MapRFileAce> aces = new ArrayList<MapRFileAce>();

        MapRFileAce ace = new MapRFileAce(MapRFileAce.AccessType.READFILE);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.WRITEFILE);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.EXECUTEFILE);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.READDIR);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.ADDCHILD);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.DELETECHILD);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        ace = new MapRFileAce (MapRFileAce.AccessType.LOOKUPDIR);
        ace.setBooleanExpression(publicAce);
        aces.add(ace);

        return aces;
    }

    // Check if ACE has to be modified due to configuration change
    private boolean isAceToBeModified(MaprVolume volume, VolumeGroupConfiguration vgconf) {

        boolean result = false;
        List<String> aces = null;

        // retrieve pair of whole volume ACEs via REST
        try {
            aces = this.getAcesWithRest(buildVolumeInfoURL(volume));
            LOG.info("readAce on volume " + volume.getName() + " : [" + aces.get(0) + "]");
            LOG.info("writeAce on volume " + volume.getName() + " : [" + aces.get(1) + "]");
        } catch (Exception e) {
             LOG.error("Error when parsing JSON input on volume info data: " + e);
        }

        // enforce ACE setting in case ACEs are configured but not set on the volume
        if (aces == null) {
            LOG.info("ACE change will be enforced on the volume " + volume.getName());
            return true;
        }

        // MapR may add / remove spaces into expressions - always remove all spaces for comparison
        String readAce = aces.get(0).replaceAll("\\s+","");
        String writeAce = aces.get(1).replaceAll("\\s+","");

        if (!readAce.equals(volume.getReadAce().replaceAll("\\s+",""))) {
            LOG.info("Read ACE configuration changed on the volume " + volume.getName() +
                    " [current='" + readAce + "' configured='" + volume.getReadAce().replaceAll("\\s+","") + "']");
            result = true;
        }

        if (!writeAce.equals(volume.getWriteAce().replaceAll("\\s+",""))) {
            LOG.info("Write ACE configuration changed on the volume " + volume.getName() +
                    " [current='" + writeAce + "' configured='" + volume.getWriteAce().replaceAll("\\s+","") + "']");
            result = true;
        }

        return result;
    }

    /**
     * builds URL string for volume creation
     */
    public String buildVolumeInfoURL(MaprVolume volume) {

        // make slashes HTTP-compatible
        final String restPath = volume.getPath().replaceAll("/", "%2F");

        // build REST URL
        StringBuilder sb = new StringBuilder();
        sb.append(VolumeManager.getRestEndPoint());
        sb.append("/rest/volume/info?");

        sb.append("name=");
        sb.append(volume.getName());

        return sb.toString();
    }

}
