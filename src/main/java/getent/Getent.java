package getent;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class wraps the Unix command getent to retrieve entries from the name
 * service switch libraries.
 */
public final class Getent {

    private static final class Directory {

        private final Map<String, User> usersByName;

        private final Map<Integer, User> usersByUID;

        private final Map<String, Group> groupsByName;

        private final Map<Integer, Group> groupsByGID;

        private Directory(final Map<String, User> usersByName,
                Map<Integer, User> usersByUID,
                final Map<String, Group> groupsByName,
                Map<Integer, Group> groupsByGID) {

            this.usersByName = Collections.unmodifiableMap(usersByName);
            this.usersByUID = Collections.unmodifiableMap(usersByUID);
            this.groupsByName = Collections.unmodifiableMap(groupsByName);
            this.groupsByGID = Collections.unmodifiableMap(groupsByGID);
        }
    }

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(Getent.class);

    /**
     * The default character set to be used for byte to character conversions.
     */
    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();

    /**
     * The default caching period in seconds.
     */
    private static final int DEFAULT_CACHING_PERIOD = 60;

    /**
     * The default command for getent group.
     */
    private static final String DEFAULT_GETENT_GROUP_CMD = "getent group";

    /**
     * The default command for getent passwd.
     */
    private static final String DEFAULT_GETENT_PWD_CMD = "getent passwd";

    /**
     * The caching period of entries in milliseconds.
     */
    private final long cachingPeriodInMilliSeconds;

    /**
     * The command to execute to retrieve the getent group information.
     */
    private final String getentGroupCmd;

    /**
     * The command to execute to retrieve the getent passwd information.
     */
    private final String getentPasswdCmd;

    /**
     * The cached directory information.
     */
    private Directory cachedDirectory = null;

    /**
     * The timestamp when the directory was cached.
     */
    private long cacheTimestamp = 0L;

    /**
     * Constructs a new getent wrapper.
     * 
     * @param cachingPeriod
     *            the period of time to cache retrieved directory entries in
     *            seconds
     * @param getentGroupCmd
     *            the command to execute for getent group
     * @param getentPasswdCmd
     *            the command to execute for getent passwd
     */
    public Getent(final int cachingPeriod, final String getentGroupCmd,
            final String getentPasswdCmd) {

        if (cachingPeriod < 0) {
            throw new IllegalArgumentException(
                    "Value for argument cachingPeriod must be 0 or larger");
        }

        this.cachingPeriodInMilliSeconds = cachingPeriod * 1000L;
        this.getentGroupCmd = getentGroupCmd;
        this.getentPasswdCmd = getentPasswdCmd;
    }

    /**
     * Constructs a new getent wrapper.
     * 
     * @param cachingPeriod
     *            the period of time to cache retrieved directory entries in
     *            seconds
     */
    public Getent(final int cachingPeriod) {
        this(cachingPeriod, DEFAULT_GETENT_GROUP_CMD, DEFAULT_GETENT_PWD_CMD);
    }

    /**
     * Constructs a new getent wrapper.
     */
    public Getent() {
        this(DEFAULT_CACHING_PERIOD, DEFAULT_GETENT_GROUP_CMD,
                DEFAULT_GETENT_PWD_CMD);
    }

    /**
     * Returns the user with the given name.
     * 
     * @param name
     *            the name of the user to look up
     * @return the user with the given name or <code>null</code> if no such user
     *         exists
     */
    public User getUserByName(final String name) {

        final Directory directory = getDirectory();
        if (directory == null) {
            return null;
        }

        return directory.usersByName.get(name);
    }

    /**
     * Returns the user with the given ID.
     * 
     * @param uid
     *            the ID of the user to look up
     * @return the user with the given ID or <code>null</code> if no such user
     *         exists
     */
    public User getUserByUID(final int uid) {

        final Directory directory = getDirectory();
        if (directory == null) {
            return null;
        }

        return directory.usersByUID.get(Integer.valueOf(uid));
    }

    /**
     * Returns the group with the given name.
     * 
     * @param name
     *            the name of the group to look up
     * @return the group with the given name or <code>null</code> if no such
     *         group exists
     */
    public Group getGroupByName(final String name) {

        final Directory directory = getDirectory();
        if (directory == null) {
            return null;
        }

        return directory.groupsByName.get(name);
    }

    /**
     * Returns the group with the given ID.
     * 
     * @param gid
     *            the ID of the group to look up
     * @return the group with the given ID or <code>null</code> if no such group
     *         exists
     */
    public Group getGroupByGID(final int gid) {

        final Directory directory = getDirectory();
        if (directory == null) {
            return null;
        }

        return directory.groupsByGID.get(Integer.valueOf(gid));
    }

    private Directory getDirectory() {

        if (this.cachingPeriodInMilliSeconds == 0L) {
            // We don't cache at all
            return createDirectory(this.getentGroupCmd, this.getentPasswdCmd);
        }

        final long now = System.currentTimeMillis();
        synchronized (this) {
            if (this.cachedDirectory == null
                    || this.cacheTimestamp + this.cachingPeriodInMilliSeconds < now) {

                final Directory directory = createDirectory(
                        this.getentGroupCmd, this.getentPasswdCmd);
                if (directory != null) {
                    this.cachedDirectory = directory;
                    this.cacheTimestamp = now;
                }
            }
            return this.cachedDirectory;
        }
    }

    private static Directory createDirectory(final String getentGroupCmd,
            final String getentPasswdCmd) {

        final Map<Integer, Group> groupsByGID = new HashMap<Integer, Group>();
        final Map<String, Group> groupsByName = new HashMap<String, Group>();
        final Map<String, Set<Group>> temporaryUserToGroupMap = new HashMap<String, Set<Group>>();

        Process p = null;
        Closeable closeable = null;

        try {

            p = Runtime.getRuntime().exec(getentGroupCmd);
            final InputStream is = p.getInputStream();
            closeable = is;
            final InputStreamReader isr = new InputStreamReader(is,
                    DEFAULT_CHARSET);
            closeable = isr;
            final BufferedReader br = new BufferedReader(isr);
            closeable = br;

            String line;
            while ((line = br.readLine()) != null) {
                parseGroupLine(line, groupsByName, groupsByGID,
                        temporaryUserToGroupMap);
            }

        } catch (IOException ioe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Error while executing " + getentGroupCmd + ": "
                        + ioe.getMessage());
            }
            return null;
        } finally {
            closeSilently(closeable);
            if (p != null) {
                try {
                    final int exitCode = p.waitFor();
                    if (exitCode != 0) {
                        if (LOG.isErrorEnabled()) {
                            LOG.error(getentGroupCmd + "returned with code "
                                    + exitCode);
                        }
                    }
                } catch (InterruptedException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error(getentGroupCmd + " received interrupt: "
                                + e.getMessage());
                    }
                }
            }
        }

        p = null;
        closeable = null;

        final Map<String, User> usersByName = new HashMap<String, User>();
        final Map<Integer, User> usersByUID = new HashMap<Integer, User>();

        try {

            p = Runtime.getRuntime().exec(getentPasswdCmd);
            final InputStream is = p.getInputStream();
            closeable = is;
            final InputStreamReader isr = new InputStreamReader(is,
                    DEFAULT_CHARSET);
            closeable = isr;
            final BufferedReader br = new BufferedReader(isr);
            closeable = br;

            String line;
            while ((line = br.readLine()) != null) {
                parsePasswdLine(line, usersByName, usersByUID, groupsByGID,
                        temporaryUserToGroupMap);
            }

        } catch (IOException ioe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Error while executing " + getentPasswdCmd + ": "
                        + ioe.getMessage());
            }
            return null;
        } finally {
            closeSilently(closeable);
            if (p != null) {
                try {
                    final int exitCode = p.waitFor();
                    if (exitCode != 0) {
                        if (LOG.isErrorEnabled()) {
                            LOG.error(getentPasswdCmd + "returned with code "
                                    + exitCode);
                        }
                    }
                } catch (InterruptedException e) {
                    if (LOG.isErrorEnabled()) {
                        LOG.error(getentPasswdCmd + " received interrupt: "
                                + e.getMessage());
                    }
                }
            }
        }

        return new Directory(usersByName, usersByUID, groupsByName, groupsByGID);
    }

    static void parseGroupLine(final String line,
            final Map<String, Group> groupsByName,
            final Map<Integer, Group> groupsByGID,
            final Map<String, Set<Group>> temporaryUserToGroupMap) {

        final String[] fields = line.split(":");
        if (fields.length < 3) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Line '" + line + "' has less than 3 fields");
            }
            return;
        }

        final int gid;
        try {
            gid = Integer.parseInt(fields[2]);
        } catch (NumberFormatException nfe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("GID " + fields[2] + " for group " + fields[0]
                        + " is not a number");
            }
            return;
        }

        final Group group = new Group(fields[0], gid);
        groupsByName.put(fields[0], group);
        groupsByGID.put(Integer.valueOf(gid), group);

        // Extract the users from the group
        if (fields.length > 3) {
            final String[] users = fields[3].split(",");
            for (String user : users) {
                Set<Group> groups = temporaryUserToGroupMap.get(user);
                if (groups == null) {
                    groups = new HashSet<Group>();
                    temporaryUserToGroupMap.put(user, groups);
                }
                groups.add(group);
            }
        }
    }

    static void parsePasswdLine(final String line,
            final Map<String, User> usersByName,
            final Map<Integer, User> usersByUID,
            final Map<Integer, Group> groupsByGID,
            final Map<String, Set<Group>> temporaryUserToGroupMap) {

        final String[] fields = line.split(":");
        if (fields.length < 4) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Line '" + line + "' has less than 4 fields");
            }
            return;
        }

        final int uid;
        try {
            uid = Integer.parseInt(fields[2]);
        } catch (NumberFormatException nfe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("UID " + fields[2] + " for user " + fields[0]
                        + " is not a number");
            }
            return;
        }

        final int primaryGID;
        try {
            primaryGID = Integer.parseInt(fields[3]);
        } catch (NumberFormatException nfe) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Primary GID " + fields[3] + " for user " + fields[0]
                        + " is not a number");
            }
            return;
        }

        final Group primaryGroup = groupsByGID.get(Integer.valueOf(primaryGID));
        if (primaryGroup == null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("Cannot find primary group with GID " + primaryGID
                        + " for user " + fields[0]);
            }
            return;
        }

        Set<Group> groups = temporaryUserToGroupMap.get(fields[0]);
        if (groups == null) {
            groups = new HashSet<Group>();
        }
        groups.add(primaryGroup);

        final User user = new User(fields[0], uid, primaryGroup,
                Collections.unmodifiableSet(groups));

        usersByName.put(fields[0], user);
        usersByUID.put(Integer.valueOf(uid), user);

        // TODO: Update group membership
    }

    /**
     * Closes the given {@link Closeable} object. Any potential
     * {@link IOException} is silently discarded.
     * 
     * @param closeable
     *            the {@link Closeable} object to close
     */
    static void closeSilently(final Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ioe) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(ioe);
                }
            }
        }
    }
}
