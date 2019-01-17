package getent;

/**
 * This class implements a group as retrieved from the Unix getent command.
 *
 * This class is thread-safe.
 */
public final class Group {

    /**
     * The name of this group.
     */
    private final String name;

    /**
     * The ID of this group.
     */
    private final int gid;

    /**
     * Constructs a new group.
     * 
     * @param name
     *            the name of this group
     * @param gid
     *            the ID of this group
     */
    Group(final String name, final int gid) {
        this.name = name;
        this.gid = gid;
    }

    /**
     * Returns the name of this group.
     * 
     * @return the name of this group
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the ID of this group.
     * 
     * @return the ID of this group
     */
    public int getGID() {
        return this.gid;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {

        if (!(obj instanceof Group)) {
            return false;
        }

        final Group other = (Group) obj;

        if (!this.name.equals(other.name)) {
            return false;
        }

        if (this.gid != other.gid) {
            return false;
        }

        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return this.name.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {

        final StringBuilder sb = new StringBuilder(64);
        sb.append(this.name);
        sb.append(" (");
        sb.append(this.gid);
        sb.append(')');

        return sb.toString();
    }
}
