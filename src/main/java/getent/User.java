package getent;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * This class implements a user as retrieved from the Unix getent command.
 *
 * This class is thread-safe.
 */
public final class User {

	/**
	 * The name of this user.
	 */
	private final String name;

	/**
	 * The ID of this user.
	 */
	private final int uid;

	/**
	 * The primary group of this user.
	 */
	private final Group primaryGroup;

	/**
	 * The groups of this user.
	 */
	private final Set<Group> groups;

	/**
	 * Constructs a new user.
	 * 
	 * @param name
	 *            the name of this user
	 * @param uid
	 *            the ID of this user
	 * @param primaryGroup
	 *            the primary group of this user
	 * @param groups
	 *            the groups of this user
	 */
	User(final String name, final int uid, final Group primaryGroup,
			final Set<Group> groups) {
		this.name = name;
		this.uid = uid;
		this.primaryGroup = primaryGroup;
		this.groups = Collections.unmodifiableSet(groups);
	}

	/**
	 * Return this name of this user.
	 * 
	 * @return the name of this user
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns the ID of this user.
	 * 
	 * @return the ID of this user
	 */
	public int getUID() {
		return this.uid;
	}

	public Group getPrimaryGroup() {
		return this.primaryGroup;
	}

	public Collection<Group> getGroups() {
		return this.groups;
	}

	public boolean isMemberOf(final Group group) {
		return this.groups.contains(group);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(final Object obj) {

		if (!(obj instanceof User)) {
			return false;
		}

		final User other = (User) obj;

		if (!this.name.equals(other.name)) {
			return false;
		}

		if (this.uid != other.uid) {
			return false;
		}

		if (!this.primaryGroup.equals(other.primaryGroup)) {
			return false;
		}

		if (this.groups.size() != other.groups.size()) {
			return false;
		}

		for (final Iterator<Group> it = groups.iterator(); it.hasNext();) {

			final Group group = it.next();
			if (!other.groups.contains(group)) {
				return false;
			}
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
		sb.append(this.uid);
		sb.append(')');

		return sb.toString();
	}
}
