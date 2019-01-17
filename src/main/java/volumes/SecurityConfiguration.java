package volumes;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;
import javax.security.auth.login.Configuration;

/**
 * This class encapsulates the security configuration. The configuration sets
 * all the necessary options for a Kerberos-based authentication.
 * <p>
 * This class is thread-safe.
 */
final class SecurityConfiguration extends Configuration {

    /**
     * The prepared configuration entry.
     */
    private final AppConfigurationEntry configurationEntry;

    /**
     * Constructs a new security configuration.
     * 
     * @param principalName
     *            the name of the principal to use for the Kerberos
     *            authentication
     * @param keyTabPath
     *            the path to the keytab file to use for the Kerberos
     *            authentication
     * @param isInitiator
     *            <code>
     */
    SecurityConfiguration(final String principalName, final String keyTabPath,
            boolean isInitiator) {

        final Map<String, Object> krb5LoginOpts = new HashMap<String, Object>();

        krb5LoginOpts.put("refreshKrb5Config", "true");
        krb5LoginOpts.put("doNotPrompt", "true");
        krb5LoginOpts.put("useKeyTab", "true");
        krb5LoginOpts.put("storeKey", "true");
        krb5LoginOpts.put("isInitiator", Boolean.toString(isInitiator));
        krb5LoginOpts.put("principal", principalName);
        krb5LoginOpts.put("keyTab", keyTabPath);

        this.configurationEntry = new AppConfigurationEntry(
                "com.sun.security.auth.module.Krb5LoginModule",
                LoginModuleControlFlag.REQUIRED,
                Collections.unmodifiableMap(krb5LoginOpts));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(final String name) {

        return new AppConfigurationEntry[] { this.configurationEntry };
    }
}
