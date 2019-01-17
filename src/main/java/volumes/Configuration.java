package volumes;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;

/**
 * This class implements a simple and light-weight configuration utility. The
 * class can read Hadoop-style XML configuration file without requiring a large
 * set of additional libraries such as hadoop-common, guava, etc.
 * <p>
 * This class is not thread-safe.
 */
final class Configuration {

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(Configuration.class);

    private final Map<String, String> keyValuePairs = new HashMap<String, String>();

    void addResource(final File file) {

        final DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory
                .newInstance();
        // Ignore comments in the XML file
        docBuilderFactory.setIgnoringComments(true);
        docBuilderFactory.setNamespaceAware(true);

        try {

            final DocumentBuilder builder = docBuilderFactory
                    .newDocumentBuilder();
            Document doc = null;
            Element root = null;

            doc = builder.parse(file);

            if (doc == null) {
                LOG.warn("Cannot load configuration: doc is null");
                return;
            }

            root = doc.getDocumentElement();
            if (root == null) {
                LOG.warn("Cannot load configuration: root is null");
                return;
            }

            if (!"configuration".equals(root.getNodeName())) {
                LOG.warn("Cannot load configuration: unknown element "
                        + root.getNodeName());
                return;
            }

            final NodeList props = root.getChildNodes();

            for (int i = 0; i < props.getLength(); i++) {

                final Node propNode = props.item(i);
                String name = null;
                String value = null;

                // Ignore text at this point
                if (propNode instanceof Text) {
                    continue;
                }

                if (!(propNode instanceof Element)) {
                    LOG.warn("Error while reading configuration: "
                            + propNode.getNodeName()
                            + " is not of type element");
                    continue;
                }

                Element property = (Element) propNode;
                if (!"property".equals(property.getNodeName())) {
                    LOG.warn("Error while reading configuration: unknown element "
                            + property.getNodeName());
                    continue;
                }

                final NodeList propChildren = property.getChildNodes();
                if (propChildren == null) {
                    LOG.warn("Error while reading configuration: property has no children, skipping...");
                    continue;
                }

                for (int j = 0; j < propChildren.getLength(); ++j) {

                    final Node propChild = propChildren.item(j);
                    if (propChild instanceof Element) {
                        if ("name".equals(propChild.getNodeName())
                                && propChild.getChildNodes() != null
                                && propChild.getChildNodes().getLength() == 1
                                && propChild.getChildNodes().item(0) instanceof Text) {

                            final Text t = (Text) propChild.getChildNodes()
                                    .item(0);
                            name = t.getTextContent();
                        }

                        if ("value".equals(propChild.getNodeName())
                                && propChild.getChildNodes() != null
                                && propChild.getChildNodes().getLength() == 1
                                && propChild.getChildNodes().item(0) instanceof Text) {

                            final Text t = (Text) propChild.getChildNodes()
                                    .item(0);
                            value = t.getTextContent();
                        }
                    }
                }

                if (name != null && value != null) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Added configuration property: " + name
                                + ", " + value);
                    }
                    this.keyValuePairs.put(name, value);
                }
            }

        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
    }

    String get(final String key, final String defaultValue) {

        final String value = this.keyValuePairs.get(key);
        if (value != null) {
            return value;
        }

        return defaultValue;
    }

    int getInt(final String key, final int defaultValue) {

        final String value = this.keyValuePairs.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
            }
        }

        return defaultValue;
    }
}
