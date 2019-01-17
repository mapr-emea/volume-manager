package volumes;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

/**
 * MaprAceParser - get readAce/writeAce from volume info JSON
 */
class MaprAceParser {

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(MaprAceParser.class);

    /**
     * The key to retrieve the status field from the JSON object.
     */
    private static final String STATUS_KEY = "status";

    /**
     * The key to retrieve the data field from the JSON object.
     */
    private static final String DATA_KEY = "data";

    /**
     * The value indicating a correct REST call.
     */
    private static final String OK_STATUS_VALUE = "OK";

    /**
     * The key to retrieve volume aces
     */
    private static final String VOLUME_ACES = "volumeAces";

    /**
     * The key to retrieve readAce
     */
    private static final String VOLUME_READ_ACE = "readAce";

    /**
     * The key to retrieve writeAce
     */
    private static final String VOLUME_WRITE_ACE = "writeAce";

    /**
     * converts JSON input stream into list of MaprVolume objects
     */
    static List<String> parse(final InputStream is) throws IOException {

        List<String> aces = new ArrayList<String>();

        try {
            // Parse JSON message
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode rootNode = mapper.readTree(is);

            // Check if the call returned with the right response code
            final JsonNode statusNode = rootNode.get(STATUS_KEY);
            if (statusNode == null
                    || statusNode.getNodeType() != JsonNodeType.STRING
                    || !OK_STATUS_VALUE.equals(statusNode.asText())) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("REST call returned with unexpected return code: "
                            + rootNode);
                }
                return Collections.emptyList();
            }

            // Iterate the data
            final JsonNode dataNode = rootNode.get(DATA_KEY);
            if (dataNode == null
                    || dataNode.getNodeType() != JsonNodeType.ARRAY) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("REST call returned with unexpected data component: "
                            + rootNode);
                }
                return Collections.emptyList();
            }

            for (final Iterator<JsonNode> it = dataNode.iterator(); it
                    .hasNext();) {

                aces = parseAces(it.next());
            }

        } catch (JsonProcessingException e) {
            throw new IOException(e);
        }

        return Collections.unmodifiableList(aces);
    }

    /**
     * convert JSON data into MapR volume data item
     */
    private static List<String> parseAces(final JsonNode volumeNode) {

        final JsonNode volumeAcesNode = volumeNode.get(VOLUME_ACES);

        if (volumeAcesNode == null) {
            if (LOG.isWarnEnabled()) {
                LOG.info("volumeAces on the volume not found: " + volumeAcesNode);
            }
            return null;
        }

        final JsonNode readAceNode = volumeAcesNode.get(VOLUME_READ_ACE);

        if (readAceNode == null
                || readAceNode.getNodeType() != JsonNodeType.STRING) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("readAce on the volume has wrong type: " + volumeNode);
            }
            return null;
        }

        final JsonNode writeAceNode = volumeAcesNode.get(VOLUME_WRITE_ACE);

        if (writeAceNode == null
                || writeAceNode.getNodeType() != JsonNodeType.STRING) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("writeAce on the volume has wrong type: " + volumeNode);
            }
            return null;
        }

        List<String> aces = new ArrayList<String>();
        aces.add(0, readAceNode.asText());
        aces.add(1, writeAceNode.asText());

        return aces;
    }
}
