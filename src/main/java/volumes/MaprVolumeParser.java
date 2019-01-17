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
 * MaprVolumeParser - converts JSON input stream into list of MaprVolume
 * objects.
 */
class MaprVolumeParser {

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(MaprVolumeParser.class);

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
     * The key to retrieve volume name
     */
    private static final String VOLUME_NAME_KEY = "volumename";

    /**
     * The key to retrieve mount directory
     */
    private static final String VOLUME_MOUNTDIR_KEY = "mountdir";

    /**
     * converts JSON input stream into list of MaprVolume objects
     */
    static List<MaprVolume> parse(final InputStream is) throws IOException {

        final List<MaprVolume> volumes = new ArrayList<MaprVolume>();

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

                final MaprVolume maprVolume = parseVolume(it.next());
                if (maprVolume != null) {
                    volumes.add(maprVolume);
                }
            }

        } catch (JsonProcessingException e) {
            throw new IOException(e);
        }

        return Collections.unmodifiableList(volumes);
    }

    /**
     * convert JSON data into MapR volume data item
     */
    private static MaprVolume parseVolume(final JsonNode volumeNode) {

        final JsonNode volumeNameNode = volumeNode.get(VOLUME_NAME_KEY);
        if (volumeNameNode == null
                || volumeNameNode.getNodeType() != JsonNodeType.STRING) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Alarm has unexpected alarm state: " + volumeNode);
            }
            return null;
        }

        final JsonNode mountDirNode = volumeNode.get(VOLUME_MOUNTDIR_KEY);
        if (mountDirNode == null
                || mountDirNode.getNodeType() != JsonNodeType.STRING) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Alarm has unexpected alarm state: " + volumeNode);
            }
            return null;
        }

        MaprVolume volume = new MaprVolume(volumeNameNode.asText());

        volume.setMountDir(mountDirNode.asText());

        return volume;
    }
}
