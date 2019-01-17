package volumes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

/**
 * MapR REST repsonse JSON parser. Reads MapR REST response JSON stream and indicates
 * whether REST call has finished successfully or with error.
 */
class MaprRestParser {

    /**
     * The log object used for debugging and reporting.
     */
    private static final Log LOG = LogFactory.getLog(MaprRestParser.class);

    /**
     * The key to retrieve the status field from the JSON object.
     */
    private static final String STATUS_KEY = "status";

    /**
     * The key to retrieve the data field from the JSON object.
     */
    private static final String ERROR_KEY = "errors";

    /**
     * The key to retrieve description in the error JSON node
     */
    private static final String ERROR_DESC_KEY = "desc";

    /**
     * The value indicating a correct REST call.
     */
    private static final String OK_STATUS_VALUE = "OK";

    /**
     * The value indicating error REST response
     */
    private static final String ERROR_STATUS_VALUE = "ERROR";

    /**
     * reads in REST response as JSON data, logs errors
     * returns true on success, false on failure
     */
    public static boolean getResponseStatus(final InputStream is)
            throws IOException {

        try {
            // Parse JSON message
            final ObjectMapper mapper = new ObjectMapper();
            final JsonNode rootNode = mapper.readTree(is);

            // Check if the call returned with the right response code
            final JsonNode statusNode = rootNode.get(STATUS_KEY);

            // something went very wrong
            if (statusNode == null || statusNode.getNodeType() !=
                    JsonNodeType.STRING) {
                LOG.error("REST call returned with unexpected response");
                return false;
            }

            // return immediately if OK status
            if (OK_STATUS_VALUE.equals(statusNode.asText())) {
                LOG.info("REST response with status OK");
                return true;
            }

            // Response status not OK and not ERROR indicates unexpected status
            // TODO : PARTIAL status handling
            if (!ERROR_STATUS_VALUE.equals(statusNode.asText())) {
                LOG.error("REST call returned with unexpected return code: " +
                        statusNode.asText());
                return false;
            }

            // Iterate JSON data
            final JsonNode dataNode = rootNode.get(ERROR_KEY);
            if (dataNode == null
                    || dataNode.getNodeType() != JsonNodeType.ARRAY) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn("REST call returned with unexpected error component: "
                            + rootNode);
                }
                return false;
            }

            // log all errors found in JSON response
            for (final Iterator<JsonNode> it = dataNode.iterator(); it
                    .hasNext();) {
                String error = parseError(it.next());
                if (error != null) {
                    LOG.error("REST error response message : " + error);
                }
            }

        } catch (JsonProcessingException e) {
            throw new IOException(e);
        }

        return false;
    }

    /**
     * convert JSON error node into string containing the error message
     */
    private static String parseError(final JsonNode errorNode) {

        final JsonNode errorDescNode = errorNode.get(ERROR_DESC_KEY);
        if (errorDescNode == null
                || errorDescNode.getNodeType() != JsonNodeType.STRING) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Could not decode error JSON: " + errorNode);
            }
            return null;
        }

        return errorDescNode.asText();
    }
}
