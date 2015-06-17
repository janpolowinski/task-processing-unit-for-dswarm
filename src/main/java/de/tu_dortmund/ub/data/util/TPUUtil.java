package de.tu_dortmund.ub.data.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;

import de.tu_dortmund.ub.data.dswarm.TPUStatics;
import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * @author tgaengler
 */
public final class TPUUtil {

	public static final String EXPORT_FILE_NAME_PREFIX  = "export-of-";
	public static final String DOT                      = ".";
	public static final String XML_FILE_ENDING          = "xml";

	public static Optional<Boolean> getBooleanConfigValue(final String configKey, final Properties config) {

		final String configValue = config.getProperty(configKey);

		final Optional<Boolean> optionalConfigValue;

		if (configValue != null && !configValue.trim().isEmpty()) {

			optionalConfigValue = Optional.of(Boolean.valueOf(configValue));
		} else {

			optionalConfigValue = Optional.empty();
		}
		return optionalConfigValue;
	}

	public static void writeResultToFile(final CloseableHttpResponse httpResponse, final Properties config, final String exportDataModelID) throws IOException {

		final String persistInFolderString = config.getProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER);
		final boolean persistInFolder = Boolean.parseBoolean(persistInFolderString);

		if (persistInFolder) {

			final InputStream xmlResponse = httpResponse.getEntity().getContent();

			final String resultsFolder = config.getProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER);
			final String fileName = resultsFolder + File.separatorChar + EXPORT_FILE_NAME_PREFIX + exportDataModelID + DOT + XML_FILE_ENDING;
			final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileName));

			IOUtils.copy(xmlResponse, outputStream);
			xmlResponse.close();
			outputStream.close();
		}
	}
}
