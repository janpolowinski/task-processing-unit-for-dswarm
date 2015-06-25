package de.tu_dortmund.ub.data.util;

import java.io.BufferedInputStream;
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
import org.apache.log4j.Logger;

/**
 * @author tgaengler
 */
public final class TPUUtil {

	private static Logger logger = Logger.getLogger(TPUUtil.class.getName());

	public static final String EXPORT_FILE_NAME_PREFIX = "export-of-";
	public static final String DOT                     = ".";
	public static final String XML_FILE_ENDING         = "xml";

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

	public static void writeResultToFile(final CloseableHttpResponse httpResponse, final Properties config, final String exportDataModelID)
			throws IOException {

		logger.info("try to write result to file");

		final String persistInFolderString = config.getProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER);
		final boolean persistInFolder = Boolean.parseBoolean(persistInFolderString);

		if (persistInFolder) {

			final InputStream responseStream = httpResponse.getEntity().getContent();
			final BufferedInputStream bis = new BufferedInputStream(responseStream, 1024);

			final String resultsFolder = config.getProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER);
			final String fileName = resultsFolder + File.separatorChar + EXPORT_FILE_NAME_PREFIX + exportDataModelID + DOT + XML_FILE_ENDING;

			logger.info(String.format("start writing result to file '%s'", fileName));

			final FileOutputStream outputStream = new FileOutputStream(fileName);
			final BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

			IOUtils.copy(bis, bufferedOutputStream);
			bufferedOutputStream.flush();
			outputStream.flush();
			bis.close();
			responseStream.close();
			bufferedOutputStream.close();
			outputStream.close();
		}
	}
}
