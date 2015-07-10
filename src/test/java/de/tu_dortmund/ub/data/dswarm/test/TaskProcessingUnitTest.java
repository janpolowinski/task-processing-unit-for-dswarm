package de.tu_dortmund.ub.data.dswarm.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

import com.google.common.io.Resources;
import de.tu_dortmund.ub.data.dswarm.TPUStatics;
import de.tu_dortmund.ub.data.dswarm.TaskProcessingUnit;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * note: to execute the tests you first need to provide a D:SWARM backend and configure the API endpoint vie tpu.properties (parameter = 'dmp_api_endpoint')
 *
 * @author tgaengler
 */
public class TaskProcessingUnitTest {

	private static final Logger LOG = LoggerFactory.getLogger(TaskProcessingUnitTest.class);

	private static final String DEFAULT_PROTOTYPE_PROJECT_ID           = "6db219f9-bfd1-76ff-36c6-d49d8057fd02";
	private static final String DEFAULT_DO_INIT                        = "true";
	private static final String DEFAULT_DO_INITIAL_DATA_MODEL_INGEST   = "false";
	private static final String DEFAULT_ALLOW_MULTIPLE_DATA_MODELS     = "true";
	private static final String DEFAULT_DO_INGEST                      = "false";
	private static final String DEFAULT_DO_TRANSFORMATIONS             = "true";
	private static final String DEFAULT_DO_INGEST_ON_THE_FLY           = "true";
	private static final String DEFAULT_DO_EXPORT_ON_THE_FLY           = "true";
	private static final String DEFAULT_DO_EXPORT                      = "false";
	private static final String DEFAULT_PERSIST_IN_DMP                 = "false";
	private static final String DEFAULT_PERSIST_IN_FOLDER              = "true";
	private static final String DEFAULT_ENGINE_THREADS                 = "1";
	private static final String DEFAULT_PROTOTYPE_OUTPUT_DATA_MODEL_ID = "5fddf2c5-916b-49dc-a07d-af04020c17f7";
	private static final String DEFAULT_RESOURCE_WATCH_FOLDER_NAME     = "defaultresourcewatchfolder";
	private static final String DEFAULT_CONFIGURATION_FILE_NAME        = "oai-pmh-dc-xml-configuration.json";
	private static final String PROJECT_POSTFIX                        = "-project";

	private static final String PROJECT_ROOT = System.getProperty("project.root");
	private static final String USER_DIR     = System.getProperty("user.dir");
	private static final String ROOT_PATH;

	static {

		if (PROJECT_ROOT != null) {

			ROOT_PATH = PROJECT_ROOT;
		} else if (USER_DIR != null) {

			ROOT_PATH = USER_DIR;
		} else {

			TaskProcessingUnitTest.LOG.error("could not determine root path - project.root and user.dir is not available");

			ROOT_PATH = "";
		}
	}

	private static final String TEST_RESOURCES_ROOT_PATH =
			ROOT_PATH + File.separator + "src" + File.separator + "test" + File.separator + "resources";
	private static final String DEFAULT_RESULTS_FOLDER   = ROOT_PATH + File.separator + "target";

	private final String dmpAPIEndpoint;

	public TaskProcessingUnitTest() {

		final URL resource = Resources.getResource("tpu.properties");
		final Properties properties = new Properties();

		try {

			properties.load(resource.openStream());
		} catch (final IOException e) {

			LOG.error("Could not load tpu.properties", e);
		}

		dmpAPIEndpoint = properties.getProperty("dmp_api_endpoint", "http://localhost:8087/dmp/");

		TaskProcessingUnitTest.LOG.info("DMP API endpoint URI is = '{}'", dmpAPIEndpoint);
	}

	@Test
	public void testTPUExceptionAtXMLData() {

		final String testName = "TPU-test-1";
		final Properties config = generateDefaultConfig(testName + "-exception-at-xml-data");
		final String resourceWatchFolderName = "tputest1rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; lineNumber: 3; columnNumber: 1; XML document structures must start and end within the same entity.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	@Test
	public void testTPUWrongData() {

		final String testName = "TPU-test-2";
		final Properties config = generateDefaultConfig(testName + "-wrong-data");
		final String resourceWatchFolderName = "tputest2rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; lineNumber: 1; columnNumber: 1; Content is not allowed in prolog.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	@Test
	public void testTPUEmptyFile() {

		final String testName = "TPU-test-3";
		final Properties config = generateDefaultConfig(testName + "-empty-file");
		final String resourceWatchFolderName = "tputest3rwf";
		final String configFileName = "xml-configuration.json";
		final String expectedErrorMessage = "{\"error\":{\"message\":\"couldn't process task (maybe XML export) successfully\",\"stacktrace\":\"org.culturegraph.mf.exceptions.MetafactureException: org.xml.sax.SAXParseException; Premature end of file.";

		executeTPUTest(testName, config, resourceWatchFolderName, configFileName, expectedErrorMessage);
	}

	private void executeTPUTest(final String testName, final Properties config, final String resourceWatchFolderName, final String configigFileName, final String expectedErrorMessage) {

		final String resourceWatchFolder = TEST_RESOURCES_ROOT_PATH + File.separator + resourceWatchFolderName;
		final String configurationFilePath = TEST_RESOURCES_ROOT_PATH + File.separator + configigFileName;

		TaskProcessingUnitTest.LOG.debug("[{}] resource watch folder = '{}'", testName, resourceWatchFolder);
		TaskProcessingUnitTest.LOG.debug("[{}] configuration file name = '{}'", testName, configurationFilePath);

		config.setProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER, resourceWatchFolder);
		config.setProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER, configurationFilePath);

		final String confFile = testName + "-dummy-config.properties";
		executeTPUTest(testName, confFile, config, expectedErrorMessage);
	}

	private void executeTPUTest(final String testName, final String confFile, final Properties config, final String expectedErrorMessage) {

		TaskProcessingUnitTest.LOG.info("start " + testName);

		try {

			final String result = TaskProcessingUnit.startTPU(confFile, config);

			TaskProcessingUnitTest.LOG.debug(result);
		} catch (final Exception e) {

			final Throwable cause1 = e.getCause();

			Assert.assertNotNull(cause1);

			final Throwable cause2 = cause1.getCause();

			Assert.assertNotNull(cause2);

			final Throwable cause3 = cause2.getCause();

			Assert.assertNotNull(cause3);

			final Throwable cause4 = cause3.getCause();

			Assert.assertNotNull(cause4);

			final String cause4Message = cause4.getMessage();

			TaskProcessingUnitTest.LOG.debug("[{}] actual error message '{}'", testName, cause4Message);

			Assert.assertTrue(cause4Message.startsWith(expectedErrorMessage));
		}

		TaskProcessingUnitTest.LOG.info("finished " + testName);
	}

	private Properties generateDefaultConfig(final String testName) {

		final String defaultResourceWatchFolder = TEST_RESOURCES_ROOT_PATH + File.separator + DEFAULT_RESOURCE_WATCH_FOLDER_NAME;
		final String defaultConfigurationName = TEST_RESOURCES_ROOT_PATH + File.separator + DEFAULT_CONFIGURATION_FILE_NAME;

		TaskProcessingUnitTest.LOG.debug("default resource watch folder = '{}'", defaultResourceWatchFolder);
		TaskProcessingUnitTest.LOG.debug("default configuration file name = '{}'", defaultConfigurationName);

		final Properties config = new Properties();

		config.setProperty(TPUStatics.SERVICE_NAME_IDENTIFIER, testName);
		config.setProperty(TPUStatics.PROJECT_NAME_IDENTIFIER, testName + PROJECT_POSTFIX);
		config.setProperty(TPUStatics.RESOURCE_WATCHFOLDER_IDENTIFIER, defaultResourceWatchFolder);
		config.setProperty(TPUStatics.CONFIGURATION_NAME_IDENTIFIER, defaultConfigurationName);
		config.setProperty(TPUStatics.PROTOTYPE_PROJECT_ID_INDENTIFIER, DEFAULT_PROTOTYPE_PROJECT_ID);
		config.setProperty(TPUStatics.PROTOTYPE_OUTPUT_DATA_MODEL_ID_IDENTIFIER, DEFAULT_PROTOTYPE_OUTPUT_DATA_MODEL_ID);
		config.setProperty(TPUStatics.DO_INIT_IDENTIFIER, DEFAULT_DO_INIT);
		config.setProperty(TPUStatics.DO_INITIAL_DATA_MODEL_INGEST_IDENTIFIER, DEFAULT_DO_INITIAL_DATA_MODEL_INGEST);
		config.setProperty(TPUStatics.ALLOW_MULTIPLE_DATA_MODELS_IDENTIFIER, DEFAULT_ALLOW_MULTIPLE_DATA_MODELS);
		config.setProperty(TPUStatics.DO_INGEST_IDENTIFIER, DEFAULT_DO_INGEST);
		config.setProperty(TPUStatics.DO_TRANSFORMATIONS_IDENTIFIER, DEFAULT_DO_TRANSFORMATIONS);
		config.setProperty(TPUStatics.DO_INGEST_ON_THE_FLY_IDENTIFIER, DEFAULT_DO_INGEST_ON_THE_FLY);
		config.setProperty(TPUStatics.DO_EXPORT_ON_THE_FLY_IDENTIFIER, DEFAULT_DO_EXPORT_ON_THE_FLY);
		config.setProperty(TPUStatics.DO_EXPORT_IDENTIFIER, DEFAULT_DO_EXPORT);
		config.setProperty(TPUStatics.PERSIST_IN_DMP_IDENTIFIER, DEFAULT_PERSIST_IN_DMP);
		config.setProperty(TPUStatics.PERSIST_IN_FOLDER_IDENTIFIER, DEFAULT_PERSIST_IN_FOLDER);
		config.setProperty(TPUStatics.RESULTS_FOLDER_IDENTIFIER, DEFAULT_RESULTS_FOLDER);
		config.setProperty(TPUStatics.ENGINE_THREADS_IDENTIFIER, DEFAULT_ENGINE_THREADS);
		config.setProperty(TPUStatics.ENGINE_DSWARM_API_IDENTIFIER, dmpAPIEndpoint);

		return config;
	}

}
