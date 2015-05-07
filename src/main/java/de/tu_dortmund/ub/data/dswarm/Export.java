/*
The MIT License (MIT)

Copyright (c) 2015, Hans-Georg Becker, http://orcid.org/0000-0003-0432-294X

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

package de.tu_dortmund.ub.data.dswarm;

import java.io.File;
import java.io.StringWriter;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.apache.http.client.methods.HttpGet;

import org.apache.http.Consts;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Export Task for Task Processing Unit for d:swarm
 *
 * @author Jan Polowinski (SLUB Dresden)
 * @version 2015-04-20
 *
 */
public class Export implements Callable<String> {

    private Properties config = null;
    private Logger logger = null;

    public Export(Properties config, Logger logger) {

        this.config = config;
        this.logger = logger;
    }

//    @Override
    public String call() {

        // init logger
        PropertyConfigurator.configure(config.getProperty("service.log4j-conf"));

        logger.info("[" + config.getProperty("service.name") + "] " + "Starting 'XML-Export (Task)' ...");

        // init IDs of the prototype project
        String dataModelID = config.getProperty("prototype.dataModelID");
        String projectID = config.getProperty("prototype.projectID");
        String outputDataModelID = config.getProperty("prototype.outputDataModelID");

        // init process values
        String message = null;

        try {
        	
        	if (Boolean.parseBoolean(config.getProperty("export.do"))) {

	            if (Boolean.parseBoolean(config.getProperty("results.persistInFolder"))) {
	            	
	                // export and save to results folder
	                String xmlResponse = exportDataModel(outputDataModelID);
	                FileUtils.writeStringToFile(new File(config.getProperty("results.folder") + File.separatorChar + "export-of-" + dataModelID + "." + ".xml"), xmlResponse);
	            }
            
        	}
        }
        catch (Exception e) {

            logger.error("[" + config.getProperty("service.name") + "] Exporting and saving datamodel '" + dataModelID + "' failed with a " + e.getClass().getSimpleName());
            e.printStackTrace();
        }

        return message;
    }

    /**
     * export the data model with the given ID as XML
     *
     * @param dataModelID
     * @return xml string
     * @throws Exception
     */
    private String exportDataModel(String dataModelID) throws Exception {

        CloseableHttpClient httpclient = HttpClients.createDefault();
        CloseableHttpResponse httpResponse;

        try {
        	
            HttpGet httpGet = new HttpGet(config.getProperty("engine.dswarm.api") + "datamodels/" + dataModelID + "/export?format=application/xml");
            
//			httpGet.setHeader(name, value);

            logger.info("[" + config.getProperty("service.name") + "] dataModelID : " + dataModelID);
            logger.info("[" + config.getProperty("service.name") + "] " + "request : " + httpGet.getRequestLine());

            httpResponse = httpclient.execute(httpGet);

            try {

                int statusCode = httpResponse.getStatusLine().getStatusCode();

                switch (statusCode) {

                    case 200: {

                        logger.info("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());

                        break;
                    }
                    default: {

                        logger.error("[" + config.getProperty("service.name") + "] " + statusCode + " : " + httpResponse.getStatusLine().getReasonPhrase());
                    }
                }
                
				final StringWriter writer = new StringWriter();
				IOUtils.copy(httpResponse.getEntity().getContent(), writer, "UTF-8");
				final String responseXML = writer.toString();
				return responseXML;
                
            } finally {
                httpResponse.close();
            }
        } finally {
            httpclient.close();
        }
    }
}
