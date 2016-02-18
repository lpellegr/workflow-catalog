/*
 * ProActive Parallel Suite(TM): The Java(TM) library for
 *    Parallel, Distributed, Multi-Core Computing for
 *    Enterprise Grids & Clouds
 *
 * Copyright (C) 1997-2016 INRIA/University of
 *                 Nice-Sophia Antipolis/ActiveEon
 * Contact: proactive@ow2.org or contact@activeeon.com
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Affero General Public License
 * as published by the Free Software Foundation; version 3 of
 * the License.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
 * USA
 *
 * If needed, contact us to obtain a release under GPL Version 2 or 3
 * or a different license than the AGPL.
 *
 * Initial developer(s):               The ProActive Team
 *                         http://proactive.inria.fr/team_members.htm
 */
package org.ow2.proactive.workflow_catalog.rest.controller;

import java.io.FileInputStream;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import org.ow2.proactive.workflow_catalog.rest.Application;
import org.ow2.proactive.workflow_catalog.rest.dto.BucketMetadata;
import org.ow2.proactive.workflow_catalog.rest.util.ProActiveWorkflowParser;
import com.google.common.io.ByteStreams;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.response.ValidatableResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static com.jayway.restassured.RestAssured.given;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

/**
 * @author ActiveEon Team
 */
@ActiveProfiles("test")
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = { Application.class })
@WebIntegrationTest
public class WorkflowRevisionControllerQueryIntegrationTest2 extends AbstractWorkflowRevisionControllerTest {

    private BucketMetadata bucket;

    private Logger log = LoggerFactory.getLogger(WorkflowRevisionControllerQueryIntegrationTest2.class);

    @Before
    public void setup() throws IOException, XMLStreamException {
        bucket = bucketService.createBucket("bucket");

        String[] files = new String[] {
                "/home/lpellegr/Téléchargements/wcql-tests/A.xml",
                "/home/lpellegr/Téléchargements/wcql-tests/B.xml",
                "/home/lpellegr/Téléchargements/wcql-tests/C.xml",
                "/home/lpellegr/Téléchargements/wcql-tests/D.xml",
//                "/home/lpellegr/Téléchargements/wcql-tests/E.xml",
//                "/home/lpellegr/Téléchargements/wcql-tests/F.xml",
        };

        for (String file : files) {
            final byte[] bytes = ByteStreams.toByteArray(new FileInputStream(file));

            ProActiveWorkflowParser parser =
                    new ProActiveWorkflowParser(new FileInputStream(file));

            workflowService.createWorkflow(bucket.id, parser.parse(), bytes);
        }
    }


    @Test
    public void test() {
        String query1 = "generic_information(\"I\",\"E\")";
        String query2 = "generic_information(\"I\", \"E\") OR generic_information(\"C\", \"E\")";
        String query3 = "variable(\"CPU\", \"5%\")";
        String query4 = "generic_information(\"I\", \"E\") OR generic_information(\"C\", \"E\") AND variable(\"CPU\", \"%\")";

        String query5 = "variable.name=\"toto\"";
        String query6 = "variable(\"toto\",\"Amazon\")";

        // avec parentheses
        String query7 = "(generic_information(\"I\", \"E\"))";


        ValidatableResponse mostRecentWorkflowRevisions = findMostRecentWorkflowRevisions(
                query7
        );
    }

    public ValidatableResponse findMostRecentWorkflowRevisions(String wcqlQuery) {
        Response response = given().pathParam("bucketId", bucket.id)
                .queryParam("size", 100)
                .queryParam("query", wcqlQuery)
                .when().get(WORKFLOWS_RESOURCE);

        logQueryAndResponse(wcqlQuery, response);

        return response.then().assertThat();
    }

    public ValidatableResponse findAllWorkflowRevisions(String wcqlQuery, long workflowId) {
        Response response = given().pathParam("bucketId", bucket.id).pathParam("workflowId", workflowId)
                .queryParam("size", 100)
                .queryParam("query", wcqlQuery)
                .when().get(WORKFLOW_REVISIONS_RESOURCE);

        logQueryAndResponse(wcqlQuery, response);

        return response.then().assertThat();
    }

    private void logQueryAndResponse(String wcqlQuery, Response response) {
        log.info("WCQL query used is '{}'", wcqlQuery);
        log.info("Response is:\n{}", prettify(response.asString()));
    }

}