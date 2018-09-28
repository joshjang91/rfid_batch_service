package com.example.appengine.demos.springboot.dao;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import com.example.appengine.demos.springboot.constants.EventServiceConstants;
import org.springframework.stereotype.Component;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.UUID;

@Component
public class BigQueryUtilities {

    public TableResult runNamed(final String queryString) throws InterruptedException, IOException {

        //BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();//FIXME
        BigQuery bigquery =
                BigQueryOptions.newBuilder().setCredentials(ServiceAccountCredentials.fromStream(
                        new FileInputStream(EventServiceConstants.LOCAL_CREDENTIALS_PATH))).build().getService();

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(queryString)
                        .setUseLegacySql(false)
                        .build();

        // Create a job ID so that we can safely retry.
        JobId jobId = JobId.of(UUID.randomUUID().toString());
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        // Wait for the query to complete.
        queryJob = queryJob.waitFor();

        // Check for errors
        if (queryJob == null) {
            throw new RuntimeException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            throw new RuntimeException(queryJob.getStatus().getError().toString());
        }

        // Get the results.
        return queryJob.getQueryResults();
    }
}
