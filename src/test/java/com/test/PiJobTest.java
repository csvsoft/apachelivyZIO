package com.test;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

public class PiJobTest {

    @Test
    public void testPi(){
        LivyClient client =  null;
        String piJar = "/Users/zrq/workspace/apachelivy/target/apachelivy-1.0-SNAPSHOT.jar";
        int samples = 100;

        try {
            client = new LivyClientBuilder()
                    .setURI(new URI("http://localhost:8998/"))
                    .build();
            System.err.printf("Uploading %s to the Spark context...\n", piJar);
            client.uploadJar(new File(piJar)).get();

            System.err.printf("Running PiJob with %d samples...\n", samples);
          //  double pi = client.submit(new PiJob(samples)).get();

           // System.out.println("Pi is roughly: " + pi);
        } catch(Exception ex){
            ex.printStackTrace();
        }finally {
            client.stop(true);
        }
    }
}
