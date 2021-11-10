package org.cloud.vehicletelematics;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;

public class Main {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // set up the execution environment
        //data/sample-traffic-3xways.csv

        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inFilePath = "./data/sample-traffic-3xways.csv";
        String outFilePath = "./data/sample-traffic-out.csv";
        CsvReader csvFile = env.readCsvFile(inFilePath);
        DataSource<String> source = env.readTextFile(inFilePath);
        source.writeAsText(outFilePath);
        try {
            env.execute("SlideProgram1");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
