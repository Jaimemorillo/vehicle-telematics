package org.cloud.vehicletelematics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

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

        // DataStream<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> d =

        /*SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> filterOut = source.map(
                new MapFunction<String>, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                String[] fieldArray = in.split(",");
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6(Integer.parseInt(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                return out;
            }
        }).filter(new FilterFunction<Tuple3<Long,String,Double>>() {
            @Override
            public boolean filter(Tuple3<Long, String, Double> in) throws Exception {
                return in.f1.equals("sensor1")); }
        });
        filterOut.writeAsCsv(outFilePath);
        }*/

    }
}
