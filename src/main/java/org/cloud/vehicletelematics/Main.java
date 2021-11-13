package org.cloud.vehicletelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the execution environment
        //data/sample-traffic-3xways.csv
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inFilePath = "./data/sample-traffic-3xways.csv";
        String outFilePath = "./data/speedfines.csv";
        //CsvReader csvFile = env.readCsvFile(inFilePath);
        DataStream<String> dataStream = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamTuple = dataStream.map(
                new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String in) throws Exception {
                String[] fieldArray = in.split(",");
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple8(Integer.parseInt(fieldArray[0]),
                        Integer.parseInt(fieldArray[1]),
                        Integer.parseInt(fieldArray[2]),
                        Integer.parseInt(fieldArray[3]),
                        Integer.parseInt(fieldArray[4]),
                        Integer.parseInt(fieldArray[5]),
                        Integer.parseInt(fieldArray[6]),
                        Integer.parseInt(fieldArray[7]));
                return out;
            }
        });

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamFilter = dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f2 > 90; }
        });

        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamOut = dataStreamFilter.project(0,1,3,6,5,2);

        env.setParallelism(1);
        dataStreamOut.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        //dataStreamOut.print();

        try {
            env.execute("Vehicule Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
