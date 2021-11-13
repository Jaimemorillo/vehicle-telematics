package org.cloud.vehicletelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/overview/

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

        int Time = 0;
        int VID = 1;
        int Spd = 2;
        int XWay = 3;
        int Lane = 4;
        int Dir = 5;
        int Seg = 6;
        int Pos = 7;

        // DataStream to Tuple
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

        // SpeedRadar
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamFilter =
                dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f2 > 90; }
        });

        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamOut =
                dataStreamFilter.project(Time,VID,XWay,Seg,Dir,Spd);

        // AverageSpeedControl
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> segments5256 =
                dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f6 == 52 | in.f6 == 56; }
        });
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> segments =
                segments5256.project(Time,VID,XWay,Dir,Seg,Pos);


        /* segments.keyBy(value -> value.f1).reduce(new ReduceFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> reduce(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value1, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value2)
                    throws Exception {
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(value2.f0, value1.f1, value1.f2+value2.f2);
                return out;
            }
        });
            }
        }); */


        // Tener en cuenta la dirección para el calculo de los metros (Dir y Pos)


        segments5256.print();

        // Write output
        //env.setParallelism(1);
        //dataStreamOut.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("Vehicule Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
