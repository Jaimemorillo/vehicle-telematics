package org.cloud.vehicletelematics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;

// https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/operators/overview/

public class Main {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the execution environment
        //data/sample-traffic-3xways.csv
        //System.out.println("Working Directory = " + System.getProperty("user.dir"));

        String inFilePath = "./data/sample-traffic-3xways.csv";
        String outFilePath = "./data/average.csv";
        //CsvReader csvFile = env.readCsvFile(inFilePath);
        DataStream<String> dataStream = env.readTextFile(inFilePath);

        int Timestamp = 0;
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
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out =
                        new Tuple8(Integer.parseInt(fieldArray[0]),
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

        ///////////////////////////////////
        /////////// SpeedRadar ////////////
        ///////////////////////////////////

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamFilter =
                dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f2 > 90; }
        });

        //Select output in order
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> dataStreamOut =
                dataStreamFilter.project(Timestamp,VID,XWay,Seg,Dir,Spd);


        ///////////////////////////////////
        /////// AverageSpeedControl ///////
        ///////////////////////////////////

        // MaxBy va iterando por el datastream y pinta el registro máximo encontrado hasta ese momento,
        // max pinta el primer regirstro siempre con el maximo valor encontrado
        //https://stackoverflow.com/questions/57932282/whats-the-practical-use-of-keyedstreammax

        // Filter reports in segment 52 and 56
        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> segment5256Ini =
                dataStreamTuple.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return in.f6 >= 52 && in.f6 <= 56;}
        });
        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> segments5256 =
                segment5256Ini.project(Timestamp,VID,XWay,Dir,Seg,Pos);

        // Create the key
        KeyedStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> keyedStreamSegments5256 =
                segments5256.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>(){
            @Override
            public long extractAscendingTimestamp(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0*1000;
            }
        }).keyBy(new KeySelector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
                    @Override
                    public Tuple3<Integer, Integer, Integer> getKey(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                        return Tuple3.of(value.f1, value.f2, value.f3);
                    }
                });

        // Window function
        class AverageSpeed implements WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                Tuple6<Integer, Integer, Integer, Integer, Integer, Double>,
                Tuple3<Integer, Integer, Integer>,
                TimeWindow> {
            @Override
            public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow window, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> input,
                              Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out) throws Exception {

                Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();

                int timeMin = 999999999;
                int posMin = 999999999;
                int timeMax = 0;
                int posMax = 0;
                int segMin = 999;
                int segMax = 0;

                while(iterator.hasNext()){
                    Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();
                    int ntime = next.f0;
                    int npos = next.f5;
                    int nseg = next.f4;

                    if (ntime <= timeMin){
                        timeMin = ntime;
                    }
                    if (npos <= posMin){
                        posMin = npos;
                    }
                    if (ntime>=timeMax){
                        timeMax = ntime;
                    }
                    if (npos >= posMax){
                        posMax = npos;
                    }
                    if (nseg <= segMin){
                        segMin = nseg;
                    }
                    if (nseg >= segMax){
                        segMax = nseg;
                    }
                }

                if (segMin == 52 && segMax==56) {
                    double speedMs = (float)(posMax - posMin) / (timeMax - timeMin);
                    double speedMhp = 2.23694 * speedMs;
                    double speedMhpRound = Math.round(speedMhp * 10000d) / 10000d;
                    if (speedMhp > 60) {
                        out.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(timeMin,timeMax,key.f0,key.f1,key.f2,speedMhpRound));
                    }
                }
            }
        }

        // Apply windows
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> averageSpeed =
                keyedStreamSegments5256
                        .window(EventTimeSessionWindows.withGap(Time.seconds(91)))
                        .apply(new AverageSpeed());

        // For checking cars
        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> segmentsF =
                segments5256.filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                        return in.f1 == 2213; }
                });

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> averageSpeedF =
                averageSpeed.filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>>() {
                    @Override
                    public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Double> in) throws Exception {
                        return in.f2 == 196144; }
                });

        //segmentsF.print();

        // Write output
        env.setParallelism(1);
        averageSpeed.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);

        try {
            env.execute("Vehicule Telematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
