package Udemy_Course.Assignment;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RideStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment=
                StreamExecutionEnvironment.getExecutionEnvironment();

//       DataStream<RideData> stream= environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/CabFlink.txt")
//                .map(new MapRideObjects())
//                .filter(new FilterObject())
//                .keyBy("destination")
//                .sum("destination")
//                .

        SingleOutputStreamOperator<Tuple8<String, String, String, String, Boolean, String, String, Integer>> stream=
                 environment.readTextFile("/Users/shailendralohia/Documents/Flink/src/main/java/Udemy_Course/InputFile/CabFlink.txt")
                .map(new MapRideData())
                .filter(new FilterObject())
                .keyBy(6)
                .sum(7);
                //.max(7);

       stream.print();

       environment.execute("assignment for popular destination");



    }
}

class MapRideObjects implements MapFunction<String,RideData> {

    public RideData map(String rideData) throws Exception {
        String[] tokens=rideData.split(",");

        RideData rdata= new RideData();

        rdata.setCab_id(tokens[0]);
        rdata.setCab_number_plate(tokens[1]);
        rdata.setCab_type(tokens[2]);
        rdata.setDriver_name(tokens[3]);
        rdata.setOngoing_trip(tokens[4]);
        rdata.setPickup_location(tokens[5]);
        rdata.setDestination(tokens[6]);
        rdata.setNumberOfPassenger(Integer.parseInt(tokens[7]));

        return rdata;

    }

}

//class FilterObject implements FilterFunction<RideData> {
//
//    public boolean filter(RideData data) {
//        return data.getOngoing_trip().equals("yes");
//    }
//}

class FilterObject implements FilterFunction<Tuple8<String, String, String, String, Boolean, String, String, Integer>> {

    public boolean filter(Tuple8<String, String, String, String, Boolean, String, String, Integer> inputData) {
        return inputData.f4;
    }
}

class MapRideData implements MapFunction<String,
        Tuple8<String, String, String, String, Boolean, String, String, Integer>> {

    public Tuple8<String, String, String, String, Boolean, String, String, Integer> map (String rideData) {
        String[] tokens=rideData.split(",");

        boolean status= false;
        if(tokens[4].equalsIgnoreCase("yes")) {
            status=true;
        }
        if(status)

        return new Tuple8<String, String, String, String, Boolean, String, String, Integer>
                (tokens[0],tokens[1],tokens[2],tokens[3],status,tokens[5],tokens[6],Integer.parseInt(tokens[7]));

        else
            return new Tuple8<String, String, String, String, Boolean, String, String, Integer>
                    (tokens[0],tokens[1],tokens[2],tokens[3],status,tokens[5],tokens[6],0);
    }
}
