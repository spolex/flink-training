package org.apache.flink.ejercicios.datastream.taxi;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;

public class TaxiDataStream {
	
	public static void main(String[] args) throws Exception {
		 
		// get an ExecutionEnvironment
		StreamExecutionEnvironment env = 
		  StreamExecutionEnvironment.getExecutionEnvironment();
		// configure event-time processing
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// get the taxi ride data stream
		DataStream<TaxiRide> rides = env.addSource(
		  new TaxiRideSource("../data/nycTaxiRides.gz"));
		rides
		.filter(new FilterFunction<TaxiRide>(){

			/**
			 * Removing events that do not start or end in New York City
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public boolean filter(TaxiRide taxi) throws Exception {
				return GeoUtils.isInNYC(taxi.startLon, taxi.startLat) || GeoUtils.isInNYC(taxi.endLon, taxi.endLat);
			}
			
		})
		.print();
		env.execute();
	}

}
