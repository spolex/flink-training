package org.apache.flink.quickstart;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class POJOExample {
	public static	class	Person	{
		public	int	id;
		public	String	name;
		public	Person()	{};
		public	Person(int	id,	String	name){
			this.id = id;
			this.name = name;
		};
	}
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		List<Tuple2<String,	Integer>>		
		data	=	new	ArrayList<Tuple2<String,	Integer>>();	
		data.add(new	Tuple2<>("odd",	1));	
		data.add(new	Tuple2<>("even",	2));	
		data.add(new	Tuple2<>("odd",	3));	
		data.add(new	Tuple2<>("even",	4));	
		DataStream<Person> personStream = env.fromElements(new Person(1, "Bob"));
		personStream.map(s -> "Flink says:".concat(s.name)).print();
		env.execute();
	}
}
