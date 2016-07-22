package com.itera.teste0;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.LongStream;

public class Aux {
	
	public static void main(String args[]) {
		
		Map<Integer, Iterable<Long>> mapReal = new HashMap<>();
		for(int i=0; i<6; i++) {
			mapReal.put(i, LongStream.rangeClosed((i*100)+1, (i+1)*100)::iterator);
		}
		System.out.println(mapReal);
		for(Long l:mapReal.get(0))
			System.out.println(l);
	}

}
