package com.ebay.pulsar.distributor.epl.func;

public class DefaultValue {
	public static Number NumberNullDefaultZero(Number v){
		return v==null?0:v;
	}
	public static Number NumberNullDefault(Number v, Number defaultValue){
		return v==null?defaultValue:v;
	}
	public static Integer IntegerNullDefaultZero(Integer v){
		return v==null?0:v;
	}
	public static Integer IntegerNullDefault(Integer v, Integer defaultValue){
		return v==null?defaultValue:v;
	}
	public static Long LongNullDefaultZero(Long v){
		return v==null?0L:v;
	}
	public static Long LongNullDefault(Long v, Long defaultValue){
		return v==null?defaultValue:v;
	}
	public static String StringNullDefaultEmpty(String v){
		return v==null?"":v;
	}
	public static String StringNullDefault(String v, String defaultValue){
		return v==null?defaultValue:v;
	}
}
