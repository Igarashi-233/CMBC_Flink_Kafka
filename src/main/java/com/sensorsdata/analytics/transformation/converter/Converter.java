package com.sensorsdata.analytics.transformation.converter;

import org.apache.flink.api.java.tuple.Tuple2;



public interface Converter {
    Tuple2<Object, String> doConvert(Object source);
}
