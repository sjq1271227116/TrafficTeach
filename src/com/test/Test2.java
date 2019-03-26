package com.test;

import com.shsxt.spark.util.StringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class Test2 extends UserDefinedAggregateFunction{
    @Override
    public StructType inputSchema() {
        return null;
    }

    @Override
    public StructType bufferSchema() {
        return null;
    }

    @Override
    public DataType dataType() {
        return null;
    }

    @Override
    public boolean deterministic() {
        return false;
    }

    @Override
    public void initialize(MutableAggregationBuffer buffer) {

    }
    //0001=1000|0002=2000|0003=3000
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        String string = buffer.getString(0);
        String string1 = input.getString(0);
        String[] split = string1.split("|");
        int addNum=1;
        String mid="";
        for (String s:split){
            if(s.indexOf("=")!=-1){
                mid=s.split("=")[0];
                addNum=Integer.parseInt(s.split("=")[1]);
            }else {
                mid=s;
            }
            String fieldFromConcatString = StringUtils.getFieldFromConcatString(string, "\\|", mid);
            if(fieldFromConcatString==null){
                string+="|"+mid+"="+addNum;
            }else {
                string= StringUtils.setFieldInConcatString(string, "\\|", mid, Integer.parseInt(fieldFromConcatString) + addNum + "");

            }
            buffer.update(0,string);


        }


    }

    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    }

    @Override
    public Object evaluate(Row buffer) {
        return null;
    }
}
