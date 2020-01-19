package com.jw9j.flink.etltest;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SourceFromSql extends RichSourceFunction<OrderDetail> {
    Connection connection=null;
    PreparedStatement ps;
    @Override
    public void run(SourceContext sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            OrderDetail orderDetail = new OrderDetail(
              resultSet.getString("OrderId"),
              resultSet.getString("OrderDetailId"),
              resultSet.getString("MemberId"),
              resultSet.getInt("ChildOrderSource"),
              resultSet.getString("ProductColor"),
              resultSet.getString("ProductName"),
              resultSet.getString("ProductTitle"),
              resultSet.getString("OrderAmount"),
              resultSet.getString("OriginalPrice"),
              resultSet.getInt("Quantity"),
              resultSet.getFloat("AmountPayable"),
              resultSet.getString("Size")
            );
            sourceContext.collect(orderDetail);
        }

    }

    @Override
    public void cancel() {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        String driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url="jdbc:sqlserver://10.126.21.125:1433;DataBaseName=CDPDB";
        String userName = "yungoal";
        String passWord = "CDP@yg123";
        Class.forName(driver);
        connection = DriverManager.getConnection(url,userName,passWord);
        String sql = "select * from original.CRM_OrderDetail";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(connection!=null){
            connection.close();
        }
        if(ps!=null){
            ps.close();
        }
    }
}
