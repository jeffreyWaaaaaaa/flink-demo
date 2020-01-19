package com.jw9j.flink.etltest;

import akka.io.Tcp;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SinkToSql extends RichSinkFunction<OrderDetail>{
    Connection connection = null;
    PreparedStatement ps;
    @Override
    public void open(Configuration parameters) throws Exception {
        String driver="com.microsoft.sqlserver.jdbc.SQLServerDriver";
        String url="jdbc:sqlserver://10.126.21.80:1433;DataBaseName=Testdata";
        String userName = "sa";
        String passWord = "1qaz@WSX!QAZ";
        Class.forName(driver);
        connection = DriverManager.getConnection(url,userName,passWord);
        String sql = "INSERT INTO [dbo].[CRM_OrderDetail]\n" +
                "           ([OrderId]\n" +
                "           ,[OrderDetailId]\n" +
                "           ,[MemberId]\n" +
                "           ,[ChildOrderSource]\n" +
                "           ,[ProductColor]\n" +
                "           ,[ProductName]\n" +
                "           ,[ProductTitle]\n" +
                "           ,[OrderAmount]\n" +
                "           ,[OriginalPrice]\n" +
                "           ,[Quantity]\n" +
                "           ,[AmountPayable]\n" +
                "           ,[Size])\n" +
                "     VALUES\n" +
                "           (?,?,?,?,?,?,?,?,?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }

    @Override
    public void invoke(OrderDetail value, Context context) throws Exception {

            ps.setString(1,value.getOrderId());
            ps.setString(2,value.getOrderDetailId());
            ps.setString(3,value.getMemberId());
            ps.setInt(4,value.getChildOrderSource());
            ps.setString(5,value.getProductColor());
            ps.setString(6,value.getProductName());
            ps.setString(7,value.getProductTitle());
            ps.setString(8,value.getOrderAmount());
            ps.setString(9,value.getOriginalPrice());
            ps.setInt (10,value.getQuantity());
            ps.setFloat(11,value.getAmountPayable());
            ps.setString(12,value.getSize());
            ps.executeUpdate();
        }
    }
