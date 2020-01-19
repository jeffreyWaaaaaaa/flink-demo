package com.jw9j.flink.sqlSource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class SinkToMysql extends RichSinkFunction<ClassRoom>{
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<ClassRoom> classRoomDataStream = env.fromElements(
//                new ClassRoom("test",100,100),
//                new ClassRoom("test2",200,200)
//        );
//        classRoomDataStream.addSink(new SinkToMysql());
//        env.execute();
//    }
    private Connection connection = null;
    private PreparedStatement ps;


    @Override
    public void invoke(ClassRoom value, Context context) throws Exception {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/sakila_dwh";
        String userName = "root";
        String password = "512731x";
        Class.forName(driver);
        connection = DriverManager.getConnection(url, userName, password);
        String sql = "insert into classroom(building,room_number,capacity) values(?,?,?)";
        ps = connection.prepareStatement(sql);
        ps.setString(1,value.building+"12");
        ps.setInt(2,value.roomNumber+1);
        ps.setInt(3,value.capacity+2);
        ps.executeUpdate();
    }
    public void SinkToMysql(DataStream<ClassRoom> dataStream) throws SQLException {

    }
}
