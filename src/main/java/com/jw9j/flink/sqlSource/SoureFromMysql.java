package com.jw9j.flink.sqlSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class SoureFromMysql extends RichSourceFunction<ClassRoom> {
    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        String driver = "com.sqlserver.jdbc.Driver";
        String url = "jdbc:sqlserver://localhost:3306/sakila_dwh";
        String userName = "root";
        String password = "512731x";
        Class.forName(driver);
        connection = DriverManager.getConnection(url,userName,password);
        String sql = "select * from classroom";
        ps = connection.prepareStatement(sql);

    }
    @Override
    public void run(SourceContext<ClassRoom> sourceContext) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while(resultSet.next()){
            ClassRoom classRoom =  new ClassRoom(
                    resultSet.getString("building").trim(),
                    resultSet.getInt("room_number"),
                    resultSet.getInt("capacity")
            );
            sourceContext.collect(classRoom);
        }
    }

    @Override
    public void cancel() {

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
