package com.jw9j.flink.sqlSource;

import java.sql.*;

public class JDBCTest {
    public static void main(String[] args) throws SQLException {
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/sakila_dwh";
        String userName = "root";
        String password = "512731x";
        Connection connection = null;
        Statement statement = null;
        try{
//            加载驱动
            Class.forName(driver);
            connection = DriverManager.getConnection(url,userName,password);
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(
                    "select * from classroom;");
            while(resultSet.next()){
                ClassRoom classRoom = new ClassRoom(
                        resultSet.getString("building").trim(),
                        resultSet.getInt("room_number"),
                        resultSet.getInt("capacity"));
                System.out.println(classRoom);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(connection !=null){
                connection.close();
            }
            if(statement != null){
                statement.close();
            }
        }
    }
}
