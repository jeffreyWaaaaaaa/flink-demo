package com.jw9j.flink.sqlSource;

public class ClassRoom {
    String building;
    int roomNumber;
    int capacity;

    public ClassRoom(String name,int roomNumber,int capacity){
        this.roomNumber=roomNumber;
        this.building=name;
        this.capacity=capacity;
    }

    public String getName() {
        return building;
    }

    public void setName(String name) {
        this.building = name;
    }

    public int getRoomNumber() {
        return roomNumber;
    }

    public void setRoomNumber(int roomNumber) {
        this.roomNumber = roomNumber;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }
    @Override
    public String toString() {
        return "ClassRoom{" +
                "name='" + building + '\'' +
                ", roomNumber=" + roomNumber +
                ", capacity=" + capacity +
                '}';
    }
}
