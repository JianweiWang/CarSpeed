package com.wjw.carSpeedMonitor;

public class TupleInfo {
	private String vehicleNumber;
	private int speed;
	private String location;
	public void setVehicleNumber(String number) {
		this.vehicleNumber = number;
	}
	public String getVehicleNumber() {
		return vehicleNumber;
	}
	public void setSpeed(int speed) {
		this.speed = speed;
	}
	public int getSpeed() {
		return speed;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getLocation() {
		return location;
	}
	
}
