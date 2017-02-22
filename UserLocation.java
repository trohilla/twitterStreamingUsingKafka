package com.tushar.kafka.core;

/**
 * @author tushar
 *
 */
public class UserLocation {
	
	private String location;
	private Long count;

	public UserLocation(String location, Long value) {
		super();
		this.location = location;
		this.count = value;
	}

	@Override
	public String toString() {
		return "UserLocation [location=" + location + ", count=" + count + "]";
	}
	
	public String getLocation() {
		return location;
	}

	public Long getCount() {
		return count;
	}
	
	
}
