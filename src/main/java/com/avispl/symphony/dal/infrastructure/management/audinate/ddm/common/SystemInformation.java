/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common;

/**
 * Enum representing system information properties for Dante Domain Manager.
 * Each property includes a name and a corresponding GraphQL value.
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public enum SystemInformation {
	CLOCKING("Sites#Clocking", "clocking"),
	CONNECTIVITY("Sites#Connectivity", "connectivity"),
	LATENCY("Sites#Latency", "latency"),
	SUBSCRIPTION("Sites#Subscriptions", "subscriptions"),
			;
	private final String name;
	private final String value;

	/**
	 * Constructor for SystemInfo.
	 *
	 * @param name The name representing the system information category.
	 * @param value The corresponding value associated with the category.
	 */
	SystemInformation(String name, String value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Retrieves {@link #name}
	 *
	 * @return value of {@link #name}
	 */
	public String getName() {
		return name;
	}

	/**
	 * Retrieves {@link #value}
	 *
	 * @return value of {@link #value}
	 */
	public String getValue() {
		return value;
	}
}
