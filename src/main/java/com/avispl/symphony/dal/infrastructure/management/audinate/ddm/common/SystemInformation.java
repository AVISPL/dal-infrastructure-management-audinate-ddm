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
	CLOCKING("Clocking", "clocking", "Domain"),
	CONNECTIVITY("Connectivity", "connectivity", "Domain"),
	LATENCY("Latency", "latency", "Domain"),
	SUBSCRIPTION("Subscriptions", "subscriptions", "Domain"),
			;
	private final String name;
	private final String value;
	private final String group;

	/**
	 * Constructor for SystemInfo.
	 *
	 * @param name The name representing the system information category.
	 * @param value The corresponding value associated with the category.
	 */
	SystemInformation(String name, String value, String group) {
		this.name = name;
		this.value = value;
		this.group = group;
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

	/**
	 * Retrieves {@link #group}
	 *
	 * @return value of {@link #group}
	 */
	public String getGroup() {
		return group;
	}
}
