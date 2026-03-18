/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common;

import java.util.Arrays;
import java.util.Optional;

/**
 * Enum representing aggregated information properties for devices.
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public enum AggregatedInformation {
	MANUFACTURER("Manufacturer", ""),
	PRODUCT_VERSION("ProductVersion", ""),
	CONNECTED_SINCE("ConnectedSince(GMT)", ""),
	ENROLMENT_STATE("EnrolmentState", ""),
	CLOCKING("Clocking", DanteDomainManagerConstant.STATUS_GROUP),
	CONNECTIVITY("Connectivity", DanteDomainManagerConstant.STATUS_GROUP),
	LATENCY("Latency", DanteDomainManagerConstant.STATUS_GROUP),
	SUBSCRIPTIONS("Subscriptions", DanteDomainManagerConstant.STATUS_GROUP),
	LOCATION("Location", ""),
	DESCRIPTION("Description", ""),
	COMMENTS("Comments", ""),
	DISCOVERY_TYPE("DiscoveryType", ""),
	DISCOVERY_DOMAIN_NAME("DiscoveryDomainName", ""),
	IP_ADDRESS("IPAddress", ""),
	MAC_ADDRESS("MACAddress", ""),
	DANTE_SOFTWARE_VERSION("DanteSoftwareVersion", ""),
	DANTE_VERSION("DanteVersion", ""),
	SITE_NAME("Site", ""),
	MUTE_STATUS("MuteStatus", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	SYNC_STATUS("SyncStatus", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	DOMAIN_CLOCKING("DomainClocking", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	PRIMARY_MULTICAST("PrimaryMulticast", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	UNICAST("Unicast", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	FREQUENCY("FrequencyOffset(ppm)", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),

	EXTERNAL_WORD_CLOCK("SyncToExternalWordClock", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	LEADER("PreferredLeader", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	UNICAST_CLOCKING("UnicastClocking", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	DELAY_REQUEST("V1DelayRequests", DanteDomainManagerConstant.CLOCK_SYNCHRONISATION_GROUP),
	;
	private final String name;
	private final String group;

	/**
	 * Constructs an AggregatedInformation with the specified name and group.
	 *
	 * @param name The name of the information property.
	 * @param group The group associated with the information property.
	 */
	AggregatedInformation(String name, String group) {
		this.name = name;
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
	 * Retrieves {@link #group}
	 *
	 * @return value of {@link #group}
	 */
	public String getGroup() {
		return group;
	}

	/**
	 * Retrieve a AggregatedInformation by its name.
	 *
	 * @param name The default name to search for.
	 * @return The AggregatedInformation with the specified default name, or null if not found.
	 */
	public static AggregatedInformation getByDefaultName(String name) {
		Optional<AggregatedInformation> property = Arrays.stream(values()).filter(item -> item.getName().equalsIgnoreCase(name)).findFirst();
		return property.orElse(null);
	}
}
