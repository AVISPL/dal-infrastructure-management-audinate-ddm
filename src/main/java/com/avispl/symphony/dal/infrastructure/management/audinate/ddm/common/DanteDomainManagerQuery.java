/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common;

/**
 * Class containing GraphQL queries used in Dante Domain Manager communication.
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public class DanteDomainManagerQuery {
	public static final String SYSTEM_INFO = "{\"query\":\"query Domains "
			+ "{ domains { "
			+ "name id "
			+ "devices { id } "
			+ "clockingGroup { "
			+ "mode "
			+ "ptp { v2 v2Priority1 v2Priority2 v2DomainNumber } "
			+ "rtp { prefixV4 rxLatency systemPacketTime transmitPort } "
			+ "} "
			+ "status { clocking connectivity latency subscriptions summary "
			+ "domainAlertMessage { "
			+ "clocking { message messageSeverity } "
			+ "connectivity { message messageSeverity } "
			+ "latency { message messageSeverity } "
			+ "subscriptions { message messageSeverity } "
			+ "} } "
			+ "} }\"}";

	public static final String DEVICES_INFO = "{\"query\":\"query Devices "
			+ "{ domains {  id name "
			+ "devices { "
			+ "id  name  enrolmentState  comments description  location "
			+ "domain { name clockingGroup { mode ptp { v2 v2Priority1 v2Priority2 } } } "
			+ "connection { state lastChanged }  "
			+ "discovery { type fqdn } "
			+ "identity { productModelName productVersion danteHardwareVersion productSoftwareVersion danteVersion } "
			+ "manufacturer { name } "
			+ "interfaces { address macAddress subnet netmask} "
			+ "capabilities { CAN_WRITE_UNICAST_DELAY_REQUESTS CAN_WRITE_PREFERRED_MASTER CAN_WRITE_EXT_WORD_CLOCK CAN_UNICAST_CLOCKING } "
			+ "status { clocking connectivity latency subscriptions summary "
			+ "alertMessage { clocking  connectivity  latency  subscriptions }}  "
			+ "rxChannels { mediaType name subscribedChannel subscribedDevice } "
			+ "txChannels { id index name mediaType } "
			+ "clockingState { followerWithoutLeader frequencyOffset grandLeader locked multicastLeader muteStatus unicastFollower unicastLeader } "
			+ "clockPreferences { externalWordClock leader unicastClocking v1UnicastDelayRequests overrides { ptp { v2Priority1 v2Priority2 } } } "
			+ "} } }\"}";

	public static final String DEVICES_BY_DOMAIN_ID = "{\"query\":\"query Devices($domainId: ID!) "
			+ "{ domain(id: $domainId) { id name "
			+ "devices { "
			+ "id name enrolmentState comments description location "
			+ "domain { name clockingGroup { mode ptp { v2 v2Priority1 v2Priority2 } } } "
			+ "connection { state lastChanged } "
			+ "discovery { type fqdn } "
			+ "identity { productModelName productVersion danteHardwareVersion productSoftwareVersion danteVersion } "
			+ "manufacturer { name } "
			+ "interfaces { address macAddress subnet netmask } "
			+ "capabilities { CAN_WRITE_UNICAST_DELAY_REQUESTS CAN_WRITE_PREFERRED_MASTER CAN_WRITE_EXT_WORD_CLOCK CAN_UNICAST_CLOCKING } "
			+ "status { clocking connectivity latency subscriptions summary "
			+ "alertMessage { clocking connectivity latency subscriptions } } "
			+ "rxChannels { mediaType name subscribedChannel subscribedDevice } "
			+ "txChannels { id index name mediaType } "
			+ "clockingState { followerWithoutLeader frequencyOffset grandLeader locked multicastLeader muteStatus unicastFollower unicastLeader } "
			+ "clockPreferences { externalWordClock leader unicastClocking v1UnicastDelayRequests overrides { ptp { v2Priority1 v2Priority2 } } } "
			+ "} } }\","
			+ "\"variables\": {\"domainId\": \"%s\"}}";

	public static final String CONTROL_CLOCK_SYNC = "{\"query\":\"mutation ControlCommand($input: %s!) "
			+ "{ %s(input: $input) "
			+ "{ ok } }\","
			+ "\"variables\": {"
			+ "\"input\" : "
			+ "{\"deviceId\":\"%s\", \"enabled\":%s}}}";
}
