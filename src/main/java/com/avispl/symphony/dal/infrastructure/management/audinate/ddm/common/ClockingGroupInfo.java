/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common;

/**
 * Enum representing clocking group information.
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public enum ClockingGroupInfo {
	MODE("ClockingGroupMode", "mode", NodeType.ROOT),
	PTP_CONFIGURATION("PTPConfiguration", "v2", NodeType.PTP),
	PTP_PRIORITY1("PTPv2Priority1", "v2Priority1", NodeType.PTP),
	PTP_PRIORITY2("PTPv2Priority2", "v2Priority2", NodeType.PTP),
	PTP_DOMAIN("PTPv2DomainNumber", "v2DomainNumber", NodeType.PTP),
	PTP_SYNC_INTERVAL("PTPv2SyncInterval", "v2SyncInterval", NodeType.PTP),
	PTP_ANNOUNCE_INTERVAL("PTPv2AnnounceInterval", "v2AnnounceInterval", NodeType.PTP),
	PTP_MULTICAST_TTL("PTPv2MulticastTTL", "v2MulticastTtl", NodeType.PTP),
	PTP_SLAVE_ONLY("PTPSlaveOnly", "followerOnly", NodeType.PTP),

	RTP_PREFIX_V4("RTPPrefixV4", "prefixV4", NodeType.RTP),
	RTP_RX_LATENCY("RxLatency(s)", "rxLatency", NodeType.RTP),
	RTP_SYSTEM_PACKET_TIME("SystemPacketTime(µs)", "systemPacketTime", NodeType.RTP),
	RTP_TRANSMIT_PORT("RTPTransmitPort", "transmitPort", NodeType.RTP);

	private final String name;
	private final String field;
	private final NodeType nodeType;

	/**
	 * Constructs an ClockingGroupInfo with the specified name, field, and node type.
	 *
	 * @param name The name of the information property.
	 * @param field The field associated with the information property.
	 * @param nodeType The type associated with the information property.
	 */
	ClockingGroupInfo(String name, String field, NodeType nodeType) {
		this.name = name;
		this.field = field;
		this.nodeType = nodeType;
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
	 * Retrieves {@link #field}
	 *
	 * @return value of {@link #field}
	 */
	public String getField() {
		return field;
	}

	/**
	 * Retrieves {@link #nodeType}
	 *
	 * @return value of {@link #nodeType}
	 */
	public NodeType getNodeType() {
		return nodeType;
	}

	/**
	 * Enum of node type
	 */
	public enum NodeType {
		ROOT, PTP, RTP
	}
}
