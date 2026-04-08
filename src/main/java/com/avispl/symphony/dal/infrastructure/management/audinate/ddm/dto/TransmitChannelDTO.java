/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm.dto;

/**
 * Represents a DTO (Data Transfer Object) for a transmit channel
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public class TransmitChannelDTO {
	private String id;
	private String index;
	private String name;
	private String mediaType;

	/**
	 * Constructs a TransmitChannelDTO with the specified properties.
	 *
	 * @param id The id of the channel.
	 * @param index The index of the channel.
	 * @param name The name of the channel.
	 */
	public TransmitChannelDTO(String id, String index, String name, String mediaType) {
		this.id = id;
		this.index = index;
		this.name = name;
		this.mediaType = mediaType;
	}

	/**
	 * Constructs an empty TransmitChannelDTO.
	 */
	public TransmitChannelDTO() {
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
	 * Sets {@link #name} value
	 *
	 * @param name new value of {@link #name}
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Retrieves {@link #id}
	 *
	 * @return value of {@link #id}
	 */
	public String getId() {
		return id;
	}

	/**
	 * Sets {@link #id} value
	 *
	 * @param id new value of {@link #id}
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * Retrieves {@link #index}
	 *
	 * @return value of {@link #index}
	 */
	public String getIndex() {
		return index;
	}

	/**
	 * Sets {@link #index} value
	 *
	 * @param index new value of {@link #index}
	 */
	public void setIndex(String index) {
		this.index = index;
	}

	/**
	 * Retrieves {@link #mediaType}
	 *
	 * @return value of {@link #mediaType}
	 */
	public String getMediaType() {
		return mediaType;
	}

	/**
	 * Sets {@link #mediaType} value
	 *
	 * @param mediaType new value of {@link #mediaType}
	 */
	public void setMediaType(String mediaType) {
		this.mediaType = mediaType;
	}
}
