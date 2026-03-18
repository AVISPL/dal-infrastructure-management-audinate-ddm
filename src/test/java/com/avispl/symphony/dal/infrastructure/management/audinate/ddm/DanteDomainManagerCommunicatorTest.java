/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.avispl.symphony.api.dal.dto.control.AdvancedControllableProperty;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;

/**
 * NaViSetAdministrator2SECommunicatorTest
 *
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public class DanteDomainManagerCommunicatorTest {
	private ExtendedStatistics extendedStatistic;
	private DanteDomainManagerCommunicator communicator;

	@BeforeEach
	void setUp() throws Exception {
		communicator = new DanteDomainManagerCommunicator();
		communicator.setHost("");
		communicator.setLogin("");
		communicator.setPassword("");
		communicator.setPort(443);
		communicator.init();
		communicator.connect();
	}

	@AfterEach
	void destroy() throws Exception {
		communicator.disconnect();
		communicator.destroy();
	}

	/**
	 * Test case for getting aggregated data from Dante Domain Manager.
	 * Verifies the retrieved statistics and controllable properties.
	 */
	@Test
	void testGetAggregatorData() throws Exception {
		extendedStatistic = (ExtendedStatistics) communicator.getMultipleStatistics().get(0);
		Map<String, String> statistics = extendedStatistic.getStatistics();
		List<AdvancedControllableProperty> advancedControllableProperties = extendedStatistic.getControllableProperties();
		Assert.assertEquals(10, statistics.size());
		Assert.assertEquals(1, advancedControllableProperties.size());
	}

	/**
	 * Test case for getting aggregated information from Dante Domain Manager.
	 * Verifies specific statistics retrieved.
	 */
	@Test
	void testGetAggregatorInfo() throws Exception {
		extendedStatistic = (ExtendedStatistics) communicator.getMultipleStatistics().get(0);
		Map<String, String> statistics = extendedStatistic.getStatistics();
		Assert.assertEquals("OK", statistics.get("Clocking"));
		Assert.assertEquals("OK", statistics.get("Connectivity"));
		Assert.assertEquals("OK", statistics.get("Latency"));
		Assert.assertEquals("4", statistics.get("NumberOfDevices"));
		Assert.assertEquals("AVI-SPL R&D", statistics.get("SiteName"));
		Assert.assertEquals("OK", statistics.get("Subscriptions"));
	}

	/**
	 * Test case for getting the number of devices from Dante Domain Manager.
	 * Verifies the number of devices retrieved.
	 */
	@Test
	void testGetNumberOfDevices() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(10000);
		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		Assert.assertEquals(4, aggregatedDeviceList.size());
	}

	/**
	 * Test case for aggregated general information retrieved from Dante Domain Manager.
	 * Verifies specific properties of a device.
	 */
	@Test
	void testAggregatedGeneralInfo() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(10000);
		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		String deviceId = "2a8d80f014a04c96921f611c5a4c9d1f";
		Optional<AggregatedDevice> aggregatedDevice = aggregatedDeviceList.stream().filter(item -> item.getDeviceId().equals(deviceId)).findFirst();
		if (aggregatedDevice.isPresent()) {
			Map<String, String> stats = aggregatedDevice.get().getProperties();
			Assert.assertEquals("Nov 28, 2025, 9:33 AM", stats.get("ConnectedSince(GMT)"));
			Assert.assertEquals("1.0.7.1175", stats.get("DanteVersion"));
			Assert.assertEquals("Dante AV H Docker", aggregatedDevice.get().getDeviceModel());
			Assert.assertEquals("Docker-AVH-AVISPL-A02", aggregatedDevice.get().getDeviceName());
			Assert.assertEquals("device.director.dante.cloud", stats.get("DiscoveryDomainName"));
			Assert.assertEquals("STATIC_CONFIG", stats.get("DiscoveryType"));
			Assert.assertEquals("ENROLLED", stats.get("EnrolmentState"));
			Assert.assertEquals("10.15.28.162", stats.get("IPAddress"));
			Assert.assertEquals("02:42:0A:0F:1C:A2", stats.get("MACAddress"));
			Assert.assertEquals("Audinate Pty Ltd", stats.get("Manufacturer"));
			Assert.assertEquals("1.1.1", stats.get("ProductVersion"));
		}
	}

	/**
	 * Test case for aggregated status information retrieved from Dante Domain Manager.
	 * Verifies specific status properties of a device.
	 */
	@Test
	void testAggregatedStatusInfo() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(10000);
		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		String deviceId = "987ad4bfc0904eb9bfb04b3d330d0486";
		Optional<AggregatedDevice> aggregatedDevice = aggregatedDeviceList.stream().filter(item -> item.getDeviceId().equals(deviceId)).findFirst();
		if (aggregatedDevice.isPresent()) {
			Map<String, String> stats = aggregatedDevice.get().getProperties();
			Assert.assertEquals("OK", stats.get("Clocking"));
			Assert.assertEquals("OK", stats.get("Connectivity"));
			Assert.assertEquals("OK", stats.get("Latency"));
			Assert.assertEquals("OK", stats.get("Subscriptions"));
		}
	}

	/**
	 * Test case for aggregated clock synchronization information retrieved from Dante Domain Manager.
	 * Verifies specific clock synchronization properties of a device.
	 */
	@Test
	void testAggregatedClockSyncInfo() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(10000);
		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		String deviceId = "987ad4bfc0904eb9bfb04b3d330d0486";
		Optional<AggregatedDevice> aggregatedDevice = aggregatedDeviceList.stream().filter(item -> item.getDeviceId().equals(deviceId)).findFirst();
		if (aggregatedDevice.isPresent()) {
			Map<String, String> stats = aggregatedDevice.get().getProperties();
			Assert.assertEquals("Grand Leader", stats.get("DomainClocking"));
			Assert.assertEquals("-22", stats.get("FrequencyOffset(ppm)"));
			Assert.assertEquals("NOT_MUTED", stats.get("MuteStatus"));
			Assert.assertEquals("Multicast Follower", stats.get("PrimaryMulticast"));
			Assert.assertEquals("LOCKED", stats.get("SyncStatus"));
			Assert.assertEquals("None", stats.get("Unicast"));
		}
	}

	/**
	 * Test case for controlling unicast clocking in Dante Domain Manager.
	 * Verifies the successful control of unicast clocking property.
	 */
	@Test
	void testUnicastClockingControl() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(20000);
		communicator.retrieveMultipleStatistics();
		ControllableProperty controllableProperty = new ControllableProperty();
		String property = "ClockSynchronisation#UnicastClocking";
		String value = "0";
		String deviceId = "987ad4bfc0904eb9bfb04b3d330d0486";
		controllableProperty.setProperty(property);
		controllableProperty.setValue(value);
		controllableProperty.setDeviceId(deviceId);
		communicator.controlProperty(controllableProperty);

		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		Optional<AdvancedControllableProperty> advancedControllableProperty = aggregatedDeviceList.get(1).getControllableProperties().stream().filter(item ->
				property.equals(item.getName())).findFirst();
		Assert.assertEquals(value, advancedControllableProperty.get().getValue());
	}

	/**
	 * Test case for controlling preferred leader in Dante Domain Manager.
	 * Verifies the successful control of preferred leader property.
	 */
	@Test
	void testPreferredLeaderControl() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(20000);
		communicator.retrieveMultipleStatistics();
		ControllableProperty controllableProperty = new ControllableProperty();
		String property = "ClockSynchronisation#PreferredLeader";
		String value = "0";
		String deviceId = "987ad4bfc0904eb9bfb04b3d330d0486";
		controllableProperty.setProperty(property);
		controllableProperty.setValue(value);
		controllableProperty.setDeviceId(deviceId);
		communicator.controlProperty(controllableProperty);

		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		Optional<AdvancedControllableProperty> advancedControllableProperty = aggregatedDeviceList.get(1).getControllableProperties().stream().filter(item ->
				property.equals(item.getName())).findFirst();
		Assert.assertEquals(value, advancedControllableProperty.get().getValue());
	}

	/**
	 * Test case for controlling delay requests in Dante Domain Manager.
	 * Verifies the successful control of delay requests property.
	 */
	@Test
	void testDelayRequestsControl() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(20000);
		communicator.retrieveMultipleStatistics();
		ControllableProperty controllableProperty = new ControllableProperty();
		String property = "ClockSynchronisation#DelayRequests";
		String value = "0";
		String deviceId = "987ad4bfc0904eb9bfb04b3d330d0486";
		controllableProperty.setProperty(property);
		controllableProperty.setValue(value);
		controllableProperty.setDeviceId(deviceId);
		communicator.controlProperty(controllableProperty);

		List<AggregatedDevice> aggregatedDeviceList = communicator.retrieveMultipleStatistics();
		Optional<AdvancedControllableProperty> advancedControllableProperty = aggregatedDeviceList.get(1).getControllableProperties().stream().filter(item ->
				property.equals(item.getName())).findFirst();
		Assert.assertEquals(value, advancedControllableProperty.get().getValue());
	}

	/**
	 * Test case for controlling site name in Dante Domain Manager.
	 */
	@Test
	void testControlSiteName() throws Exception {
		communicator.getMultipleStatistics();
		communicator.retrieveMultipleStatistics();
		Thread.sleep(20000);
		communicator.retrieveMultipleStatistics();
		ControllableProperty controllableProperty = new ControllableProperty();
		String property = "Site";
		String value = "AVI-Test";
		String deviceId = "2a8d80f014a04c96921f611c5a4c9d1f";
		controllableProperty.setProperty(property);
		controllableProperty.setValue(value);
		controllableProperty.setDeviceId(deviceId);
		communicator.controlProperty(controllableProperty);
	}
}
