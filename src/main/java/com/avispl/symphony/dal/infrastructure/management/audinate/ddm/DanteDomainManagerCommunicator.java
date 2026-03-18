/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.security.auth.login.FailedLoginException;

import com.avispl.symphony.api.dal.control.Controller;
import com.avispl.symphony.api.dal.dto.control.AdvancedControllableProperty;
import com.avispl.symphony.api.dal.dto.control.ControllableProperty;
import com.avispl.symphony.api.dal.dto.monitor.ExtendedStatistics;
import com.avispl.symphony.api.dal.dto.monitor.Statistics;
import com.avispl.symphony.api.dal.dto.monitor.aggregator.AggregatedDevice;
import com.avispl.symphony.api.dal.error.ResourceNotReachableException;
import com.avispl.symphony.api.dal.monitor.Monitorable;
import com.avispl.symphony.api.dal.monitor.aggregator.Aggregator;
import com.avispl.symphony.dal.aggregator.parser.AggregatedDeviceProcessor;
import com.avispl.symphony.dal.aggregator.parser.PropertiesMapping;
import com.avispl.symphony.dal.aggregator.parser.PropertiesMappingParser;
import com.avispl.symphony.dal.communicator.RestCommunicator;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.AggregatedInformation;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.DanteDomainManagerConstant;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.DanteDomainManagerQuery;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.SystemInformation;
import com.avispl.symphony.dal.util.StringUtils;

/**
 * DanteDomainManagerCommunicator
 * Supported features are:
 * Monitoring Aggregator Device:
 *  <ul>
 *  <li> - Clocking</li>
 *  <li> - Connectivity</li>
 *  <li> - Latency</li>
 *  <li> - NumberOfDevices</li>
 *  <li> - SiteName</li>
 *  <li> - Subscriptions</li>
 *  <ul>
 *
 * General Info Aggregated Device:
 * <ul>
 * <li> - Comments</li>
 * <li> - ConnectedSince</li>
 * <li> - DanteSoftwareVersion</li>
 * <li> - DanteVersion</li>
 * <li> - Description</li>
 * <li> - deviceId</li>
 * <li> - deviceModel</li>
 * <li> - deviceName</li>
 * <li> - deviceOnline</li>
 * <li> - DiscoveryDomainName</li>
 * <li> - DiscoveryType</li>
 * <li> - EnrolmentState</li>
 * <li> - IPAddress</li>
 * <li> - Location</li>
 * <li> - MACAddress</li>
 * <li> - Manufacturer</li>
 * <li> - ProductVersion</li>
 * <li> - Site</li>
 * </ul>
 *
 * ClockSynchronisation Group:
 * <ul>
 * <li> - DomainClocking</li>
 * <li> - FrequencyOffset(ppm)</li>
 * <li> - MuteStatus</li>
 * <li> - PreferredLeader</li>
 * <li> - PrimaryMulticast</li>
 * <li> - SyncStatus</li>
 * <li> - Unicast</li>
 * <li> - UnicastClocking</li>
 * <li> - V1DelayRequests</li>
 * </ul>
 *
 * Status Group:
 * <ul>
 * <li> - Clocking</li>
 * <li> - Connectivity</li>
 * <li> - Latency</li>
 * <li> - Subscriptions</li>
 * </ul>
 * @author Harry / Symphony Dev Team<br>
 * @since 1.0.0
 */
public class DanteDomainManagerCommunicator extends RestCommunicator implements Aggregator, Monitorable, Controller {
	/**
	 * Process that is running constantly and triggers collecting data from Dante Domain Manager SE API endpoints, based on the given timeouts and thresholds.
	 *
	 * @author Harry
	 * @since 1.0.0
	 */
	class DanteDomainManagerDataLoader implements Runnable {
		private volatile boolean inProgress;

		public DanteDomainManagerDataLoader() {
			inProgress = true;
		}

		@Override
		public void run() {
			loop:
			while (inProgress) {
				try {
					TimeUnit.MILLISECONDS.sleep(500);
				} catch (InterruptedException e) {
					logger.info(String.format("Sleep for 0.5 second was interrupted with error message: %s", e.getMessage()));
				}

				if (!inProgress) {
					break loop;
				}

				// next line will determine whether Dante Domain Manager monitoring was paused
				updateAggregatorStatus();
				if (devicePaused) {
					continue loop;
				}
				if (logger.isDebugEnabled()) {
					logger.debug("Fetching other than aggregated device list");
				}

				while (nextDevicesCollectionIterationTimestamp > System.currentTimeMillis()) {
					try {
						TimeUnit.MILLISECONDS.sleep(1000);
					} catch (InterruptedException e) {
						logger.info(String.format("Sleep for 1 second was interrupted with error message: %s", e.getMessage()));
					}
				}

				if (!inProgress) {
					break loop;
				}

				long startCycle = System.currentTimeMillis();
				try {
					if (logger.isDebugEnabled()) {
						logger.debug("Fetching devices list");
					}
					populateDeviceDetails();
				} catch (Exception e) {
					logger.error("Error occurred during device list retrieval: " + e.getMessage(), e);
				}

				try{
					nextDevicesCollectionIterationTimestamp = System.currentTimeMillis() + (getMonitoringRate() * 60000L);
				} catch (NoSuchMethodError error){
					nextDevicesCollectionIterationTimestamp = System.currentTimeMillis() + 60000L;
					logger.warn("Unsupported feature: getMonitoringRate isn't available on current Cloud Connector version.", error);
				}
				lastMonitoringCycleDuration = Math.max((System.currentTimeMillis() - startCycle) / 1000, 1L);

				if (logger.isDebugEnabled()) {
					logger.debug("Finished collecting devices statistics cycle at " + new Date() + ", total duration: " + lastMonitoringCycleDuration);
				}
			}
			// Finished collecting
		}

		/**
		 * Triggers main loop to stop
		 */
		public void stop() {
			inProgress = false;
		}
	}

	/**
	 * Indicates whether a device is considered as paused.
	 * True by default so if the system is rebooted and the actual value is lost -> the device won't start stats
	 * collection unless the {@link DanteDomainManagerCommunicator#retrieveMultipleStatistics()} method is called which will change it
	 * to a correct value
	 */
	private volatile boolean devicePaused = true;

	/**
	 * We don't want the statistics to be collected constantly, because if there's not a big list of devices -
	 * new devices' statistics loop will be launched before the next monitoring iteration. To avoid that -
	 * this variable stores a timestamp which validates it, so when the devices' statistics is done collecting, variable
	 * is set to currentTime + 30s, at the same time, calling {@link #retrieveMultipleStatistics()} and updating the
	 */
	private long nextDevicesCollectionIterationTimestamp;

	/**
	 * This parameter holds timestamp of when we need to stop performing API calls
	 * It used when device stop retrieving statistic. Updated each time of called #retrieveMultipleStatistics
	 */
	private volatile long validRetrieveStatisticsTimestamp;

	/**
	 * Aggregator inactivity timeout. If the {@link DanteDomainManagerCommunicator#retrieveMultipleStatistics()}  method is not
	 * called during this period of time - device is considered to be paused, thus the Cloud API
	 * is not supposed to be called
	 */
	private static final long retrieveStatisticsTimeOut = 3 * 60 * 1000;

	/**
	 * Update the status of the device.
	 * The device is considered as paused if did not receive any retrieveMultipleStatistics()
	 * calls during {@link DanteDomainManagerCommunicator}
	 */
	private synchronized void updateAggregatorStatus() {
		devicePaused = validRetrieveStatisticsTimestamp < System.currentTimeMillis();
	}

	/**
	 * Uptime time stamp to valid one
	 */
	private synchronized void updateValidRetrieveStatisticsTimestamp() {
		validRetrieveStatisticsTimestamp = System.currentTimeMillis() + retrieveStatisticsTimeOut;
		updateAggregatorStatus();
	}

	/**
	 * A mapper for reading and writing JSON using Jackson library.
	 * ObjectMapper provides functionality for converting between Java objects and JSON.
	 * It can be used to serialize objects to JSON format, and deserialize JSON data to objects.
	 */
	ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Executor that runs all the async operations, that is posting and
	 */
	private ExecutorService executorService;

	/**
	 * A private field that represents an instance of the DanteDomainManagerLoader class, which is responsible for loading device data for Dante Domain Manager
	 */
	private DanteDomainManagerDataLoader deviceDataLoader;

	/**
	 * A private final ReentrantLock instance used to provide exclusive access to a shared resource
	 * that can be accessed by multiple threads concurrently. This lock allows multiple reentrant
	 * locks on the same shared resource by the same thread.
	 */
	private final ReentrantLock reentrantLock = new ReentrantLock();

	/**
	 * Adapter metadata properties - adapter version and build date
	 */
	private Properties adapterProperties;

	/**
	 * How much time last monitoring cycle took to finish
	 */
	private long lastMonitoringCycleDuration;

	/**
	 * Private variable representing the local extended statistics.
	 */
	private ExtendedStatistics localExtendedStatistics;

	/**
	 * An instance of the AggregatedDeviceProcessor class used to process and aggregate device-related data.
	 */
	private AggregatedDeviceProcessor aggregatedDeviceProcessor;

	/**
	 * A JSON node containing the response from an aggregator.
	 */
	private List<JsonNode> domainList = Collections.synchronizedList(new ArrayList<>());

	/**
	 * List of aggregated device
	 */
	private List<AggregatedDevice> aggregatedDeviceList = Collections.synchronizedList(new ArrayList<>());

	/**
	 * cache data for aggregated
	 */
	private List<AggregatedDevice> cachedData = Collections.synchronizedList(new ArrayList<>());

	/**
	 * Store the device name
	 */
	private final String aggregatorDeviceName = "DDM";

	/**
	 * current site value
	 */
	private JsonNode currentSiteValue;

	/**
	 * Constructs a new instance of DanteDomainManagerCommunicator.
	 *
	 * @throws IOException If an I/O error occurs while loading the properties mapping YAML file.
	 */
	public DanteDomainManagerCommunicator() throws IOException {
		adapterProperties = new Properties();
		Map<String, PropertiesMapping> mapping = new PropertiesMappingParser().loadYML(DanteDomainManagerConstant.MODEL_MAPPING_AGGREGATED_DEVICE, getClass());
		aggregatedDeviceProcessor = new AggregatedDeviceProcessor(mapping);
		adapterProperties.load(getClass().getResourceAsStream("/version.properties"));
		this.setTrustAllCertificates(true);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Statistics> getMultipleStatistics() throws Exception {
		reentrantLock.lock();
		try {
			Map<String, String> statistics = new HashMap<>();
			Map<String, String> dynamicStatistics = new HashMap<>();
			List<AdvancedControllableProperty> advancedControllableProperties = new ArrayList<>();
			ExtendedStatistics extendedStatistics = new ExtendedStatistics();
			retrieveSystemInfo();
			populateSystemInfo(statistics, advancedControllableProperties);
			extendedStatistics.setStatistics(statistics);
			extendedStatistics.setDynamicStatistics(dynamicStatistics);
			extendedStatistics.setControllableProperties(advancedControllableProperties);
			localExtendedStatistics = extendedStatistics;
		} finally {
			reentrantLock.unlock();
		}
		return Collections.singletonList(localExtendedStatistics);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void controlProperty(ControllableProperty controllableProperty) throws Exception {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void controlProperties(List<ControllableProperty> controllableProperties) throws Exception {
		if (CollectionUtils.isEmpty(controllableProperties)) {
			throw new IllegalArgumentException("ControllableProperties can not be null or empty");
		}
		for (ControllableProperty p : controllableProperties) {
			try {
				controlProperty(p);
			} catch (Exception e) {
				logger.error(String.format("An error occurred during %s control property processing", p.getProperty()), e);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AggregatedDevice> retrieveMultipleStatistics() throws Exception {
		if (executorService == null) {
			executorService = Executors.newFixedThreadPool(1);
			executorService.submit(deviceDataLoader = new DanteDomainManagerDataLoader());
		}
		nextDevicesCollectionIterationTimestamp = System.currentTimeMillis();
		updateValidRetrieveStatisticsTimestamp();
		if (cachedData.isEmpty()) {
			return Collections.emptyList();
		}
		return cloneAndPopulateAggregatedDeviceList();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<AggregatedDevice> retrieveMultipleStatistics(List<String> list) throws Exception {
		return retrieveMultipleStatistics().stream().filter(aggregatedDevice -> list.contains(withAggregatorPrefix(aggregatedDevice.getDeviceId()))).collect(Collectors.toList());
	}

	/**
	 * {@inheritDoc}
	 * set API Key into Header of Request
	 */
	@Override
	protected HttpHeaders putExtraRequestHeaders(HttpMethod httpMethod, String uri, HttpHeaders headers) {
		headers.set("Authorization", this.getPassword());
		return headers;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void authenticate() throws Exception {

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void internalInit() throws Exception {
		if (logger.isDebugEnabled()) {
			logger.debug("Internal init is called.");
		}
		executorService = Executors.newFixedThreadPool(1);
		executorService.submit(deviceDataLoader = new DanteDomainManagerDataLoader());
		super.internalInit();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void internalDestroy() {
		if (logger.isDebugEnabled()) {
			logger.debug("Internal destroy is called.");
		}
		if (deviceDataLoader != null) {
			deviceDataLoader.stop();
			deviceDataLoader = null;
		}
		if (executorService != null) {
			executorService.shutdownNow();
			executorService = null;
		}
		if (localExtendedStatistics != null && localExtendedStatistics.getStatistics() != null && localExtendedStatistics.getControllableProperties() != null) {
			localExtendedStatistics.getStatistics().clear();
			localExtendedStatistics.getControllableProperties().clear();
		}
		domainList = null;
		currentSiteValue = null;
		nextDevicesCollectionIterationTimestamp = 0;
		aggregatedDeviceList.clear();
		cachedData.clear();
		super.internalDestroy();
	}

	/**
	 * Retrieves system information by making a POST request to Dante Domain Manager and updating the domain list.
	 * Throws exceptions in case of errors during the process, such as failed login, resource not reachable, or missing data.
	 *
	 * @throws FailedLoginException If there is an error during login. Please check the credentials.
	 * @throws ResourceNotReachableException If there is an error retrieving system information or the number of sites is 0.
	 */
	private void retrieveSystemInfo() throws Exception {
		JsonNode response = this.doPost(DanteDomainManagerConstant.URL, DanteDomainManagerQuery.SYSTEM_INFO, JsonNode.class);

		if (response.has(DanteDomainManagerConstant.ERRORS) && checkUnauthenticated(response.get(DanteDomainManagerConstant.ERRORS))) {
			throw new FailedLoginException("Unable to login. Please check the credentials");
		}

		if (!response.has(DanteDomainManagerConstant.DATA) || !response.get(DanteDomainManagerConstant.DATA).has(DanteDomainManagerConstant.DOMAINS)) {
			throw new RuntimeException("An error occurred during system information request.");
		}

		if (response.get(DanteDomainManagerConstant.DATA).get(DanteDomainManagerConstant.DOMAINS).isEmpty()) {
			throw new RuntimeException("No domains found for the current account.");
		} else {
			domainList.clear();
			for (JsonNode item : response.get(DanteDomainManagerConstant.DATA).get(DanteDomainManagerConstant.DOMAINS)) {
				domainList.add(item);
			}
		}
	}

	/**
	 * Checks if the provided JSON data contains an "UNAUTHENTICATED" code in the "extensions" field.
	 *
	 * @param data The JSON data to check for unauthenticated status.
	 * @return true if "UNAUTHENTICATED" is found; false otherwise.
	 */
	private boolean checkUnauthenticated(JsonNode data) {
		if (data.isArray()) {
			for (JsonNode item : data) {
				if ("UNAUTHENTICATED".equals(item.get(DanteDomainManagerConstant.EXTENSIONS).get(DanteDomainManagerConstant.CODE).asText())) {
					return true;
				}
			}
		}
		return false;
	}

	/**
	 * Populates system information into the provided stats map and advanced controllable properties list.
	 * System information includes properties from the {@link SystemInformation} enumeration.
	 *
	 * @param stats The map to store system information properties.
	 * @param advancedControllableProperties The list to store advanced controllable properties.
	 */
	private void populateSystemInfo(Map<String, String> stats, List<AdvancedControllableProperty> advancedControllableProperties) {
		List<String> siteNameList = domainList.stream().map(node -> node.get(DanteDomainManagerConstant.NAME).asText()).collect(Collectors.toList());
		if (currentSiteValue == null) {
			currentSiteValue = domainList.get(0);
		} else {
			Optional<JsonNode> matchingDomain = domainList.stream().filter(item -> item.get(DanteDomainManagerConstant.NAME).asText().equals(currentSiteValue.get(DanteDomainManagerConstant.NAME).asText())).findFirst();
			currentSiteValue = matchingDomain.orElse(currentSiteValue);
		}
		for (SystemInformation item : SystemInformation.values()) {
			String propertyName = item.getName();
			String value = getDefaultValueForNullData(currentSiteValue.get(DanteDomainManagerConstant.STATUS).get(item.getValue()).asText());
			JsonNode message = currentSiteValue.get(DanteDomainManagerConstant.STATUS).get("domainAlertMessage").get(item.getValue());
			if (message != null && message.has(DanteDomainManagerConstant.MESSAGE)) {
				stats.put(propertyName + "Message", message.get(DanteDomainManagerConstant.MESSAGE).asText());
			}
			stats.put(propertyName, value);
		}
		//Name
		String name = currentSiteValue.get(DanteDomainManagerConstant.NAME).asText();
		if (domainList.size() > 1) {
			addAdvancedControlProperties(advancedControllableProperties, stats, createDropdown("Sites#" + DanteDomainManagerConstant.SITE_NAME, siteNameList.toArray(new String[0]), name), name);
		} else {
			advancedControllableProperties.removeIf(item -> item.getName().equalsIgnoreCase(DanteDomainManagerConstant.SITE_NAME));
			stats.put("Sites#" + DanteDomainManagerConstant.SITE_NAME + DanteDomainManagerConstant.SPACE, name);
		}
		stats.put("Sites#SiteDeviceCount", String.valueOf(currentSiteValue.get(DanteDomainManagerConstant.DEVICES).size()));
	}

	/**
	 * Populates device details by making a POST request to retrieve information from Dante Domain Manager.
	 * The method clears the existing aggregated device list, processes the response, and updates the list accordingly.
	 * Any error during the process is logged.
	 */
	private void populateDeviceDetails() {
		try {
			JsonNode response = this.doPost(DanteDomainManagerConstant.URL, DanteDomainManagerQuery.DEVICES_INFO, JsonNode.class);
			if (response.has(DanteDomainManagerConstant.DATA) && response.get(DanteDomainManagerConstant.DATA).has(DanteDomainManagerConstant.DOMAINS)) {
				cachedData.clear();
				for (JsonNode domainNode : response.get(DanteDomainManagerConstant.DATA).get(DanteDomainManagerConstant.DOMAINS)) {
					String domainId = domainNode.get(DanteDomainManagerConstant.ID).asText();
					if (checkExistDomainId(domainId)) {
						JsonNode jsonArray = domainNode.get(DanteDomainManagerConstant.DEVICES);
						for (JsonNode jsonNode : jsonArray) {
							JsonNode node = objectMapper.createArrayNode().add(jsonNode);

							String id = jsonNode.get(DanteDomainManagerConstant.ID).asText();
							cachedData.removeIf(item -> item.getDeviceId().equals(id));
							cachedData.addAll(aggregatedDeviceProcessor.extractDevices(node));
						}
					}
				}
			}
		} catch (Exception e) {
			logger.error("An error occurred during aggregated device information request.", e);
		}
	}

	/**
	 * Clones and populates a new list of aggregated devices with mapped monitoring properties.
	 *
	 * @return A new list of {@link AggregatedDevice} objects with mapped monitoring properties.
	 */
	private List<AggregatedDevice> cloneAndPopulateAggregatedDeviceList() {
		aggregatedDeviceList.clear();
		synchronized (cachedData) {
			for (AggregatedDevice item : cachedData) {
				AggregatedDevice aggregatedDevice = new AggregatedDevice();
				Map<String, String> cachedValue = item.getProperties();
				aggregatedDevice.setDeviceId(withAggregatorPrefix(item.getDeviceId()));
				aggregatedDevice.setDeviceModel(item.getDeviceModel());
				aggregatedDevice.setDeviceName(item.getDeviceName());
				aggregatedDevice.setDeviceOnline(item.getDeviceOnline());
				aggregatedDevice.setDeviceOnline(item.getDeviceOnline());

				List<AdvancedControllableProperty> controllableProperties = new ArrayList<>();
				Map<String, String> stats = new HashMap<>();
				Map<String, String> controlStats = new HashMap<>();
				mapMonitoringProperty(cachedValue, stats, controlStats, controllableProperties);
				if (Boolean.TRUE.equals(aggregatedDevice.getDeviceOnline())) {
					stats.putAll(controlStats);
					aggregatedDevice.setControllableProperties(controllableProperties);
				}
				aggregatedDevice.setProperties(stats);
				aggregatedDeviceList.add(aggregatedDevice);
			}
		}
		return aggregatedDeviceList;
	}

	/**
	 * Checks if a given domain ID exists in the list of domains.
	 *
	 * @param id The domain ID to check for existence.
	 * @return true if the domain ID exists in the list; false otherwise.
	 */
	private boolean checkExistDomainId(String id) {
		for (JsonNode item : domainList) {
			if (item.has(DanteDomainManagerConstant.ID) && item.get(DanteDomainManagerConstant.ID).asText().equals(id)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Maps monitoring properties from cached values to statistics and advanced control properties.
	 *
	 * @param cachedValue The cached values map containing raw monitoring data.
	 * @param stats The statistics map to store mapped monitoring properties.
	 * @param statsControl The advanced control map to store properties requiring control.
	 * @param advancedControllableProperties The list of advanced controllable properties to be populated.
	 */
	private void mapMonitoringProperty(Map<String, String> cachedValue, Map<String, String> stats, Map<String, String> statsControl, List<AdvancedControllableProperty> advancedControllableProperties) {
		for (AggregatedInformation property : AggregatedInformation.values()) {
			String name = property.getName();
			String propertyName = property.getGroup() + name;
			String value = getDefaultValueForNullData(cachedValue.get(name));
			switch (property) {
				case CLOCKING:
				case LATENCY:
				case CONNECTIVITY:
				case SUBSCRIPTIONS:
					if (StringUtils.isNotNullOrEmpty(cachedValue.get(name + "Message"))) {
						value += " (" + cachedValue.get(name + "Message") + ")";
					}
					stats.put(propertyName, value);
					break;
				case CONNECTED_SINCE:
					stats.put(propertyName, convertDateTimeFormat(value));
					break;
				case DOMAIN_CLOCKING:
					stats.put(propertyName, DanteDomainManagerConstant.TRUE.equals(value) ? "Grand Leader" : DanteDomainManagerConstant.NOT_AVAILABLE);
					break;
				case UNICAST:
					String unicastFollower = getDefaultValueForNullData(cachedValue.get("UnicastFollower"));
					String unicastLeader = getDefaultValueForNullData(cachedValue.get("UnicastLeader"));
					if (DanteDomainManagerConstant.TRUE.equals(unicastLeader) && DanteDomainManagerConstant.TRUE.equals(unicastFollower)) {
						value = "Unicast Leader, Unicast Follower";
					} else if (DanteDomainManagerConstant.TRUE.equals(unicastLeader)) {
						value = "Unicast Leader";
					} else if (DanteDomainManagerConstant.TRUE.equals(unicastFollower)) {
						value = "Unicast Follower";
					} else {
						value = DanteDomainManagerConstant.NOT_AVAILABLE;
					}
					stats.put(propertyName, value);
					break;
				case PRIMARY_MULTICAST:
					String subNet = getDefaultValueForNullData(cachedValue.get("Subnet"));
					String netMask = getDefaultValueForNullData(cachedValue.get("Netmask"));
					String newValue = DanteDomainManagerConstant.TRUE.equals(value) ? "Multicast Leader" : "Multicast Follower";
					if (!DanteDomainManagerConstant.NOT_AVAILABLE.equals(subNet) && !DanteDomainManagerConstant.NOT_AVAILABLE.equals(netMask)) {
						newValue += " (" + subNet + "/" + netMask + ")";
					}
					stats.put(propertyName, newValue);
					break;
				case EXTERNAL_WORD_CLOCK:
				case LEADER:
				case UNICAST_CLOCKING:
					if (DanteDomainManagerConstant.TRUE.equals(getDefaultValueForNullData(cachedValue.get(name + DanteDomainManagerConstant.CAPABILITY)))) {
						addAdvancedControlProperties(advancedControllableProperties, statsControl,
								createSwitch(propertyName, DanteDomainManagerConstant.TRUE.equals(value) ? 1 : 0, DanteDomainManagerConstant.OFF, DanteDomainManagerConstant.ON),
								DanteDomainManagerConstant.TRUE.equals(value) ? DanteDomainManagerConstant.NUMBER_ONE : DanteDomainManagerConstant.ZERO);
					}
					break;
				case DELAY_REQUEST:
					if (DanteDomainManagerConstant.TRUE.equals(getDefaultValueForNullData(cachedValue.get(name + DanteDomainManagerConstant.CAPABILITY)))) {
						addAdvancedControlProperties(advancedControllableProperties, statsControl,
								createSwitch(propertyName, DanteDomainManagerConstant.TRUE.equals(value) ? 1 : 0, "Multicast", "Unicast"),
								DanteDomainManagerConstant.TRUE.equals(value) ? DanteDomainManagerConstant.NUMBER_ONE : DanteDomainManagerConstant.ZERO);
					}
					break;
				default:
					stats.put(propertyName, value);
			}
		}
	}

	/**
	 * Converts a date-time string from the default format to the target format with GMT timezone.
	 *
	 * @param inputDateTime The input date-time string in the default format.
	 * @return The date-time string after conversion to the target format with GMT timezone.
	 * Returns {@link DanteDomainManagerConstant#NOT_AVAILABLE} if there is an error during conversion.
	 * @throws Exception If there is an error parsing the input date-time string.
	 */
	private String convertDateTimeFormat(String inputDateTime) {
		if (DanteDomainManagerConstant.NOT_AVAILABLE.equals(inputDateTime)) {
			return inputDateTime;
		}
		try {
			SimpleDateFormat inputFormat = new SimpleDateFormat(DanteDomainManagerConstant.DEFAULT_FORMAT_DATETIME);
			inputFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

			SimpleDateFormat outputFormat = new SimpleDateFormat(DanteDomainManagerConstant.TARGET_FORMAT_DATETIME);
			outputFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

			Date date = inputFormat.parse(inputDateTime);
			return outputFormat.format(date);
		} catch (Exception e) {
			logger.warn(String.format("Failed to convert date time. input='%s'. Error: %s", inputDateTime, e.getMessage()));
			return DanteDomainManagerConstant.NOT_AVAILABLE;
		}
	}

	/**
	 * check value is null or empty
	 *
	 * @param value input value
	 * @return value after checking
	 */
	private String getDefaultValueForNullData(String value) {
		return StringUtils.isNotNullOrEmpty(value) ? value : DanteDomainManagerConstant.NOT_AVAILABLE;
	}

	/**
	 * Uptime is received in seconds, need to normalize it and make it human-readable, like
	 * 1 day 5 hour 12 minute 55 minute
	 * Incoming parameter is may have a decimal point, so in order to safely process this - it's rounded first.
	 * We don't need to add a segment of time if it's 0.
	 *
	 * @param uptimeSeconds value in seconds
	 * @return string value of format 'x d x hr x min x sec'
	 */
	public static String normalizeUptime(long uptimeSeconds) {
		StringBuilder normalizedUptime = new StringBuilder();

		long seconds = uptimeSeconds % 60;
		long minutes = uptimeSeconds % 3600 / 60;
		long hours = uptimeSeconds % 86400 / 3600;
		long days = uptimeSeconds / 86400;

		if (days > 0) {
			normalizedUptime.append(days).append(" d ");
		}
		if (hours > 0) {
			normalizedUptime.append(hours).append(" hr ");
		}
		if (minutes > 0) {
			normalizedUptime.append(minutes).append(" min ");
		}
		if (seconds > 0 || normalizedUptime.length() == 0) {
			normalizedUptime.append(seconds).append(" sec");
		}
		return normalizedUptime.toString().trim();
	}

	/**
	 * Create switch is control property for metric
	 *
	 * @param name the name of property
	 * @param status initial status (0|1)
	 * @return AdvancedControllableProperty switch instance
	 */
	private AdvancedControllableProperty createSwitch(String name, int status, String labelOff, String labelOn) {
		AdvancedControllableProperty.Switch toggle = new AdvancedControllableProperty.Switch();
		toggle.setLabelOff(labelOff);
		toggle.setLabelOn(labelOn);

		AdvancedControllableProperty advancedControllableProperty = new AdvancedControllableProperty();
		advancedControllableProperty.setName(name);
		advancedControllableProperty.setValue(status);
		advancedControllableProperty.setType(toggle);
		advancedControllableProperty.setTimestamp(new Date());

		return advancedControllableProperty;
	}

	/***
	 * Create dropdown advanced controllable property
	 *
	 * @param name the name of the control
	 * @param values  The array of values for the dropdown options.
	 * @param initialValue initial value of the control
	 * @return AdvancedControllableProperty dropdown instance
	 */
	private AdvancedControllableProperty createDropdown(String name, String[] values, String initialValue) {
		AdvancedControllableProperty.DropDown dropDown = new AdvancedControllableProperty.DropDown();
		dropDown.setOptions(values);
		dropDown.setLabels(values);

		return new AdvancedControllableProperty(name, new Date(), dropDown, initialValue);
	}

	/**
	 * Adds the aggregator device name as a prefix to the given device ID.
	 *
	 * @param deviceId the original device identifier (without prefix)
	 * @return the device ID with the aggregator prefix applied
	 */
	private String withAggregatorPrefix(String deviceId) {
		return aggregatorDeviceName + "_" + deviceId;
	}

	/**
	 * Add addAdvancedControlProperties if advancedControllableProperties different empty
	 *
	 * @param advancedControllableProperties advancedControllableProperties is the list that store all controllable properties
	 * @param stats store all statistics
	 * @param property the property is item advancedControllableProperties
	 * @throws IllegalStateException when exception occur
	 */
	private void addAdvancedControlProperties(List<AdvancedControllableProperty> advancedControllableProperties, Map<String, String> stats, AdvancedControllableProperty property, String value) {
		if (property != null) {
			for (AdvancedControllableProperty controllableProperty : advancedControllableProperties) {
				if (controllableProperty.getName().equals(property.getName())) {
					advancedControllableProperties.remove(controllableProperty);
					break;
				}
			}
			if (StringUtils.isNotNullOrEmpty(value)) {
				stats.put(property.getName(), value);
			} else {
				stats.put(property.getName(), DanteDomainManagerConstant.NOT_AVAILABLE);
			}
			advancedControllableProperties.add(property);
		}
	}
}
