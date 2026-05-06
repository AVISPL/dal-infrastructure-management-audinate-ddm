/*
 *  Copyright (c) 2026 AVI-SPL, Inc. All Rights Reserved.
 */

package com.avispl.symphony.dal.infrastructure.management.audinate.ddm;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.core.type.TypeReference;
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
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.AggregatedControllableProperty;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.AggregatedInformation;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.ClockingGroupInfo;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.DanteDomainManagerConstant;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.DanteDomainManagerQuery;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.common.SystemInformation;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.dto.ReceiveChannelDTO;
import com.avispl.symphony.dal.infrastructure.management.audinate.ddm.dto.TransmitChannelDTO;
import com.avispl.symphony.dal.util.StringUtils;
import static com.avispl.symphony.dal.util.ControllablePropertyFactory.*;

/**
 * DanteDomainManagerCommunicator
 * Supported features are:
 * Monitoring Aggregator Device:
 *  <ul>
 *  <li> - Clocking</li>
 *  <li> - Connectivity</li>
 *  <li> - Latency</li>
 *  <li> - DomainDeviceCount</li>
 *  <li> - DomainName</li>
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
 * <li> - Domain</li>
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

	/** Set of group filter for {@code displayPropertyGroups}. */
	private static final Set<String> GROUP_FILTERS = Set.of(
			DanteDomainManagerConstant.CLOCK_SYNCHRONISATION,
			DanteDomainManagerConstant.STATUS_GROUP_FILTER,
			DanteDomainManagerConstant.DOMAIN,
			DanteDomainManagerConstant.TRANSMIT,
			DanteDomainManagerConstant.RECEIVE,
			DanteDomainManagerConstant.GENERAL
	);

	/** Indicates whether groups are displayed; defaults is General. */
	private final Set<String> displayPropertyGroups = new HashSet<>(Set.of(DanteDomainManagerConstant.GENERAL));

	/**
	 * Returns a comma-separated list of property group names that are configured to be displayed.
	 *
	 * @return a comma-separated string of display property group names; may be empty if no groups are configured
	 */
	public String getDisplayPropertyGroups() {
		return this.displayPropertyGroups.stream()
				.sorted()
				.collect(Collectors.joining(DanteDomainManagerConstant.COMMA_SPACE));
	}

	/**
	 * Sets the display property groups based on a comma-separated list.
	 * <p>
	 * Trims values automatically. If All is present, SUPPORTED_GROUP_FILTERS are added.
	 * Invalid groups trigger a warning and only the default group applied. {@code null} or empty input is ignored.
	 * </p>
	 *`
	 * @param displayPropertyGroups comma-separated group names; may be {@code null} or empty
	 */
	public void setDisplayPropertyGroups(String displayPropertyGroups) {
		if (StringUtils.isNullOrEmpty(displayPropertyGroups, true)) {
			return;
		}
		Set<String> checkedGroups = Arrays.stream(displayPropertyGroups.split(DanteDomainManagerConstant.COMMA))
				.map(String::trim)
				.filter(p -> !p.isEmpty())
				.collect(Collectors.toSet());

		this.displayPropertyGroups.clear();

		if (checkedGroups.contains(DanteDomainManagerConstant.ALL)) {
			this.displayPropertyGroups.addAll(GROUP_FILTERS);
			return;
		}

		if (!CollectionUtils.containsAny(GROUP_FILTERS, checkedGroups)) {
			this.logger.warn("No valid display property groups found from input: '%s'".formatted(displayPropertyGroups));
		}
		this.displayPropertyGroups.add(DanteDomainManagerConstant.GENERAL);
		checkedGroups.stream().filter(GROUP_FILTERS::contains).forEach(this.displayPropertyGroups::add);
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
	 * Device adapter instantiation timestamp.
	 */
	private long adapterInitializationTimestamp;

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

	private String domainNameFilter = "";

	/**
	 * Retrieves {@link #domainNameFilter}
	 *
	 * @return value of {@link #domainNameFilter}
	 */
	public String getDomainNameFilter() {
		return domainNameFilter;
	}

	/**
	 * Sets {@link #domainNameFilter} value
	 *
	 * @param domainNameFilter new value of {@link #domainNameFilter}
	 */
	public void setDomainNameFilter(String domainNameFilter) {
		this.domainNameFilter = domainNameFilter;
	}

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
			ExtendedStatistics extendedStatistics = new ExtendedStatistics();
			retrieveMetadata(statistics, dynamicStatistics);
			retrieveSystemInfo();
			if(validateGroupDisplay(DanteDomainManagerConstant.DOMAIN)){
				populateDomainInfo(statistics);
			}
			extendedStatistics.setStatistics(statistics);
			extendedStatistics.setDynamicStatistics(dynamicStatistics);
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
		reentrantLock.lock();
		try {
			String property = controllableProperty.getProperty();
			String deviceId = controllableProperty.getDeviceId();
			String value = String.valueOf(controllableProperty.getValue());

			String[] propertyList = property.split(DanteDomainManagerConstant.HASH);
			String propertyName = property;
			if (property.contains(DanteDomainManagerConstant.HASH)) {
				propertyName = propertyList[1];
			}
			Optional<AggregatedDevice> aggregatedDevice = aggregatedDeviceList.stream().filter(item -> item.getDeviceId().equals(deviceId)).findFirst();
			if (aggregatedDevice.isPresent()) {
				AggregatedInformation item = AggregatedInformation.getByDefaultName(propertyName);
				AggregatedControllableProperty aggregatedProperty = AggregatedControllableProperty.getByDefaultName(propertyName);
				switch (item) {
					case LEADER:
					case EXTERNAL_WORD_CLOCK:
					case UNICAST_CLOCKING:
						String requestValue = DanteDomainManagerConstant.NUMBER_ONE.equals(value) ? DanteDomainManagerConstant.TRUE : DanteDomainManagerConstant.FALSE;
						sendCommandToControlClockSync(deviceId, requestValue, aggregatedProperty);
						updateCacheValue(deviceId, propertyName, requestValue);
						break;
					case DELAY_REQUEST:
						String currentValue = "Unicast".equals(value) ? DanteDomainManagerConstant.TRUE : DanteDomainManagerConstant.FALSE;
						sendCommandToControlClockSync(deviceId, currentValue, aggregatedProperty);
						updateCacheValue(deviceId, propertyName, currentValue);
						break;

					default:
						if (logger.isWarnEnabled()) {
							logger.warn(String.format("Unable to execute %s command on device %s: Not Supported", property, deviceId));
						}
						break;
				}
			} else {
				throw new IllegalArgumentException(String.format("Unable to control property: %s as the device does not exist.", property));
			}
		} finally {
			reentrantLock.unlock();
		}
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
		return retrieveMultipleStatistics().stream().filter(aggregatedDevice -> list.contains(aggregatedDevice.getDeviceId())).collect(Collectors.toList());
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
		adapterInitializationTimestamp = System.currentTimeMillis();
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
		nextDevicesCollectionIterationTimestamp = 0;
		aggregatedDeviceList.clear();
		cachedData.clear();
		displayPropertyGroups.clear();
		super.internalDestroy();
	}

	/**
	 * Retrieves metadata information and updates the provided statistics and dynamic map.
	 *
	 * @param stats the map where statistics will be stored
	 * @param dynamicStatistics the map where dynamic statistics will be stored
	 */
	private void retrieveMetadata(Map<String, String> stats, Map<String, String> dynamicStatistics) {
		try {
			dynamicStatistics.put(DanteDomainManagerConstant.MONITORING_CYCLE_DURATION, String.valueOf(lastMonitoringCycleDuration));
			stats.put(DanteDomainManagerConstant.ADAPTER_VERSION,
					getDefaultValueForNullData(adapterProperties.getProperty("aggregator.version")));
			stats.put(DanteDomainManagerConstant.ADAPTER_BUILD_DATE,
					getDefaultValueForNullData(adapterProperties.getProperty("aggregator.build.date")));
			long adapterUptime = System.currentTimeMillis() - adapterInitializationTimestamp;

			stats.put(DanteDomainManagerConstant.ADAPTER_UPTIME_MIN, String.valueOf(adapterUptime / (1000 * 60)));
			stats.put(DanteDomainManagerConstant.ADAPTER_UPTIME, normalizeUptime(adapterUptime / 1000));
			stats.put(DanteDomainManagerConstant.ACTIVE_PROPERTY_GROUPS, this.getDisplayPropertyGroups());
			try{
				stats.put(DanteDomainManagerConstant.SYSTEM_MONITORING_CYCLE, String.valueOf(getMonitoringRate()));
			}catch (NoSuchMethodError error){
				logger.warn("Unsupported feature: getMonitoringRate isn't available on current Cloud Connector version.", error);
			}
			dynamicStatistics.put(DanteDomainManagerConstant.MONITORED_DEVICES_TOTAL, String.valueOf(aggregatedDeviceList.size()));
		} catch (Exception e) {
			logger.error("Failed to populate metadata information", e);
		}
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

		if (response == null) {
			throw new RuntimeException("System info response is null.");
		}
		JsonNode errors = response.get(DanteDomainManagerConstant.ERRORS);
		if (errors != null && checkUnauthenticated(errors)) {
			throw new FailedLoginException("Unable to login. Please check the credentials");
		}

		JsonNode dataNode = response.get(DanteDomainManagerConstant.DATA);
		if (dataNode == null) {
			throw new RuntimeException("Missing 'data' in system info response.");
		}
		JsonNode domainsNode = dataNode.get(DanteDomainManagerConstant.DOMAINS);
		if (domainsNode == null || !domainsNode.isArray()) {
			throw new RuntimeException("An error occurred during system information request.");
		}
		if (domainsNode.isEmpty()) {
			throw new RuntimeException("No domains found for the current account.");
		}
		synchronized (domainList) {
			domainList.clear();
			for (JsonNode item : domainsNode) {
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
	 */
	private void populateDomainInfo(Map<String, String> stats) {
		for (JsonNode domain : domainList) {
			String domainName = getText(domain, DanteDomainManagerConstant.NAME);
			String groupDomain = buildGroupDomain(domainName);

			JsonNode statusNode = domain.get(DanteDomainManagerConstant.STATUS);
			JsonNode alertNode = statusNode != null ? statusNode.get("domainAlertMessage") : null;

			stats.put(buildKey(groupDomain, DanteDomainManagerConstant.DOMAIN_NAME), domainName);
			stats.put(buildKey(groupDomain, "DomainDeviceCount"),
					String.valueOf(domain.path(DanteDomainManagerConstant.DEVICES).size()));

			populateSystemInfo(stats, groupDomain, statusNode, alertNode);
			populateClockingGroup(stats, groupDomain, domain.get("clockingGroup"));
		}
	}

	/**
	 * Populates clocking group information into the stats map based on the domain mode.
	 * The displayed fields vary depending on the mode (DEFAULT, SMPTE, AES67) and
	 * PTP configuration (Default or Custom).
	 *
	 * @param stats         the map storing domain statistics
	 * @param groupDomain   the domain group prefix for stat keys
	 * @param clockingGroup the JSON node containing clocking group data
	 */
	private void populateClockingGroup(Map<String, String> stats, String groupDomain, JsonNode clockingGroup) {
		if (clockingGroup == null) return;
		String mode = getText(clockingGroup, ClockingGroupInfo.MODE.getField());
		JsonNode ptp = clockingGroup.get("ptp");
		JsonNode rtp = clockingGroup.get("rtp");

		boolean isCustom = ptp != null
				&& ptp.has(ClockingGroupInfo.PTP_CONFIGURATION.getField())
				&& ptp.get(ClockingGroupInfo.PTP_CONFIGURATION.getField()).asBoolean();

		String ptpConfig = isCustom ? "Custom" : "Default";

		stats.put(buildKey(groupDomain, ClockingGroupInfo.MODE.getName()), "DEFAULT".equals(mode) ? uppercaseFirstCharacter(mode.toLowerCase()) : mode);
		switch (mode.toUpperCase()) {
			case "DEFAULT":
				stats.put(buildKey(groupDomain, ClockingGroupInfo.PTP_CONFIGURATION.getName()), ptpConfig);
				if (isCustom) {
					putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_DOMAIN, ptp);
					putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY1, ptp);
					putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY2, ptp);
					putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_SYNC_INTERVAL, ptp);
					putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_ANNOUNCE_INTERVAL, ptp);
				}
				break;
			case "SMPTE":
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_V1_MULTICAST, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_DOMAIN, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY1, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY2, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_SYNC_INTERVAL, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_ANNOUNCE_INTERVAL, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_MULTICAST_TTL, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_SLAVE_ONLY, ptp);

				putIfPresent(stats, groupDomain, ClockingGroupInfo.RTP_TRANSMIT_PORT, rtp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.RTP_SYSTEM_PACKET_TIME, rtp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.RTP_RX_LATENCY, rtp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.RTP_PREFIX_V4, rtp);
				break;
			case "AES67":
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY1, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_PRIORITY2, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.PTP_MULTICAST_TTL, ptp);
				putIfPresent(stats, groupDomain, ClockingGroupInfo.RTP_PREFIX_V4, rtp);
				break;

			default:
				break;
		}
	}

	/**
	 * Populates system status information and corresponding alert messages into the stats map.
	 * Values are derived from the provided status and alert nodes based on {@link SystemInformation}.
	 *
	 * @param stats        the map storing domain statistics
	 * @param groupDomain  the domain group prefix for stat keys
	 * @param statusNode   the JSON node containing system status values
	 * @param alertNode    the JSON node containing alert messages (optional)
	 */
	private void populateSystemInfo(Map<String, String> stats, String groupDomain, JsonNode statusNode, JsonNode alertNode) {
		if (statusNode == null) return;
		for (SystemInformation item : SystemInformation.values()) {
			String propertyName = item.getName();
			String key = buildKey(groupDomain, propertyName);
			String value = getDefaultValueForNullData(
					getText(statusNode, item.getValue())
			);
			stats.put(key, value);
			if (alertNode != null) {
				JsonNode messageNode = alertNode.get(item.getValue());
				if (messageNode != null && messageNode.has(DanteDomainManagerConstant.MESSAGE)) {
					stats.put(propertyName + "Message", messageNode.get(DanteDomainManagerConstant.MESSAGE).asText());
				}
			}
		}
	}

	/**
	 * Populates device details by making a POST request to retrieve information from Dante Domain Manager.
	 * The method clears the existing aggregated device list, processes the response, and updates the list accordingly.
	 * Any error during the process is logged.
	 */
	private void populateDeviceDetails() {
		try {
			JsonNode response = fetchDeviceResponse();
			if (response == null || !response.has(DanteDomainManagerConstant.DATA)) {
				return;
			}
			cachedData.clear();
			JsonNode dataNode = response.get(DanteDomainManagerConstant.DATA);

			if (dataNode.has("domain")) {
				processDevices(dataNode.get("domain"));
			}	else if (dataNode.has(DanteDomainManagerConstant.DOMAINS)) {
				for (JsonNode domainNode : dataNode.get(DanteDomainManagerConstant.DOMAINS)) {
					String domainId = domainNode.get(DanteDomainManagerConstant.ID).asText();
					if (checkExistDomainId(domainId)) {
						processDevices(domainNode);
					}
				}
			}
		} catch (Exception e) {
			logger.error("An error occurred during aggregated device information request.", e);
		}
	}

	/**
	 * Processes devices within a given domain node and updates the cached data.
	 * Each device is extracted and transformed using {@link #aggregatedDeviceProcessor},
	 * replacing any existing entries with the same device ID.
	 *
	 * @param domainNode the JSON node containing device information for a domain
	 */
	private void processDevices(JsonNode domainNode) {
		JsonNode devices = domainNode.get(DanteDomainManagerConstant.DEVICES);
		if (devices == null || !devices.isArray()) {
			return;
		}
		for (JsonNode device : devices) {
			JsonNode node = objectMapper.createArrayNode().add(device);
			String deviceId = device.get(DanteDomainManagerConstant.ID).asText();
			cachedData.removeIf(item -> item.getDeviceId().equals(deviceId));
			cachedData.addAll(aggregatedDeviceProcessor.extractDevices(node));
		}
	}

	/**
	 * Fetches device information from the API based on the current domain filter.
	 * If {@link #domainNameFilter} is provided, it resolves the corresponding domain ID
	 * and retrieves devices for that specific domain. Otherwise, it retrieves devices
	 * across all domains.
	 * @throws Exception if an error occurs during the API request
	 */
	private JsonNode fetchDeviceResponse() throws Exception {
		String command;
		if (StringUtils.isNotNullOrEmpty(domainNameFilter)) {
			String domainId = getDomainIdByName(domainNameFilter);
			if (domainId == null) {
				logger.warn(String.format("Domain name '%s' not found", domainNameFilter));
				return null;
			}
			command = String.format(DanteDomainManagerQuery.DEVICES_BY_DOMAIN_ID, domainId);
		} else {
			command = DanteDomainManagerQuery.DEVICES_INFO;
		}
		return this.doPost(DanteDomainManagerConstant.URL, command, JsonNode.class);
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
				aggregatedDevice.setDeviceId(item.getDeviceId());
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
			String groupForFilter = normalizeGroup(property.getGroup());
			if (!validateGroupDisplay(groupForFilter)) {
				continue;
			}
			String name = property.getName();
			String propertyName = property.getGroup() + name;
			String value = getDefaultValueForNullData(cachedValue.get(name));
			String domainMode = getDefaultValueForNullData(cachedValue.get("DomainMode"));
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
				case LEADER:
					if(!"SMPTE".equals(domainMode) && DanteDomainManagerConstant.TRUE.equals(getDefaultValueForNullData(cachedValue.get(name + DanteDomainManagerConstant.CAPABILITY)))){
						addAdvancedControlProperties(advancedControllableProperties, statsControl,
								createSwitch(propertyName, DanteDomainManagerConstant.TRUE.equals(value) ? 1 : 0, DanteDomainManagerConstant.OFF, DanteDomainManagerConstant.ON),
								DanteDomainManagerConstant.TRUE.equals(value) ? DanteDomainManagerConstant.NUMBER_ONE : DanteDomainManagerConstant.ZERO);
					}
					break;
				case PTP_PRIORITY1:
					handleSMPTEPriority(domainMode, cachedValue, stats, propertyName, value, "DomainPTPV2Priority1");
					break;
				case PTP_PRIORITY2:
					handleSMPTEPriority(domainMode, cachedValue, stats, propertyName, value, "DomainPTPV2Priority2");
					break;
				case EXTERNAL_WORD_CLOCK:
				case UNICAST_CLOCKING:
					if (DanteDomainManagerConstant.TRUE.equals(getDefaultValueForNullData(cachedValue.get(name + DanteDomainManagerConstant.CAPABILITY)))) {
						addAdvancedControlProperties(advancedControllableProperties, statsControl,
								createSwitch(propertyName, DanteDomainManagerConstant.TRUE.equals(value) ? 1 : 0, DanteDomainManagerConstant.OFF, DanteDomainManagerConstant.ON),
								DanteDomainManagerConstant.TRUE.equals(value) ? DanteDomainManagerConstant.NUMBER_ONE : DanteDomainManagerConstant.ZERO);
					}
					break;
				case DELAY_REQUEST:
					if (DanteDomainManagerConstant.TRUE.equals(getDefaultValueForNullData(cachedValue.get(name + DanteDomainManagerConstant.CAPABILITY)))) {
						List<String> listDelayRequest = Arrays.asList("Multicast", "Unicast");
						addAdvancedControlProperties(advancedControllableProperties, statsControl,
								createDropdown(propertyName, listDelayRequest, DanteDomainManagerConstant.TRUE.equals(value) ? "Unicast" : "Multicast"),
								DanteDomainManagerConstant.TRUE.equals(value) ? "Unicast" : "Multicast");
					}
					break;
				case RECEIVE_CHANNELS:
					try {
						List<ReceiveChannelDTO> channelList = objectMapper.readValue(value, new TypeReference<List<ReceiveChannelDTO>>() {
						});
						if (!channelList.isEmpty()) {
							for (ReceiveChannelDTO item : channelList) {
								String channelName = item.getName();
									stats.put(DanteDomainManagerConstant.RECEIVE_GROUP + channelName + DanteDomainManagerConstant.HASH + "SubscribedChannel", item.getSubscribedChannel());
									stats.put(DanteDomainManagerConstant.RECEIVE_GROUP + channelName + DanteDomainManagerConstant.HASH + "SubscribedDevice", item.getSubscribedDevice());
									stats.put(DanteDomainManagerConstant.RECEIVE_GROUP + channelName + DanteDomainManagerConstant.HASH + "MediaType", item.getMediaType());
									stats.put(DanteDomainManagerConstant.RECEIVE_GROUP + channelName + DanteDomainManagerConstant.HASH + "Name", item.getName());
							}
						}
					} catch (Exception e) {
						logger.error("Error occurred while retrieving receive channels", e);
					}
					break;
				case TRANSMIT_CHANNELS:
					try{
						List<TransmitChannelDTO> txList = objectMapper.readValue(value, new TypeReference<List<TransmitChannelDTO>>() {});
						if(!txList.isEmpty()){
							for(TransmitChannelDTO item : txList){
								String channelName = item.getName();
								stats.put(DanteDomainManagerConstant.TRANSMIT_GROUP + channelName + DanteDomainManagerConstant.HASH + "ID", item.getId());
								stats.put(DanteDomainManagerConstant.TRANSMIT_GROUP + channelName + DanteDomainManagerConstant.HASH + "Name", item.getName());
								stats.put(DanteDomainManagerConstant.TRANSMIT_GROUP + channelName + DanteDomainManagerConstant.HASH + "MediaType", item.getMediaType());
							}
						}
					} catch (Exception e) {
						logger.error("Error occurred while retrieving transmit channels", e);
					}
					break;
				default:
					stats.put(propertyName, value);
			}
		}
	}

	/**
	 * Sends a control command to the specified device for a controllable property.
	 *
	 * @param deviceId The ID of the device to control.
	 * @param value The value to set for the controllable property.
	 * @param property The controllable property to control.
	 */
	private void sendCommandToControlClockSync(String deviceId, String value, AggregatedControllableProperty property) {
		try {
			String command = String.format(DanteDomainManagerQuery.CONTROL_CLOCK_SYNC, property.getCommandParam(), property.getCommandName(), deviceId, value);
			JsonNode response = this.doPost(DanteDomainManagerConstant.URL, command, JsonNode.class);
			if (response.has(DanteDomainManagerConstant.ERRORS)) {
				throw new IllegalArgumentException("The command response is error");
			}

		} catch (Exception e) {
			throw new IllegalArgumentException(
					String.format("Can't control %s with value is %s. %s", property.getName(), DanteDomainManagerConstant.TRUE.equals(value) ? DanteDomainManagerConstant.ON : DanteDomainManagerConstant.OFF, e.getMessage()));
		}
	}

	/**
	 * Updates the cache value for a specified property in the aggregated device list.
	 *
	 * @param deviceId The ID of the device whose cache value needs to be updated.
	 * @param name The name of the property to be updated.
	 * @param value The new value to set for the property.
	 */
	private void updateCacheValue(String deviceId, String name, String value) {
		cachedData.stream().filter(item -> deviceId.equals(item.getDeviceId()))
				.findFirst().ifPresent(item -> item.getProperties().put(name, value));
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

	/**
	 * capitalize the first character of the string
	 *
	 * @param input input string
	 * @return string after fix
	 */
	private String uppercaseFirstCharacter(String input) {
		return Character.toUpperCase(input.charAt(0)) + input.substring(1);
	}

	/**
	 * Builds a composite key using the group prefix and property name.
	 *
	 * @param group    the group prefix
	 * @param property the property name
	 * @return the combined key in the format "group#property"
	 */
	private String buildKey(String group, String property) {
		return group + DanteDomainManagerConstant.HASH + property;
	}

	/**
	 * Constructs a sanitized domain group identifier based on the domain name.
	 *
	 * @param domainName the domain name
	 * @return the formatted and sanitized group domain string
	 */
	private String buildGroupDomain(String domainName) {
		return sanitizeGroup(SystemInformation.values()[0].getGroup() + "_" + domainName);
	}

	/**
	 * Retrieves a text value from the given JSON node for the specified field.
	 * Returns a default value if the field is missing or null.
	 *
	 * @param node  the source JSON node
	 * @param field the field name to extract
	 * @return the field value as text, or a default fallback if unavailable
	 */
	private String getText(JsonNode node, String field) {
		return node != null && node.has(field) && !node.get(field).isNull()
				? node.get(field).asText()
				: DanteDomainManagerConstant.NOT_AVAILABLE;
	}

	/**
	 * Adds a property to the stats map if the specified field exists and is not null in the given JSON node.
	 *
	 * @param stats       the map storing statistics
	 * @param groupDomain the domain group prefix for the key
	 * @param field       the clocking group field definition
	 * @param node        the JSON node containing the data
	 */
	private void putIfPresent(Map<String, String> stats, String groupDomain, ClockingGroupInfo field, JsonNode node) {
		if (node == null) return;
		JsonNode valueNode = node.get(field.getField());
		if (valueNode != null && !valueNode.isNull()) {
			stats.put(buildKey(groupDomain, field.getName()), valueNode.asText());
		}
	}

	/**
	 * Handles SMPTE-specific priority properties by applying a fallback value when needed.
	 * If the current value is not available, it retrieves the corresponding domain-level value.
	 *
	 * @param domainMode  the current domain mode
	 * @param cachedValue the cached domain values used for fallback
	 * @param stats       the map storing statistics
	 * @param propertyName the property key to update
	 * @param value       the current property value
	 * @param fallbackKey the key used to retrieve the fallback value from cache
	 */
	private void handleSMPTEPriority(String domainMode, Map<String, String> cachedValue, Map<String, String> stats, String propertyName, String value, String fallbackKey) {
		if (!"SMPTE".equals(domainMode)) return;
		String fallbackValue = getDefaultValueForNullData(cachedValue.get(fallbackKey));
		stats.put(propertyName, DanteDomainManagerConstant.NOT_AVAILABLE.equals(value) ? fallbackValue : value);
	}

	/**
	 * Sanitizes a string to be used as a group key by replacing non-alphanumeric
	 * characters (except dots) with underscores and trimming leading/trailing underscores.
	 *
	 * @param input the raw group name
	 * @return a sanitized string suitable for use as a group identifier
	 */
	private String sanitizeGroup(String input) {
		return input.replaceAll("[^a-zA-Z0-9.\\/-]+", "_")
				.replaceAll("^_|_$", "");
	}

	/**
	 * Resolves the domain ID corresponding to the given domain name.
	 * Performs a case-insensitive match against the available domain list.
	 *
	 * @param domainName the name of the domain to look up
	 * @return the matching domain ID, or {@code null} if no match is found
	 */
	private String getDomainIdByName(String domainName) {
		if (domainName == null || domainName.isEmpty()) {
			return null;
		}
		List<JsonNode> domainCurrent;
		synchronized (domainList) {
			domainCurrent = new ArrayList<>(domainList);
		}
		return domainCurrent.stream()
				.filter(domain -> domainName.equalsIgnoreCase(
						domain.get(DanteDomainManagerConstant.NAME).asText()))
				.map(domain -> domain.get(DanteDomainManagerConstant.ID).asText())
				.findFirst()
				.orElse(null);
	}

	/**
	 * Checks whether the given group name is configured to be displayed.
	 *
	 * @param groupName the group name to check
	 * @return {@code true} if {@code displayPropertyGroups} is not empty and contains {@code groupName}; otherwise {@code false}
	 */
	private boolean validateGroupDisplay(String groupName) {
		return !CollectionUtils.isEmpty(this.displayPropertyGroups) && this.displayPropertyGroups.contains(groupName);
	}

	/**
	 * Normalizes group name by removing trailing separators such as '#' and '_'.
	 *
	 * @param group the raw group value
	 * @return normalized group name used for filtering
	 */
	private String normalizeGroup(String group) {
		if (StringUtils.isNullOrEmpty(group, true)) {
			return DanteDomainManagerConstant.GENERAL;
		}

		// trim + remove trailing '#' or '_'
		return group.trim().replaceAll("[#_]+$", "");
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
