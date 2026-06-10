# Philips PPDS Wave Integration - Capabilities & Configuration
This document covers Philips PPDS Wave Device Aggregator Capabilities and Configuration.

Symphony integrates with the Philips PPDS Wave platform to provide monitoring and control of commercial Philips signage displays across an organization. The adapter communicates with the PPDS Wave cloud API and exposes display health, audio/video settings, power scheduling, network details, firmware tracking, and alert monitoring.

Main features are: real-time display monitoring, audio/video settings control, power mode and reboot management, firmware tracking, alert tracking with graphs, and multi-customer/site inventory.

## Philips PPDS Wave - Main use cases
- **Monitor** display presence, power state, firmware versions, audio/video settings, network details, and alerts
- **Control** volume, mute, brightness, screen orientation, content source, power mode, reboot, LED strip color, and input/keyboard/IR control modes
- **Track** firmware update jobs, power schedules, and alert history (graphable)
- **Inventory** Philips displays registered in PPDS Wave across customers and sites

## Philips PPDS Wave - Prerequisites
The PPDS Wave Aggregator authenticates using an API Access Token from the PPDS Wave platform.

- Obtain the API Access Token from the PPDS Wave portal
- Use the token as the **Password** in the Symphony device configuration (Username is left blank)

## Philips PPDS Wave - Device Configuration and Provisioning

### Philips PPDS Wave - Connection Setup

| Field | Value |
|---|---|
| Device Type | Infrastructure |
| Category | Management |
| Manufacturer | Philips |
| Model | PPDS Wave (Monitoring Proxy) |
| Monitoring Service | Advanced Monitoring |
| Monitoring Source | Direct |
| Management Address | `api.wave.ppds.com` (default — may differ in proxy or custom DNS environments) |
| Protocol | HTTP |
| Username | Leave blank |
| Password | PPDS Wave API Access Token |
| Port Number | 443 |

### Philips PPDS Wave - Device Provisioning

After the aggregator retrieves device metadata, displays appear on the Devices tab. Unprovisioned devices must be imported before monitoring data is shown.

To import a PPDS Wave device:
1. Open Aggregated Devices
2. Select the unprovisioned device
3. Fill in required provisioning fields
4. Import into Symphony

### Philips PPDS Wave - Adapter configuration properties

| Property | Description |
|---|---|
| deviceMetaDataRetrievalTimeout | Delay between organization and metadata API requests. Default: 300000ms (5 min) |
| deviceDetailsRetrievalTimeout | Delay between device details requests. Default: 30000ms (30 sec) |
| customerHandleFilter | Limits monitoring to specified customer handles. Default: all customers |
| deviceTypeFilter | Type of displays to monitor. Fixed to Signage displays for v1.0 |
| displayPropertyGroups | Comma-separated list of additional property groups to display. Fixed to screenshot for v1.0 |

For detailed information on the aggregator and its configuration, please refer to our knowledgebase -> https://symphony.knowledgeowl.com/help/philips-ppds-wave-aggregator

## Philips PPDS Wave - Available Monitored Data

The final set of properties varies by device model and capabilities reported by the PPDS Wave API.

### Aggregator Properties
Adapter metadata: AdapterBuildDate, AdapterVersion, AdapterUptime, AdapterUptime(min), LastMonitoringCycleDuration(sec), MonitoredDevicesTotal.

### Aggregated Device Properties

| Property Group | Properties |
|---|---|
| Device Info | DisplayType, DisplayPresence, PresenceLastUpdated, AgentReleaseChannel, CommercialTypeNumber, ContentSource, ContentSourceName, CurrentContentSource |
| Audio | MuteStatus, VolumeLevel |
| Video | BrightnessLevel, ContentSource, ScreenOrientation, SignalDetectionState |
| System | DisplayAlias, Groups, InfraredControlMode, KeyboardControlMode, LEDStripColor, PortsControlMode, RecommendedSettings, HasEmptyShadow, HasSensitiveData |
| Power | PowerMode, PowerState, PowerSchedule (DailyPowerOnTime, DailyStandbyEnterTime, ScheduleTitle, IsSynchronized, CreatedDate, Description) |
| Firmware | Version, ScalerVersion, LatestJobCreated, LatestJobPlanned, LatestJobTargetVersion |
| Network | MACAddress, IPAddress, NetworkType, WiFiMACAddress; per-interface: IPAddress, IsActive, MACAddress, Name, SSID |
| Site / Customer | SiteName, SiteAddress, TimeZone, CustomerHandle, CustomerName |
| Alerts | AlertDate, AlertMessage, AlertOccurrences, TotalAlertsCount (graphable) |

## Philips PPDS Wave - Control Capabilities

| Group | Controllable Properties |
|---|---|
| Audio | MuteStatus, VolumeLevel |
| Video | BrightnessLevel, ScreenOrientation, ContentSource |
| System | DisplayAlias, InfraredControlMode, KeyboardControlMode, LEDStripColor, PortsControlMode |
| Power | PowerMode (Standby/On), PowerState, Reboot |

**Note:** Available controls depend on individual device model and capabilities reported by the PPDS Wave API.

## Philips PPDS Wave - Troubleshooting

**Login Error**
- Verify the Password field contains the correct PPDS Wave API Access Token
- Confirm the token has not expired or been revoked in the PPDS Wave portal

**API Error**
- Check the API error description in the aggregator extended properties
- Confirm the Management Address is correct — default is `api.wave.ppds.com`, but may differ in proxy or custom DNS environments

**Link Error / Ping Timeout**
- Verify the Cloud Connector can reach `api.wave.ppds.com` on port 443
- Check the Ping Protocol in the Symphony device configuration

**Devices Not Appearing / Missing Properties**
- Confirm the device has been provisioned (imported) in Symphony
- Check `customerHandleFilter` — if set, only devices for the specified customer are retrieved
- Verify the PPDS Wave API reports the expected capabilities for the device model

If none of the recommended steps help, please enter an SOS ticket at {https://avi-spl.atlassian.net/servicedesk/customer/portals}

## Philips PPDS Wave - What AI Assistant can do with it:
- Find Philips PPDS Wave Aggregated Devices (PPDS Wave as Monitoring Proxy) in Symphony
- Verify Philips PPDS Wave Aggregator configuration and adapter property settings
- Report on display presence, power state, audio/video settings, firmware versions, and alert counts

## Philips PPDS Wave - What AI Assistant cannot do with it:
- Provision devices
- Generate or manage API Access Tokens in the PPDS Wave portal
- Configure power schedules directly on the device
