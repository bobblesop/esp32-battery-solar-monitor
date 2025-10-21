/*
 * Heltec ESP32 LoRa V3 - Renogy Rover 30A MPPT + Junctek Battery Monitor
 * 
 * This code connects to:
 * - Renogy Rover 30A MPPT controller via BT-2/BT-TH Bluetooth dongle
 * - Junctek battery shunt via Bluetooth
 * 
 * Features:
 * - 15-minute BLE connection cycle for both devices
 * - Auto-selects from available WiFi networks
 * - MQTT, InfluxDB, and LoRa publishing
 * - OTA firmware updates
 * - 5-minute display timeout with button wake
 * 
 * Updated:
 * - Alsways accept Junctek values (had to be positive and greater then zero before))
 */

// OTA Configuration
#define OTA_ENABLED true
#define FIRMWARE_VERSION "1.1.8"

#if OTA_ENABLED
const char* firmwareUrl = "https://raw.githubusercontent.com/bobblesop/esp32-battery-solar-monitor/refs/heads/main/OTA-firmware/Heltec_ESP32S3_Renogy_Junctek_Tx_OTA.bin";
const char* versionCheckUrl = "https://raw.githubusercontent.com/bobblesop/esp32-battery-solar-monitor/refs/heads/main/OTA-firmware/version.txt";
const char* ota_update_topic = "duffy_moon/ota/update";
const char* ota_status_topic = "duffy_moon/ota/status";
bool otaInProgress = false;
String otaStatusMessage = "";
#endif

#define HELTEC_POWER_BUTTON

#include <heltec_unofficial.h>
#include <WiFi.h>
#include <ArduinoJson.h>
#include <NimBLEDevice.h>
#include <NimBLEClient.h>
#include <NimBLERemoteService.h>
#include <NimBLERemoteCharacteristic.h>
#include <NimBLEAdvertisedDevice.h>
#include <WiFiMulti.h>
#include <HTTPClient.h>
#include <Update.h>

// Feature Enable/Disable
#define MQTT_ENABLED true
#define INFLUXDB_ENABLED true
#define LORA_ENABLED true

#if MQTT_ENABLED
#include <PubSubClient.h>
#endif

#if INFLUXDB_ENABLED
#include <InfluxDbClient.h>
#include <InfluxDbCloud.h>
#endif

WiFiMulti wifiMulti;

// Configuration
#define DEBUG_ENABLED true
#define MQTT_DEBUG_ENABLED true
#define LORA_DEBUG_ENABLED true
#define INFLUXDB_DEBUG_ENABLED true

// Display Configuration
#define DISPLAY_TIMEOUT 300000
#define DISPLAY_WAKE_EVENTS false

// LED Configuration
#define LED_BLINK_INTERVAL 5000
#define LED_BLINK_DURATION 100
#define LED_FAULT_INTERVAL 1000

// WiFi Configuration
struct WiFiNetwork {
  const char* ssid;
  const char* password;
};

WiFiNetwork wifiNetworks[] = {
  {"Duffy Moon", "Dufchewy456!"},
  {"Three_AA8DC2", "6rH3632YW46Y3P6"},
};

const int numWiFiNetworks = sizeof(wifiNetworks) / sizeof(wifiNetworks[0]);

// MQTT Configuration
#if MQTT_ENABLED
const char* mqtt_server = "broker.emqx.io";
const int mqtt_port = 1883;
const char* mqtt_client_id = "duffymoon";
const char* mqtt_pass = "emqchewy1";
const char mqtt_subscribe_topic[] = "/duffy_moon";
const char* mqtt_publish_topic = "duffy_moon/battery_solar_monitor";
const char* mqtt_read_topic = "duffy_moon/read";
const char* mqtt_lora_transmit_topic = "duffy_moon/lora/transmit";
const char* mqtt_lora_status_topic = "duffy_moon/lora/status";
const char* mqtt_lora_test_topic = "duffy_moon/lora/test";
const char* mqtt_reboot_topic = "duffy_moon/reboot";
#endif

// InfluxDB Configuration
#if INFLUXDB_ENABLED
#define INFLUXDB_URL "https://eu-central-1-1.aws.cloud2.influxdata.com"
#define INFLUXDB_TOKEN "thVjxK0UkoUCSAV5ITHOIoV_-skFwg4LCWLnk-XWK3Qxe4sJ_gOsmiRhexnnzkWx3a-RxW-uOkf2ZB31zJHevg=="
#define INFLUXDB_ORG "924c2e9293059481"
#define INFLUXDB_BUCKET "renogy"
#define TZ_INFO "CET-1CEST,M3.5.0,M10.5.0/3"
#define DEVICE "ESP32"

InfluxDBClient influxdb_client(INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET, INFLUXDB_TOKEN, InfluxDbCloud2CACert);
Point sensor("battery_solar");
#endif

#define INFLUXDB_WIFI_RECONNECT_TIMEOUT 15000

// LoRa Configuration
#if LORA_ENABLED
#define LORA_FREQUENCY 868.1
#define LORA_BANDWIDTH 125.0
#define LORA_SPREADING_FACTOR 12
#define LORA_CODING_RATE 5
#define LORA_OUTPUT_POWER 14
#define LORA_SYNC_WORD 0x1424
#define LORA_PREAMBLE_LENGTH 14
#endif

// Renogy BT-TH/BT-2 Bluetooth Configuration
#define RENOGY_SERVICE_UUID "0000ffd0-0000-1000-8000-00805f9b34fb"
#define RENOGY_WRITE_CHAR_UUID "0000ffd1-0000-1000-8000-00805f9b34fb"
#define RENOGY_NOTIFY_CHAR_UUID "0000fff1-0000-1000-8000-00805f9b34fb"
#define RENOGY_FFF0_SERVICE_UUID "0000fff0-0000-1000-8000-00805f9b34fb"

// Junctek Configuration - SIMPLIFIED
#define JUNCTEK_NOTIFY_SERVICE_UUID "0000ffe0-0000-1000-8000-00805f9b34fb"
#define JUNCTEK_NOTIFY_CHAR_UUID "0000ffe1-0000-1000-8000-00805f9b34fb"

const String JUNCTEK_START = "BB";
const String JUNCTEK_END = "EE";
#define JUNCTEK_BUFFER_TIMEOUT 3000
#define JUNCTEK_READ_TIMEOUT 8000  // 8 second maximum read time

// Track which Junctek identifiers have non-zero values
struct JunctekIdentifierStatus {
  bool hasVolts = false;
  bool hasAmps = false;
  bool hasStatus = false;
  bool hasAhRemaining = false;
  bool hasDischarge = false;
  bool hasCharge = false;
  bool hasMinRemaining = false;
  bool hasImpedance = false;
  bool hasPower = false;
  bool hasTemp = false;
  bool hasCapacity = false;
};

JunctekIdentifierStatus junctekFound;

// MODBUS Configuration
#define MODBUS_BROADCAST_ADDRESS 0xFF
#define MODBUS_FUNCTION_READ 0x03

// BLE Connection Cycle Configuration
#define BLE_CONNECTION_CYCLE 900000
#define BLE_PRE_READ_DELAY 5000
#define BLE_POST_READ_DELAY 5000
#define BLE_SCAN_TIME 10
#define BLE_CONNECTION_TIMEOUT 30000
#define COMMAND_DELAY 1000
#define DEVICE_ID_DISCOVERY_DELAY 2000

// WiFi/LoRa settings
#define WIFI_CONNECT_TIMEOUT 20000
#define WIFI_RETRY_DELAY 30000

#if LORA_ENABLED
#define LORA_TRANSMISSION_DELAY 5000
#endif

// Device Types
enum DeviceType {
  DEVICE_RENOGY,
  DEVICE_JUNCTEK
};

// Connection State Machine
enum BLEConnectionState {
  BLE_STATE_WAITING,
  BLE_STATE_CONNECTING,
  BLE_STATE_PRE_READ_DELAY,
  BLE_STATE_READING_DATA,
  BLE_STATE_POST_READ_DELAY,
  BLE_STATE_DISCONNECTING
};

#if LORA_ENABLED
enum LoRaTransmissionState {
  LORA_STATE_IDLE,
  LORA_STATE_FIRST_SENT,
  LORA_STATE_COMPLETED
};
#endif

// Data structures - SIMPLIFIED Junctek
struct JunctekData {
  float batteryVolts = 0;
  float loadVolts = 0;
  float loadAmps = 0;
  int loadStatus = 0;
  float ahRemaining = 0;
  float discharge = 0;
  float charge = 0;
  int minRemaining = 0;
  float impedance = 0;
  float loadPowerWatts = 0;
  float temp = 0;
  float batteryPercentage = 0;
  float maxCapacity = 220;
  unsigned long timestamp = 0;
  bool dataValid = false;
};

struct MPPTData {
  String deviceModel;
  int deviceId;
  float batteryVoltage;
  float batteryCurrent;
  int batteryPercentage;
  int controllerTemperature;
  float pvVoltage;
  float pvCurrent;
  int pvPower;
  String chargingStatus;
  int chargingStatusCode;
  String batteryType;
  int powerGenerationToday;
  int chargingAmpHoursToday;
  int maxChargingPowerToday;
  long powerGenerationTotal;
  bool loadStatus;
  float loadVoltage;
  float loadCurrent;
  int loadPower;
  unsigned long timestamp;
  bool isFakeData;
  uint32_t faultCode;
  bool hasFaults;
  String activeFaults;
};

// Global Variables
WiFiClient espClient;

#if MQTT_ENABLED
PubSubClient mqttClient(espClient);
bool mqttConnected = false;
int mqttConnectionAttempts = 0;
int mqttSuccessfulPublishes = 0;
int mqttFailedPublishes = 0;
String lastMQTTError = "";
#endif

#if INFLUXDB_ENABLED
bool influxdbConnected = false;
bool publishedThisCycle = false;
#endif

// Junctek - SIMPLIFIED (no more sample buffer!)
String junctekDataBuffer = "";
unsigned long lastJunctekFragment = 0;

// BLE objects
NimBLEClient* pRenogyClient = nullptr;
NimBLEClient* pJunctekClient = nullptr;
NimBLERemoteCharacteristic* pRenogyWriteChar = nullptr;
NimBLERemoteCharacteristic* pRenogyNotifyChar = nullptr;
NimBLERemoteCharacteristic* pJunctekNotifyChar = nullptr;
NimBLEAdvertisedDevice* targetRenogyDevice = nullptr;
NimBLEAdvertisedDevice* targetJunctekDevice = nullptr;

bool renogyConnected = false;
bool junctekConnected = false;
bool wifiConnected = false;
bool freshDataAvailable = false;

// Display state
bool displayOn = true;
unsigned long lastDisplayActivity = 0;
unsigned long lastLEDBlink = 0;
bool ledBlinkState = false;

bool ledAnimationActive = false;
unsigned long ledAnimationStart = 0;
int ledAnimationStep = 0;
bool ledAnimationDirection = true;

// WiFi status
String connectedWiFiSSID = "";
int wifiRSSI = 0;
unsigned long lastWiFiAttempt = 0;

// BLE State Management
BLEConnectionState bleState = BLE_STATE_WAITING;
unsigned long bleStateStartTime = 0;
unsigned long lastBLECycle = 0;
unsigned long nextBLECycle = 0;
DeviceType currentDevice = DEVICE_RENOGY;
bool bothDevicesProcessed = false;
bool renogyProcessedThisCycle = false;
bool junctekProcessedThisCycle = false;

#if LORA_ENABLED
LoRaTransmissionState loraState = LORA_STATE_IDLE;
unsigned long loraTransmissionStart = 0;
bool loraTransmissionPending = false;
bool loraTestModeActive = false;
unsigned long lastLoraTestTransmit = 0;
int loraTestCounter = 0;
#define LORA_TEST_INTERVAL 6000  // 6 seconds between test transmissions
#endif

// Renogy device ID tracking
uint8_t discoveredDeviceId = 0;
uint8_t responseDeviceId = 0;
bool deviceIdDiscovered = false;
bool useDiscoveredId = false;

unsigned long startupTime = 0;

// Response tracking for Renogy
uint8_t responseBuffer[120];
size_t responseLength = 0;
unsigned long lastCommandTime = 0;
int currentCommand = 0;
unsigned long lastCommandSent = 0;
bool waitingForResponse = false;
bool allCommandsCompleted = false;

// Data storage
JunctekData currentJunctekData;
MPPTData currentMPPTData;

// Renogy command sequence
struct ModbusCommand {
  uint16_t startAddress;
  uint16_t numRegisters;
  String description;
  bool useDeviceId;
};

ModbusCommand renogyCommands[] = {
  {0x000C, 8, "Read model", false},
  {0x001A, 1, "Read device address", false},
  {0x0100, 34, "Read main data", true},
  {0xE004, 1, "Read battery type", true},
  {0x0121, 2, "Read fault codes", true}
};

const int numRenogyCommands = sizeof(renogyCommands) / sizeof(renogyCommands[0]);

// Function Prototypes
void setupWiFi();
void setupBLE();
void initializeData();
bool connectToRenogy();
bool connectToJunctek();
void sendNextRenogyCommand();
void publishAllData();
void debugPrint(String message);
void mqttDebugPrint(String message);
void loraDebugPrint(String message);
void influxdbDebugPrint(String message);
void onRenogyBLENotify(NimBLERemoteCharacteristic* pChar, uint8_t* pData, size_t length, bool isNotify);
void onJunctekBLENotify(NimBLERemoteCharacteristic* pChar, uint8_t* pData, size_t length, bool isNotify);
std::vector<uint8_t> createModbusCommandWithAddress(uint16_t startAddress, uint16_t numRegisters, uint8_t deviceAddr);
void parseModbusResponse(uint8_t* data, size_t length, int commandIndex);
void parseJunctekData(String data);
String createJsonData();
void updateDisplay();
void turnDisplayOn();
void turnDisplayOff();
void wakeDisplay();
void handleDisplayTimeout();
void reportStatus();
void cleanupBLE();
uint16_t calculateCRC(uint8_t* data, uint8_t length);
uint8_t getCurrentDeviceAddress();
void resetDeviceIdDiscovery();
void handleBLEConnectionCycle();
void setBLEState(BLEConnectionState newState);
String getBLEStateString(BLEConnectionState state);
bool attemptWiFiConnection();
void disconnectWiFi();
bool reconnectWiFiForInfluxDB();
bool deviceAvailableForCurrentType();
bool tryOtherDevice();
void scanForDevices();
void forceBLECycle();
void forceLoRaTransmit();
void handleLoRaTestMode();
void startLoRaTestMode();
void stopLoRaTestMode();

#if MQTT_ENABLED
void setupMQTT();
void publishToMQTT();
void mqttCallback(char* topic, byte* payload, unsigned int length);
bool attemptMQTTConnection();
#endif

#if INFLUXDB_ENABLED
void setupInfluxDB();
void publishToInfluxDB();
#endif

#if LORA_ENABLED
void setupLoRa();
void transmitLoRa(int LoRaTxNum);
void handleLoRaTransmission();
#endif

#if OTA_ENABLED
void performOTAUpdate(String url);
void checkForOTAUpdate();
bool downloadAndInstallFirmware(String url);
void publishOTAStatus(String status);
String getLatestVersion();
#endif

// ===== SIMPLIFIED JUNCTEK HELPER FUNCTIONS =====

// Reset Junctek identifier tracking
void resetJunctekTracking() {
  junctekFound.hasVolts = false;
  junctekFound.hasAmps = false;
  junctekFound.hasStatus = false;
  junctekFound.hasAhRemaining = false;
  junctekFound.hasDischarge = false;
  junctekFound.hasCharge = false;
  junctekFound.hasMinRemaining = false;
  junctekFound.hasImpedance = false;
  junctekFound.hasPower = false;
  junctekFound.hasTemp = false;
  junctekFound.hasCapacity = false;
}

// Check if all Junctek identifiers have non-zero values
bool allJunctekIdentifiersFound() {
  return junctekFound.hasVolts &&
         junctekFound.hasAmps &&
         junctekFound.hasStatus &&
         junctekFound.hasAhRemaining &&
         junctekFound.hasDischarge &&
         junctekFound.hasCharge &&
         junctekFound.hasMinRemaining &&
         junctekFound.hasImpedance &&
         junctekFound.hasPower &&
         junctekFound.hasTemp &&
         junctekFound.hasCapacity;
}

// Get identifier status string for debugging
String getJunctekIdentifierStatus() {
  String status = "";
  int found = 0;
  int total = 11;
  
  if (junctekFound.hasVolts) found++;
  if (junctekFound.hasAmps) found++;
  if (junctekFound.hasStatus) found++;
  if (junctekFound.hasAhRemaining) found++;
  if (junctekFound.hasDischarge) found++;
  if (junctekFound.hasCharge) found++;
  if (junctekFound.hasMinRemaining) found++;
  if (junctekFound.hasImpedance) found++;
  if (junctekFound.hasPower) found++;
  if (junctekFound.hasTemp) found++;
  if (junctekFound.hasCapacity) found++;
  
  status = String(found) + "/" + String(total) + " [";
  status += junctekFound.hasVolts ? "V" : "_";
  status += junctekFound.hasAmps ? "A" : "_";
  status += junctekFound.hasStatus ? "S" : "_";
  status += junctekFound.hasAhRemaining ? "H" : "_";
  status += junctekFound.hasDischarge ? "D" : "_";
  status += junctekFound.hasCharge ? "C" : "_";
  status += junctekFound.hasMinRemaining ? "M" : "_";
  status += junctekFound.hasImpedance ? "I" : "_";
  status += junctekFound.hasPower ? "P" : "_";
  status += junctekFound.hasTemp ? "T" : "_";
  status += junctekFound.hasCapacity ? "B" : "_";
  status += "]";
  
  return status;
}

float extract2ByteValue(String data, String identifier) {
  int pos = data.indexOf(identifier);
  if (pos == -1 || pos < 4) return 0;
  
  // Read the 4 hex characters as a decimal number (they're BCD!)
  int bcdValue = data.substring(pos - 4, pos).toInt();
  
  return bcdValue / 100.0;
}

float extract3ByteValue(String data, String identifier) {
  int pos = data.indexOf(identifier);
  if (pos == -1 || pos < 6) return 0;
  
  // Read the 6 hex characters as a decimal number (they're BCD!)
  int bcdValue = data.substring(pos - 6, pos).toInt();
  
  return bcdValue / 1000.0;
}

int extract1ByteValue(String data, String identifier) {
  int pos = data.indexOf(identifier);
  if (pos == -1 || pos < 2) return 0;
  
  // Read the 2 hex characters as a decimal number (they're BCD!)
  return data.substring(pos - 2, pos).toInt();
}

// ===== SIMPLIFIED JUNCTEK PARSING =====

void parseJunctekData(String data) {
  debugPrint("Parsing Junctek data: " + data);
  
  if (data.startsWith(JUNCTEK_START)) {
    data = data.substring(2);
  }
  if (data.endsWith(JUNCTEK_END)) {
    data = data.substring(0, data.length() - 2);
  }
  
  // C0 - Voltage - ALWAYS update if identifier is present
  if (data.indexOf("C0") != -1) {
    float newVolts = extract2ByteValue(data, "C0");
    currentJunctekData.batteryVolts = newVolts;
    junctekFound.hasVolts = true;
    debugPrint("  ✓ Volts: " + String(newVolts, 2) + "V");
  }
  
  // C1 - Current - ALWAYS update if identifier is present
  if (data.indexOf("C1") != -1) {
    float newAmps = extract2ByteValue(data, "C1");
    currentJunctekData.loadAmps = newAmps;
    junctekFound.hasAmps = true;
    debugPrint("  ✓ Amps: " + String(newAmps, 2) + "A");
  }
  
  // D1 - Load Status - ALWAYS update if identifier is present
  if (data.indexOf("D1") != -1) {
    int newStatus = extract1ByteValue(data, "D1");
    currentJunctekData.loadStatus = newStatus;
    junctekFound.hasStatus = true;
    debugPrint("  ✓ Status: " + String(newStatus));
  }
  
  // D2 - Ah Remaining - ALWAYS update if identifier is present
  if (data.indexOf("D2") != -1) {
    float newAh = extract3ByteValue(data, "D2");
    currentJunctekData.ahRemaining = newAh;
    junctekFound.hasAhRemaining = true;
    debugPrint("  ✓ Ah: " + String(newAh, 1) + "Ah");
  }
  
  // D3 - Discharge kWh - ALWAYS update if identifier is present
  if (data.indexOf("D3") != -1) {
    float newDischarge = extract3ByteValue(data, "D3");
    currentJunctekData.discharge = newDischarge;
    junctekFound.hasDischarge = true;
    debugPrint("  ✓ Discharge: " + String(newDischarge, 3) + " kWh");
  }
  
  // D4 - Charge kWh - ALWAYS update if identifier is present
  if (data.indexOf("D4") != -1) {
    float newCharge = extract3ByteValue(data, "D4");
    currentJunctekData.charge = newCharge;
    junctekFound.hasCharge = true;
    debugPrint("  ✓ Charge: " + String(newCharge, 3) + " kWh");
  }
  
  // D6 - Minutes Remaining - ALWAYS update if identifier is present
  int d6Pos = data.indexOf("D6");
  if (d6Pos != -1 && d6Pos >= 4) {
    int newMinRemaining = data.substring(d6Pos - 4, d6Pos).toInt();
    currentJunctekData.minRemaining = newMinRemaining;
    junctekFound.hasMinRemaining = true;
    debugPrint("  ✓ Minutes: " + String(newMinRemaining));
  }
  
  // D7 - Impedance - ALWAYS update if identifier is present
  if (data.indexOf("D7") != -1) {
    float newImpedance = extract2ByteValue(data, "D7") * 10;
    currentJunctekData.impedance = newImpedance;
    junctekFound.hasImpedance = true;
    debugPrint("  ✓ Impedance: " + String(newImpedance, 1) + " mΩ");
  }
  
  // D8 - Power - ALWAYS update if identifier is present
  if (data.indexOf("D8") != -1) {
    float newPower = extract3ByteValue(data, "D8");
    currentJunctekData.loadPowerWatts = newPower;
    junctekFound.hasPower = true;
    debugPrint("  ✓ Power: " + String(newPower, 1) + "W");
  }
  
  // D9 - Temperature - ALWAYS update if identifier is present
  int tempPos = data.indexOf("D9");
  if (tempPos != -1 && tempPos >= 4) {
    int validity = data.substring(tempPos - 4, tempPos - 2).toInt();
    int tempValue = data.substring(tempPos - 2, tempPos).toInt();
    
    if (validity == 1) {
      currentJunctekData.temp = (float)tempValue;
      junctekFound.hasTemp = true;
      debugPrint("  ✓ Temp: " + String(tempValue) + "°C");
    } else {
      // No sensor, mark as found anyway
      junctekFound.hasTemp = true;
    }
  }
  
  // B1 - Battery Capacity - ALWAYS update if identifier is present
  int b1Pos = data.indexOf("B1");
  if (b1Pos != -1 && b1Pos >= 4) {
    float newCapacity = data.substring(b1Pos - 4, b1Pos).toInt() / 10.0;
    currentJunctekData.maxCapacity = newCapacity;
    junctekFound.hasCapacity = true;
    debugPrint("  ✓ Capacity: " + String(newCapacity, 1) + "Ah");
  }
  
  // Calculate battery percentage
  if (currentJunctekData.ahRemaining > 0 && currentJunctekData.maxCapacity > 0) {
    currentJunctekData.batteryPercentage = 
      (currentJunctekData.ahRemaining / currentJunctekData.maxCapacity) * 100.0;
    
    if (currentJunctekData.batteryPercentage > 100) currentJunctekData.batteryPercentage = 100;
    if (currentJunctekData.batteryPercentage < 0) currentJunctekData.batteryPercentage = 0;
  }
  
  currentJunctekData.dataValid = true;
  currentJunctekData.timestamp = millis();
  
  // Check if we have all identifiers
  if (allJunctekIdentifiersFound()) {
    debugPrint("✓ All Junctek identifiers found!");
  }
}

// BLE Callback Classes
class RenogyClientCallback : public NimBLEClientCallbacks {
  void onConnect(NimBLEClient* pclient) {
    debugPrint(F("✓ Renogy BLE Connected"));
    renogyConnected = true;
    resetDeviceIdDiscovery();
    if (DISPLAY_WAKE_EVENTS) wakeDisplay();
  }

  void onDisconnect(NimBLEClient* pclient) {
    debugPrint(F("✗ Renogy BLE Disconnected"));
    renogyConnected = false;
    pRenogyWriteChar = nullptr;
    pRenogyNotifyChar = nullptr;
    currentCommand = 0;
    waitingForResponse = false;
    allCommandsCompleted = false;
    resetDeviceIdDiscovery();
  }
};

class JunctekClientCallback : public NimBLEClientCallbacks {
  void onConnect(NimBLEClient* pclient) {
    debugPrint(F("✓ Junctek BLE Connected"));
    junctekConnected = true;
    if (DISPLAY_WAKE_EVENTS) wakeDisplay();
  }

  void onDisconnect(NimBLEClient* pclient) {
    debugPrint(F("✗ Junctek BLE Disconnected"));
    junctekConnected = false;
    pJunctekNotifyChar = nullptr;
  }
};

class MyAdvertisedDeviceCallbacks: public NimBLEAdvertisedDeviceCallbacks {
  void onResult(NimBLEAdvertisedDevice* advertisedDevice) {
    std::string deviceName = advertisedDevice->getName();
    debugPrint("Found BLE device: " + String(deviceName.c_str()));
    
    bool deviceFound = false;
    
    if ((deviceName.find("BT-TH") != std::string::npos || 
         deviceName.find("BT-2") != std::string::npos ||
         deviceName.find("Renogy") != std::string::npos) && 
         targetRenogyDevice == nullptr) {
      
      debugPrint("✓ Found Renogy device: " + String(deviceName.c_str()));
      targetRenogyDevice = new NimBLEAdvertisedDevice(*advertisedDevice);
      
      if (!renogyProcessedThisCycle) {
        currentDevice = DEVICE_RENOGY;
        debugPrint("→ Setting Renogy as current device");
        deviceFound = true;
      }
    }
    
    if ((deviceName.find("BTG") != std::string::npos || 
         deviceName.find("btg") != std::string::npos) && 
         targetJunctekDevice == nullptr) {
      
      debugPrint("✓ Found Junctek device: " + String(deviceName.c_str()));
      targetJunctekDevice = new NimBLEAdvertisedDevice(*advertisedDevice);
      
      if (!junctekProcessedThisCycle && !deviceFound) {
        currentDevice = DEVICE_JUNCTEK;
        debugPrint("→ Setting Junctek as current device");
        deviceFound = true;
      }
    }
    
    if (deviceFound || (targetRenogyDevice != nullptr && targetJunctekDevice != nullptr)) {
      debugPrint("→ Stopping BLE scan");
      advertisedDevice->getScan()->stop();
    }
  }
};

// WiFi management
void disconnectWiFi() {
  if (WiFi.status() == WL_CONNECTED) {
    influxdbDebugPrint(F("Disconnecting WiFi..."));
    WiFi.disconnect(true);
    delay(1000);
    wifiConnected = false;
    connectedWiFiSSID = "";
    wifiRSSI = 0;
  }
}

bool reconnectWiFiForInfluxDB() {
  influxdbDebugPrint(F("WiFi reconnection for InfluxDB..."));
  
  yield();
  disconnectWiFi();
  
  wifiMulti.APlistClean();
  for (int i = 0; i < numWiFiNetworks; i++) {
    wifiMulti.addAP(wifiNetworks[i].ssid, wifiNetworks[i].password);
    yield();
  }
  
  unsigned long startAttempt = millis();
  while (millis() - startAttempt < INFLUXDB_WIFI_RECONNECT_TIMEOUT) {
    yield();
    if (wifiMulti.run() == WL_CONNECTED) {
      wifiConnected = true;
      connectedWiFiSSID = WiFi.SSID();
      wifiRSSI = WiFi.RSSI();
      influxdbDebugPrint(F("✓ WiFi reconnected!"));
      return true;
    }
    delay(500);
  }
  
  influxdbDebugPrint(F("✗ WiFi reconnection failed"));
  return false;
}

// Display control
void turnDisplayOn() {
  if (!displayOn) {
    debugPrint(F("Display ON"));
    displayOn = true;
    display.displayOn();
    display.clear();
    display.display();
  }
  lastDisplayActivity = millis();
}

void turnDisplayOff() {
  if (displayOn) {
    debugPrint(F("Display OFF"));
    displayOn = false;
    display.clear();
    display.display();
    display.displayOff();
  }
}

void wakeDisplay() {
  turnDisplayOn();
}

void handleDisplayTimeout() {
  unsigned long now = millis();
  if (displayOn && (now - lastDisplayActivity) > DISPLAY_TIMEOUT) {
    turnDisplayOff();
  }
}

void setup() {
  heltec_setup();
  heltec_ve(true);
  
  Serial.begin(115200);
  delay(1000);
  
  startupTime = millis();
  lastDisplayActivity = millis();
  
  debugPrint(F("=== Battery + Solar Monitor - SIMPLIFIED ==="));
  debugPrint("Firmware v" + String(FIRMWARE_VERSION));
  
  String features = "BLE";
  #if MQTT_ENABLED
  features += "+M";
  #endif
  #if INFLUXDB_ENABLED
  features += "+I";
  #endif
  #if LORA_ENABLED
  features += "+L";
  #endif
  debugPrint("Features: " + features);
  
  display.init();
  display.flipScreenVertically();
  display.setFont(ArialMT_Plain_10);
  display.clear();
  display.drawString(0, 0, F("Battery+Solar Monitor"));
  display.drawString(0, 16, F("SIMPLIFIED Junctek"));
  display.drawString(0, 32, "v" + String(FIRMWARE_VERSION));
  display.drawString(0, 48, "Features: " + features);
  display.display();
  
  setupWiFi();
  setupBLE();
  
  #if MQTT_ENABLED
    setupMQTT();
    #if OTA_ENABLED
    mqttClient.subscribe(ota_update_topic);
    #endif
    mqttClient.subscribe(mqtt_lora_transmit_topic);
    mqttClient.subscribe(mqtt_lora_test_topic);
    mqttClient.subscribe(mqtt_reboot_topic);
  #endif

  #if INFLUXDB_ENABLED
  setupInfluxDB();
  #endif
  
  #if LORA_ENABLED
  setupLoRa();
  #endif
  
  initializeData();
  resetDeviceIdDiscovery();
  resetJunctekTracking();  // Initialize Junctek identifier tracking
  
  nextBLECycle = millis() + 5000;
  setBLEState(BLE_STATE_WAITING);
  
  bothDevicesProcessed = false;
  renogyProcessedThisCycle = false;
  junctekProcessedThisCycle = false;
  currentDevice = DEVICE_RENOGY;
  
  #if LORA_ENABLED
  loraState = LORA_STATE_IDLE;
  loraTransmissionPending = false;
  #endif
  
  debugPrint(F("Setup complete!"));
  delay(2000);
  updateDisplay();
}

void loop() {
  heltec_loop();

  static unsigned long lastHeapCheck = 0;
  if (millis() - lastHeapCheck > 5000) {
    lastHeapCheck = millis();
    debugPrint("Heap: " + String(ESP.getFreeHeap()) + 
               " Min: " + String(ESP.getMinFreeHeap()));
    
    if (ESP.getFreeHeap() < 10000) {
      debugPrint("CRITICAL: Low memory, restarting...");
      delay(1000);
      ESP.restart();
    }
  }
  
  yield();

  #if MQTT_ENABLED
  static unsigned long lastMQTTCheck = 0;
  #endif
  
  static unsigned long lastDisplayUpdate = 0;
  unsigned long now = millis();

  #if INFLUXDB_ENABLED
  sensor.clearFields();
  #endif
  
  handleDisplayTimeout();
  
  // LED handling
  if (!ledAnimationActive) {
    unsigned long currentInterval = 0;
    
    if (currentMPPTData.hasFaults) {
      currentInterval = LED_FAULT_INTERVAL;
    } else if (!displayOn) {
      currentInterval = LED_BLINK_INTERVAL;
    }
    
    if (currentInterval > 0) {
      if (now - lastLEDBlink >= currentInterval) {
        lastLEDBlink = now;
        ledBlinkState = true;
        heltec_led(100);
      } else if (ledBlinkState && (now - lastLEDBlink >= LED_BLINK_DURATION)) {
        ledBlinkState = false;
        heltec_led(0);
      }
    }
  }
  
  if (displayOn && (now - lastDisplayUpdate > 1000)) {
    lastDisplayUpdate = now;
    updateDisplay();
  }
  
  if (ledAnimationActive) {
    if (now - ledAnimationStart > 5) {
      ledAnimationStart = now;
      heltec_led(ledAnimationStep);
      
      if (ledAnimationDirection) {
        ledAnimationStep++;
        if (ledAnimationStep >= 100) ledAnimationDirection = false;
      } else {
        ledAnimationStep--;
        if (ledAnimationStep <= 0) {
          ledAnimationActive = false;
          heltec_led(0);
        }
      }
    }
  }
  
  // WiFi management
  if (WiFi.status() != WL_CONNECTED && !wifiConnected) {
    if (now - lastWiFiAttempt > WIFI_RETRY_DELAY) {
      lastWiFiAttempt = now;
      attemptWiFiConnection();
    }
  } else if (WiFi.status() == WL_CONNECTED && !wifiConnected) {
    wifiConnected = true;
    connectedWiFiSSID = WiFi.SSID();
    wifiRSSI = WiFi.RSSI();
    debugPrint("✓ WiFi: " + connectedWiFiSSID);
    if (DISPLAY_WAKE_EVENTS) wakeDisplay();
  }
  
  #if MQTT_ENABLED
  if (wifiConnected && !mqttClient.connected()) {
    if (now - lastMQTTCheck > 5000) {
      lastMQTTCheck = now;
      attemptMQTTConnection();
    }
  }
  
  if (mqttConnected) {
    mqttClient.loop();
    if (!mqttClient.connected()) mqttConnected = false;
  }
  #endif
  
  handleBLEConnectionCycle();
  
  // Renogy command sequence
  if (renogyConnected && currentDevice == DEVICE_RENOGY && 
      bleState == BLE_STATE_READING_DATA && !waitingForResponse && !allCommandsCompleted) {
    unsigned long requiredDelay = COMMAND_DELAY;
    if (currentCommand == 2 && deviceIdDiscovered && !useDiscoveredId) {
      requiredDelay = DEVICE_ID_DISCOVERY_DELAY;
      useDiscoveredId = true;
    }
    
    if (now - lastCommandSent > requiredDelay) {
      sendNextRenogyCommand();
    }
  }
  
  if (waitingForResponse && currentDevice == DEVICE_RENOGY && (millis() - lastCommandSent > 3000)) {
    debugPrint("⚠ Renogy timeout");
    waitingForResponse = false;
    currentCommand++;
    if (currentCommand >= numRenogyCommands) {
      allCommandsCompleted = true;
      freshDataAvailable = true;
    }
  }
  
  // Junctek reading - SIMPLIFIED with early exit when all identifiers found
  if (junctekConnected && currentDevice == DEVICE_JUNCTEK && 
      bleState == BLE_STATE_READING_DATA) {
    static unsigned long junctekReadStart = 0;
    static unsigned long lastStatusLog = 0;
    static unsigned long lastIdentifierUpdate = 0;
    
    if (junctekReadStart == 0) {
      junctekReadStart = now;
      lastStatusLog = now;
      lastIdentifierUpdate = now;
      resetJunctekTracking();
      debugPrint("Junctek: Starting to collect identifiers...");
    }
    
    unsigned long elapsed = now - junctekReadStart;
    
    // Log progress every 5 seconds
    if (now - lastStatusLog > 5000) {
      lastStatusLog = now;
      debugPrint("Junctek progress (" + String(elapsed / 1000) + "s): " + getJunctekIdentifierStatus());
    }
    
    // Check if we've received new identifiers recently
    if (currentJunctekData.timestamp > lastIdentifierUpdate) {
      lastIdentifierUpdate = currentJunctekData.timestamp;
    }

    // Exit conditions - check in priority order
    bool shouldExit = false;
    String exitReason = "";
    
    // 1. All identifiers found - highest priority
    if (allJunctekIdentifiersFound()) {
      shouldExit = true;
      exitReason = "all identifiers found in " + String(elapsed / 1000) + "s";
    }
    // 2. Hard timeout - ensures we never wait too long
    else if (elapsed > JUNCTEK_READ_TIMEOUT) {
      shouldExit = true;
      exitReason = "timeout at " + String(JUNCTEK_READ_TIMEOUT / 1000) + "s";
    }
    // 3. No new data for 3 seconds - device stopped transmitting
    else if (elapsed > 3000 && (now - lastIdentifierUpdate) > 3000) {
      shouldExit = true;
      exitReason = "no new data for 3s";
    }
    // 4. At least 6 identifiers after 3 seconds - early exit with good data
    else if (elapsed > 3000) {
      int foundCount = 0;
      if (junctekFound.hasVolts) foundCount++;
      if (junctekFound.hasAmps) foundCount++;
      if (junctekFound.hasAhRemaining) foundCount++;
      if (junctekFound.hasDischarge) foundCount++;
      if (junctekFound.hasCharge) foundCount++;
      if (junctekFound.hasPower) foundCount++;
      if (junctekFound.hasStatus) foundCount++;
      if (junctekFound.hasMinRemaining) foundCount++;
      
      if (foundCount >= 6) {
        shouldExit = true;
        exitReason = String(foundCount) + "/11 identifiers after 3s";
      }
    }
    
    // Execute exit if any condition was met
    if (shouldExit) {
      allCommandsCompleted = true;
      freshDataAvailable = true;
      debugPrint("✓ Junctek complete - " + exitReason);
      debugPrint("  Final status: " + getJunctekIdentifierStatus());
      junctekReadStart = 0;
      if (DISPLAY_WAKE_EVENTS) wakeDisplay();
    }    
  }
  
  #if LORA_ENABLED
  handleLoRaTransmission();
  handleLoRaTestMode();
  #endif
  
  // Button handling
  if (button.isSingleClick()) {
    if (!displayOn) {
      wakeDisplay();
      updateDisplay();
    } else {
      debugPrint("Button - immediate cycle");
      wakeDisplay();
      nextBLECycle = millis() + 1000;
      setBLEState(BLE_STATE_WAITING);
      reportStatus();
      
      ledAnimationActive = true;
      ledAnimationStart = now;
      ledAnimationStep = 0;
      ledAnimationDirection = true;
    }
  }

  if (button.isDoubleClick()) {
    #if OTA_ENABLED
    debugPrint("Double-click - OTA check");
    wakeDisplay();
    checkForOTAUpdate();
    #endif
  }

  if (button.pressedFor(2000)) {
    static bool longPressHandled = false;
    if (!longPressHandled) {
      longPressHandled = true;
      debugPrint("Long press - forced OTA");
      wakeDisplay();
      publishOTAStatus("Forced OTA update");
      performOTAUpdate(firmwareUrl);
    }
    if (!button.pressed()) {
      longPressHandled = false;
    }
  }

  delay(10);
}


void forceBLECycle() {
  debugPrint("Force cycle via MQTT");
  wakeDisplay();
  nextBLECycle = millis() + 1000;
  setBLEState(BLE_STATE_WAITING);
  reportStatus();
  
  unsigned long now = millis();
  ledAnimationActive = true;
  ledAnimationStart = now;
  ledAnimationStep = 0;
  ledAnimationDirection = true;
}

void forceLoRaTransmit() {
  debugPrint("=== FORCE LORA TRANSMIT ===");
  
  #if !LORA_ENABLED
  debugPrint("✗ LoRa is not enabled!");
  #if MQTT_ENABLED
  if (mqttConnected) {
    mqttClient.publish(mqtt_lora_status_topic, "ERROR: LoRa not enabled");
  }
  #endif
  return;
  #endif
  
  #if LORA_ENABLED
  wakeDisplay();
  
  // Check if we have any data to transmit
  bool hasRenogyData = (currentMPPTData.timestamp > 0);
  bool hasJunctekData = currentJunctekData.dataValid;
  
  debugPrint("Data available - Renogy: " + String(hasRenogyData ? "YES" : "NO") + 
             ", Junctek: " + String(hasJunctekData ? "YES" : "NO"));
  
  if (!hasRenogyData && !hasJunctekData) {
    debugPrint("✗ No data available for LoRa transmission");
    #if MQTT_ENABLED
    if (mqttConnected) {
      mqttClient.publish(mqtt_lora_status_topic, "ERROR: No data available");
    }
    #endif
    return;
  }
  
  // Check if transmission already pending
  if (loraTransmissionPending) {
    debugPrint("⚠ LoRa transmission already pending");
    #if MQTT_ENABLED
    if (mqttConnected) {
      mqttClient.publish(mqtt_lora_status_topic, "WARNING: Already pending");
    }
    #endif
    return;
  }
  
  // Queue the LoRa transmission
  loraTransmissionPending = true;
  loraState = LORA_STATE_IDLE;
  loraDebugPrint("✓ LoRa re-transmission queued");
  
  #if MQTT_ENABLED
  if (mqttConnected) {
    mqttClient.publish(mqtt_lora_status_topic, "Transmission queued");
    debugPrint("✓ Published status to MQTT");
  }
  #endif
  
  // Start LED animation to show activity
  unsigned long now = millis();
  ledAnimationActive = true;
  ledAnimationStart = now;
  ledAnimationStep = 0;
  ledAnimationDirection = true;
  
  debugPrint("=== LoRa transmit queued successfully ===");
  #endif
}

#if LORA_ENABLED
void handleLoRaTransmission() {
  if (!loraTransmissionPending) return;
  
  unsigned long now = millis();
  
  switch (loraState) {
    case LORA_STATE_IDLE:
      loraDebugPrint("Starting LoRa transmission sequence...");
      transmitLoRa(1);
      loraState = LORA_STATE_FIRST_SENT;
      loraTransmissionStart = now;
      break;
      
    case LORA_STATE_FIRST_SENT:
      if (now - loraTransmissionStart >= LORA_TRANSMISSION_DELAY) {
        transmitLoRa(2);
        loraState = LORA_STATE_COMPLETED;
      }
      break;
      
    case LORA_STATE_COMPLETED:
      loraDebugPrint("LoRa transmission sequence complete");
      loraTransmissionPending = false;
      loraState = LORA_STATE_IDLE;
      break;
  }
}
#endif

bool attemptWiFiConnection() {
  debugPrint("WiFi setup with auto-selection...");
  
  wifiMulti.APlistClean();
  for (int i = 0; i < numWiFiNetworks; i++) {
    wifiMulti.addAP(wifiNetworks[i].ssid, wifiNetworks[i].password);
  }
  
  unsigned long startAttempt = millis();
  while (millis() - startAttempt < WIFI_CONNECT_TIMEOUT) {
    if (wifiMulti.run() == WL_CONNECTED) {
      wifiConnected = true;
      connectedWiFiSSID = WiFi.SSID();
      wifiRSSI = WiFi.RSSI();
      debugPrint("✓ WiFi connected!");
      return true;
    }
    delay(500);
  }
  
  debugPrint("✗ WiFi failed");
  return false;
}

void setupWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.persistent(true);
  attemptWiFiConnection();
}

void handleBLEConnectionCycle() {
  unsigned long now = millis();
  unsigned long stateElapsed = now - bleStateStartTime;
  
  switch (bleState) {
    case BLE_STATE_WAITING:
      if (now >= nextBLECycle) {
        debugPrint(F("=== New BLE cycle ==="));
        bothDevicesProcessed = false;
        renogyProcessedThisCycle = false;
        junctekProcessedThisCycle = false;
        publishedThisCycle = false;
        
        if (targetRenogyDevice) {
          delete targetRenogyDevice;
          targetRenogyDevice = nullptr;
        }
        if (targetJunctekDevice) {
          delete targetJunctekDevice;
          targetJunctekDevice = nullptr;
        }
        
        if (DISPLAY_WAKE_EVENTS) wakeDisplay();
        setBLEState(BLE_STATE_CONNECTING);
      }
      break;
      
    case BLE_STATE_CONNECTING:
      if ((currentDevice == DEVICE_RENOGY && targetRenogyDevice != nullptr && !renogyConnected) ||
          (currentDevice == DEVICE_JUNCTEK && targetJunctekDevice != nullptr && !junctekConnected)) {
        
        if (stateElapsed > BLE_CONNECTION_TIMEOUT) {
          debugPrint("✗ Connection timeout");
          
          if (currentDevice == DEVICE_RENOGY) {
            renogyProcessedThisCycle = true;
          } else {
            junctekProcessedThisCycle = true;
          }
          
          if (!bothDevicesProcessed && tryOtherDevice()) {
            setBLEState(BLE_STATE_CONNECTING);
            bleStateStartTime = now;
          } else {
            nextBLECycle = now + BLE_CONNECTION_CYCLE;
            setBLEState(BLE_STATE_WAITING);
          }
        } else {
          static unsigned long lastAttempt = 0;
          if (now - lastAttempt > 5000) {
            lastAttempt = now;
            if (currentDevice == DEVICE_RENOGY) {
              connectToRenogy();
            } else {
              connectToJunctek();
            }
          }
        }
      } else if (!deviceAvailableForCurrentType()) {
        scanForDevices();
        delay(BLE_SCAN_TIME * 1000 + 1000);
        
        if (!deviceAvailableForCurrentType()) {
          if (currentDevice == DEVICE_RENOGY) {
            renogyProcessedThisCycle = true;
          } else {
            junctekProcessedThisCycle = true;
          }
          
          if (!bothDevicesProcessed && tryOtherDevice()) {
            setBLEState(BLE_STATE_CONNECTING);
            bleStateStartTime = now;
          } else {
            bothDevicesProcessed = true;
            
            if (!publishedThisCycle) {
              publishAllData();
              publishedThisCycle = true;
            }
            
            nextBLECycle = now + BLE_CONNECTION_CYCLE;
            setBLEState(BLE_STATE_WAITING);
          }
        }
      } else {
        debugPrint("✓ Connected");
        setBLEState(BLE_STATE_PRE_READ_DELAY);
      }
      break;

    case BLE_STATE_PRE_READ_DELAY:
      if (stateElapsed >= BLE_PRE_READ_DELAY) {
        if (currentDevice == DEVICE_RENOGY) {
          currentCommand = 0;
          waitingForResponse = false;
          allCommandsCompleted = false;
        } else {
          allCommandsCompleted = false;
          currentJunctekData.dataValid = false;
        }
        setBLEState(BLE_STATE_READING_DATA);
      }
      break;
      
    case BLE_STATE_READING_DATA:
      if (allCommandsCompleted) {
        setBLEState(BLE_STATE_POST_READ_DELAY);
      } else if (stateElapsed > 60000) {
        debugPrint("⚠ Reading timeout");
        setBLEState(BLE_STATE_POST_READ_DELAY);
      }
      break;
      
    case BLE_STATE_POST_READ_DELAY:
      if (stateElapsed >= BLE_POST_READ_DELAY) {
        setBLEState(BLE_STATE_DISCONNECTING);
      }
      break;
      
    case BLE_STATE_DISCONNECTING:
      if (currentDevice == DEVICE_RENOGY) {
        if (pRenogyClient && pRenogyClient->isConnected()) {
          pRenogyClient->disconnect();
        }
        renogyProcessedThisCycle = true;
      } else {
        if (pJunctekClient && pJunctekClient->isConnected()) {
          pJunctekClient->disconnect();
        }
        junctekProcessedThisCycle = true;
      }
      
      if (!bothDevicesProcessed && tryOtherDevice()) {
        setBLEState(BLE_STATE_CONNECTING);
      } else {
        bothDevicesProcessed = true;
        
        if (!publishedThisCycle) {
          publishAllData();
          publishedThisCycle = true;
        }
        
        nextBLECycle = now + BLE_CONNECTION_CYCLE;
        setBLEState(BLE_STATE_WAITING);
      }
      break;
  }
  
  bothDevicesProcessed = renogyProcessedThisCycle && junctekProcessedThisCycle;
}

bool deviceAvailableForCurrentType() {
  if (currentDevice == DEVICE_RENOGY) {
    return (targetRenogyDevice != nullptr);
  } else {
    return (targetJunctekDevice != nullptr);
  }
}

bool tryOtherDevice() {
  if (currentDevice == DEVICE_RENOGY && !junctekProcessedThisCycle) {
    currentDevice = DEVICE_JUNCTEK;
    return true;
  } else if (currentDevice == DEVICE_JUNCTEK && !renogyProcessedThisCycle) {
    currentDevice = DEVICE_RENOGY;
    return true;
  }
  return false;
}

void scanForDevices() {
  debugPrint(F("BLE scan..."));
  NimBLEScan* pBLEScan = NimBLEDevice::getScan();
  pBLEScan->setAdvertisedDeviceCallbacks(new MyAdvertisedDeviceCallbacks());
  pBLEScan->setInterval(1349);
  pBLEScan->setWindow(449);
  pBLEScan->setActiveScan(true);
  pBLEScan->start(BLE_SCAN_TIME, false);
  
  for (int i = 0; i < BLE_SCAN_TIME; i++) {
    delay(1000);
    yield();
  }
}

void setBLEState(BLEConnectionState newState) {
  if (newState != bleState) {
    debugPrint("BLE: " + getBLEStateString(bleState) + " -> " + getBLEStateString(newState));
    bleState = newState;
    bleStateStartTime = millis();
  }
}

String getBLEStateString(BLEConnectionState state) {
  switch (state) {
    case BLE_STATE_WAITING: return F("WAIT");
    case BLE_STATE_CONNECTING: return F("CONNECT");
    case BLE_STATE_PRE_READ_DELAY: return F("PRE_READ");
    case BLE_STATE_READING_DATA: return F("READING");
    case BLE_STATE_POST_READ_DELAY: return F("POST_READ");
    case BLE_STATE_DISCONNECTING: return F("DISCONNECT");
    default: return F("BLE UNKNOWN");
  }
}

void publishAllData() {
  bool hasRenogyData = (currentMPPTData.timestamp > 0);
  bool hasJunctekData = currentJunctekData.dataValid;
  
  debugPrint("=== PUBLISH ALL DATA ===");
  debugPrint("Renogy data: " + String(hasRenogyData ? "YES" : "NO"));
  debugPrint("Junctek data: " + String(hasJunctekData ? "YES" : "NO"));
  
  if (!hasRenogyData && !hasJunctekData) {
    debugPrint("✗ No data to publish");
    return;
  }
  
  debugPrint("Publishing: " + 
             String(hasRenogyData ? "R" : "") + 
             String(hasJunctekData ? "J" : ""));
  
  #if MQTT_ENABLED
  debugPrint("MQTT enabled: " + String(mqttConnected ? "Connected" : "Disconnected"));
  if (mqttConnected) {
    publishToMQTT();
  }
  #endif
  
  #if INFLUXDB_ENABLED
  publishToInfluxDB();
  #endif
  
  #if LORA_ENABLED
  debugPrint("Setting LoRa transmission pending...");
  if (!loraTransmissionPending) {
    loraTransmissionPending = true;
    loraState = LORA_STATE_IDLE;
  }
  #endif
  
  debugPrint("=== PUBLISH COMPLETE ===");
}

// Remaining functions continue...
// (Including all Renogy functions, MQTT, InfluxDB, LoRa, OTA, display, etc.)

bool connectToRenogy() {
  debugPrint(F("Renogy connection..."));
  
  if (pRenogyClient) {
    if (pRenogyClient->isConnected()) pRenogyClient->disconnect();
    NimBLEDevice::deleteClient(pRenogyClient);
    pRenogyClient = nullptr;
  }
  
  if (targetRenogyDevice == nullptr) {
    scanForDevices();
    delay(BLE_SCAN_TIME * 1000 + 1000);
    if (targetRenogyDevice == nullptr) return false;
  }
  
  pRenogyClient = NimBLEDevice::createClient();
  if (pRenogyClient == nullptr) return false;
  
  pRenogyClient->setClientCallbacks(new RenogyClientCallback());
  pRenogyClient->setConnectionParams(12, 12, 0, 51);
  pRenogyClient->setConnectTimeout(15);
  
  if (!pRenogyClient->connect(targetRenogyDevice)) return false;
  
  delay(2000);
  
  NimBLERemoteService* pServiceFFD0 = pRenogyClient->getService(RENOGY_SERVICE_UUID);
  if (pServiceFFD0 == nullptr) return false;
  
  pRenogyWriteChar = pServiceFFD0->getCharacteristic(RENOGY_WRITE_CHAR_UUID);
  if (pRenogyWriteChar == nullptr) return false;
  
  NimBLERemoteService* pServiceFFF0 = pRenogyClient->getService(RENOGY_FFF0_SERVICE_UUID);
  if (pServiceFFF0 == nullptr) return false;
  
  pRenogyNotifyChar = pServiceFFF0->getCharacteristic(RENOGY_NOTIFY_CHAR_UUID);
  if (pRenogyNotifyChar == nullptr) return false;
  
  if (!pRenogyNotifyChar->subscribe(true, onRenogyBLENotify)) return false;
  
  renogyConnected = true;
  return true;
}

bool connectToJunctek() {
  debugPrint(F("Junctek connection..."));
  
  if (pJunctekClient) {
    if (pJunctekClient->isConnected()) pJunctekClient->disconnect();
    NimBLEDevice::deleteClient(pJunctekClient);
    pJunctekClient = nullptr;
  }
  
  if (targetJunctekDevice == nullptr) {
    scanForDevices();
    delay(BLE_SCAN_TIME * 1000 + 1000);
    if (targetJunctekDevice == nullptr) return false;
  }
  
  pJunctekClient = NimBLEDevice::createClient();
  if (pJunctekClient == nullptr) return false;
  
  pJunctekClient->setClientCallbacks(new JunctekClientCallback());
  pJunctekClient->setConnectionParams(12, 12, 0, 51);
  pJunctekClient->setConnectTimeout(15);
  
  if (!pJunctekClient->connect(targetJunctekDevice)) return false;
  
  delay(2000);
  
  NimBLERemoteService* pService = pJunctekClient->getService(JUNCTEK_NOTIFY_SERVICE_UUID);
  if (pService == nullptr) return false;
  
  pJunctekNotifyChar = pService->getCharacteristic(JUNCTEK_NOTIFY_CHAR_UUID);
  if (pJunctekNotifyChar == nullptr) return false;
  
  if (!pJunctekNotifyChar->subscribe(true, onJunctekBLENotify)) return false;
  
  junctekConnected = true;
  junctekDataBuffer = "";
  resetJunctekTracking();  // Reset identifier tracking for new connection
  return true;
}

void resetDeviceIdDiscovery() {
  discoveredDeviceId = 0;
  responseDeviceId = 0;
  deviceIdDiscovered = false;
  useDiscoveredId = false;
}

uint8_t getCurrentDeviceAddress() {
  if (useDiscoveredId && deviceIdDiscovered && discoveredDeviceId != 0) {
    return discoveredDeviceId;
  }
  return MODBUS_BROADCAST_ADDRESS;
}

void sendNextRenogyCommand() {
  if (!renogyConnected || pRenogyWriteChar == nullptr || currentCommand >= numRenogyCommands) return;
  
  ModbusCommand& cmd = renogyCommands[currentCommand];
  
  uint8_t deviceAddr;
  if (cmd.useDeviceId && useDiscoveredId && deviceIdDiscovered) {
    deviceAddr = discoveredDeviceId;
  } else {
    deviceAddr = MODBUS_BROADCAST_ADDRESS;
  }
  
  std::vector<uint8_t> modbusCmd = createModbusCommandWithAddress(cmd.startAddress, cmd.numRegisters, deviceAddr);
  responseLength = 0;
  
  if (pRenogyWriteChar->writeValue(modbusCmd.data(), modbusCmd.size())) {
    lastCommandSent = millis();
    waitingForResponse = true;
  } else {
    currentCommand++;
    if (currentCommand >= numRenogyCommands) allCommandsCompleted = true;
  }
}

std::vector<uint8_t> createModbusCommandWithAddress(uint16_t startAddress, uint16_t numRegisters, uint8_t deviceAddr) {
  std::vector<uint8_t> cmd;
  cmd.push_back(deviceAddr);
  cmd.push_back(MODBUS_FUNCTION_READ);
  cmd.push_back((startAddress >> 8) & 0xFF);
  cmd.push_back(startAddress & 0xFF);
  cmd.push_back((numRegisters >> 8) & 0xFF);
  cmd.push_back(numRegisters & 0xFF);
  
  uint16_t crc = calculateCRC(cmd.data(), cmd.size());
  cmd.push_back(crc & 0xFF);
  cmd.push_back((crc >> 8) & 0xFF);
  
  return cmd;
}

uint16_t calculateCRC(uint8_t* data, uint8_t length) {
  uint16_t crc = 0xFFFF;
  for (uint8_t i = 0; i < length; i++) {
    crc ^= data[i];
    for (uint8_t j = 0; j < 8; j++) {
      if (crc & 0x0001) {
        crc = (crc >> 1) ^ 0xA001;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc;
}

// ===== SIMPLIFIED JUNCTEK NOTIFICATION HANDLER =====
void onJunctekBLENotify(NimBLERemoteCharacteristic* pChar, uint8_t* pData, size_t length, bool isNotify) {
  String hexFragment = "";
  for (int i = 0; i < length; i++) {
    if (pData[i] < 16) hexFragment += "0";
    hexFragment += String(pData[i], HEX);
  }
  hexFragment.toUpperCase();
  
  junctekDataBuffer += hexFragment;
  lastJunctekFragment = millis();
  
  // Process ALL complete messages in the buffer
  while (true) {
    int startPos = junctekDataBuffer.indexOf(JUNCTEK_START);
    if (startPos == -1) {
      // No start marker found, clear any garbage before it
      junctekDataBuffer = "";
      break;
    }
    
    // Remove any data before the start marker
    if (startPos > 0) {
      junctekDataBuffer = junctekDataBuffer.substring(startPos);
      startPos = 0;
    }
    
    // Look for end marker AFTER start marker
    int endPos = junctekDataBuffer.indexOf(JUNCTEK_END, startPos + 2);
    if (endPos == -1) {
      // Incomplete message, wait for more data
      // But check if buffer is getting too long (might have missed end marker)
      if (junctekDataBuffer.length() > 500) {
        debugPrint("⚠ Buffer overflow, clearing");
        junctekDataBuffer = "";
      }
      break;
    }
    
    // Extract and process complete message
    String completeMsg = junctekDataBuffer.substring(startPos, endPos + 2);
    
    // Only process if message is reasonable length (between 5 and 100 bytes)
    // Changed from 7 to 5 - many valid Junctek messages are 5-6 bytes
    int msgLength = completeMsg.length() / 2; // Convert hex string length to byte count
    if (msgLength >= 5 && msgLength <= 100) {
      debugPrint("✓ Junctek message (" + String(msgLength) + " bytes)");
      parseJunctekData(completeMsg);
    } else {
      debugPrint("⚠ Invalid message length: " + String(msgLength));
    }
    
    // Remove processed message from buffer, keep the rest
    junctekDataBuffer = junctekDataBuffer.substring(endPos + 2);
    
    // If buffer is now empty or too short for another message, exit loop
    if (junctekDataBuffer.length() < 4) {
      break;
    }
  }
  
  // Timeout check only if buffer has been idle
  if (junctekDataBuffer.length() > 0 && 
      (millis() - lastJunctekFragment) > JUNCTEK_BUFFER_TIMEOUT) {
    debugPrint("⚠ Buffer timeout (" + String(junctekDataBuffer.length()) + " chars)");
    junctekDataBuffer = "";
  }
}

void onRenogyBLENotify(NimBLERemoteCharacteristic* pChar, uint8_t* pData, size_t length, bool isNotify) {
  if (length <= sizeof(responseBuffer)) {
    memcpy(responseBuffer, pData, length);
    responseLength = length;
    
    if (length >= 1) {
      responseDeviceId = pData[0];
    }
    
    if (waitingForResponse) {
      parseModbusResponse(responseBuffer, responseLength, currentCommand);
      waitingForResponse = false;
      currentCommand++;
      if (currentCommand >= numRenogyCommands) {
        allCommandsCompleted = true;
        freshDataAvailable = true;
      }
    }
  }
}

String parseFaultCodes(uint32_t faultCode) {
  if (faultCode == 0) return "No faults";
  
  String faults = "";
  int count = 0;
  
  if (faultCode & (1UL << 30)) { if (count++ > 0) faults += ", "; faults += "Charge MOS short"; }
  if (faultCode & (1UL << 29)) { if (count++ > 0) faults += ", "; faults += "Anti-reverse MOS short"; }
  if (faultCode & (1UL << 28)) { if (count++ > 0) faults += ", "; faults += "PV reverse"; }
  if (faultCode & (1UL << 27)) { if (count++ > 0) faults += ", "; faults += "PV overvoltage"; }
  if (faultCode & (1UL << 21)) { if (count++ > 0) faults += ", "; faults += "Controller temp high"; }
  if (faultCode & (1UL << 18)) { if (count++ > 0) faults += ", "; faults += "Battery undervoltage"; }
  if (faultCode & (1UL << 17)) { if (count++ > 0) faults += ", "; faults += "Battery overvoltage"; }
  
  return faults.length() > 0 ? faults : "FC Unknown";
}

void parseModbusResponse(uint8_t* data, size_t length, int commandIndex) {
  if (data == nullptr || length < 5) return;
  
  uint8_t receivedDeviceId = data[0];
  if (data[1] != MODBUS_FUNCTION_READ) return;
  
  responseDeviceId = receivedDeviceId;
  uint8_t byteCount = data[2];
  uint8_t* payload = &data[3];
  
  switch (commandIndex) {
    case 0: // Model
      if (byteCount >= 16) {
        String model = "";
        for (int i = 0; i < 16; i++) {
          if (payload[i] >= 32 && payload[i] <= 126) {
            model += (char)payload[i];
          }
        }
        model.trim();
        if (model.length() > 0) currentMPPTData.deviceModel = model;
      }
      break;
      
    case 1: // Device address
      if (byteCount >= 2) {
        uint16_t reportedId = (payload[0] << 8) | payload[1];
        currentMPPTData.deviceId = reportedId;
        
        if (responseDeviceId != 0 && responseDeviceId != 255) {
          discoveredDeviceId = responseDeviceId;
          deviceIdDiscovered = true;
        }
      }
      break;
      
    case 2: // Main data
      if (byteCount >= 68) {
        currentMPPTData.batteryPercentage = (payload[0] << 8) | payload[1];
        currentMPPTData.batteryVoltage = ((payload[2] << 8) | payload[3]) * 0.1;
        currentMPPTData.batteryCurrent = ((payload[4] << 8) | payload[5]) * 0.01;
        
        uint16_t tempRaw = (payload[6] << 8) | payload[7];
        currentMPPTData.controllerTemperature = (tempRaw / 100) - 29;
        
        if (byteCount >= 16) {
          currentMPPTData.pvVoltage = ((payload[14] << 8) | payload[15]) * 0.1;
        }
        if (byteCount >= 18) {
          currentMPPTData.pvCurrent = ((payload[16] << 8) | payload[17]) * 0.01;
        }
        if (byteCount >= 20) {
          currentMPPTData.pvPower = (payload[18] << 8) | payload[19];
        }
        if (byteCount >= 66) {
          uint16_t statusRaw = (payload[64] << 8) | payload[65];
          uint8_t chargingState = statusRaw & 0xFF;
          currentMPPTData.chargingStatusCode = chargingState;
          
          switch (chargingState) {
            case 0x00: currentMPPTData.chargingStatus = "deactivated"; break;
            case 0x01: currentMPPTData.chargingStatus = "activated"; break;
            case 0x02: currentMPPTData.chargingStatus = "mppt"; break;
            case 0x03: currentMPPTData.chargingStatus = "equalizing"; break;
            case 0x04: currentMPPTData.chargingStatus = "boost"; break;
            case 0x05: currentMPPTData.chargingStatus = "floating"; break;
            case 0x06: currentMPPTData.chargingStatus = "current_limiting"; break;
            default: currentMPPTData.chargingStatus = "cs unknown"; break;
          }
          
          currentMPPTData.loadStatus = ((statusRaw >> 8) & 0x80) ? true : false;
        }
      }
      break;
      
    case 3: // Battery type
      if (byteCount >= 2) {
        uint16_t batteryType = (payload[0] << 8) | payload[1];
        switch (batteryType) {
          case 1: currentMPPTData.batteryType = "open"; break;
          case 2: currentMPPTData.batteryType = "sealed"; break;
          case 3: currentMPPTData.batteryType = "gel"; break;
          case 4: currentMPPTData.batteryType = "lithium"; break;
          default: currentMPPTData.batteryType = "custom"; break;
        }
      }
      break;
      
    case 4: // Fault codes
      if (byteCount >= 4) {
        currentMPPTData.faultCode = ((uint32_t)payload[0] << 24) | 
                                    ((uint32_t)payload[1] << 16) | 
                                    ((uint32_t)payload[2] << 8) | 
                                    (uint32_t)payload[3];
        
        currentMPPTData.hasFaults = (currentMPPTData.faultCode != 0);
        currentMPPTData.activeFaults = parseFaultCodes(currentMPPTData.faultCode);
        
        if (currentMPPTData.hasFaults && DISPLAY_WAKE_EVENTS) {
          wakeDisplay();
        }
      }
      break;
  }
  
  currentMPPTData.isFakeData = false;
  currentMPPTData.timestamp = millis();
}

void cleanupBLE() {
  if (pRenogyClient != nullptr) {
    if (pRenogyClient->isConnected()) pRenogyClient->disconnect();
    NimBLEDevice::deleteClient(pRenogyClient);
    pRenogyClient = nullptr;
  }
  
  if (pJunctekClient != nullptr) {
    if (pJunctekClient->isConnected()) pJunctekClient->disconnect();
    NimBLEDevice::deleteClient(pJunctekClient);
    pJunctekClient = nullptr;
  }
  
  pRenogyWriteChar = nullptr;
  pRenogyNotifyChar = nullptr;
  pJunctekNotifyChar = nullptr;
  
  if (targetRenogyDevice != nullptr) {
    delete targetRenogyDevice;
    targetRenogyDevice = nullptr;
  }
  
  if (targetJunctekDevice != nullptr) {
    delete targetJunctekDevice;
    targetJunctekDevice = nullptr;
  }
  
  currentCommand = 0;
  waitingForResponse = false;
  allCommandsCompleted = false;
  resetDeviceIdDiscovery();
}

void setupBLE() {
  NimBLEDevice::init("");
  NimBLEDevice::setPower(ESP_PWR_LVL_P9);
  NimBLEDevice::setMTU(517);
}

#if MQTT_ENABLED
void setupMQTT() {
  mqttClient.setServer(mqtt_server, mqtt_port);
  mqttClient.setCallback(mqttCallback);
  mqttClient.setBufferSize(2048);
}
#endif

#if INFLUXDB_ENABLED
void setupInfluxDB() {
  sensor.addTag("device", DEVICE);
  timeSync(TZ_INFO, "pool.ntp.org", "time.nis.gov");
  
  if (influxdb_client.validateConnection()) {
    influxdbConnected = true;
  }
}
#endif

#if LORA_ENABLED
void setupLoRa() {
  debugPrint("Initializing LoRa transmitter...");
  int state = radio.begin(LORA_FREQUENCY);
  if (state == RADIOLIB_ERR_NONE) {
    radio.setBandwidth(LORA_BANDWIDTH);
    radio.setSpreadingFactor(LORA_SPREADING_FACTOR);
    radio.setCodingRate(LORA_CODING_RATE);
    radio.setOutputPower(LORA_OUTPUT_POWER);
    radio.setSyncWord(LORA_SYNC_WORD);
    radio.setPreambleLength(LORA_PREAMBLE_LENGTH);
    debugPrint("✓ LoRa transmitter initialized successfully");
    debugPrint("  Frequency: " + String(LORA_FREQUENCY) + " MHz");
    debugPrint("  Spreading Factor: " + String(LORA_SPREADING_FACTOR));
    debugPrint("  Bandwidth: " + String(LORA_BANDWIDTH) + " kHz");
  } else {
    debugPrint("✗ LoRa transmitter init FAILED, code: " + String(state));
  }
}
#endif

void initializeData() {
  currentMPPTData.deviceModel = "Dev Unknown";
  currentMPPTData.deviceId = 0;
  currentMPPTData.batteryVoltage = 0.0;
  currentMPPTData.batteryCurrent = 0.0;
  currentMPPTData.batteryPercentage = 0;
  currentMPPTData.controllerTemperature = 0;
  currentMPPTData.pvVoltage = 0.0;
  currentMPPTData.pvCurrent = 0.0;
  currentMPPTData.pvPower = 0;
  currentMPPTData.chargingStatus = "CS Unknown";
  currentMPPTData.chargingStatusCode = 255;
  currentMPPTData.batteryType = "Batt Unknown";
  currentMPPTData.timestamp = 0;
  currentMPPTData.isFakeData = false;
  currentMPPTData.faultCode = 0;
  currentMPPTData.hasFaults = false;
  currentMPPTData.activeFaults = "No faults";
  
  currentJunctekData.batteryVolts = 0;
  currentJunctekData.loadVolts = 0;
  currentJunctekData.loadAmps = 0;
  currentJunctekData.loadStatus = 0;
  currentJunctekData.ahRemaining = 0;
  currentJunctekData.discharge = 0;
  currentJunctekData.charge = 0;
  currentJunctekData.minRemaining = 0;
  currentJunctekData.impedance = 0;
  currentJunctekData.loadPowerWatts = 0;
  currentJunctekData.temp = 0;
  currentJunctekData.batteryPercentage = 0;
  currentJunctekData.maxCapacity = 220;
  currentJunctekData.timestamp = 0;
  currentJunctekData.dataValid = false;
}

#if MQTT_ENABLED
bool attemptMQTTConnection() {
  mqttConnectionAttempts++;
  String clientId = "BatterySolarMonitor-" + String(random(0xffff), HEX);
  
  if (mqttClient.connect(clientId.c_str(), mqtt_client_id, mqtt_pass)) {
    mqttConnected = true;
    
    // Subscribe to all topics
    mqttClient.subscribe(mqtt_subscribe_topic);
    debugPrint("MQTT subscribed to: " + String(mqtt_subscribe_topic));
    
    mqttClient.subscribe(mqtt_read_topic);
    debugPrint("MQTT subscribed to: " + String(mqtt_read_topic));
    
    mqttClient.subscribe(mqtt_lora_transmit_topic);
    debugPrint("MQTT subscribed to: " + String(mqtt_lora_transmit_topic));
    
    mqttClient.subscribe(mqtt_lora_test_topic);
    debugPrint("MQTT subscribed to: " + String(mqtt_lora_test_topic));

    mqttClient.subscribe(mqtt_reboot_topic);
    debugPrint("MQTT subscribed to: " + String(mqtt_reboot_topic));
    
    #if OTA_ENABLED
    mqttClient.subscribe(ota_update_topic);
    debugPrint("MQTT subscribed to: " + String(ota_update_topic));
    #endif
    
    debugPrint("✓ MQTT connected and subscribed to all topics");
    return true;
  }
  
  mqttConnected = false;
  debugPrint("✗ MQTT connection failed, rc=" + String(mqttClient.state()));
  return false;
}
#endif

#if MQTT_ENABLED
void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String message = "";
  for (unsigned int i = 0; i < length; i++) {
    message += (char)payload[i];
  }
  
  debugPrint("MQTT RX [" + String(topic) + "]: " + message);
  
  if (String(topic) == mqtt_read_topic) {
    debugPrint("→ Triggering forced BLE cycle");
    forceBLECycle();
    return;
  }
  
  if (String(topic) == mqtt_lora_transmit_topic) {
    debugPrint("→ Triggering forced LoRa transmit");
    forceLoRaTransmit();
    return;
  }
  
  if (String(topic) == mqtt_lora_test_topic) {
    debugPrint("→ LoRa test mode command");
    if (message == "start" || message == "on" || message == "1") {
      startLoRaTestMode();
    } else if (message == "stop" || message == "off" || message == "0") {
      stopLoRaTestMode();
    }
    return;
  }

if (String(topic) == mqtt_reboot_topic) {
    debugPrint("→ Reboot command received");
    
    if (message == "reboot" || message == "restart" || message == "1") {
      debugPrint("=== REBOOTING DEVICE ===");
      
      #if MQTT_ENABLED
      if (mqttConnected) {
        mqttClient.publish(mqtt_reboot_topic, "Rebooting device...");
        delay(500);  // Give time for message to send
      }
      #endif
      
      // Clean disconnect from BLE devices
      if (renogyConnected && pRenogyClient) {
        pRenogyClient->disconnect();
      }
      if (junctekConnected && pJunctekClient) {
        pJunctekClient->disconnect();
      }
      
      // Show reboot message on display
      if (displayOn) {
        display.clear();
        display.drawString(0, 0, "REBOOTING...");
        display.drawString(0, 16, "Please wait");
        display.display();
      }
      
      delay(1000);
      ESP.restart();
    }
    return;
  }

  #if OTA_ENABLED
  if (String(topic) == ota_update_topic) {
    debugPrint("→ OTA command received");
    if (message == "update") {
      publishOTAStatus("OTA update started");
      wakeDisplay();
      performOTAUpdate(firmwareUrl);
    } else if (message == "check") {
      checkForOTAUpdate();
    } else if (message.startsWith("http")) {
      publishOTAStatus("OTA with custom URL");
      wakeDisplay();
      performOTAUpdate(message);
    }
  }
  #endif
}
#endif

#if MQTT_ENABLED
void publishToMQTT() {
  mqttDebugPrint("Publishing to MQTT...");
  
  if (!mqttConnected) {
    mqttDebugPrint("✗ MQTT not connected!");
    return;
  }
  
  String jsonData = createJsonData();
  mqttDebugPrint("JSON size: " + String(jsonData.length()) + " bytes");
  
  bool result = mqttClient.publish(mqtt_publish_topic, jsonData.c_str());
  
  if (result) {
    mqttSuccessfulPublishes++;
    mqttDebugPrint("✓ MQTT published successfully");
  } else {
    mqttFailedPublishes++;
    mqttDebugPrint("✗ MQTT publish failed!");
  }
}
#endif

#if INFLUXDB_ENABLED
void publishToInfluxDB() {
  influxdbDebugPrint("Publishing to InfluxDB...");
  
  bool hasRenogyData = (currentMPPTData.timestamp > 0);
  bool hasJunctekData = currentJunctekData.dataValid;
  
  if (!hasRenogyData && !hasJunctekData) {
    influxdbDebugPrint("✗ No data to publish");
    return;
  }
  
  influxdbDebugPrint("Data: " + String(hasRenogyData ? "R" : "") + String(hasJunctekData ? "J" : ""));
  
  if (!reconnectWiFiForInfluxDB()) {
    influxdbDebugPrint("✗ WiFi reconnection failed");
    return;
  }
  
  sensor.clearFields();
  
  if (hasRenogyData) {
    sensor.addField("mppt_battery_voltage", currentMPPTData.batteryVoltage);
    sensor.addField("mppt_battery_current", currentMPPTData.batteryCurrent);
    sensor.addField("mppt_battery_percentage", currentMPPTData.batteryPercentage);
    sensor.addField("mppt_controller_temperature", currentMPPTData.controllerTemperature);
    sensor.addField("solar_voltage", currentMPPTData.pvVoltage);
    sensor.addField("solar_current", currentMPPTData.pvCurrent);
    sensor.addField("solar_power", currentMPPTData.pvPower);
    sensor.addField("charging_status", currentMPPTData.chargingStatus);
    sensor.addField("mppt_fault_code", String(currentMPPTData.faultCode, HEX));
    sensor.addField("mppt_has_faults", currentMPPTData.hasFaults);
  }
  
  if (hasJunctekData) {
    sensor.addField("battery_voltage", currentJunctekData.batteryVolts);
    sensor.addField("battery_current", currentJunctekData.loadAmps);
    sensor.addField("battery_power", currentJunctekData.loadPowerWatts);
    sensor.addField("battery_percentage", currentJunctekData.batteryPercentage);
    sensor.addField("battery_ah_remaining", currentJunctekData.ahRemaining);
    sensor.addField("battery_discharge_kwh", currentJunctekData.discharge);
    sensor.addField("battery_charge_kwh", currentJunctekData.charge);
    sensor.addField("battery_minutes_remaining", currentJunctekData.minRemaining);
    sensor.addField("battery_impedance", currentJunctekData.impedance);
    sensor.addField("battery_temperature", currentJunctekData.temp);
  }
  
  sensor.addField("version", FIRMWARE_VERSION);
  
  if (influxdb_client.writePoint(sensor)) {
    influxdbDebugPrint("✓ Published successfully");
  } else {
    influxdbDebugPrint("✗ Publish failed!");
  }
}
#endif

#if LORA_ENABLED
void transmitLoRa(int LoRaTxNum) {
  loraDebugPrint("Transmitting LoRa packet " + String(LoRaTxNum) + "...");
  delay(100);  // ADD THIS - give receiver time to be ready
  
  StaticJsonDocument<200> doc;

  if (LoRaTxNum == 1) {
    doc["bv"] = String(currentJunctekData.batteryVolts, 1).toFloat();
    doc["lv"] = String(currentJunctekData.loadVolts, 1).toFloat();
    doc["bc"] = String(currentJunctekData.loadAmps, 1).toFloat();
    doc["bp"] = currentJunctekData.batteryPercentage;
    doc["ah"] = String(currentJunctekData.ahRemaining, 1).toFloat();
    doc["cs"] = currentMPPTData.chargingStatusCode;
    doc["ct"] = currentMPPTData.controllerTemperature;
  } else {
    doc["pv"] = String(currentMPPTData.pvVoltage, 1).toFloat();
    doc["pc"] = String(currentMPPTData.pvCurrent, 1).toFloat();
    doc["pw"] = currentMPPTData.pvPower;
    doc["kw"] = String(currentJunctekData.discharge + currentJunctekData.charge, 2).toFloat();
    doc["mr"] = currentJunctekData.minRemaining;
    doc["err"] = currentMPPTData.faultCode;
    doc["tst"] = loraTestCounter;  // Add test counter
  }
  
  String jsonData;
  serializeJson(doc, jsonData);
  loraDebugPrint("LoRa JSON: " + jsonData);
  
  int state = radio.transmit(jsonData);
  
  if (state == RADIOLIB_ERR_NONE) {
    loraDebugPrint("✓ LoRa packet " + String(LoRaTxNum) + " sent");
  } else {
    loraDebugPrint("✗ LoRa transmit failed, code: " + String(state));
  }
}
#endif

String createJsonData() {
  StaticJsonDocument<1024> doc;
  
  doc["timestamp"] = millis();
  doc["firmware"] = FIRMWARE_VERSION;
  doc["device_model_mppt"] = currentMPPTData.deviceModel;
  
  doc["mppt_battery_voltage"] = currentMPPTData.batteryVoltage;
  doc["mppt_battery_current"] = currentMPPTData.batteryCurrent;
  doc["mppt_battery_percentage"] = currentMPPTData.batteryPercentage;
  doc["mppt_controller_temp"] = currentMPPTData.controllerTemperature;
  doc["charging_status"] = currentMPPTData.chargingStatus;
  
  doc["solar_voltage"] = currentMPPTData.pvVoltage;
  doc["solar_current"] = currentMPPTData.pvCurrent;
  doc["solar_power"] = currentMPPTData.pvPower;
  
  doc["mppt_fault_code"] = String(currentMPPTData.faultCode, HEX);
  doc["mppt_has_faults"] = currentMPPTData.hasFaults;
  
  doc["battery_voltage"] = currentJunctekData.loadVolts;
  doc["battery_current"] = currentJunctekData.loadAmps;
  doc["battery_power"] = currentJunctekData.loadPowerWatts;
  doc["battery_percentage"] = currentJunctekData.batteryPercentage;
  doc["battery_ah_remaining"] = currentJunctekData.ahRemaining;
  doc["battery_total_kwh"] = currentJunctekData.discharge + currentJunctekData.charge;
  doc["battery_minutes_remaining"] = currentJunctekData.minRemaining;
  doc["battery_temperature"] = currentJunctekData.temp;
  
  doc["ble_state"] = getBLEStateString(bleState);
  doc["wifi_ssid"] = connectedWiFiSSID;
  doc["wifi_rssi"] = wifiRSSI;
  
  String jsonString;
  serializeJson(doc, jsonString);
  return jsonString;
}

void updateDisplay() {
  if (!displayOn) return;
  
  display.clear();
  
  // ADD TEST MODE INDICATOR AT TOP
  if (loraTestModeActive) {
    display.drawString(0, 0, "TEST MODE #" + String(loraTestCounter));
    display.drawString(0, 12, "LoRa: Transmitting...");
    display.drawString(0, 24, "Interval: " + String(LORA_TEST_INTERVAL/1000) + "s");
    display.drawString(0, 36, "Next: " + String((LORA_TEST_INTERVAL - (millis() - lastLoraTestTransmit))/1000) + "s");
    
    String status = "";
    if (wifiConnected) status += "W";
    if (mqttConnected) status += "M";
    display.drawString(110, 0, status);
    
    display.display();
    return;  // Skip normal display when in test mode
  }
  
  display.drawString(0, 0, "Battery+Solar");
  
  String status = "";
  if (renogyConnected) status += "R";
  if (junctekConnected) status += "J";
  if (wifiConnected) status += "W";
  if (mqttConnected) status += "M";
  if (currentMPPTData.hasFaults) status += "(!)";
  display.drawString(90, 0, status);
  
  display.drawString(0, 12, "Bat: " + String(currentJunctekData.batteryVolts, 1) + "V " + 
                     String(currentJunctekData.batteryPercentage, 0) + "%");
  
  display.drawString(0, 24, "Sol: " + String(currentMPPTData.pvVoltage, 1) + "V " + 
                     String(currentMPPTData.pvPower) + "W");
  
  display.drawString(0, 36, currentMPPTData.chargingStatus + " " + 
                     String(currentJunctekData.loadAmps, 1) + "A");
  
  if (bleState == BLE_STATE_WAITING) {
    unsigned long timeToNext = (nextBLECycle > millis()) ? (nextBLECycle - millis()) / 1000 : 0;
    String timeStr = String(timeToNext / 60) + "m" + String(timeToNext % 60) + "s";
    display.drawString(0, 48, "Next:" + timeStr);
  } else {
    display.drawString(0, 48, getBLEStateString(bleState));
  }
  
  display.display();
}

void reportStatus() {
  debugPrint(F("=== STATUS ==="));
  debugPrint("FW: " + String(FIRMWARE_VERSION));
  debugPrint("Renogy: " + String(renogyConnected ? "OK" : "FAIL"));
  debugPrint("Junctek: " + String(junctekConnected ? "OK" : "FAIL"));
  debugPrint("WiFi: " + connectedWiFiSSID);
  #if MQTT_ENABLED
  debugPrint("MQTT: " + String(mqttConnected ? "OK" : "FAIL"));
  #endif
}

void debugPrint(String message) {
  if (DEBUG_ENABLED) {
    Serial.print(F("["));
    Serial.print(millis());
    Serial.print(F("] "));
    Serial.println(message);
  }
}

void mqttDebugPrint(String message) {
  if (MQTT_DEBUG_ENABLED) {
    Serial.print(F("[MQTT] "));
    Serial.println(message);
  }
}

void influxdbDebugPrint(String message) {
  if (INFLUXDB_DEBUG_ENABLED) {
    Serial.print(F("[InfluxDB] "));
    Serial.println(message);
  }
}

void loraDebugPrint(String message) {
  if (LORA_DEBUG_ENABLED) {
    Serial.print(F("[LoRa] "));
    Serial.println(message);
  }
}

void startLoRaTestMode() {
  #if !LORA_ENABLED
  debugPrint("✗ LoRa test mode: LoRa not enabled!");
  #if MQTT_ENABLED
  if (mqttConnected) {
    mqttClient.publish(mqtt_lora_status_topic, "ERROR: LoRa not enabled");
  }
  #endif
  return;
  #endif
  
  #if LORA_ENABLED
  if (loraTestModeActive) {
    debugPrint("⚠ LoRa test mode already active");
    return;
  }
  
  debugPrint("=== STARTING LORA TEST MODE ===");
  loraTestModeActive = true;
  loraTestCounter = 0;
  lastLoraTestTransmit = 0;  // Force immediate first transmission
  
  wakeDisplay();
  
  #if MQTT_ENABLED
  if (mqttConnected) {
    mqttClient.publish(mqtt_lora_status_topic, "Test mode started");
  }
  #endif
  
  // LED animation to show test mode activation
  unsigned long now = millis();
  ledAnimationActive = true;
  ledAnimationStart = now;
  ledAnimationStep = 0;
  ledAnimationDirection = true;
  
  debugPrint("✓ LoRa test mode activated");
  debugPrint("  Transmit interval: " + String(LORA_TEST_INTERVAL) + "ms");
  #endif
}

void stopLoRaTestMode() {
  #if !LORA_ENABLED
  return;
  #endif
  
  #if LORA_ENABLED
  if (!loraTestModeActive) {
    debugPrint("⚠ LoRa test mode not active");
    return;
  }
  
  debugPrint("=== STOPPING LORA TEST MODE ===");
  debugPrint("  Total test packets sent: " + String(loraTestCounter));
  
  loraTestModeActive = false;
  loraTestCounter = 0;
  
  #if MQTT_ENABLED
  if (mqttConnected) {
    String statusMsg = "Test mode stopped. Sent " + String(loraTestCounter) + " packets";
    mqttClient.publish(mqtt_lora_status_topic, statusMsg.c_str());
  }
  #endif
  
  debugPrint("✓ LoRa test mode deactivated");
  #endif
}

void handleLoRaTestMode() {
  #if !LORA_ENABLED
  return;
  #endif
  
  #if LORA_ENABLED
  if (!loraTestModeActive) return;
  
  unsigned long now = millis();
  
  // Check if it's time for the next test transmission
  if (now - lastLoraTestTransmit >= LORA_TEST_INTERVAL) {
    lastLoraTestTransmit = now;
    loraTestCounter++;
    
    loraDebugPrint("=== LoRa Test Transmission #" + String(loraTestCounter) + " ===");
    
    // Create test packet with current data + test counter
    StaticJsonDocument<256> doc;
    
    // Alternate between two packet types (like normal operation)
    if (loraTestCounter % 2 == 1) {
      // First packet type - battery data
      doc["bv"] = String(currentJunctekData.batteryVolts, 1).toFloat();
      doc["lv"] = String(currentJunctekData.loadVolts, 1).toFloat();
      doc["bc"] = String(currentJunctekData.loadAmps, 1).toFloat();
      doc["bp"] = currentJunctekData.batteryPercentage;
      doc["ah"] = String(currentJunctekData.ahRemaining, 1).toFloat();
      doc["cs"] = currentMPPTData.chargingStatusCode;
      
      loraDebugPrint("  Type: Battery data");
    } else {
      // Second packet type - PV data
      doc["pv"] = String(currentMPPTData.pvVoltage, 1).toFloat();
      doc["pc"] = String(currentMPPTData.pvCurrent, 1).toFloat();
      doc["pw"] = currentMPPTData.pvPower;
      doc["kw"] = String(currentJunctekData.discharge + currentJunctekData.charge, 2).toFloat();
      doc["mr"] = currentJunctekData.minRemaining;
      doc["err"] = currentMPPTData.faultCode;
      doc["tst"] = loraTestCounter;  // Add test counter
      
      loraDebugPrint("  Type: PV data");
    }
    
    String jsonData;
    serializeJson(doc, jsonData);
    loraDebugPrint("  JSON: " + jsonData);
    loraDebugPrint("  Size: " + String(jsonData.length()) + " bytes");
    
    // Transmit
    int state = radio.transmit(jsonData);
    
    if (state == RADIOLIB_ERR_NONE) {
      loraDebugPrint("✓ Test packet #" + String(loraTestCounter) + " sent successfully");
      
      // Publish status to MQTT every 10 packets
      #if MQTT_ENABLED
      if (mqttConnected && (loraTestCounter % 10 == 0)) {
        String statusMsg = "Test mode active: " + String(loraTestCounter) + " packets sent";
        mqttClient.publish(mqtt_lora_status_topic, statusMsg.c_str());
      }
      #endif
    } else {
      loraDebugPrint("✗ Test packet #" + String(loraTestCounter) + " failed, code: " + String(state));
    }
  }
  #endif
}

#if OTA_ENABLED
void publishOTAStatus(String status) {
  otaStatusMessage = status;
  debugPrint("[OTA] " + status);
  
  #if MQTT_ENABLED
  if (mqttConnected) {
    mqttClient.publish(ota_status_topic, status.c_str());
  }
  #endif
  
  if (displayOn) {
    display.clear();
    display.drawString(0, 0, "OTA UPDATE");
    display.drawString(0, 16, status);
    display.display();
  }
}

String getLatestVersion() {
  if (!wifiConnected) return "";
  
  HTTPClient http;
  http.begin(versionCheckUrl);
  http.setTimeout(10000);
  
  int httpCode = http.GET();
  String version = "";
  
  if (httpCode == HTTP_CODE_OK) {
    version = http.getString();
    version.trim();
  }
  
  http.end();
  return version;
}

void checkForOTAUpdate() {
  publishOTAStatus("Checking...");
  
  String latest = getLatestVersion();
  if (latest.length() == 0) {
    publishOTAStatus("Check failed");
    return;
  }
  
  if (latest == String(FIRMWARE_VERSION)) {
    publishOTAStatus("Up to date");
  } else {
    publishOTAStatus("Update available: v" + latest);
  }
  
  delay(3000);
}

void performOTAUpdate(String url) {
  if (otaInProgress) return;
  if (!wifiConnected) {
    publishOTAStatus("No WiFi");
    return;
  }
  
  otaInProgress = true;
  publishOTAStatus("Starting...");
  
  if (renogyConnected && pRenogyClient) pRenogyClient->disconnect();
  if (junctekConnected && pJunctekClient) pJunctekClient->disconnect();
  
  delay(1000);
  
  bool success = downloadAndInstallFirmware(url);
  
  if (success) {
    publishOTAStatus("Success! Rebooting...");
    delay(3000);
    ESP.restart();
  } else {
    publishOTAStatus("Failed!");
    otaInProgress = false;
    delay(3000);
  }
}

bool downloadAndInstallFirmware(String url) {
  HTTPClient http;
  http.begin(url);
  http.setTimeout(30000);
  
  int httpCode = http.GET();
  if (httpCode != HTTP_CODE_OK) {
    http.end();
    return false;
  }
  
  int contentLength = http.getSize();
  if (contentLength <= 0 || !Update.begin(contentLength)) {
    http.end();
    return false;
  }
  
  WiFiClient* stream = http.getStreamPtr();
  size_t written = 0;
  uint8_t buff[1024];
  
  while (http.connected() && (written < contentLength)) {
    size_t size = stream->available();
    if (size > 0) {
      int c = stream->readBytes(buff, ((size > sizeof(buff)) ? sizeof(buff) : size));
      if (Update.write(buff, c) != c) {
        Update.abort();
        http.end();
        return false;
      }
      written += c;
      
      int percent = (written * 100) / contentLength;
      if (percent % 20 == 0) {
        publishOTAStatus(String(percent) + "%");
      }
    }
    delay(1);
  }
  
  http.end();
  
  if (written != contentLength) {
    Update.abort();
    return false;
  }
  
  return Update.end(true) && Update.isFinished();
}
#endif