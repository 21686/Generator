// log_generator.cpp
#include "log_generator.hpp"

#include <errors/errors.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace db_simulator {

// ─────────────────────────────────────────────────────────────────────────────
// Constructor / factory
// ─────────────────────────────────────────────────────────────────────────────

LogGenerator::LogGenerator(const LoggerConfig& cfg)
    : config(cfg),
      logger("db_simulator"),
      rng(std::random_device{}()),
      uniformDist(0.0, 1.0),
      currentTimeMs(std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count()),
      masterLsn(1'000'000),
      replicaLsn(1'000'000),
      currentLagSec(normalLagMin),
      anomaliesGenerated(0),
      currentSegment(0) {}

std::tuple<std::shared_ptr<LogGenerator>, error> LogGenerator::Create(
    const LoggerConfig& cfg) {
  auto gen = std::shared_ptr<LogGenerator>(new LogGenerator(cfg));
  if (auto err = gen->initializeFiles()) {
    return {nullptr, err};
  }
  return {gen, nullptr};
}

// ─────────────────────────────────────────────────────────────────────────────
// Public interface
// ─────────────────────────────────────────────────────────────────────────────

error LogGenerator::Generate() {
  for (int s = 0; s < config.totalSegments; ++s) {
    currentSegment = s;
    anomaliesGenerated = 0;
    if (auto err = generateSegment()) {
      return err;
    }
  }
  return nullptr;
}

error LogGenerator::Finalize() {
  // Write CSV buffer
  {
    std::lock_guard<std::mutex> lock(fileMutex);
    for (const auto& row : csvBuffer) {
      csvFile << row.timestampMs << "," << row.templateId << "," << row.logLevel
              << "," << std::fixed << std::setprecision(3) << row.lagSec << ","
              << row.lossPercent << "," << row.masterLsn << ","
              << row.replicaLsn << "\n";
    }
    csvBuffer.clear();

    // Close JSON array
    jsonFile << "\n]\n";
    jsonFile.flush();
    csvFile.flush();
  }
  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Initialization
// ─────────────────────────────────────────────────────────────────────────────

error LogGenerator::initializeFiles() {
  jsonFile.open(config.outputJsonFile, std::ios::out | std::ios::trunc);
  if (!jsonFile.is_open()) {
    return errors::New("failed to open JSON output file: " +
                       config.outputJsonFile);
  }

  csvFile.open(config.outputCsvFile, std::ios::out | std::ios::trunc);
  if (!csvFile.is_open()) {
    return errors::New("failed to open CSV output file: " +
                       config.outputCsvFile);
  }

  // JSON array opening bracket
  jsonFile << "[\n";

  // CSV header
  csvFile << "timestamp_ms,template_id,log_level,lag_sec,loss_percent,"
             "master_lsn,replica_lsn\n";

  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Segment generation
// ─────────────────────────────────────────────────────────────────────────────

error LogGenerator::generateSegment() {
  // Distribute normal logs evenly between anomaly slots
  int normalLogsPerAnomaly = static_cast<int>(
      config.timeSegmentSeconds / (config.anomaliesPerSegment + 1) / 5.0);
  if (normalLogsPerAnomaly < 2) normalLogsPerAnomaly = 2;

  for (int a = 0; a < config.anomaliesPerSegment; ++a) {
    if (auto err = generateNormalLogs(normalLogsPerAnomaly)) {
      return err;
    }
    if (auto err = generateAnomaly()) {
      return err;
    }
  }

  // Trailing normal logs
  if (auto err = generateNormalLogs(normalLogsPerAnomaly)) {
    return err;
  }
  return nullptr;
}

error LogGenerator::generateNormalLogs(int count) {
  for (int i = 0; i < count; ++i) {
    currentLagSec = randomDouble(normalLagMin, normalLagMax);
    masterLsn += lsnStepNormal;
    replicaLsn += lsnStepNormal;

    advanceTime(randomDouble(3.0, 8.0));

    int choice = randomInt(0, 4);
    switch (choice) {
      case 0:
        logReplicationStatus();
        break;
      case 1:
        logDataIntegrityCheckStarted();
        break;
      case 2: {
        std::string table = "table_" + std::to_string(randomInt(1, 5));
        int64_t rows = randomInt(1000, 50000);
        logTableChecksumCalculated(table, rows);
        break;
      }
      default:
        logReplicationStatus();
        break;
    }
  }
  return nullptr;
}

error LogGenerator::generateAnomaly() {
  // Phase 1: lag increased
  double prevLag = currentLagSec;
  for (int i = 0; i < anomalySequenceLength; ++i) {
    currentLagSec += lagIncreaseStep * randomDouble(5.0, 15.0);
    masterLsn += lsnStepNormal;
    advanceTime(randomDouble(1.0, 3.0));
    logReplicationLagIncreased(prevLag);
    prevLag = currentLagSec;
  }

  // Phase 2: packet loss + critical lag
  double lossPercent = randomDouble(packetLossMin, packetLossMax);
  advanceTime(randomDouble(1.0, 2.0));
  logNetworkPacketLossDetected();

  currentLagSec += randomDouble(20.0, 60.0);
  advanceTime(randomDouble(1.0, 2.0));
  logReplicationLagCritical(prevLag);

  // Occasionally escalate to divergence / stopped
  if (randomDouble(0.0, 1.0) < 0.3) {
    advanceTime(randomDouble(0.5, 1.5));
    std::string table = "table_" + std::to_string(randomInt(1, 5));
    logDataDivergenceDetected(table);
  }

  if (randomDouble(0.0, 1.0) < 0.15) {
    advanceTime(randomDouble(0.5, 1.0));
    logReplicationStopped();
  }

  // Recovery phase
  advanceTime(randomDouble(2.0, 5.0));
  logNetworkConnectionRecovered();

  advanceTime(randomDouble(1.0, 3.0));
  logReplicationCatchingUp();

  double recoveryTime = randomDouble(5.0, 20.0);
  advanceTime(recoveryTime);
  replicaLsn = masterLsn;
  currentLagSec = randomDouble(normalLagMin, normalLagMax);
  logReplicationRecovered(recoveryTime);

  anomaliesGenerated++;
  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Individual log events
// ─────────────────────────────────────────────────────────────────────────────

void LogGenerator::logReplicationStatus() {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationStatus);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "replication_status"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"lag_sec", currentLagSec},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info(
      "replication_status replica={} lag={:.2f}s master_lsn={} "
      "replica_lsn={}",
      config.replicaId, currentLagSec, formatLsn(masterLsn),
      formatLsn(replicaLsn));
}

void LogGenerator::logReplicationLagIncreased(double previousLag) {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationLagIncreased);
  stats.logLevel = 1;  // WARN
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "WARN"},
                          {"template_id", stats.templateId},
                          {"event", "replication_lag_increased"},
                          {"replica_id", config.replicaId},
                          {"previous_lag_sec", previousLag},
                          {"current_lag_sec", currentLagSec},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.warn(
      "replication_lag_increased replica={} prev={:.2f}s "
      "current={:.2f}s",
      config.replicaId, previousLag, currentLagSec);
}

void LogGenerator::logNetworkPacketLossDetected() {
  double lossPercent = randomDouble(packetLossMin, packetLossMax);

  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::NetworkPacketLossDetected);
  stats.logLevel = 2;  // ERROR
  stats.lagSec = currentLagSec;
  stats.lossPercent = lossPercent;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {
      {"timestamp", getCurrentTimestamp()},
      {"level", "ERROR"},
      {"template_id", stats.templateId},
      {"event", "network_packet_loss_detected"},
      {"replica_id", config.replicaId},
      {"master_host", config.masterHost},
      {"loss_percent", lossPercent},
      {"connection_quality", getConnectionQuality(lossPercent)},
      {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.error(
      "network_packet_loss_detected replica={} loss={:.1f}% "
      "quality={}",
      config.replicaId, lossPercent, getConnectionQuality(lossPercent));
}

void LogGenerator::logReplicationLagCritical(double previousLag) {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationLagCritical);
  stats.logLevel = 2;  // ERROR
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "ERROR"},
                          {"template_id", stats.templateId},
                          {"event", "replication_lag_critical"},
                          {"replica_id", config.replicaId},
                          {"lag_sec", currentLagSec},
                          {"threshold_sec", 100.0},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.error(
      "replication_lag_critical replica={} lag={:.2f}s "
      "threshold=100s",
      config.replicaId, currentLagSec);
}

void LogGenerator::logNetworkConnectionRecovered() {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId =
      static_cast<int>(EventTemplate::NetworkConnectionRecovered);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "network_connection_recovered"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info("network_connection_recovered replica={} master={}",
              config.replicaId, config.masterHost);
}

void LogGenerator::logReplicationCatchingUp() {
  replicaLsn += lsnStepCatchingUp;

  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationCatchingUp);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "replication_catching_up"},
                          {"replica_id", config.replicaId},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info(
      "replication_catching_up replica={} master_lsn={} "
      "replica_lsn={}",
      config.replicaId, formatLsn(masterLsn), formatLsn(replicaLsn));
}

void LogGenerator::logReplicationRecovered(double recoveryTimeSec) {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationRecovered);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "replication_recovered"},
                          {"replica_id", config.replicaId},
                          {"recovery_time_sec", recoveryTimeSec},
                          {"current_lag_sec", currentLagSec},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info(
      "replication_recovered replica={} recovery_time={:.2f}s "
      "lag={:.2f}s",
      config.replicaId, recoveryTimeSec, currentLagSec);
}

void LogGenerator::logDataIntegrityCheckStarted() {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::DataIntegrityCheckStarted);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "data_integrity_check_started"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info("data_integrity_check_started replica={}", config.replicaId);
}

void LogGenerator::logTableChecksumCalculated(const std::string& table,
                                              int64_t rows) {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::TableChecksumCalculated);
  stats.logLevel = 0;  // INFO
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  int64_t checksum = static_cast<int64_t>(randomDouble(1e9, 9e9));

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "table_checksum_calculated"},
                          {"replica_id", config.replicaId},
                          {"table", table},
                          {"rows_checked", rows},
                          {"checksum", checksum},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.info(
      "table_checksum_calculated replica={} table={} rows={} "
      "checksum={}",
      config.replicaId, table, rows, checksum);
}

void LogGenerator::logDataDivergenceDetected(const std::string& table) {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::DataDivergenceDetected);
  stats.logLevel = 3;  // CRITICAL
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  int64_t masterChecksum = static_cast<int64_t>(randomDouble(1e9, 9e9));
  int64_t replicaChecksum = masterChecksum + randomInt(1, 10000);

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "CRITICAL"},
                          {"template_id", stats.templateId},
                          {"event", "data_divergence_detected"},
                          {"replica_id", config.replicaId},
                          {"table", table},
                          {"master_checksum", masterChecksum},
                          {"replica_checksum", replicaChecksum},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.critical(
      "data_divergence_detected replica={} table={} "
      "master_checksum={} replica_checksum={}",
      config.replicaId, table, masterChecksum, replicaChecksum);
}

void LogGenerator::logReplicationStopped() {
  EventStats stats;
  stats.timestampMs = getCurrentTimestampMs();
  stats.templateId = static_cast<int>(EventTemplate::ReplicationStopped);
  stats.logLevel = 3;  // CRITICAL
  stats.lagSec = currentLagSec;
  stats.lossPercent = 0.0;
  stats.masterLsn = masterLsn;
  stats.replicaLsn = replicaLsn;

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "CRITICAL"},
                          {"template_id", stats.templateId},
                          {"event", "replication_stopped"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"lag_sec", currentLagSec},
                          {"master_lsn", formatLsn(masterLsn)},
                          {"replica_lsn", formatLsn(replicaLsn)},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);
  logger.critical("replication_stopped replica={} lag={:.2f}s",
                  config.replicaId, currentLagSec);
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

std::string LogGenerator::getCurrentTimestamp() const {
  auto ms = currentTimeMs;
  auto sec = ms / 1000;
  auto msRemainder = ms % 1000;

  std::time_t t = static_cast<std::time_t>(sec);
  std::tm tm_buf{};
#ifdef _WIN32
  gmtime_s(&tm_buf, &t);
#else
  gmtime_r(&t, &tm_buf);
#endif

  std::ostringstream oss;
  oss << std::put_time(&tm_buf, "%Y-%m-%dT%H:%M:%S") << "." << std::setfill('0')
      << std::setw(3) << msRemainder << "Z";
  return oss.str();
}

int64_t LogGenerator::getCurrentTimestampMs() const { return currentTimeMs; }

std::string LogGenerator::formatLsn(int64_t lsn) const {
  // Postgres-style LSN: XXXXXXXX/XXXXXXXX
  uint32_t hi = static_cast<uint32_t>(lsn >> 32);
  uint32_t lo = static_cast<uint32_t>(lsn & 0xFFFFFFFF);
  std::ostringstream oss;
  oss << std::uppercase << std::hex << hi << "/" << lo;
  return oss.str();
}

std::string LogGenerator::getConnectionQuality(double lossPercent) const {
  if (lossPercent < 1.0) return "excellent";
  if (lossPercent < 3.0) return "good";
  if (lossPercent < 7.0) return "fair";
  if (lossPercent < 12.0) return "poor";
  return "critical";
}

void LogGenerator::advanceTime(double seconds) {
  currentTimeMs += static_cast<int64_t>(seconds * 1000.0);
}

bool LogGenerator::shouldGenerateAnomaly() const {
  return anomaliesGenerated < config.anomaliesPerSegment;
}

void LogGenerator::writeJsonLog(const nlohmann::json& logEntry,
                                int /*templateId*/) {
  std::lock_guard<std::mutex> lock(fileMutex);
  static bool firstEntry = true;
  if (!firstEntry) {
    jsonFile << ",\n";
  }
  firstEntry = false;
  jsonFile << logEntry.dump(2);
}

void LogGenerator::bufferCsvStats(const EventStats& stats) {
  std::lock_guard<std::mutex> lock(fileMutex);
  csvBuffer.push_back(stats);
}

double LogGenerator::randomDouble(double min, double max) {
  return min + (max - min) * uniformDist(rng);
}

int LogGenerator::randomInt(int min, int max) {
  std::uniform_int_distribution<int> dist(min, max);
  return dist(rng);
}

}  // namespace db_simulator