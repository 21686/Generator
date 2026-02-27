// log_generator.cpp
#include "log_generator.hpp"

#include <iomanip>
#include <iostream>
#include <sstream>

#include "ports/errors/errors.hpp"

namespace db_simulator {

// ─────────────────────────────────────────────────────────────────────────────
// Constructor / factory
// ─────────────────────────────────────────────────────────────────────────────

LogGenerator::LogGenerator(const LoggerConfig& cfg)
    : config(cfg),
      logger(kvalog::CreateLogger("db_simulator", "log_generator")),
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
  {
    std::lock_guard<std::mutex> lock(fileMutex);

    for (const auto& row : csvBuffer) {
      csvFile << row.timestampMs << "," << row.templateId << "," << row.logLevel
              << "," << std::fixed << std::setprecision(3) << row.lagSec << ","
              << row.lossPercent << "," << row.masterLsn << ","
              << row.replicaLsn << "\n";
    }
    csvBuffer.clear();

    jsonFile << "\n]\n";
    jsonFile.flush();
    csvFile.flush();
  }

  logger.Flush();
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

  jsonFile << "[\n";
  csvFile << "timestamp_ms,template_id,log_level,lag_sec,loss_percent,"
             "master_lsn,replica_lsn\n";

  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Segment generation
// ─────────────────────────────────────────────────────────────────────────────

error LogGenerator::generateSegment() {
  int normalLogsPerSlot = static_cast<int>(
      config.timeSegmentSeconds / (config.anomaliesPerSegment + 1) / 5.0);
  if (normalLogsPerSlot < 2) normalLogsPerSlot = 2;

  for (int a = 0; a < config.anomaliesPerSegment; ++a) {
    if (auto err = generateNormalLogs(normalLogsPerSlot)) {
      return err;
    }
    if (auto err = generateAnomaly()) {
      return err;
    }
  }

  return generateNormalLogs(normalLogsPerSlot);
}

error LogGenerator::generateNormalLogs(int count) {
  for (int i = 0; i < count; ++i) {
    currentLagSec = randomDouble(normalLagMin, normalLagMax);
    masterLsn += lsnStepNormal;
    replicaLsn += lsnStepNormal;
    advanceTime(randomDouble(3.0, 8.0));

    switch (randomInt(0, 3)) {
      case 0:
        logReplicationStatus();
        break;
      case 1:
        logDataIntegrityCheckStarted();
        break;
      case 2: {
        std::string table = "table_" + std::to_string(randomInt(1, 5));
        logTableChecksumCalculated(table, randomInt(1000, 50000));
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
  // Phase 1: gradual lag growth
  double prevLag = currentLagSec;
  for (int i = 0; i < anomalySequenceLength; ++i) {
    currentLagSec += lagIncreaseStep * randomDouble(5.0, 15.0);
    masterLsn += lsnStepNormal;
    advanceTime(randomDouble(1.0, 3.0));
    logReplicationLagIncreased(prevLag);
    prevLag = currentLagSec;
  }

  // Phase 2: packet loss + critical lag
  advanceTime(randomDouble(1.0, 2.0));
  logNetworkPacketLossDetected();

  currentLagSec += randomDouble(20.0, 60.0);
  advanceTime(randomDouble(1.0, 2.0));
  logReplicationLagCritical(prevLag);

  // Phase 3: optional escalation
  if (randomDouble(0.0, 1.0) < 0.3) {
    advanceTime(randomDouble(0.5, 1.5));
    logDataDivergenceDetected("table_" + std::to_string(randomInt(1, 5)));
  }
  if (randomDouble(0.0, 1.0) < 0.15) {
    advanceTime(randomDouble(0.5, 1.0));
    logReplicationStopped();
  }

  // Phase 4: recovery
  advanceTime(randomDouble(2.0, 5.0));
  logNetworkConnectionRecovered();

  advanceTime(randomDouble(1.0, 3.0));
  logReplicationCatchingUp();

  double recoveryTime = randomDouble(5.0, 20.0);
  advanceTime(recoveryTime);
  replicaLsn = masterLsn;
  currentLagSec = randomDouble(normalLagMin, normalLagMax);
  logReplicationRecovered(recoveryTime);

  ++anomaliesGenerated;
  return nullptr;
}

// ─────────────────────────────────────────────────────────────────────────────
// Individual log events
// ─────────────────────────────────────────────────────────────────────────────

void LogGenerator::logReplicationStatus() {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationStatus),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "replication_status replica=" << config.replicaId
      << " lag=" << std::fixed << std::setprecision(2) << currentLagSec << "s"
      << " master_lsn=" << formatLsn(masterLsn)
      << " replica_lsn=" << formatLsn(replicaLsn);
  logger.Info(msg.str());
}

void LogGenerator::logReplicationLagIncreased(double previousLag) {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationLagIncreased),
                   1,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "replication_lag_increased replica=" << config.replicaId << std::fixed
      << std::setprecision(2) << " prev=" << previousLag << "s"
      << " current=" << currentLagSec << "s";
  logger.Warning(msg.str());
}

void LogGenerator::logNetworkPacketLossDetected() {
  double lossPercent = randomDouble(packetLossMin, packetLossMax);

  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::NetworkPacketLossDetected),
                   2,
                   currentLagSec,
                   lossPercent,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "network_packet_loss_detected replica=" << config.replicaId
      << std::fixed << std::setprecision(1) << " loss=" << lossPercent << "%"
      << " quality=" << getConnectionQuality(lossPercent);
  logger.Error(msg.str());
}

void LogGenerator::logReplicationLagCritical(double /*previousLag*/) {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationLagCritical),
                   2,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "replication_lag_critical replica=" << config.replicaId << std::fixed
      << std::setprecision(2) << " lag=" << currentLagSec << "s threshold=100s";
  logger.Error(msg.str());
}

void LogGenerator::logNetworkConnectionRecovered() {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::NetworkConnectionRecovered),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "network_connection_recovered"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);

  logger.Info("network_connection_recovered replica=" + config.replicaId +
              " master=" + config.masterHost);
}

void LogGenerator::logReplicationCatchingUp() {
  replicaLsn += lsnStepCatchingUp;

  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationCatchingUp),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  logger.Info("replication_catching_up replica=" + config.replicaId +
              " master_lsn=" + formatLsn(masterLsn) +
              " replica_lsn=" + formatLsn(replicaLsn));
}

void LogGenerator::logReplicationRecovered(double recoveryTimeSec) {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationRecovered),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "replication_recovered replica=" << config.replicaId << std::fixed
      << std::setprecision(2) << " recovery_time=" << recoveryTimeSec << "s"
      << " lag=" << currentLagSec << "s";
  logger.Info(msg.str());
}

void LogGenerator::logDataIntegrityCheckStarted() {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::DataIntegrityCheckStarted),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

  nlohmann::json entry = {{"timestamp", getCurrentTimestamp()},
                          {"level", "INFO"},
                          {"template_id", stats.templateId},
                          {"event", "data_integrity_check_started"},
                          {"replica_id", config.replicaId},
                          {"master_host", config.masterHost},
                          {"process_id", config.processId}};

  writeJsonLog(entry, stats.templateId);
  bufferCsvStats(stats);

  logger.Info("data_integrity_check_started replica=" + config.replicaId);
}

void LogGenerator::logTableChecksumCalculated(const std::string& table,
                                              int64_t rows) {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::TableChecksumCalculated),
                   0,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  logger.Info("table_checksum_calculated replica=" + config.replicaId +
              " table=" + table + " rows=" + std::to_string(rows) +
              " checksum=" + std::to_string(checksum));
}

void LogGenerator::logDataDivergenceDetected(const std::string& table) {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::DataDivergenceDetected),
                   3,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  logger.Critical("data_divergence_detected replica=" + config.replicaId +
                  " table=" + table +
                  " master_checksum=" + std::to_string(masterChecksum) +
                  " replica_checksum=" + std::to_string(replicaChecksum));
}

void LogGenerator::logReplicationStopped() {
  EventStats stats{getCurrentTimestampMs(),
                   static_cast<int>(EventTemplate::ReplicationStopped),
                   3,
                   currentLagSec,
                   0.0,
                   masterLsn,
                   replicaLsn};

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

  std::ostringstream msg;
  msg << "replication_stopped replica=" << config.replicaId << std::fixed
      << std::setprecision(2) << " lag=" << currentLagSec << "s";
  logger.Critical(msg.str());
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