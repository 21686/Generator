// log_generator.hpp
#pragma once

#include <chrono>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <sstream>
#include <vector>

#include "ports/errors/errors.hpp"
#include "ports/kvalog/kvalog.hpp"

namespace db_simulator {

// Шаблоны событий с нумерацией
enum class EventTemplate : int {
  // INFO шаблоны
  ReplicationStatus = 1,           // replication_status
  DataIntegrityCheckStarted = 2,   // data_integrity_check_started
  TableChecksumCalculated = 3,     // table_checksum_calculated
  ReplicationCatchingUp = 4,       // replication_catching_up
  ReplicationRecovered = 5,        // replication_recovered
  NetworkConnectionRecovered = 6,  // network_connection_recovered

  // WARN шаблоны
  ReplicationLagIncreased = 7,  // replication_lag_increased

  // ERROR шаблоны
  NetworkPacketLossDetected = 8,  // network_packet_loss_detected
  ReplicationLagCritical = 9,     // replication_lag_critical

  // CRITICAL шаблоны
  DataDivergenceDetected = 10,  // data_divergence_detected
  ReplicationStopped = 11       // replication_stopped
};

struct LoggerConfig {
  double timeSegmentSeconds = 60.0;
  int anomaliesPerSegment = 2;
  int totalSegments = 10;
  std::string outputJsonFile = "logs.json";
  std::string outputCsvFile = "logs.csv";
  std::string replicaId = "replica-01";
  std::string masterHost = "db-master-01";
  int64_t processId = 12345;
};

struct EventStats {
  int64_t timestampMs;
  int templateId;
  int logLevel;
  double lagSec = 0.0;
  double lossPercent = 0.0;
  int64_t masterLsn = 0;
  int64_t replicaLsn = 0;
};

class LogGenerator {
 private:
  LoggerConfig config;
  kvalog::Logger logger;
  std::ofstream jsonFile;
  std::ofstream csvFile;
  std::mt19937 rng;
  std::uniform_real_distribution<> uniformDist;
  int64_t currentTimeMs;
  int64_t masterLsn;
  int64_t replicaLsn;
  double currentLagSec;
  int anomaliesGenerated;
  int currentSegment;
  std::vector<EventStats> csvBuffer;
  std::mutex fileMutex;

  static constexpr double normalLagMin = 1.0;
  static constexpr double normalLagMax = 3.0;
  static constexpr double lagIncreaseStep = 0.3;
  static constexpr int64_t lsnStepNormal = 1200;
  static constexpr int64_t lsnStepCatchingUp = 5000;
  static constexpr double packetLossMin = 5.0;
  static constexpr double packetLossMax = 15.0;
  static constexpr int anomalySequenceLength = 4;

  explicit LogGenerator(const LoggerConfig& cfg);

 public:
  static std::tuple<std::shared_ptr<LogGenerator>, error> Create(
      const LoggerConfig& cfg);

  error Generate();

  error Finalize();

 private:
  error initializeFiles();

  error generateSegment();

  error generateNormalLogs(int count);

  error generateAnomaly();

  void logReplicationStatus();
  void logReplicationLagIncreased(double previousLag);
  void logNetworkPacketLossDetected();
  void logReplicationLagCritical(double previousLag);
  void logNetworkConnectionRecovered();
  void logReplicationCatchingUp();
  void logReplicationRecovered(double recoveryTimeSec);
  void logDataIntegrityCheckStarted();
  void logTableChecksumCalculated(const std::string& table, int64_t rows);
  void logDataDivergenceDetected(const std::string& table);
  void logReplicationStopped();

  std::string getCurrentTimestamp() const;
  int64_t getCurrentTimestampMs() const;
  std::string formatLsn(int64_t lsn) const;
  std::string getConnectionQuality(double lossPercent) const;
  void advanceTime(double seconds);
  bool shouldGenerateAnomaly() const;

  void writeJsonLog(const nlohmann::json& logEntry, int templateId);
  void bufferCsvStats(const EventStats& stats);

  double randomDouble(double min, double max);
  int randomInt(int min, int max);
};

}  // namespace db_simulator