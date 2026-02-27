
#include "csv_analyzer.hpp"

#include <iomanip>
#include <iostream>

#include "ports/errors/errors.hpp"

namespace db_simulator {

std::tuple<CsvAnalyzer::AnalysisResult, error> CsvAnalyzer::Analyze(
    const std::string& csvFile) {
  auto [rows, err] = parseCsv(csvFile);
  if (err) {
    return {AnalysisResult(), err};
  }

  if (rows.empty()) {
    return {AnalysisResult(), errors::New("CSV file is empty")};
  }

  AnalysisResult result;
  result.totalEvents = static_cast<int>(rows.size());

  double totalLag = 0.0;
  double totalLoss = 0.0;
  int lagCount = 0;
  int lossCount = 0;

  for (const auto& row : rows) {
    result.templateFrequency[row.templateId]++;

    result.levelFrequency[row.logLevel]++;

    if (row.lagSec > 0.0) {
      totalLag += row.lagSec;
      lagCount++;
      result.maxLagSec = std::max(result.maxLagSec, row.lagSec);
    }

    if (row.lossPercent > 0.0) {
      totalLoss += row.lossPercent;
      lossCount++;
      result.maxLossPercent = std::max(result.maxLossPercent, row.lossPercent);
    }
  }

  if (lagCount > 0) {
    result.avgLagSec = totalLag / lagCount;
  }
  if (lossCount > 0) {
    result.avgLossPercent = totalLoss / lossCount;
  }

  result.anomaliesDetected = detectAnomalies(rows) ? 1 : 0;

  return {result, nullptr};
}

void CsvAnalyzer::PrintReport(const AnalysisResult& result, std::ostream& out) {
  out << "========================================" << std::endl;
  out << "         CSV ANALYSIS REPORT" << std::endl;
  out << "========================================" << std::endl;
  out << std::endl;

  out << "Total Events: " << result.totalEvents << std::endl;
  out << "Anomalies Detected: " << result.anomaliesDetected << std::endl;
  out << std::endl;

  out << "Template Frequency:" << std::endl;
  out << "-------------------" << std::endl;
  for (const auto& [templateId, count] : result.templateFrequency) {
    std::string templateName;
    switch (templateId) {
      case 1:
        templateName = "ReplicationStatus";
        break;
      case 2:
        templateName = "DataIntegrityCheckStarted";
        break;
      case 3:
        templateName = "TableChecksumCalculated";
        break;
      case 4:
        templateName = "ReplicationCatchingUp";
        break;
      case 5:
        templateName = "ReplicationRecovered";
        break;
      case 6:
        templateName = "NetworkConnectionRecovered";
        break;
      case 7:
        templateName = "ReplicationLagIncreased";
        break;
      case 8:
        templateName = "NetworkPacketLossDetected";
        break;
      case 9:
        templateName = "ReplicationLagCritical";
        break;
      case 10:
        templateName = "DataDivergenceDetected";
        break;
      case 11:
        templateName = "ReplicationStopped";
        break;
      default:
        templateName = "Unknown";
    }
    out << "  Template " << std::setw(2) << templateId << " (" << std::setw(30)
        << templateName << "): " << count << std::endl;
  }

  out << std::endl;
  out << "Log Level Distribution:" << std::endl;
  out << "-----------------------" << std::endl;
  const char* levelNames[] = {"INFO", "WARN", "ERROR", "CRITICAL"};
  for (const auto& [level, count] : result.levelFrequency) {
    if (level >= 0 && level <= 3) {
      out << "  " << levelNames[level] << ": " << count << std::endl;
    }
  }

  out << std::endl;
  out << "Performance Metrics:" << std::endl;
  out << "-------------------" << std::endl;
  out << std::fixed << std::setprecision(2);
  out << "  Average Lag: " << result.avgLagSec << " sec" << std::endl;
  out << "  Maximum Lag: " << result.maxLagSec << " sec" << std::endl;
  out << "  Average Packet Loss: " << result.avgLossPercent << "%" << std::endl;
  out << "  Maximum Packet Loss: " << result.maxLossPercent << "%" << std::endl;

  out << "========================================" << std::endl;
}

std::tuple<std::vector<CsvAnalyzer::CsvRow>, error> CsvAnalyzer::parseCsv(
    const std::string& csvFile) {
  std::ifstream file(csvFile);
  if (!file.is_open()) {
    return {std::vector<CsvRow>(),
            errors::New("failed to open CSV file: " + csvFile)};
  }

  std::vector<CsvRow> rows;
  std::string line;

  std::getline(file, line);

  while (std::getline(file, line)) {
    if (line.empty()) continue;

    CsvRow row;
    std::stringstream ss(line);
    std::string token;

    // timestamp_ms
    std::getline(ss, token, ',');
    row.timestampMs = std::stoll(token);

    // template_id
    std::getline(ss, token, ',');
    row.templateId = std::stoi(token);

    // log_level
    std::getline(ss, token, ',');
    row.logLevel = std::stoi(token);

    // lag_sec
    std::getline(ss, token, ',');
    row.lagSec = std::stod(token);

    // loss_percent
    std::getline(ss, token, ',');
    row.lossPercent = std::stod(token);

    // master_lsn
    std::getline(ss, token, ',');
    row.masterLsn = std::stoll(token);

    // replica_lsn
    std::getline(ss, token, ',');
    row.replicaLsn = std::stoll(token);

    rows.push_back(row);
  }

  return {rows, nullptr};
}

bool CsvAnalyzer::detectAnomalies(const std::vector<CsvRow>& rows) {
  int anomalyCount = 0;
  for (size_t i = 0; i < rows.size(); ++i) {
    const auto& row = rows[i];

    bool highLag = row.lagSec > 100.0;
    bool packetLoss = row.lossPercent > 5.0;
    bool errorOrCritical = row.logLevel >= 2;

    if ((highLag && packetLoss) || (highLag && errorOrCritical)) {
      anomalyCount++;

      if (i + 3 < rows.size()) {
        bool sequenceFound = true;
        for (size_t j = i; j < i + 4 && j < rows.size(); ++j) {
          if (rows[j].logLevel < 1) {
            sequenceFound = false;
            break;
          }
        }
        if (sequenceFound) {
          return true;
        }
      }
    }
  }

  return anomalyCount > 0;
}

}  // namespace db_simulator