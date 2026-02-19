
#pragma once

#include <algorithm>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

namespace db_simulator {

class CsvAnalyzer {
 public:
  struct AnalysisResult {
    int totalEvents = 0;
    int anomaliesDetected = 0;
    std::map<int, int> templateFrequency;  // template_id -> count
    std::map<int, int> levelFrequency;     // log_level -> count
    double avgLagSec = 0.0;
    double maxLagSec = 0.0;
    double avgLossPercent = 0.0;
    double maxLossPercent = 0.0;
  };

  static std::tuple<AnalysisResult, error> Analyze(const std::string& csvFile);

  static void PrintReport(const AnalysisResult& result,
                          std::ostream& out = std::cout);

 private:
  struct CsvRow {
    int64_t timestampMs;
    int templateId;
    int logLevel;
    double lagSec;
    double lossPercent;
    int64_t masterLsn;
    int64_t replicaLsn;
  };

  static std::tuple<std::vector<CsvRow>, error> parseCsv(
      const std::string& csvFile);
  static bool detectAnomalies(const std::vector<CsvRow>& rows);
};

}  // namespace db_simulator