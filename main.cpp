
#include <iostream>
#include <thread>

#include "log_generator.hpp"

int main() {
  db_simulator::LoggerConfig config;
  config.timeSegmentSeconds = 600.0;
  config.anomaliesPerSegment = 2;
  config.totalSegments = 5;
  config.outputJsonFile = "simulation_logs.json";
  config.outputCsvFile = "simulation_logs.csv";
  config.replicaId = "replica-01";
  config.masterHost = "db-master-01";
  config.processId = 12345;

  auto [generator, err] = db_simulator::LogGenerator::Create(config);
  if (err) {
    std::cerr << "Error creating log generator: " << err->What() << std::endl;
    return 1;
  }

  std::cout << "Log generator created successfully" << std::endl;
  std::cout << "Configuration:" << std::endl;
  std::cout << "  Time segment: " << config.timeSegmentSeconds << " seconds"
            << std::endl;
  std::cout << "  Anomalies per segment: " << config.anomaliesPerSegment
            << std::endl;
  std::cout << "  Total segments: " << config.totalSegments << std::endl;
  std::cout << "  Output JSON: " << config.outputJsonFile << std::endl;
  std::cout << "  Output CSV: " << config.outputCsvFile << std::endl;

  std::cout << "\nStarting log generation..." << std::endl;
  err = generator->Generate();
  if (err) {
    std::cerr << "Error during generation: " << err->What() << std::endl;
    return 1;
  }

  err = generator->Finalize();
  if (err) {
    std::cerr << "Error during finalization: " << err->What() << std::endl;
    return 1;
  }

  std::cout << "\nLog generation completed successfully!" << std::endl;
  std::cout << "Files created:" << std::endl;
  std::cout << "  - " << config.outputJsonFile << std::endl;
  std::cout << "  - " << config.outputCsvFile << std::endl;

  return 0;
}