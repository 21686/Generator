
#include <iostream>

#include "csv_analyzer.hpp"

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <csv_file>" << std::endl;
    return 1;
  }

  std::string csvFile = argv[1];

  std::cout << "Analyzing CSV file: " << csvFile << std::endl;
  std::cout << std::endl;

  auto [result, err] = db_simulator::CsvAnalyzer::Analyze(csvFile);
  if (err) {
    std::cerr << "Error: " << err->What() << std::endl;
    return 1;
  }

  db_simulator::CsvAnalyzer::PrintReport(result);

  return 0;
}