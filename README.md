# DB Log Simulator

This repository contains a C++ project that simulates database replication logs and
provides a CSV analyzer for generated log data. The project uses modern C++23,
CMake for build configuration and fetches third-party dependencies using
`FetchContent`.

## Components

- **log_generator**: Produces JSON and CSV log files containing simulated
events, including normal and anomalous conditions. It uses `kvalog` for log
output and allows configuration of time segments, anomaly frequency, and
output file names.
- **csv_analyzer**: Reads a CSV file produced by the generator and computes a
report with statistics such as template frequency, log level distribution,
and performance metrics. It also detects anomalies.
- **ports/**: Contains utility headers for errors and `kvalog` code used by both
components.
- `main.cpp` / `analyzer_main.cpp`: Example entry points for the generator and
analyzer respectively.

## Features

- Configurable generator with parameters like time segment length, number of
anomalies, total segments, replicas, and file paths.
- Detailed analysis report with averages, maxima, and anomaly detection logic.
- Clean separation of concerns and reusable helper classes.

## Dependencies

- [spdlog](https://github.com/gabime/spdlog) for logging (used by
`log_generator`).
- [nlohmann/json](https://github.com/nlohmann/json) for JSON serialization.

Dependencies are fetched automatically by CMake.

## Installation

### Prerequisites

- A Linux system with a C++ compiler supporting C++23 (e.g. `g++` or `clang`).
- CMake 3.15 or newer.
- Git (for initial clone).

### Clone and build

```sh
git clone https://your.repo.url/your-project.git
cd your-project
mkdir -p build && cd build
cmake ..
cmake --build .
```

The CMake configuration step will automatically download and build any
third-party dependencies (`spdlog`, `nlohmann/json`).

#### Optional: Install system-wide

You can install the executables and headers to system directories if desired:

```sh
sudo cmake --install .
```

## Development environment

For an improved development experience with Visual Studio Code, the following
extensions are recommended:

- **C/C++** (ms-vscode.cpptools) – IntelliSense and debugging support.
- **CMake Tools** (ms-vscode.cmake-tools) – configure, build and run CMake
  projects from the editor.
- **Clang-Format** (xaver.clang-format) – code formatting.
- **CMake** (twxs.cmake) – syntax highlighting for CMake files.
- **CodeLLDB** (vadimcn.vscode-lldb) – native debugging on Linux.

Configure the CMake Tools extension to use the same build directory
(`build/`) and your preferred compiler. A `.vscode/settings.json` can be
added to lock these settings.


## Building

```sh
mkdir -p build && cd build
cmake ..
cmake --build .
```

This produces two executables:

- `log_simulator`: run the log generator
- `csv_analyzer`: analyze a CSV file

## Usage

Generate logs:

```sh
./log_simulator
```

The program prints configuration details and creates `simulation_logs.json` and
`simulation_logs.csv` (names can be modified in `main.cpp` or via a custom
`LoggerConfig`).

Analyze CSV data:

```sh
./csv_analyzer simulation_logs.csv
```

The analyzer outputs a formatted report summarizing the content and
highlighting anomalies.

## Project Structure

```
├── CMakeLists.txt           # build configuration
├── main.cpp                 # example generator application
├── analyzer_main.cpp        # example analyzer application
├── log_generator.hpp/cpp    # log generator implementation
├── csv_analyzer.hpp/cpp     # CSV parsing and analysis
└── ports/                   # third-party and utility headers
    ├── errors/              # error handling helpers
    └── kvalog/              # simple logging facility
```

## Extending

- Add new event templates or adjust anomaly logic in `LogGenerator`.
- Improve CSV analysis (additional metrics or formats) in `CsvAnalyzer`.
- Integrate command-line argument parsing for runtime configuration.

## License

This project is provided as-is with no specific license. Feel free to reuse and
modify.
