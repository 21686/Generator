cmake_minimum_required(VERSION 3.15)
project(db_log_simulator)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# Fetch spdlog
FetchContent_Declare(
  spdlog
  GIT_REPOSITORY https://github.com/gabime/spdlog.git
  GIT_TAG        v1.12.0
)
FetchContent_MakeAvailable(spdlog)

# Fetch nlohmann_json (FIXED SYNTAX)
FetchContent_Declare(
  json
  GIT_REPOSITORY https://github.com/nlohmann/json.git
  GIT_TAG        v3.11.3
)
FetchContent_MakeAvailable(json)  

# Verify source files exist before configuring targets
function(verify_sources)
    foreach(file ${ARGN})
        if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/${file}")
            message(FATAL_ERROR "Missing required source file: ${file}\n"
                    "Expected at: ${CMAKE_CURRENT_SOURCE_DIR}/${file}\n"
                    "Please create this file or adjust your project structure.")
        endif()
    endforeach()
endfunction()

# Check required files exist
verify_sources(
    log_generator.cpp
    log_generator.hpp
    csv_analyzer.cpp
    csv_analyzer.hpp
    main.cpp
    analyzer_main.cpp
    ports/errors/errors.hpp
    ports/kvalog/kvalog.hpp
)

# Source files (using relative paths - cleaner than ${CMAKE_SOURCE_DIR})
set(SOURCES
    log_generator.cpp
    log_generator.hpp
    csv_analyzer.cpp
    csv_analyzer.hpp
    main.cpp
)

set(ANALYZER_SOURCES
    csv_analyzer.cpp
    csv_analyzer.hpp
    analyzer_main.cpp
)

add_executable(log_simulator ${SOURCES})
target_link_libraries(log_simulator PRIVATE spdlog::spdlog nlohmann_json::nlohmann_json)
target_include_directories(log_simulator PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/ports
)

add_executable(csv_analyzer ${ANALYZER_SOURCES})
target_link_libraries(csv_analyzer PRIVATE nlohmann_json::nlohmann_json)
target_include_directories(csv_analyzer PRIVATE 
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/ports
)