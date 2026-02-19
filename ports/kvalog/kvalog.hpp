#pragma once

#include <spdlog/async.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <source_location>
#include <sstream>
#include <string>
#include <thread>

// Add system headers for process ID
#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace kvalog
{

// Log levels matching spdlog
enum class LogLevel {
    Off,
    Trace,
    Debug,
    Info,
    Warning,
    Error,
    Critical
};

// Configuration for which fields to include in logs
struct LogFieldConfig {
    bool includeAppName = true;
    bool includeProcessId = true;
    bool includeThreadId = true;
    bool includeModuleName = true;
    bool includeLogLevel = true;
    bool includeFile = true;
    bool includeMessage = true;
    bool includeTime = true;
};

// Output format type
enum class OutputFormat {
    Json,
    Terminal
};

// Interface for network sink adapters
class NetworkSinkInterface
{
public:
    virtual ~NetworkSinkInterface() = default;
    virtual void SendLog(const std::string & jsonLog) = 0;
    virtual bool IsConnected() const = 0;
};

// Custom sink for network logging
class NetworkSink : public spdlog::sinks::base_sink<std::mutex>
{
public:
    explicit NetworkSink(std::shared_ptr<NetworkSinkInterface> initialAdapter)
        : adapter(std::move(initialAdapter))
    {
    }

    void SetAdapter(std::shared_ptr<NetworkSinkInterface> initialAdapter)
    {
        std::lock_guard<std::mutex> lock(mutex_);  // mutex_ is from spdlog::base_sink
        this->adapter = std::move(initialAdapter);
    }

protected:
    // overrides spdlog::base_sink::sink_it_()
    void sink_it_(const spdlog::details::log_msg & msg) override
    {
        if (this->adapter && this->adapter->IsConnected()) {
            spdlog::memory_buf_t formatted;
            this->formatter_->format(msg, formatted);  // formatter_ is from spdlog::base_sink
            this->adapter->SendLog(fmt::to_string(formatted));
        }
    }

    // overrides spdlog::base_sink::flush_()
    void flush_() override
    {
        // Network sinks typically don't need explicit flushing
    }

private:
    std::shared_ptr<NetworkSinkInterface> adapter;
};

// Main Logger class
class Logger
{
public:
    // Logging mode
    enum class Mode {
        Sync,
        Async
    };

    // Logger configuration
    struct Config {
        OutputFormat format = OutputFormat::Terminal;
        LogFieldConfig fields = LogFieldConfig();
        Mode asyncMode = Mode::Sync;

        // Output destinations
        bool logToConsole = true;
        std::optional<std::string> logFilePath = std::nullopt;
        std::shared_ptr<NetworkSinkInterface> networkAdapter = nullptr;

        // Async settings (only used if async_mode = true)
        size_t asyncQueueSize = 8192;
        size_t asyncThreadCount = 1;
    };

    // Context information for logs
    struct Context {
        std::string appName = "";
        std::string moduleName = "";
    };

    // Constructor with configuration
    explicit Logger(const Config & initialConfig)
        : config(initialConfig), context(Context()), processId(Logger::getProcessId())
    {
        this->initializeLogger();
    }

    // Constructor with configuration and context
    explicit Logger(const Config & initialConfig, const Context & initialContext)
        : config(initialConfig), context(initialContext), processId(Logger::getProcessId())
    {
        this->initializeLogger();
    }

    // Copy constructor to create logger with copied config and context
    Logger(const Logger & other)
        : config(other.config), context(other.context), processId(Logger::getProcessId())
    {
        this->initializeLogger();
    }

    // Create new logger with config from existing one
    static Logger WithConfigFrom(const Logger & source, const Context & newContext)
    {
        Config newConfig = source.config;
        return Logger(newConfig, newContext);
    }

    // Update field configuration at runtime
    void SetFieldConfig(const LogFieldConfig & fields)
    {
        this->config.fields = fields;
    }

    // Get current field configuration
    LogFieldConfig GetFieldConfig() const
    {
        return this->config.fields;
    }

    // Update output format at runtime
    void SetOutputFormat(OutputFormat format)
    {
        this->config.format = format;
    }

    // Logging methods with automatic source location

    void Trace(const std::string & message,
               const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Trace, message, location);
    }

    void Debug(const std::string & message,
               const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Debug, message, location);
    }

    void Info(const std::string & message,
              const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Info, message, location);
    }

    void Warning(const std::string & message,
                 const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Warning, message, location);
    }

    void Error(const std::string & message,
               const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Error, message, location);
    }

    void Critical(const std::string & message,
                  const std::source_location & location = std::source_location::current())
    {
        this->log(LogLevel::Critical, message, location);
    }

    // Set minimum log level
    void SetLevel(LogLevel level)
    {
        if (this->logger) {
            this->level = level;
            this->logger->set_level(this->toSpdlogLevel(level));
        }
    }

    // Flush logs
    void Flush()
    {
        if (this->logger) {
            this->logger->flush();
        }
    }

private:
    LogLevel level;
    std::shared_ptr<spdlog::logger> logger;
    Config config;
    Context context;
    int processId;

    void initializeLogger()
    {
        std::vector<spdlog::sink_ptr> sinks;

        // Console sink
        if (this->config.logToConsole) {
            auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
            consoleSink->set_pattern("%v");  // We'll format ourselves
            sinks.push_back(consoleSink);
        }

        // File sink
        if (this->config.logFilePath) {
            auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(
                *this->config.logFilePath, true);
            fileSink->set_pattern("%v");
            sinks.push_back(fileSink);
        }

        // Network sink
        if (this->config.networkAdapter) {
            auto networkSink = std::make_shared<NetworkSink>(config.networkAdapter);
            networkSink->set_pattern("%v");
            sinks.push_back(networkSink);
        }

        // Create logger (sync or async)
        if (this->config.asyncMode == Mode::Async) {
            spdlog::init_thread_pool(config.asyncQueueSize, config.asyncThreadCount);
            this->logger = std::make_shared<spdlog::async_logger>(
                "async_logger", sinks.begin(), sinks.end(), spdlog::thread_pool(),
                spdlog::async_overflow_policy::block);
        } else {
            this->logger =
                std::make_shared<spdlog::logger>("sync_logger", sinks.begin(), sinks.end());
        }

        this->level = LogLevel::Trace;
        this->logger->set_level(spdlog::level::trace);
    }

    void log(LogLevel level, const std::string & message, const std::source_location & location)
    {
        if (static_cast<int>(this->level) > static_cast<int>(level)) {
            return;
        }

        if (!this->logger) {
            return;
        }

        std::string formatted_msg;

        if (config.format == OutputFormat::Json) {
            formatted_msg = this->formatJson(level, message, location);
        } else {
            formatted_msg = this->formatTerminal(level, message, location);
        }

        this->logger->log(this->toSpdlogLevel(level), formatted_msg);
    }

    std::string formatJson(LogLevel level, const std::string & message,
                           const std::source_location & location)
    {
        nlohmann::json log_entry;

        if (this->config.fields.includeTime) {
            log_entry["time"] = this->getCurrentTimeString();
        }
        if (this->config.fields.includeAppName) {
            if (!this->context.appName.empty()) {
                log_entry["app"] = this->context.appName;
            }
        }
        if (this->config.fields.includeProcessId) {
            log_entry["process_id"] = std::to_string(this->processId);
        }
        if (this->config.fields.includeThreadId) {
            log_entry["thread_id"] = this->getThreadId();
        }
        if (this->config.fields.includeModuleName) {
            if (!this->context.moduleName.empty()) {
                log_entry["module"] = this->context.moduleName;
            }
        }
        if (this->config.fields.includeLogLevel) {
            log_entry["level"] = this->levelToString(level);
        }
        if (this->config.fields.includeFile) {
            log_entry["file"] = this->formatFileLine(location);
        }
        if (this->config.fields.includeMessage) {
            log_entry["message"] = message;
        }

        return log_entry.dump();
    }

    std::string formatTerminal(LogLevel level, const std::string & message,
                               const std::source_location & location)
    {
        std::ostringstream oss;

        auto section = [](const std::string & content) { return "[" + content + "]"; };

        if (this->config.fields.includeTime) {
            oss << section(this->getCurrentTimeString());
        }
        if (this->config.fields.includeAppName) {
            if (!this->context.appName.empty()) {
                oss << section(this->context.appName);
            }
        }
        if (this->config.fields.includeModuleName) {
            if (!this->context.moduleName.empty()) {
                oss << section(this->context.moduleName);
            }
        }
        if (this->config.fields.includeProcessId) {
            oss << section("PID:" + std::to_string(this->processId));
        }
        if (this->config.fields.includeThreadId) {
            oss << section("TID:" + this->getThreadId());
        }
        if (this->config.fields.includeLogLevel) {
            oss << section(this->levelToString(level));
        }
        if (this->config.fields.includeFile) {
            oss << section(this->formatFileLine(location));
        }
        if (this->config.fields.includeMessage) {
            oss << " " << message;
        }

        return oss.str();
    }

    static std::string formatFileLine(const std::source_location & location)
    {
        std::string file = location.file_name();
        size_t pos1 = file.find_last_of("/\\");
        if (pos1 != std::string::npos) {
            file = file.substr(pos1 + 1);
        }
        std::ostringstream file_line;
        file_line << file;
        file_line << ":" << location.line();
        return file_line.str();
    }

    static std::string levelToString(LogLevel level)
    {
        switch (level) {
            case LogLevel::Trace:
                return "TRC";
            case LogLevel::Debug:
                return "DBG";
            case LogLevel::Info:
                return "INF";
            case LogLevel::Warning:
                return "WRN";
            case LogLevel::Error:
                return "ERR";
            case LogLevel::Critical:
                return "CRT";
            default:
                return "INF";
        }
    }

    static spdlog::level::level_enum toSpdlogLevel(LogLevel level)
    {
        switch (level) {
            case LogLevel::Off:
                return spdlog::level::off;
            case LogLevel::Trace:
                return spdlog::level::trace;
            case LogLevel::Debug:
                return spdlog::level::debug;
            case LogLevel::Info:
                return spdlog::level::info;
            case LogLevel::Warning:
                return spdlog::level::warn;
            case LogLevel::Error:
                return spdlog::level::err;
            case LogLevel::Critical:
                return spdlog::level::critical;
            default:
                return spdlog::level::info;
        }
    }

    static int getProcessId()
    {
#ifdef _WIN32
        return static_cast<int>(GetCurrentProcessId());
#else
        return static_cast<int>(getpid());
#endif
    }

    static std::string getThreadId()
    {
        std::ostringstream oss;
        oss << std::this_thread::get_id();
        return oss.str();
    }

    static std::string getCurrentTimeString()
    {
        using namespace std::chrono;
        auto now = system_clock::now();
        auto ms = duration_cast<milliseconds>(now.time_since_epoch()) % 1000;
        auto in_time_t = system_clock::to_time_t(now);

        std::ostringstream oss;
        oss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
        oss << '.' << std::setfill('0') << std::setw(3) << ms.count();
        return oss.str();
    }
};

inline Logger::Config MakeDefaultLoggerConfig()
{
    Logger::Config config;
    config.format = OutputFormat::Terminal;
    config.fields = LogFieldConfig{ 
        .includeAppName = true,
        .includeProcessId = false,
        .includeThreadId = false,
        .includeModuleName = true,
        .includeLogLevel = true,
        .includeFile = true,
        .includeMessage = true,
        .includeTime = false 
    };
    config.asyncMode = Logger::Mode::Sync;
    config.logToConsole = true;
    config.logFilePath = std::nullopt;
    config.networkAdapter = nullptr;
    config.asyncQueueSize = 8192;
    config.asyncThreadCount = 1;
    return config;
}

inline Logger CreateLogger(const std::string& appName, const std::string& moduleName)
{
    auto defaultConfig = MakeDefaultLoggerConfig();
    
    Logger::Context context;
    context.appName = appName;
    context.moduleName = moduleName;
    
    return Logger(defaultConfig, context);
}

}  // namespace kvalog