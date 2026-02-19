#pragma once

#ifndef ERRORS_HPP
#define ERRORS_HPP

#include <cstdio>
#include <format>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// Forward declarations
namespace errors
{
class Error;
}
using error = std::shared_ptr<errors::Error>;

namespace errors
{

// --- Core Error Interface ---

/**
 * @class Error
 * @brief The base interface for error types.
 */
class Error
{
public:
    virtual ~Error() = default;
    [[nodiscard]] virtual std::string What() const = 0;

    // Provides access to the next error in a wrapped chain.
    [[nodiscard]] virtual error Unwrap() const
    {
        return nullptr;
    }

    // Provides access to a list of errors for joined errors.
    [[nodiscard]] virtual std::vector<error> GetJoined() const
    {
        return {};
    }
};

// --- Concrete Error Types ---

/**
 * @class StringError
 * @brief A simple error type that holds a text message. Used by New() and
 * Errorf().
 */
class StringError : public Error
{
private:
    std::string message;

public:
    explicit StringError(std::string msg) : message(std::move(msg)) {}
    [[nodiscard]] std::string What() const override
    {
        return this->message;
    }
};

/**
 * @class WrappedError
 * @brief An error type that wraps another error, adding context. Used by Wrap()
 * and Wrapf().
 */
class WrappedError : public Error
{
private:
    std::string message;
    error err;

public:
    WrappedError(std::string msg, error e) : message(std::move(msg)), err(std::move(e)) {}
    [[nodiscard]] std::string What() const override
    {
        return this->message + ": " + err->What();
    }
    [[nodiscard]] error Unwrap() const override
    {
        return err;
    }
};

/**
 * @class JoinedError
 * @brief An error type that combines multiple errors into one. Used by Join().
 */
class JoinedError : public Error
{
private:
    std::vector<error> errs;

public:
    explicit JoinedError(std::vector<error> es) : errs(std::move(es)) {}
    [[nodiscard]] std::string What() const override
    {
        std::stringstream ss;
        for (size_t i = 0; i < errs.size(); ++i) {
            ss << errs[i]->What();
            if (i < errs.size() - 1) {
                ss << "; ";
            }
        }
        return ss.str();
    }
    [[nodiscard]] std::vector<error> GetJoined() const override
    {
        return errs;
    }
};

// --- Public API Functions ---

/**
 * @brief Creates a new error with the given text message.
 */
inline error New(const std::string & message)
{
    return std::make_shared<StringError>(message);
}

/**
 * @brief Creates a new error with a formatted message.
 */
template <typename... Args>
error Errorf(const std::format_string<Args...> & fmt, Args &&... args)
{
    try {
        auto msg = std::format(fmt, std::forward<Args>(args)...);
        return errors::New(msg);
    } catch (const std::format_error & e) {
        return errors::New(std::string("errors::Errorf(): invalid format: ") + e.what());
    }
}

/**
 * @brief Wraps an existing error with a static message.
 *
 * @note If the provided error is nullptr, a new error is created with the
 * given message.
 */
inline error Wrap(error err, const std::string & msg)
{
    if (err == nullptr) {
        return errors::New(msg);
    }
    return std::make_shared<WrappedError>(msg, err);
}

/**
 * @brief Wraps an existing error with a formatted message.
 *
 * @note If the provided error is nullptr, a new error is created with the
 * formatted message.
 */
template <typename... Args>
error Wrapf(error err, const std::format_string<Args...> & fmt, Args &&... args)
{
    if (err == nullptr) {
        return errors::Errorf(fmt, std::forward<Args>(args)...);
    }

    try {
        std::string msg = std::format(fmt, std::forward<Args>(args)...);
        return errors::Wrap(err, msg);
    } catch (const std::format_error & e) {
        return errors::Wrap(err, std::string("errors::Wrapf(): invalid format: ") + e.what());
    }
}

/**
 * @brief Reports whether any error in err's chain matches target.
 */
inline bool Is(error err, error target)
{
    if (target == nullptr) {
        return err == nullptr;
    }

    std::vector<error> stack;
    if (err) {
        stack.push_back(err);
    }

    while (!stack.empty()) {
        error current = stack.back();
        stack.pop_back();

        if (current == target) {
            return true;
        }

        // Add joined errors to the stack for traversal
        auto joined = current->GetJoined();
        if (!joined.empty()) {
            stack.insert(stack.end(), joined.rbegin(), joined.rend());
        }

        // Add unwrapped error to the stack
        if (error next = current->Unwrap()) {
            stack.push_back(next);
        }
    }
    return false;
}

/**
 * @brief Finds the first error in err's chain that matches the type T, and if
 * so, sets target to that error value and returns true.
 */
template <typename T>
bool As(error err, std::shared_ptr<T> * target)
{
    if (err == nullptr || target == nullptr) {
        return false;
    }

    std::vector<error> stack;
    if (err) {
        stack.push_back(err);
    }

    while (!stack.empty()) {
        error current = stack.back();
        stack.pop_back();

        auto specific_err = std::dynamic_pointer_cast<T>(current);
        if (specific_err) {
            *target = specific_err;
            return true;
        }

        // Add joined errors to the stack for traversal
        auto joined = current->GetJoined();
        if (!joined.empty()) {
            stack.insert(stack.end(), joined.rbegin(), joined.rend());
        }

        // Add unwrapped error to the stack
        if (error next = current->Unwrap()) {
            stack.push_back(next);
        }
    }

    return false;
}

/**
 * @brief Combines multiple errors into a single error. Nullptr errors are
 * filtered out.
 */
template <typename... Args>
error Join(Args... args)
{
    std::vector<error> errs;
    // Use a fold expression (C++17) to push all non-nullptr errors into the
    // vector
    (
        [&](const error & e) {
            if (e) {
                errs.push_back(e);
            }
        }(args),
        ...);

    if (errs.empty()) {
        return nullptr;
    }
    if (errs.size() == 1) {
        return errs[0];
    }
    return std::make_shared<JoinedError>(errs);
}

}  // namespace errors

#endif  // ERRORS_HPP
