// Logger Utility - Simple logging for the video feed services
export enum LogLevel {
  DEBUG = 0,
  INFO = 1,
  WARN = 2,
  ERROR = 3
}

export interface LogContext {
  [key: string]: any
}

export class Logger {
  private level: LogLevel
  private prefix: string

  constructor(level: LogLevel = LogLevel.INFO, prefix: string = 'Sonet') {
    this.level = level
    this.prefix = prefix
  }

  setLevel(level: LogLevel): void {
    this.level = level
  }

  private formatMessage(level: string, message: string, context?: LogContext): string {
    const timestamp = new Date().toISOString()
    const contextStr = context ? ` ${JSON.stringify(context)}` : ''
    return `[${timestamp}] [${this.prefix}] [${level}] ${message}${contextStr}`
  }

  private shouldLog(level: LogLevel): boolean {
    return level >= this.level
  }

  debug(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.DEBUG)) {
      console.debug(this.formatMessage('DEBUG', message, context))
    }
  }

  info(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.INFO)) {
      console.info(this.formatMessage('INFO', message, context))
    }
  }

  warn(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.WARN)) {
      console.warn(this.formatMessage('WARN', message, context))
    }
  }

  error(message: string, context?: LogContext): void {
    if (this.shouldLog(LogLevel.ERROR)) {
      console.error(this.formatMessage('ERROR', message, context))
    }
  }

  // Convenience methods for common logging patterns
  logEngagementEvent(eventType: string, userId: string, videoId: string, context?: LogContext): void {
    this.info(`Engagement event: ${eventType}`, {
      eventType,
      userId,
      videoId,
      ...context
    })
  }

  logVideoFeedRequest(feedType: string, algorithm: string, userId?: string, context?: LogContext): void {
    this.info(`Video feed request`, {
      feedType,
      algorithm,
      userId,
      ...context
    })
  }

  logMLPrediction(videoId: string, algorithm: string, score: number, context?: LogContext): void {
    this.debug(`ML prediction generated`, {
      videoId,
      algorithm,
      score,
      ...context
    })
  }

  logContentFiltering(videoId: string, result: string, confidence: number, context?: LogContext): void {
    this.info(`Content filtering result`, {
      videoId,
      result,
      confidence,
      ...context
    })
  }

  logRealTimeConnection(connectionId: string, action: string, context?: LogContext): void {
    this.info(`Real-time connection ${action}`, {
      connectionId,
      action,
      ...context
    })
  }

  logDatabaseOperation(operation: string, table: string, context?: LogContext): void {
    this.debug(`Database operation: ${operation}`, {
      operation,
      table,
      ...context
    })
  }

  logCacheOperation(operation: string, key: string, context?: LogContext): void {
    this.debug(`Cache operation: ${operation}`, {
      operation,
      key,
      ...context
    })
  }

  // Performance logging
  logPerformance(operation: string, duration: number, context?: LogContext): void {
    this.info(`Performance: ${operation} took ${duration}ms`, {
      operation,
      duration,
      ...context
    })
  }

  // Error logging with stack traces
  logErrorWithStack(message: string, error: Error, context?: LogContext): void {
    this.error(message, {
      error: {
        name: error.name,
        message: error.message,
        stack: error.stack
      },
      ...context
    })
  }

  // Batch operation logging
  logBatchOperation(operation: string, count: number, context?: LogContext): void {
    this.info(`Batch operation: ${operation} processed ${count} items`, {
      operation,
      count,
      ...context
    })
  }

  // User activity logging
  logUserActivity(userId: string, activity: string, context?: LogContext): void {
    this.info(`User activity: ${activity}`, {
      userId,
      activity,
      ...context
    })
  }

  // Feed generation logging
  logFeedGeneration(feedType: string, itemCount: number, algorithm: string, context?: LogContext): void {
    this.info(`Feed generated: ${feedType} with ${itemCount} items`, {
      feedType,
      itemCount,
      algorithm,
      ...context
    })
  }
}

// Create default logger instance
export const defaultLogger = new Logger(LogLevel.INFO, 'Sonet')

// Create service-specific loggers
export const createServiceLogger = (serviceName: string, level?: LogLevel): Logger => {
  return new Logger(level || LogLevel.INFO, `Sonet:${serviceName}`)
}