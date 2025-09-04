// Cache Interface - Abstract caching operations for video feed
import {Logger} from '../utils/logger'

export interface CacheConfig {
  host: string
  port: number
  password?: string
  database?: number
  keyPrefix?: string
  defaultTTL?: number
  maxMemory?: string
  maxMemoryPolicy?: 'allkeys-lru' | 'volatile-lru' | 'allkeys-random' | 'volatile-random' | 'volatile-ttl'
}

export interface CacheOptions {
  ttl?: number
  tags?: string[]
  compress?: boolean
}

export abstract class Cache {
  protected logger: Logger
  protected config: CacheConfig
  protected isConnected: boolean = false
  protected defaultTTL: number

  constructor(logger: Logger, config: CacheConfig) {
    this.logger = logger
    this.config = config
    this.defaultTTL = config.defaultTTL || 300 // 5 minutes default
  }

  abstract connect(): Promise<void>
  abstract disconnect(): Promise<void>
  abstract get<T>(key: string): Promise<T | null>
  abstract set(key: string, value: any, options?: CacheOptions): Promise<void>
  abstract delete(key: string): Promise<void>
  abstract exists(key: string): Promise<boolean>
  abstract ping(): Promise<boolean>

  // Video Feed specific methods
  async getVideoFeed(
    feedType: string,
    algorithm: string,
    params: any
  ): Promise<any[] | null> {
    try {
      const cacheKey = this.generateVideoFeedKey(feedType, algorithm, params)
      const cached = await this.get<any[]>(cacheKey)
      
      if (cached) {
        this.logger.debug('Cache hit for video feed', {feedType, algorithm, cacheKey})
        return cached
      }
      
      this.logger.debug('Cache miss for video feed', {feedType, algorithm, cacheKey})
      return null

    } catch (error) {
      this.logger.error('Error getting video feed from cache', {error, feedType, algorithm})
      return null
    }
  }

  async setVideoFeed(
    feedType: string,
    algorithm: string,
    params: any,
    videos: any[],
    options?: CacheOptions
  ): Promise<void> {
    try {
      const cacheKey = this.generateVideoFeedKey(feedType, algorithm, params)
      const ttl = options?.ttl || this.defaultTTL
      
      await this.set(cacheKey, videos, {
        ttl,
        tags: [`feed:${feedType}`, `algorithm:${algorithm}`],
        ...options
      })
      
      this.logger.debug('Cached video feed', {feedType, algorithm, cacheKey, ttl})

    } catch (error) {
      this.logger.error('Error caching video feed', {error, feedType, algorithm})
    }
  }

  async getVideoById(videoId: string): Promise<any | null> {
    try {
      const cacheKey = `video:${videoId}`
      const cached = await this.get<any>(cacheKey)
      
      if (cached) {
        this.logger.debug('Cache hit for video', {videoId, cacheKey})
        return cached
      }
      
      this.logger.debug('Cache miss for video', {videoId, cacheKey})
      return null

    } catch (error) {
      this.logger.error('Error getting video from cache', {error, videoId})
      return null
    }
  }

  async setVideo(videoId: string, video: any, options?: CacheOptions): Promise<void> {
    try {
      const cacheKey = `video:${videoId}`
      const ttl = options?.ttl || this.defaultTTL * 2 // Videos cache longer
      
      await this.set(cacheKey, video, {
        ttl,
        tags: ['video', `video:${videoId}`],
        ...options
      })
      
      this.logger.debug('Cached video', {videoId, cacheKey, ttl})

    } catch (error) {
      this.logger.error('Error caching video', {error, videoId})
    }
  }

  async getUserEngagementProfile(userId: string): Promise<any | null> {
    try {
      const cacheKey = `user_profile:${userId}`
      const cached = await this.get<any>(cacheKey)
      
      if (cached) {
        this.logger.debug('Cache hit for user profile', {userId, cacheKey})
        return cached
      }
      
      this.logger.debug('Cache miss for user profile', {userId, cacheKey})
      return null

    } catch (error) {
      this.logger.error('Error getting user profile from cache', {error, userId})
      return null
    }
  }

  async setUserEngagementProfile(
    userId: string,
    profile: any,
    options?: CacheOptions
  ): Promise<void> {
    try {
      const cacheKey = `user_profile:${userId}`
      const ttl = options?.ttl || this.defaultTTL * 3 // User profiles cache even longer
      
      await this.set(cacheKey, profile, {
        ttl,
        tags: ['user_profile', `user:${userId}`],
        ...options
      })
      
      this.logger.debug('Cached user profile', {userId, cacheKey, ttl})

    } catch (error) {
      this.logger.error('Error caching user profile', {error, userId})
    }
  }

  async getVideoEngagementMetrics(videoId: string): Promise<any | null> {
    try {
      const cacheKey = `video_metrics:${videoId}`
      const cached = await this.get<any>(cacheKey)
      
      if (cached) {
        this.logger.debug('Cache hit for video metrics', {videoId, cacheKey})
        return cached
      }
      
      this.logger.debug('Cache miss for video metrics', {videoId, cacheKey})
      return null

    } catch (error) {
      this.logger.error('Error getting video metrics from cache', {error, videoId})
      return null
    }
  }

  async setVideoEngagementMetrics(
    videoId: string,
    metrics: any,
    options?: CacheOptions
  ): Promise<void> {
    try {
      const cacheKey = `video_metrics:${videoId}`
      const ttl = options?.ttl || this.defaultTTL
      
      await this.set(cacheKey, metrics, {
        ttl,
        tags: ['video_metrics', `video:${videoId}`],
        ...options
      })
      
      this.logger.debug('Cached video metrics', {videoId, cacheKey, ttl})

    } catch (error) {
      this.logger.error('Error caching video metrics', {error, videoId})
    }
  }

  async getTrendingVideos(timeWindow: string): Promise<any[] | null> {
    try {
      const cacheKey = `trending:${timeWindow}`
      const cached = await this.get<any[]>(cacheKey)
      
      if (cached) {
        this.logger.debug('Cache hit for trending videos', {timeWindow, cacheKey})
        return cached
      }
      
      this.logger.debug('Cache miss for trending videos', {timeWindow, cacheKey})
      return null

    } catch (error) {
      this.logger.error('Error getting trending videos from cache', {error, timeWindow})
      return null
    }
  }

  async setTrendingVideos(
    timeWindow: string,
    videos: any[],
    options?: CacheOptions
  ): Promise<void> {
    try {
      const cacheKey = `trending:${timeWindow}`
      const ttl = options?.ttl || this.defaultTTL / 2 // Trending data expires faster
      
      await this.set(cacheKey, videos, {
        ttl,
        tags: ['trending', `time_window:${timeWindow}`],
        ...options
      })
      
      this.logger.debug('Cached trending videos', {timeWindow, cacheKey, ttl})

    } catch (error) {
      this.logger.error('Error caching trending videos', {error, timeWindow})
    }
  }

  // Cache invalidation methods
  async invalidateVideoFeed(feedType: string, algorithm?: string): Promise<void> {
    try {
      if (algorithm) {
        // Invalidate specific algorithm
        const pattern = `feed:${feedType}:${algorithm}:*`
        await this.deletePattern(pattern)
        this.logger.info('Invalidated specific video feed cache', {feedType, algorithm})
      } else {
        // Invalidate all algorithms for feed type
        const pattern = `feed:${feedType}:*`
        await this.deletePattern(pattern)
        this.logger.info('Invalidated all video feed cache', {feedType})
      }
    } catch (error) {
      this.logger.error('Error invalidating video feed cache', {error, feedType, algorithm})
    }
  }

  async invalidateVideo(videoId: string): Promise<void> {
    try {
      const keys = [
        `video:${videoId}`,
        `video_metrics:${videoId}`,
        `video_features:${videoId}`
      ]
      
      for (const key of keys) {
        await this.delete(key)
      }
      
      this.logger.info('Invalidated video cache', {videoId, keys})

    } catch (error) {
      this.logger.error('Error invalidating video cache', {error, videoId})
    }
  }

  async invalidateUserProfile(userId: string): Promise<void> {
    try {
      const cacheKey = `user_profile:${userId}`
      await this.delete(cacheKey)
      
      this.logger.info('Invalidated user profile cache', {userId})

    } catch (error) {
      this.logger.error('Error invalidating user profile cache', {error, userId})
    }
  }

  async invalidateByTags(tags: string[]): Promise<void> {
    try {
      for (const tag of tags) {
        const pattern = `*:${tag}:*`
        await this.deletePattern(pattern)
      }
      
      this.logger.info('Invalidated cache by tags', {tags})

    } catch (error) {
      this.logger.error('Error invalidating cache by tags', {error, tags})
    }
  }

  // Utility methods
  private generateVideoFeedKey(feedType: string, algorithm: string, params: any): string {
    const paramHash = this.hashParams(params)
    return `feed:${feedType}:${algorithm}:${paramHash}`
  }

  private hashParams(params: any): string {
    // Simple hash function for cache keys
    const str = JSON.stringify(params)
    let hash = 0
    
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i)
      hash = ((hash << 5) - hash) + char
      hash = hash & hash // Convert to 32-bit integer
    }
    
    return Math.abs(hash).toString(36)
  }

  // Abstract methods that implementations must provide
  abstract deletePattern(pattern: string): Promise<void>
  abstract flush(): Promise<void>
  abstract getStats(): Promise<{
    totalKeys: number
    memoryUsage: string
    hitRate: number
    missRate: number
  }>

  // Health check
  async healthCheck(): Promise<{status: string, details: any}> {
    try {
      const isHealthy = await this.ping()
      
      return {
        status: isHealthy ? 'healthy' : 'unhealthy',
        details: {
          connected: this.isConnected,
          ping: isHealthy,
          timestamp: new Date().toISOString()
        }
      }

    } catch (error) {
      this.logger.error('Cache health check failed', {error})
      return {
        status: 'unhealthy',
        details: {
          error: error.message,
          timestamp: new Date().toISOString()
        }
      }
    }
  }

  // Cache warming methods
  async warmVideoFeedCache(
    feedType: string,
    algorithm: string,
    commonParams: any[]
  ): Promise<void> {
    try {
      this.logger.info('Warming video feed cache', {feedType, algorithm, paramCount: commonParams.length})
      
      // This would typically be called by a background job
      // to pre-populate cache with common feed requests
      
    } catch (error) {
      this.logger.error('Error warming video feed cache', {error, feedType, algorithm})
    }
  }

  async warmUserProfileCache(userIds: string[]): Promise<void> {
    try {
      this.logger.info('Warming user profile cache', {userCount: userIds.length})
      
      // This would typically be called by a background job
      // to pre-populate cache with active user profiles
      
    } catch (error) {
      this.logger.error('Error warming user profile cache', {error, userCount: userIds.length})
    }
  }

  // Cache analytics
  async getCacheAnalytics(): Promise<{
    totalKeys: number
    memoryUsage: string
    hitRate: number
    missRate: number
    topKeys: Array<{key: string, accessCount: number}>
    topTags: Array<{tag: string, keyCount: number}>
  }> {
    try {
      const stats = await this.getStats()
      
      // This would provide detailed cache analytics
      // Implementation depends on the specific cache backend
      
      return {
        ...stats,
        topKeys: [],
        topTags: []
      }

    } catch (error) {
      this.logger.error('Error getting cache analytics', {error})
      return {
        totalKeys: 0,
        memoryUsage: '0B',
        hitRate: 0,
        missRate: 0,
        topKeys: [],
        topTags: []
      }
    }
  }
}