// Database Interface - Abstract database operations for video feed
import {Logger} from '../utils/logger'

export interface DatabaseConfig {
  host: string
  port: number
  database: string
  username: string
  password: string
  ssl?: boolean
  connectionPool?: {
    min: number
    max: number
  }
}

export interface QueryOptions {
  timeout?: number
  transaction?: boolean
  isolation?: 'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE'
}

export interface QueryResult<T = any> {
  rows: T[]
  rowCount: number
  fields?: any[]
}

export interface DatabaseTransaction {
  commit(): Promise<void>
  rollback(): Promise<void>
  query<T>(sql: string, params?: any[]): Promise<QueryResult<T>>
}

export abstract class Database {
  protected logger: Logger
  protected config: DatabaseConfig
  protected isConnected: boolean = false

  constructor(logger: Logger, config: DatabaseConfig) {
    this.logger = logger
    this.config = config
  }

  abstract connect(): Promise<void>
  abstract disconnect(): Promise<void>
  abstract query<T>(sql: string, params?: any[], options?: QueryOptions): Promise<QueryResult<T>>
  abstract beginTransaction(): Promise<DatabaseTransaction>
  abstract ping(): Promise<boolean>

  // Video Feed specific methods
  async getVideos(params: {
    limit: number
    offset: number
    categories?: string[]
    excludeCategories?: string[]
    minDuration?: number
    maxDuration?: number
    qualityPreference?: string
    cursor?: string
  }): Promise<any[]> {
    try {
      const {limit, offset, categories, excludeCategories, minDuration, maxDuration, qualityPreference, cursor} = params
      
      let sql = `
        SELECT 
          v.id, v.title, v.description, v.category, v.tags, v.language,
          v.duration, v.quality, v.thumbnail_url, v.playback_url,
          v.resolution, v.aspect_ratio, v.file_size, v.encoding,
          v.bitrate, v.frame_rate, v.created_at,
          v.brightness, v.contrast, v.saturation, v.motion_intensity,
          v.scene_complexity, v.audio_quality, v.background_music, v.speech_clarity,
          c.id as creator_id, c.username as creator_username, c.display_name as creator_display_name,
          c.avatar_url as creator_avatar_url,
          vm.view_count, vm.like_count, vm.renote_count, vm.reply_count,
          vm.share_count, vm.bookmark_count, vm.completion_rate, vm.average_watch_time
        FROM videos v
        LEFT JOIN creators c ON v.creator_id = c.id
        LEFT JOIN video_metrics vm ON v.id = vm.video_id
        WHERE 1=1
      `
      
      const queryParams: any[] = []
      let paramIndex = 1

      if (categories && categories.length > 0) {
        sql += ` AND v.category = ANY($${paramIndex++})`
        queryParams.push(categories)
      }

      if (excludeCategories && excludeCategories.length > 0) {
        sql += ` AND v.category != ALL($${paramIndex++})`
        queryParams.push(excludeCategories)
      }

      if (minDuration !== undefined) {
        sql += ` AND v.duration >= $${paramIndex++}`
        queryParams.push(minDuration)
      }

      if (maxDuration !== undefined) {
        sql += ` AND v.duration <= $${paramIndex++}`
        queryParams.push(maxDuration)
      }

      if (qualityPreference) {
        sql += ` AND v.quality = $${paramIndex++}`
        queryParams.push(qualityPreference)
      }

      if (cursor) {
        sql += ` AND v.id > $${paramIndex++}`
        queryParams.push(cursor)
      }

      sql += ` ORDER BY v.created_at DESC LIMIT $${paramIndex++} OFFSET $${paramIndex++}`
      queryParams.push(limit, offset)

      const result = await this.query(sql, queryParams)
      return result.rows

    } catch (error) {
      this.logger.error('Error fetching videos from database', {error, params})
      throw error
    }
  }

  async getVideoById(videoId: string): Promise<any | null> {
    try {
      const sql = `
        SELECT 
          v.*, c.username as creator_username, c.display_name as creator_display_name,
          vm.view_count, vm.like_count, vm.renote_count, vm.reply_count,
          vm.share_count, vm.bookmark_count, vm.completion_rate, vm.average_watch_time
        FROM videos v
        LEFT JOIN creators c ON v.creator_id = c.id
        LEFT JOIN video_metrics vm ON v.id = vm.video_id
        WHERE v.id = $1
      `
      
      const result = await this.query(sql, [videoId])
      return result.rows[0] || null

    } catch (error) {
      this.logger.error('Error fetching video by ID', {error, videoId})
      throw error
    }
  }

  async getVideosByCreator(creatorId: string, limit: number = 20): Promise<any[]> {
    try {
      const sql = `
        SELECT 
          v.*, vm.view_count, vm.like_count, vm.renote_count, vm.reply_count,
          vm.share_count, vm.bookmark_count, vm.completion_rate, vm.average_watch_time
        FROM videos v
        LEFT JOIN video_metrics vm ON v.id = vm.video_id
        WHERE v.creator_id = $1
        ORDER BY v.created_at DESC
        LIMIT $2
      `
      
      const result = await this.query(sql, [creatorId, limit])
      return result.rows

    } catch (error) {
      this.logger.error('Error fetching videos by creator', {error, creatorId, limit})
      throw error
    }
  }

  async getTrendingVideos(limit: number = 20, timeWindow: string = '24h'): Promise<any[]> {
    try {
      const timeMap: Record<string, number> = {
        '1h': 60 * 60 * 1000,
        '6h': 6 * 60 * 60 * 1000,
        '24h': 24 * 60 * 60 * 1000,
        '7d': 7 * 24 * 60 * 60 * 1000,
        '30d': 30 * 24 * 60 * 60 * 1000
      }
      
      const timeMs = timeMap[timeWindow] || timeMap['24h']
      const cutoffTime = new Date(Date.now() - timeMs)

      const sql = `
        SELECT 
          v.*, c.username as creator_username, c.display_name as creator_display_name,
          vm.view_count, vm.like_count, vm.renote_count, vm.reply_count,
          vm.share_count, vm.bookmark_count, vm.completion_rate, vm.average_watch_time,
          (vm.view_count * 0.1 + vm.like_count * 0.3 + vm.renote_count * 0.5 + 
           vm.reply_count * 0.4 + vm.share_count * 0.6) as trending_score
        FROM videos v
        LEFT JOIN creators c ON v.creator_id = c.id
        LEFT JOIN video_metrics vm ON v.id = vm.video_id
        WHERE v.created_at >= $1
        ORDER BY trending_score DESC
        LIMIT $2
      `
      
      const result = await this.query(sql, [cutoffTime, limit])
      return result.rows

    } catch (error) {
      this.logger.error('Error fetching trending videos', {error, limit, timeWindow})
      throw error
    }
  }

  async updateVideoMetrics(videoId: string, metrics: {
    viewCount?: number
    likeCount?: number
    renoteCount?: number
    replyCount?: number
    shareCount?: number
    bookmarkCount?: number
    completionRate?: number
    averageWatchTime?: number
  }): Promise<void> {
    try {
      const sql = `
        INSERT INTO video_metrics (video_id, view_count, like_count, renote_count, reply_count, share_count, bookmark_count, completion_rate, average_watch_time, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (video_id) DO UPDATE SET
          view_count = video_metrics.view_count + EXCLUDED.view_count,
          like_count = video_metrics.like_count + EXCLUDED.like_count,
          renote_count = video_metrics.renote_count + EXCLUDED.renote_count,
          reply_count = video_metrics.reply_count + EXCLUDED.reply_count,
          share_count = video_metrics.share_count + EXCLUDED.share_count,
          bookmark_count = video_metrics.bookmark_count + EXCLUDED.bookmark_count,
          completion_rate = (video_metrics.completion_rate + EXCLUDED.completion_rate) / 2,
          average_watch_time = (video_metrics.average_watch_time + EXCLUDED.average_watch_time) / 2,
          updated_at = NOW()
      `
      
      const params = [
        videoId,
        metrics.viewCount || 0,
        metrics.likeCount || 0,
        metrics.renoteCount || 0,
        metrics.replyCount || 0,
        metrics.shareCount || 0,
        metrics.bookmarkCount || 0,
        metrics.completionRate || 0,
        metrics.averageWatchTime || 0
      ]

      await this.query(sql, params)
      this.logger.info('Updated video metrics', {videoId, metrics})

    } catch (error) {
      this.logger.error('Error updating video metrics', {error, videoId, metrics})
      throw error
    }
  }

  async searchVideos(query: string, limit: number = 20): Promise<any[]> {
    try {
      const sql = `
        SELECT 
          v.*, c.username as creator_username, c.display_name as creator_display_name,
          vm.view_count, vm.like_count, vm.renote_count, vm.reply_count,
          vm.share_count, vm.bookmark_count, vm.completion_rate, vm.average_watch_time,
          ts_rank(to_tsvector('english', v.title || ' ' || v.description), plainto_tsquery('english', $1)) as relevance
        FROM videos v
        LEFT JOIN creators c ON v.creator_id = c.id
        LEFT JOIN video_metrics vm ON v.id = vm.video_id
        WHERE to_tsvector('english', v.title || ' ' || v.description) @@ plainto_tsquery('english', $1)
        ORDER BY relevance DESC, v.created_at DESC
        LIMIT $2
      `
      
      const result = await this.query(sql, [query, limit])
      return result.rows

    } catch (error) {
      this.logger.error('Error searching videos', {error, query, limit})
      throw error
    }
  }

  // User engagement methods
  async getUserEngagementProfile(userId: string): Promise<any | null> {
    try {
      const sql = `
        SELECT 
          up.*, u.username, u.display_name, u.avatar_url, u.created_at as user_created_at
        FROM user_profiles up
        LEFT JOIN users u ON up.user_id = u.id
        WHERE up.user_id = $1
      `
      
      const result = await this.query(sql, [userId])
      return result.rows[0] || null

    } catch (error) {
      this.logger.error('Error fetching user engagement profile', {error, userId})
      throw error
    }
  }

  async updateUserEngagementProfile(userId: string, profile: any): Promise<void> {
    try {
      const sql = `
        INSERT INTO user_profiles (user_id, interests, content_preferences, engagement_patterns, demographics, last_active, total_engagement, updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
          interests = EXCLUDED.interests,
          content_preferences = EXCLUDED.content_preferences,
          engagement_patterns = EXCLUDED.engagement_patterns,
          demographics = EXCLUDED.demographics,
          last_active = NOW(),
          total_engagement = EXCLUDED.total_engagement,
          updated_at = NOW()
      `
      
      const params = [
        userId,
        JSON.stringify(profile.interests),
        JSON.stringify(profile.contentPreferences),
        JSON.stringify(profile.engagementPatterns),
        JSON.stringify(profile.demographics),
        profile.totalEngagement || 0
      ]

      await this.query(sql, params)
      this.logger.info('Updated user engagement profile', {userId})

    } catch (error) {
      this.logger.error('Error updating user engagement profile', {error, userId})
      throw error
    }
  }

  async storeEngagementEvent(event: {
    userId: string
    videoId: string
    eventType: string
    timestamp: Date
    duration?: number
    completionRate?: number
    context?: any
  }): Promise<void> {
    try {
      const sql = `
        INSERT INTO engagement_events (user_id, video_id, event_type, timestamp, duration, completion_rate, context, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
      `
      
      const params = [
        event.userId,
        event.videoId,
        event.eventType,
        event.timestamp,
        event.duration || null,
        event.completionRate || null,
        event.context ? JSON.stringify(event.context) : null
      ]

      await this.query(sql, params)
      this.logger.debug('Stored engagement event', {event})

    } catch (error) {
      this.logger.error('Error storing engagement event', {error, event})
      throw error
    }
  }

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
      this.logger.error('Database health check failed', {error})
      return {
        status: 'unhealthy',
        details: {
          error: error.message,
          timestamp: new Date().toISOString()
        }
      }
    }
  }
}