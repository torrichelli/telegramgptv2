"""
Database module for Telegram bot reporting system.
Provides CRUD operations for SQLite database with user events tracking.
"""

import sqlite3
import logging
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timezone
import os
from contextlib import contextmanager

# Setup logging
logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    SQLite database manager for Telegram bot reporting system.
    Handles all database operations with proper error handling and transactions.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections with automatic cleanup.
        
        Yields:
            sqlite3.Connection: Database connection
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
            conn.execute("PRAGMA foreign_keys=ON")  # Enable foreign key constraints
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def init_database(self) -> None:
        """
        Initialize database by running migrations.
        Creates all necessary tables and indexes.
        """
        migrations_path = os.path.join(os.path.dirname(__file__), 'migrations.sql')
        
        try:
            with open(migrations_path, 'r', encoding='utf-8') as f:
                migrations_sql = f.read()
            
            with self.get_connection() as conn:
                conn.executescript(migrations_sql)
                
                # Add username column to inviters if it doesn't exist
                try:
                    conn.execute("ALTER TABLE inviters ADD COLUMN username TEXT")
                    logger.info("Added username column to inviters table")
                except Exception:
                    # Column already exists
                    pass
                
                conn.commit()
                logger.info("Database initialized successfully")
                
        except FileNotFoundError:
            logger.error(f"Migrations file not found: {migrations_path}")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def insert_user_if_not_exists(self, tg_user_id: int, username: Optional[str] = None, 
                                   name: Optional[str] = None) -> int:
        """
        Insert user if not exists, return user ID. Uses atomic UPSERT operation.
        
        Args:
            tg_user_id: Telegram user ID
            username: Telegram username (optional)
            name: User display name (optional)
            
        Returns:
            int: User ID in database
        """
        with self.get_connection() as conn:
            # Atomic UPSERT operation
            cursor = conn.execute(
                """INSERT INTO users (tg_user_id, username, name) VALUES (?, ?, ?)
                   ON CONFLICT(tg_user_id) DO UPDATE SET 
                   username = COALESCE(excluded.username, username),
                   name = COALESCE(excluded.name, name)""",
                (tg_user_id, username, name)
            )
            
            # Get the user ID (either newly inserted or existing)
            cursor = conn.execute(
                "SELECT id FROM users WHERE tg_user_id = ?",
                (tg_user_id,)
            )
            row = cursor.fetchone()
            conn.commit()
            
            if row is None:
                raise RuntimeError(f"Failed to insert/get user with tg_user_id={tg_user_id}")
            
            logger.info(f"User upserted: tg_user_id={tg_user_id}, username={username}")
            return row[0]
    
    def get_inviter_by_link(self, invite_link: str) -> Optional[int]:
        """
        Get inviter ID by invite link.
        
        Args:
            invite_link: Telegram invite link
            
        Returns:
            Optional[int]: Inviter ID if found, None otherwise
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM inviters WHERE invite_link = ?",
                (invite_link,)
            )
            row = cursor.fetchone()
            return row[0] if row else None

    def upsert_inviter(self, name: str, username: Optional[str] = None, 
                       invite_link: Optional[str] = None, channel_id: Optional[str] = None) -> int:
        """
        Insert or update inviter with username support.
        
        Args:
            name: Inviter name
            username: Inviter username (with or without @)
            invite_link: Invite link (optional)
            channel_id: Channel ID (optional)
            
        Returns:
            int: Inviter ID
        """
        # Clean username (remove @ if present, add if missing for storage)
        if username and not username.startswith('@'):
            username = f'@{username}'
        
        with self.get_connection() as conn:
            # Try to find existing inviter by username or name
            cursor = conn.execute(
                "SELECT id FROM inviters WHERE username = ? OR name = ?",
                (username, name)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Update existing
                conn.execute(
                    """UPDATE inviters SET name = ?, username = ?, invite_link = ?, channel_id = ?
                       WHERE id = ?""",
                    (name, username, invite_link, channel_id, existing[0])
                )
                conn.commit()
                return existing[0]
            else:
                # Insert new
                cursor = conn.execute(
                    "INSERT INTO inviters (name, username, invite_link, channel_id) VALUES (?, ?, ?, ?)",
                    (name, username, invite_link, channel_id)
                )
                conn.commit()
                return cursor.lastrowid
    
    def insert_journal_event(self, event_type: str, tg_user_id: int, 
                           username: Optional[str] = None, name: Optional[str] = None,
                           inviter_id: Optional[int] = None, status: str = 'subscribed',
                           note: Optional[str] = None, telegram_update_id: Optional[int] = None) -> int:
        """
        Insert journal event with proper validation and duplicate detection.
        
        Args:
            event_type: Type of event ('subscribe', 'unsubscribe', 'manual_add', etc.)
            tg_user_id: Telegram user ID
            username: Telegram username (optional)
            name: User display name (optional)
            inviter_id: ID of inviter (optional)
            status: User status ('subscribed', 'left', etc.)
            note: Additional notes (e.g., 'repeat' for duplicate invites)
            telegram_update_id: Telegram update ID for idempotency (optional)
            
        Returns:
            int: Journal entry ID
        """
        # Use UTC timezone for consistency
        event_time = datetime.now(timezone.utc).isoformat()
        
        with self.get_connection() as conn:
            # Check if this is a repeat invitation
            if event_type == 'subscribe' and inviter_id:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM journal WHERE tg_user_id = ? AND event_type = 'subscribe' AND inviter_id IS NOT NULL AND inviter_id != ?",
                    (tg_user_id, inviter_id)
                )
                previous_invites = cursor.fetchone()[0]
                if previous_invites > 0:
                    note = 'repeat' if note is None else f"{note},repeat"
            
            # Insert journal event with idempotency via telegram_update_id
            if telegram_update_id:
                # For better idempotency, use INSERT OR IGNORE then SELECT
                cursor = conn.execute(
                    """INSERT OR IGNORE INTO journal (event_time, event_type, tg_user_id, username, name, inviter_id, status, note, telegram_update_id) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (event_time, event_type, tg_user_id, username, name, inviter_id, status, note, telegram_update_id)
                )
                
                if cursor.rowcount == 0:
                    # Duplicate - find existing record
                    cursor = conn.execute(
                        "SELECT id FROM journal WHERE telegram_update_id = ?",
                        (telegram_update_id,)
                    )
                    existing = cursor.fetchone()
                    if existing:
                        logger.info(f"Duplicate event skipped: update_id={telegram_update_id}")
                        conn.commit()
                        return existing[0]
                    else:
                        raise RuntimeError(f"Insert ignored but no existing record found for update_id={telegram_update_id}")
                
                journal_id = cursor.lastrowid
                if journal_id is None:
                    raise RuntimeError(f"Failed to insert journal event for tg_user_id={tg_user_id}")
                
                conn.commit()
                logger.info(f"Journal event inserted: {event_type} for tg_user_id={tg_user_id}, journal_id={journal_id}")
                return journal_id
            else:
                # No telegram_update_id - regular insert
                cursor = conn.execute(
                    """INSERT INTO journal (event_time, event_type, tg_user_id, username, name, inviter_id, status, note, telegram_update_id) 
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (event_time, event_type, tg_user_id, username, name, inviter_id, status, note, None)
                )
                conn.commit()
                
                journal_id = cursor.lastrowid
                if journal_id is None:
                    raise RuntimeError(f"Failed to insert journal event for tg_user_id={tg_user_id}")
                
                logger.info(f"Journal event inserted: {event_type} for tg_user_id={tg_user_id}, journal_id={journal_id}")
                return journal_id
    
    def get_subscriptions_for_retention_check(self, retention_days: int, check_date: str) -> List[Dict[str, Any]]:
        """
        Get subscriptions that need retention check. Only selects subscriptions 
        that happened exactly N days ago and haven't been checked yet.
        
        Args:
            retention_days: Number of days to check retention after subscription
            check_date: Date of the check (ISO format)
            
        Returns:
            List of journal entries that need retention check
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT j.* FROM journal j 
                   WHERE j.event_type = 'subscribe' 
                   AND date(j.event_time) = date(?, ? || ' days')
                   AND NOT EXISTS (
                       SELECT 1 FROM retention_checks rc 
                       WHERE rc.journal_id = j.id
                   )""",
                (check_date, f'-{retention_days}')
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def check_user_retention(self, journal_id: int, tg_user_id: int, 
                           subscription_time: str) -> str:
        """
        Check if user is retained (no unsubscribe event after subscription).
        
        Args:
            journal_id: Journal entry ID of the subscription
            tg_user_id: Telegram user ID
            subscription_time: Time of subscription (ISO format)
            
        Returns:
            str: 'retained' or 'not_retained'
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT COUNT(*) FROM journal 
                   WHERE tg_user_id = ? AND event_type = 'unsubscribe' 
                   AND event_time > ?""",
                (tg_user_id, subscription_time)
            )
            unsubscribe_count = cursor.fetchone()[0]
            
            return 'not_retained' if unsubscribe_count > 0 else 'retained'
    
    def insert_retention_check(self, journal_id: int, check_date: str, result: str) -> None:
        """
        Insert retention check result to avoid duplicate processing.
        Uses UNIQUE constraint for idempotency.
        
        Args:
            journal_id: Journal entry ID
            check_date: Date of the check (ISO format)
            result: Result of the check ('retained', 'not_retained', 'pending')
        """
        with self.get_connection() as conn:
            try:
                conn.execute(
                    "INSERT INTO retention_checks (journal_id, check_date, result) VALUES (?, ?, ?)",
                    (journal_id, check_date, result)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                # Duplicate (journal_id, check_date) - ignore silently
                logger.debug(f"Retention check already exists: journal_id={journal_id}, check_date={check_date}")
                pass
    
    def get_journal_for_excel(self) -> List[Dict[str, Any]]:
        """
        Get all journal entries for Excel export.
        
        Returns:
            List of journal entries with inviter names and usernames
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT j.id, j.event_time, j.event_type, j.tg_user_id, 
                          j.username, j.name, 
                          COALESCE(i.username, i.name, 'Не указан') as inviter_name, 
                          j.status, j.note
                   FROM journal j
                   LEFT JOIN inviters i ON j.inviter_id = i.id
                   ORDER BY j.event_time DESC"""
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_statistics_data(self) -> List[Dict[str, Any]]:
        """
        Get aggregated statistics for all inviters.
        
        Returns:
            List of statistics per inviter
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                    COALESCE(i.username, i.name, 'Не указан') as inviter_name,
                    COUNT(CASE WHEN j.event_type = 'subscribe' THEN 1 END) as total_invited,
                    COUNT(CASE WHEN j.event_type = 'subscribe' AND j.tg_user_id NOT IN (
                        SELECT j2.tg_user_id FROM journal j2 
                        WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j.event_time
                    ) THEN 1 END) as currently_subscribed,
                    COUNT(CASE WHEN j.event_type = 'subscribe' AND j.tg_user_id IN (
                        SELECT j2.tg_user_id FROM journal j2 
                        WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j.event_time
                    ) THEN 1 END) as unsubscribed,
                    ROUND(
                        CAST(COUNT(CASE WHEN j.event_type = 'subscribe' AND j.tg_user_id NOT IN (
                            SELECT j2.tg_user_id FROM journal j2 
                            WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j.event_time
                        ) THEN 1 END) AS FLOAT) * 100.0 / 
                        NULLIF(COUNT(CASE WHEN j.event_type = 'subscribe' THEN 1 END), 0), 2
                    ) as retention_percentage
                FROM inviters i
                LEFT JOIN journal j ON i.id = j.inviter_id
                GROUP BY i.id, i.name, i.username
                HAVING total_invited > 0
                ORDER BY total_invited DESC"""
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_daily_stats(self, report_date: str) -> Dict[str, Any]:
        """
        Get daily statistics for a specific date.
        
        Args:
            report_date: Date in YYYY-MM-DD format
            
        Returns:
            Dict with daily statistics
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                   COUNT(CASE WHEN event_type = 'subscribe' THEN 1 END) as total_subscriptions,
                   COUNT(CASE WHEN event_type = 'unsubscribe' THEN 1 END) as total_unsubscriptions,
                   COUNT(DISTINCT CASE WHEN event_type = 'subscribe' THEN tg_user_id END) as unique_subscribers,
                   COUNT(CASE WHEN event_type = 'subscribe' AND note LIKE '%repeat%' THEN 1 END) as repeat_subscribers
                   FROM journal 
                   WHERE date(event_time) = ?""",
                (report_date,)
            )
            row = cursor.fetchone()
            
            stats = dict(row) if row else {}
            stats['net_growth'] = stats.get('total_subscriptions', 0) - stats.get('total_unsubscriptions', 0)
            return stats
    
    def get_weekly_stats(self, week_start: str) -> Dict[str, Any]:
        """
        Get weekly statistics starting from given date.
        
        Args:
            week_start: Start date of week in YYYY-MM-DD format
            
        Returns:
            Dict with weekly statistics
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                   COUNT(CASE WHEN event_type = 'subscribe' THEN 1 END) as total_subscriptions,
                   COUNT(CASE WHEN event_type = 'unsubscribe' THEN 1 END) as total_unsubscriptions,
                   COUNT(DISTINCT CASE WHEN event_type = 'subscribe' THEN tg_user_id END) as unique_subscribers,
                   COUNT(CASE WHEN event_type = 'subscribe' AND note LIKE '%repeat%' THEN 1 END) as repeat_subscribers
                   FROM journal 
                   WHERE date(event_time) >= date(?) 
                   AND date(event_time) < date(?, '+7 days')""",
                (week_start, week_start)
            )
            row = cursor.fetchone()
            
            stats = dict(row) if row else {}
            stats['net_growth'] = stats.get('total_subscriptions', 0) - stats.get('total_unsubscriptions', 0)
            return stats
    
    def get_monthly_stats(self, month_start: str) -> Dict[str, Any]:
        """
        Get monthly statistics starting from given date.
        
        Args:
            month_start: Start date of month in YYYY-MM-DD format
            
        Returns:
            Dict with monthly statistics
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                   COUNT(CASE WHEN event_type = 'subscribe' THEN 1 END) as total_subscriptions,
                   COUNT(CASE WHEN event_type = 'unsubscribe' THEN 1 END) as total_unsubscriptions,
                   COUNT(DISTINCT CASE WHEN event_type = 'subscribe' THEN tg_user_id END) as unique_subscribers,
                   COUNT(CASE WHEN event_type = 'subscribe' AND note LIKE '%repeat%' THEN 1 END) as repeat_subscribers
                   FROM journal 
                   WHERE date(event_time) >= date(?) 
                   AND date(event_time) < date(?, '+1 month')""",
                (month_start, month_start)
            )
            row = cursor.fetchone()
            
            stats = dict(row) if row else {}
            stats['net_growth'] = stats.get('total_subscriptions', 0) - stats.get('total_unsubscriptions', 0)
            return stats
    
    def get_events_for_period(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Get all events for a specific period.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            List of events
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT * FROM journal 
                   WHERE date(event_time) >= ? AND date(event_time) <= ?
                   ORDER BY event_time DESC""",
                (start_date, end_date)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_retention_stats(self, retention_days: int, check_date: str) -> Dict[str, Any]:
        """
        Get retention statistics for a specific period.
        
        Args:
            retention_days: Number of days for retention check
            check_date: Date to check from
            
        Returns:
            Dict with retention statistics
        """
        # Get subscriptions that happened N days ago
        subscriptions_to_check = self.get_subscriptions_for_retention_check(retention_days, check_date)
        
        if not subscriptions_to_check:
            return {
                'total_subscriptions': 0,
                'retained': 0,
                'not_retained': 0,
                'retention_rate': 0.0
            }
        
        retained_count = 0
        not_retained_count = 0
        
        for subscription in subscriptions_to_check:
            retention_result = self.check_user_retention(
                subscription['id'], 
                subscription['tg_user_id'], 
                subscription['event_time']
            )
            
            if retention_result == 'retained':
                retained_count += 1
            elif retention_result == 'not_retained':
                not_retained_count += 1
        
        total_subscriptions = len(subscriptions_to_check)
        retention_rate = retained_count / total_subscriptions if total_subscriptions > 0 else 0.0
        
        return {
            'total_subscriptions': total_subscriptions,
            'retained': retained_count,
            'not_retained': not_retained_count,
            'retention_rate': retention_rate
        }
    
    def get_retention_checks_for_excel(self) -> List[Dict[str, Any]]:
        """
        Get all retention checks for Excel export.
        
        Returns:
            List of retention check records
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT rc.*, j.tg_user_id, j.username, j.event_time as subscription_time
                   FROM retention_checks rc
                   JOIN journal j ON rc.journal_id = j.id
                   ORDER BY rc.check_date DESC, j.event_time DESC"""
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_user_stats_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary statistics for all users.
        
        Returns:
            List of user statistics
        """
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                   j.tg_user_id,
                   j.username,
                   j.name,
                   COUNT(CASE WHEN j.event_type = 'subscribe' THEN 1 END) as total_subscriptions,
                   COUNT(CASE WHEN j.event_type = 'unsubscribe' THEN 1 END) as total_unsubscriptions,
                   MIN(CASE WHEN j.event_type = 'subscribe' THEN j.event_time END) as first_subscription,
                   MAX(j.event_time) as last_activity,
                   CASE WHEN EXISTS (
                       SELECT 1 FROM journal j2 
                       WHERE j2.tg_user_id = j.tg_user_id 
                       AND j2.event_type = 'subscribe'
                       AND j2.tg_user_id NOT IN (
                           SELECT j3.tg_user_id FROM journal j3 
                           WHERE j3.event_type = 'unsubscribe' AND j3.event_time > j2.event_time
                       )
                   ) THEN 'active' ELSE 'inactive' END as status
                   FROM journal j
                   GROUP BY j.tg_user_id, j.username, j.name
                   ORDER BY last_activity DESC"""
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_daily_report_data(self, report_date: str) -> Dict[str, Any]:
        """
        Generate daily report data for specific date.
        
        Args:
            report_date: Date for the report (ISO format)
            
        Returns:
            Dictionary with report data
        """
        with self.get_connection() as conn:
            # New subscriptions today
            cursor = conn.execute(
                "SELECT COUNT(*) FROM journal WHERE event_type = 'subscribe' AND date(event_time) = ?",
                (report_date,)
            )
            new_subscriptions = cursor.fetchone()[0]
            
            # Unsubscriptions today
            cursor = conn.execute(
                "SELECT COUNT(*) FROM journal WHERE event_type = 'unsubscribe' AND date(event_time) = ?",
                (report_date,)
            )
            unsubscriptions = cursor.fetchone()[0]
            
            # Total active subscribers
            cursor = conn.execute(
                """SELECT COUNT(DISTINCT j1.tg_user_id) FROM journal j1
                   WHERE j1.event_type = 'subscribe' 
                   AND j1.tg_user_id NOT IN (
                       SELECT j2.tg_user_id FROM journal j2 
                       WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j1.event_time
                   )"""
            )
            total_active = cursor.fetchone()[0]
            
            # Top inviters today
            cursor = conn.execute(
                """SELECT i.name, COUNT(*) as new_invites
                   FROM journal j
                   JOIN inviters i ON j.inviter_id = i.id
                   WHERE j.event_type = 'subscribe' AND date(j.event_time) = ?
                   GROUP BY i.id, i.name
                   ORDER BY new_invites DESC
                   LIMIT 5""",
                (report_date,)
            )
            top_inviters = [dict(row) for row in cursor.fetchall()]
            
            return {
                'date': report_date,
                'new_subscriptions': new_subscriptions,
                'unsubscriptions': unsubscriptions,
                'total_active': total_active,
                'top_inviters': top_inviters
            }
    
    def get_all_inviters(self) -> List[Dict[str, Any]]:
        """Get all inviters from database."""
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT id, name, invite_link FROM inviters")
            return [dict(row) for row in cursor.fetchall()]
    
    def count_users_by_inviter(self, inviter_name: str) -> int:
        """Count total users invited by specific inviter."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT COUNT(*) FROM journal j
                   JOIN inviters i ON j.inviter_id = i.id
                   WHERE i.name = ? AND j.event_type = 'subscribe'""",
                (inviter_name,)
            )
            return cursor.fetchone()[0]
    
    def count_active_users_by_inviter(self, inviter_name: str) -> int:
        """Count currently active users invited by specific inviter."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT COUNT(DISTINCT j1.tg_user_id) FROM journal j1
                   JOIN inviters i ON j1.inviter_id = i.id
                   WHERE i.name = ? AND j1.event_type = 'subscribe'
                   AND j1.tg_user_id NOT IN (
                       SELECT j2.tg_user_id FROM journal j2 
                       WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j1.event_time
                   )""",
                (inviter_name,)
            )
            return cursor.fetchone()[0]
    
    def get_top_inviters_for_date(self, target_date: str, limit: int = 3) -> List[Dict[str, Any]]:
        """Get top inviters for specific date with retention data."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                """SELECT 
                    i.name as inviter_name,
                    COUNT(*) as invited_count,
                    COUNT(CASE WHEN j1.tg_user_id NOT IN (
                        SELECT j2.tg_user_id FROM journal j2 
                        WHERE j2.event_type = 'unsubscribe' AND j2.event_time > j1.event_time
                    ) THEN 1 END) as retained_count
                   FROM journal j1
                   JOIN inviters i ON j1.inviter_id = i.id
                   WHERE j1.event_type = 'subscribe' AND date(j1.event_time) = ?
                   GROUP BY i.id, i.name
                   ORDER BY invited_count DESC
                   LIMIT ?""",
                (target_date, limit)
            )
            return [dict(row) for row in cursor.fetchall()]
    
    def get_retention_for_date(self, target_date: str, retention_days: int) -> Dict[str, Any]:
        """Get retention statistics for users who subscribed on specific date."""
        with self.get_connection() as conn:
            # Get users who subscribed on target date
            cursor = conn.execute(
                """SELECT COUNT(*) FROM journal 
                   WHERE event_type = 'subscribe' AND date(event_time) = ?""",
                (target_date,)
            )
            total_subscriptions = cursor.fetchone()[0]
            
            if total_subscriptions == 0:
                return {'total_subscriptions': 0, 'retained': 0, 'retention_rate': 0}
            
            # Get users who subscribed on target date and are still active after retention_days
            from datetime import datetime, timedelta
            check_date = datetime.strptime(target_date, "%Y-%m-%d") + timedelta(days=retention_days)
            check_date_str = check_date.strftime("%Y-%m-%d")
            
            cursor = conn.execute(
                """SELECT COUNT(DISTINCT j1.tg_user_id) FROM journal j1
                   WHERE j1.event_type = 'subscribe' AND date(j1.event_time) = ?
                   AND j1.tg_user_id NOT IN (
                       SELECT j2.tg_user_id FROM journal j2 
                       WHERE j2.event_type = 'unsubscribe' 
                       AND j2.event_time > j1.event_time
                       AND date(j2.event_time) <= ?
                   )""",
                (target_date, check_date_str)
            )
            retained = cursor.fetchone()[0]
            
            retention_rate = (retained / total_subscriptions) * 100 if total_subscriptions > 0 else 0
            
            return {
                'total_subscriptions': total_subscriptions,
                'retained': retained,
                'retention_rate': retention_rate
            }


# Global database instance
db_manager: Optional[DatabaseManager] = None


def init_db(db_path: str) -> None:
    """
    Initialize global database manager.
    
    Args:
        db_path: Path to SQLite database file
    """
    global db_manager
    db_manager = DatabaseManager(db_path)


def get_db() -> DatabaseManager:
    """
    Get global database manager instance.
    
    Returns:
        DatabaseManager: Database manager instance
        
    Raises:
        RuntimeError: If database not initialized
    """
    if db_manager is None:
        raise RuntimeError("Database not initialized. Call init_db() first.")
    return db_manager


async def init_database() -> None:
    """
    Initialize database for startup scripts.
    Async wrapper for init_db function.
    """
    db_path = os.getenv('DB_PATH', 'data/bot.sqlite3')
    
    # Создаём директорию для БД если путь содержит папки
    db_dir = os.path.dirname(db_path)
    if db_dir:  # Только если путь не пустой
        os.makedirs(db_dir, exist_ok=True)
    
    # Инициализируем базу данных
    init_db(db_path)
    logger.info(f"Database initialized: {db_path}")