#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Time utilities module for Telegram bot reporting system.
Provides timezone-aware date/time operations and formatting functions.
"""

from datetime import datetime, date, timezone, timedelta
from typing import Optional, Union
import pytz

# Almaty timezone
ALMATY_TZ = pytz.timezone('Asia/Almaty')
UTC_TZ = pytz.timezone('UTC')


def get_almaty_now() -> datetime:
    """
    Get current datetime in Almaty timezone.
    
    Returns:
        datetime: Current datetime in Almaty timezone
    """
    return datetime.now(ALMATY_TZ)


def get_utc_now() -> datetime:
    """
    Get current datetime in UTC timezone.
    
    Returns:
        datetime: Current datetime in UTC timezone
    """
    return datetime.now(UTC_TZ)


def almaty_to_utc(almaty_dt: datetime) -> datetime:
    """
    Convert Almaty datetime to UTC.
    
    Args:
        almaty_dt: Datetime in Almaty timezone
        
    Returns:
        datetime: Datetime in UTC timezone
    """
    if almaty_dt.tzinfo is None:
        almaty_dt = ALMATY_TZ.localize(almaty_dt)
    return almaty_dt.astimezone(UTC_TZ)


def utc_to_almaty(utc_dt: datetime) -> datetime:
    """
    Convert UTC datetime to Almaty timezone.
    
    Args:
        utc_dt: Datetime in UTC timezone
        
    Returns:
        datetime: Datetime in Almaty timezone
    """
    if utc_dt.tzinfo is None:
        utc_dt = UTC_TZ.localize(utc_dt)
    return utc_dt.astimezone(ALMATY_TZ)


def get_today_date_str(tz: Optional[timezone] = None) -> str:
    """
    Get today's date as ISO string.
    
    Args:
        tz: Timezone to use (default: Moscow)
        
    Returns:
        str: Today's date in YYYY-MM-DD format
    """
    if tz is None:
        return get_almaty_now().date().isoformat()
    else:
        return datetime.now(tz).date().isoformat()


def get_date_n_days_ago(days: int, tz: Optional[timezone] = None) -> str:
    """
    Get date N days ago as ISO string.
    
    Args:
        days: Number of days ago
        tz: Timezone to use (default: Moscow)
        
    Returns:
        str: Date in YYYY-MM-DD format
    """
    if tz is None:
        base_date = get_almaty_now().date()
    else:
        base_date = datetime.now(tz).date()
    
    target_date = base_date - timedelta(days=days)
    return target_date.isoformat()


def format_datetime_for_report(dt: datetime, include_time: bool = True) -> str:
    """
    Format datetime for reports in Russian locale.
    
    Args:
        dt: Datetime to format
        include_time: Whether to include time component
        
    Returns:
        str: Formatted datetime string
    """
    # Convert to Almaty timezone if needed
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    
    almaty_dt = dt.astimezone(ALMATY_TZ)
    
    months_ru = [
        'января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
        'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'
    ]
    
    day = almaty_dt.day
    month = months_ru[almaty_dt.month - 1]
    year = almaty_dt.year
    
    if include_time:
        hour = almaty_dt.hour
        minute = almaty_dt.minute
        return f"{day} {month} {year} г. в {hour:02d}:{minute:02d}"
    else:
        return f"{day} {month} {year} г."


def parse_iso_datetime(iso_string: str) -> datetime:
    """
    Parse ISO datetime string to timezone-aware datetime object.
    
    Args:
        iso_string: ISO format datetime string
        
    Returns:
        datetime: Parsed datetime object (always timezone-aware UTC)
        
    Raises:
        ValueError: If the input string is not a valid ISO datetime
    """
    try:
        # Try parsing with timezone info
        dt = datetime.fromisoformat(iso_string)
    except ValueError:
        try:
            # Fallback: handle 'Z' suffix and other common formats
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError(f"Invalid ISO datetime string: {iso_string}")
    
    # Ensure timezone-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = UTC_TZ.localize(dt)
    
    # Convert to UTC for consistency
    return dt.astimezone(UTC_TZ)


def get_week_start_date(target_date: Optional[Union[date, str]] = None) -> str:
    """
    Get the start date of the week (Monday) for the given date.
    
    Args:
        target_date: Target date (default: today)
        
    Returns:
        str: Week start date in YYYY-MM-DD format
    """
    if target_date is None:
        target_date = get_almaty_now().date()
    elif isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)
    
    # Calculate days since Monday (0=Monday, 6=Sunday)
    days_since_monday = target_date.weekday()
    week_start = target_date - timedelta(days=days_since_monday)
    
    return week_start.isoformat()


def get_month_start_date(target_date: Optional[Union[date, str]] = None) -> str:
    """
    Get the start date of the month for the given date.
    
    Args:
        target_date: Target date (default: today)
        
    Returns:
        str: Month start date in YYYY-MM-DD format
    """
    if target_date is None:
        target_date = get_almaty_now().date()
    elif isinstance(target_date, str):
        target_date = date.fromisoformat(target_date)
    
    month_start = target_date.replace(day=1)
    return month_start.isoformat()


def get_date_range_days(start_date: str, end_date: str) -> int:
    """
    Calculate number of days between two dates.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        
    Returns:
        int: Number of days between dates
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    
    return (end - start).days


def is_valid_date_string(date_string: str) -> bool:
    """
    Check if string is a valid ISO date.
    
    Args:
        date_string: Date string to validate
        
    Returns:
        bool: True if valid ISO date format
    """
    try:
        date.fromisoformat(date_string)
        return True
    except ValueError:
        return False


def get_retention_check_dates(retention_days: int, check_date: Optional[str] = None) -> list[str]:
    """
    Get list of dates that need retention checks for the given retention period.
    
    Args:
        retention_days: Number of days for retention check (e.g., 7, 14, 30)
        check_date: Date to check from (default: today)
        
    Returns:
        list[str]: List of dates that need retention checks
    """
    if check_date is None:
        check_date = get_today_date_str()
    
    check_date_obj = date.fromisoformat(check_date)
    target_date = check_date_obj - timedelta(days=retention_days)
    
    return [target_date.isoformat()]


def format_time_period_ru(days: int) -> str:
    """
    Format time period in Russian with correct pluralization.
    
    Args:
        days: Number of days
        
    Returns:
        str: Formatted period in Russian
    """
    if days == 1:
        return "1 день"
    
    # Special case for numbers ending in 11-14 (e.g., 11, 12, 13, 14, 111, 112, etc.)
    if days % 100 in [11, 12, 13, 14]:
        return f"{days} дней"
    elif days % 10 == 1:
        return f"{days} день"
    elif days % 10 in [2, 3, 4]:
        return f"{days} дня"
    else:
        return f"{days} дней"