-- users table (если уже есть — использовать имеющуюся)
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tg_user_id INTEGER UNIQUE NOT NULL,
  username TEXT,
  name TEXT
);

-- inviters table (если уже есть — использовать)
CREATE TABLE IF NOT EXISTS inviters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  username TEXT,  -- username пригласителя (например @alex)
  invite_link TEXT UNIQUE,
  channel_id TEXT  -- ID канала, где создано приглашение
);

-- journal: все события (подписка/отписка/прочее)
CREATE TABLE IF NOT EXISTS journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_time TEXT NOT NULL,        -- ISO timestamp with TZ
  event_type TEXT NOT NULL,        -- 'subscribe' | 'unsubscribe' | 'join_request' | 'manual_add' ...
  tg_user_id INTEGER NOT NULL,     -- Telegram user id
  username TEXT,
  name TEXT,
  inviter_id INTEGER,              -- nullable
  status TEXT,                     -- 'subscribed'|'left' etc.
  note TEXT,                       -- e.g. 'repeat' if not first inviter
  telegram_update_id INTEGER,      -- for idempotency (nullable)
  FOREIGN KEY(inviter_id) REFERENCES inviters(id)
);

-- Unique constraint for idempotency when telegram_update_id is provided
CREATE UNIQUE INDEX IF NOT EXISTS idx_journal_telegram_update_id ON journal(telegram_update_id) WHERE telegram_update_id IS NOT NULL;

-- NOTE: For production upgrades on existing databases, you would need:
-- ALTER TABLE journal ADD COLUMN telegram_update_id INTEGER; 
-- then backfill/deduplicate existing data before creating the unique index

-- retention_checks table: stores retention evaluation results (so we don't re-evaluate same user)
CREATE TABLE IF NOT EXISTS retention_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  journal_id INTEGER NOT NULL,
  check_date TEXT NOT NULL,        -- date when check happened (ISO date)
  result TEXT NOT NULL,            -- 'retained'|'not_retained'|'pending'
  FOREIGN KEY(journal_id) REFERENCES journal(id),
  UNIQUE(journal_id, check_date)
);

-- Indexes for better performance
CREATE INDEX IF NOT EXISTS idx_journal_event_time ON journal(event_time);
CREATE INDEX IF NOT EXISTS idx_journal_tg_user_id ON journal(tg_user_id);
CREATE INDEX IF NOT EXISTS idx_journal_event_type ON journal(event_type);
CREATE INDEX IF NOT EXISTS idx_retention_checks_journal_id ON retention_checks(journal_id);
CREATE INDEX IF NOT EXISTS idx_retention_checks_check_date ON retention_checks(check_date);