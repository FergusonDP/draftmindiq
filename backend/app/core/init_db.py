from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone

from app.core.database import db_cursor


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_password(password: str, salt: str | None = None) -> str:
    """
    Simple SHA-256 salted hash for local MVP use.
    Later, replace with passlib/bcrypt or argon2.
    """
    if salt is None:
        salt = secrets.token_hex(16)

    digest = hashlib.sha256(f"{salt}:{password}".encode("utf-8")).hexdigest()
    return f"{salt}${digest}"


def create_tables() -> None:
    with db_cursor(commit=True) as cur:
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT UNIQUE,
                full_name TEXT,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'admin')),
                is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
                created_at TEXT NOT NULL,
                updated_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

            CREATE TABLE IF NOT EXISTS user_subscription_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL UNIQUE,
                plan_name TEXT NOT NULL DEFAULT 'free',
                plan_status TEXT NOT NULL DEFAULT 'inactive'
                    CHECK (plan_status IN ('inactive', 'trial', 'active', 'past_due', 'canceled', 'comped')),
                price_cents INTEGER NOT NULL DEFAULT 0,
                billing_cycle TEXT NOT NULL DEFAULT 'monthly'
                    CHECK (billing_cycle IN ('monthly', 'yearly', 'none')),
                discount_type TEXT
                    CHECK (discount_type IN ('percent', 'fixed') OR discount_type IS NULL),
                discount_value REAL,
                free_months INTEGER NOT NULL DEFAULT 0,
                promo_code TEXT,
                promo_start TEXT,
                promo_end TEXT,
                trial_end TEXT,
                current_period_start TEXT,
                current_period_end TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_subscription_plan_status
                ON user_subscription_profiles(plan_status);
            CREATE INDEX IF NOT EXISTS idx_subscription_plan_name
                ON user_subscription_profiles(plan_name);

            CREATE TABLE IF NOT EXISTS system_flags (
                key TEXT PRIMARY KEY,
                value_text TEXT,
                value_json TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_user_id INTEGER NOT NULL,
                target_user_id INTEGER,
                action TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (admin_user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY (target_user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_admin_audit_admin_user
                ON admin_audit_log(admin_user_id);
            CREATE INDEX IF NOT EXISTS idx_admin_audit_target_user
                ON admin_audit_log(target_user_id);
            CREATE INDEX IF NOT EXISTS idx_admin_audit_action
                ON admin_audit_log(action);
            """
        )


def seed_system_flags() -> None:
    now = utc_now()
    defaults = [
        ("maintenance_mode", "false", None, now),
        ("maintenance_message", "Platform updates in progress.", None, now),
        ("allow_admin_bypass", "true", None, now),
    ]

    with db_cursor(commit=True) as cur:
        cur.executemany(
            """
            INSERT OR IGNORE INTO system_flags (key, value_text, value_json, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            defaults,
        )


def create_user(
    username: str,
    password: str,
    email: str | None = None,
    full_name: str | None = None,
    role: str = "user",
    is_active: int = 1,
    plan_name: str = "free",
    plan_status: str = "inactive",
    price_cents: int = 0,
    billing_cycle: str = "none",
) -> int:
    now = utc_now()
    password_hash = hash_password(password)

    with db_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO users (
                username, email, full_name, password_hash, role, is_active, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                username,
                email,
                full_name,
                password_hash,
                role,
                is_active,
                now,
                now,
            ),
        )
        user_id = int(cur.lastrowid)

        cur.execute(
            """
            INSERT INTO user_subscription_profiles (
                user_id, plan_name, plan_status, price_cents, billing_cycle,
                free_months, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                plan_name,
                plan_status,
                price_cents,
                billing_cycle,
                0,
                now,
                now,
            ),
        )

    return user_id


def seed_admin_user() -> None:
    with db_cursor() as cur:
        cur.execute("SELECT id FROM users WHERE username = ?", ("admin",))
        row = cur.fetchone()

    if row:
        return

    create_user(
        username="admin",
        password="ChangeMe123!",
        email="admin@draftmindiq.local",
        full_name="DraftMindIQ Admin",
        role="admin",
        is_active=1,
        plan_name="internal",
        plan_status="active",
        price_cents=0,
        billing_cycle="none",
    )


def bootstrap_database() -> None:
    create_tables()
    seed_system_flags()
    seed_admin_user()


if __name__ == "__main__":
    bootstrap_database()
    print("Database initialized successfully.")
