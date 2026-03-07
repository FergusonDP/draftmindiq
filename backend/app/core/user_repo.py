from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.core.database import db_cursor


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_to_dict(row) -> dict[str, Any] | None:
    return dict(row) if row else None


def list_users(search: str | None = None) -> list[dict[str, Any]]:
    sql = """
        SELECT
            u.id,
            u.username,
            u.email,
            u.full_name,
            u.role,
            u.is_active,
            u.created_at,
            usp.plan_name,
            usp.plan_status,
            usp.price_cents,
            usp.billing_cycle,
            usp.discount_type,
            usp.discount_value,
            usp.free_months,
            usp.promo_code,
            usp.promo_start,
            usp.promo_end,
            usp.trial_end
        FROM users u
        LEFT JOIN user_subscription_profiles usp
            ON usp.user_id = u.id
    """
    params: list[Any] = []

    if search:
        sql += """
            WHERE
                u.username LIKE ?
                OR u.email LIKE ?
                OR u.full_name LIKE ?
        """
        like = f"%{search}%"
        params.extend([like, like, like])

    sql += " ORDER BY u.created_at DESC"

    with db_cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT
                u.id,
                u.username,
                u.email,
                u.full_name,
                u.role,
                u.is_active,
                u.created_at,
                u.updated_at,
                usp.plan_name,
                usp.plan_status,
                usp.price_cents,
                usp.billing_cycle,
                usp.discount_type,
                usp.discount_value,
                usp.free_months,
                usp.promo_code,
                usp.promo_start,
                usp.promo_end,
                usp.trial_end,
                usp.current_period_start,
                usp.current_period_end,
                usp.notes
            FROM users u
            LEFT JOIN user_subscription_profiles usp
                ON usp.user_id = u.id
            WHERE u.id = ?
            """,
            (user_id,),
        )
        row = cur.fetchone()
        return row_to_dict(row)


def get_user_by_username(username: str) -> dict[str, Any] | None:
    with db_cursor() as cur:
        cur.execute(
            """
            SELECT id, username, email, full_name, password_hash, role, is_active
            FROM users
            WHERE username = ?
            """,
            (username,),
        )
        row = cur.fetchone()
        return row_to_dict(row)


def update_user_admin_fields(
    user_id: int,
    *,
    role: str | None = None,
    is_active: int | None = None,
    plan_name: str | None = None,
    plan_status: str | None = None,
    price_cents: int | None = None,
    billing_cycle: str | None = None,
    discount_type: str | None = None,
    discount_value: float | None = None,
    free_months: int | None = None,
    promo_code: str | None = None,
    promo_start: str | None = None,
    promo_end: str | None = None,
    trial_end: str | None = None,
    notes: str | None = None,
) -> None:
    now = utc_now()

    user_updates = []
    user_params: list[Any] = []

    if role is not None:
        user_updates.append("role = ?")
        user_params.append(role)

    if is_active is not None:
        user_updates.append("is_active = ?")
        user_params.append(is_active)

    if user_updates:
        user_updates.append("updated_at = ?")
        user_params.append(now)
        user_params.append(user_id)

        with db_cursor(commit=True) as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {", ".join(user_updates)}
                WHERE id = ?
                """,
                user_params,
            )

    subscription_updates = []
    subscription_params: list[Any] = []

    if plan_name is not None:
        subscription_updates.append("plan_name = ?")
        subscription_params.append(plan_name)

    if plan_status is not None:
        subscription_updates.append("plan_status = ?")
        subscription_params.append(plan_status)

    if price_cents is not None:
        subscription_updates.append("price_cents = ?")
        subscription_params.append(price_cents)

    if billing_cycle is not None:
        subscription_updates.append("billing_cycle = ?")
        subscription_params.append(billing_cycle)

    if discount_type is not None:
        subscription_updates.append("discount_type = ?")
        subscription_params.append(discount_type)

    if discount_value is not None:
        subscription_updates.append("discount_value = ?")
        subscription_params.append(discount_value)

    if free_months is not None:
        subscription_updates.append("free_months = ?")
        subscription_params.append(free_months)

    if promo_code is not None:
        subscription_updates.append("promo_code = ?")
        subscription_params.append(promo_code)

    if promo_start is not None:
        subscription_updates.append("promo_start = ?")
        subscription_params.append(promo_start)

    if promo_end is not None:
        subscription_updates.append("promo_end = ?")
        subscription_params.append(promo_end)

    if trial_end is not None:
        subscription_updates.append("trial_end = ?")
        subscription_params.append(trial_end)

    if notes is not None:
        subscription_updates.append("notes = ?")
        subscription_params.append(notes)

    if subscription_updates:
        subscription_updates.append("updated_at = ?")
        subscription_params.append(now)
        subscription_params.append(user_id)

        with db_cursor(commit=True) as cur:
            cur.execute(
                f"""
                UPDATE user_subscription_profiles
                SET {", ".join(subscription_updates)}
                WHERE user_id = ?
                """,
                subscription_params,
            )


def add_admin_audit_log(
    admin_user_id: int,
    action: str,
    *,
    target_user_id: int | None = None,
    old_value: str | None = None,
    new_value: str | None = None,
    notes: str | None = None,
) -> None:
    with db_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO admin_audit_log (
                admin_user_id, target_user_id, action, old_value, new_value, notes, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                admin_user_id,
                target_user_id,
                action,
                old_value,
                new_value,
                notes,
                utc_now(),
            ),
        )


def get_system_flags() -> dict[str, str | None]:
    with db_cursor() as cur:
        cur.execute("SELECT key, value_text FROM system_flags")
        rows = cur.fetchall()
        return {row["key"]: row["value_text"] for row in rows}


def set_system_flag(key: str, value_text: str) -> None:
    with db_cursor(commit=True) as cur:
        cur.execute(
            """
            INSERT INTO system_flags (key, value_text, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value_text = excluded.value_text,
                updated_at = excluded.updated_at
            """,
            (key, value_text, utc_now()),
        )
