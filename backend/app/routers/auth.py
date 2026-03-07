from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.core.user_repo import get_user_by_username
from app.core.init_db import create_user, hash_password

router = APIRouter(prefix="/auth", tags=["Auth"])


class RegisterRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    full_name: str | None = None


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/register")
def register_user(payload: RegisterRequest):

    existing = get_user_by_username(payload.username)

    if existing:
        raise HTTPException(status_code=400, detail="Username already exists")

    user_id = create_user(
        username=payload.username,
        password=payload.password,
        email=payload.email,
        full_name=payload.full_name,
        role="user",
    )

    return {"ok": True, "user_id": user_id}


@router.post("/login")
def login(payload: LoginRequest):

    user = get_user_by_username(payload.username)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    stored_hash = user["password_hash"]
    salt, digest = stored_hash.split("$")

    test_hash = hash_password(payload.password, salt)

    if test_hash != stored_hash:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {
        "ok": True,
        "user": {"id": user["id"], "username": user["username"], "role": user["role"]},
    }
