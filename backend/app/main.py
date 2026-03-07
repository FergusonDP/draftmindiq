from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from app.routers.auth import router as auth_router
from app.sports.mma.fighter_cards_router import router as fighter_cards_router
from app.modules.news.router import router as news_router
from app.modules.video.router import router as video_router
from app.sports.mma.dk.router import router as mma_router
from app.sports.mma.history_explorer_router import router as mma_history_router
from app.modules.data_explorer.router import router as data_explorer_router
from app.core.init_db import bootstrap_database


app = FastAPI(title="DraftMindIQ API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(fighter_cards_router)
app.include_router(auth_router)
app.include_router(mma_router)
app.include_router(news_router)
app.include_router(video_router)
app.include_router(mma_history_router)
app.include_router(data_explorer_router)


@app.get("/health")
def health():
    return {"ok": True}


@app.on_event("startup")
def startup_event() -> None:
    bootstrap_database()
