// src/app/settings.js
const KEY = "dmiq.settings.v1";

export const DEFAULT_SETTINGS = {
  timezone: "America/New_York",
  theme: "DARK_NAVY",
  layout: "STANDARD",
  favoriteSport: "ALL",
};

export function loadSettings() {
  try {
    const raw = localStorage.getItem(KEY);
    if (!raw) return DEFAULT_SETTINGS;
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_SETTINGS, ...parsed };
  } catch {
    return DEFAULT_SETTINGS;
  }
}

export function saveSettings(next) {
  localStorage.setItem(KEY, JSON.stringify(next));
}