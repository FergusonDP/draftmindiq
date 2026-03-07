// src/app/SettingsContext.js
import React, { createContext, useContext, useMemo, useState } from "react";
import { loadSettings, saveSettings } from "./settings";

const SettingsCtx = createContext(null);

export function SettingsProvider({ children }) {
  const [settings, setSettingsState] = useState(loadSettings());

  const setSettings = (patch) => {
    setSettingsState((prev) => {
      const next = typeof patch === "function" ? patch(prev) : { ...prev, ...patch };
      saveSettings(next);
      return next;
    });
  };

  const value = useMemo(() => ({ settings, setSettings }), [settings]);
  return <SettingsCtx.Provider value={value}>{children}</SettingsCtx.Provider>;
}

export function useSettings() {
  const ctx = useContext(SettingsCtx);
  if (!ctx) throw new Error("useSettings must be used inside SettingsProvider");
  return ctx;
}