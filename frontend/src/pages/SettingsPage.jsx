// src/pages/SettingsPage.jsx
import React, { useMemo } from "react";
import { useSettings } from "../app/SettingsContext";

const TIMEZONES = [
  { value: "America/New_York", label: "Eastern (America/New_York)" },
  { value: "America/Chicago", label: "Central (America/Chicago)" },
  { value: "America/Denver", label: "Mountain (America/Denver)" },
  { value: "America/Los_Angeles", label: "Pacific (America/Los_Angeles)" },
  { value: "UTC", label: "UTC" },
];

export default function SettingsPage() {
  const { settings, setSettings } = useSettings();

  const preview = useMemo(() => {
    return new Date().toLocaleString("en-US", {
      weekday: "short",
      month: "short",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      timeZone: settings.timezone,
    });
  }, [settings.timezone]);

  return (
    <div style={{ padding: 24 }}>
      <h2 style={{ marginTop: 0 }}>Settings</h2>
      <p style={{ opacity: 0.75, marginTop: 6 }}>
        Timezone controls the navbar clock and any slate times.
      </p>

      <div style={{ marginTop: 18, maxWidth: 520 }}>
        <label style={{ display: "block", fontWeight: 800, marginBottom: 8 }}>
          Timezone
        </label>

        <select
          value={settings.timezone}
          onChange={(e) => setSettings({ timezone: e.target.value })}
          style={{ width: "100%", height: 42, borderRadius: 10, padding: "0 10px" }}
        >
          {TIMEZONES.map((tz) => (
            <option key={tz.value} value={tz.value}>
              {tz.label}
            </option>
          ))}
        </select>

        <div style={{ marginTop: 10, opacity: 0.8 }}>
          Preview: <strong>{preview}</strong>
        </div>
      </div>
    </div>
  );
}