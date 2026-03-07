import React from "react";
import { BrowserRouter, Routes, Route, Navigate, Outlet } from "react-router-dom";

import LoginPage from "./pages/LoginPage";
import SignupPage from "./pages/SignupPage";
import DashboardRouter from "./pages/DashboardRouter";
import MMADatabasePage from "./pages/MMADatabasePage";
import MMAOptimizerPage from "./pages/MMAOptimizerPage";
import SettingsPage from "./pages/SettingsPage";
import AdminMmaHub from "./pages/AdminMmaHub";

/* -------------------------
   Auth Gate
-------------------------- */
function isAuthed() {
  return (
    localStorage.getItem("beta_ok") === "true" ||
    sessionStorage.getItem("beta_ok") === "true"
  );
}

function RequireAuth() {
  return isAuthed() ? <Outlet /> : <Navigate to="/login" replace />;
}

/* -------------------------
   Placeholder Pages
-------------------------- */
function OptimizerPlaceholder({ sport }) {
  return (
    <div style={{ padding: 24 }}>
      <h2>{sport} Optimizer</h2>
      <p>Placeholder route. Will wire real optimizer after dashboard is finalized.</p>
    </div>
  );
}

/* -------------------------
   App Router
-------------------------- */
export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        {/* Public auth routes */}
        <Route path="/login" element={<LoginPage />} />
        <Route path="/signup" element={<SignupPage />} />

        {/* Protected app routes */}
        <Route element={<RequireAuth />}>
          <Route path="/" element={<Navigate to="/dashboard" replace />} />

          <Route path="/dashboard" element={<DashboardRouter />} />
          <Route path="/databases" element={<MMADatabasePage />} />
          <Route path="/settings" element={<SettingsPage />} />

          {/* Admin pages */}
          <Route path="/admin/mma" element={<AdminMmaHub />} />

          {/* Optimizers */}
          <Route path="/optimizer/mma" element={<MMAOptimizerPage />} />
          <Route path="/optimizer/nfl" element={<OptimizerPlaceholder sport="NFL" />} />
          <Route path="/optimizer/nba" element={<OptimizerPlaceholder sport="NBA" />} />
          <Route path="/optimizer/mlb" element={<OptimizerPlaceholder sport="MLB" />} />
          <Route path="/optimizer/nhl" element={<OptimizerPlaceholder sport="NHL" />} />
        </Route>

        {/* Catch-all */}
        <Route path="*" element={<Navigate to="/dashboard" replace />} />
      </Routes>
    </BrowserRouter>
  );
}