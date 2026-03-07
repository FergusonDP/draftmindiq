import React from "react";
import AdminDashboard from "./AdminDashboard";
import UserDashboard from "./UserDashboard";

function getStoredUser() {
  try {
    return JSON.parse(localStorage.getItem("dm_user") || "{}");
  } catch {
    return {};
  }
}

export default function DashboardRouter() {
  const user = getStoredUser();
  const role = String(user?.role || "user").toLowerCase();

  return role === "admin" ? <AdminDashboard /> : <UserDashboard />;
}