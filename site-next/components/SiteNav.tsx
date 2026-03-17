"use client";

import { Home, PlayCircle, Shield, Users, Zap } from "lucide-react";
import { NavBar } from "@/components/ui/tubelight-navbar";

const navItems = [
  { name: "Home", url: "#hero", icon: Home },
  { name: "How It Works", url: "#how", icon: Zap },
  { name: "Features", url: "#features", icon: Shield },
  { name: "For Who", url: "#who", icon: Users },
  { name: "Demo", url: "#demo", icon: PlayCircle },
];

export function SiteNav() {
  return <NavBar items={navItems} />;
}
