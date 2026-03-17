"use client";

import React, { useEffect, useState } from "react";
import { motion } from "framer-motion";
import Link from "next/link";
import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

interface NavItem {
  name: string;
  url: string;
  icon: LucideIcon;
}

interface NavBarProps {
  items: NavItem[];
  className?: string;
}

export function NavBar({ items, className }: NavBarProps) {
  const [activeTab, setActiveTab] = useState(items[0].name);
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const handleResize = () => setIsMobile(window.innerWidth < 768);
    handleResize();
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  return (
    <div
      className={cn(
        "fixed top-0 left-1/2 z-50 w-[calc(100%-1.5rem)] max-w-max -translate-x-1/2 pt-4 md:pt-6",
        className,
      )}
    >
      <div className="flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-1 py-1 shadow-lg backdrop-blur-xl">
        {items.map((item) => {
          const Icon = item.icon;
          const isActive = activeTab === item.name;
          return (
            <Link
              key={item.name}
              href={item.url}
              onClick={() => setActiveTab(item.name)}
              className={cn(
                "relative cursor-pointer rounded-full px-4 py-2 text-sm font-semibold transition-colors md:px-6",
                "text-white/70 hover:text-[#00C2CB]",
                isActive && "text-[#00C2CB]",
              )}
            >
              <span className={cn(!isMobile && "hidden md:inline")}>
                {item.name}
              </span>
              <span className={cn(isMobile ? "inline-flex" : "md:hidden")}>
                <Icon size={18} strokeWidth={2.5} />
              </span>
              {isActive && (
                <motion.div
                  layoutId="lamp"
                  className="absolute inset-0 -z-10 w-full rounded-full bg-[#00C2CB]/10"
                  initial={false}
                  transition={{ type: "spring", stiffness: 300, damping: 30 }}
                >
                  <div className="absolute -top-2 left-1/2 h-1 w-8 -translate-x-1/2 rounded-t-full bg-[#00C2CB]">
                    <div className="absolute -top-2 -left-2 h-6 w-12 rounded-full bg-[#00C2CB]/20 blur-md" />
                    <div className="absolute -top-1 h-6 w-8 rounded-full bg-[#00C2CB]/20 blur-md" />
                  </div>
                </motion.div>
              )}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
