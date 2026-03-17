"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { Card, CardContent } from "@/components/ui/card";
import { cn } from "@/lib/utils";

const logLines = [
  "Ingesting 50,000 records...",
  "Normalizing column aliases...",
  "Tier 1 match: worker_id -> 47,843 matched",
  "Tier 2 match: pk -> 1,483 matched",
  "Gating: extreme_salary_ratio -> 456 flagged",
  "Gating: hire_date_wave -> 1,182 routed to REVIEW",
  "Active/$0 safety check -> 178 blocked",
  "Sanity gate: evaluating...",
  "GATE: PASS",
  "Generating corrections manifest...",
  "Writing CHRO approval document...",
  "Run complete. 49,345 matched. 625 unmatched.",
];

const statPills = [
  { value: "99.7%", label: "Det. Match Rate" },
  { value: "0", label: "AI Guesses" },
  { value: "100%", label: "Auditable" },
];

function lineClass(line: string) {
  if (line.includes("PASS")) return "text-[var(--emerald)]";
  if (line.includes("blocked")) return "text-[var(--red)]";
  if (line.includes("flagged") || line.includes("REVIEW")) {
    return "text-[var(--amber)]";
  }
  if (line.includes("match")) return "text-[var(--teal)]";
  if (line.includes("Run complete")) return "text-[var(--white)]";
  return "text-white/65";
}

export function TerminalAnimation() {
  const [visibleCount, setVisibleCount] = useState(1);

  useEffect(() => {
    if (visibleCount < logLines.length) {
      const timer = window.setTimeout(() => {
        setVisibleCount((count) => count + 1);
      }, 800);
      return () => window.clearTimeout(timer);
    }

    const loopTimer = window.setTimeout(() => {
      setVisibleCount(1);
    }, 2000);

    return () => window.clearTimeout(loopTimer);
  }, [visibleCount]);

  return (
    <div className="space-y-5">
      <Card className="overflow-hidden rounded-[30px] border-white/12 bg-[rgba(8,16,29,0.82)] shadow-[0_25px_80px_rgba(0,0,0,0.35)] teal-ring">
        <div className="flex items-center justify-between border-b border-white/8 px-6 py-4">
          <div className="flex items-center gap-2">
            <span className="h-3 w-3 rounded-full bg-[var(--red)]" />
            <span className="h-3 w-3 rounded-full bg-[var(--amber)]" />
            <span className="h-3 w-3 rounded-full bg-[var(--emerald)]" />
          </div>
          <span className="mono text-xs uppercase tracking-[0.18em] text-white/45">
            recon-engine v1.0
          </span>
        </div>
        <CardContent className="min-h-[380px] space-y-3 p-6">
          {logLines.slice(0, visibleCount).map((line) => (
            <motion.div
              key={`${line}-${visibleCount}`}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4, ease: "easeOut" }}
              className={cn(
                "mono flex items-start gap-3 text-sm md:text-[0.95rem]",
                lineClass(line),
              )}
            >
              <span className="mt-1 h-2 w-2 rounded-full bg-current opacity-75" />
              <span>{line}</span>
            </motion.div>
          ))}
          <div className="mono flex items-center gap-2 pt-2 text-sm text-[var(--teal)]">
            <span className="opacity-80">&gt;</span>
            <motion.span
              animate={{ opacity: [0, 1, 0] }}
              transition={{ duration: 1.1, repeat: Infinity, ease: "easeInOut" }}
              className="inline-block h-4 w-2 rounded-sm bg-[var(--teal)]"
            />
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-3 sm:grid-cols-3">
        {statPills.map((pill) => (
          <div
            key={pill.label}
            className="glass-card rounded-full px-4 py-3 text-center transition-all duration-200 ease-out hover:scale-[1.02] hover:shadow-[0_0_30px_rgba(0,194,203,0.15)]"
          >
            <div className="mono text-lg font-medium text-[var(--teal)]">
              {pill.value}
            </div>
            <div className="text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
              {pill.label}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
