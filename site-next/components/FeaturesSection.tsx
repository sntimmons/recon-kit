import {
  BarChart2,
  Calendar,
  DollarSign,
  RefreshCw,
  TrendingDown,
  UserX,
} from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { Card, CardContent } from "@/components/ui/card";

const features = [
  {
    icon: TrendingDown,
    severity: "CRITICAL",
    tone: "var(--red)",
    badgeBg: "rgba(239, 68, 68, 0.14)",
    title: "Salary decimal shifts",
    body: "$315,000 entered as $31,500. The engine detects salary ratios outside the 85-115% band and routes them to review before they become payroll errors.",
  },
  {
    icon: DollarSign,
    severity: "CRITICAL",
    tone: "var(--red)",
    badgeBg: "rgba(239, 68, 68, 0.14)",
    title: "Active employees with $0 salary",
    body: "178 employees. Active status. No salary. This data will not survive a payroll run. The engine flags and blocks every one before corrections are staged.",
  },
  {
    icon: Calendar,
    severity: "HIGH",
    tone: "var(--amber)",
    badgeBg: "rgba(245, 158, 11, 0.16)",
    title: "Hire date wave defaults",
    body: "1,200 employees with the same hire date is not a coincidence. It is a bulk import default. The engine detects the pattern and triggers a sanity gate review.",
  },
  {
    icon: UserX,
    severity: "HIGH",
    tone: "var(--amber)",
    badgeBg: "rgba(245, 158, 11, 0.16)",
    title: "Wrong-person match risks",
    body: "When confidence drops below 75%, the engine blocks the match entirely. No corrections. No assumptions. The pair goes to manual review with a full explanation.",
  },
  {
    icon: RefreshCw,
    severity: "HIGH",
    tone: "var(--amber)",
    badgeBg: "rgba(245, 158, 11, 0.16)",
    title: "Active to terminated flips",
    body: "Every status change from Active to Terminated is held for human review. No automated correction touches an employment status without a reviewer signing off.",
  },
  {
    icon: BarChart2,
    severity: "MEDIUM",
    tone: "var(--teal)",
    badgeBg: "rgba(0, 194, 203, 0.16)",
    title: "Pay equity variance",
    body: "Same title. Same department. 40% salary gap. The audit engine flags variance above 30% within the same role and surfaces it before it becomes a legal question.",
  },
];

export function FeaturesSection() {
  return (
    <section id="features" className="section-pad bg-[rgba(15,37,68,0.38)]">
      <div className="section-frame space-y-14">
        <FadeUp>
          <div className="mx-auto max-w-3xl space-y-5 text-center">
            <h2 className="section-title">
              What the engine catches automatically
            </h2>
            <p className="section-subtitle">
              The mistakes that slip through manual review every single time.
            </p>
          </div>
        </FadeUp>

        <div className="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
          {features.map((feature, index) => {
            const Icon = feature.icon;
            return (
              <FadeUp key={feature.title} delay={(index % 3) * 0.1}>
                <Card
                  className="glass-card-hover relative h-full rounded-[28px] border-white/10"
                  style={{
                    boxShadow: "0 10px 40px rgba(0,0,0,0.18)",
                  }}
                >
                  <CardContent className="flex h-full flex-col gap-5 p-8">
                    <div className="flex items-start justify-between gap-4">
                      <div
                        className="rounded-2xl p-3"
                        style={{
                          backgroundColor: "rgba(255,255,255,0.03)",
                          color: feature.tone,
                        }}
                      >
                        <Icon className="h-6 w-6" />
                      </div>
                      <span
                        className="severity-pill mono"
                        style={{
                          backgroundColor: feature.badgeBg,
                          color: feature.tone,
                        }}
                      >
                        {feature.severity}
                      </span>
                    </div>
                    <h3 className="text-2xl tracking-[-0.03em]">
                      {feature.title}
                    </h3>
                    <p className="leading-7 text-[var(--muted)]">
                      {feature.body}
                    </p>
                  </CardContent>
                </Card>
              </FadeUp>
            );
          })}
        </div>
      </div>
    </section>
  );
}
