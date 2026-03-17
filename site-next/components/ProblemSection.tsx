import { AlertTriangle, GitBranch, XCircle } from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { Card, CardContent } from "@/components/ui/card";

const problems = [
  {
    icon: AlertTriangle,
    color: "var(--amber)",
    title: "3 days from go-live. 4,000 records flagged.",
    body: "Your implementation partner says it looks clean. Your HRIS analyst knows something is wrong. There is no time to check 50,000 rows manually.",
  },
  {
    icon: XCircle,
    color: "var(--red)",
    title: "The CHRO wants to sign off. On what exactly?",
    body: "There is no document. No audit trail. No proof that the data was checked. Just a verbal yes and a prayer that payroll runs correctly on Monday.",
  },
  {
    icon: GitBranch,
    color: "var(--teal)",
    title: "Two systems. Neither one is the source of truth.",
    body: "ADP says 15,584 active employees. Workday shows 15,602. The difference is 18 people. Do you know which system is right?",
  },
];

export function ProblemSection() {
  return (
    <section className="section-pad bg-white/[0.03]">
      <div className="section-frame space-y-14">
        <FadeUp>
          <div className="mx-auto max-w-4xl text-center">
            <h2 className="section-title">
              Every migration has the same problems. Most teams find out too late.
            </h2>
          </div>
        </FadeUp>

        <div className="grid gap-6 lg:grid-cols-3">
          {problems.map((problem, index) => {
            const Icon = problem.icon;
            return (
              <FadeUp key={problem.title} delay={index * 0.1}>
                <Card
                  className="group glass-card-hover relative h-full overflow-hidden rounded-[28px] border-white/10"
                  style={{ borderLeft: "1px solid rgba(240, 244, 248, 0.1)" }}
                >
                  <CardContent
                    className="h-full space-y-5 p-8"
                    style={{ boxShadow: "inset 0 0 0 1px rgba(255,255,255,0.01)" }}
                  >
                    <div
                      className="inline-flex rounded-2xl p-3"
                      style={{
                        color: problem.color,
                        backgroundColor: "rgba(255,255,255,0.03)",
                      }}
                    >
                      <Icon className="h-6 w-6" />
                    </div>
                    <h3 className="text-2xl leading-tight tracking-[-0.03em]">
                      {problem.title}
                    </h3>
                    <p className="text-base leading-7 text-[var(--muted)]">
                      {problem.body}
                    </p>
                  </CardContent>
                  <div
                    className="absolute inset-y-0 left-0 w-1 opacity-0 transition-opacity duration-200 group-hover:opacity-100"
                    style={{ backgroundColor: problem.color }}
                  />
                </Card>
              </FadeUp>
            );
          })}
        </div>
      </div>
    </section>
  );
}
