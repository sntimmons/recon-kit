import { Download, GitMerge, Upload } from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";

const steps = [
  {
    number: "01",
    icon: Upload,
    title: "Upload your exports",
    body: "Drop your ADP legacy file and your Workday file. CSV or Excel. The engine reads both and normalizes column names automatically using the system connector library.",
  },
  {
    number: "02",
    icon: GitMerge,
    title: "The engine matches every record",
    body: "Six deterministic matching tiers from worker ID to name and date of birth. Every match has a confidence score. Every decision is logged. Nothing is guessed.",
  },
  {
    number: "03",
    icon: Download,
    title: "Download your corrections and sign-off",
    body: "A corrections manifest ready to load into Workday. A review queue for the records that need human eyes. A CHRO approval document with a signature block.",
  },
];

const stats = [
  ["99.7%", "Deterministic match rate"],
  ["6", "Matching tiers"],
  ["0", "AI-generated decisions"],
  ["100%", "Audit trail coverage"],
];

export function HowItWorksSection() {
  return (
    <section id="how" className="section-pad border-y border-white/8 bg-[var(--navy)]">
      <div className="section-frame space-y-14">
        <FadeUp>
          <div className="space-y-5 text-center">
            <Badge className="border-[rgba(0,194,203,0.45)] bg-[rgba(0,194,203,0.08)] text-[var(--teal)]">
              The Process
            </Badge>
            <h2 className="section-title">Three steps. Zero guessing.</h2>
          </div>
        </FadeUp>

        <div className="relative grid gap-6 lg:grid-cols-3">
          <div className="absolute top-16 left-[16.66%] right-[16.66%] hidden h-px bg-gradient-to-r from-transparent via-[rgba(0,194,203,0.35)] to-transparent lg:block" />
          {steps.map((step, index) => {
            const Icon = step.icon;
            return (
              <FadeUp key={step.number} delay={index * 0.1}>
                <Card className="glass-card-hover relative h-full rounded-[28px] border-white/10">
                  <CardContent className="space-y-6 p-8">
                    <div className="flex items-center gap-4">
                      <div className="mono flex h-14 w-14 items-center justify-center rounded-full border border-[rgba(0,194,203,0.35)] bg-[rgba(0,194,203,0.08)] text-lg text-[var(--teal)]">
                        {step.number}
                      </div>
                      <div className="rounded-2xl bg-white/5 p-3 text-[var(--teal)]">
                        <Icon className="h-6 w-6" />
                      </div>
                    </div>
                    <h3 className="text-2xl tracking-[-0.03em]">{step.title}</h3>
                    <p className="leading-7 text-[var(--muted)]">{step.body}</p>
                  </CardContent>
                </Card>
              </FadeUp>
            );
          })}
        </div>

        <FadeUp delay={0.2}>
          <div className="grid gap-6 rounded-[30px] border-y border-[rgba(0,194,203,0.35)] bg-[rgba(0,194,203,0.1)] px-6 py-8 md:grid-cols-4">
            {stats.map(([value, label]) => (
              <div key={label} className="text-center md:text-left">
                <div className="mono text-3xl text-[var(--teal)]">{value}</div>
                <div className="mono mt-2 text-xs uppercase tracking-[0.18em] text-white/70">
                  {label}
                </div>
              </div>
            ))}
          </div>
        </FadeUp>
      </div>
    </section>
  );
}
