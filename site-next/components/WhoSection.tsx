import { Briefcase, Code2, Shield } from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { Card, CardContent } from "@/components/ui/card";

const personas = [
  {
    icon: Code2,
    color: "var(--teal)",
    role: "HRIS Analyst",
    title: "You run reconciliations in Excel. Stop.",
    body: "40 hours of manual matching. VLOOKUPs that break. A spreadsheet that only you understand. Data Whisperer does it in minutes and produces an audit trail you can actually hand to someone.",
    stat: "40 hrs saved per migration",
  },
  {
    icon: Briefcase,
    color: "var(--amber)",
    role: "Implementation Partner",
    title: "Your clients ask you to sign off. Now you can.",
    body: "You deliver Workday implementations. Your reputation is on the data quality. This is the pre-load validation report your clients will start asking for by name.",
    stat: "Used on every client engagement",
  },
  {
    icon: Shield,
    color: "var(--emerald)",
    role: "HR Director / CHRO",
    title: "You sign before the load. Sign with confidence.",
    body: "A four-page PDF with your name on it. Run ID. File hashes. Gate result. Correction counts. The document that goes in the compliance file and answers every question an auditor will ask.",
    stat: "CHRO approval document included",
  },
];

export function WhoSection() {
  return (
    <section id="who" className="section-pad bg-[var(--navy)]">
      <div className="section-frame space-y-14">
        <FadeUp>
          <div className="mx-auto max-w-4xl text-center">
            <h2 className="section-title">
              Built for everyone in the room on go-live day
            </h2>
          </div>
        </FadeUp>

        <div className="grid gap-6 lg:grid-cols-3">
          {personas.map((persona, index) => {
            const Icon = persona.icon;
            return (
              <FadeUp key={persona.role} delay={index * 0.1}>
                <Card className="glass-card-hover h-full rounded-[28px] border-white/10">
                  <CardContent className="flex h-full flex-col gap-5 p-8">
                    <div
                      className="inline-flex w-fit rounded-2xl p-3"
                      style={{
                        backgroundColor: "rgba(255,255,255,0.03)",
                        color: persona.color,
                      }}
                    >
                      <Icon className="h-6 w-6" />
                    </div>
                    <div className="mono text-xs uppercase tracking-[0.18em] text-[var(--muted)]">
                      {persona.role}
                    </div>
                    <h3 className="text-2xl tracking-[-0.03em]">{persona.title}</h3>
                    <p className="flex-1 leading-7 text-[var(--muted)]">
                      {persona.body}
                    </p>
                    <div className="mono rounded-full border border-white/10 bg-white/5 px-4 py-3 text-sm text-[var(--teal)]">
                      {persona.stat}
                    </div>
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
