import { ArrowRight, Zap } from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { TerminalAnimation } from "@/components/TerminalAnimation";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

export function HeroSection() {
  return (
    <section
      id="hero"
      className="hero-grid relative flex min-h-screen items-center overflow-hidden border-b border-white/8 pt-28"
    >
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_75%_25%,rgba(0,194,203,0.16),transparent_22%),radial-gradient(circle_at_25%_75%,rgba(26,58,92,0.5),transparent_28%)]" />
      <div className="section-frame section-pad relative grid items-center gap-16 lg:grid-cols-[1.15fr_0.85fr]">
        <div className="space-y-8">
          <FadeUp delay={0}>
            <Badge className="border-[rgba(0,194,203,0.45)] bg-[rgba(0,194,203,0.08)] text-[var(--teal)]">
              <Zap className="h-3.5 w-3.5" />
              Deterministic HR Data Reconciliation
            </Badge>
          </FadeUp>

          <FadeUp delay={0.1}>
            <div className="space-y-6">
              <h1 className="max-w-[12ch] text-[3rem] leading-[0.95] font-extrabold tracking-[-0.05em] text-[var(--white)] md:text-[4.5rem]">
                Your migration data is <span className="text-[var(--teal)]">wrong.</span> We find it first.
              </h1>
              <p className="max-w-2xl text-lg leading-8 text-[var(--muted)] md:text-xl">
                Data Whisperer reconciles ADP and Workday records deterministically.
                No AI guessing. No spreadsheets. No surprises on go-live day.
              </p>
            </div>
          </FadeUp>

          <FadeUp delay={0.2}>
            <div className="space-y-4">
              <div className="flex flex-col gap-4 sm:flex-row">
                <a href="#how">
                  <Button size="lg">
                    See How It Works
                    <ArrowRight className="h-4 w-4" />
                  </Button>
                </a>
                <a href="#demo">
                  <Button size="lg" variant="secondary">
                    Request a Demo
                  </Button>
                </a>
              </div>
              <p className="text-sm italic text-[var(--muted)]">
                Before AI tells you the story, make sure the data is correct.
              </p>
            </div>
          </FadeUp>
        </div>

        <FadeUp delay={0.3}>
          <TerminalAnimation />
        </FadeUp>
      </div>
    </section>
  );
}
