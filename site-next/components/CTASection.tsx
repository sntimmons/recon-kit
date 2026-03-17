import { ArrowRight } from "lucide-react";
import { FadeUp } from "@/components/FadeUp";
import { Button } from "@/components/ui/button";

export function CTASection() {
  return (
    <section
      id="demo"
      className="section-pad relative overflow-hidden border-t border-white/8"
    >
      <div className="hero-grid absolute inset-0 opacity-60" />
      <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(0,194,203,0.28),rgba(10,22,40,0.95)_55%,rgba(10,22,40,1))]" />
      <div className="section-frame relative">
        <div className="mx-auto max-w-4xl space-y-8 text-center">
          <FadeUp>
            <h2 className="section-title text-[clamp(3rem,7vw,4rem)]">
              Ready to see what is in your data?
            </h2>
          </FadeUp>
          <FadeUp delay={0.1}>
            <p className="mx-auto max-w-2xl text-lg leading-8 text-[var(--muted)] md:text-xl">
              Upload your ADP and Workday exports. Get your corrections manifest
              and CHRO approval document in minutes.
            </p>
          </FadeUp>
          <FadeUp delay={0.2}>
            <div className="space-y-6">
              <a href="mailto:demo@datawhisperer.ai" className="inline-flex">
                <Button
                  size="lg"
                  className="bg-[var(--white)] px-10 text-[var(--navy)] hover:bg-white"
                >
                  Request a Demo
                  <ArrowRight className="h-4 w-4" />
                </Button>
              </a>
              <p className="text-sm italic text-[var(--muted)]">
                Before AI tells you the story, make sure the data is correct.
              </p>
            </div>
          </FadeUp>
          <FadeUp delay={0.3}>
            <p className="mono pt-10 text-xs uppercase tracking-[0.2em] text-white/55">
              Data Whisperer - HR Migration Reconciliation Engine
            </p>
          </FadeUp>
        </div>
      </div>
    </section>
  );
}
