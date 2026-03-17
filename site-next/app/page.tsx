import { CTASection } from "@/components/CTASection";
import { FeaturesSection } from "@/components/FeaturesSection";
import { HeroSection } from "@/components/HeroSection";
import { HowItWorksSection } from "@/components/HowItWorksSection";
import { ProblemSection } from "@/components/ProblemSection";
import { WhoSection } from "@/components/WhoSection";

export default function Home() {
  return (
    <main className="page-shell">
      <HeroSection />
      <ProblemSection />
      <HowItWorksSection />
      <FeaturesSection />
      <WhoSection />
      <CTASection />
    </main>
  );
}
