import * as React from "react";
import { cn } from "@/lib/utils";

type ButtonVariant = "primary" | "secondary" | "ghost";
type ButtonSize = "default" | "lg";

const variantClasses: Record<ButtonVariant, string> = {
  primary:
    "bg-[var(--teal)] text-[var(--navy)] shadow-[0_0_24px_rgba(0,194,203,0.18)] hover:bg-[#40d7de]",
  secondary:
    "border border-[rgba(0,194,203,0.45)] bg-transparent text-[var(--teal)] hover:border-[var(--teal)] hover:bg-[rgba(0,194,203,0.08)]",
  ghost:
    "border border-white/10 bg-white/5 text-[var(--white)] hover:border-[var(--teal)] hover:bg-white/8",
};

const sizeClasses: Record<ButtonSize, string> = {
  default: "h-12 px-6 text-sm",
  lg: "h-14 px-8 text-base",
};

export interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  asChild?: boolean;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant = "primary", size = "default", ...props }, ref) => {
    return (
      <button
        ref={ref}
        className={cn(
          "inline-flex items-center justify-center gap-2 rounded-full font-semibold transition-all duration-200 ease-out hover:scale-[1.02] focus:outline-none focus:ring-2 focus:ring-[var(--teal)] focus:ring-offset-2 focus:ring-offset-[var(--navy)] disabled:pointer-events-none disabled:opacity-60",
          variantClasses[variant],
          sizeClasses[size],
          className,
        )}
        {...props}
      />
    );
  },
);

Button.displayName = "Button";
