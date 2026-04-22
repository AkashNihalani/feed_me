"use client";

import * as React from "react";

import { cn } from "@/lib/utils";

type LiquidGlassVariant = "shell" | "header" | "nav";

export interface LiquidGlassProps extends React.HTMLAttributes<HTMLDivElement> {
  variant?: LiquidGlassVariant;
  compressed?: boolean;
}

export function LiquidGlass({
  children,
  className,
  compressed = false,
  style,
  variant = "shell",
  ...props
}: LiquidGlassProps) {
  return (
    <div
      className={cn(
        variant === "header"
          ? "fm-liquid-header"
          : variant === "nav"
            ? "fm-liquid-nav"
            : "fm-liquid-shell",
        variant === "header" && compressed && "fm-liquid-header--compressed",
        className,
      )}
      style={style}
      {...props}
    >
      {children}
    </div>
  );
}
