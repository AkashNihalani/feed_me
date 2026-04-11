import { cn } from "@/lib/utils";

type InputProps = React.InputHTMLAttributes<HTMLInputElement>;

export function Input({ className, ...props }: InputProps) {
  return (
    <input
      className={cn(
        "fm-depth-inner w-full rounded-[16px] border border-black/8 bg-white/74 px-4 py-3 font-black text-foreground placeholder:text-foreground/34 transition-[background-color,border-color,box-shadow,transform,color] duration-200 outline-none",
        "shadow-[inset_0_1px_0_rgba(255,255,255,0.84),inset_0_2px_8px_rgba(15,23,42,0.04)]",
        "focus:border-[#E11D48]/26 focus:bg-white/84 focus:shadow-[inset_0_1px_0_rgba(255,255,255,0.92),inset_0_2px_8px_rgba(15,23,42,0.04),0_0_0_1px_rgba(225,29,72,0.1)]",
        "dark:border-white/8 dark:bg-white/[0.04] dark:placeholder:text-white/28",
        "dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_3px_10px_rgba(0,0,0,0.32)]",
        "dark:focus:border-[#E11D48]/34 dark:focus:bg-white/[0.06] dark:focus:shadow-[inset_0_1px_0_rgba(255,255,255,0.06),inset_0_3px_10px_rgba(0,0,0,0.28),0_0_0_1px_rgba(225,29,72,0.12)]",
        className
      )}
      {...props}
    />
  );
}
