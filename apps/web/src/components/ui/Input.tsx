import { cn } from "@/lib/utils";

type InputProps = React.InputHTMLAttributes<HTMLInputElement>;

export function Input({ className, ...props }: InputProps) {
  return (
    <input
      className={cn(
        "neo-input shadow-hard transition-all focus:translate-x-1 focus:translate-y-1 focus:shadow-none",
        className
      )}
      {...props}
    />
  );
}
