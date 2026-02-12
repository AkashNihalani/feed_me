import { cn } from "@/lib/utils";

interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  title?: string;
  action?: React.ReactNode;
}

export function Card({ className, title, action, children, ...props }: CardProps) {
  return (
    <div 
      className={cn(
        "neo-card p-6",
        className
      )}
      {...props}
    >
      {(title || action) && (
        <div className="flex items-center justify-between mb-4 border-b-3 border-black dark:border-white pb-2">
          {title && <h3 className="font-black text-xl uppercase italic">{title}</h3>}
          {action && <div>{action}</div>}
        </div>
      )}
      <div className="font-bold">
        {children}
      </div>
    </div>
  );
}
