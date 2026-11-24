/** Baton brand: the mark plus the word, drawn as text so it follows the theme. */
export const BRAND_NAME = "Baton";

export function BrandLogo({ size = "md", className = "" }: { size?: "sm" | "md" | "lg"; className?: string }) {
  const dims = { sm: "h-7 w-7", md: "h-9 w-9", lg: "h-14 w-14" }[size];
  const text = { sm: "text-lg", md: "text-[22px]", lg: "text-4xl" }[size];
  return (
    <span className={`inline-flex items-center gap-2.5 ${className}`}>
      <img src="/assets/baton_mark.svg" alt="" aria-hidden="true" className={`${dims} shrink-0 rounded-[28%] shadow-sm`} />
      <span className={`${text} font-semibold tracking-tight text-foreground`}>{BRAND_NAME}</span>
    </span>
  );
}
