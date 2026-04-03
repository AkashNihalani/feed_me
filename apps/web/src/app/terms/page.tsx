export default function TermsPage() {
  return (
    <main className="min-h-screen bg-[#f4f7f9] px-6 py-12 text-[#111] dark:bg-[#030303] dark:text-white">
      <div className="mx-auto max-w-3xl">
        <div className="text-[11px] font-black uppercase tracking-[0.18em] text-foreground/45 dark:text-white/40">
          Feed Me
        </div>
        <h1 className="mt-3 text-4xl font-black tracking-[-0.05em]">Terms of Service</h1>
        <p className="mt-4 text-sm font-medium leading-7 text-foreground/72 dark:text-white/66">
          Feed Me provides subscription access to feed intelligence, tracking, and alerting tools. By using the product,
          you agree to use the service responsibly and keep your account information accurate.
        </p>
        <div className="mt-8 space-y-6 text-sm leading-7 text-foreground/72 dark:text-white/66">
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.12em] text-foreground dark:text-white">Subscription</h2>
            <p className="mt-2">Each Feed Pass expands feeder coverage. Pricing, included post limits, and overage policies are shown in-product at the time of billing.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.12em] text-foreground dark:text-white">Usage</h2>
            <p className="mt-2">You are responsible for the feeds and handles you add to your account and for complying with platform and local laws when using the service.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.12em] text-foreground dark:text-white">Billing Support</h2>
            <p className="mt-2">Questions about refunds, cancellations, and billing support can be directed to <a className="underline underline-offset-4" href="mailto:support@feedmemore.com">support@feedmemore.com</a>.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.12em] text-foreground dark:text-white">Availability</h2>
            <p className="mt-2">We may evolve product features, surfaces, and workflows over time to improve performance, reliability, and safety.</p>
          </section>
        </div>
      </div>
    </main>
  );
}
