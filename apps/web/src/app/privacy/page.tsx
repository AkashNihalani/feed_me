export default function PrivacyPage() {
  return (
    <main className="min-h-screen bg-[var(--fm-page)] px-6 py-12 text-[#111] dark:text-white">
      <div className="mx-auto max-w-3xl">
        <div className="text-[12px] font-black uppercase tracking-[0.14em] text-foreground/45 dark:text-white/40">
          Feed Me
        </div>
        <h1 className="mt-3 text-4xl font-black tracking-[-0.04em]">Privacy Policy</h1>
        <p className="mt-4 text-sm font-medium leading-7 text-foreground/72 dark:text-white/66">
          Feed Me stores the account, feed, feeder, billing, and notification information required to operate the product,
          deliver Fire alerts, and support subscription management. We use the minimum information necessary to run your
          workspace and process payments.
        </p>
        <div className="mt-8 space-y-6 text-sm leading-7 text-foreground/72 dark:text-white/66">
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.14em] text-foreground dark:text-white">What We Collect</h2>
            <p className="mt-2">Account details, feed configuration, feeder handles, notification preferences, and transaction records connected to your subscription.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.14em] text-foreground dark:text-white">How We Use It</h2>
            <p className="mt-2">We use this data to power discovery, Fire alerts, analytics, billing, customer support, and product reliability.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.14em] text-foreground dark:text-white">Payments</h2>
            <p className="mt-2">Payments are processed through our payment provider. Feed Me stores transaction metadata needed for confirmation, support, and reconciliation.</p>
          </section>
          <section>
            <h2 className="text-base font-black uppercase tracking-[0.14em] text-foreground dark:text-white">Support</h2>
            <p className="mt-2">For privacy questions or account requests, contact <a className="underline underline-offset-4" href="mailto:support@feedmemore.com">support@feedmemore.com</a>.</p>
          </section>
        </div>
      </div>
    </main>
  );
}
