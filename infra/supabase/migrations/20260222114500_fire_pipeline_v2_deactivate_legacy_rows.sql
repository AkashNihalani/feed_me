begin;

update public.fire_alerts
set status = 'dismissed',
    updated_at = now()
where business_date_ist between date '2026-02-17' and date '2026-02-21'
  and status in ('new','sent')
  and (
    dedupe_key is null
    or dedupe_key not like 'v2:%'
    or signal_code in ('S0_slot_state','A1_baseline_displacement','A4_engagement_efficiency')
  );

commit;
