update public.feeders
set profile_pic_url = 'https://unavatar.io/instagram/' || handle,
    updated_at = now()
where status = 'active'
  and verification_status = 'verified'
  and coalesce(profile_pic_url, '') = '';
