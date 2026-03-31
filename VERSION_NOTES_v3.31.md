# Makii Reservation System — v3.31

**Release Date:** 2026-03-31  
**Status:** Stable / Production

## Features & Fixes in this Release

### Booking Flow
- **Same-day booking** — customers can now book for today (timezone-aware for MY)
- **3DS payment flow** — client-side Stripe 3D Secure confirmation with `confirmPayment()`
- **Add-on images fix** — corrected image display for course add-ons

### Staff Dashboard
- **Abandoned bookings tracking** — new "Abandoned" tab showing incomplete bookings
- **Cancelled bookings hidden by default** — cleaner default view for staff
- **Same-day postpone for staff** — staff can postpone reservations on the same day
- **Pax +/- in edit modal** — stepper control for adjusting party size
- **Course edit modal** — inline editing of course selections
- **Notification badge clearing** — badges clear properly on interaction
- **Abandoned badge clearing** — abandoned count badge resets correctly

### Course Management
- **Course reorder (up/down arrows)** — drag-free reordering of courses in admin
- **7-day availability checkboxes** — per-day-of-week course availability toggles

### Infrastructure
- **PM2 cleanup** — removed stale PM2 processes, single clean `makii-api` instance

## Backup Locations

| Asset | Location |
|-------|----------|
| Git tag | `v3.31` on `abigailmini/makii-reservation` |
| Backend code | `deploy@157.245.146.133:/home/deploy/backups/v3.31/makii-api/` |
| DB schema | `deploy@157.245.146.133:/home/deploy/backups/v3.31/schema.sql` |
| PM2 config | `deploy@157.245.146.133:/home/deploy/backups/v3.31/pm2-config.txt` |
| Frontend (reservation) | `SiteGround:www/makiisushi.com/public_html/backups/v3.31/reservation-index.html` |
| Frontend (staffview) | `SiteGround:www/makiisushi.com/public_html/backups/v3.31/staffview-index.html` |
