require('dotenv').config();
const http = require('http');
const fs = require('fs');
const nodePath = require('path');
const { Pool } = require('pg');
const { Resend } = require('resend');
const Busboy = require('busboy');

const UPLOAD_DIR = nodePath.join(__dirname, 'uploads');

let stripe = null;
if (process.env.STRIPE_SECRET_KEY) {
  try { stripe = require('stripe')(process.env.STRIPE_SECRET_KEY); }
  catch (e) { console.warn('Stripe not loaded:', e.message); }
}

const PORT = process.env.PORT || 3458;
const pool = new Pool({
  host: process.env.PG_HOST || process.env.PGHOST || 'localhost',
  port: parseInt(process.env.PG_PORT || process.env.PGPORT) || 5432,
  database: process.env.PG_DATABASE || process.env.PGDATABASE || 'makii_reservation',
  user: process.env.PG_USER || process.env.PGUSER || 'makii_app',
  password: process.env.PG_PASSWORD || process.env.PGPASSWORD,
  max: 10
});

const resend = process.env.RESEND_API_KEY ? new Resend(process.env.RESEND_API_KEY) : null;
const EMAIL_FROM = process.env.RESEND_FROM || 'reservation@makiisushi.com';
const EMAIL_REPLY_TO = process.env.RESEND_REPLY_TO || 'makiisushibar@gmail.com';
const BRAND = {
  bg: '#E8DCCA',
  gold: '#6B5A3E',
  dark: '#2C2418',
};

// ── Room Logic ──
const MAIN_ROOM_MAX = 10;
const COZY_ROOM_MAX = 5;
const TOTAL_SESSION_MAX = 15;
const CUSTOM_COURSES = ['Custom Selection'];

// ── Helpers ──
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => resolve(Buffer.concat(chunks).toString()));
    req.on('error', reject);
  });
}

function cors(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Bypass-Tunnel-Reminder');
  res.setHeader('Access-Control-Allow-Credentials', 'false');
}

function json(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}

function parseUrl(url) {
  const [path, qs] = url.split('?');
  const params = {};
  if (qs) qs.split('&').forEach(p => { const [k, v] = p.split('='); params[k] = decodeURIComponent(v || ''); });
  return { path, params };
}

// ── Booking Cutoff ──
function isSessionPastCutoff(date, sessionName) {
  const cutoffHours = { 'Lunch': 10, 'Dinner 1': 16, 'Dinner 2': 16 };
  const hour = cutoffHours[sessionName];
  if (hour == null) return false;
  const cutoff = new Date(`${date}T${String(hour).padStart(2,'0')}:00:00+08:00`);
  return new Date() >= cutoff;
}

// ── DB Query Helpers ──
async function q(text, params) { return pool.query(text, params); }
async function qRows(text, params) { return (await pool.query(text, params)).rows; }
async function qOne(text, params) { const r = await pool.query(text, params); return r.rows[0] || null; }


async function createStaffNotification(type, title, message, metadata = {}) {
  try {
    await q(
      'INSERT INTO notifications (type, title, message, metadata, read, created_at) VALUES ($1,$2,$3,$4,false,NOW())',
      [type, title, message, JSON.stringify(metadata || {})]
    );
  } catch (e) {
    console.error('[NOTIFICATION] create failed:', e.message);
  }
}

function formatReservationDate(dateStr) {
  try {
    const d = new Date(`${dateStr}T12:00:00+08:00`);
    return new Intl.DateTimeFormat('en-MY', {
      weekday: 'long',
      day: '2-digit',
      month: 'long',
      year: 'numeric',
      timeZone: 'Asia/Kuala_Lumpur'
    }).format(d);
  } catch (_) {
    return dateStr;
  }
}

function reservationEmailHtml(kind, booking) {
  const titles = {
    confirmation: 'Reservation Confirmed',
    reminder: 'Reservation Reminder',
    cancellation: 'Reservation Cancelled',
    postpone: 'Reservation Rescheduled',
  };
  const intros = {
    confirmation: 'We look forward to welcoming you for an unforgettable evening.',
    reminder: 'This is a friendly reminder that your reservation is tomorrow. We look forward to seeing you.',
    cancellation: 'Your reservation has been cancelled. If you believe this is an error, please contact us via WhatsApp at <a href="https://wa.me/60167611931" style="color:#6B5A3E">+60 16-761 1931</a>.',
    postpone: `Your reservation has been rescheduled to <strong>${formatReservationDate(booking.newDate || booking.date)}</strong> at <strong>${booking.newSession || booking.session || '-'}</strong>. If you have any questions, please contact us via WhatsApp at <a href="https://wa.me/60167611931" style="color:#6B5A3E">+60 16-761 1931</a>.`,
  };
  const title = titles[kind] || 'Reservation Confirmed';
  const intro = intros[kind] || intros.confirmation;

  const sessionTime = booking.sessionTime || booking.session_time || null;
  const sessionDisplay = sessionTime
    ? `${booking.session || '-'} (${sessionTime})`
    : booking.session || '-';
  const paxDisplay = booking.pax ? `${booking.pax} pax` : '-';
  const courseDisplay = booking.course || '-';
  const depositAmount = booking.depositAmount ?? booking.deposit_amount ?? booking.deposit ?? (booking.pax ? booking.pax * 100 : null);
  const depositDisplay = depositAmount === null || depositAmount === undefined ? '-' : `RM${Number(depositAmount).toLocaleString('en-MY')}`;

  const rowSep = '<tr><td colspan="2" style="border-top:1px solid #F0EAE2"></td></tr>';
  const makeRow = (label, value) => `<tr><td style="padding:20px 0;color:#A89880;font-size:11px;letter-spacing:2px;text-transform:uppercase;width:120px;vertical-align:top">${label}</td><td style="padding:20px 0;color:#2C2418;font-size:15px;text-align:right">${value}</td></tr>`;

  let rows;
  if (kind === 'cancellation') {
    rows = [
      makeRow('Date', formatReservationDate(booking.date)),
      makeRow('Session', sessionDisplay),
      makeRow('Guests', paxDisplay),
      makeRow('Course', courseDisplay),
    ].join(rowSep);
  } else if (kind === 'postpone') {
    const origDate = booking.originalDate || booking.date;
    const origSession = booking.originalSession || booking.session;
    rows = [
      makeRow('Original Date', formatReservationDate(origDate)),
      makeRow('Original Session', origSession || '-'),
      makeRow('New Date', formatReservationDate(booking.newDate || booking.date)),
      makeRow('New Session', booking.newSession || booking.session || '-'),
      makeRow('Guests', paxDisplay),
      makeRow('Course', courseDisplay),
    ].join(rowSep);
  } else {
    rows = [
      makeRow('Date', formatReservationDate(booking.date)),
      makeRow('Session', sessionDisplay),
      makeRow('Guests', paxDisplay),
      makeRow('Course', courseDisplay),
      makeRow('Card Guarantee', depositDisplay),
    ].join(rowSep);
  }

  const footerText = kind === 'cancellation'
    ? 'If you have any questions, please reach out to us via WhatsApp or email.'
    : kind === 'postpone'
    ? 'If you have any questions about your rescheduled reservation, please contact us.'
    : 'For changes or cancellations, please contact us at least 24 hours in advance.';

  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head><body style="margin:0;padding:0;background:#F5EDE3;font-family:Georgia,Times,serif"><table width="100%" cellpadding="0" cellspacing="0" style="background:#F5EDE3;padding:32px 16px"><tr><td align="center"><table width="540" cellpadding="0" cellspacing="0" style="max-width:540px;width:100%;background:#FFFBF6;border-radius:8px;overflow:hidden"><tr><td style="background:#2C2418;padding:48px 40px;text-align:center"><img src="https://makiisushi.com/reservation/images/logo-email.png" width="260" alt="Fusion Omakase by Makii Sushi" style="display:block;margin:0 auto;width:260px;height:auto"></td></tr><tr><td style="padding:44px 48px 12px;text-align:center"><p style="margin:0;font-size:11px;letter-spacing:3px;color:#A89880;text-transform:uppercase">${title}</p></td></tr><tr><td style="padding:24px 48px 36px;text-align:center"><p style="margin:0;font-size:16px;line-height:1.8;color:#3D3225">${intro}</p></td></tr><tr><td style="padding:0 48px"><table width="100%" cellpadding="0" cellspacing="0" style="border-top:1px solid #E5DDD2">${rows}</table></td></tr><tr><td style="padding:36px 48px 40px;text-align:center"><p style="margin:0 0 6px;font-size:13px;color:#3D3225;font-weight:bold">Fusion Omakase by Makii Sushi</p><p style="margin:0;font-size:13px;line-height:1.8;color:#8A7A64">E2-0-21, Jalan 1/152, Taman OUG Parklane<br>Kuala Lumpur 58200</p><p style="margin:12px 0 0;font-size:13px;color:#8A7A64">+60 16-761 1931 · makiisushibar@gmail.com</p></td></tr><tr><td style="background:#2C2418;padding:18px 40px;text-align:center"><p style="margin:0;font-size:10px;letter-spacing:1px;color:#D4C4A8">${footerText}</p></td></tr></table></td></tr></table></body></html>`;
}

async function sendReservationEmail(kind, booking) {
  if (!resend) {
    console.warn('[EMAIL] Resend not configured; skipped', kind, booking?.email);
    return { ok: false, reason: 'resend_not_configured' };
  }
  if (!booking?.email) return { ok: false, reason: 'missing_email' };

  const subjects = {
    confirmation: `Reservation Confirmed: Fusion Omakase on ${booking.date}`,
    reminder: `Reminder: Your Fusion Omakase reservation on ${booking.date}`,
    cancellation: `Reservation Cancelled: Fusion Omakase on ${booking.date}`,
    postpone: `Reservation Rescheduled: Fusion Omakase to ${booking.newDate || booking.date}`,
  };
  const subject = subjects[kind] || subjects.confirmation;

  try {
    const rsp = await resend.emails.send({
      from: EMAIL_FROM,
      to: booking.email,
      replyTo: EMAIL_REPLY_TO,
      subject,
      html: reservationEmailHtml(kind, booking),
    });
    const msgId = rsp?.data?.id || null;
    console.log(`[EMAIL] ${kind} sent to ${booking.email} (${msgId || 'no-id'})`);
    return { ok: true, messageId: msgId };
  } catch (e) {
    console.error(`[EMAIL] ${kind} failed for ${booking.email}:`, e.message);
    return { ok: false, reason: e.message };
  }
}

async function notifyCustomer(type, booking) {
  if (type === 'booking-confirmation') {
    const sent = await sendReservationEmail('confirmation', booking);
    if (sent.ok && booking.id) {
      await q(
        'UPDATE reservations SET confirmation_sent_at = NOW(), confirmation_message_id = $1 WHERE id = $2',
        [sent.messageId, booking.id]
      );
    }
    return;
  }

  if (type === 'booking-cancelled') {
    // DISABLED — cancel/postpone emails paused until ID-based lookup is stable
    // const sent = await sendReservationEmail('cancellation', booking);
    // console.log(`[CUSTOMER NOTIFY] cancellation email ${sent.ok ? 'sent' : 'failed'}: ${booking.name} - ${booking.email}`);
    console.log(`[CUSTOMER NOTIFY] cancellation email SKIPPED (disabled): ${booking.name} - ${booking.email}`);
    return;
  }

  if (type === 'booking-postponed') {
    // DISABLED — cancel/postpone emails paused until ID-based lookup is stable
    // const sent = await sendReservationEmail('postpone', booking);
    // console.log(`[CUSTOMER NOTIFY] postpone email ${sent.ok ? 'sent' : 'failed'}: ${booking.name} - ${booking.email}`);
    console.log(`[CUSTOMER NOTIFY] postpone email SKIPPED (disabled): ${booking.name} - ${booking.email}`);
    return;
  }

  console.log(`[CUSTOMER NOTIFY] ${type}: ${booking.name} - ${booking.email} - ${booking.phone}`);
}

async function scheduleFeedbackSurvey(booking) {
  // TODO: trigger feedback survey X hours after dinner
  console.log(`[CUSTOMER NOTIFY] feedback-survey-placeholder: ${booking.name} - ${booking.email} - ${booking.phone}`);
}

// ── Config ──
async function getConfig() {
  const sessions = (await qRows("SELECT name, start_time, end_time FROM sessions"))
    .map(s => ({ 'Session Name': s.name, 'Start Time': s.start_time, 'End Time': s.end_time, 'Status': 'active' }));
  const blocked = (await qRows('SELECT date, reason FROM blocked_dates'))
    .map(b => ({ 'Blocked Date': b.date, 'Reason': b.reason }));
  const courses = (await qRows("SELECT id, course, price, description, start_date, end_date FROM config WHERE status='active' ORDER BY sort_order ASC, id ASC"))
    .map(c => ({ 'Course': c.course, 'Price (RM)': c.price, 'Status': 'active' }));
  return { sessions, blocked, courses };
}

// ── Locked ──
async function getLockedEntries(date) {
  return (await qRows('SELECT * FROM locked WHERE date = $1', [date]))
    .map(l => ({ Date: l.date, Session: l.session, Reason: l.reason, 'Locked By': l.locked_by, 'Locked At': l.locked_at }));
}

function isSessionLocked(locks, sessionName) {
  return locks.some(l => l.Session === 'all' || l.Session === sessionName);
}

// ── Availability ──
async function getAvailability(date) {
  const config = await getConfig();
  const blockedEntry = config.blocked.find(b => b['Blocked Date'] === date);
  if (blockedEntry) return { date, blocked: true, reason: blockedEntry['Reason'] || 'Blocked', sessions: [] };

  const locks = await getLockedEntries(date);
  const reservations = await qRows("SELECT * FROM reservations WHERE date = $1 AND status IN ('confirmed', 'manual')", [date]);

  const sessions = config.sessions.map(s => {
    const locked = isSessionLocked(locks, s['Session Name']);
    const pastCutoff = isSessionPastCutoff(date, s['Session Name']);
    const booked = reservations.filter(r => r.session === s['Session Name']);
    const totalPax = booked.reduce((sum, r) => sum + (r.pax || 0), 0);
    const maxPax = TOTAL_SESSION_MAX;
    let mainUsed = 0, cozyUsed = 0;
    booked.forEach(r => { const p = r.pax || 0; if (CUSTOM_COURSES.includes(r.course)) cozyUsed += p; else mainUsed += p; });
    const closed = locked || pastCutoff;
    return {
      name: s['Session Name'], startTime: s['Start Time'], endTime: s['End Time'],
      maxPax, bookedPax: totalPax,
      remaining: closed ? 0 : Math.max(0, maxPax - totalPax),
      available: closed ? false : totalPax < maxPax,
      locked: closed,
      mainRoom: { used: mainUsed, max: MAIN_ROOM_MAX },
      cozyRoom: { used: cozyUsed, max: COZY_ROOM_MAX }
    };
  });

  const dayOfWeek = new Date(date + "T12:00:00").getDay();
  const courseRows = await qRows("SELECT id, course, price, description, start_date, end_date, day_availability FROM config WHERE status='active' AND (start_date IS NULL OR start_date <= $1) AND (end_date IS NULL OR end_date >= $2) ORDER BY sort_order ASC, id ASC", [date, date]);
  const courses = courseRows
    .filter(c => { const da = c.day_availability || [0,1,2,3,4,5,6]; return da.includes(dayOfWeek); })
    .map(c => ({ name: c.course, price: c.price, description: c.description || '' }));

  const addonRows = await qRows("SELECT id, name, price, description, image_url, sort_order, display_row, start_date, end_date FROM addons WHERE status='active' AND (start_date IS NULL OR start_date <= $1) AND (end_date IS NULL OR end_date >= $2) ORDER BY sort_order ASC, id ASC", [date, date]);
  const addons = addonRows.map(a => ({ name: a.name, price: a.price, description: a.description || '', image_url: a.image_url || '', display_row: a.display_row || 'top' }));

  return { date, blocked: false, sessions, courses, addons };
}

// ── Reserve ──
async function makeReservation(data, createHold = false) {
  const { date, session, course, name, phone, email, pax, notes, payment_method, return_url, guestNames, allergy, special_occasion, occasion, addons, confirmed_payment_intent } = data;
  if (!date || !session || !course || !name || !phone || !email || !pax)
    return { error: 'Missing required fields: date, session, course, name, phone, email, pax' };

  const numPax = parseInt(pax);
  if (isNaN(numPax) || numPax < 1 || numPax > 15) return { error: 'Pax must be between 1 and 15' };

  const avail = await getAvailability(date);
  if (avail.blocked) return { error: `Restaurant is closed on ${date}: ${avail.reason}` };

  const sessionInfo = avail.sessions.find(s => s.name === session);
  if (!sessionInfo) return { error: `Invalid session: ${session}` };
  if (sessionInfo.locked) return { error: 'This session is currently fully booked.' };
  if (isSessionPastCutoff(date, session)) return { error: 'Booking for this session has closed.' };

  const now = new Date().toISOString();
  let piId = '';
  if (createHold) {
    if (!stripe) return { error: 'Stripe not configured' };
    try {
      // Create PI with manual capture — do NOT confirm server-side
      // Frontend will call stripe.confirmCardPayment() to trigger 3DS natively
      const intent = await stripe.paymentIntents.create({
        amount: numPax * 10000, currency: 'myr', capture_method: 'manual',
        payment_method_types: ['card'],
        payment_method_options: { card: { request_three_d_secure: 'any' } },
        payment_method: payment_method || undefined,
        metadata: { restaurant: 'Fusion Omakase by Makii Sushi', date, session, course, name, email, phone }
      });
      console.log('[STRIPE] PI created (not confirmed): ' + intent.id);
      // Return client_secret so frontend can confirm with 3DS
      return {
        requires_confirmation: true,
        client_secret: intent.client_secret,
        payment_intent: intent.id,
        // Pass booking data back so frontend can send it to /book/complete
        booking_data: { date, session, course, name, phone, email, pax: numPax, notes, guestNames, allergy, special_occasion: special_occasion || occasion || '', addons }
      };
    } catch (e) {
      console.error('Stripe error:', e.message);
      await createStaffNotification(
        'payment_failure',
        '⚠️ Payment guarantee failed',
        `${new Date().toLocaleString('en-MY')} · ${name} (${phone}) · ${session} ${date}`,
        { date, session, course, name, phone, email, pax: numPax, error: e.message }
      );
      return { error: 'Payment authorization failed: ' + e.message };
    }
  }

  const guestNamesStr = guestNames || '';
  const allergyStr = allergy || '';
  const specialOccasionStr = special_occasion || occasion || '';
  const addonsStr = addons || '';

  if (sessionInfo.remaining >= numPax) {
    const reservation = await qOne(
      'INSERT INTO reservations (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, edited_at, edited_by, room, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id, date, session, course, name, phone, email, pax, created_at',
      [date, session, course, name, phone, email, numPax, 'confirmed', notes || '', now, piId, guestNamesStr, allergyStr, specialOccasionStr, '', '', '', addonsStr]
    );
    console.log(`[BOOKING] ${name} (${numPax}pax) → ${course} @ ${session} on ${date} — CONFIRMED${piId ? (' (PI:'+piId+')') : ''}`);
    await createStaffNotification(
      'new_reservation',
      '✅ New reservation confirmed',
      `${new Date().toLocaleString('en-MY')} · ${date} ${session} · ${numPax} pax · ${course}`,
      { date, session, course, name, phone, email, pax: numPax, payment_intent: piId }
    );
    await notifyCustomer('booking-confirmation', { ...reservation, payment_intent: piId });
    await scheduleFeedbackSurvey({ ...reservation, payment_intent: piId });
    return { status: 'confirmed', date, session, course, name, pax: numPax, payment_intent: piId };
  } else {
    await q('INSERT INTO waitlist (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
      [date, session, course, name, phone, email, numPax, 'waitlist', notes || '', now, piId, guestNamesStr, allergyStr, specialOccasionStr, addonsStr]);
    console.log(`[BOOKING] ${name} (${numPax}pax) → ${session} on ${date} — WAITLISTED`);
    return { status: 'waitlist', date, session, name, pax: numPax, message: 'Session is full. You have been added to the waitlist.', payment_intent: piId };
  }
}

async function capturePaymentIntent(piId) { if (!stripe) throw new Error('Stripe not configured'); return stripe.paymentIntents.capture(piId); }
async function cancelPaymentIntent(piId) { if (!stripe) throw new Error('Stripe not configured'); return stripe.paymentIntents.cancel(piId); }

// ── Server ──
const server = http.createServer(async (req, res) => {
  cors(req, res);
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  const { path, params } = parseUrl(req.url);

  try {
    if (path === '/health') return json(res, 200, { status: 'ok', service: 'makii-reservation-api' });

    // ── Public Menu/Courses API ──
    if (req.method === 'GET' && (path === '/api/menu' || path === '/api/courses')) {
      const today = params.date || new Date().toISOString().slice(0, 10);
      const rows = await qRows("SELECT id, course, price, description, start_date, end_date, sort_order, status, day_availability FROM config WHERE status='active' ORDER BY sort_order ASC, id ASC");

      const mapped = rows.map(c => {
        const startDate = c.start_date || null;
        const endDate = c.end_date || null;
        const currentlyActive = (!startDate || startDate <= today) && (!endDate || endDate >= today);
        return {
          id: c.id,
          name: c.course,
          price: Number(c.price || 0),
          description: c.description || '',
          is_active: currentlyActive,
          start_date: startDate,
          end_date: endDate,
          day_availability: c.day_availability || [0,1,2,3,4,5,6]
        };
      });

      const courses = params.all === '1' ? mapped : mapped.filter(c => c.is_active);
      return json(res, 200, { date: today, count: courses.length, courses });
    }

    // ── Courses CRUD ──
    if (path === '/config/courses' && req.method === 'GET') {
      const rows = params.date
        ? await qRows("SELECT id, course, price, description, start_date, end_date, day_availability FROM config WHERE status='active' AND (start_date IS NULL OR start_date <= $1) AND (end_date IS NULL OR end_date >= $2) ORDER BY sort_order ASC, id ASC", [params.date, params.date])
        : await qRows("SELECT id, course, price, description, start_date, end_date, day_availability FROM config WHERE status='active' ORDER BY sort_order ASC, id ASC");
      return json(res, 200, rows.map(c => ({ id: c.id, name: c.course, price: c.price, description: c.description || '', start_date: c.start_date || null, end_date: c.end_date || null, day_availability: c.day_availability || [0,1,2,3,4,5,6] })));
    }
    if (req.method === 'POST' && path === '/config/courses') {
      const body = JSON.parse(await readBody(req));
      const dayAvail = Array.isArray(body.day_availability) ? JSON.stringify(body.day_availability) : '[0,1,2,3,4,5,6]';
      await q("INSERT INTO config (course, price, description, start_date, end_date, status, day_availability) VALUES ($1,$2,$3,$4,$5,'active',$6::jsonb)", [body.name, body.price||0, body.description||'', body.start_date||null, body.end_date||null, dayAvail]);
      return json(res, 200, { ok: true });
    }
    if (req.method === 'PUT' && path.match(/^\/config\/courses\/\d+$/)) {
      const id = path.split('/').pop();
      const body = JSON.parse(await readBody(req));
      const dayAvail = Array.isArray(body.day_availability) ? JSON.stringify(body.day_availability) : '[0,1,2,3,4,5,6]';
      await q("UPDATE config SET course=$1, price=$2, description=$3, start_date=$4, end_date=$5, day_availability=$6::jsonb WHERE id=$7", [body.name, body.price||0, body.description||'', body.start_date||null, body.end_date||null, dayAvail, id]);
      return json(res, 200, { ok: true });
    }
    if (req.method === 'DELETE' && path.startsWith('/config/courses/')) {
      const id = path.split('/').pop();
      await q("DELETE FROM config WHERE id=$1", [id]);
      return json(res, 200, { ok: true });
    }

    // ── Addons CRUD ──
    if (path === '/config/addons' && req.method === 'GET') {
      const rows = params.date
        ? await qRows("SELECT id, name, price, description, image_url, sort_order, display_row, start_date, end_date FROM addons WHERE status='active' AND (start_date IS NULL OR start_date <= $1) AND (end_date IS NULL OR end_date >= $2) ORDER BY sort_order ASC, id ASC", [params.date, params.date])
        : await qRows("SELECT id, name, price, description, image_url, sort_order, display_row, start_date, end_date FROM addons WHERE status='active' ORDER BY sort_order ASC, id ASC");
      return json(res, 200, rows.map(a => ({ id: a.id, name: a.name, price: a.price, description: a.description || '', image_url: a.image_url || '', sort_order: a.sort_order || 0, display_row: a.display_row || 'top', start_date: a.start_date || null, end_date: a.end_date || null })));
    }
    if (req.method === 'POST' && path === '/config/addons') {
      const body = JSON.parse(await readBody(req));
      await q("INSERT INTO addons (name, price, description, start_date, end_date, sort_order, display_row, status) VALUES ($1,$2,$3,$4,$5,$6,$7,'active')", [body.name, body.price||0, body.description||'', body.start_date||null, body.end_date||null, body.sort_order||0, body.display_row||'top']);
      return json(res, 200, { ok: true });
    }
    if (req.method === 'PUT' && path.match(/^\/config\/addons\/\d+$/)) {
      const id = path.split('/').pop();
      const body = JSON.parse(await readBody(req));
      await q("UPDATE addons SET name=$1, price=$2, description=$3, start_date=$4, end_date=$5, sort_order=$6, display_row=$7 WHERE id=$8", [body.name, body.price||0, body.description||'', body.start_date||null, body.end_date||null, body.sort_order||0, body.display_row||'top', id]);
      return json(res, 200, { ok: true });
    }
    if (req.method === 'DELETE' && path.startsWith('/config/addons/') && !path.includes('/image')) {
      const id = path.split('/').pop();
      await q("DELETE FROM addons WHERE id=$1", [id]);
      return json(res, 200, { ok: true });
    }

    // ── Addon Image Upload (multipart/form-data via busboy) ──
    if (req.method === 'POST' && path.match(/^\/config\/addons\/\d+\/image$/)) {
      const id = path.split('/')[3];
      return new Promise((resolve) => {
        try {
          const bb = Busboy({ headers: req.headers });
          let savedFile = null;
          bb.on('file', (fieldname, file, info) => {
            const ext = (info.mimeType || '').includes('png') ? '.png' : '.jpeg';
            const fname = `addon-${id}-${Date.now()}${ext}`;
            if (!fs.existsSync(UPLOAD_DIR)) fs.mkdirSync(UPLOAD_DIR, { recursive: true });
            const fpath = nodePath.join(UPLOAD_DIR, fname);
            const ws = fs.createWriteStream(fpath);
            file.pipe(ws);
            savedFile = `/uploads/${fname}`;
            ws.on('finish', () => {});
          });
          bb.on('finish', async () => {
            if (savedFile) {
              // Remove old image file if exists
              const old = await qOne("SELECT image_url FROM addons WHERE id=$1", [id]);
              if (old && old.image_url) {
                const oldPath = nodePath.join(__dirname, old.image_url);
                try { fs.unlinkSync(oldPath); } catch(e) {}
              }
              await q("UPDATE addons SET image_url=$1 WHERE id=$2", [savedFile, id]);
              resolve(json(res, 200, { ok: true, image_url: savedFile }));
            } else {
              resolve(json(res, 400, { error: 'No file uploaded' }));
            }
          });
          bb.on('error', (err) => {
            console.error('Upload error:', err);
            resolve(json(res, 500, { error: 'Upload failed' }));
          });
          req.pipe(bb);
        } catch(err) {
          console.error('Upload error:', err);
          resolve(json(res, 500, { error: 'Upload failed' }));
        }
      });
    }

    // ── Addon Image Delete ──
    if (req.method === 'DELETE' && path.match(/^\/config\/addons\/\d+\/image$/)) {
      const id = path.split('/')[3];
      const old = await qOne("SELECT image_url FROM addons WHERE id=$1", [id]);
      if (old && old.image_url) {
        const oldPath = nodePath.join(__dirname, old.image_url);
        try { fs.unlinkSync(oldPath); } catch(e) {}
      }
      await q("UPDATE addons SET image_url='' WHERE id=$1", [id]);
      return json(res, 200, { ok: true });
    }

    // ── Course Reorder ──
    if (req.method === 'PUT' && path === '/config/courses/reorder') {
      const body = JSON.parse(await readBody(req));
      if (body.order && Array.isArray(body.order)) {
        for (let i = 0; i < body.order.length; i++) {
          await q("UPDATE config SET sort_order=$1 WHERE id=$2", [i, body.order[i]]);
        }
      }
      return json(res, 200, { ok: true });
    }

    // ── Addon Reorder ──
    if (req.method === 'PUT' && path === '/config/addons/reorder') {
      const body = JSON.parse(await readBody(req));
      if (body.order && Array.isArray(body.order)) {
        for (let i = 0; i < body.order.length; i++) {
          await q("UPDATE addons SET sort_order=$1 WHERE id=$2", [i, body.order[i]]);
        }
      }
      return json(res, 200, { ok: true });
    }

    // ── Serve uploaded files ──
    if (req.method === 'GET' && path.startsWith('/uploads/')) {
      const filePath = nodePath.join(__dirname, path);
      if (fs.existsSync(filePath)) {
        const ext = nodePath.extname(filePath).toLowerCase();
        const mimeTypes = { '.jpeg': 'image/jpeg', '.jpg': 'image/jpeg', '.png': 'image/png', '.gif': 'image/gif', '.webp': 'image/webp' };
        res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'application/octet-stream', 'Cache-Control': 'public, max-age=86400' });
        fs.createReadStream(filePath).pipe(res);
        return;
      }
      return json(res, 404, { error: 'Not found' });
    }

    if (path === '/config') return json(res, 200, await getConfig());
    if (path === '/stripe-key') return json(res, 200, { publishableKey: process.env.STRIPE_PUBLISHABLE_KEY || '' });

    if ((path === '/availability' || path === '/slots') && params.date) {
      return json(res, 200, await getAvailability(params.date));
    }

    if (req.method === 'POST' && path === '/book') {
      const body = JSON.parse(await readBody(req));
      const result = await makeReservation(body, !!body.createHold);
      return json(res, result.error ? 400 : 200, result);
    }

    // ── Complete booking after frontend 3DS confirmation ──
    if (req.method === 'POST' && path === '/book/complete') {
      const body = JSON.parse(await readBody(req));
      const { payment_intent: piId, date, session, course, name, phone, email, pax, notes, guestNames, allergy, special_occasion, occasion, addons } = body;
      if (!piId) return json(res, 400, { error: 'payment_intent required' });
      if (!date || !session || !course || !name || !phone || !email || !pax)
        return json(res, 400, { error: 'Missing required booking fields' });

      // Verify PI status with Stripe
      if (!stripe) return json(res, 500, { error: 'Stripe not configured' });
      try {
        const pi = await stripe.paymentIntents.retrieve(piId);
        if (pi.status !== 'requires_capture') {
          console.error(`[BOOK/COMPLETE] PI ${piId} status is ${pi.status}, expected requires_capture`);
          return json(res, 400, { error: `Payment not ready. Status: ${pi.status}` });
        }
      } catch (e) {
        console.error(`[BOOK/COMPLETE] Failed to verify PI ${piId}:`, e.message);
        return json(res, 400, { error: 'Could not verify payment: ' + e.message });
      }

      // Check availability
      const numPax = parseInt(pax);
      const avail = await getAvailability(date);
      if (avail.blocked) return json(res, 400, { error: `Restaurant is closed on ${date}` });
      const sessionInfo = avail.sessions.find(s => s.name === session);
      if (!sessionInfo) return json(res, 400, { error: `Invalid session: ${session}` });

      const now = new Date().toISOString();
      const guestNamesStr = guestNames || '';
      const allergyStr = allergy || '';
      const specialOccasionStr = special_occasion || occasion || '';
      const addonsStr = addons || '';

      if (sessionInfo.remaining >= numPax) {
        const reservation = await qOne(
          'INSERT INTO reservations (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, edited_at, edited_by, room, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18) RETURNING id, date, session, course, name, phone, email, pax, created_at',
          [date, session, course, name, phone, email, numPax, 'confirmed', notes || '', now, piId, guestNamesStr, allergyStr, specialOccasionStr, '', '', '', addonsStr]
        );
        console.log(`[BOOK/COMPLETE] ${name} (${numPax}pax) → ${course} @ ${session} on ${date} — CONFIRMED (PI:${piId})`);
        // Mark any matching partial booking as converted
        try {
          await q(`UPDATE partial_bookings SET status='converted', payment_intent=$1 WHERE status='partial' AND email=$2 AND date=$3 AND session=$4`, [piId, email, date, session]);
        } catch(e) { console.error('[PARTIAL] Convert update failed:', e.message); }
        await createStaffNotification(
          'new_reservation',
          '✅ New reservation confirmed',
          `${new Date().toLocaleString('en-MY')} · ${date} ${session} · ${numPax} pax · ${course}`,
          { date, session, course, name, phone, email, pax: numPax, payment_intent: piId }
        );
        await notifyCustomer('booking-confirmation', { ...reservation, payment_intent: piId });
        return json(res, 200, { status: 'confirmed', date, session, course, name, pax: numPax, payment_intent: piId });
      } else {
        await q('INSERT INTO waitlist (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
          [date, session, course, name, phone, email, numPax, 'waitlist', notes || '', now, piId, guestNamesStr, allergyStr, specialOccasionStr, addonsStr]);
        console.log(`[BOOK/COMPLETE] ${name} (${numPax}pax) → ${session} on ${date} — WAITLISTED`);
        return json(res, 200, { status: 'waitlist', message: 'Session is full. Added to waitlist.', payment_intent: piId });
      }
    }

    if (req.method === 'POST' && path === '/reserve') {
      const body = JSON.parse(await readBody(req));
      const result = await makeReservation(body, false);
      return json(res, result.error ? 400 : 200, result);
    }

    if (req.method === 'POST' && path === '/waitlist') {
      const body = JSON.parse(await readBody(req));
      const now = new Date().toISOString();
      await q('INSERT INTO waitlist (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
        [body.date, body.session, body.course||'', body.name, body.phone, body.email, parseInt(body.pax)||1, 'waitlist', body.notes||'', now, '', body.guestNames||'', body.allergy||'', body.special_occasion||body.occasion||'', body.addons||'']);
      return json(res, 200, { status: 'waitlist', message: 'Added to waitlist' });
    }

    if (path === '/reservations' && params.date) {
      const rows = await qRows('SELECT * FROM reservations WHERE date = $1', [params.date]);
      return json(res, 200, { date: params.date, reservations: rows.map(r => ({
        'Date': r.date, 'Session': r.session, 'Course': r.course, 'Name': r.name,
        'Phone': r.phone, 'Email': r.email, 'Pax': String(r.pax), 'Status': r.status,
        'Notes': r.notes, 'Created At': r.created_at, 'PaymentIntent': r.payment_intent,
        'GuestNames': r.guest_names, 'Allergy': r.allergy, 'Special Occasion': r.special_occasion
      }))});
    }

    // ── Staff Dashboard ──
    if (path === '/staff/reservations' && params.date) {
      const rows = await qRows('SELECT * FROM reservations WHERE date = $1', [params.date]);
      return json(res, 200, { date: params.date, reservations: rows.map(r => ({
        id: r.id,
        date: r.date||'',
        name: r.name||'', phone: r.phone||'', email: r.email||'', pax: String(r.pax||1),
        session: r.session||'', course: (r.course||'').replace(/\n/g, ', '),
        notes: r.notes||'', status: r.status||'', paymentIntent: r.payment_intent||'',
        paymentMethodId: r.payment_method_id||'', setupIntentId: r.setup_intent_id||'',
        createdAt: r.created_at||'', guestNames: r.guest_names||'', allergy: r.allergy||'', specialOccasion: r.special_occasion||'', addons: r.addons||''
      }))});
    }

    if (req.method === 'GET' && (path === '/api/bookings/search' || path === '/staff/bookings/search')) {
      const qText = (params.q || '').trim();
      const dateFilter = (params.date || '').trim();
      const queryNoPrefix = qText.replace(/^BKG-/i, '').trim();

      if (dateFilter && !/^\d{4}-\d{2}-\d{2}$/.test(dateFilter)) {
        return json(res, 400, { error: 'date must be in YYYY-MM-DD format' });
      }

      const sql = `
        SELECT id, name, phone, email, date, session, course, pax, status, notes, allergy, special_occasion, payment_intent, created_at
        FROM reservations
        WHERE ($1::text = '' OR date = $1::text)
          AND (
            $2::text = ''
            OR name ILIKE $3
            OR phone ILIKE $3
            OR COALESCE(email, '') ILIKE $3
            OR CAST(id AS text) ILIKE $4
          )
        ORDER BY date DESC, created_at DESC
        LIMIT 120
      `;
      const like = `%${qText}%`;
      const idLike = `%${queryNoPrefix}%`;
      const rows = await qRows(sql, [dateFilter, qText, like, idLike]);

      return json(res, 200, {
        query: qText,
        date: dateFilter || null,
        count: rows.length,
        bookings: rows.map(r => {
          const statusRaw = (r.status || '').toLowerCase();
          const hasHold = !!(r.payment_intent && String(r.payment_intent).trim());
          const paymentStatus = statusRaw.startsWith('charged')
            ? 'paid'
            : (hasHold ? 'hold' : 'pending');

          return {
            id: r.id,
            reference: `BKG-${r.id}`,
            customerName: r.name || '',
            phone: r.phone || '',
            email: r.email || '',
            bookingDate: r.date || '',
            session: r.session || '',
            course: (r.course || '').replace(/\n/g, ', '),
            partySize: Number(r.pax || 0),
            dietaryRestrictions: r.allergy || '',
            allergies: r.allergy || '',
            specialRequests: r.notes || '',
            specialOccasion: r.special_occasion || '',
            notes: r.notes || '',
            paymentStatus,
            bookingStatus: r.status || '',
            createdAt: r.created_at || ''
          };
        })
      });
    }

    if (req.method === 'POST' && path === '/staff/lock') {
      const body = JSON.parse(await readBody(req));
      const { date, session, reason } = body;
      if (!date) return json(res, 400, { error: 'date required' });
      const sess = session || 'all';
      const now = new Date().toISOString();
      await q('INSERT INTO locked (date, session, reason, locked_by, locked_at) VALUES ($1,$2,$3,$4,$5)', [date, sess, reason||'Locked', 'staff', now]);
      console.log(`[LOCK] ${date} / ${sess} — ${reason}`);
      return json(res, 200, { success: true, date, session: sess, reason });
    }

    if (req.method === 'POST' && path === '/staff/unlock') {
      const body = JSON.parse(await readBody(req));
      const { date, session } = body;
      if (!date) return json(res, 400, { error: 'date required' });
      const sess = session || 'all';
      await q("DELETE FROM locked WHERE date = $1 AND (session = $2 OR $2 = 'all' OR session = 'all')", [date, sess]);
      console.log(`[UNLOCK] ${date} / ${sess}`);
      return json(res, 200, { success: true, date, session: sess });
    }

    if (path === '/staff/locks' && params.date) {
      const locks = await getLockedEntries(params.date);
      return json(res, 200, { date: params.date, locks });
    }

    if (path === '/staff/waitlist' && params.date) {
      const rows = await qRows('SELECT * FROM waitlist WHERE date = $1', [params.date]);
      return json(res, 200, { date: params.date, waitlist: rows.map(r => ({
        name: r.name||'', phone: r.phone||'', email: r.email||'', pax: r.pax||1,
        session: r.session||'', course: r.course||'', notes: r.notes||'', specialOccasion: r.special_occasion||'', createdAt: r.created_at||''
      }))});
    }

    if (req.method === 'POST' && path === '/staff/manual-book') {
      const body = JSON.parse(await readBody(req));
      const { date, session, course, name, phone, email, pax, notes, guestNames, addons } = body;
      if (!date || !session || !name || !pax) return json(res, 400, { error: 'date, session, name, pax required' });
      const now = new Date().toISOString();
      await q('INSERT INTO reservations (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, edited_at, edited_by, room, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)',
        [date, session, course||'', name, phone||'', email||'', parseInt(pax), 'manual', notes||'', now, '', guestNames||'', '', '', '', '', addons||'']);
      console.log(`[MANUAL BOOKING] ${name} (${pax}pax) → ${session} on ${date}`);
      return json(res, 200, { success: true, status: 'manual', name, pax });
    }

    if (req.method === 'POST' && path === '/staff/update') {
      const body = JSON.parse(await readBody(req));
      const { id, createdAt, notes, guestNames, allergy, phone, email, specialOccasion, pax } = body;
      if (!id && !createdAt) return json(res, 400, { error: 'id or createdAt is required' });
      const row = id
        ? await qOne('SELECT * FROM reservations WHERE id = $1', [id])
        : await qOne('SELECT * FROM reservations WHERE created_at = $1', [createdAt]);
      if (!row) return json(res, 404, { error: 'Reservation not found' });
      const rowId = row.id;
      const now = new Date().toISOString();
      if (notes !== undefined) await q('UPDATE reservations SET notes=$1 WHERE id=$2', [notes, rowId]);
      if (phone !== undefined) await q('UPDATE reservations SET phone=$1 WHERE id=$2', [phone, rowId]);
      if (email !== undefined) await q('UPDATE reservations SET email=$1 WHERE id=$2', [email, rowId]);
      if (guestNames !== undefined) await q('UPDATE reservations SET guest_names=$1 WHERE id=$2', [guestNames, rowId]);
      if (allergy !== undefined) await q('UPDATE reservations SET allergy=$1 WHERE id=$2', [allergy, rowId]);
      if (specialOccasion !== undefined) await q('UPDATE reservations SET special_occasion=$1 WHERE id=$2', [specialOccasion, rowId]);
      if (pax !== undefined) await q('UPDATE reservations SET pax=$1 WHERE id=$2', [parseInt(pax)||1, rowId]);
      await q('UPDATE reservations SET edited_at=$1, edited_by=$2 WHERE id=$3', [now, 'staff', rowId]);
      console.log(`[STAFF EDIT] Reservation id=${rowId} (created_at=${createdAt}) updated`);
      await createStaffNotification(
        'booking_change',
        '📝 Booking updated',
        `${new Date().toLocaleString('en-MY')} · ${row.name} (${row.phone}) · ${row.date} ${row.session}`,
        { createdAt, id: row.id, date: row.date, session: row.session, name: row.name, phone: row.phone }
      );
      await notifyCustomer('booking-updated', row);
      return json(res, 200, { success: true, row: row.id });
    }

    if (req.method === 'POST' && path === '/staff/resend-confirmation') {
      const body = JSON.parse(await readBody(req));
      const { id } = body;
      if (!id) return json(res, 400, { error: 'id is required' });
      const row = await qOne('SELECT * FROM reservations WHERE id = $1', [id]);
      if (!row) return json(res, 404, { error: 'Reservation not found' });
      const sent = await sendReservationEmail('confirmation', row);
      if (sent.ok) {
        await q('UPDATE reservations SET confirmation_sent_at = NOW(), confirmation_message_id = $1 WHERE id = $2', [sent.messageId, id]);
        console.log(`[STAFF] Resent confirmation to ${row.email} for booking id=${id}`);
        return json(res, 200, { success: true, email: row.email, messageId: sent.messageId });
      } else {
        return json(res, 500, { error: 'Failed to send email', reason: sent.reason });
      }
    }

    if (req.method === 'POST' && path === '/staff/action') {
      const body = JSON.parse(await readBody(req));
      const { id, createdAt, action: act } = body;
      if (!id && !createdAt) return json(res, 400, { error: 'id or createdAt required' });
      if (!act) return json(res, 400, { error: 'action required' });
      if (!id && (typeof createdAt !== 'string' || createdAt.trim().length < 10)) {
        console.error(`[SAFETY] Invalid createdAt rejected: ${JSON.stringify(createdAt)}`);
        return json(res, 400, { error: 'Invalid createdAt value' });
      }
      const row = id
        ? await qOne('SELECT * FROM reservations WHERE id = $1', [id])
        : await qOne('SELECT * FROM reservations WHERE created_at = $1', [createdAt]);
      if (!row) return json(res, 404, { error: 'Reservation not found' });
      const rowId = row.id;

      // Safety: check for duplicate created_at values
      const dupeCheck = await qOne('SELECT COUNT(*)::int AS cnt FROM reservations WHERE created_at = $1', [createdAt]);
      if (dupeCheck.cnt > 1) {
        console.warn(`[SAFETY] created_at ${createdAt} matches ${dupeCheck.cnt} rows — using id=${rowId} for safe update`);
      }

      if (act === 'cancel') {
        const now = new Date().toISOString();
        const result = await q('UPDATE reservations SET status=$1, edited_at=$2, edited_by=$3 WHERE id=$4', ['cancelled', now, 'staff-cancel', rowId]);
        if (result.rowCount !== 1) {
          console.error(`[SAFETY ALERT] Cancel UPDATE affected ${result.rowCount} rows for id=${rowId}! Expected 1.`);
          return json(res, 500, { error: 'Safety check failed: unexpected row count' });
        }
        if (row.payment_intent && stripe) { try { await cancelPaymentIntent(row.payment_intent); } catch(e) { console.error('Release failed:', e.message); } }
        await createStaffNotification(
          'cancellation',
          '❌ Booking cancelled',
          `${new Date().toLocaleString('en-MY')} · ${row.name} (${row.phone}) · ${row.date} ${row.session}`,
          { createdAt, id: row.id, date: row.date, session: row.session, name: row.name, phone: row.phone, by: 'staff' }
        );
        await notifyCustomer('booking-cancelled', row);
        return json(res, 200, { success: true, action: 'cancelled', row: row.id });
      }

      if (act === 'postpone') {
        const now = new Date().toISOString();
        const targetDate = (body.newDate || '').trim();
        const targetSession = (body.newSession || '').trim();
        if (!targetDate || !targetSession) return json(res, 400, { error: 'newDate and newSession required for postpone' });

        const validSession = ['Lunch', 'Dinner 1', 'Dinner 2'].includes(targetSession);
        if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate) || !validSession) {
          return json(res, 400, { error: 'Invalid newDate or newSession' });
        }

        const avail = await getAvailability(targetDate);
        if (avail.blocked) return json(res, 400, { error: `Restaurant is closed on ${targetDate}: ${avail.reason}` });

        const sessionInfo = avail.sessions.find(s => s.name === targetSession);
        if (!sessionInfo) return json(res, 400, { error: 'Invalid target session' });
        // Only block on manual locks, not cutoff (staff bypass)
        const manualLocks = await getLockedEntries(targetDate);
        if (isSessionLocked(manualLocks, targetSession)) return json(res, 400, { error: 'Target session is manually locked' });
        // Staff bypass: no cutoff check for staff postpone — staff knows what they're doing

        const pax = parseInt(row.pax) || 1;
        // Use actual capacity (ignore cutoff for staff)
        let remaining = sessionInfo.maxPax - sessionInfo.bookedPax;
        if (row.date === targetDate && row.session === targetSession) remaining += pax;
        if (pax > remaining) return json(res, 400, { error: `Only ${remaining} seats remaining in target session` });

        const result = await q('UPDATE reservations SET date=$1, session=$2, status=$3, edited_at=$4, edited_by=$5 WHERE id=$6',
          [targetDate, targetSession, 'confirmed', now, 'staff-postpone', rowId]);
        if (result.rowCount !== 1) {
          console.error(`[SAFETY ALERT] Postpone UPDATE affected ${result.rowCount} rows for id=${rowId}! Expected 1.`);
          return json(res, 500, { error: 'Safety check failed: unexpected row count' });
        }

        await createStaffNotification(
          'booking_change',
          '📝 Booking rescheduled',
          `${new Date().toLocaleString('en-MY')} · ${row.name} (${row.phone}) · ${row.date} ${row.session} → ${targetDate} ${targetSession}`,
          { createdAt, id: row.id, fromDate: row.date, fromSession: row.session, toDate: targetDate, toSession: targetSession, name: row.name, phone: row.phone, by: 'staff' }
        );

        await notifyCustomer('booking-postponed', { ...row, originalDate: row.date, originalSession: row.session, date: targetDate, session: targetSession, newDate: targetDate, newSession: targetSession });
        return json(res, 200, { success: true, action: 'postponed', row: row.id, date: targetDate, session: targetSession });
      }

      if (act === 'charge100' || act === 'charge50') {
        if (!row.payment_intent) return json(res, 400, { error: 'No payment intent found' });
        if (!stripe) return json(res, 500, { error: 'Stripe not configured' });
        try {
          const now = new Date().toISOString();
          if (act === 'charge50') {
            const pi = await stripe.paymentIntents.retrieve(row.payment_intent);
            const half = Math.round(pi.amount / 2);
            const cap = await stripe.paymentIntents.capture(row.payment_intent, { amount_to_capture: half });
            await q('UPDATE reservations SET status=$1, edited_at=$2, edited_by=$3 WHERE id=$4', ['charged-50%', now, 'staff-charge-50', rowId]);
            return json(res, 200, { success: true, action: 'charged-50%', intent: cap, row: row.id });
          } else {
            const cap = await capturePaymentIntent(row.payment_intent);
            await q('UPDATE reservations SET status=$1, edited_at=$2, edited_by=$3 WHERE id=$4', ['charged-100%', now, 'staff-charge-100', rowId]);
            return json(res, 200, { success: true, action: 'charged-100%', intent: cap, row: row.id });
          }
        } catch(e) { return json(res, 500, { error: 'Charge failed: ' + e.message }); }
      }

      if (act === 'nocharge') {
        const now = new Date().toISOString();
        await q('UPDATE reservations SET status=$1, edited_at=$2, edited_by=$3 WHERE id=$4', ['no-charge', now, 'staff-nocharge', rowId]);
        if (row.payment_intent && stripe) { try { await cancelPaymentIntent(row.payment_intent); } catch(e) { console.error('Release failed:', e.message); } }
        return json(res, 200, { success: true, action: 'no-charge', row: row.id });
      }

      return json(res, 400, { error: 'Unknown action: ' + act });
    }

    if (req.method === 'GET' && path === '/staff/notifications') {
      const rows = await qRows('SELECT id, type, title, message, metadata, read, created_at FROM notifications ORDER BY created_at DESC LIMIT 50');
      return json(res, 200, { notifications: rows });
    }

    if (req.method === 'GET' && path === '/staff/notifications/unread-count') {
      const row = await qOne('SELECT COUNT(*)::int AS count FROM notifications WHERE read = false');
      return json(res, 200, { unread: row?.count || 0 });
    }

    if (req.method === 'POST' && path === '/staff/notifications/read') {
      const body = JSON.parse(await readBody(req));
      if (body.all === true) {
        await q('UPDATE notifications SET read = true WHERE read = false');
        return json(res, 200, { success: true, all: true });
      }
      if (!body.id) return json(res, 400, { error: 'id or all:true required' });
      await q('UPDATE notifications SET read = true WHERE id = $1', [body.id]);
      return json(res, 200, { success: true, id: body.id });
    }


    // ── Partial Booking (Abandoned Tracking) ──
    if (req.method === 'POST' && path === '/book/partial') {
      const body = JSON.parse(await readBody(req));
      const { name, phone, email, date, session, course, pax, addons, notes } = body;
      if (!name || !phone || !email || !date || !session) {
        return json(res, 400, { error: 'name, phone, email, date, session required' });
      }
      try {
        const row = await qOne(
          `INSERT INTO partial_bookings (name, phone, email, date, session, course, pax, addons, notes, status, created_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'partial',NOW()) RETURNING id`,
          [name, phone||'', email||'', date, session, course||'', parseInt(pax)||1, addons||'', notes||'']
        );
        console.log(`[PARTIAL] Saved partial booking: ${name} (${email}) for ${date} ${session}`);
        return json(res, 200, { ok: true, partial_id: row.id });
      } catch(e) {
        console.error('[PARTIAL] Save failed:', e.message);
        return json(res, 500, { error: 'Failed to save partial booking' });
      }
    }

    // ── Update Partial Booking Status ──
    if (req.method === 'POST' && path === '/book/partial/update') {
      const body = JSON.parse(await readBody(req));
      const { partial_id, status: newStatus, error_detail, payment_intent } = body;
      if (!partial_id || !newStatus) return json(res, 400, { error: 'partial_id and status required' });
      try {
        await q(
          `UPDATE partial_bookings SET status=$1, error_detail=COALESCE($2, error_detail), payment_intent=COALESCE($3, payment_intent) WHERE id=$4`,
          [newStatus, error_detail||null, payment_intent||null, partial_id]
        );
        return json(res, 200, { ok: true });
      } catch(e) {
        console.error('[PARTIAL] Update failed:', e.message);
        return json(res, 500, { error: 'Failed to update partial booking' });
      }
    }

    // ── Abandoned Bookings List (Staff) ──
    if (req.method === 'GET' && path === '/abandoned') {
      try {
        // 1. Get all non-converted partial bookings
        const partials = await qRows(
          `SELECT id, name, phone, email, date, session, course, pax, addons, created_at, status, error_detail, payment_intent
           FROM partial_bookings
           WHERE status IN ('partial','payment_failed')
           ORDER BY created_at DESC
           LIMIT 100`
        );

        // 2. Query Stripe for recent failed/abandoned payment intents
        let stripeAbandoned = [];
        if (stripe) {
          try {
            const recentPIs = await stripe.paymentIntents.list({
              limit: 50,
              created: { gte: Math.floor(Date.now()/1000) - 7*86400 } // last 7 days
            });
            stripeAbandoned = (recentPIs.data || [])
              .filter(pi => ['requires_action','requires_payment_method','canceled'].includes(pi.status))
              .map(pi => ({
                stripe_pi: pi.id,
                name: pi.metadata?.name || '',
                phone: pi.metadata?.phone || '',
                email: pi.metadata?.email || '',
                date: pi.metadata?.date || '',
                session: pi.metadata?.session || '',
                course: pi.metadata?.course || '',
                pax: parseInt(pi.metadata?.pax) || 0,
                amount: pi.amount,
                status: pi.status === 'canceled' ? 'payment_failed' : 'partial',
                created_at: new Date(pi.created * 1000).toISOString(),
                source: 'stripe'
              }));
          } catch(e) {
            console.error('[ABANDONED] Stripe query failed:', e.message);
          }
        }

        // 3. Deduplicate: if a Stripe PI matches a partial by email or phone, skip it
        const partialEmails = new Set(partials.map(p => (p.email||'').toLowerCase()).filter(Boolean));
        const partialPhones = new Set(partials.map(p => (p.phone||'').replace(/\D/g,'')).filter(Boolean));
        const partialPIs = new Set(partials.map(p => p.payment_intent).filter(Boolean));

        const uniqueStripe = stripeAbandoned.filter(s => {
          if (s.stripe_pi && partialPIs.has(s.stripe_pi)) return false;
          const sEmail = (s.email||'').toLowerCase();
          const sPhone = (s.phone||'').replace(/\D/g,'');
          if (sEmail && partialEmails.has(sEmail)) return false;
          if (sPhone && partialPhones.has(sPhone)) return false;
          return true;
        });

        // 4. Merge and sort
        const merged = [
          ...partials.map(p => ({ ...p, source: 'partial_bookings' })),
          ...uniqueStripe
        ].sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

        return json(res, 200, { abandoned: merged, count: merged.length });
      } catch(e) {
        console.error('[ABANDONED] Error:', e.message);
        return json(res, 500, { error: 'Failed to load abandoned bookings' });
      }
    }

    // ── Update Abandoned Booking (follow up / dismiss) ──
    if (req.method === 'POST' && path === '/abandoned/update') {
      const body = JSON.parse(await readBody(req));
      const { id, status: newStatus } = body;
      if (!id || !newStatus) return json(res, 400, { error: 'id and status required' });
      if (!['followed_up','dismissed'].includes(newStatus)) return json(res, 400, { error: 'Invalid status' });
      try {
        await q('UPDATE partial_bookings SET status=$1 WHERE id=$2', [newStatus, id]);
        return json(res, 200, { ok: true });
      } catch(e) {
        return json(res, 500, { error: 'Update failed' });
      }
    }

    if (req.method === 'POST' && path === '/cancel') {
      return json(res, 200, { status: 'cancel_requested', message: 'Cancellation logged.' });
    }

    // ── Setup Intent (new card-guarantee flow) ──
    if (req.method === 'POST' && path === '/setup-intent') {
      if (!stripe) return json(res, 500, { error: 'Stripe not configured' });
      const body = JSON.parse(await readBody(req));
      const { date, session, course, name, phone, email, pax } = body;
      if (!date || !session || !course || !name || !phone || !email || !pax)
        return json(res, 400, { error: 'Missing required fields' });
      const numPax = parseInt(pax);
      // Check availability & cutoff before creating SI
      const avail = await getAvailability(date);
      if (avail.blocked) return json(res, 400, { error: `Restaurant is closed on ${date}: ${avail.reason}` });
      const sessionInfo = avail.sessions.find(s => s.name === session);
      if (!sessionInfo) return json(res, 400, { error: `Invalid session: ${session}` });
      if (sessionInfo.locked) return json(res, 400, { error: 'This session is currently fully booked.' });
      if (isSessionPastCutoff(date, session)) return json(res, 400, { error: 'Booking for this session has closed.' });
      try {
        const si = await stripe.setupIntents.create({
          usage: 'off_session',
          payment_method_types: ['card'],
          payment_method_options: { card: { request_three_d_secure: 'any' } },
          metadata: { restaurant: 'Fusion Omakase by Makii Sushi', date, session, course, name, email, phone, pax: String(numPax) }
        });
        console.log('[STRIPE] SetupIntent created: ' + si.id);
        return json(res, 200, { setupIntentClientSecret: si.client_secret, setupIntentId: si.id });
      } catch (e) {
        console.error('[SETUP-INTENT] Stripe error:', e.message);
        return json(res, 500, { error: 'Failed to create card setup: ' + e.message });
      }
    }

    // ── Confirm booking after SetupIntent (new flow) ──
    if (req.method === 'POST' && path === '/book/confirm') {
      if (!stripe) return json(res, 500, { error: 'Stripe not configured' });
      const body = JSON.parse(await readBody(req));
      const { setupIntentId, date, session, course, name, phone, email, pax, notes, guestNames, allergy, special_occasion, occasion, addons } = body;
      if (!setupIntentId) return json(res, 400, { error: 'setupIntentId required' });
      if (!date || !session || !course || !name || !phone || !email || !pax)
        return json(res, 400, { error: 'Missing required booking fields' });
      // Verify SetupIntent succeeded
      let si;
      try {
        si = await stripe.setupIntents.retrieve(setupIntentId);
      } catch (e) {
        return json(res, 400, { error: 'Could not verify card setup: ' + e.message });
      }
      if (si.status !== 'succeeded') {
        console.error(`[BOOK/CONFIRM] SI ${setupIntentId} status is ${si.status}, expected succeeded`);
        return json(res, 400, { error: `Card setup not complete. Status: ${si.status}` });
      }
      const paymentMethodId = si.payment_method;
      if (!paymentMethodId) return json(res, 400, { error: 'No payment method on setup intent' });
      // Check availability
      const numPax = parseInt(pax);
      const avail = await getAvailability(date);
      if (avail.blocked) return json(res, 400, { error: `Restaurant is closed on ${date}` });
      const sessionInfo = avail.sessions.find(s => s.name === session);
      if (!sessionInfo) return json(res, 400, { error: `Invalid session: ${session}` });
      const now = new Date().toISOString();
      const guestNamesStr = guestNames || '';
      const allergyStr = allergy || '';
      const specialOccasionStr = special_occasion || occasion || '';
      const addonsStr = addons || '';
      try {
        if (sessionInfo.remaining >= numPax) {
          const reservation = await qOne(
            'INSERT INTO reservations (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, setup_intent_id, payment_method_id, guest_names, allergy, special_occasion, edited_at, edited_by, room, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) RETURNING id, date, session, course, name, phone, email, pax, created_at',
            [date, session, course, name, phone, email, numPax, 'confirmed', notes || '', now, '', setupIntentId, paymentMethodId, guestNamesStr, allergyStr, specialOccasionStr, '', '', '', addonsStr]
          );
          console.log(`[BOOK/CONFIRM] ${name} (${numPax}pax) → ${course} @ ${session} on ${date} — CONFIRMED (SI:${setupIntentId} PM:${paymentMethodId})`);
          try {
            await q(`UPDATE partial_bookings SET status='converted', payment_intent=$1 WHERE status='partial' AND email=$2 AND date=$3 AND session=$4`, [setupIntentId, email, date, session]);
          } catch(e) { console.error('[PARTIAL] Convert update failed:', e.message); }
          await createStaffNotification(
            'new_reservation',
            '✅ New reservation confirmed (card guarantee)',
            `${new Date().toLocaleString('en-MY')} · ${date} ${session} · ${numPax} pax · ${course}`,
            { date, session, course, name, phone, email, pax: numPax, setup_intent_id: setupIntentId, payment_method_id: paymentMethodId }
          );
          await notifyCustomer('booking-confirmation', { ...reservation, payment_intent: '' });
          return json(res, 200, { status: 'confirmed', date, session, course, name, pax: numPax, setupIntentId, paymentMethodId });
        } else {
          await q('INSERT INTO waitlist (date, session, course, name, phone, email, pax, status, notes, created_at, payment_intent, guest_names, allergy, special_occasion, addons) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)',
            [date, session, course, name, phone, email, numPax, 'waitlist', notes || '', now, '', guestNamesStr, allergyStr, specialOccasionStr, addonsStr]);
          console.log(`[BOOK/CONFIRM] ${name} (${numPax}pax) → ${session} on ${date} — WAITLISTED`);
          return json(res, 200, { status: 'waitlist', message: 'Session is full. Added to waitlist.' });
        }
      } catch(e) {
        console.error('[BOOK/CONFIRM] DB error:', e.message);
        return json(res, 500, { error: 'Booking failed: ' + e.message });
      }
    }

    // ── Staff: Charge No-Show against saved card ──
    if (req.method === 'POST' && path === '/staff/charge-noshow') {
      if (!stripe) return json(res, 500, { error: 'Stripe not configured' });
      const body = JSON.parse(await readBody(req));
      const { reservationId, amount } = body;
      if (!reservationId) return json(res, 400, { error: 'reservationId required' });
      if (!amount || isNaN(parseFloat(amount))) return json(res, 400, { error: 'amount (RM) required' });
      const row = await qOne('SELECT * FROM reservations WHERE id = $1', [reservationId]);
      if (!row) return json(res, 404, { error: 'Reservation not found' });
      if (!row.payment_method_id) return json(res, 400, { error: 'No saved card on file for this reservation' });
      const amountCents = Math.round(parseFloat(amount) * 100);
      try {
        const pi = await stripe.paymentIntents.create({
          amount: amountCents,
          currency: 'myr',
          payment_method: row.payment_method_id,
          confirm: true,
          off_session: true,
          description: `No-show charge — ${row.name} — ${row.date} ${row.session}`,
          metadata: { reservation_id: String(row.id), name: row.name, date: row.date, session: row.session }
        });
        const now = new Date().toISOString();
        const noShowNote = `No-show charged: RM${amount} [PI: ${pi.id}]`;
        const updatedNotes = row.notes ? `${row.notes}
${noShowNote}` : noShowNote;
        await q('UPDATE reservations SET status=$1, edited_at=$2, edited_by=$3, notes=$4 WHERE id=$5', ['cancelled', now, 'staff-noshow', updatedNotes, row.id]);
        console.log(`[NOSHOW] Charged RM${amount} to ${row.name} (${row.id}) — PI: ${pi.id}, status set to cancelled`);
        await createStaffNotification(
          'noshow_charge',
          `💳 No-show charge RM${amount}`,
          `${new Date().toLocaleString('en-MY')} · ${row.name} (${row.phone}) · ${row.date} ${row.session}`,
          { id: row.id, name: row.name, phone: row.phone, date: row.date, session: row.session, amount, pi: pi.id }
        );
        return json(res, 200, { success: true, chargedAmount: amount, paymentIntentId: pi.id, status: 'cancelled' });
      } catch (e) {
        console.error('[NOSHOW] Charge failed:', e.message);
        // Stripe error codes for common off-session failures
        let userMsg = e.message;
        if (e.code === 'card_declined') userMsg = 'Card declined. Contact the customer for an alternative payment method.';
        else if (e.code === 'expired_card') userMsg = 'Card has expired. Contact the customer.';
        else if (e.code === 'authentication_required') userMsg = 'Card requires authentication. Contact the customer directly.';
        return json(res, 500, { error: userMsg, code: e.code || 'unknown' });
      }
    }

    json(res, 404, { error: 'Not found' });
  } catch (e) {
    console.error('Error:', e.message, e.stack);
    json(res, 500, { error: 'Internal server error' });
  }
});

async function send24hReminders() {
  try {
    const rows = await qRows(`
      SELECT r.id, r.date, r.session, r.course, r.name, r.email, r.phone, r.pax,
             s.start_time AS session_time
      FROM reservations r
      LEFT JOIN sessions s ON s.name = r.session
      WHERE r.status IN ('confirmed', 'manual')
        AND r.reminder_sent_at IS NULL
        AND COALESCE(r.email, '') <> ''
        AND NOW() >= ((r.date::date + COALESCE(s.start_time, '19:00')::time) - INTERVAL '24 hours')
        AND NOW() <  ((r.date::date + COALESCE(s.start_time, '19:00')::time) - INTERVAL '23 hours 45 minutes')
      ORDER BY r.date, r.session
    `);

    for (const booking of rows) {
      const sent = await sendReservationEmail('reminder', booking);
      if (sent.ok) {
        await q(
          'UPDATE reservations SET reminder_sent_at = NOW(), reminder_message_id = $1 WHERE id = $2',
          [sent.messageId, booking.id]
        );
      }
    }
    if (rows.length) console.log(`[REMINDER] Processed ${rows.length} reservation reminder(s)`);
  } catch (e) {
    console.error('[REMINDER] job failed:', e.message);
  }
}

setInterval(send24hReminders, 15 * 60 * 1000);
setTimeout(send24hReminders, 30 * 1000);

server.listen(PORT, () => console.log(`Makii Reservation API running on port ${PORT} (PostgreSQL)`));
