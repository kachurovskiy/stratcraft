import express, { NextFunction, Request, Response } from 'express';
import { SETTING_KEYS } from '../constants';
import { RequestQuotaError, type AuthSession } from '../services/AuthService';

const router = express.Router();
const DEFAULT_SESSION_COOKIE_DAYS = 30;

const formatRetryAfter = (retryAfterMs: number | null): string => {
  if (!retryAfterMs || retryAfterMs <= 0) {
    return 'Please try again later.';
  }
  const totalSeconds = Math.ceil(retryAfterMs / 1000);
  if (totalSeconds < 60) {
    return `Please try again in ${totalSeconds} seconds.`;
  }
  const totalMinutes = Math.ceil(totalSeconds / 60);
  return `Please try again in ${totalMinutes} minutes.`;
};

const resolveSessionCookieMaxAge = async (req: Request): Promise<number> => {
  const rawValue = await req.db.settings.getSettingValue(SETTING_KEYS.SESSION_COOKIE_VALID_DAYS);
  const parsedDays = rawValue ? Number.parseInt(rawValue, 10) : NaN;
  const normalizedDays = Number.isFinite(parsedDays) && parsedDays > 0
    ? parsedDays
    : DEFAULT_SESSION_COOKIE_DAYS;
  return normalizedDays * 24 * 60 * 60 * 1000;
};

const issueSessionCookie = async (req: Request, res: Response, session: AuthSession): Promise<void> => {
  const maxAge = await resolveSessionCookieMaxAge(req);
  const expiresAt = new Date(Date.now() + maxAge);
  const sessionToken = await req.authService.generateSessionToken(
    session,
    expiresAt,
    req.ip || 'unknown',
    req.get('User-Agent') || 'unknown'
  );
  res.cookie('session_token', sessionToken, {
    maxAge,
    httpOnly: true,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax'
  });
};

// Login page
router.get('/login', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.redirectIfAuthenticated(req, res, next);
}, (req: Request, res: Response) => {
  res.render('pages/login', {
    title: `Sign In to ${res.locals.siteName}`,
    error: req.query.error as string,
    success: req.query.success as string,
    hideNavbar: true
  });
});

// Accept invite link
router.get('/invite', async (req: Request, res: Response) => {
  try {
    const token = typeof req.query.token === 'string' ? req.query.token : '';
    if (!token) {
      return res.redirect('/auth/login?error=Invitation link is invalid or expired');
    }

    const session = await req.authService.verifyInviteToken(token);
    if (!session) {
      return res.redirect('/auth/login?error=Invitation link is invalid or expired');
    }

    await issueSessionCookie(req, res, session);

    res.redirect('/dashboard');
  } catch (error) {
    console.error('Error verifying invite link:', error);
    res.redirect('/auth/login?error=Invitation link is invalid or expired');
  }
});

// Send OTP
router.post('/send-otp', async (req: Request, res: Response) => {
  const successMessage = 'If your email is eligible, an access code has been sent.';
  try {
    const email = typeof req.body?.email === 'string' ? req.body.email.trim() : '';

    if (!email || !email.includes('@')) {
      return res.redirect('/auth/login?error=Please enter a valid email address');
    }

    const otpResult = await req.authService.sendOTP(email, req.ip);
    const finalMessage = otpResult.fallbackOtp
      ? `Email delivery is not configured. Here's your access code - ${otpResult.fallbackOtp} - configure email settings in Admin Settings ASAP!`
      : successMessage;
    res.redirect(`/auth/login?success=${encodeURIComponent(finalMessage)}`);
  } catch (error) {
    if (error instanceof RequestQuotaError) {
      req.loggingService.warn('auth', 'OTP send rate limited', {
        email: typeof req.body?.email === 'string' ? req.body.email.trim() : '',
        ip: req.ip,
        retryAfterMs: error.retryAfterMs ?? undefined
      });
      const message = `Too many requests. ${formatRetryAfter(error.retryAfterMs)}`;
      return res.redirect(`/auth/login?error=${encodeURIComponent(message)}`);
    }
    console.error('Error sending OTP:', error);
    res.redirect(`/auth/login?success=${encodeURIComponent(successMessage)}`);
  }
});

// Verify OTP and login
router.post('/verify-otp', async (req: Request, res: Response) => {
  try {
    const email = typeof req.body?.email === 'string' ? req.body.email.trim() : '';
    const otp = typeof req.body?.otp === 'string' ? req.body.otp.trim() : '';

    if (!email || !otp) {
      return res.redirect('/auth/login?error=Email and access code are required');
    }

    const session = await req.authService.verifyOTP(email, otp, req.ip);

    if (!session) {
      return res.redirect('/auth/login?error=Invalid access code or code expired');
    }

    await issueSessionCookie(req, res, session);

    res.redirect('/dashboard');
  } catch (error) {
    if (error instanceof RequestQuotaError) {
      req.loggingService.warn('auth', 'OTP verification rate limited', {
        email: typeof req.body?.email === 'string' ? req.body.email.trim() : '',
        ip: req.ip,
        retryAfterMs: error.retryAfterMs ?? undefined
      });
      const message = `Too many attempts. ${formatRetryAfter(error.retryAfterMs)}`;
      return res.redirect(`/auth/login?error=${encodeURIComponent(message)}`);
    }
    console.error('Error verifying OTP:', error);
    res.redirect('/auth/login?error=Login failed. Please try again.');
  }
});

// Logout (POST - for form submissions)
router.post('/logout', async (req: Request, res: Response) => {
  const token = req.cookies?.session_token;
  if (token) {
    await req.authService.clearSessionToken(token);
  }
  res.clearCookie('session_token');
  res.clearCookie('csrf_token');
  res.redirect('/auth/login?success=You have been signed out');
});

// Logout all sessions (POST)
router.post('/logout-all', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, async (req: Request, res: Response) => {
  const userId = req.user?.userId ?? req.user?.id;
  if (userId) {
    await req.authService.clearAllSessionsForUser(userId);
  }
  res.clearCookie('session_token');
  res.clearCookie('csrf_token');
  res.redirect('/auth/login?success=You have been signed out everywhere');
});

export default router;
