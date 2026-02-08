import express, { NextFunction, Request, Response } from 'express';
import { MtlsAccessBundleEmailError } from '../services/MtlsLockdownService';
import { SETTING_KEYS } from '../constants';

const router = express.Router();

const normalizeInviteLinkDays = (rawValue: string | null): number => {
  return Number(rawValue) || 7;
};

// Admin users
router.get('/users', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const [allUsers, inviteLinkDays, activeSessions, clientCertLockdown] = await Promise.all([
      req.authService.getAllUsers(),
      req.db.settings.getSettingValue(SETTING_KEYS.INVITE_LINK_VALID_DAYS).then(normalizeInviteLinkDays),
      req.db.users.listActiveUserSessions(),
      req.mtlsLockdownService.getLockdownState()
    ]);

    const sessionsByUserId = new Map<number, typeof activeSessions>();
    for (const session of activeSessions) {
      const entries = sessionsByUserId.get(session.userId);
      if (entries) {
        entries.push(session);
      } else {
        sessionsByUserId.set(session.userId, [session]);
      }
    }

    const usersWithSessions = allUsers.map(user => ({
      ...user,
      sessions: sessionsByUserId.get(user.id) ?? []
    }));

    res.render('pages/users', {
      title: 'User Administration',
      page: 'admin-users',
      allUsers: usersWithSessions,
      inviteLinkDays,
      user: req.user,
      clientCertLockdownSupported: clientCertLockdown.supported,
      clientCertLockdownHelperAvailable: clientCertLockdown.helperAvailable,
      clientCertLockdownControlsEnabled: clientCertLockdown.controlsEnabled,
      clientCertLockdownEnabled: clientCertLockdown.lockdownEnabled,
      clientCertBundleAvailable: clientCertLockdown.bundleAvailable,
      success: req.query.success as string,
      error: req.query.error as string
    });
  } catch (error) {
    console.error('Error loading admin users page:', error);
    res.status(500).render('pages/error', {
      title: 'Error',
      error: 'Failed to load user administration'
    });
  }
});

router.post('/access-lockdown/generate-client-cert', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    await req.mtlsLockdownService.generateClientCertificateBundleFromDatabase(req.db);

    const state = await req.mtlsLockdownService.getLockdownState();
    if (!state.lockdownEnabled) {
      res.redirect('/admin/users?success=Client certificate bundle generated. Download it below before enabling lockdown.');
      return;
    }

    if (state.controlsEnabled) {
      try {
        await req.mtlsLockdownService.enableLockdown();
      } catch (error) {
        console.error('Error reloading nginx after client certificate rotation:', error);
        const message = error instanceof Error
          ? error.message
          : 'Failed to reload nginx after rotating the client certificate';
        res.redirect(`/admin/users?error=${encodeURIComponent(message)}`);
        return;
      }
    }

    try {
      const { sent, adminCount } = await req.mtlsLockdownService.emailAccessBundleToAllUsersOrRollback({
        emailService: req.emailService
      });

      const reloadNote = state.controlsEnabled
        ? 'nginx reloaded'
        : 'nginx reload required';
      const message = `Client certificate rotated (${reloadNote}). Emailed access certificate to ${sent} user${sent === 1 ? '' : 's'} (${adminCount} admin${adminCount === 1 ? '' : 's'}).`;
      res.redirect(`/admin/users?success=${encodeURIComponent(message)}`);
    } catch (error) {
      console.error('Error emailing client certificate access bundle:', error);
      const details = error instanceof MtlsAccessBundleEmailError
        ? error.details
        : error instanceof Error
          ? error.message
          : 'Failed to email access certificate';
      const rollbackSucceeded = error instanceof MtlsAccessBundleEmailError
        ? error.rollbackSucceeded
        : false;
      const helperPath = error instanceof MtlsAccessBundleEmailError
        ? error.helperPath
        : req.mtlsLockdownService.helperPath;
      const message = rollbackSucceeded
        ? `Client certificate rotated, but emailing the access certificate failed (${details}). Lockdown has been disabled again to prevent lockouts.`
        : `Client certificate rotated, but emailing the access certificate failed (${details}). Lockdown may still be enabled; disable via SSH: sudo ${helperPath} disable`;

      res.redirect(`/admin/users?error=${encodeURIComponent(message)}`);
    }
  } catch (error) {
    console.error('Error generating client certificate bundle:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to generate client certificate bundle';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

router.get('/access-lockdown/download-client-cert', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    if (!req.mtlsLockdownService.isClientCertificateBundleAvailable()) {
      return res.redirect('/admin/users?error=Client certificate bundle is missing. Generate it first.');
    }

    res.download(req.mtlsLockdownService.clientP12Path, 'stratcraft-access.p12');
  } catch (error) {
    console.error('Error downloading client certificate bundle:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to download client certificate bundle';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

router.post('/access-lockdown/enable', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  const state = await req.mtlsLockdownService.getLockdownState();
  if (!state.supported) {
    res.redirect('/admin/users?error=Client certificate lockdown is only supported on Linux deployments.');
    return;
  }
  if (!state.bundleAvailable) {
    res.redirect('/admin/users?error=Generate the client certificate bundle first.');
    return;
  }
  if (!state.helperAvailable) {
    res.redirect('/admin/users?error=nginx client-cert helper is missing. Re-run deploy.sh update or follow the manual nginx instructions.');
    return;
  }

  try {
    await req.mtlsLockdownService.enableLockdown();
  } catch (error) {
    console.error('Error enabling client certificate lockdown:', error);
    const message = error instanceof Error ? error.message : 'Failed to enable client certificate lockdown';
    res.redirect(`/admin/users?error=${encodeURIComponent(message)}`);
    return;
  }

  try {
    const { sent, adminCount } = await req.mtlsLockdownService.emailAccessBundleToAllUsersOrRollback({
      emailService: req.emailService
    });

    const message = `Client certificate lockdown enabled (nginx reloaded). Emailed access certificate to ${sent} user${sent === 1 ? '' : 's'} (${adminCount} admin${adminCount === 1 ? '' : 's'}).`;
    res.redirect(`/admin/users?success=${encodeURIComponent(message)}`);
  } catch (error) {
    console.error('Error emailing client certificate access bundle:', error);
    const details = error instanceof MtlsAccessBundleEmailError
      ? error.details
      : error instanceof Error
        ? error.message
        : 'Failed to email access certificate';
    const rollbackSucceeded = error instanceof MtlsAccessBundleEmailError
      ? error.rollbackSucceeded
      : false;
    const helperPath = error instanceof MtlsAccessBundleEmailError
      ? error.helperPath
      : req.mtlsLockdownService.helperPath;
    const message = rollbackSucceeded
      ? `Lockdown was enabled, but emailing the access certificate failed (${details}). Lockdown has been disabled again to prevent lockouts.`
      : `Lockdown was enabled, but emailing the access certificate failed (${details}). Lockdown may still be enabled; disable via SSH: sudo ${helperPath} disable`;

    res.redirect(`/admin/users?error=${encodeURIComponent(message)}`);
  }
});

router.post('/access-lockdown/disable', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  const state = await req.mtlsLockdownService.getLockdownState();
  if (!state.supported) {
    res.redirect('/admin/users?error=Client certificate lockdown is only supported on Linux deployments.');
    return;
  }
  if (!state.helperAvailable) {
    res.redirect('/admin/users?error=nginx client-cert helper is missing. Disable by SSH: edit /etc/nginx/stratcraft-mtls.conf and reload nginx.');
    return;
  }

  try {
    await req.mtlsLockdownService.disableLockdown();
    res.redirect('/admin/users?success=Client certificate lockdown disabled (nginx reloaded).');
  } catch (error) {
    console.error('Error disabling client certificate lockdown:', error);
    const message = error instanceof Error ? error.message : 'Failed to disable client certificate lockdown';
    res.redirect(`/admin/users?error=${encodeURIComponent(message)}`);
  }
});

// Invite user
router.post('/invite-user', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const { email } = req.body;
    if (!email || typeof email !== 'string') {
      return res.redirect('/admin/users?error=Email is required');
    }

    const inviteLinkDays = normalizeInviteLinkDays(
      await req.db.settings.getSettingValue(SETTING_KEYS.INVITE_LINK_VALID_DAYS)
    );
    const host = req.get('host');
    if (!host) {
      return res.redirect('/admin/users?error=Unable to determine host for invite link');
    }
    const baseUrl = `${req.protocol}://${host}`;

    await req.authService.inviteUser(email, inviteLinkDays, baseUrl);

    res.redirect(`/admin/users?success=${encodeURIComponent(`Invitation sent to ${email}`)}`);
  } catch (error) {
    console.error('Error inviting user:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to send invitation';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Delete user (admin only)
router.post('/delete-user', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.redirect('/admin/users?error=User ID is required');
    }

    await req.authService.deleteUser(parseInt(userId));

    res.redirect('/admin/users?success=User deleted successfully');
  } catch (error) {
    console.error('Error deleting user:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to delete user';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Promote user to admin (admin only)
router.post('/make-admin', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const { userId } = req.body;

    if (!userId) {
      return res.redirect('/admin/users?error=User ID is required');
    }

    await req.authService.promoteToAdmin(parseInt(userId));

    res.redirect('/admin/users?success=User promoted to admin');
  } catch (error) {
    console.error('Error promoting user to admin:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to promote user';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

// Delete session (admin only)
router.post('/delete-session', (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAuth(req, res, next);
}, (req: Request, res: Response, next: NextFunction) => {
  req.authMiddleware.requireAdmin(req, res, next);
}, async (req: Request, res: Response) => {
  try {
    const { sessionId } = req.body;
    const parsedSessionId = Number.parseInt(sessionId, 10);
    if (!Number.isFinite(parsedSessionId)) {
      return res.redirect('/admin/users?error=Session ID is required');
    }

    const deleted = await req.db.users.deleteUserSessionById(parsedSessionId);
    if (!deleted) {
      return res.redirect('/admin/users?error=Session not found');
    }

    res.redirect('/admin/users?success=Session revoked');
  } catch (error) {
    console.error('Error deleting session:', error);
    const errorMessage = error instanceof Error ? error.message : 'Failed to delete session';
    res.redirect(`/admin/users?error=${encodeURIComponent(errorMessage)}`);
  }
});

export default router;
