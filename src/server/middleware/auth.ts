import { Request, Response, NextFunction } from 'express';
import { AuthService, AuthSession } from '../services/AuthService';
import { LoggingService } from '../services/LoggingService';

// Extend Express Request type to include user session
declare global {
  namespace Express {
    interface Request {
      user?: AuthSession;
      authService: AuthService;
    }
  }
}

export class AuthMiddleware {
  private authService: AuthService;
  private loggingService: LoggingService;

  constructor(authService: AuthService, loggingService: LoggingService) {
    this.authService = authService;
    this.loggingService = loggingService;
  }

  // Middleware to require authentication
  requireAuth = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const token = req.cookies?.session_token;

    if (!token) {
      this.loggingService.warn('auth', 'Authentication required but no token provided', {
        path: req.path,
        ip: req.ip,
        userAgent: req.get('User-Agent')
      });
      return res.redirect('/auth/login');
    }

    const session = await this.authService.verifySessionToken(token);

    if (!session) {
      this.loggingService.warn('auth', 'Invalid session token provided', {
        path: req.path,
        ip: req.ip,
        userAgent: req.get('User-Agent')
      });
      res.clearCookie('session_token');
      return res.redirect('/auth/login');
    }

    req.user = session;
    next();
  };

  // Middleware to require admin role
  requireAdmin = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    if (!req.user) {
      this.loggingService.warn('auth', 'Admin access attempted without authentication', {
        path: req.path,
        ip: req.ip
      });
      return res.redirect('/auth/login');
    }

    if (req.user.role !== 'admin') {
      this.loggingService.warn('auth', 'Admin access attempted by non-admin user', {
        user_id: req.user.id,
        email: req.user.email,
        role: req.user.role,
        path: req.path,
        ip: req.ip
      });
      return res.status(403).render('pages/error', {
        title: 'Access Denied',
        error: 'You do not have permission to access this page.'
      });
    }

    next();
  };

  // Middleware to make user session available (optional auth)
  optionalAuth = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const token = req.cookies?.session_token;

    if (token) {
      const session = await this.authService.verifySessionToken(token);
      if (session) {
        req.user = session;
      } else {
        res.clearCookie('session_token');
      }
    }

    next();
  };

  // Middleware to redirect authenticated users away from login page
  redirectIfAuthenticated = async (req: Request, res: Response, next: NextFunction): Promise<void> => {
    const token = req.cookies?.session_token;

    if (token) {
      const session = await this.authService.verifySessionToken(token);
      if (session) {
        return res.redirect('/dashboard');
      }
    }

    next();
  };
}
