import express, { Request, Response } from 'express';

const router = express.Router();

// Home page - redirect to dashboard
router.get('/', (req: Request, res: Response) => {
  res.redirect('/dashboard');
});

// Login page redirect
router.get('/login', (req: Request, res: Response) => {
  res.redirect('/auth/login');
});

export default router;
