import fs from 'fs';
import path from 'path';
import express from 'express';

export interface InlineAssetsOptions {
  cssPath?: string;
  jsPath?: string;
}

export class InlineAssetsMiddleware {
  private cssContent: string | null = null;
  private jsContent: string | null = null;
  private cssPath: string;
  private jsPath: string;

  constructor(options: InlineAssetsOptions = {}) {
    this.cssPath = options.cssPath || path.join(__dirname, '../../public/css/style.css');
    this.jsPath = options.jsPath || path.join(__dirname, '../../public/js/app.js');
  }

  async initialize(): Promise<void> {
    try {
      // Read CSS file
      if (fs.existsSync(this.cssPath)) {
        this.cssContent = fs.readFileSync(this.cssPath, 'utf8');
      } else {
        throw new Error(`CSS file not found at ${this.cssPath}`);
      }

      // Read JS file
      if (fs.existsSync(this.jsPath)) {
        this.jsContent = fs.readFileSync(this.jsPath, 'utf8');
      } else {
        throw new Error(`JS file not found at ${this.jsPath}`);
      }
    } catch (error) {
      console.error('Error reading asset files for inlining:', error);
    }
  }

  getInlineCSS(): string {
    return this.cssContent || '';
  }

  getInlineJS(): string {
    return this.jsContent || '';
  }


  // Middleware function to add inlined assets to response locals
  middleware() {
    return (req: express.Request, res: express.Response, next: express.NextFunction) => {
      const isAuthRoute = typeof req.path === 'string' && req.path.startsWith('/auth');

      res.locals.isAuthRoute = isAuthRoute;

      if (!isAuthRoute) {
        res.locals.inlineCSS = this.getInlineCSS();
        res.locals.inlineJS = this.getInlineJS();
      } else {
        res.locals.inlineCSS = '';
        res.locals.inlineJS = '';
      }

      next();
    };
  }
}
