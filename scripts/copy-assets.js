#!/usr/bin/env node

/**
 * Cross-platform asset copying script for StratCraft
 * Copies necessary files and directories to the dist folder after TypeScript compilation
 */

const fs = require('fs');
const path = require('path');

function removeJsonFiles(dirPath) {
    if (!fs.existsSync(dirPath)) {
        return;
    }

    for (const entry of fs.readdirSync(dirPath)) {
        const entryPath = path.join(dirPath, entry);
        const stat = fs.statSync(entryPath);

        if (stat.isDirectory()) {
            removeJsonFiles(entryPath);
            continue;
        }

        if (entry.toLowerCase().endsWith('.json')) {
            fs.unlinkSync(entryPath);
        }
    }
}

// Colors for console output
const colors = {
    reset: '\x1b[0m',
    bright: '\x1b[1m',
    red: '\x1b[31m',
    green: '\x1b[32m',
    yellow: '\x1b[33m',
    blue: '\x1b[34m'
};

function log(message, color = 'reset') {
    console.log(`${colors[color]}${message}${colors.reset}`);
}

function ensureDir(dirPath) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true });
        log(`Created directory: ${dirPath}`, 'blue');
    }
}

function copyFile(src, dest) {
    try {
        ensureDir(path.dirname(dest));
        fs.copyFileSync(src, dest);
        // log(`Copied: ${src} → ${dest}`, 'green');
    } catch (error) {
        log(`Error copying ${src}: ${error.message}`, 'red');
        process.exit(1);
    }
}

function copyDir(src, dest) {
    try {
        ensureDir(dest);

        const items = fs.readdirSync(src);

        for (const item of items) {
            const srcPath = path.join(src, item);
            const destPath = path.join(dest, item);
            const stat = fs.statSync(srcPath);

            if (stat.isDirectory()) {
                copyDir(srcPath, destPath);
            } else {
                copyFile(srcPath, destPath);
            }
        }

        // log(`Copied directory: ${src} → ${dest}`, 'green');
    } catch (error) {
        log(`Error copying directory ${src}: ${error.message}`, 'red');
        process.exit(1);
    }
}

function main() {
    log('Starting asset copy process...', 'bright');

    const rootDir = path.join(__dirname, '..');
    const distDir = path.join(rootDir, 'dist');

    // Ensure dist directory exists
    ensureDir(distDir);

    // Copy pg.sql
    const pgSchemaSrc = path.join(rootDir, 'src', 'server', 'database', 'pg.sql');
    const pgSchemaDest = path.join(distDir, 'server', 'database', 'pg.sql');

    if (fs.existsSync(pgSchemaSrc)) {
        copyFile(pgSchemaSrc, pgSchemaDest);
    } else {
        log(`Warning: PostgreSQL schema file not found at ${pgSchemaSrc}`, 'yellow');
    }

    // Copy views directory
    const viewsSrc = path.join(rootDir, 'src', 'views');
    const viewsDest = path.join(distDir, 'views');

    if (fs.existsSync(viewsSrc)) {
        copyDir(viewsSrc, viewsDest);
    } else {
        log(`Warning: Views directory not found at ${viewsSrc}`, 'yellow');
    }

    // Copy strategies directory
    const strategiesSrc = path.join(rootDir, 'src', 'server', 'strategies');
    const strategiesDest = path.join(distDir, 'server', 'strategies');

    if (fs.existsSync(strategiesSrc)) {
        removeJsonFiles(strategiesDest);
        copyDir(strategiesSrc, strategiesDest);
    } else {
        log(`Warning: Strategies directory not found at ${strategiesSrc}`, 'yellow');
    }

    // Copy public directory
    const publicSrc = path.join(rootDir, 'src', 'public');
    const publicDest = path.join(distDir, 'public');

    if (fs.existsSync(publicSrc)) {
        copyDir(publicSrc, publicDest);
    } else {
        log(`Warning: Public directory not found at ${publicSrc}`, 'yellow');
    }

    log('Asset copy process completed successfully!', 'green');
}

// Run the script
if (require.main === module) {
    main();
}

module.exports = { copyFile, copyDir, ensureDir };
