#!/usr/bin/env node

const { exec } = require('child_process');
const { promisify } = require('util');

const execAsync = promisify(exec);

async function killPort(port) {
  try {
    console.log(`🔍 Checking for processes on port ${port}...`);
    
    // Find processes using the port
    const { stdout } = await execAsync(`netstat -ano | findstr :${port}`);
    
    if (!stdout.trim()) {
      console.log(`✅ No processes found running on port ${port}`);
      return;
    }
    
    console.log(`⚠️  Found processes on port ${port}:`);
    console.log(stdout);
    
    // Extract PIDs from the output
    const lines = stdout.trim().split('\n');
    const pids = new Set();
    
    lines.forEach(line => {
      const parts = line.trim().split(/\s+/);
      if (parts.length >= 5) {
        const pid = parts[parts.length - 1];
        if (pid && pid !== '0') {
          pids.add(pid);
        }
      }
    });
    
    if (pids.size === 0) {
      console.log(`✅ No valid PIDs found to kill`);
      return;
    }
    
    // Kill each process
    for (const pid of pids) {
      try {
        console.log(`🔪 Killing process ${pid}...`);
        await execAsync(`taskkill /PID ${pid} /F`);
        console.log(`✅ Successfully killed process ${pid}`);
      } catch (error) {
        console.log(`⚠️  Could not kill process ${pid}: ${error.message}`);
      }
    }
    
    console.log(`✅ Finished killing processes on port ${port}`);
    
  } catch (error) {
    if (error.message.includes('findstr')) {
      console.log(`✅ No processes found running on port ${port}`);
    } else {
      console.error(`❌ Error killing processes on port ${port}:`, error.message);
      process.exit(1);
    }
  }
}

// Get port from command line argument or use default
const port = process.argv[2] || '3000';

killPort(port);
