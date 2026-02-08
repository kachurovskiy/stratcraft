# StratCraft Deployment Guide (Hetzner Cloud / Ubuntu)

Quick deployment guide for StratCraft on a Hetzner Cloud Ubuntu server with HTTPS and manual updates.

## Hetzner Cloud prerequisites

1. Create a server in Hetzner Cloud:
   - Image: Ubuntu 22.04 LTS (or 24.04)
   - Size: **CPX62 (16 vCPU)** recommended
   - Add your SSH key (recommended)
   - If you use a Hetzner Firewall, allow inbound TCP `22`, `80`, `443`

2. Buy a domain name (required for HTTPS via Let's Encrypt):
   - Any registrar works (e.g. Namecheap)
   - Budget TLDs like `.click` are $2-$3/year

3. Create an `A` record for your domain in Namecheap Advanced DNS

## No SSH key? (Password login)

If you don't have an SSH key yet, you can still deploy using Hetzner's password-based root login:

1. Create the server **without** adding an SSH key.
2. Copy the server's **root password** from the Hetzner server creation email
3. Connect and enter the password when prompted:
   ```bash
   ssh root@your-server-ip
   ```

After you're in, consider adding an SSH key and disabling password auth for better security.

## Quick Start

1. **Connect to your server as root:**
   ```bash
   ssh root@your-server-ip
   ```

2. **Create `deploy.sh` on the server and open it in nano:**
   ```bash
   nano deploy.sh
   ```

3. **Copy and paste the script contents into nano from one of these sources:**
   - Copy [official repo HEAD deploy.sh](https://github.com/kachurovskiy/stratcraft/blob/master/scripts/deploy.sh) or your own fork
   - Save and exit nano: `Ctrl+X` then `y` and `Enter`

4. **Run the deployment script:**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh deploy
   ```

5. **Follow the prompts:**
   - Enter your domain name (e.g. `example.com`; no protocol or port)
   - Enter your GitHub username (default: `kachurovskiy`)
   - Enter your GitHub repository name (default: `stratcraft`)

## What the Script Does

- Installs Node.js 20.x, Rust/Cargo (engine toolchain), PM2, nginx, and Certbot
- Creates a dedicated `stratcraft` user
- Clones and builds your StratCraft application (HTTPS; SSH deploy keys if needed)
- Configures nginx with SSL (Let's Encrypt)
- Sets up firewall (UFW) and fail2ban security
- Starts the application with PM2
- **Sets up a manual update trigger (Admin -> Deployment)**

## Commands

```bash
# Full deployment (first time)
./deploy.sh deploy

# Update application
./deploy.sh update

# Restart application
./deploy.sh restart

# Check status
./deploy.sh status

# View logs
./deploy.sh logs

# Setup GitHub key only
./deploy.sh setup-key
```

## Manual Updates

The script installs a manual update trigger:
- Use `Admin -> Deployment -> Trigger Server Update` to start an update (checks once per minute).
- Pulls latest changes from GitHub
- Rebuilds the application
- Restarts the service
- Logs all activity to `/var/log/stratcraft-update.log`

## Service Management

```bash
# PM2 commands
pm2 status
pm2 logs stratcraft
pm2 restart stratcraft
pm2 monit

# Nginx commands
nginx -t
systemctl reload nginx
```

## Optional: Client certificate lockdown (mTLS)

StratCraft can optionally tell nginx to require a browser client certificate (mTLS) for **all HTTPS requests** before any request reaches the app. This is separate from StratCraft sign-in (OTP); it only gates network access.

Setup (recommended):

1. Sign in as an admin.
2. Go to `Admin -> Users -> Server Access Lockdown (Client Certificate)`.
3. Click **Generate / Rotate Access Cert**, then **Download .p12** and import it into your browser / OS certificate store (password is set in `Admin -> Settings -> User Access`; default: `stratcraft`).
4. Click **Enable Lockdown**.

Emergency unlock (if you lock yourself out):

```bash
ssh root@your-server-ip
sudo /usr/local/bin/stratcraft-mtls disable
sudo nginx -t && sudo systemctl reload nginx
```

If the helper script is missing, re-run `./deploy.sh update` as root, or disable mTLS manually by setting `ssl_verify_client off;` in `/etc/nginx/stratcraft-mtls.conf` and reloading nginx.

## File Locations

- Application: `/opt/stratcraft/stratcraft`
- Logs: `/opt/stratcraft/stratcraft/logs`
- Configuration: `/opt/stratcraft/stratcraft/.env`
- SSL Certificates: `/etc/letsencrypt/live/your-domain/`

## Troubleshooting

**Service won't start:**
```bash
pm2 logs stratcraft
pm2 status
```

**SSL issues:**
```bash
certbot certificates
nginx -t
```

**Check update logs:**
```bash
tail -f /var/log/stratcraft-update.log
```

## Security Features

- SSL/TLS encryption with Let's Encrypt
- UFW firewall (SSH, HTTP, HTTPS only)
- Fail2ban intrusion prevention
- Security headers
- Rate limiting
- Auto-updating SSL certificates

That's it! Your StratCraft application will be running at `https://your-domain` with manual updates.
