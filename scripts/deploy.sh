#!/bin/bash

# StratCraft Unified Deployment Script
# This script handles complete deployment, updates, and maintenance for StratCraft
#
# Usage:
#   ./deploy.sh [command]
#
# Commands:
#   deploy     - Full initial deployment (default)
#   update     - Pull latest changes and restart
#   restart    - Restart the application
#   status     - Show application status
#   logs       - Show application logs
#   setup-key  - Setup GitHub deploy key only

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Configuration variables
DOMAIN=""
GITHUB_REPO=""
GITHUB_USER=""
APP_USER="stratcraft"
APP_DIR="/opt/stratcraft"
NGINX_DIR="/etc/nginx"
SERVICE_NAME="stratcraft"
LOG_FILE="/var/log/stratcraft-deploy.log"
POSTGRES_DB="stratcraft"
POSTGRES_USER="postgres"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"
POSTGRES_PASSWORD=""
DATABASE_KEY=""

# Function to log messages
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message" >> "$LOG_FILE"
}

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
    log_message "INFO" "$1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
    log_message "SUCCESS" "$1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
    log_message "WARNING" "$1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    log_message "ERROR" "$1"
}

print_header() {
    echo -e "${PURPLE}[HEADER]${NC} $1"
    log_message "HEADER" "$1"
}

# Function to check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        print_error "This script must be run as root"
        exit 1
    fi
}

# Function to get user input
get_input() {
    local prompt="$1"
    local var_name="$2"
    local default="$3"

    if [[ -n "$default" ]]; then
        read -p "$prompt [$default]: " input
        eval "$var_name=\${input:-$default}"
    else
        read -p "$prompt: " input
        eval "$var_name=\"$input\""
    fi
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}



# Function to setup GitHub deploy key
setup_github_deploy_key() {
    print_status "Setting up GitHub deploy key..."

    # Create .ssh directory for app user
    sudo -u "$APP_USER" mkdir -p "$APP_DIR/.ssh"
    sudo -u "$APP_USER" chmod 700 "$APP_DIR/.ssh"

    # Check if deploy key file exists
    if [[ -f "/tmp/stratcraft_deploy_key" ]]; then
        # Copy deploy key from temporary location and set proper ownership/permissions
        cp /tmp/stratcraft_deploy_key "$APP_DIR/.ssh/id_ed25519"
        chown "$APP_USER:$APP_USER" "$APP_DIR/.ssh/id_ed25519"
        chmod 600 "$APP_DIR/.ssh/id_ed25519"
        print_success "Deploy key copied from /tmp/stratcraft_deploy_key"
    elif [[ -f "$APP_DIR/.ssh/id_ed25519" ]]; then
        print_success "Deploy key already exists, skipping key setup"
    else
        print_warning "Deploy key not found! Generating new key..."
        generate_ssh_key_pair
    fi

    # Add GitHub to known_hosts
    sudo -u "$APP_USER" ssh-keyscan github.com >> "$APP_DIR/.ssh/known_hosts" 2>/dev/null || true

    print_success "GitHub deploy key configured"
}

# Function to generate SSH key pair
generate_ssh_key_pair() {
    print_status "Generating SSH key pair for GitHub deployment..."

    # Generate ED25519 key pair
    ssh-keygen -t ed25519 -f /tmp/stratcraft_deploy_key -N "" -C "stratcraft-deploy-$(date +%Y-%m-%dT%H:%M:%S.%3NZ)"

    if [[ $? -eq 0 ]]; then
        print_success "SSH key pair generated successfully"

        # Copy to app user
        cp /tmp/stratcraft_deploy_key "$APP_DIR/.ssh/id_ed25519"
        chown "$APP_USER:$APP_USER" "$APP_DIR/.ssh/id_ed25519"
        chmod 600 "$APP_DIR/.ssh/id_ed25519"

        # Display the public key
        echo
        print_status "Generated Public Key (copy this to GitHub):"
        echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
        cat /tmp/stratcraft_deploy_key.pub
        echo -e "${YELLOW}═══════════════════════════════════════════════════════════════${NC}"
        echo

        print_warning "IMPORTANT: Add this public key to your GitHub repository's deploy keys!"
        print_status "Go to: https://github.com/$GITHUB_USER/$GITHUB_REPO/settings/keys"
        print_status "Click 'Add deploy key' and paste the key above"

        read -p "Press Enter after adding the key to GitHub..."
    else
        print_error "Failed to generate SSH key pair"
        exit 1
    fi
}

# Function to update system packages
update_system() {
    print_status "Updating system packages..."
    apt update && apt upgrade -y
    apt install -y curl wget git unzip openssl software-properties-common apt-transport-https ca-certificates gnupg lsb-release build-essential pkg-config
    print_success "System packages updated"
}

# Function to install Node.js
install_nodejs() {
    print_status "Installing Node.js 20.x..."

    # Add NodeSource repository
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt install -y nodejs

    # Verify installation
    node_version=$(node --version)
    npm_version=$(npm --version)
    print_success "Node.js $node_version and npm $npm_version installed"
}

# Function to install Rust toolchain for engine builds
install_rust_toolchain() {
    print_status "Ensuring Rust toolchain is installed for $APP_USER..."

    if ! id "$APP_USER" &>/dev/null; then
        print_error "Application user not found: $APP_USER"
        exit 1
    fi

    local cargo_bin="$APP_DIR/.cargo/bin/cargo"
    local rustup_bin="$APP_DIR/.cargo/bin/rustup"
    local rust_env=(HOME="$APP_DIR" CARGO_HOME="$APP_DIR/.cargo" RUSTUP_HOME="$APP_DIR/.rustup")

    if [[ ! -x "$cargo_bin" ]]; then
        print_status "Rust not found; installing via rustup..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sudo -u "$APP_USER" env "${rust_env[@]}" sh -s -- -y --profile minimal --default-toolchain stable
    else
        print_status "Rust already installed; ensuring stable toolchain is available..."
    fi

    if [[ -x "$rustup_bin" ]]; then
        sudo -u "$APP_USER" env "${rust_env[@]}" "$rustup_bin" toolchain install stable --profile minimal >/dev/null
        sudo -u "$APP_USER" env "${rust_env[@]}" "$rustup_bin" default stable >/dev/null
    fi

    if [[ ! -x "$cargo_bin" ]]; then
        print_error "Cargo binary not found after Rust installation"
        exit 1
    fi

    ln -sf "$cargo_bin" /usr/local/bin/cargo

    local cargo_version
    cargo_version=$(sudo -u "$APP_USER" env "${rust_env[@]}" "$cargo_bin" --version 2>/dev/null || true)
    if [[ -n "$cargo_version" ]]; then
        print_success "Rust toolchain ready ($cargo_version)"
    else
        print_success "Rust toolchain ready"
    fi
}

# Function to install PM2
install_pm2() {
    print_status "Installing PM2 process manager..."

    # Install PM2 globally
    npm install -g pm2

    # Verify installation
    pm2_version=$(pm2 --version)
    print_success "PM2 $pm2_version installed"
}

# Function to setup PM2 for the application user
setup_pm2_user() {
    print_status "Setting up PM2 for application user..."

    # Create PM2 home directory for the user
    sudo -u "$APP_USER" mkdir -p "$APP_DIR/.pm2"

    # Set proper permissions
    chown -R "$APP_USER:$APP_USER" "$APP_DIR/.pm2"

    # Initialize PM2 daemon for the user
    sudo -u "$APP_USER" pm2 ping 2>/dev/null || true

    print_success "PM2 setup completed for $APP_USER"
}

# Function to install nginx
install_nginx() {
    print_status "Installing nginx..."
    apt install -y nginx

    # Start and enable nginx
    systemctl start nginx
    systemctl enable nginx

    print_success "Nginx installed and started"
}

# Function to install Certbot for SSL
install_certbot() {
    print_status "Installing Certbot for SSL certificates..."

    # Install snapd if not present
    apt install -y snapd
    snap install core; snap refresh core
    snap install --classic certbot

    # Create symlink
    ln -sf /snap/bin/certbot /usr/bin/certbot

    print_success "Certbot installed"
}

# Function to install PostgreSQL
install_postgres() {
    print_status "Installing PostgreSQL..."

    if dpkg -s postgresql >/dev/null 2>&1; then
        print_warning "PostgreSQL already installed, ensuring service is running"
    else
        apt install -y postgresql postgresql-contrib
        print_success "PostgreSQL packages installed"
    fi

    systemctl enable postgresql
    systemctl start postgresql

    print_success "PostgreSQL service is available"
}

install_lightgbm() {
    print_status "Installing LightGBM CLI..."

    if command_exists lightgbm; then
        print_success "LightGBM CLI already installed"
        return
    fi

    if apt install -y lightgbm libgomp1; then
        if command_exists lightgbm; then
            print_success "LightGBM CLI installed via apt"
            return
        fi
    fi

    print_warning "LightGBM apt package unavailable; building from source"
    apt install -y build-essential cmake git libgomp1

    local tmp_dir="/tmp/lightgbm-src"
    rm -rf "$tmp_dir"
    git clone --depth 1 --recurse-submodules --shallow-submodules https://github.com/microsoft/LightGBM.git "$tmp_dir"
    cmake -S "$tmp_dir" -B "$tmp_dir/build" -DUSE_GPU=0
    cmake --build "$tmp_dir/build" --target lightgbm -j"$(nproc)"
    local built_bin=""
    if [[ -f "$tmp_dir/build/lightgbm" ]]; then
        built_bin="$tmp_dir/build/lightgbm"
    elif [[ -f "$tmp_dir/lightgbm" ]]; then
        built_bin="$tmp_dir/lightgbm"
    else
        built_bin=$(find "$tmp_dir" -maxdepth 3 -type f -name lightgbm -perm -u+x 2>/dev/null | head -n 1 || true)
    fi

    if [[ -z "$built_bin" || ! -f "$built_bin" ]]; then
        print_error "LightGBM build succeeded but binary was not found"
        rm -rf "$tmp_dir"
        exit 1
    fi

    cp "$built_bin" /usr/local/bin/lightgbm
    chmod +x /usr/local/bin/lightgbm
    rm -rf "$tmp_dir"

    if command_exists lightgbm; then
        print_success "LightGBM CLI installed from source"
        return
    fi

    print_error "Failed to install LightGBM CLI"
    exit 1
}

generate_random_password() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 24
        return 0
    fi

    tr -dc 'A-Za-z0-9' </dev/urandom | head -c 48
}

generate_database_key() {
    if command -v openssl >/dev/null 2>&1; then
        openssl rand -hex 32
        return 0
    fi

    head -c 32 </dev/urandom | od -An -tx1 | tr -d ' \n'
}

extract_password_from_database_url() {
    local database_url="$1"
    local credentials="${database_url#*://}"
    credentials="${credentials%%@*}"
    if [[ "$credentials" != *:* ]]; then
        return 1
    fi
    local password="${credentials#*:}"
    if [[ -z "$password" ]]; then
        return 1
    fi
    echo "$password"
}

configure_postgres_credentials() {
    print_status "Configuring PostgreSQL credentials..."

    local env_file="$APP_DIR/stratcraft/.env"
    if [[ -z "$POSTGRES_PASSWORD" && -f "$env_file" ]]; then
        local existing_url
        existing_url=$(grep -E '^DATABASE_URL=' "$env_file" | tail -n 1 | cut -d= -f2-)
        if [[ -n "$existing_url" ]]; then
            local extracted
            extracted=$(extract_password_from_database_url "$existing_url" || true)
            if [[ -n "$extracted" ]]; then
                POSTGRES_PASSWORD="$extracted"
                print_status "Using PostgreSQL password from existing .env"
            fi
        fi
    fi

    if [[ -z "$POSTGRES_PASSWORD" ]]; then
        POSTGRES_PASSWORD=$(generate_random_password)
        print_status "Generated random PostgreSQL password"
    fi

    sudo -u postgres psql -tAc "ALTER USER ${POSTGRES_USER} WITH PASSWORD '${POSTGRES_PASSWORD}';" >/dev/null

    local db_exists
    db_exists=$(sudo -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'")
    if [[ "$db_exists" != "1" ]]; then
        sudo -u postgres createdb "$POSTGRES_DB"
        print_success "Database ${POSTGRES_DB} created"
    else
        print_status "Database ${POSTGRES_DB} already exists"
    fi

    print_success "PostgreSQL credentials configured"
}

# Function to create application user
create_app_user() {
    print_status "Creating application user: $APP_USER"

    if ! id "$APP_USER" &>/dev/null; then
        useradd -r -s /bin/bash -d "$APP_DIR" -m "$APP_USER"
        print_success "User $APP_USER created"
    else
        print_warning "User $APP_USER already exists"
    fi
}

# Function to clone or update repository
clone_or_update_repository() {
    print_status "Checking StratCraft repository..."

    # Check if repository already exists
    if [[ -d "$APP_DIR/stratcraft" ]]; then
        print_success "Repository already exists, updating..."
        cd "$APP_DIR/stratcraft"

        # Update existing repository
        sudo -u "$APP_USER" git fetch origin

        local default_branch_ref
        local default_branch
        default_branch_ref=$(sudo -u "$APP_USER" git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null || true)
        default_branch=${default_branch_ref##*/}
        if [[ -z "$default_branch" ]]; then
            default_branch="master"
        fi

        sudo -u "$APP_USER" git reset --hard "origin/$default_branch"

        # Install/update dependencies (including dev dependencies for build)
        print_status "Installing/updating Node.js dependencies..."
        sudo -u "$APP_USER" npm install --include=dev

        # Build the application (use build for production)
        print_status "Building StratCraft application..."
        sudo -u "$APP_USER" npm run build

        # Ensure data directory exists
        sudo -u "$APP_USER" mkdir -p "$APP_DIR/stratcraft/data"

        print_success "Repository updated and built"
    else
        print_status "Cloning StratCraft repository..."

        local https_clone_url="https://github.com/$GITHUB_USER/$GITHUB_REPO.git"
        local ssh_clone_url="git@github.com:$GITHUB_USER/$GITHUB_REPO.git"

        # Prefer HTTPS for quick deploys (public repos) and fall back to SSH deploy key when needed.
        if sudo -u "$APP_USER" git clone "$https_clone_url" "$APP_DIR/stratcraft"; then
            print_success "Repository cloned via HTTPS"
        else
            print_warning "HTTPS clone failed; falling back to SSH deploy key..."
            setup_github_deploy_key
            sudo -u "$APP_USER" git clone "$ssh_clone_url" "$APP_DIR/stratcraft"
            print_success "Repository cloned via SSH"
        fi

        # Change to app directory
        cd "$APP_DIR/stratcraft"

        # Install dependencies (including dev dependencies for build)
        print_status "Installing Node.js dependencies..."
        sudo -u "$APP_USER" npm install --include=dev

        # Build the application (use build for production)
        print_status "Building StratCraft application..."
        sudo -u "$APP_USER" npm run build

        # Create data directory
        sudo -u "$APP_USER" mkdir -p "$APP_DIR/stratcraft/data"

        print_success "Repository cloned and built"
    fi
}

# Function to create PM2 configuration
create_pm2_config() {
    print_status "Creating/updating PM2 configuration..."

    # Create PM2 ecosystem file (overwrite if exists)
    cat > "$APP_DIR/stratcraft/ecosystem.config.js" << EOF
module.exports = {
  apps: [{
    name: '$SERVICE_NAME',
    script: 'dist/server/server.js',
    cwd: '$APP_DIR/stratcraft',
    instances: 1,
    exec_mode: 'fork',
    autorestart: true,
    watch: false,
    max_memory_restart: '100G',
    env: {
      NODE_ENV: 'production',
      SERVER_HOST: '127.0.0.1',
      SERVER_PORT: 3000
    },
    error_file: '$APP_DIR/stratcraft/logs/err.log',
    out_file: '$APP_DIR/stratcraft/logs/out.log',
    log_file: '$APP_DIR/stratcraft/logs/combined.log',
    time: true,
    log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
    merge_logs: true,
    max_restarts: 10,
    min_uptime: '10s',
    restart_delay: 4000
  }]
};
EOF

    # Set proper ownership
    chown "$APP_USER:$APP_USER" "$APP_DIR/stratcraft/ecosystem.config.js"

    print_success "PM2 configuration created"
}

# Function to configure nginx (initial setup without SSL)
configure_nginx() {
    print_status "Configuring nginx (initial setup)..."

    # Remove default nginx site
    rm -f "$NGINX_DIR/sites-enabled/default"

    # Create initial nginx configuration (HTTP only, for Let's Encrypt)
    cat > "$NGINX_DIR/sites-available/$SERVICE_NAME" << EOF
# Rate limiting and connection limiting
limit_req_zone \$binary_remote_addr zone=api:10m rate=10r/s;
limit_req_zone \$binary_remote_addr zone=login:10m rate=5r/m;
limit_conn_zone \$binary_remote_addr zone=perip:10m;
limit_req_status 429;
limit_req_log_level warn;
limit_conn_status 429;

# Upstream for StratCraft
upstream stratcraft {
    server 127.0.0.1:3000;
    keepalive 32;
}

# HTTP server (will be updated to HTTPS after SSL setup)
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;
    server_tokens off;
    limit_conn perip 20;
    keepalive_timeout 15s;
    keepalive_requests 100;
    client_body_timeout 30s;
    client_header_timeout 30s;
    send_timeout 30s;

    # Let's Encrypt challenge (must come before main location block)
    location /.well-known/acme-challenge/ {
        root /var/www/html;
        try_files \$uri =404;
    }

    # Main application
    location / {
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_cache_bypass \$http_upgrade;
    }

    # API rate limiting
    location /api/ {
        limit_req zone=api burst=20 nodelay;
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Health check endpoint
    location /api/health {
        limit_req zone=api burst=5 nodelay;
        proxy_pass http://stratcraft;
        access_log off;
    }

    # Auth endpoints (OTP/login/invite/logout)
    location /auth/ {
        limit_req zone=login burst=5 nodelay;
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

    # Ensure client-cert include file exists (referenced by the nginx config above)
    if [[ ! -f "$NGINX_DIR/stratcraft-mtls.conf" ]]; then
        cat > "$NGINX_DIR/stratcraft-mtls.conf" << 'EOF_MTLS'
# Managed by StratCraft - mutual TLS client certificate gate (disabled)
ssl_verify_client off;
EOF_MTLS
        chmod 644 "$NGINX_DIR/stratcraft-mtls.conf"
    fi

    # Enable the site
    ln -sf "$NGINX_DIR/sites-available/$SERVICE_NAME" "$NGINX_DIR/sites-enabled/"

    # Test nginx configuration
    nginx -t

    print_success "Nginx configured (initial HTTP setup)"
}

# Function to update nginx with SSL configuration
update_nginx_ssl() {
    print_status "Updating nginx configuration with SSL..."

    # Create full nginx configuration with SSL
    cat > "$NGINX_DIR/sites-available/$SERVICE_NAME" << EOF
# Rate limiting and connection limiting
limit_req_zone \$binary_remote_addr zone=api:10m rate=10r/s;
limit_req_zone \$binary_remote_addr zone=login:10m rate=5r/m;
limit_conn_zone \$binary_remote_addr zone=perip:10m;
limit_req_status 429;
limit_req_log_level warn;
limit_conn_status 429;

# Upstream for StratCraft
upstream stratcraft {
    server 127.0.0.1:3000;
    keepalive 32;
}

# HTTP to HTTPS redirect
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN;
    server_tokens off;
    limit_conn perip 20;
    keepalive_timeout 15s;
    keepalive_requests 100;
    client_body_timeout 30s;
    client_header_timeout 30s;
    send_timeout 30s;

    # Let's Encrypt challenge (must come before redirect)
    location /.well-known/acme-challenge/ {
        root /var/www/html;
        try_files \$uri =404;
    }

    # Redirect all other traffic to HTTPS
    location / {
        return 301 https://\$server_name\$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl http2;
    listen [::]:443 ssl http2;
    server_name $DOMAIN;
    server_tokens off;
    limit_conn perip 20;
    keepalive_timeout 15s;
    keepalive_requests 100;

    # SSL configuration
    ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;
    ssl_trusted_certificate /etc/letsencrypt/live/$DOMAIN/chain.pem;

    # Optional mutual TLS client certificate gate (managed by StratCraft)
    include $NGINX_DIR/stratcraft-mtls.conf;

    # SSL security settings
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA512:DHE-RSA-AES256-GCM-SHA512:ECDHE-RSA-AES256-GCM-SHA384:DHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-SHA384;
    ssl_prefer_server_ciphers off;
    ssl_session_cache shared:SSL:10m;
    ssl_session_timeout 10m;
    ssl_stapling on;
    ssl_stapling_verify on;

    # Security headers
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains" always;
    add_header X-Frame-Options DENY always;
    add_header X-Content-Type-Options nosniff always;
    add_header X-XSS-Protection "1; mode=block" always;
    add_header Referrer-Policy "strict-origin-when-cross-origin" always;
    add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.jsdelivr.net https://s3.tradingview.com; style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; img-src 'self' data: https:; font-src 'self' https://cdn.jsdelivr.net; connect-src 'self'; frame-src 'self' https://s.tradingview.com;" always;

    # Gzip compression
    gzip on;
    gzip_vary on;
    gzip_min_length 1024;
    gzip_proxied any;
    gzip_comp_level 6;
    gzip_types
        text/plain
        text/css
        text/xml
        text/javascript
        application/json
        application/javascript
        application/xml+rss
        application/atom+xml
        image/svg+xml;

    # Client settings
    client_max_body_size 10M;
    client_body_timeout 30s;
    client_header_timeout 30s;
    send_timeout 30s;

    # Proxy settings
    proxy_connect_timeout 60s;
    proxy_send_timeout 60s;
    proxy_read_timeout 60s;
    proxy_buffering on;
    proxy_buffer_size 4k;
    proxy_buffers 8 4k;

    # Main application
    location / {
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection 'upgrade';
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_cache_bypass \$http_upgrade;
    }

    # API rate limiting
    location /api/ {
        limit_req zone=api burst=20 nodelay;
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }

    # Static files caching
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff|woff2|ttf|eot)$ {
        proxy_pass http://stratcraft;
        proxy_cache_valid 200 1d;
        proxy_cache_valid 404 1m;
        add_header Cache-Control "public, immutable";
        expires 1y;
    }

    # Health check endpoint
    location /api/health {
        limit_req zone=api burst=5 nodelay;
        proxy_pass http://stratcraft;
        access_log off;
    }

    # Auth endpoints (OTP/login/invite/logout)
    location /auth/ {
        limit_req zone=login burst=5 nodelay;
        proxy_pass http://stratcraft;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

    # Enable the site
    ln -sf "$NGINX_DIR/sites-available/$SERVICE_NAME" "$NGINX_DIR/sites-enabled/"

    print_success "Nginx configured with SSL"
}

# Function to setup the nginx mTLS helper used by the admin panel
setup_client_cert_lockdown() {
    print_status "Setting up nginx client-certificate lockdown helper..."

    # Ensure client-cert include file exists
    if [[ ! -f "$NGINX_DIR/stratcraft-mtls.conf" ]]; then
        cat > "$NGINX_DIR/stratcraft-mtls.conf" << 'EOF_MTLS'
# Managed by StratCraft - mutual TLS client certificate gate (disabled)
ssl_verify_client off;
EOF_MTLS
        chmod 644 "$NGINX_DIR/stratcraft-mtls.conf"
    fi

    # Ensure app-side certificate directory exists (generated via the admin panel)
    mkdir -p "$APP_DIR/stratcraft/.mtls"
    chown "$APP_USER:$APP_USER" "$APP_DIR/stratcraft/.mtls"
    chmod 700 "$APP_DIR/stratcraft/.mtls"

    # Install helper script (invoked via sudo from the web app)
    cat > "/usr/local/bin/stratcraft-mtls" << 'EOF'
#!/bin/bash
set -euo pipefail

SERVICE_NAME="stratcraft"
APP_DIR="/opt/stratcraft"
SITE_PATH="/etc/nginx/sites-available/${SERVICE_NAME}"
INCLUDE_PATH="/etc/nginx/stratcraft-mtls.conf"
CA_CERT_PATH="${APP_DIR}/stratcraft/.mtls/ca.crt"

ensure_conf_file() {
  if [[ ! -f "$INCLUDE_PATH" ]]; then
    cat > "$INCLUDE_PATH" << 'EOF_MTLS'
# Managed by StratCraft - mutual TLS client certificate gate (disabled)
ssl_verify_client off;
EOF_MTLS
    chmod 644 "$INCLUDE_PATH"
  fi
}

ensure_include() {
  if [[ ! -f "$SITE_PATH" ]]; then
    echo "nginx site config missing: $SITE_PATH" >&2
    exit 1
  fi

  if grep -qF "include ${INCLUDE_PATH};" "$SITE_PATH"; then
    return 0
  fi

  if ! grep -q "ssl_trusted_certificate" "$SITE_PATH"; then
    echo "Could not find ssl_trusted_certificate in $SITE_PATH. Add this inside the HTTPS server block:" >&2
    echo "  include ${INCLUDE_PATH};" >&2
    exit 1
  fi

  tmp_file=$(mktemp)
  awk -v include_path="${INCLUDE_PATH}" '
    !done && $0 ~ /ssl_trusted_certificate/ {
      print;
      print "";
      print "    # Optional mutual TLS client certificate gate (managed by StratCraft)";
      print "    include " include_path ";";
      done=1;
      next;
    }
    { print }
  ' "$SITE_PATH" > "$tmp_file"

  cat "$tmp_file" > "$SITE_PATH"
  rm -f "$tmp_file"
}

enable_lockdown() {
  ensure_conf_file
  ensure_include

  if [[ ! -f "$CA_CERT_PATH" ]]; then
    echo "CA certificate not found: $CA_CERT_PATH" >&2
    echo "Generate it first in StratCraft: Admin -> Users -> Server Access Lockdown." >&2
    exit 1
  fi

  cat > "$INCLUDE_PATH" << EOF_MTLS
# Managed by StratCraft - mutual TLS client certificate gate
ssl_verify_client on;
ssl_client_certificate ${CA_CERT_PATH};
ssl_verify_depth 2;
EOF_MTLS

  chmod 644 "$INCLUDE_PATH"
  nginx -t
  systemctl reload nginx
  echo "enabled"
}

disable_lockdown() {
  ensure_conf_file
  ensure_include

  cat > "$INCLUDE_PATH" << 'EOF_MTLS'
# Managed by StratCraft - mutual TLS client certificate gate (disabled)
ssl_verify_client off;
EOF_MTLS

  chmod 644 "$INCLUDE_PATH"
  nginx -t
  systemctl reload nginx
  echo "disabled"
}

print_status() {
  if [[ -f "$INCLUDE_PATH" ]] && grep -q "ssl_verify_client on;" "$INCLUDE_PATH"; then
    echo "enabled"
  else
    echo "disabled"
  fi
}

bootstrap() {
  ensure_conf_file
  ensure_include
  nginx -t
  systemctl reload nginx
  echo "bootstrapped"
}

case "${1:-}" in
  "enable")
    enable_lockdown
    ;;
  "disable")
    disable_lockdown
    ;;
  "status")
    print_status
    ;;
  "bootstrap")
    bootstrap
    ;;
  *)
    echo "Usage: stratcraft-mtls {enable|disable|status|bootstrap}" >&2
    exit 2
    ;;
esac
EOF

    chmod +x "/usr/local/bin/stratcraft-mtls"

    # Allow the app user to run the helper without a password
    cat > "/etc/sudoers.d/stratcraft-mtls" << 'EOF'
stratcraft ALL=(root) NOPASSWD: /usr/local/bin/stratcraft-mtls status, /usr/local/bin/stratcraft-mtls enable, /usr/local/bin/stratcraft-mtls disable, /usr/local/bin/stratcraft-mtls bootstrap
EOF
    chmod 440 "/etc/sudoers.d/stratcraft-mtls"

    if ! /usr/local/bin/stratcraft-mtls bootstrap >/dev/null 2>&1; then
        print_warning "Could not auto-patch nginx config for mTLS include. You can still add it manually to your nginx HTTPS server: include $NGINX_DIR/stratcraft-mtls.conf;"
    fi

    print_success "nginx client-certificate lockdown helper installed"
}

# Function to setup SSL certificates
setup_ssl() {
    print_status "Setting up SSL certificates with Let's Encrypt..."

    # Create web root for ACME challenge
    mkdir -p /var/www/html
    chown -R www-data:www-data /var/www/html
    chmod -R 755 /var/www/html

    # Get SSL certificate (skip if already exists)
    if [[ ! -f "/etc/letsencrypt/live/$DOMAIN/fullchain.pem" ]]; then
        certbot certonly --webroot -w /var/www/html -d "$DOMAIN" --non-interactive --agree-tos --email "admin@$DOMAIN"
    else
        print_success "SSL certificate already exists, skipping certificate generation"
    fi

    # Setup auto-renewal (only if not already set)
    if ! crontab -l 2>/dev/null | grep -q "certbot renew"; then
        echo "0 12 * * * /usr/bin/certbot renew --quiet" | crontab -
        print_success "SSL auto-renewal cron job added"
    else
        print_success "SSL auto-renewal cron job already exists"
    fi

    # Update nginx configuration with SSL
    update_nginx_ssl

    # Test and reload nginx with new SSL configuration
    nginx -t
    systemctl reload nginx

    print_success "SSL certificates configured with auto-renewal"
}

# Function to configure firewall
configure_firewall() {
    print_status "Configuring UFW firewall..."

    # Reset UFW to defaults
    ufw --force reset

    # Set default policies
    ufw default deny incoming
    ufw default allow outgoing

    # Allow SSH (be careful with this!)
    ufw allow ssh

    # Allow HTTP and HTTPS
    ufw allow 80/tcp
    ufw allow 443/tcp

    # Enable firewall
    ufw --force enable

    print_success "Firewall configured"
}

# Function to install and configure fail2ban
setup_fail2ban() {
    print_status "Installing and configuring fail2ban..."

    apt install -y fail2ban

    # Create fail2ban configuration
    cat > /etc/fail2ban/jail.local << EOF
[DEFAULT]
bantime = 3600
findtime = 600
maxretry = 5
backend = systemd

[sshd]
enabled = true
port = ssh
logpath = %(sshd_log)s
backend = %(sshd_backend)s

[nginx-http-auth]
enabled = true
filter = nginx-http-auth
port = http,https
logpath = /var/log/nginx/error.log

[nginx-limit-req]
enabled = true
filter = nginx-limit-req
port = http,https
logpath = /var/log/nginx/error.log
maxretry = 10
EOF

    # Start and enable fail2ban
    systemctl start fail2ban
    systemctl enable fail2ban

    print_success "Fail2ban configured"
}

# Function to setup log rotation
setup_log_rotation() {
    print_status "Setting up log rotation..."

    cat > "/etc/logrotate.d/$SERVICE_NAME" << EOF
$APP_DIR/stratcraft/logs/*.log {
    daily
    missingok
    rotate 30
    compress
    delaycompress
    notifempty
    create 644 $APP_USER $APP_USER
    postrotate
        systemctl reload $SERVICE_NAME > /dev/null 2>&1 || true
    endscript
}
EOF

    print_success "Log rotation configured"
}

# Function to create environment file
create_environment_file() {
    print_status "Creating environment configuration..."
    local env_file="$APP_DIR/stratcraft/.env"
    local database_url="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}"
    local database_key="$DATABASE_KEY"
    local generated_database_key="false"

    # Check if .env already exists
    local domain=""
    if [[ -n "$DOMAIN" ]]; then
        domain="$DOMAIN"
    fi

    if [[ -f "$env_file" ]]; then
        print_warning "Environment file already exists, preserving existing configuration"
        if [[ -z "$database_key" ]]; then
            local existing_key
            existing_key=$(grep -E '^DATABASE_KEY=' "$env_file" | tail -n 1 | cut -d= -f2-)
            if [[ -n "$existing_key" ]]; then
                database_key="$existing_key"
                print_status "Using DATABASE_KEY from existing .env"
            fi
        fi
        if [[ -z "$database_key" ]]; then
            database_key=$(generate_database_key)
            print_status "Generated DATABASE_KEY"
            generated_database_key="true"
        fi
        if ! grep -q '^DATABASE_URL=' "$env_file"; then
            echo "DATABASE_URL=$database_url" >> "$env_file"
            chown "$APP_USER:$APP_USER" "$env_file"
            chmod 600 "$env_file"
            print_success "DATABASE_URL added to existing environment file"
        fi
        if [[ -n "$domain" ]] && ! grep -q '^DOMAIN=' "$env_file"; then
            echo "DOMAIN=$domain" >> "$env_file"
            chown "$APP_USER:$APP_USER" "$env_file"
            chmod 600 "$env_file"
            print_success "DOMAIN added to existing environment file"
        fi
        if ! grep -q '^DATABASE_KEY=' "$env_file"; then
            echo "DATABASE_KEY=$database_key" >> "$env_file"
            chown "$APP_USER:$APP_USER" "$env_file"
            chmod 600 "$env_file"
            print_success "DATABASE_KEY added to existing environment file"
        fi
        if [[ "$generated_database_key" == "true" ]]; then
            echo
            echo -e "${YELLOW}[IMPORTANT]${NC} Save this DATABASE_KEY in a safe place."
            echo -e "${YELLOW}[IMPORTANT]${NC} It is required to decrypt secrets when restoring the database."
            echo -e "${YELLOW}[IMPORTANT]${NC} DATABASE_KEY=${database_key}"
            echo
        fi
        return 0
    fi

    if [[ -z "$database_key" ]]; then
        database_key=$(generate_database_key)
        print_status "Generated DATABASE_KEY"
        generated_database_key="true"
    fi

    cat > "$env_file" <<EOF
# Production Environment Configuration
NODE_ENV=production
SERVER_HOST=127.0.0.1
SERVER_PORT=3000
DATABASE_URL=$database_url
DOMAIN=$domain
DATABASE_KEY=$database_key
EOF

    # Set proper ownership
    chown "$APP_USER:$APP_USER" "$env_file"
    chmod 600 "$env_file"

    print_success "Environment file created"

    if [[ "$generated_database_key" == "true" ]]; then
        echo
        echo -e "${YELLOW}[IMPORTANT]${NC} Save this DATABASE_KEY in a safe place."
        echo -e "${YELLOW}[IMPORTANT]${NC} It is required to decrypt secrets when restoring the database."
        echo -e "${YELLOW}[IMPORTANT]${NC} DATABASE_KEY=${database_key}"
        echo
    fi
}

# Function to install update script (used by manual update trigger)
setup_update_script() {
    print_status "Installing update script for manual triggers..."

    # Create update script
    cat > "/usr/local/bin/stratcraft-update.sh" << 'EOF'
#!/bin/bash

# StratCraft Update Script
# This script is called by the manual update trigger to update the application

LOG_FILE="/var/log/stratcraft-update.log"
APP_DIR="/opt/stratcraft"
APP_USER="stratcraft"
SERVICE_NAME="stratcraft"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log_message "Starting update check..."

# Check if we're already updating
if [[ -f "/tmp/stratcraft-updating" ]]; then
    log_message "Update already in progress, skipping"
    exit 0
fi

# Create lock file
touch /tmp/stratcraft-updating

# Change to app directory
cd "$APP_DIR/stratcraft" || {
    log_message "ERROR: Cannot access app directory"
    rm -f /tmp/stratcraft-updating
    exit 1
}

# Check for updates
sudo -u "$APP_USER" git fetch origin >/dev/null 2>&1

# Check if there are updates
LOCAL=$(sudo -u "$APP_USER" git rev-parse HEAD)
DEFAULT_BRANCH_REF=$(sudo -u "$APP_USER" git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null || true)
DEFAULT_BRANCH="${DEFAULT_BRANCH_REF##*/}"
if [[ -z "$DEFAULT_BRANCH" ]]; then
    DEFAULT_BRANCH="master"
fi
REMOTE=$(sudo -u "$APP_USER" git rev-parse "origin/$DEFAULT_BRANCH")

if [[ "$LOCAL" == "$REMOTE" ]]; then
    log_message "No updates available; restarting service anyway"
    sudo -u "$APP_USER" pm2 restart "$SERVICE_NAME"
    sleep 5
    if sudo -u "$APP_USER" pm2 list | grep -q "$SERVICE_NAME.*online"; then
        log_message "Service restarted successfully with no code changes"
    else
        log_message "ERROR: Service failed to restart without updates"
    fi
    rm -f /tmp/stratcraft-updating
    exit 0
fi

log_message "Updates found, starting deployment..."


# Update repository
sudo -u "$APP_USER" git reset --hard "origin/$DEFAULT_BRANCH"

# Install dependencies
sudo -u "$APP_USER" npm install --include=dev

# Build application
sudo -u "$APP_USER" npm run build

# Restart service
sudo -u "$APP_USER" pm2 restart "$SERVICE_NAME"

# Wait for service to start
sleep 10

# Check if service is running
if sudo -u "$APP_USER" pm2 list | grep -q "$SERVICE_NAME.*online"; then
    log_message "Update completed successfully"

else
    log_message "ERROR: Service failed to start after update"
fi

# Remove lock file
rm -f /tmp/stratcraft-updating
EOF

    chmod +x "/usr/local/bin/stratcraft-update.sh"

    print_success "Update script installed: /usr/local/bin/stratcraft-update.sh"
}

# Function to setup manual update trigger cron job
setup_manual_update() {
    print_status "Setting up manual update trigger..."

    setup_update_script

    # Create manual update check script
    cat > "/usr/local/bin/stratcraft-manual-update-check.sh" << 'EOF'
#!/bin/bash
TRIGGER_FILE="/tmp/stratcraft-manual-update-trigger"
UPDATE_SCRIPT="/usr/local/bin/stratcraft-update.sh"
LOG_FILE="/var/log/stratcraft-update.log"

log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

if [[ -f "$TRIGGER_FILE" ]]; then
    log_message "Manual update trigger found. Removing trigger and starting update."
    rm -f "$TRIGGER_FILE"

    # Ensure the update script is executable
    if [[ -x "$UPDATE_SCRIPT" ]]; then
        # Execute the update script in the background to not hold up the cron job
        $UPDATE_SCRIPT &
    else
        log_message "ERROR: Update script not found or not executable: $UPDATE_SCRIPT"
    fi
fi
EOF

    chmod +x "/usr/local/bin/stratcraft-manual-update-check.sh"

    # Add cron job (check for trigger file every minute)
    (crontab -l 2>/dev/null | grep -v -F "stratcraft-manual-update-check.sh" ; echo "* * * * * /usr/local/bin/stratcraft-manual-update-check.sh") | crontab -
    print_success "Manual update trigger cron job added (runs every minute)"
}

# Function to start services
start_services() {
    print_status "Starting services..."

    # Check if application directory exists
    if [[ ! -d "$APP_DIR/stratcraft" ]]; then
        print_error "Application directory not found: $APP_DIR/stratcraft"
        print_status "Please ensure the repository was cloned successfully"
        exit 1
    fi

    # Check if built application exists
    if [[ ! -f "$APP_DIR/stratcraft/dist/server/server.js" ]]; then
        print_error "Built application not found: $APP_DIR/stratcraft/dist/server/server.js"
        print_status "Please ensure the application was built successfully"
        exit 1
    fi

    # Create logs directory
    sudo -u "$APP_USER" mkdir -p "$APP_DIR/stratcraft/logs"

    # Start or restart StratCraft with PM2
    print_status "Starting StratCraft with PM2..."

    # Check if PM2 process exists (suppress errors)
    if sudo -u "$APP_USER" pm2 list 2>/dev/null | grep -q "$SERVICE_NAME.*online\|$SERVICE_NAME.*stopped"; then
        print_status "Restarting existing PM2 process..."
        sudo -u "$APP_USER" pm2 restart "$SERVICE_NAME" --update-env
    else
        print_status "Starting new PM2 process..."
        sudo -u "$APP_USER" pm2 start "$APP_DIR/stratcraft/ecosystem.config.js"
    fi

    # Save PM2 configuration for auto-start on boot
    sudo -u "$APP_USER" pm2 save

    # Setup PM2 to start on boot (run as root to create systemd service)
    pm2 startup systemd -u "$APP_USER" --hp "$APP_DIR" 2>/dev/null || print_warning "PM2 startup already configured"

    # Reload nginx
    systemctl reload nginx

    # Check service status
    sleep 3  # Give PM2 time to start
    if sudo -u "$APP_USER" pm2 list 2>/dev/null | grep -q "$SERVICE_NAME.*online"; then
        print_success "StratCraft service is running with PM2"

        # Check if application is listening on port 3000
        if netstat -tlnp 2>/dev/null | grep -q ":3000.*LISTEN" || ss -tlnp 2>/dev/null | grep -q ":3000.*LISTEN"; then
            print_success "Application is listening on port 3000"
        else
            print_warning "Application may not be listening on port 3000"
            print_status "Checking if port 3000 is accessible..."
            if curl -s http://127.0.0.1:3000/api/health >/dev/null 2>&1; then
                print_success "Application is responding on port 3000"
            else
                print_warning "Application is not responding on port 3000"
                print_status "Checking PM2 logs for errors..."
                sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --lines 10 2>/dev/null || print_warning "No logs available"
            fi
        fi
    else
        print_error "Failed to start StratCraft service with PM2"
        print_status "Checking PM2 logs..."
        sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --lines 20 2>/dev/null || print_warning "No logs available yet"
        print_status "PM2 process list:"
        sudo -u "$APP_USER" pm2 list 2>/dev/null || print_warning "PM2 list failed"
        exit 1
    fi

    if systemctl is-active --quiet nginx; then
        print_success "Nginx is running"
    else
        print_error "Nginx is not running"
        systemctl status nginx
        exit 1
    fi
}

# Function to cleanup temporary files
cleanup_temp_files() {
    print_status "Cleaning up temporary files..."

    # Remove temporary deploy key files
    if [[ -f "/tmp/stratcraft_deploy_key" ]]; then
        rm -f /tmp/stratcraft_deploy_key
        print_success "Removed temporary private key"
    fi

    if [[ -f "/tmp/stratcraft_deploy_key.pub" ]]; then
        rm -f /tmp/stratcraft_deploy_key.pub
        print_success "Removed temporary public key"
    fi

    print_success "Temporary files cleaned up"
}

# Function to show application status
show_status() {
    print_header "StratCraft Application Status"
    echo

    # PM2 Status
    print_status "PM2 Process Status:"
    sudo -u "$APP_USER" pm2 list 2>/dev/null || print_warning "PM2 not available"
    echo

    # Nginx Status
    print_status "Nginx Status:"
    systemctl status nginx --no-pager -l || print_warning "Nginx not available"
    echo

    # SSL Certificate Status
    if [[ -n "$DOMAIN" ]]; then
        print_status "SSL Certificate Status:"
        certbot certificates 2>/dev/null | grep -A 5 "$DOMAIN" || print_warning "SSL certificate not found"
        echo
    fi

    # Disk Usage
    print_status "Disk Usage:"
    df -h "$APP_DIR" 2>/dev/null || print_warning "Cannot check disk usage"
    echo

    # Memory Usage
    print_status "Memory Usage:"
    free -h
    echo

    # Recent Logs
    print_status "Recent Application Logs (last 10 lines):"
    sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --lines 10 2>/dev/null || print_warning "No logs available"
}

# Function to show logs
show_logs() {
    print_header "StratCraft Application Logs"
    echo
    print_status "Following logs (Ctrl+C to exit):"
    sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --follow 2>/dev/null || print_warning "No logs available"
}

# Function to restart application
restart_application() {
    print_status "Restarting StratCraft application..."

    # Restart PM2 process
    sudo -u "$APP_USER" pm2 restart "$SERVICE_NAME"

    # Wait for service to start
    sleep 5

    # Check status
    if sudo -u "$APP_USER" pm2 list 2>/dev/null | grep -q "$SERVICE_NAME.*online"; then
        print_success "Application restarted successfully"
    else
        print_error "Failed to restart application"
        sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --lines 10
        exit 1
    fi
}

# Function to update application
update_application() {
    print_status "Updating StratCraft application..."

    # Update repository
    cd "$APP_DIR/stratcraft"
    sudo -u "$APP_USER" git fetch origin

    local default_branch_ref
    local default_branch
    default_branch_ref=$(sudo -u "$APP_USER" git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null || true)
    default_branch=${default_branch_ref##*/}
    if [[ -z "$default_branch" ]]; then
        default_branch="master"
    fi
    sudo -u "$APP_USER" git reset --hard "origin/$default_branch"

    # Ensure Rust/Cargo are available for runtime engine compilation jobs
    install_rust_toolchain

    # Install dependencies
    sudo -u "$APP_USER" npm install --include=dev

    # Build application
    sudo -u "$APP_USER" npm run build

    # Restart service
    sudo -u "$APP_USER" pm2 restart "$SERVICE_NAME"

    # Ensure nginx client-cert lockdown helper stays installed (optional)
    if command_exists nginx && [[ -d "$NGINX_DIR" ]]; then
        if ! setup_client_cert_lockdown; then
            print_warning "Failed to (re)install nginx client-cert lockdown helper"
        fi
    fi

    # Wait for service to start
    sleep 5

    # Check status
    if sudo -u "$APP_USER" pm2 list 2>/dev/null | grep -q "$SERVICE_NAME.*online"; then
        print_success "Application updated successfully"
    else
        print_error "Failed to update application"
        sudo -u "$APP_USER" pm2 logs "$SERVICE_NAME" --lines 10
        exit 1
    fi
}

# Function to display final information
display_final_info() {
    echo
    print_success "StratCraft deployment completed successfully!"
    echo
    echo -e "${BLUE}Application Information:${NC}"
    echo "  - Domain: https://$DOMAIN"
    echo "  - Application Directory: $APP_DIR/stratcraft"
    echo "  - Service Name: $SERVICE_NAME"
    echo "  - Application User: $APP_USER"
    echo
    echo -e "${BLUE}Service Management:${NC}"
    echo "  - Start: pm2 start $SERVICE_NAME"
    echo "  - Stop: pm2 stop $SERVICE_NAME"
    echo "  - Restart: pm2 restart $SERVICE_NAME"
    echo "  - Status: pm2 status"
    echo "  - Logs: pm2 logs $SERVICE_NAME"
    echo "  - Monitor: pm2 monit"
    echo
    echo -e "${BLUE}Deployment Script Commands:${NC}"
    echo "  - Update: ./deploy.sh update"
    echo "  - Restart: ./deploy.sh restart"
    echo "  - Status: ./deploy.sh status"
    echo "  - Logs: ./deploy.sh logs"
    echo
    echo -e "${BLUE}Updates:${NC}"
    echo "  - Manual trigger: Admin -> Deployment -> Trigger Server Update"
    echo "  - Update script: /usr/local/bin/stratcraft-update.sh"
    echo "  - Logs: /var/log/stratcraft-update.log"
    echo "  - Auto-updates: Disabled by default"
    echo
    echo -e "${BLUE}Nginx Management:${NC}"
    echo "  - Test config: nginx -t"
    echo "  - Reload: systemctl reload nginx"
    echo "  - Logs: tail -f /var/log/nginx/access.log"
    echo
    echo -e "${BLUE}SSL Certificate:${NC}"
    echo "  - Auto-renewal: Configured via cron"
    echo "  - Manual renewal: certbot renew"
    echo
    echo -e "${BLUE}Security Features:${NC}"
    echo "  - UFW Firewall: Enabled"
    echo "  - Fail2ban: Active"
    echo "  - SSL/TLS: A+ rating configuration"
    echo "  - Security Headers: Implemented"
    echo
    echo -e "${YELLOW}Next Steps:${NC}"
    echo "  1. Visit https://$DOMAIN to access your application"
    echo "  2. Monitor logs: ./deploy.sh logs"
    echo "  3. Update your DNS records if needed"
    echo "  4. Consider setting up monitoring (e.g., UptimeRobot)"
    echo
    print_warning "Remember to keep your server updated regularly!"
}

# Main deployment function
deploy() {
    print_header "Starting StratCraft deployment..."

    # Check if running as root
    check_root

    # Get user input
    echo
    while [[ -z "$DOMAIN" ]]; do
        get_input "Enter your domain name (e.g., example.com)" "DOMAIN"
        if [[ -z "$DOMAIN" ]]; then
            print_warning "Domain is required (used for nginx + Let's Encrypt)"
        fi
    done

    get_input "Enter your GitHub username" "GITHUB_USER" "kachurovskiy"
    get_input "Enter your GitHub repository name" "GITHUB_REPO" "stratcraft"

    echo
    print_status "Starting deployment with the following configuration:"
    echo "  Domain: $DOMAIN"
    echo "  GitHub: $GITHUB_USER/$GITHUB_REPO"
    echo "  App User: $APP_USER"
    echo "  App Directory: $APP_DIR"
    echo

    read -p "Continue with deployment? (y/N): " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        print_warning "Deployment cancelled"
        exit 0
    fi


    # Execute deployment steps
    update_system
    install_nodejs
    install_pm2
    install_nginx
    install_certbot
    install_postgres
    install_lightgbm
    create_app_user
    install_rust_toolchain
    setup_pm2_user
    clone_or_update_repository
    create_pm2_config
    configure_nginx
    setup_ssl
    setup_client_cert_lockdown
    configure_firewall
    setup_fail2ban
    setup_log_rotation
    configure_postgres_credentials
    create_environment_file
    setup_manual_update
    start_services
    cleanup_temp_files

    # Display final information
    display_final_info
}

# Main execution
main() {
    # Create log file
    touch "$LOG_FILE"

    # Get command
    COMMAND="${1:-deploy}"

    case "$COMMAND" in
        "deploy")
            deploy
            ;;
        "update")
            check_root
            update_application
            ;;
        "restart")
            check_root
            restart_application
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs
            ;;
        "setup-key")
            check_root
            generate_ssh_key_pair
            ;;
        *)
            echo "Usage: $0 [command]"
            echo
            echo "Commands:"
            echo "  deploy     - Full initial deployment (default)"
            echo "  update     - Pull latest changes and restart"
            echo "  restart    - Restart the application"
            echo "  status     - Show application status"
            echo "  logs       - Show application logs"
            echo "  setup-key  - Setup GitHub deploy key only"
            exit 1
            ;;
    esac
}

# Run main function
main "$@"
