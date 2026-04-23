# Local setup

Prereqs: Node.js + npm, Rust toolchain (for `engine`), and a Postgres instance.

From-zero (localhost):

1. Copy `.env.example` to `.env` and set at least `DATABASE_URL` and `DATABASE_KEY`.
2. Ensure Postgres is running and the role in `DATABASE_URL` exists. StratCraft will attempt to create the database named in `DATABASE_URL` on startup (requires DB create privileges); otherwise create the database manually.
3. Start the server:

```bash
npm install
npm start
```

4. Open `http://localhost:3000/` and request an access code for your email.
   - The first user becomes an admin.
   - If email delivery isn't configured yet, StratCraft will show the OTP in the UI for bootstrap login.
5. As an admin, configure a candle data provider in `Admin -> Settings` (Tiingo/EODHD/Alpaca). StratCraft needs candle data to backtest/optimize and generate signals.
   To support the project, consider using [Tiingo](https://www.tiingo.com/) as your candle source. Tiingo is an excellent daily candle provider and our partner.
6. Create a dedicated Alpaca paper account for loading the universe and enter provided keys in `Admin -> Settings -> Alpaca`
7. For email delivery, get a [Resend](https://resend.com/) API key, save it in `Admin -> Settings -> Email`, and add the 3 TXT DNS records for your sending domain exactly as instructed by Resend.

Server initializes the Postgres schema from `src/server/database/pg.sql` and builds the Rust engine from source on each start.
