#!/usr/bin/env bash
# Watches the BIN BOT systemd services and alerts via Telegram the moment
# one stops being active. Deliberately a standalone script with no Python
# or project dependency: the whole point is to still work if the bot's
# own process, venv, or event loop is what's broken (the 2026-08-10
# crash-loop incident showed a fully-dead process can't notify about its
# own death from inside itself).
#
# Reads TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID from the environment — the
# systemd unit below points EnvironmentFile at the same .env the bot
# itself uses, so there's only one place to configure these.
#
# Deployed as a systemd timer (binbot-watchdog.timer), not a cron job or
# a sleep loop, so it's supervised and logged the same way as everything
# else on this server.
set -uo pipefail

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TELEGRAM_CHAT_ID:-}"

# Telegram not configured yet — nothing to do, and nothing to fail on.
if [ -z "$BOT_TOKEN" ] || [ -z "$CHAT_ID" ]; then
    exit 0
fi

# Relative to the systemd unit's WorkingDirectory (the project root) — not
# $HOME, which systemd does not set for a service unless it runs under a
# full login environment (User=/PAMName=), so referencing it under `set -u`
# fails with "HOME: unbound variable" the moment this runs as a service.
STATE_FILE="${BINBOT_WATCHDOG_STATE_FILE:-data_store/watchdog_state.txt}"
SERVICES=(binbot-scanner binbot-dashboard)

send_alert() {
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        --data-urlencode "chat_id=${CHAT_ID}" \
        --data-urlencode "text=$1" >/dev/null
}

mkdir -p "$(dirname "$STATE_FILE")"
prev_state=""
[ -f "$STATE_FILE" ] && prev_state="$(cat "$STATE_FILE")"

down=()
for svc in "${SERVICES[@]}"; do
    if ! systemctl is-active --quiet "$svc"; then
        down+=("$svc")
    fi
done

if [ "${#down[@]}" -gt 0 ]; then
    curr_state="down:${down[*]}"
    # Only alert on a state CHANGE, not every 3-minute check — otherwise a
    # prolonged outage spams the chat with an identical message forever.
    if [ "$curr_state" != "$prev_state" ]; then
        send_alert "🚨 BIN BOT: service(s) DOWN on $(hostname) — ${down[*]}"
    fi
    echo "$curr_state" > "$STATE_FILE"
else
    if [ "$prev_state" != "up" ]; then
        send_alert "✅ BIN BOT: all services back up on $(hostname)"
    fi
    echo "up" > "$STATE_FILE"
fi
