#!/usr/bin/env bash
# Install the Loom Claude worker as a systemd service (run with sudo).
#   sudo bash worker/install.sh
# Override defaults with env vars, e.g.:
#   sudo RUN_USER=ubuntu WORKER_ID=loom-worker-1 bash worker/install.sh
set -euo pipefail

RUN_USER="${RUN_USER:-ubuntu}"
WORKER_ID="${WORKER_ID:-loom-worker-1}"
AWS_REGION="${AWS_REGION:-us-east-1}"
IOT_ENDPOINT="${IOT_ENDPOINT:-a4su630os08rw-ats.iot.us-east-1.amazonaws.com}"
ANTHROPIC_MODEL="${ANTHROPIC_MODEL:-us.anthropic.claude-sonnet-5-5}"
INSTALL_DIR=/opt/loom-claude-worker
ENV_FILE=/etc/loom-claude-worker.env
SRC_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_HOME="$(getent passwd "$RUN_USER" | cut -d: -f6)"
[ -n "$RUN_HOME" ] || { echo "user $RUN_USER does not exist" >&2; exit 1; }
WORKDIR="${WORKDIR:-$RUN_HOME/work}"

if ! python3 -c 'import venv, ensurepip' 2>/dev/null; then
  apt-get update -y && apt-get install -y python3-venv
fi

install -d -m 0755 "$INSTALL_DIR"
install -m 0644 "$SRC_DIR/claude_worker.py" "$SRC_DIR/requirements.txt" "$INSTALL_DIR/"
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip >/dev/null
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt"

install -d -o "$RUN_USER" -g "$RUN_USER" -m 0750 "$WORKDIR"

# Non-secret settings only. Credentials come from the instance role.
cat > "$ENV_FILE" <<EOF
WORKER_ID=$WORKER_ID
AWS_REGION=$AWS_REGION
IOT_ENDPOINT=$IOT_ENDPOINT
WORKDIR=$WORKDIR
FILES_BUCKET=${FILES_BUCKET:-loom-claude-files-$("$INSTALL_DIR/venv/bin/python" -c "import boto3; print(boto3.client('sts', region_name='$AWS_REGION').get_caller_identity()['Account'])")-$AWS_REGION}
UPLOADS_DIR=$RUN_HOME/loom-uploads
CLAUDE_CODE_USE_BEDROCK=1
ANTHROPIC_MODEL=$ANTHROPIC_MODEL
PATH=$RUN_HOME/.local/bin:$RUN_HOME/.npm-global/bin:/usr/local/bin:/usr/bin:/bin
EOF
chmod 0640 "$ENV_FILE"
chown root:"$RUN_USER" "$ENV_FILE"

cat > /etc/systemd/system/loom-claude-worker.service <<EOF
[Unit]
Description=Loom Claude worker (outbound-only)
After=network-online.target
Wants=network-online.target

[Service]
User=$RUN_USER
Group=$RUN_USER
EnvironmentFile=$ENV_FILE
# Optional, owned by $RUN_USER: variables your tools/skills expect from your
# shell (what \`claude\` in a terminal inherits from ~/.bashrc). KEY=value lines.
EnvironmentFile=-$RUN_HOME/.config/loom-claude-worker/env
WorkingDirectory=$WORKDIR
ExecStart=$INSTALL_DIR/venv/bin/python $INSTALL_DIR/claude_worker.py
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable loom-claude-worker
# restart (not just start) so re-running this script upgrades a running worker
systemctl restart loom-claude-worker
sleep 3
systemctl --no-pager status loom-claude-worker | head -15
echo "Logs: journalctl -u loom-claude-worker -f"
