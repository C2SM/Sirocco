#!/usr/bin/env bash
set -ev

# Setup SSH on localhost (you might already have this)
ssh-keygen -q -t rsa -b 4096 -N "" -f "${HOME}/.ssh/id_rsa"
ssh-keygen -y -f "${HOME}/.ssh/id_rsa" >> "${HOME}/.ssh/authorized_keys"
ssh-keyscan -H localhost >> "${HOME}/.ssh/known_hosts"
chmod 755 "${HOME}"

# Setup SLURM SSH key
CONFIG="${GITHUB_WORKSPACE}/.github/config"
cp "${CONFIG}/slurm_rsa" "${HOME}/.ssh/slurm_rsa"
chmod 600 "${HOME}/.ssh/slurm_rsa"

# Replace placeholders in config files
sed -i "s|PLACEHOLDER_SSH_KEY|${HOME}/.ssh/slurm_rsa|" "${CONFIG}/slurm-ssh-config.yaml"

# Setup SLURM computer in AiiDA
verdi computer setup --non-interactive --config "${CONFIG}/slurm-ssh.yaml"
verdi computer configure core.ssh slurm-ssh --non-interactive --config "${CONFIG}/slurm-ssh-config.yaml" -n
verdi computer test slurm-ssh --print-traceback
