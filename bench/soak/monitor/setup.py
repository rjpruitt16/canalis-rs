# Copyright 2026 Rahmi Pruitt
# SPDX-License-Identifier: Apache-2.0

"""Creates the Canalis soak monitor on Claude Managed Agents, the same way
Aquifer's soak does (benchmark/soak/monitor in that repository): a
locked-down environment, a vault holding the two secrets, the agent, and a
scheduled deployment (created paused, so a manual dry run comes first):

    ANTHROPIC_API_KEY_FILE=~/.config/canalis-soak/anthropic-key \
        SOAK_REPORT_TOKEN_FILE=~/.config/canalis-soak/report-token \
        GITHUB_TOKEN_FILE=~/.config/canalis-soak/github-token \
        python3 bench/soak/monitor/setup.py

The report token is the driver's SOAK_REPORT_TOKEN. Re-running updates the
agent's instructions in place. Secrets are read from files and sent only to
the vault (the API key only to the API); nothing is printed except resource
IDs, which are written to monitor/ids.json.
"""
import json
import os
import pathlib
import urllib.error
import urllib.request

API = "https://api.anthropic.com/v1"
HERE = pathlib.Path(__file__).parent

TARGET = {
    "HOST": "canalis-soak-driver.fly.dev",
    "REPO": "rjpruitt16/canalis-rs",
    "cron": "30 */6 * * *",
}
TARGET_NAME = "canalis"
STATUS_HOST = TARGET["HOST"]
MODEL = os.environ.get("SOAK_MONITOR_MODEL", "claude-haiku-5-5")
BUDGET_CENTS = os.environ.get("SOAK_MONITOR_BUDGET_CENTS", "100")  # per run


API_KEY = (pathlib.Path(os.path.expanduser(os.environ["ANTHROPIC_API_KEY_FILE"])).read_text().strip()
           if "ANTHROPIC_API_KEY_FILE" in os.environ else os.environ["ANTHROPIC_API_KEY"])


def call(method, path, body=None):
    req = urllib.request.Request(
        f"{API}{path}{'&' if '?' in path else '?'}beta=true",
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "x-api-key": API_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "managed-agents-2026-04-01",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as err:
        raise SystemExit(f"{method} {path}: {err.code} {err.read().decode()[:800]}")


def secret(env):
    return pathlib.Path(os.path.expanduser(os.environ[env])).read_text().strip()




def instructions():
    text = (HERE / "system.md").read_text()
    for key in ("HOST", "REPO"):
        text = text.replace("{{" + key + "}}", TARGET[key])
    if "{{" in text:
        raise SystemExit("system.md has a placeholder setup.py doesn't fill")
    return text


ids_path = HERE / "ids.json"
ids = json.loads(ids_path.read_text()) if ids_path.exists() else {}

if "environment" not in ids:
    env = call("POST", "/environments", {
        "name": f"{TARGET_NAME}-soak-monitor",
        "config": {"type": "cloud", "networking": {
            "type": "limited",
            "allowed_hosts": [STATUS_HOST, "api.github.com"],
        }},
    })
    ids["environment"] = env["id"]

if "vault" not in ids:
    vault = call("POST", "/vaults", {"display_name": f"{TARGET_NAME}-soak-monitor"})
    ids["vault"] = vault["id"]
    for name, file_env, host in [
        ("SOAK_REPORT_TOKEN", "SOAK_REPORT_TOKEN_FILE", STATUS_HOST),
        ("GITHUB_TOKEN", "GITHUB_TOKEN_FILE", "api.github.com"),
    ]:
        call("POST", f"/vaults/{vault['id']}/credentials", {
            "display_name": name,
            "auth": {
                "type": "environment_variable",
                "secret_name": name,
                "secret_value": secret(file_env),
                "networking": {"type": "limited", "allowed_hosts": [host]},
                "injection_location": {"header": True},
            },
        })

agent_body = {
    "name": "Canalis soak monitor",
    "model": MODEL,
    "system": instructions(),
    "tools": [{"type": "agent_toolset_20260401"}],
}
if "agent" not in ids:
    ids["agent"] = call("POST", "/agents", agent_body)["id"]
else:
    call("POST", f"/agents/{ids['agent']}", agent_body)

if "deployment" not in ids:
    dep = call("POST", "/deployments", {
        "name": "Canalis soak monitor, every 6h",
        "agent": ids["agent"],
        "environment_id": ids["environment"],
        "vault_ids": [ids["vault"]],
        "initial_events": [{"type": "user.message", "content": [{"type": "text", "text":
            "Run your soak-test check now: read your earlier reports, review the status pages, "
            "post one report, and raise a soak-alert issue only if something warrants it."}]}],
        "schedule": {"type": "cron", "expression": TARGET["cron"], "timezone": "UTC"},
        "budget": {"type": "limit", "max_list_cost": {"amount": BUDGET_CENTS, "currency": "USD"}},
    })
    ids["deployment"] = dep["id"]
    call("POST", f"/deployments/{dep['id']}/pause")

ids_path.write_text(json.dumps(ids, indent=2) + "\n")
print(json.dumps(ids, indent=2))
