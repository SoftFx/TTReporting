#!/usr/bin/env python3
"""Create (or check) the Authentik side of TT Reporting SSO (AUTHENTIK_URL).

Idempotent: every object is looked up first and only created when missing.
Groups and memberships are touched ONLY when named explicitly on the command
line -- each group name and its members must be approved by the owner first.

  python prod/authentik/setup-authentik.py --dry-run   # show what exists / would change
  python prod/authentik/setup-authentik.py             # provider + application + outpost only
  python prod/authentik/setup-authentik.py --group NAME            # create NAME, bind it to the app
  python prod/authentik/setup-authentik.py --group NAME --member NAME=user1 --member NAME=user2

  # second application on the same outpost (Cronicle, admins only):
  python prod/authentik/setup-authentik.py --name "TT Reporting Cronicle" --slug tt-reporting-cronicle       --external-host https://<cronicle-host> --group tt-reporting-admins

What it sets up:
  * Proxy provider "TT Reporting" in Forward auth (single application) mode,
    external host = TT_REPORTING_URL (or --external-host).
  * Application "TT Reporting" (slug tt-reporting) with that provider,
    policy engine mode "any". Each --group is bound to it: only members of
    bound groups may log in at all. With no group bound, Authentik lets every
    user in, so Caddy denies by group header as well.
  * Outpost "tt-reporting outpost" (proxy) serving that provider. It runs as the
    authentik-outpost container on ReportingServer (prod/docker-compose.yml); its
    token lives in /opt/automation/jobs/authentik-outpost/outpost.env. The embedded
    outpost on the Authentik server is NOT used: in this installation it answers 404
    for forward-auth requests of other hosts (same as the logs server, which has its own).
  * --member GROUP=USER adds an existing Authentik user (users are never created).
    GROUP must also be given with --group in the same run.

Reads AUTHENTIK_TOKEN (an Authentik API token with admin rights) and optional
AUTHENTIK_URL / TT_REPORTING_URL from prod/.env or the environment.
The token is never printed.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

APP_NAME = "TT Reporting"
APP_SLUG = "tt-reporting"
EMBEDDED_OUTPOST = "goauthentik.io/outposts/embedded"
OUTPOST_NAME = "tt-reporting outpost"


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


class Api:
    def __init__(self, base: str, token: str, dry_run: bool):
        self.base = base.rstrip("/") + "/api/v3"
        self.token = token
        self.dry_run = dry_run

    def call(self, method: str, path: str, body=None, params=None):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self.token}")
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")[:500]
            sys.exit(f"ERROR {method} {path}: HTTP {e.code} {detail}")

    def get(self, path, **params):
        return self.call("GET", path, params=params or None)

    def write(self, method, path, body, what):
        if self.dry_run:
            print(f"  [dry-run] would {what}")
            return None
        result = self.call(method, path, body)
        print(f"  {what}: done")
        return result


def first(results, **match):
    for item in results:
        if all(item.get(k) == v for k, v in match.items()):
            return item
    return None


def pick_flow(api: Api, designation: str, preferred_slug: str):
    flows = api.get("/flows/instances/", designation=designation)["results"]
    flow = first(flows, slug=preferred_slug) or (flows[0] if flows else None)
    print(f"flow {designation}: {flow['slug'] if flow else 'none found'}")
    return flow["pk"] if flow else None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="only show what exists and what would change")
    parser.add_argument("--group", action="append", default=[], metavar="NAME",
                        help="approved group to create (if missing) and bind to the application; repeatable")
    parser.add_argument("--member", action="append", default=[], metavar="GROUP=USER",
                        help="approved membership: add existing user USER to GROUP; repeatable")
    parser.add_argument("--name", default=APP_NAME, help=f"application and provider name (default {APP_NAME!r})")
    parser.add_argument("--slug", default=APP_SLUG, help=f"application slug (default {APP_SLUG!r})")
    parser.add_argument("--external-host", help="provider external host (default: TT_REPORTING_URL)")
    parser.add_argument("--env", type=Path, default=Path(__file__).resolve().parent.parent / ".env")
    args = parser.parse_args()

    members = {}
    for item in args.member:
        group_name, sep, username = item.partition("=")
        if not sep or not group_name or not username:
            sys.exit(f"--member must be GROUP=USER, got: {item}")
        if group_name not in args.group:
            sys.exit(f"--member {item}: group {group_name} must also be passed with --group")
        members.setdefault(group_name, []).append(username)

    load_env(args.env)
    token = os.environ.get("AUTHENTIK_TOKEN", "")
    if not token:
        sys.exit(f"AUTHENTIK_TOKEN is not set (looked in {args.env} and the environment)")
    base = os.environ.get("AUTHENTIK_URL", "")
    external_host = (args.external_host or os.environ.get("TT_REPORTING_URL", "")).rstrip("/")
    if not base.startswith("https://") or not external_host.startswith("https://"):
        sys.exit("AUTHENTIK_URL and TT_REPORTING_URL (or --external-host) must be set to https:// URLs in prod/.env")
    app_name, app_slug = args.name, args.slug
    api = Api(base, token, args.dry_run)

    me = api.get("/core/users/me/")["user"]
    print(f"Authentik {base} as {me['username']} (superuser={me.get('is_superuser')})")

    # ── Provider ──
    auth_flow = pick_flow(api, "authorization", "default-provider-authorization-implicit-consent")
    inval_flow = pick_flow(api, "invalidation", "default-provider-invalidation-flow")
    provider = first(api.get("/providers/proxy/", name=app_name)["results"], name=app_name)
    if provider:
        print(f"provider '{app_name}': exists (pk {provider['pk']}, mode {provider['mode']}, external_host {provider['external_host']})")
        if provider["mode"] != "forward_single" or provider["external_host"].rstrip("/") != external_host:
            api.write("PATCH", f"/providers/proxy/{provider['pk']}/",
                      {"mode": "forward_single", "external_host": external_host},
                      f"fix provider mode/external_host -> forward_single {external_host}")
    else:
        body = {
            "name": app_name,
            "authorization_flow": auth_flow,
            "mode": "forward_single",
            "external_host": external_host,
            "access_token_validity": "hours=12",
        }
        if inval_flow:
            body["invalidation_flow"] = inval_flow
        provider = api.write("POST", "/providers/proxy/", body, f"create proxy provider '{app_name}' ({external_host})")
    provider_pk = provider["pk"] if provider else None

    # ── Application ──
    # superuser_full_list: without it the list holds only apps the token's own user may open.
    app = first(api.get("/core/applications/", superuser_full_list="true", page_size=500)["results"], slug=app_slug)
    if app:
        print(f"application '{app_slug}': exists (provider {app.get('provider')})")
        if provider_pk and app.get("provider") != provider_pk:
            api.write("PATCH", f"/core/applications/{app_slug}/", {"provider": provider_pk},
                      f"attach provider {provider_pk} to application")
    else:
        app = api.write("POST", "/core/applications/", {
            "name": app_name,
            "slug": app_slug,
            "provider": provider_pk,
            "meta_launch_url": external_host + "/",
            "policy_engine_mode": "any",
        }, f"create application '{app_name}'")

    # ── Groups and bindings ──
    group_pks = {}
    for name in args.group:
        group = first(api.get("/core/groups/", name=name)["results"], name=name)
        if group:
            print(f"group {name}: exists")
        else:
            group = api.write("POST", "/core/groups/", {"name": name, "is_superuser": False}, f"create group {name}")
        group_pks[name] = group["pk"] if group else None

    if app:
        bindings = api.get("/policies/bindings/", target=app["pk"])["results"]
        bound = {b.get("group") for b in bindings}
        print(f"application bindings: {len(bindings)}")
        for order, name in enumerate(args.group, start=len(bindings)):
            pk = group_pks[name]
            if pk and pk in bound:
                print(f"binding {name}: exists")
                continue
            api.write("POST", "/policies/bindings/", {
                "target": app["pk"], "group": pk, "order": order,
                "enabled": True, "negate": False, "timeout": 30,
            }, f"bind group {name} to application")
    elif args.group:
        print("  [dry-run] would bind " + ", ".join(args.group) + " to the new application")

    # ── Dedicated outpost (the embedded one must not serve this provider) ──
    outposts = api.get("/outposts/instances/", page_size=100)["results"]
    outpost = first(outposts, name=OUTPOST_NAME)
    if outpost:
        print(f"outpost '{OUTPOST_NAME}': exists, providers {outpost['providers']}")
        if provider_pk and provider_pk not in outpost["providers"]:
            api.write("PATCH", f"/outposts/instances/{outpost['pk']}/",
                      {"providers": outpost["providers"] + [provider_pk]},
                      f"attach provider to outpost '{OUTPOST_NAME}'")
    else:
        api.write("POST", "/outposts/instances/", {
            "name": OUTPOST_NAME, "type": "proxy",
            "providers": [provider_pk] if provider_pk else [],
            "config": {"authentik_host": base, "authentik_host_browser": base,
                       "authentik_host_insecure": False, "log_level": "info"},
        }, f"create outpost '{OUTPOST_NAME}' (then put its token into the server outpost.env)")
    embedded = first(outposts, managed=EMBEDDED_OUTPOST)
    if embedded and provider_pk and provider_pk in embedded["providers"]:
        api.write("PATCH", f"/outposts/instances/{embedded['pk']}/",
                  {"providers": [p for p in embedded["providers"] if p != provider_pk]},
                  "detach provider from the embedded outpost")

    # ── Members ──
    if members:
        missing = []
        for group_name, usernames in members.items():
            group_pk = group_pks[group_name]
            current = set()
            if group_pk:
                current = {u["username"] for u in api.get(f"/core/groups/{group_pk}/")["users_obj"]}
            for username in usernames:
                if username in current:
                    print(f"member {username} in {group_name}: exists")
                    continue
                found = first(api.get("/core/users/", username=username)["results"], username=username)
                if not found:
                    missing.append(username)
                    continue
                if group_pk:
                    api.write("POST", f"/core/groups/{group_pk}/add_user/", {"pk": found["pk"]},
                              f"add {username} to {group_name}")
                else:
                    print(f"  [dry-run] would add {username} to {group_name}")
        if missing:
            print("NOT FOUND in Authentik (add them by hand once they have accounts): "
                  + ", ".join(sorted(set(missing))))


if __name__ == "__main__":
    main()
