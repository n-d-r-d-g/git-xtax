import io
import os
try:
  import readline  # noqa: F401 — enables line editing in input()
except ImportError:
  pass  # readline is not available on Windows
import shlex
import subprocess
import sys
if sys.platform == 'win32':
  import msvcrt
else:
  import tty
  import termios
from typing import Dict, List, NoReturn, Optional, Tuple

from git_xtax import __version__, utils
from git_xtax.annotation import Annotation
from git_xtax.code_hosting import CodeHostingClient, CodeHostingSpec, OrganizationAndRepository, PullRequest
from git_xtax.exceptions import ExitCode, InteractionStopped, XtaxException, UnderlyingGitException, UnexpectedXtaxException
from git_xtax.gitlab import GITLAB_CLIENT_SPEC
from git_xtax.github import GITHUB_CLIENT_SPEC
from git_xtax.git_operations import (
    AnyRevision, GitContext, LocalBranchShortName,
    FullCommitHash, BranchPair,
)
from git_xtax.stack_state import StackStorage, XtaxState
from git_xtax.utils import (
    AnsiEscapeCodes, bold, colored, debug, dim, fmt, hyperlink, rl_safe, strikethrough, underline, warn,
)

_KNOWN_SPECS: List[CodeHostingSpec] = [GITLAB_CLIENT_SPEC, GITHUB_CLIENT_SPEC]


USAGE = f"""\
usage: git xtax <command> [<args>]

{bold('Stack management:')}
  init                                  Create a new stack (interactive)
  stack <branch> [--onto=<parent>]      Add branch above current branch (or --onto parent)
  tuck <branch>                         Add branch below current branch
  slideout <branch>                     Slide branch out of stack
  delete <name>                         Delete a stack
  rename <old> <new>                    Rename a stack
  rename-branch <old> <new>             Rename a branch in its stack
  edit                                  Open stack definition in editor
  switch <name>                         Switch to stack and checkout first branch

{bold('Navigation:')}
  v, view                               Show the current stack (interactive)
  l, list                               Show all stacks (interactive)
  u, up                                 Go to child branch (up the stack)
  d, down                               Go to parent branch (down the stack)
  t, top                                Go to top (leaf) branch in stack
  b, bottom                             Go to bottom (first) branch in stack
  <N>                                   Go N up (positive) or down (negative); 0 = leaf

{bold('Sync & share:')}
  s, sync [--current|--cascade] [--continue] [--include-merged]
                                        Rebase + push entire stack
                                        --current: current branch only
                                        --cascade: current branch and above
                                        --include-merged: also sync branches with merged MRs
  push                                  Push stacks to remote
  pull                                  Pull stacks from remote

{bold('General:')}
  completions <shell>                   Print shell completion script (zsh, bash)
  h, help                               Show this help
  version                               Show version
"""

ZSH_COMPLETION = r'''
_git-xtax() {
  local -a commands
  commands=(
    'init:Create a new stack'
    'stack:Add branch above current branch'
    'tuck:Add branch below current branch'
    'slideout:Slide branch out of stack'
    'delete:Delete a stack'
    'rename:Rename a stack'
    'rename-branch:Rename a branch in its stack'
    'edit:Open stack definition in editor'
    'switch:Switch to stack and checkout first branch'
    'v:Show stack tree'
    'list:Show all stacks'
    'l:Show all stacks'
    'view:Show stack tree'
    'up:Go to child branch (up the stack)'
    'u:Go to child branch (up the stack)'
    'down:Go to parent branch (down the stack)'
    'd:Go to parent branch (down the stack)'
    'top:Go to top (leaf) branch in stack'
    't:Go to top (leaf) branch in stack'
    'f:Go to first branch in stack'
    'bottom:Go to bottom (first) branch in stack'
    'b:Go to bottom (first) branch in stack'
    'l:Go to last branch in stack'
    's:Rebase and push entire stack'
    'sync:Rebase and push entire stack'
    'push:Push stacks to remote'
    'pull:Pull stacks from remote'
    'completions:Print shell completion script'
    'help:Show help'
    'version:Show version'
  )

  _arguments -C \
    '1:command:->command' \
    '*:arg:->args'

  case $state in
    command)
      _describe 'command' commands
      ;;
    args)
      local subcmd=${words[2]}
      (( CURRENT-- ))
      shift words
      case $subcmd in
        init)
          _arguments -s \
            ':stack name:' \
            ':branch:__git_xtax_branch_names' \
            '--root=[Base branch]:branch:__git_xtax_branch_names'
          ;;
        stack)
          _arguments -s \
            ':branch:__git_xtax_branch_names' \
            '--onto=[Parent branch]:branch:__git_xtax_branch_names'
          ;;
        tuck)
          _arguments ':branch:__git_xtax_branch_names'
          ;;
        slideout)
          _arguments ':branch:__git_xtax_branch_names'
          ;;
        delete|switch)
          _arguments ':stack:__git_xtax_stack_names'
          ;;
        sync)
          _arguments '--current[Sync current branch only]' '--cascade[Sync current branch and above]' '--continue[Continue after conflict]'
          ;;
        view|v)
          _arguments '--all[Show all stacks]'
          ;;
        completions)
          _arguments ':shell:(zsh bash)'
          ;;
      esac
      ;;
  esac
}

__git_xtax_branch_names() {
  local -a branches
  branches=(${${(f)"$(git branch --format='%(refname:short)' 2>/dev/null)"}})
  _describe 'branch' branches
}

__git_xtax_stack_names() {
  local -a stacks
  stacks=(${${(f)"$(git-xtax view --all 2>/dev/null | grep '│' | sed 's/^.*[○◉] //')"}})
  if [[ ${#stacks} -gt 0 ]]; then
    _describe 'stack' stacks
  fi
}

compdef _git-xtax git-xtax
compdef _git-xtax gx
'''


class XtaxClient:

  def __init__(self, git: GitContext, storage: StackStorage) -> None:
    self._git = git
    self._storage = storage
    self._hosting_info: Optional[Tuple[CodeHostingClient, CodeHostingSpec]] = None
    self._hosting_info_resolved: bool = False
    self._pr_cache: Dict[str, Optional[Any]] = {}  # identifier -> PullRequest or None
    self._pr_comment_count_cache: Dict[str, int] = {}
    self._pr_approved_cache: Dict[str, Optional[bool]] = {}

  def _fetch_stacks(self) -> None:
    """Fetch and fast-forward _xtax from origin."""
    try:
      result = self._storage.fetch_and_fast_forward()
      if result == 'created':
        print(dim("Fetched stack data from origin (new)"))
      elif result == 'updated':
        print(dim("Fetched stack data from origin (updated)"))
      elif result == 'ahead':
        print(dim("Stack data has local changes not yet pushed"))
      elif result == 'diverged':
        self._resolve_xtax_divergence()
    except XtaxException:
      raise
    except Exception as e:
      debug(f"Failed to fetch stacks: {e}")
    self._link_unlinked_mrs()

  def _link_unlinked_mrs(self) -> None:
    """Find branches with open MRs on the forge but no annotation in the stack YAML."""
    try:
      hosting = self._get_code_hosting_client()
      if not hosting:
        debug("No code hosting client available for MR linking")
        return
      client, spec = hosting
    except Exception as e:
      debug(f"Failed to get code hosting client for MR linking: {e}")
      return

    stacks = self._storage.list_stacks()
    for stack_name in stacks:
      content = self._storage.read_stack_definition(stack_name)
      if not content:
        continue
      state = StackStorage.parse_definition(content)
      changed = False

      for branch in state.managed_branches:
        annotation = state.annotations.get(branch)
        identifier = self._extract_pr_identifier(annotation, spec)
        if identifier is not None:
          continue  # Already has an MR linked

        # Check forge for an open MR with this branch as source
        try:
          open_prs = client.get_open_pull_requests_by_head(branch)
          if open_prs:
            pr = open_prs[0]
            pr_text = f"{spec.pr_short_name} {spec.pr_ordinal_char}{pr.identifier}"
            if annotation and annotation.qualifiers.is_non_default():
              pr_text = f"{pr_text} {annotation.qualifiers}"
            state.annotations[branch] = Annotation.parse(pr_text)
            changed = True
            print(dim(f"Linked {pr_text} to {bold(str(branch))}"))
        except Exception as e:
          debug(f"Failed to check MR for {branch}: {e}")

      if changed:
        self._save_state(stack_name, state)

  def _resolve_xtax_divergence(self) -> None:
    """Resolve divergence between local and remote _xtax using three-way merge."""
    branch = self._storage.BRANCH
    remote_ref = f'origin/{branch}'

    from git_xtax.stack_state import popen_cmd
    _, ancestor_hash, _ = popen_cmd('git', 'merge-base', branch, remote_ref)
    ancestor_hash = ancestor_hash.strip()

    _, local_diff, _ = popen_cmd('git', 'diff', '--name-only', ancestor_hash, branch)
    local_changed = set(f for f in local_diff.strip().splitlines() if f and f.startswith('stacks/'))
    _, remote_diff, _ = popen_cmd('git', 'diff', '--name-only', ancestor_hash, remote_ref)
    remote_changed = set(f for f in remote_diff.strip().splitlines() if f and f.startswith('stacks/'))

    conflicts = local_changed & remote_changed
    local_only = local_changed - remote_changed

    if not conflicts:
      # No conflicting files — merge automatically
      self._merge_xtax_trees(local_only, set(), branch, remote_ref, ancestor_hash)
      return

    # Three-way merge conflicting files using git merge-file
    import tempfile as _tempfile
    conflict_dir = os.path.join(self._git._root_dir, '.git', 'xtax-conflicts')
    os.makedirs(conflict_dir, exist_ok=True)

    has_unresolved = False
    resolved_files: Dict[str, str] = {}  # path -> merged content

    for path in sorted(conflicts):
      # Extract base, local, remote versions to temp files
      base_file = _tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
      local_file = _tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
      remote_file = _tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False)
      try:
        ec, base_content, _ = popen_cmd('git', 'show', f'{ancestor_hash}:{path}')
        base_file.write(base_content if ec == 0 else '')
        base_file.close()

        _, local_content, _ = popen_cmd('git', 'show', f'{branch}:{path}')
        local_file.write(local_content)
        local_file.close()

        _, remote_content, _ = popen_cmd('git', 'show', f'{remote_ref}:{path}')
        remote_file.write(remote_content)
        remote_file.close()

        # git merge-file modifies local_file in place, returns 0 if clean merge
        merge_ec, _, _ = popen_cmd(
          'git', 'merge-file',
          '-L', 'local', '-L', 'base', '-L', 'remote',
          local_file.name, base_file.name, remote_file.name)

        with open(local_file.name) as f:
          merged_content = f.read()

        if merge_ec == 0:
          # Clean merge — no conflicts
          resolved_files[path] = merged_content
        else:
          # Has conflict markers — write to conflict dir for user to resolve
          has_unresolved = True
          conflict_path = os.path.join(conflict_dir, os.path.basename(path))
          with open(conflict_path, 'w') as f:
            f.write(merged_content)
      finally:
        os.unlink(base_file.name)
        os.unlink(local_file.name)
        os.unlink(remote_file.name)

    if has_unresolved:
      print(colored("Stack metadata has conflicts that need to be resolved.", AnsiEscapeCodes.YELLOW))
      print(f"Conflicted files are in: {bold(conflict_dir)}")
      print(f"Resolve the conflicts, then run: {bold('git xtax push --continue')}")
      # Save merge state
      self._storage.save_merge_state({
        'ancestor': ancestor_hash,
        'local_only': list(local_only),
        'resolved': {k: v for k, v in resolved_files.items()},
        'conflict_files': [os.path.basename(p) for p in conflicts if
                           os.path.exists(os.path.join(conflict_dir, os.path.basename(p)))],
      })
      raise XtaxException("Resolve stack metadata conflicts before continuing")

    # All conflicts resolved cleanly — merge everything
    all_local = local_only | set(resolved_files.keys())
    self._merge_xtax_trees(all_local, resolved_files, branch, remote_ref, ancestor_hash)

  def _merge_xtax_trees(self, local_files: set, resolved_content: Dict[str, str],
                         branch: str, remote_ref: str, ancestor_hash: str) -> None:
    """Merge _xtax by starting from remote tree and overlaying local/resolved files."""
    import tempfile
    from git_xtax.stack_state import popen_cmd

    _, remote_hash, _ = popen_cmd('git', 'rev-parse', remote_ref)
    remote_hash = remote_hash.strip()

    if not local_files and not resolved_content:
      popen_cmd('git', 'update-ref', f'refs/heads/{branch}', remote_hash)
      return

    tmp_index = tempfile.mktemp(prefix='xtax-merge-index-')
    try:
      env = {**os.environ, 'GIT_INDEX_FILE': tmp_index}
      popen_cmd('git', 'read-tree', remote_ref, env=env)

      for path in local_files:
        if path in resolved_content:
          # Use the cleanly merged content
          ec, blob_hash_out, _ = popen_cmd(
            'git', 'hash-object', '-w', '--stdin', input=resolved_content[path])
          blob_hash = blob_hash_out.strip()
          popen_cmd('git', 'update-index', '--add', '--cacheinfo', '100644', blob_hash, path, env=env)
        else:
          # Use local version as-is
          ec, blob_out, _ = popen_cmd('git', 'rev-parse', f'{branch}:{path}')
          if ec == 0:
            popen_cmd('git', 'update-index', '--add', '--cacheinfo', '100644', blob_out.strip(), path, env=env)
          else:
            popen_cmd('git', 'update-index', '--force-remove', path, env=env)

      _, tree_out, _ = popen_cmd('git', 'write-tree', env=env)
      merged_tree = tree_out.strip()

      _, commit_out, _ = popen_cmd(
        'git', 'commit-tree', merged_tree, '-p', remote_hash, '-m', 'xtax: merge stack metadata')
      popen_cmd('git', 'update-ref', f'refs/heads/{branch}', commit_out.strip())
    finally:
      if os.path.exists(tmp_index):
        os.unlink(tmp_index)

  def _finish_xtax_merge(self, merge_state: dict) -> None:
    """Finish resolving _xtax merge conflicts after user has edited the conflict files."""
    from git_xtax.stack_state import popen_cmd

    branch = self._storage.BRANCH
    remote_ref = f'origin/{branch}'
    conflict_dir = os.path.join(self._git._root_dir, '.git', 'xtax-conflicts')

    local_only = set(merge_state.get('local_only', []))
    resolved = merge_state.get('resolved', {})
    conflict_files = merge_state.get('conflict_files', [])
    ancestor_hash = merge_state['ancestor']

    # Read user-resolved conflict files
    for filename in conflict_files:
      conflict_path = os.path.join(conflict_dir, filename)
      if not os.path.exists(conflict_path):
        raise XtaxException(f"Missing resolved file: {conflict_path}")
      with open(conflict_path) as f:
        content = f.read()
      if '<<<<<<<' in content or '>>>>>>>' in content:
        raise XtaxException(f"Conflict markers still present in {conflict_path}")
      path = f'stacks/{filename}'
      resolved[path] = content

    all_local = local_only | set(resolved.keys())
    self._merge_xtax_trees(all_local, resolved, branch, remote_ref, ancestor_hash)

  @staticmethod
  def _resolve_ssh_alias(url: str) -> Optional[str]:
    """Resolve SSH aliases in git remote URLs (e.g. git@alias:org/repo → git@github.com:org/repo)."""
    import re
    import shutil
    match = re.match(r'^git@([^:]+):(.+)$', url)
    if not match:
      return None
    ssh_host = match.group(1)
    path = match.group(2)
    ssh = shutil.which('ssh')
    if not ssh:
      return None
    exit_code, stdout, _ = utils.popen_cmd(ssh, '-G', ssh_host, hide_debug_output=True)
    if exit_code != 0:
      return None
    for line in stdout.splitlines():
      if line.startswith('hostname '):
        real_host = line.split(' ', 1)[1].strip()
        if real_host != ssh_host:
          return f'git@{real_host}:{path}'
        return None
    return None

  def _get_code_hosting_client(self) -> Optional[Tuple[CodeHostingClient, CodeHostingSpec]]:
    """Try to create a code hosting client from the origin remote URL."""
    raw_url = self._git.get_url_of_remote('origin')
    if not raw_url:
      return None
    # Build list of URLs to try: raw, git insteadOf resolved, and SSH alias resolved
    urls = [raw_url]
    resolved_result = self._git._popen_git('remote', 'get-url', 'origin', allow_non_zero=True)
    resolved_url = resolved_result.stdout.strip() if resolved_result.exit_code == 0 else None
    if resolved_url and resolved_url != raw_url:
      urls.append(resolved_url)
    ssh_resolved = self._resolve_ssh_alias(urls[-1])
    if ssh_resolved:
      urls.append(ssh_resolved)
    for url in urls:
      for spec in _KNOWN_SPECS:
        org_repo = OrganizationAndRepository.from_url(spec.default_domain, url)
        if org_repo:
          client = spec.create_client(
            domain=spec.default_domain,
            organization=org_repo.organization,
            repository=org_repo.repository,
          )
          if not client.has_token():
            print(colored(
              f"No {spec.display_name} API token found. "
              f"{spec.pr_short_name}s will not be created or updated.\n"
              f"  Provide a token via one of the: {spec.token_providers_message}",
              AnsiEscapeCodes.YELLOW
            ), file=sys.stderr)
            return None
          return (client, spec)
    return None

  def _extract_pr_identifier(self, annotation: Optional[Annotation], spec: CodeHostingSpec) -> Optional[str]:
    """Extract PR/MR identifier from annotation text like 'MR !123' or 'PR #456'."""
    if not annotation or not annotation.text_without_qualifiers:
      return None
    text = annotation.text_without_qualifiers
    import re
    pattern = re.escape(spec.pr_short_name) + r'\s*' + re.escape(spec.pr_ordinal_char) + r'(\S+)'
    match = re.match(pattern, text)
    return match.group(1) if match else None

  @staticmethod
  def _elapsed_str(finished_at: str) -> str:
    """Return human-readable elapsed time since finished_at ISO timestamp."""
    import datetime
    finished = datetime.datetime.fromisoformat(finished_at.replace('Z', '+00:00'))
    diff = int((datetime.datetime.now(datetime.timezone.utc) - finished).total_seconds())
    if diff < 3600:
      return f"{diff // 60}m"
    elif diff < 86400:
      return f"{diff // 3600}h"
    elif diff < 86400 * 30:
      return f"{diff // 86400}d"
    else:
      return f"{diff // (86400 * 30)}mo"

  def _get_pr_url(self, annotation: Optional[Annotation]) -> Optional[str]:
    """Build a web URL for the PR/MR in the annotation, if any."""
    if not self._hosting_info_resolved:
      self._hosting_info_resolved = True
      try:
        self._hosting_info = self._get_code_hosting_client()
      except Exception:
        pass
    if not self._hosting_info or not annotation:
      return None
    client, spec = self._hosting_info
    identifier = self._extract_pr_identifier(annotation, spec)
    if identifier is None:
      return None
    return client.get_pr_url(identifier)

  def _get_cached_pr(self, annotation: Optional[Annotation]) -> Optional[Any]:
    """Return cached PullRequest for the annotation, or None."""
    if not self._hosting_info or not annotation:
      return None
    _, spec = self._hosting_info
    identifier = self._extract_pr_identifier(annotation, spec)
    if identifier is None:
      return None
    return self._pr_cache.get(identifier)

  def _get_pr_status(self, annotation: Optional[Annotation]) -> Optional[str]:
    """Return normalized PR/MR status: 'open', 'merged', 'closed', or None."""
    if not self._hosting_info_resolved:
      self._hosting_info_resolved = True
      try:
        self._hosting_info = self._get_code_hosting_client()
      except Exception:
        pass
    pr = self._get_cached_pr(annotation)
    return pr.state if pr else None

  def _get_pr_pipeline(self, annotation: Optional[Annotation]) -> Tuple[Optional[str], Optional[str]]:
    """Return (pipeline_status, pipeline_finished_at) for the PR/MR in the annotation."""
    pr = self._get_cached_pr(annotation)
    if not pr:
      return None, None
    return pr.pipeline_status, pr.pipeline_finished_at

  def _get_pr_unresolved_count(self, annotation: Optional[Annotation]) -> Optional[int]:
    """Return cached unresolved comment count for the PR/MR in the annotation."""
    if not self._hosting_info or not annotation:
      return None
    _, spec = self._hosting_info
    identifier = self._extract_pr_identifier(annotation, spec)
    if identifier is None:
      return None
    return self._pr_comment_count_cache.get(identifier)

  def _get_pr_approved(self, annotation: Optional[Annotation]) -> Optional[bool]:
    """Return cached approval status for the PR/MR in the annotation."""
    if not self._hosting_info or not annotation:
      return None
    _, spec = self._hosting_info
    identifier = self._extract_pr_identifier(annotation, spec)
    if identifier is None:
      return None
    return self._pr_approved_cache.get(identifier)

  def _prefetch_pr_data(self, state: XtaxState) -> None:
    """Prefetch PR status and unresolved comment count for all annotated branches in parallel."""
    if not self._hosting_info_resolved:
      self._hosting_info_resolved = True
      try:
        self._hosting_info = self._get_code_hosting_client()
      except Exception:
        pass
    if not self._hosting_info:
      return
    client, spec = self._hosting_info

    to_fetch = []
    for annotation in state.annotations.values():
      identifier = self._extract_pr_identifier(annotation, spec)
      if identifier and (
        identifier not in self._pr_cache or
        identifier not in self._pr_comment_count_cache or
        identifier not in self._pr_approved_cache
      ):
        to_fetch.append(identifier)

    if not to_fetch:
      return

    def fetch_one(identifier: str) -> None:
      if identifier not in self._pr_cache:
        try:
          self._pr_cache[identifier] = client.get_pull_request_by_identifier_or_none(identifier)
        except Exception:
          self._pr_cache[identifier] = None
      if identifier not in self._pr_comment_count_cache:
        try:
          self._pr_comment_count_cache[identifier] = client.get_unresolved_comment_count(identifier)
        except Exception:
          self._pr_comment_count_cache[identifier] = 0
      if identifier not in self._pr_approved_cache:
        try:
          self._pr_approved_cache[identifier] = client.get_pr_approved(identifier)
        except Exception:
          self._pr_approved_cache[identifier] = None

    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=min(len(to_fetch), 8)) as executor:
      list(executor.map(fetch_one, to_fetch))

  def _ensure_pr(self, client: CodeHostingClient, spec: CodeHostingSpec,
                 branch: LocalBranchShortName, parent: LocalBranchShortName,
                 state: XtaxState, stack_name: str) -> None:
    """Create or retarget a PR/MR for branch."""
    pr_label = f"{spec.pr_short_name} {spec.pr_ordinal_char}"
    annotation = state.annotations.get(branch)
    existing_pr_id = self._extract_pr_identifier(annotation, spec)

    # If we already have a PR identifier, check if it's still open and needs retargeting
    if existing_pr_id is not None:
      pr = client.get_pull_request_by_identifier_or_none(existing_pr_id)
      if pr and pr.state == 'open':
        if pr.base != str(parent):
          client.set_base_of_pull_request(existing_pr_id, parent)
          print(f"  Retargeted {pr_label}{existing_pr_id} for {bold(branch)} → {bold(parent)}")
        else:
          debug(f"Branch {branch} already has {pr_label}{existing_pr_id} targeting {parent}")
        return
      # PR is closed/merged — fall through to find or create a new one

    # Check if an open PR already exists with matching source and target
    existing_prs = client.get_open_pull_requests_by_head(branch)
    matching_pr = next(
      (m for m in existing_prs if m.head == str(branch) and m.base == str(parent)),
      None
    )
    if matching_pr:
      pr = matching_pr
      print(f"  Found existing {pr_label}{pr.identifier} for {bold(branch)} → {bold(parent)}")
    else:
      # Create new PR
      org_repo = client.get_org_and_repo()
      pr = client.create_pull_request(
        head=str(branch),
        head_org_repo=org_repo,
        base=str(parent),
        title=str(branch),
        description='',
        draft=True,
      )
      print(f"  Created {pr_label}{pr.identifier} for {bold(branch)} → {bold(parent)}")

    # Save PR annotation
    pr_text = f"{spec.pr_short_name} {spec.pr_ordinal_char}{pr.identifier}"
    qualifiers_text = ''
    if annotation and annotation.qualifiers.is_non_default():
      qualifiers_text = str(annotation.qualifiers)
    full_text = f"{pr_text} {qualifiers_text}".strip() if qualifiers_text else pr_text
    state.annotations[branch] = Annotation.parse(full_text)
    self._save_state(stack_name, state)

  def _save_state(self, name: str, state: XtaxState) -> None:
    content = StackStorage.render_definition(state)
    self._storage.write_stack_definition(name, content)

  def _current_branch(self) -> LocalBranchShortName:
    branch = self._git.get_currently_checked_out_branch_or_none()
    if not branch:
      raise XtaxException("Not on any branch (detached HEAD)")
    return branch

  @staticmethod
  def _input_with_prefill(prompt: str, prefill: str) -> str:
    label = prompt.rstrip(": ")
    hint = dim(f"[press Enter for {prefill}]")
    result = input(f"{label} {hint}: ").strip()
    return result if result else prefill

  def _resolve_stack_for_branch(self, branch: LocalBranchShortName) -> Tuple[str, XtaxState]:
    """Find the stack that contains a managed branch."""
    stack_name = self._storage.find_stack_for_branch(branch)
    if not stack_name:
      raise XtaxException(
        f"Branch {bold(branch)} is not in any stack. "
        f"Check out a branch in a stack, or use --onto=<parent>.")
    content = self._storage.read_stack_definition(stack_name)
    if content is None:
      raise XtaxException(f"Stack {bold(stack_name)} not found")
    state = StackStorage.parse_definition(content)
    return stack_name, state

  def _resolve_current_stack(self) -> Tuple[str, XtaxState]:
    """Resolve the stack from the currently checked out branch."""
    current = self._current_branch()
    return self._resolve_stack_for_branch(current)

  # --- Commands ---

  def cmd_init(self, args: List[str]) -> None:
    self._fetch_stacks()

    existing = self._storage.list_stacks()

    if args:
      # Legacy: git xtax init <name> <branch> [--root=<base>]
      root_arg: Optional[str] = None
      positional = []
      for arg in args:
        if arg.startswith('--root='):
          root_arg = arg[len('--root='):]
        else:
          positional.append(arg)
      if len(positional) < 2:
        raise XtaxException("Usage: git xtax init <stack> <branch> [--root=<base>]")
      name = positional[0]
      first_branch_name = positional[1]
      root = root_arg if root_arg else str(self._current_branch())
    else:
      # Interactive mode
      name = input(rl_safe("Stack name: ")).strip()
      if not name:
        raise XtaxException("Aborted")
      if name in existing:
        raise XtaxException(f"Stack {bold(name)} already exists")
      current = self._git.get_currently_checked_out_branch_or_none()
      default_branch = str(current) if current else ""
      first_branch_name = self._input_with_prefill("First branch name: ", default_branch)
      if not first_branch_name:
        raise XtaxException("Aborted")
      # Detect the repo's default branch (e.g. main, master, develop)
      result = self._git._popen_git('symbolic-ref', 'refs/remotes/origin/HEAD', allow_non_zero=True)
      if result.exit_code == 0:
        default_root = result.stdout.strip().replace('refs/remotes/origin/', '')
      else:
        default_root = "main"
      root = self._input_with_prefill("Root branch: ", default_root)
      if not root:
        raise XtaxException("Aborted")

    if name in existing:
      raise XtaxException(f"Stack {bold(name)} already exists")

    # Check that first branch isn't already in another stack
    first_branch = LocalBranchShortName.of(first_branch_name)
    existing_stack = self._storage.find_stack_for_branch(first_branch)
    if existing_stack:
      raise XtaxException(
        f"Branch {bold(first_branch)} is already in stack {bold(existing_stack)}")

    root_branch = LocalBranchShortName.of(root)

    # Create branch if it doesn't exist
    if first_branch not in self._git.get_local_branches():
      answer = input(rl_safe(f"Branch {bold(first_branch)} does not exist. Create it from {bold(root_branch)}? [y/N] "))
      if answer.lower() not in ('y', 'yes'):
        raise XtaxException("Aborted")
      self._git.create_branch(first_branch, AnyRevision.of(str(root_branch)), switch_head=False)
      print(f"Created branch {bold(first_branch)} from {bold(root_branch)}")

    state = XtaxState()
    state.root = root_branch
    state.managed_branches.append(first_branch)
    state.down_branches_for[root_branch] = [first_branch]
    state.up_branch_for[first_branch] = root_branch

    self._save_state(name, state)
    print(f"Created stack {bold(name)} (root: {bold(root)}, branch: {bold(first_branch)})")

  def cmd_stack(self, args: List[str], stack_name: Optional[str] = None) -> None:
    self._fetch_stacks()
    if not args:
      raise XtaxException("Usage: git xtax stack <branch> [--onto=<parent>]")

    branch_name = args[0]
    onto: Optional[str] = None
    for arg in args[1:]:
      if arg.startswith('--onto='):
        onto = arg[len('--onto='):]

    branch = LocalBranchShortName.of(branch_name)

    # Check branch isn't already in a stack
    existing_stack = self._storage.find_stack_for_branch(branch)
    if existing_stack:
      raise XtaxException(
        f"Branch {bold(branch)} is already in stack {bold(existing_stack)}")

    # Resolve parent and stack
    if stack_name:
      content = self._storage.read_stack_definition(stack_name)
      if content is None:
        raise XtaxException(f"Stack {bold(stack_name)} not found")
      name = stack_name
      state = StackStorage.parse_definition(content)
      parent = LocalBranchShortName.of(onto) if onto else state.root
    elif onto is not None:
      parent = LocalBranchShortName.of(onto)
      name, state = self._resolve_stack_for_branch(parent)
    else:
      current = self._current_branch()
      parent = current
      name, state = self._resolve_stack_for_branch(current)

    # Reject adding onto root (unless stack is empty)
    if parent == state.root and state.managed_branches:
      raise XtaxException(
        f"Cannot add onto root branch {bold(parent)}. "
        f"Add onto a non-root branch in the stack.")

    if parent != state.root and parent not in state.managed_branches:
      raise XtaxException(f"Branch {bold(parent)} is not in the stack")

    # Create branch if it doesn't exist
    if branch not in self._git.get_local_branches():
      answer = input(rl_safe(f"Branch {bold(branch)} does not exist. Create it from {bold(parent)}? [y/N] "))
      if answer.lower() not in ('y', 'yes'):
        raise XtaxException("Aborted")
      self._git.create_branch(branch, AnyRevision.of(str(parent)), switch_head=False)
      print(f"Created branch {bold(branch)} from {bold(parent)}")

    existing_child = None
    if parent in state.down_branches_for and state.down_branches_for[parent]:
      existing_child = state.down_branches_for[parent][0]

    if existing_child:
      # Insert between parent and existing child:
      # parent -> branch -> existing_child (was: parent -> existing_child)
      state.down_branches_for[parent] = [branch]
      state.up_branch_for[branch] = parent
      state.down_branches_for[branch] = [existing_child]
      state.up_branch_for[existing_child] = branch
      managed_idx = state.managed_branches.index(existing_child)
      state.managed_branches.insert(managed_idx, branch)
    else:
      state.down_branches_for[parent] = [branch]
      state.up_branch_for[branch] = parent
      state.managed_branches.append(branch)

    self._save_state(name, state)
    print(f"Stacked {bold(branch)} onto {bold(parent)} in stack {bold(name)}")

  def cmd_tuck(self, args: List[str]) -> None:
    self._fetch_stacks()
    if not args:
      raise XtaxException("Usage: git xtax tuck <branch>")

    branch_name = args[0]
    branch = LocalBranchShortName.of(branch_name)

    # Check branch isn't already in a stack
    existing_stack = self._storage.find_stack_for_branch(branch)
    if existing_stack:
      raise XtaxException(
        f"Branch {bold(branch)} is already in stack {bold(existing_stack)}")

    # The current branch is the one we insert above
    current = self._current_branch()
    name, state = self._resolve_stack_for_branch(current)

    if current not in state.managed_branches:
      raise XtaxException(f"Branch {bold(current)} is not in the stack")

    parent = state.up_branch_for.get(current, state.root)

    # Create branch if it doesn't exist
    if branch not in self._git.get_local_branches():
      answer = input(rl_safe(f"Branch {bold(branch)} does not exist. Create it from {bold(parent)}? [y/N] "))
      if answer.lower() not in ('y', 'yes'):
        raise XtaxException("Aborted")
      self._git.create_branch(branch, AnyRevision.of(str(parent)), switch_head=False)
      print(f"Created branch {bold(branch)} from {bold(parent)}")

    # Insert branch between parent and current:
    # parent -> branch -> current (was: parent -> current)

    # 1. Replace current with branch in parent's children list
    if parent in state.down_branches_for:
      children = state.down_branches_for[parent]
      idx = children.index(current)
      children[idx] = branch
    else:
      state.down_branches_for[parent] = [branch]

    # 2. branch becomes child of parent
    state.up_branch_for[branch] = parent

    # 3. current becomes child of branch
    state.up_branch_for[current] = branch
    state.down_branches_for[branch] = [current]

    # 4. Add branch to managed list (before current)
    managed_idx = state.managed_branches.index(current)
    state.managed_branches.insert(managed_idx, branch)

    self._save_state(name, state)
    print(f"Tucked {bold(branch)} under {bold(current)} in stack {bold(name)}")

  def cmd_slideout(self, args: List[str], stack_name: Optional[str] = None) -> None:
    self._fetch_stacks()
    if not args:
      raise XtaxException("Usage: git xtax slideout <branch>")

    if stack_name:
      content = self._storage.read_stack_definition(stack_name)
      if content is None:
        raise XtaxException(f"Stack {bold(stack_name)} not found")
      name = stack_name
      state = StackStorage.parse_definition(content)
    else:
      name, state = self._resolve_current_stack()
    branch = LocalBranchShortName.of(args[0])

    if branch not in state.managed_branches:
      raise XtaxException(f"Branch {bold(branch)} is not in the stack")

    # Re-parent children to branch's parent (slide out)
    parent = state.up_branch_for.get(branch)
    children = state.down_branches_for.get(branch, [])

    for child in children:
      if parent and parent != state.root:
        state.up_branch_for[child] = parent
      elif parent == state.root:
        # Child becomes a direct child of root
        state.up_branch_for[child] = state.root
      else:
        del state.up_branch_for[child]

    if parent and parent in state.down_branches_for:
      idx = state.down_branches_for[parent].index(branch)
      state.down_branches_for[parent][idx:idx + 1] = children
      if not state.down_branches_for[parent]:
        del state.down_branches_for[parent]

    state.managed_branches.remove(branch)
    if branch in state.down_branches_for:
      del state.down_branches_for[branch]
    if branch in state.up_branch_for:
      del state.up_branch_for[branch]
    if branch in state.annotations:
      del state.annotations[branch]

    self._save_state(name, state)
    print(f"Removed {bold(branch)} from stack (children re-parented)")

  def cmd_delete(self, args: List[str]) -> None:
    if not args:
      raise XtaxException("Usage: git xtax delete <name>")

    name = args[0]
    stacks = self._storage.list_stacks()
    if name not in stacks:
      raise XtaxException(
        f"Stack {bold(name)} not found. Available stacks: {', '.join(stacks) or '(none)'}")

    self._storage.delete_stack(name)
    print(f"Deleted stack {bold(name)}")

  def _branch_exists_anywhere(self, branch: LocalBranchShortName) -> bool:
    """Check if branch exists locally or on remote."""
    if branch in self._git.get_local_branches():
      return True
    result = self._git._popen_git(
      "rev-parse", "--verify", f"refs/remotes/origin/{branch}", allow_non_zero=True)
    return result.exit_code == 0

  def _branch_info_str(self, branch: LocalBranchShortName, state: XtaxState,
                        highlighted: Optional[LocalBranchShortName],
                        checked_out: Optional[LocalBranchShortName] = None) -> str:
    deleted_color = "\033[38;2;231;63;63m"
    is_deleted = not self._branch_exists_anywhere(branch)

    if is_deleted:
      is_highlighted = branch == highlighted
      node = "◉" if is_highlighted else "○"
      return colored(f"{node} {branch}  deleted", deleted_color)

    # Node marker follows highlighted; name color follows checked_out
    active_color = AnsiEscapeCodes.GREEN
    is_highlighted = branch == highlighted
    is_checked_out = branch == (checked_out if checked_out is not None else highlighted)

    pointer = "❯ " if is_highlighted else "  "
    if is_highlighted:
      if is_checked_out:
        node = colored("◉", active_color)
        branch_str = f"{pointer}{node} " + colored(bold(str(branch)), active_color)
      else:
        node = "◉"
        branch_str = f"{pointer}{node} " + bold(str(branch))
    elif is_checked_out:
      branch_str = f"{pointer}{dim('○')} " + colored(str(branch), active_color)
    else:
      branch_str = f"{pointer}{dim('○')} " + dim(str(branch))

    # Annotation
    anno = ""
    if branch in state.annotations and state.annotations[branch].formatted_full_text:
      annotation = state.annotations[branch]
      pr_status = self._get_pr_status(annotation)
      if pr_status == 'open':
        anno_text = annotation.unformatted_full_text
      elif pr_status == 'merged':
        anno_text = dim(annotation.unformatted_full_text)
      elif pr_status == 'closed':
        anno_text = dim(strikethrough(annotation.unformatted_full_text))
      else:
        anno_text = annotation.formatted_full_text
      pr_url = self._get_pr_url(annotation)
      if pr_url:
        anno_text = hyperlink(anno_text, pr_url)
      # Pipeline status
      pipeline_status, pipeline_finished_at = self._get_pr_pipeline(annotation)
      pr = self._get_cached_pr(annotation)
      pipeline_str = ""
      if pr is not None:
        pending_statuses = {'running', 'pending', 'created', 'waiting_for_resource', 'preparing', 'scheduled', 'manual'}
        if pipeline_status == 'success':
          pipeline_dot = colored('PASS', AnsiEscapeCodes.GREEN)
        elif pipeline_status == 'failed':
          pipeline_dot = colored('FAIL', AnsiEscapeCodes.RED)
        elif pipeline_status in pending_statuses:
          pipeline_dot = colored('PEND', AnsiEscapeCodes.YELLOW)
        else:
          pipeline_dot = colored('UNKW', AnsiEscapeCodes.PURPLE)
        approved = self._get_pr_approved(annotation)
        if approved is True:
          approval_str = colored('(✔)', AnsiEscapeCodes.GREEN)
        elif approved is False:
          approval_str = colored('(✔)', AnsiEscapeCodes.RED)
        else:
          approval_str = ""
        pipeline_str = f" {pipeline_dot}{approval_str}"
        if pipeline_status not in pending_statuses and pipeline_finished_at:
          import datetime
          diff = int((datetime.datetime.now(datetime.timezone.utc) -
                      datetime.datetime.fromisoformat(pipeline_finished_at.replace('Z', '+00:00'))).total_seconds())
          elapsed = self._elapsed_str(pipeline_finished_at)
          elapsed_color = AnsiEscapeCodes.RED if diff >= 86400 else None
          elapsed_inner = colored(elapsed, elapsed_color) if elapsed_color else dim(elapsed)
          elapsed_str = f"{dim('(')}{elapsed_inner}{dim(')')}"
          pipeline_str += elapsed_str
      anno_text += pipeline_str

      unresolved = self._get_pr_unresolved_count(annotation)
      if unresolved:
        review_word = "review" if unresolved == 1 else "reviews"
        anno_text += f"{dim('(')}{colored(f'{unresolved} {review_word}', AnsiEscapeCodes.YELLOW)}{dim(')')}"

      anno = f"  {dim('→')}  " + anno_text

    # Ahead/behind parent, colored by sync status
    parent_info = ""
    parent = state.up_branch_for.get(branch)
    if parent:
      try:
        ahead_behind = self._git._popen_git(
          "rev-list", "--count", "--left-right",
          f"{parent}...{branch}", allow_non_zero=True)
        if ahead_behind.exit_code == 0:
          parts = ahead_behind.stdout.strip().split('\t')
          if len(parts) == 2:
            behind, ahead = parts
            parts_str = []
            if behind != "0":
              parts_str.append(colored(f"{behind}↓", AnsiEscapeCodes.RED))
            if ahead != "0":
              parts_str.append(colored(f"{ahead}↑", AnsiEscapeCodes.GREEN))
            if parts_str:
              parent_info = f"  {dim('→')}  {dim('(')}{' '.join(parts_str)}{dim(')')}"
      except Exception as e:
        debug(f"Failed to get ahead/behind for {branch} vs {parent}: {e}")

    # Merged indicator: branch had a remote tracking ref that's now gone
    merged_info = ""
    try:
      upstream = self._git._popen_git(
        "config", f"branch.{branch}.remote", allow_non_zero=True)
      if upstream.exit_code == 0 and upstream.stdout.strip():
        remote = upstream.stdout.strip()
        remote_ref = self._git._popen_git(
          "rev-parse", "--verify", f"refs/remotes/{remote}/{branch}", allow_non_zero=True)
        if remote_ref.exit_code != 0:
          merged_info = "  " + dim("merged")
    except Exception:
      pass

    return f"{branch_str}{anno}{parent_info}{merged_info}"

  def _build_view_lines(self, name: str, state: XtaxState,
                         highlighted: Optional[LocalBranchShortName],
                         checked_out: Optional[LocalBranchShortName] = None
                         ) -> List[Tuple[str, Optional[LocalBranchShortName]]]:
    """Build the view output as a list of (line, branch_or_None) pairs."""
    self._prefetch_pr_data(state)
    lines: List[Tuple[str, Optional[LocalBranchShortName]]] = []

    # Root branch — show ahead/behind vs remote tracking branch
    root_info = ""
    try:
      remote_ref = f"origin/{state.root}"
      ahead_behind = self._git._popen_git(
        "rev-list", "--count", "--left-right",
        f"{remote_ref}...{state.root}", allow_non_zero=True)
      if ahead_behind.exit_code == 0:
        parts = ahead_behind.stdout.strip().split('\t')
        if len(parts) == 2:
          behind, ahead = parts
          parts_str = []
          if behind != "0":
            parts_str.append(colored(f"{behind}↓", AnsiEscapeCodes.RED))
          if ahead != "0":
            parts_str.append(colored(f"{ahead}↑", AnsiEscapeCodes.GREEN))
          if parts_str:
            root_info = f"  {dim('→')}  {dim('(')}{' '.join(parts_str)}{dim(')')}"
    except Exception as e:
      debug(f"Failed to get ahead/behind for root {state.root}: {e}")
    lines.append((f"    {dim('▲')} {dim(str(state.root))}{root_info}", None))

    root_children = state.down_branches_for.get(state.root, [])
    if not root_children:
      return lines

    def collect_node(branch: LocalBranchShortName, depth: int) -> None:
      info = self._branch_info_str(branch, state, highlighted, checked_out)
      lines.append((info, branch))

      children = state.down_branches_for.get(branch, [])
      for child in children:
        collect_node(child, depth + 1)

    for child in root_children:
      collect_node(child, 1)

    # Reverse so root is at bottom and leaves at top (standard stack metaphor)
    lines.reverse()

    # Insert connector lines between branches
    i = 1
    while i < len(lines):
      lines.insert(i, (f"    {dim('│')}", None))
      i += 2

    # Add indentation for managed branches
    for i, (line, branch) in enumerate(lines):
      if branch is not None:
        lines[i] = (f"  {line}", branch)

    # Stack name header at the top
    lines.insert(0, ("", None))
    lines.insert(1, (f"Stack: {bold(name)}{self._xtax_ahead_behind_str()}", None))
    lines.insert(2, ("", None))

    return lines

  def _read_key(self) -> str:
    if sys.platform == 'win32':
      return self._read_key_windows()
    return self._read_key_unix()

  def _read_key_windows(self) -> str:
    ch = msvcrt.getwch()
    if ch in ('\x00', '\xe0'):
      # Special key — read the second byte
      ch2 = msvcrt.getwch()
      return {'H': 'up', 'P': 'down'}.get(ch2, '')
    elif ch == '\x1b':
      return 'escape'
    elif ch in ('\r', '\n'):
      return 'enter'
    elif ch == '\x03':
      return 'ctrl-c'
    elif ch == 'k':
      return 'up'
    elif ch == 'j':
      return 'down'
    elif ch == 'q':
      return 'ctrl-c'
    elif ch == 's':
      return 'stack'
    elif ch == 't':
      return 'tuck'
    elif ch == 'd':
      return 'delete'
    elif ch == 'r':
      return 'rename'
    elif ch == 'c':
      return 'commits'
    elif ch == 'm':
      return 'merge'
    return ''

  def _read_key_unix(self) -> str:
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
      tty.setraw(fd)
      ch = os.read(fd, 1)
      if ch == b'\x1b':
        import select as _select
        if _select.select([fd], [], [], 0.1)[0]:
          ch2 = os.read(fd, 1)
          if ch2 == b'[':
            ch3 = os.read(fd, 1)
            return {b'A': 'up', b'B': 'down'}.get(ch3, '')
        else:
          return 'escape'
      elif ch in (b'\r', b'\n'):
        return 'enter'
      elif ch == b'\x03':
        return 'ctrl-c'
      elif ch == b'k':
        return 'up'
      elif ch == b'j':
        return 'down'
      elif ch == b'q':
        return 'ctrl-c'
      elif ch == b's':
        return 'stack'
      elif ch == b't':
        return 'tuck'
      elif ch == b'd':
        return 'delete'
      elif ch == b'r':
        return 'rename'
      elif ch == b'c':
        return 'commits'
      elif ch == b'm':
        return 'merge'
      return ''
    finally:
      termios.tcsetattr(fd, termios.TCSADRAIN, old)

  def _exit_interactive(self, num_lines: int) -> None:
    """Clear interactive display and restore cursor."""
    if num_lines > 1:
      sys.stdout.write(f"\x1b[{num_lines - 1}A")
    sys.stdout.write('\r\x1b[J')
    sys.stdout.write('\x1b[?25h')
    sys.stdout.flush()

  def cmd_list(self, args: List[str]) -> None:
    self._link_unlinked_mrs()
    return self._cmd_view_all(args)

  def _print_view(self) -> None:
    """Print static (non-interactive) stack view."""
    name, state = self._resolve_current_stack()
    current = self._git.get_currently_checked_out_branch_or_none()
    for line, _ in self._build_view_lines(name, state, current):
      print(line)

  def cmd_view(self, args: List[str], stack_name: Optional[str] = None) -> Optional[str]:
    self._link_unlinked_mrs()
    if stack_name:
      content = self._storage.read_stack_definition(stack_name)
      if content is None:
        raise XtaxException(f"Stack {bold(stack_name)} not found")
      name = stack_name
      state = StackStorage.parse_definition(content)
    else:
      name, state = self._resolve_current_stack()
    current = self._git.get_currently_checked_out_branch_or_none()

    if not sys.stdin.isatty():
      for line, _ in self._build_view_lines(name, state, current):
        print(line)
      return

    managed = state.managed_branches
    if not managed:
      if not sys.stdin.isatty():
        for line, _ in self._build_view_lines(name, state, current):
          print(line)
        return
      # Empty stack — show view with hint, allow append
      hint = f"  s{dim(': stack first branch')}"
      lines = self._build_view_lines(name, state, current)
      num_lines = len(lines) + 1
      sys.stdout.write('\x1b[?25l')
      sys.stdout.write('\n'.join(line for line, _ in lines) + '\n' + hint)
      sys.stdout.flush()
      try:
        while True:
          key = self._read_key()
          if key == 'stack':
            self._exit_interactive(num_lines)
            root = state.root
            branch_name = input(rl_safe(f"Enter branch name to add to {bold(name)}: ")).strip()
            if branch_name:
              try:
                self.cmd_stack([branch_name, f'--onto={root}'], stack_name=name)
              except XtaxException as e:
                print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
            # Re-enter cmd_view to show updated state
            return self.cmd_view(args, stack_name=stack_name)
          elif key == 'escape':
            self._exit_interactive(num_lines)
            return 'back'
          elif key in ('ctrl-c', 'enter'):
            self._exit_interactive(num_lines)
            for line, _ in lines:
              print(line)
            return
      except Exception:
        sys.stdout.write('\x1b[?25h')
        sys.stdout.flush()
        raise

    cursor = 0
    if current and current in managed:
      cursor = managed.index(current)

    while True:
      # Refresh state each iteration (actions may have changed it)
      if stack_name:
        content = self._storage.read_stack_definition(stack_name)
        if content is None:
          return
        name = stack_name
        state = StackStorage.parse_definition(content)
      else:
        name, state = self._resolve_current_stack()
      current = self._git.get_currently_checked_out_branch_or_none()
      managed = state.managed_branches
      if not managed:
        for line, _ in self._build_view_lines(name, state, current):
          print(line)
        return
      cursor = min(cursor, len(managed) - 1)

      hint = f"s{dim(': stack')}  t{dim(': tuck')}  r{dim(': rename')}  d{dim(': slide out')}  c{dim(': view commits')}  m{dim(': merge')}"

      def render_lines(cursor_idx: int) -> List[str]:
        highlighted = managed[cursor_idx]
        lines = self._build_view_lines(name, state, highlighted, checked_out=current)
        return [line for line, _ in lines] + ["", hint]

      num_lines = len(self._build_view_lines(name, state, current)) + 2
      sys.stdout.write('\x1b[?25l')

      def draw(cursor_idx: int) -> None:
        out = render_lines(cursor_idx)
        for i, line in enumerate(out):
          sys.stdout.write(f'\r\x1b[K{line}')
          if i < len(out) - 1:
            sys.stdout.write('\n')
        sys.stdout.flush()

      def move_to_top() -> None:
        if num_lines > 1:
          sys.stdout.write(f"\x1b[{num_lines - 1}A")

      draw(cursor)

      action = None
      try:
        while True:
          key = self._read_key()
          if key == 'up' and cursor < len(managed) - 1:
            cursor += 1
          elif key == 'down' and cursor > 0:
            cursor -= 1
          elif key == 'enter':
            selected = managed[cursor]
            if not self._branch_exists_anywhere(selected):
              continue
            self._exit_interactive(num_lines)
            if selected != current:
              self._git.checkout(selected)
            for line, _ in self._build_view_lines(name, state, selected):
              print(line)
            return
          elif key == 'escape':
            self._exit_interactive(num_lines)
            return 'back'
          elif key == 'ctrl-c':
            self._exit_interactive(num_lines)
            for line, _ in self._build_view_lines(name, state, current):
              print(line)
            return
          elif key in ('stack', 'tuck', 'delete', 'rename', 'commits', 'merge'):
            action = key
            self._exit_interactive(num_lines)
            break
          else:
            continue

          move_to_top()
          draw(cursor)
      except Exception:
        sys.stdout.write('\x1b[?25h')
        sys.stdout.flush()
        raise

      # Handle actions outside interactive mode
      selected_branch = managed[cursor]
      if action == 'stack':
        branch_name = input(rl_safe(f"Enter branch name to stack above {bold(selected_branch)}: ")).strip()
        if branch_name:
          try:
            self.cmd_stack([branch_name, f'--onto={selected_branch}'])
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
      elif action == 'tuck':
        branch_name = input(rl_safe(f"Enter branch name to tuck under {bold(selected_branch)}: ")).strip()
        if branch_name:
          try:
            # Checkout selected branch so tuck works relative to it
            if current != selected_branch:
              self._git.checkout(selected_branch)
            self.cmd_tuck([branch_name])
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
          finally:
            # Restore original checkout
            if current and current != selected_branch:
              self._git.checkout(current)
      elif action == 'delete':
        confirm = input(rl_safe(f"Remove {bold(selected_branch)} from stack? [y/N] ")).strip()
        if confirm.lower() in ('y', 'yes'):
          try:
            self.cmd_slideout([str(selected_branch)], stack_name=name)
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
      elif action == 'rename':
        new_name = input(rl_safe(f"Rename {bold(selected_branch)} to: ")).strip()
        if new_name:
          try:
            self.cmd_rename_branch([str(selected_branch), new_name])
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
      elif action == 'commits':
        parent = state.up_branch_for.get(selected_branch, state.root)
        result = self._git._popen_git(
          'log', f'{parent}..{selected_branch}', '--pretty=format:%h %s', allow_non_zero=True)
        lines_out = ["", f"Branch: {bold(str(selected_branch))}", ""]
        if result.stdout.strip():
          for commit_line in result.stdout.rstrip().splitlines():
            parts = commit_line.split(' ', 1)
            if len(parts) == 2:
              hash_str, message = parts
              lines_out.append(f"  {colored(hash_str, AnsiEscapeCodes.YELLOW)} {message}")
            else:
              lines_out.append(f"  {commit_line}")
        else:
          lines_out.append(dim(f"  No commits between {parent} and {selected_branch}"))
        lines_out.append("")
        lines_out.append(dim("Press Esc to return..."))
        print('\n'.join(lines_out))
        sys.stdout.flush()
        while self._read_key() != 'escape':
          pass
        # Clear the commits output
        sys.stdout.write(f"\x1b[{len(lines_out)}A\r\x1b[J")
        sys.stdout.flush()
      elif action == 'merge':
        annotation = state.annotations.get(selected_branch)
        if not self._hosting_info_resolved:
          self._hosting_info_resolved = True
          try:
            self._hosting_info = self._get_code_hosting_client()
          except Exception:
            pass
        hosting = self._hosting_info
        if not hosting:
          print(colored("Error: no code hosting client configured", AnsiEscapeCodes.RED))
        else:
          client, spec = hosting
          identifier = self._extract_pr_identifier(annotation, spec)
          if not identifier:
            print(colored(f"Error: no {spec.pr_short_name} found for {selected_branch}", AnsiEscapeCodes.RED))
          else:
            pr_label = f"{spec.pr_short_name} {spec.pr_ordinal_char}{identifier}"
            pr = client.get_pull_request_by_identifier_or_none(identifier)
            self._pr_cache[identifier] = pr
            if pr and str(pr.base) != str(state.root):
              print(colored(f"Warning: {pr_label} targets {bold(str(pr.base))}, not {bold(str(state.root))}", AnsiEscapeCodes.YELLOW))
            target = str(pr.base) if pr else str(state.root)
            confirm = input(rl_safe(f"Merge {bold(str(selected_branch))} into {bold(target)} ({pr_label})? [y/N] ")).strip()
            if confirm.lower() in ('y', 'yes'):
              try:
                client.merge_pull_request(identifier)
                print(f"Merged {bold(pr_label)}")
                slide = input(rl_safe(f"Slide out {bold(selected_branch)} from stack? [y/N] ")).strip()
                if slide.lower() in ('y', 'yes'):
                  was_checked_out = current == selected_branch
                  children = state.down_branches_for.get(selected_branch, [])
                  self.cmd_slideout([str(selected_branch)], stack_name=name)
                  if was_checked_out:
                    checkout_target = children[0] if children else state.root
                    if checkout_target:
                      self._git.checkout(LocalBranchShortName.of(str(checkout_target)))
                  cursor = max(0, cursor - 1)
              except XtaxException as e:
                print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
              except Exception as e:
                print(colored(f"Merge failed: {e}", AnsiEscapeCodes.RED))

      # Loop back to re-render interactive view

  def cmd_up(self, args: List[str]) -> None:
    name, state = self._resolve_current_stack()
    current = self._current_branch()
    if current not in state.managed_branches:
      raise XtaxException(f"Branch {bold(current)} is not in the stack")
    children = state.down_branches_for.get(current, [])
    if not children:
      raise XtaxException(f"Branch {bold(current)} is at the top of the stack")
    self._git.checkout(children[0])
    self._print_view()

  def cmd_down(self, args: List[str]) -> None:
    name, state = self._resolve_current_stack()
    current = self._current_branch()
    if current not in state.managed_branches:
      raise XtaxException(f"Branch {bold(current)} is not in the stack")
    parent = state.up_branch_for.get(current)
    if not parent or parent == state.root:
      raise XtaxException(f"Branch {bold(current)} is at the bottom of the stack")
    self._git.checkout(parent)
    self._print_view()

  def cmd_top(self, args: List[str]) -> None:
    name, state = self._resolve_current_stack()
    root_children = state.down_branches_for.get(state.root, [])
    if not root_children:
      raise XtaxException("Stack has no branches")
    # Follow first children to the deepest leaf (top of stack)
    target = root_children[0]
    while True:
      children = state.down_branches_for.get(target, [])
      if not children:
        break
      target = children[0]
    self._git.checkout(target)
    self._print_view()

  def cmd_bottom(self, args: List[str]) -> None:
    name, state = self._resolve_current_stack()
    root_children = state.down_branches_for.get(state.root, [])
    if not root_children:
      raise XtaxException("Stack has no branches")
    # First child of root (bottom of stack)
    target = root_children[0]
    self._git.checkout(target)
    self._print_view()

  def cmd_go_n(self, n: int) -> None:
    """Navigate by integer. Positive = up N (toward leaf), negative = down N (toward root), 0 = leaf."""
    name, state = self._resolve_current_stack()
    current = self._current_branch()
    if current not in state.managed_branches:
      raise XtaxException(f"Branch {bold(current)} is not in the stack")

    if n == 0:
      # Go to leaf branch in stack (deepest child)
      root_children = state.down_branches_for.get(state.root, [])
      if not root_children:
        raise XtaxException("Stack has no branches")
      target = root_children[0]
      while True:
        children = state.down_branches_for.get(target, [])
        if not children:
          break
        target = children[0]
      self._git.checkout(target)
      self._print_view()
      return

    if n > 0:
      # Positive = up toward leaf (children)
      target = current
      for _ in range(n):
        children = state.down_branches_for.get(target, [])
        if not children:
          raise XtaxException(
            f"Cannot go {n} up from {bold(current)} - "
            f"reached top of stack at {bold(target)}")
        target = children[0]
    else:
      # Negative = down toward root (parent)
      target = current
      for _ in range(-n):
        parent = state.up_branch_for.get(target)
        if not parent or parent == state.root:
          raise XtaxException(
            f"Cannot go {-n} down from {bold(current)} - "
            f"reached bottom of stack at {bold(target)}")
        target = parent

    self._git.checkout(target)
    self._print_view()

  def cmd_sync(self, args: List[str]) -> None:
    self._fetch_stacks()
    is_continue = '--continue' in args
    is_current_only = '--current' in args
    is_cascade = '--cascade' in args
    include_merged = '--include-merged' in args

    if is_continue:
      self._sync_continue()
      return

    try:
      name, state = self._resolve_current_stack()
    except XtaxException:
      stacks = self._storage.list_stacks()
      if not stacks:
        print(fmt("<yellow>No stacks defined. Use `git xtax init <stack> <branch>` to create one.</yellow>"))
        return
      current = self._git.get_currently_checked_out_branch_or_none()
      print(fmt(f"<yellow>Branch {bold(current)} is not in any stack.</yellow>"))
      if not sys.stdin.isatty():
        raise
      print("Select a stack to sync:")
      action, selected = self._interactive_stack_select(stacks, None)
      if action != 'select' or not selected:
        raise InteractionStopped()
      content = self._storage.read_stack_definition(selected)
      if content is None:
        raise XtaxException(f"Stack {bold(selected)} not found")
      name = selected
      state = StackStorage.parse_definition(content)
    current = self._current_branch()

    print("Fetching from origin...")
    self._git.fetch_remote('origin')

    # Fast-forward root branch if possible
    root = state.root
    remote_root = f'origin/{root}'
    has_remote_root = self._git._popen_git(
      'rev-parse', '--verify', remote_root, allow_non_zero=True).exit_code == 0
    if has_remote_root:
      ff_result = self._git._popen_git(
        'merge-base', '--is-ancestor', str(root), remote_root, allow_non_zero=True)
      if ff_result.exit_code == 0:
        # Local root is behind or equal — safe to fast-forward
        self._git._popen_git('update-ref', f'refs/heads/{root}',
                             self._git._popen_git('rev-parse', remote_root).stdout.strip())
        print(f"Fast-forwarded {bold(root)} to {bold(remote_root)}")
      else:
        # Local root has commits not on remote
        print(colored(
          f"Warning: {bold(root)} has local commits not on remote — skipping fast-forward",
          AnsiEscapeCodes.YELLOW))

    if is_current_only:
      if current not in state.managed_branches:
        raise XtaxException(f"Branch {bold(current)} is not in the stack")
      branches_to_sync = [current]
    elif is_cascade:
      if current not in state.managed_branches:
        raise XtaxException(f"Branch {bold(current)} is not in the stack")
      branches_to_sync = self._get_dfs_order(state, current)
    else:
      # Sync entire stack from root's children
      branches_to_sync: List[LocalBranchShortName] = []
      root_children = state.down_branches_for.get(state.root, [])
      for child in root_children:
        branches_to_sync.extend(self._get_dfs_order(state, child))

    if not include_merged:
      self._prefetch_pr_data(state)
      filtered = []
      for branch in branches_to_sync:
        annotation = state.annotations.get(branch)
        status = self._get_pr_status(annotation)
        if status == 'merged':
          print(dim(f"Skipping {bold(branch)} (MR already merged)"))
        else:
          filtered.append(branch)
      branches_to_sync = filtered

    self._sync_branches(name, state, branches_to_sync, start_index=0,
                        original_branch=str(current))

    currently_on = self._git.get_currently_checked_out_branch_or_none()
    if currently_on != current:
      print(f"\nRestoring checkout to {bold(current)}...")
      self._git.checkout(current)

  def _get_dfs_order(self, state: XtaxState, start: LocalBranchShortName) -> List[LocalBranchShortName]:
    result: List[LocalBranchShortName] = [start]

    def dfs(branch: LocalBranchShortName) -> None:
      for child in state.down_branches_for.get(branch, []):
        result.append(child)
        dfs(child)

    dfs(start)
    return result

  def _sync_branches(self, stack_name: str, state: XtaxState,
                     branches: List[LocalBranchShortName], start_index: int,
                     original_branch: Optional[str] = None) -> None:
    hosting = self._get_code_hosting_client()

    for i in range(start_index, len(branches)):
      branch = branches[i]
      parent = state.up_branch_for.get(branch)

      if not parent:
        print(f"\n{bold(branch)} (no parent - skipping rebase)")
        continue

      print(f"\nRebasing {bold(branch)} onto {bold(parent)}...")

      self._storage.save_sync_state({
        'stack_name': stack_name,
        'branches': [str(b) for b in branches],
        'current_index': i,
        'original_branch': original_branch,
      })

      try:
        remote_parent_ref = f'origin/{parent}'
        has_remote_parent = self._git._popen_git(
          'rev-parse', '--verify', remote_parent_ref,
          allow_non_zero=True
        ).exit_code == 0

        if has_remote_parent:
          self._git.rebase_onto(
            AnyRevision.of(str(parent)),
            AnyRevision.of(remote_parent_ref),
            branch,
          )
        else:
          # No remote parent — use local parent as upstream
          self._git.rebase_onto(
            AnyRevision.of(str(parent)),
            AnyRevision.of(str(parent)),
            branch,
          )
      except UnderlyingGitException:
        print(colored(
          f"\nConflict while rebasing {bold(branch)} onto {bold(parent)}.\n"
          f"Resolve the conflict, then run: git xtax sync --continue",
          AnsiEscapeCodes.RED
        ))
        return

      print(f"Pushing {bold(branch)}...")
      try:
        self._git.push('origin', branch, force_with_lease=True)
      except UnderlyingGitException as e:
        warn(f"Failed to push {bold(branch)}: {e}")

      # Create or update PR/MR
      if hosting and parent:
        client, spec = hosting
        try:
          self._ensure_pr(client, spec, branch, parent, state, stack_name)
        except Exception as e:
          warn(f"Failed to create/update {spec.pr_short_name} for {bold(branch)}: {e}")

    self._storage.clear_sync_state()

    # Push stack metadata
    try:
      self._storage.push_stacks()
      print(f"Pushed stack metadata to origin")
    except Exception as e:
      warn(f"Failed to push stack metadata: {e}")

    print(f"\n{fmt('<green><b>Sync complete!</b></green>')}")

  def _sync_continue(self) -> None:
    sync_state = self._storage.load_sync_state()
    if not sync_state:
      raise XtaxException("No sync in progress. Nothing to continue.")

    stack_name = sync_state['stack_name']
    branches = [LocalBranchShortName.of(b) for b in sync_state['branches']]
    current_index = sync_state['current_index']
    original_branch = sync_state.get('original_branch')

    content = self._storage.read_stack_definition(stack_name)
    if content is None:
      raise XtaxException(f"Stack {bold(stack_name)} not found")
    state = StackStorage.parse_definition(content)

    git_dir = self._git.get_current_worktree_git_dir()
    rebase_in_progress = (
      os.path.isdir(os.path.join(git_dir, 'rebase-merge')) or
      os.path.isdir(os.path.join(git_dir, 'rebase-apply'))
    )

    if rebase_in_progress:
      raise XtaxException(
        "A rebase is still in progress. Complete it first with "
        "`git rebase --continue`, then run `git xtax sync --continue`.")

    branch = branches[current_index]
    print(f"Pushing {bold(branch)} after conflict resolution...")
    try:
      self._git.push('origin', branch, force_with_lease=True)
    except UnderlyingGitException as e:
      warn(f"Failed to push {bold(branch)}: {e}")

    self._sync_branches(stack_name, state, branches, start_index=current_index + 1,
                        original_branch=original_branch)

    if original_branch:
      currently_on = self._git.get_currently_checked_out_branch_or_none()
      target = LocalBranchShortName.of(original_branch)
      if currently_on != target:
        print(f"\nRestoring checkout to {bold(target)}...")
        self._git.checkout(target)

  def cmd_push(self, args: List[str]) -> None:
    remote = 'origin'
    is_continue = '--continue' in args
    for arg in args:
      if arg.startswith('--remote='):
        remote = arg[len('--remote='):]

    # Check if resuming after merge conflict resolution
    merge_state = self._storage.load_merge_state()
    if merge_state or is_continue:
      if not merge_state:
        raise XtaxException("No merge conflict in progress")
      self._finish_xtax_merge(merge_state)
      self._storage.push_stacks(remote)
      self._storage.clear_merge_state()
      print(f"Merged stack metadata and pushed to {remote}")
      return

    # Check if resuming after rebase conflict resolution
    push_state = self._storage.load_push_state()
    if push_state:
      self._storage.push_stacks(remote)
      original = push_state.get('original_branch')
      self._storage.clear_push_state()
      if original:
        self._git.checkout(LocalBranchShortName.of(original))
        print(f"Pushed stacks to {remote}, restored checkout to {original}")
      else:
        print(f"Pushed stacks to {remote}")
      return

    # Normal push: fetch + rebase if needed
    result = self._storage.fetch_and_fast_forward(remote)

    if result in (None, 'created', 'updated', 'ahead'):
      self._storage.push_stacks(remote)
      print(f"Pushed stacks to {remote}")
      return

    # Diverged — need to rebase
    current = self._git.get_currently_checked_out_branch_or_none()
    self._storage.save_push_state({
      'original_branch': str(current) if current else None
    })

    subprocess.run(['git', 'checkout', '_xtax'], check=True)
    rebase_result = subprocess.run(['git', 'rebase', f'{remote}/_xtax'])

    if rebase_result.returncode != 0:
      print("Rebase conflict on _xtax. Resolve conflicts, then run:")
      print("  git rebase --continue")
      print("  gx push")
      return

    # Rebase succeeded — push and restore
    self._storage.push_stacks(remote)
    self._storage.clear_push_state()
    if current:
      self._git.checkout(current)
    print(f"Pushed stacks to {remote}")

  def cmd_pull(self, args: List[str]) -> None:
    remote = 'origin'
    for arg in args:
      if arg.startswith('--remote='):
        remote = arg[len('--remote='):]

    result = self._storage.fetch_and_fast_forward(remote)
    subprocess.run(['git', 'remote', 'prune', remote], capture_output=True)
    if result == 'created':
      print(f"Created local stacks from {remote}")
    elif result == 'updated':
      print(f"Pulled stacks from {remote}")
    elif result == 'ahead':
      print(f"Local stacks are ahead of {remote} (nothing to pull)")
    elif result == 'diverged':
      self._resolve_xtax_divergence()
      print(f"Merged diverged stacks from {remote}")
    else:
      print(f"Stacks already up to date with {remote}")

    self._link_unlinked_mrs()

  def _cmd_view_all(self, args: List[str]) -> None:
    while True:
      current_stack = None
      try:
        current = self._git.get_currently_checked_out_branch_or_none()
        if current:
          current_stack = self._storage.find_stack_for_branch(current)
      except Exception:
        pass

      stacks = self._storage.list_stacks()
      if not stacks:
        print(fmt("<yellow>No stacks defined. Use `git xtax init <stack> <branch>` to create one.</yellow>"))
        return

      if not sys.stdin.isatty():
        self._print_stack_list(stacks, current_stack)
        return

      action, name = self._interactive_stack_select(stacks, current_stack)

      if action == 'select':
        result = self.cmd_view([], stack_name=name)
        if result == 'back':
          continue  # Go back to stack list
        return
      elif action == 'delete':
        confirm = input(rl_safe(f"Delete stack {bold(name)}? [y/N] ")).strip()
        if confirm.lower() in ('y', 'yes'):
          try:
            self.cmd_delete([name])
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
        # Loop back to re-render
      elif action == 'rename':
        new_name = input(rl_safe(f"Rename stack {bold(name)} to: ")).strip()
        if new_name:
          try:
            self.cmd_rename([name, new_name])
          except XtaxException as e:
            print(colored(f"Error: {e}", AnsiEscapeCodes.RED))
        # Loop back to re-render

  def _xtax_ahead_behind_str(self) -> str:
    """Get ahead/behind string for _xtax branch."""
    try:
      remote_check = self._git._popen_git(
        "rev-parse", "--verify", "origin/_xtax", allow_non_zero=True)
      if remote_check.exit_code == 0:
        ahead_behind = self._git._popen_git(
          "rev-list", "--count", "--left-right",
          "origin/_xtax..._xtax", allow_non_zero=True)
        if ahead_behind.exit_code == 0:
          parts = ahead_behind.stdout.strip().split('\t')
          if len(parts) == 2:
            behind, ahead = parts
            ahead_str = colored(f"{ahead}↑", AnsiEscapeCodes.GREEN) if ahead != "0" else dim(f"{ahead}↑")
            behind_str = colored(f"{behind}↓", AnsiEscapeCodes.RED) if behind != "0" else dim(f"{behind}↓")
            return f" {dim('(')}{behind_str} {ahead_str}{dim(')')}"
      else:
        # No remote — count all local commits as ahead
        count = self._git._popen_git(
          "rev-list", "--count", "_xtax", allow_non_zero=True)
        if count.exit_code == 0:
          ahead = count.stdout.strip()
          ahead_str = colored(f"{ahead}↑", AnsiEscapeCodes.GREEN) if ahead != "0" else dim(f"{ahead}↑")
          behind_str = dim("0↓")
          return f" {dim('(')}{behind_str} {ahead_str}{dim(')')}"
    except Exception as e:
      debug(f"Failed to get ahead/behind for _xtax: {e}")
    return ""

  def _print_stack_list(self, stacks: List[str],
                         current_stack: Optional[str]) -> None:
    """Print static stack list (non-interactive)."""
    roots: Dict[str, List[str]] = {}
    for s in stacks:
      content = self._storage.read_stack_definition(s)
      if content:
        state = StackStorage.parse_definition(content)
        root = str(state.root)
      else:
        root = "?"
      roots.setdefault(root, []).append(s)

    print()
    print(f"{dim('_xtax')}{self._xtax_ahead_behind_str()}")

    for root, stack_names in roots.items():
      print()
      print(f"  {dim(root)}")
      for s in stack_names:
        if s == current_stack:
          pointer = "❯ "
          node = colored("◉", AnsiEscapeCodes.GREEN)
          name_str = colored(s, AnsiEscapeCodes.GREEN)
        else:
          pointer = "  "
          node = dim("○")
          name_str = dim(s)
        print(f"  {pointer}{node} {name_str}")

  def _interactive_stack_select(self, stacks: List[str],
                                 current_stack: Optional[str]
                                 ) -> Tuple[str, Optional[str]]:
    """Arrow-key selector for stacks, grouped by root branch.
    Returns (action, stack_name) where action is 'select' or 'delete'."""
    roots: Dict[str, List[str]] = {}
    for s in stacks:
      content = self._storage.read_stack_definition(s)
      if content:
        state = StackStorage.parse_definition(content)
        root = str(state.root)
      else:
        root = "?"
      roots.setdefault(root, []).append(s)

    entries: List[Tuple[Optional[str], Optional[str]]] = []
    entries.append(('__separator__', None))
    entries.append(('__xtax_header__', None))
    for root, stack_names in roots.items():
      entries.append(('__separator__', None))
      entries.append((root, None))
      for s in stack_names:
        entries.append((None, s))

    selectable = [i for i, (_, name) in enumerate(entries) if name is not None]
    if not selectable:
      raise XtaxException("No stacks available")

    cursor = 0
    if current_stack:
      for idx, sel_i in enumerate(selectable):
        if entries[sel_i][1] == current_stack:
          cursor = idx
          break

    xtax_info = self._xtax_ahead_behind_str()

    def render(cursor_idx: int) -> str:
      highlighted = entries[selectable[cursor_idx]][1]
      out = []
      for root_name, stack_name in entries:
        if root_name == '__xtax_header__':
          out.append(f"{dim('_xtax')}{xtax_info}")
        elif root_name == '__separator__':
          out.append("")
        elif root_name is not None:
          out.append(f"  {dim(root_name)}")
        else:
          is_active = stack_name == current_stack
          if stack_name == highlighted:
            pointer = "❯ "
            node = colored("◉", AnsiEscapeCodes.GREEN) if is_active else "◉"
            name_str = colored(bold(stack_name), AnsiEscapeCodes.GREEN) if is_active else bold(stack_name)
          elif is_active:
            pointer = "  "
            node = colored("○", AnsiEscapeCodes.GREEN)
            name_str = colored(stack_name, AnsiEscapeCodes.GREEN)
          else:
            pointer = "  "
            node = dim("○")
            name_str = dim(stack_name)
          out.append(f"  {pointer}{node} {name_str}")
      out.append("")
      out.append(f"r{dim(': rename')}  d{dim(': delete')}")
      return '\n'.join(out)

    num_lines = len(entries) + 2
    sys.stdout.write('\x1b[?25l')

    def draw(cursor_idx: int) -> None:
      sys.stdout.write('\r')
      sys.stdout.write('\x1b[J')
      sys.stdout.write(render(cursor_idx))
      sys.stdout.flush()

    def move_to_top() -> None:
      if num_lines > 1:
        sys.stdout.write(f"\x1b[{num_lines - 1}A")

    draw(cursor)

    try:
      while True:
        key = self._read_key()
        if key == 'up' and cursor > 0:
          cursor -= 1
        elif key == 'down' and cursor < len(selectable) - 1:
          cursor += 1
        elif key == 'enter':
          selected = entries[selectable[cursor]][1]
          self._exit_interactive(num_lines)
          return ('select', selected)
        elif key in ('ctrl-c', 'escape'):
          self._exit_interactive(num_lines)
          raise InteractionStopped()
        elif key == 'delete':
          selected = entries[selectable[cursor]][1]
          self._exit_interactive(num_lines)
          return ('delete', selected)
        elif key == 'rename':
          selected = entries[selectable[cursor]][1]
          self._exit_interactive(num_lines)
          return ('rename', selected)
        else:
          continue

        move_to_top()
        draw(cursor)
    except Exception:
      sys.stdout.write('\x1b[?25h')
      sys.stdout.flush()
      raise

  def cmd_switch(self, args: List[str]) -> None:
    if not args:
      stacks = self._storage.list_stacks()
      if not stacks:
        raise XtaxException("<yellow>No stacks defined. Use `git xtax init <stack> <branch>` to create one.</yellow>")
      current_stack = None
      try:
        current = self._git.get_currently_checked_out_branch_or_none()
        if current:
          current_stack = self._storage.find_stack_for_branch(current)
      except Exception:
        pass
      action, name = self._interactive_stack_select(stacks, current_stack)
      if action != 'select':
        return
    else:
      name = args[0]
    stacks = self._storage.list_stacks()
    if name not in stacks:
      raise XtaxException(
        f"Stack {bold(name)} not found. Available stacks: {', '.join(stacks) or '(none)'}")

    content = self._storage.read_stack_definition(name)
    if content is None:
      raise XtaxException(f"Stack {bold(name)} not found")
    state = StackStorage.parse_definition(content)

    # Checkout first branch in the stack
    target = None
    for b in state.managed_branches:
      if self._branch_exists_anywhere(b):
        target = b
        break
    if target:
      self._git.checkout(target)
      print(f"Switched to stack {bold(name)}, checked out {bold(target)}")
    else:
      raise XtaxException(f"Stack {bold(name)} has no existing branches")

  def cmd_edit(self, args: List[str]) -> None:
    self._fetch_stacks()
    name, state = self._resolve_current_stack()
    editor = os.environ.get('XTAX_EDITOR') or os.environ.get('VISUAL') or os.environ.get('EDITOR') or 'vi'

    import tempfile
    content = StackStorage.render_definition(state)
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xtax', delete=False) as f:
      f.write(content)
      tmp_path = f.name

    try:
      exit_code = subprocess.call(shlex.split(editor) + [tmp_path])
      if exit_code != 0:
        raise XtaxException(f"Editor exited with code {exit_code}")
      with open(tmp_path, 'r') as f:
        new_content = f.read()
      StackStorage.parse_definition(new_content)
      self._storage.write_stack_definition(name, new_content)
      print(f"Updated stack {bold(name)}")
    finally:
      os.unlink(tmp_path)

  def cmd_rename(self, args: List[str]) -> None:
    self._fetch_stacks()
    if len(args) < 2:
      raise XtaxException("Usage: git xtax rename <old-name> <new-name>")

    old_name = args[0]
    new_name = args[1]

    content = self._storage.read_stack_definition(old_name)
    if content is None:
      raise XtaxException(f"Stack {bold(old_name)} does not exist")

    if self._storage.read_stack_definition(new_name) is not None:
      raise XtaxException(f"Stack {bold(new_name)} already exists")

    self._storage.write_stack_definition(new_name, content)
    self._storage.delete_stack(old_name)
    print(f"Renamed stack {bold(old_name)} to {bold(new_name)}")

  def cmd_rename_branch(self, args: List[str]) -> None:
    self._fetch_stacks()
    if len(args) < 2:
      raise XtaxException("Usage: git xtax rename-branch <old-branch> <new-branch>")

    old_name = LocalBranchShortName.of(args[0])
    new_name = LocalBranchShortName.of(args[1])

    # Find which stack this branch belongs to
    stack_name = self._storage.find_stack_for_branch(old_name)
    if not stack_name:
      raise XtaxException(f"Branch {bold(old_name)} is not in any stack")

    # Check new name isn't already in a stack
    existing = self._storage.find_stack_for_branch(new_name)
    if existing:
      raise XtaxException(f"Branch {bold(new_name)} is already in stack {bold(existing)}")

    content = self._storage.read_stack_definition(stack_name)
    state = StackStorage.parse_definition(content)

    # Rename the git branch
    result = subprocess.run(['git', 'branch', '-m', str(old_name), str(new_name)],
                            capture_output=True, text=True)
    if result.returncode != 0:
      raise XtaxException(f"Failed to rename git branch: {result.stderr.strip()}")

    # Update stack state
    idx = state.managed_branches.index(old_name)
    state.managed_branches[idx] = new_name

    if old_name in state.up_branch_for:
      parent = state.up_branch_for.pop(old_name)
      state.up_branch_for[new_name] = parent
      if parent in state.down_branches_for:
        children = state.down_branches_for[parent]
        children[children.index(old_name)] = new_name

    if old_name in state.down_branches_for:
      children = state.down_branches_for.pop(old_name)
      state.down_branches_for[new_name] = children
      for child in children:
        state.up_branch_for[child] = new_name

    if old_name in state.annotations:
      state.annotations[new_name] = state.annotations.pop(old_name)

    self._save_state(stack_name, state)
    print(f"Renamed branch {bold(old_name)} to {bold(new_name)} in stack {bold(stack_name)}")


# --- Dispatch ---

COMMANDS = {
  'init': 'cmd_init',
  'stack': 'cmd_stack',
  'tuck': 'cmd_tuck',
  'slideout': 'cmd_slideout',
  'delete': 'cmd_delete',
  'rename': 'cmd_rename',
  'rename-branch': 'cmd_rename_branch',
  'view': 'cmd_view',
  'v': 'cmd_view',
  'up': 'cmd_up',
  'u': 'cmd_up',
  'down': 'cmd_down',
  'd': 'cmd_down',
  'top': 'cmd_top',
  't': 'cmd_top',
  'bottom': 'cmd_bottom',
  'b': 'cmd_bottom',
  'list': 'cmd_list',
  'l': 'cmd_list',
  'sync': 'cmd_sync',
  's': 'cmd_sync',
  'push': 'cmd_push',
  'pull': 'cmd_pull',
  'switch': 'cmd_switch',
  'edit': 'cmd_edit',
}


def _enable_ansi_on_windows() -> None:
  """Enable ANSI escape sequence processing on Windows 10+."""
  if sys.platform != 'win32':
    return
  try:
    import ctypes
    kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
    # STD_OUTPUT_HANDLE = -11, ENABLE_VIRTUAL_TERMINAL_PROCESSING = 0x0004
    handle = kernel32.GetStdHandle(-11)
    mode = ctypes.c_ulong()
    kernel32.GetConsoleMode(handle, ctypes.byref(mode))
    kernel32.SetConsoleMode(handle, mode.value | 0x0004)
  except Exception:
    pass


def main() -> None:
  _enable_ansi_on_windows()
  args = sys.argv[1:]

  if not args or args[0] in ('-h', '--help', 'help', 'h'):
    print(USAGE)
    sys.exit(ExitCode.SUCCESS)

  if args[0] in ('--version', 'version'):
    print(f"git-xtax version {__version__}")
    sys.exit(ExitCode.SUCCESS)

  if args[0] == 'completions':
    shell = args[1] if len(args) > 1 else None
    if shell == 'zsh':
      print(ZSH_COMPLETION.strip())
    elif shell == 'bash':
      print("# Bash completions not yet supported. Contributions welcome!", file=sys.stderr)
      sys.exit(ExitCode.ARGUMENT_ERROR)
    else:
      print("Usage: git xtax completions <zsh|bash>", file=sys.stderr)
      sys.exit(ExitCode.ARGUMENT_ERROR)
    sys.exit(ExitCode.SUCCESS)

  debug_mode = False
  verbose_mode = False
  remaining_args = []
  for arg in args:
    if arg == '--debug':
      debug_mode = True
      utils.debug_mode = True
    elif arg in ('--verbose', '-v'):
      verbose_mode = True
      utils.verbose_mode = True
    else:
      remaining_args.append(arg)
  args = remaining_args

  if not args:
    print(USAGE)
    sys.exit(ExitCode.SUCCESS)

  cmd = args[0]
  cmd_args = args[1:]

  try:
    git = GitContext()
    storage = StackStorage(git)
    client = XtaxClient(git, storage)

    try:
      n = int(cmd)
      client.cmd_go_n(n)
      sys.exit(ExitCode.SUCCESS)
    except ValueError:
      pass

    if cmd in COMMANDS:
      method = getattr(client, COMMANDS[cmd])
      method(cmd_args)
    else:
      print(f"Unknown command: {cmd}\n", file=sys.stderr)
      print(USAGE, file=sys.stderr)
      sys.exit(ExitCode.ARGUMENT_ERROR)

  except XtaxException as e:
    print(colored(f"Error: {e}", AnsiEscapeCodes.RED), file=sys.stderr)
    sys.exit(ExitCode.XTAX_EXCEPTION)
  except UnderlyingGitException as e:
    print(colored(f"Git error: {e}", AnsiEscapeCodes.RED), file=sys.stderr)
    sys.exit(ExitCode.XTAX_EXCEPTION)
  except InteractionStopped:
    sys.exit(ExitCode.SUCCESS)
  except KeyboardInterrupt:
    # Suppress noisy threading shutdown traceback on Python 3.14+
    os._exit(ExitCode.KEYBOARD_INTERRUPT)
  except EOFError:
    sys.exit(ExitCode.END_OF_FILE_SIGNAL)
