import json
import os
import tempfile
from typing import Any, Dict, List, Optional

import yaml

from .annotation import Annotation, Qualifiers
from .exceptions import XtaxException
from .git_operations import GitContext, LocalBranchShortName
from .utils import bold, debug, popen_cmd, run_cmd


class XtaxState:
  def __init__(self) -> None:
    self.root: Optional[LocalBranchShortName] = None
    self.managed_branches: List[LocalBranchShortName] = []
    self.up_branch_for: Dict[LocalBranchShortName, LocalBranchShortName] = {}
    self.down_branches_for: Dict[LocalBranchShortName, List[LocalBranchShortName]] = {}
    self.annotations: Dict[LocalBranchShortName, Annotation] = {}


class StackStorage:
  """Orphan-branch-based stack storage.

  Stack definitions are YAML files on an orphan branch (_xtax):
    stacks/<name>.yml — branch tree with metadata

  Stack is resolved from the currently checked out branch.
  """

  BRANCH = '_xtax'
  STACKS_DIR = 'stacks'

  def __init__(self, git: GitContext) -> None:
    self._git = git
    self._root_dir = git.get_main_worktree_root_dir()

  def _git_dir(self) -> str:
    return self._git.get_main_worktree_git_dir()

  def _sync_state_path(self) -> str:
    return os.path.join(self._git_dir(), 'xtax-sync-state')

  def _stack_file_path(self, name: str) -> str:
    return f'{self.STACKS_DIR}/{name}.yml'

  # --- Orphan branch plumbing ---

  def _branch_exists(self) -> bool:
    exit_code, _, _ = popen_cmd('git', 'rev-parse', '--verify', f'refs/heads/{self.BRANCH}')
    return exit_code == 0

  def _read_file(self, path: str) -> Optional[str]:
    if not self._branch_exists():
      return None
    exit_code, stdout, _ = popen_cmd('git', 'show', f'{self.BRANCH}:{path}')
    if exit_code != 0:
      return None
    return stdout

  def _write_file(self, path: str, content: str, message: str) -> None:
    exit_code, stdout, stderr = popen_cmd('git', 'hash-object', '-w', '--stdin', input=content)
    if exit_code != 0:
      raise XtaxException(f"Failed to hash content: {stderr}")
    blob_hash = stdout.strip()

    tmp_index = tempfile.mktemp(prefix='xtax-index-')
    try:
      env = {**os.environ, 'GIT_INDEX_FILE': tmp_index}

      if self._branch_exists():
        popen_cmd('git', 'read-tree', self.BRANCH, env=env)

      popen_cmd('git', 'update-index', '--add', '--cacheinfo', '100644', blob_hash, path, env=env)

      exit_code, stdout, stderr = popen_cmd('git', 'write-tree', env=env)
      if exit_code != 0:
        raise XtaxException(f"Failed to write tree: {stderr}")
      tree_hash = stdout.strip()

      if self._branch_exists():
        _, parent_out, _ = popen_cmd('git', 'rev-parse', self.BRANCH)
        _, stdout, stderr = popen_cmd(
          'git', 'commit-tree', tree_hash, '-p', parent_out.strip(), '-m', message)
      else:
        _, stdout, stderr = popen_cmd('git', 'commit-tree', tree_hash, '-m', message)
      commit_hash = stdout.strip()

      popen_cmd('git', 'update-ref', f'refs/heads/{self.BRANCH}', commit_hash)
    finally:
      if os.path.exists(tmp_index):
        os.unlink(tmp_index)

  def _delete_file(self, path: str, message: str) -> None:
    if not self._branch_exists():
      return

    tmp_index = tempfile.mktemp(prefix='xtax-index-')
    try:
      env = {**os.environ, 'GIT_INDEX_FILE': tmp_index}
      exit_code, _, stderr = popen_cmd('git', 'read-tree', self.BRANCH, env=env,
                                        cwd=self._root_dir)
      if exit_code != 0:
        raise XtaxException(f"Failed to read tree: {stderr}")

      exit_code, _, stderr = popen_cmd('git', 'update-index', '--force-remove', path, env=env,
                                        cwd=self._root_dir)
      if exit_code != 0:
        raise XtaxException(f"Failed to remove {path} from index: {stderr}")

      exit_code, stdout, stderr = popen_cmd('git', 'write-tree', env=env,
                                             cwd=self._root_dir)
      if exit_code != 0:
        raise XtaxException(f"Failed to write tree: {stderr}")
      tree_hash = stdout.strip()

      _, parent_out, _ = popen_cmd('git', 'rev-parse', self.BRANCH,
                                    cwd=self._root_dir)
      exit_code, stdout, stderr = popen_cmd(
        'git', 'commit-tree', tree_hash, '-p', parent_out.strip(), '-m', message,
        cwd=self._root_dir)
      if exit_code != 0:
        raise XtaxException(f"Failed to commit tree: {stderr}")
      commit_hash = stdout.strip()

      popen_cmd('git', 'update-ref', f'refs/heads/{self.BRANCH}', commit_hash,
                cwd=self._root_dir)
    finally:
      if os.path.exists(tmp_index):
        os.unlink(tmp_index)

  # --- Stack operations ---

  def read_stack_definition(self, name: str) -> Optional[str]:
    return self._read_file(self._stack_file_path(name))

  def write_stack_definition(self, name: str, content: str) -> None:
    self._write_file(self._stack_file_path(name), content, f'xtax: update stack {name}')

  def delete_stack(self, name: str) -> None:
    self._delete_file(self._stack_file_path(name), f'xtax: delete stack {name}')

  def list_stacks(self) -> List[str]:
    if not self._branch_exists():
      return []
    exit_code, stdout, _ = popen_cmd(
      'git', 'ls-tree', '--name-only', f'{self.BRANCH}', f'{self.STACKS_DIR}/',
      cwd=self._root_dir)
    if exit_code != 0 or not stdout.strip():
      return []
    names = []
    for line in stdout.strip().splitlines():
      filename = line.split('/')[-1] if '/' in line else line
      if filename.endswith('.yml'):
        names.append(filename[:-4])
    return sorted(names)

  # --- Sync state ---

  def save_sync_state(self, state: dict) -> None:
    path = self._sync_state_path()
    with open(path, 'w') as f:
      json.dump(state, f)

  def load_sync_state(self) -> Optional[dict]:
    path = self._sync_state_path()
    if not os.path.exists(path):
      return None
    with open(path, 'r') as f:
      return json.load(f)

  def clear_sync_state(self) -> None:
    path = self._sync_state_path()
    if os.path.exists(path):
      os.remove(path)

  # --- Push state (for rebase recovery) ---

  def _push_state_path(self) -> str:
    return os.path.join(self._git_dir(), 'xtax-push-state')

  def save_push_state(self, state: dict) -> None:
    path = self._push_state_path()
    with open(path, 'w') as f:
      json.dump(state, f)

  def load_push_state(self) -> Optional[dict]:
    path = self._push_state_path()
    if not os.path.exists(path):
      return None
    with open(path, 'r') as f:
      return json.load(f)

  def clear_push_state(self) -> None:
    path = self._push_state_path()
    if os.path.exists(path):
      os.remove(path)

  # --- Push/fetch ---

  def push_stacks(self, remote: str = 'origin') -> None:
    exit_code = run_cmd('git', 'push', remote, self.BRANCH)
    if exit_code != 0:
      raise XtaxException(f"Failed to push stacks to {remote}")

  def fetch_and_fast_forward(self, remote: str = 'origin') -> Optional[str]:
    """Fetch remote _xtax and fast-forward local ref if behind.

    Returns: None if no changes, 'created' if new, 'updated' if fast-forwarded,
             'ahead' if local is ahead of remote.
    """
    exit_code, stdout, _ = popen_cmd(
      'git', 'ls-remote', remote, f'refs/heads/{self.BRANCH}')
    if exit_code != 0 or not stdout.strip():
      return None

    popen_cmd('git', 'fetch', remote, self.BRANCH)
    remote_ref = f'{remote}/{self.BRANCH}'

    if not self._branch_exists():
      _, stdout, _ = popen_cmd('git', 'rev-parse', remote_ref)
      popen_cmd('git', 'update-ref', f'refs/heads/{self.BRANCH}', stdout.strip())
      return 'created'

    _, local_out, _ = popen_cmd('git', 'rev-parse', self.BRANCH)
    _, remote_out, _ = popen_cmd('git', 'rev-parse', remote_ref)
    if local_out.strip() == remote_out.strip():
      return None

    exit_code, _, _ = popen_cmd('git', 'merge-base', '--is-ancestor', self.BRANCH, remote_ref)
    if exit_code == 0:
      popen_cmd('git', 'update-ref', f'refs/heads/{self.BRANCH}', remote_out.strip())
      return 'updated'

    exit_code, _, _ = popen_cmd('git', 'merge-base', '--is-ancestor', remote_ref, self.BRANCH)
    if exit_code == 0:
      return 'ahead'

    return 'diverged'

  def fetch_stacks(self, remote: str = 'origin') -> None:
    exit_code = run_cmd('git', 'fetch', remote, f'{self.BRANCH}:{self.BRANCH}')
    if exit_code != 0:
      raise XtaxException(f"Failed to fetch stacks from {remote}")

  def list_remote_stacks(self, remote: str = 'origin') -> List[str]:
    exit_code, stdout, _ = popen_cmd(
      'git', 'ls-remote', remote, f'refs/heads/{self.BRANCH}')
    if exit_code != 0 or not stdout.strip():
      return []
    popen_cmd('git', 'fetch', remote, f'{self.BRANCH}')
    exit_code, stdout, _ = popen_cmd(
      'git', 'ls-tree', '--name-only', f'{remote}/{self.BRANCH}', f'{self.STACKS_DIR}/',
      cwd=self._root_dir)
    if exit_code != 0 or not stdout.strip():
      return []
    names = []
    for line in stdout.strip().splitlines():
      filename = line.split('/')[-1] if '/' in line else line
      if filename.endswith('.yml'):
        names.append(filename[:-4])
    return sorted(names)

  # --- Cross-stack checks ---

  def find_stack_for_branch(self, branch: LocalBranchShortName) -> Optional[str]:
    """Find which stack a branch belongs to as a managed branch."""
    for stack_name in self.list_stacks():
      content = self.read_stack_definition(stack_name)
      if content is None:
        continue
      state = StackStorage.parse_definition(content)
      if branch in state.managed_branches:
        return stack_name
    return None

  # --- Parse/render ---

  @staticmethod
  def _branch_to_dict(branch: LocalBranchShortName, state: XtaxState) -> Dict[str, Any]:
    entry: Dict[str, Any] = {'name': str(branch)}
    annotation = state.annotations.get(branch)
    if annotation:
      if annotation.text_without_qualifiers:
        entry['annotation'] = annotation.text_without_qualifiers
      if annotation.qualifiers.is_non_default():
        entry['qualifiers'] = str(annotation.qualifiers)
    children = state.down_branches_for.get(branch, [])
    if children:
      entry['children'] = [StackStorage._branch_to_dict(c, state) for c in children]
    return entry

  @staticmethod
  def _dict_to_branches(branch_list: List[Dict[str, Any]], parent: LocalBranchShortName,
                         state: XtaxState) -> None:
    for entry in branch_list:
      name = entry.get('name')
      if not name:
        raise XtaxException("Branch entry missing 'name' field")
      branch = LocalBranchShortName.of(name)

      if branch in state.managed_branches:
        raise XtaxException(f"Branch {bold(branch)} appears more than once")
      if branch == state.root:
        raise XtaxException(f"Branch {bold(branch)} cannot be both root and managed")

      state.managed_branches.append(branch)
      state.up_branch_for[branch] = parent
      if parent in state.down_branches_for:
        state.down_branches_for[parent].append(branch)
      else:
        state.down_branches_for[parent] = [branch]

      annotation_text = entry.get('annotation', '')
      qualifiers_text = entry.get('qualifiers', '')
      if annotation_text or qualifiers_text:
        full_text = f"{annotation_text} {qualifiers_text}".strip()
        state.annotations[branch] = Annotation.parse(full_text)

      children = entry.get('children')
      if children:
        StackStorage._dict_to_branches(children, branch, state)

  @staticmethod
  def parse_definition(content: str) -> XtaxState:
    """Parse a YAML stack definition into XtaxState.

    Format:
        root: develop
        branches:
          - name: feature-a
            annotation: "MR !123"
            children:
              - name: feature-b
    """
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
      raise XtaxException("Invalid stack definition: expected a YAML mapping")

    root_name = data.get('root')
    if not root_name:
      raise XtaxException("Stack definition missing 'root' field")

    state = XtaxState()
    state.root = LocalBranchShortName.of(str(root_name))

    branches = data.get('branches')
    if branches:
      StackStorage._dict_to_branches(branches, state.root, state)

    return state

  @staticmethod
  def render_definition(state: XtaxState) -> str:
    """Render XtaxState as a YAML string."""
    root_children = state.down_branches_for.get(state.root, [])
    data: Dict[str, Any] = {
      'root': str(state.root),
      'branches': [StackStorage._branch_to_dict(c, state) for c in root_children],
    }
    return yaml.dump(data, default_flow_style=False, sort_keys=False)
