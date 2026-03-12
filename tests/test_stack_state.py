"""Tests for orphan-branch-based stack storage and YAML definition parsing."""
import os
import subprocess
import pytest

from git_xtax.git_operations import LocalBranchShortName
from git_xtax.stack_state import StackStorage, XtaxState
from git_xtax.exceptions import XtaxException


def run_git(*args, cwd=None):
    return subprocess.run(
        ["git"] + list(args),
        cwd=cwd, capture_output=True, text=True
    )


# --- Parse/render tests ---

class TestParseDefinition:
    def test_simple_linear_stack(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    children:\n"
            "      - name: feature-b\n"
        )
        state = StackStorage.parse_definition(content)
        assert str(state.root) == "develop"
        assert [str(b) for b in state.managed_branches] == ["feature-a", "feature-b"]
        assert str(state.up_branch_for[LocalBranchShortName.of("feature-a")]) == "develop"
        assert str(state.up_branch_for[LocalBranchShortName.of("feature-b")]) == "feature-a"

    def test_branching_stack(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    children:\n"
            "      - name: feature-b\n"
            "      - name: feature-c\n"
        )
        state = StackStorage.parse_definition(content)
        assert str(state.root) == "develop"
        assert len(state.managed_branches) == 3
        children_of_a = state.down_branches_for[LocalBranchShortName.of("feature-a")]
        assert [str(b) for b in children_of_a] == ["feature-b", "feature-c"]

    def test_annotations_preserved(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    annotation: MR !123\n"
        )
        state = StackStorage.parse_definition(content)
        assert state.annotations[LocalBranchShortName.of("feature-a")].text_without_qualifiers == "MR !123"

    def test_qualifiers_preserved(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    qualifiers: rebase=no\n"
        )
        state = StackStorage.parse_definition(content)
        anno = state.annotations[LocalBranchShortName.of("feature-a")]
        assert anno.qualifiers.rebase is False

    def test_duplicate_branch_raises(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "  - name: feature-a\n"
        )
        with pytest.raises(XtaxException, match="appears more than once"):
            StackStorage.parse_definition(content)

    def test_missing_root_raises(self):
        content = "branches:\n  - name: feature-a\n"
        with pytest.raises(XtaxException, match="missing 'root'"):
            StackStorage.parse_definition(content)

    def test_missing_name_raises(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - annotation: something\n"
        )
        with pytest.raises(XtaxException, match="missing 'name'"):
            StackStorage.parse_definition(content)

    def test_root_only(self):
        content = "root: develop\nbranches: []\n"
        state = StackStorage.parse_definition(content)
        assert str(state.root) == "develop"
        assert len(state.managed_branches) == 0

    def test_root_only_no_branches_key(self):
        content = "root: develop\n"
        state = StackStorage.parse_definition(content)
        assert str(state.root) == "develop"
        assert len(state.managed_branches) == 0


class TestRenderDefinition:
    def test_roundtrip(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    children:\n"
            "      - name: feature-b\n"
        )
        state = StackStorage.parse_definition(content)
        rendered = StackStorage.render_definition(state)
        state2 = StackStorage.parse_definition(rendered)
        assert str(state2.root) == "develop"
        assert [str(b) for b in state2.managed_branches] == ["feature-a", "feature-b"]

    def test_roundtrip_with_annotation(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    annotation: MR !123\n"
        )
        state = StackStorage.parse_definition(content)
        rendered = StackStorage.render_definition(state)
        state2 = StackStorage.parse_definition(rendered)
        assert state2.annotations[LocalBranchShortName.of("feature-a")].text_without_qualifiers == "MR !123"

    def test_roundtrip_branching(self):
        content = (
            "root: develop\n"
            "branches:\n"
            "  - name: feature-a\n"
            "    children:\n"
            "      - name: feature-b\n"
            "      - name: feature-c\n"
        )
        state = StackStorage.parse_definition(content)
        rendered = StackStorage.render_definition(state)
        state2 = StackStorage.parse_definition(rendered)
        assert str(state2.root) == "develop"
        children_of_a = state2.down_branches_for[LocalBranchShortName.of("feature-a")]
        assert [str(b) for b in children_of_a] == ["feature-b", "feature-c"]


# --- Storage tests (require a real git repo) ---

class TestStackStorage:
    @pytest.fixture
    def git_repo(self, tmp_path):
        repo = tmp_path / "repo"
        repo.mkdir()
        run_git("init", cwd=repo)
        run_git("config", "user.name", "Test", cwd=repo)
        run_git("config", "user.email", "test@test.com", cwd=repo)
        run_git("commit", "--allow-empty", "-m", "init", cwd=repo)
        old_cwd = os.getcwd()
        os.chdir(repo)
        yield repo
        os.chdir(old_cwd)

    @pytest.fixture
    def storage(self, git_repo):
        from git_xtax.git_operations import GitContext
        return StackStorage(GitContext())

    def test_write_and_read_stack(self, storage):
        content = "root: develop\nbranches:\n  - name: feature-a\n"
        storage.write_stack_definition("my-stack", content)
        result = storage.read_stack_definition("my-stack")
        assert result == content

    def test_read_nonexistent_stack(self, storage):
        assert storage.read_stack_definition("nonexistent") is None

    def test_list_stacks(self, storage):
        storage.write_stack_definition("alpha", "root: develop\nbranches:\n  - name: a\n")
        storage.write_stack_definition("beta", "root: develop\nbranches:\n  - name: b\n")
        assert storage.list_stacks() == ["alpha", "beta"]

    def test_delete_stack(self, storage):
        storage.write_stack_definition("to-delete", "root: develop\nbranches:\n  - name: x\n")
        assert "to-delete" in storage.list_stacks()
        storage.delete_stack("to-delete")
        assert "to-delete" not in storage.list_stacks()

    def test_sync_state(self, storage):
        assert storage.load_sync_state() is None
        storage.save_sync_state({"key": "value"})
        assert storage.load_sync_state() == {"key": "value"}
        storage.clear_sync_state()
        assert storage.load_sync_state() is None

    def test_orphan_branch_created(self, storage, git_repo):
        storage.write_stack_definition("test", "root: develop\nbranches:\n  - name: x\n")
        result = run_git("rev-parse", "--verify", "refs/heads/_xtax", cwd=git_repo)
        assert result.returncode == 0

    def test_orphan_branch_has_commits(self, storage, git_repo):
        storage.write_stack_definition("s1", "root: develop\nbranches:\n  - name: a\n")
        storage.write_stack_definition("s2", "root: develop\nbranches:\n  - name: b\n")
        result = run_git("log", "--oneline", "_xtax", cwd=git_repo)
        assert result.returncode == 0
        lines = [l for l in result.stdout.strip().splitlines() if l]
        assert len(lines) == 2

    def test_find_stack_for_branch(self, storage):
        storage.write_stack_definition("stack-a",
            "root: develop\nbranches:\n  - name: feature-a\n    children:\n      - name: feature-b\n")
        storage.write_stack_definition("stack-b",
            "root: develop\nbranches:\n  - name: feature-x\n")

        assert storage.find_stack_for_branch(LocalBranchShortName.of("feature-a")) == "stack-a"
        assert storage.find_stack_for_branch(LocalBranchShortName.of("feature-b")) == "stack-a"
        assert storage.find_stack_for_branch(LocalBranchShortName.of("feature-x")) == "stack-b"
        assert storage.find_stack_for_branch(LocalBranchShortName.of("nonexistent")) is None

    def test_find_stack_does_not_match_root(self, storage):
        storage.write_stack_definition("my-stack",
            "root: develop\nbranches:\n  - name: feature-a\n")
        assert storage.find_stack_for_branch(LocalBranchShortName.of("develop")) is None
