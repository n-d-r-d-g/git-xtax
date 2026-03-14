"""Integration tests for git-xtax CLI commands."""
import os
import subprocess
import pytest


def run_git(*args, cwd=None):
    result = subprocess.run(
        ["git"] + list(args),
        cwd=cwd, capture_output=True, text=True
    )
    return result


def run_xtax(*args, cwd=None, stdin=None, extra_env=None):
    """Run git-xtax CLI via Python module."""
    import sys
    env = {**os.environ, "PYTHONPATH": os.path.dirname(os.path.dirname(__file__))}
    if extra_env:
      env.update(extra_env)
    result = subprocess.run(
        [sys.executable, "-m", "git_xtax"] + list(args),
        cwd=cwd, capture_output=True, text=True, input=stdin,
        env=env,
    )
    return result


@pytest.fixture
def git_repo(tmp_path):
    """Create a temp git repo with develop and two feature branches."""
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git("init", cwd=repo)
    run_git("config", "user.name", "Test", cwd=repo)
    run_git("config", "user.email", "test@test.com", cwd=repo)
    run_git("checkout", "-b", "develop", cwd=repo)
    run_git("commit", "--allow-empty", "-m", "init", cwd=repo)

    # Create feature branches
    run_git("checkout", "-b", "feature-a", cwd=repo)
    run_git("commit", "--allow-empty", "-m", "feature-a work", cwd=repo)

    run_git("checkout", "-b", "feature-b", cwd=repo)
    run_git("commit", "--allow-empty", "-m", "feature-b work", cwd=repo)

    run_git("checkout", "develop", cwd=repo)

    old_cwd = os.getcwd()
    os.chdir(repo)
    yield repo
    os.chdir(old_cwd)


class TestInit:
    def test_init_creates_stack(self, git_repo):
        result = run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        assert result.returncode == 0
        assert "Created stack" in result.stdout

    def test_init_root_defaults_to_current_branch(self, git_repo):
        # On develop, init without --root should use develop as root
        result = run_xtax("init", "my-stack", "feature-a", cwd=git_repo)
        assert result.returncode == 0
        assert "root: develop" in result.stdout

    def test_init_requires_branch(self, git_repo):
        result = run_xtax("init", "my-stack", cwd=git_repo)
        assert result.returncode != 0
        assert "Usage" in result.stderr

    def test_init_duplicate_fails(self, git_repo):
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("init", "my-stack", "feature-b", "--root=develop", cwd=git_repo)
        assert result.returncode != 0
        assert "already exists" in result.stderr

    def test_init_branch_in_another_stack_fails(self, git_repo):
        run_xtax("init", "stack-1", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("init", "stack-2", "feature-a", "--root=develop", cwd=git_repo)
        assert result.returncode != 0
        assert "already in stack" in result.stderr

    def test_init_creates_nonexistent_branch_on_confirm(self, git_repo):
        result = run_xtax("init", "my-stack", "new-branch", "--root=develop", cwd=git_repo, stdin="y\n")
        assert result.returncode == 0
        assert "Created branch" in result.stdout
        assert "Created stack" in result.stdout
        # Verify branch was actually created
        branch = run_git("rev-parse", "--verify", "refs/heads/new-branch", cwd=git_repo)
        assert branch.returncode == 0

    def test_init_aborts_on_decline(self, git_repo):
        result = run_xtax("init", "my-stack", "new-branch", "--root=develop", cwd=git_repo, stdin="n\n")
        assert result.returncode != 0
        assert "Aborted" in result.stderr
        # Verify branch was not created
        branch = run_git("rev-parse", "--verify", "refs/heads/new-branch", cwd=git_repo)
        assert branch.returncode != 0


class TestView:
    def test_view_shows_stack(self, git_repo):
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        run_xtax("stack", "feature-b", cwd=git_repo)

        result = run_xtax("v", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-a" in result.stdout
        assert "feature-b" in result.stdout


class TestStack:
    def test_stack_branch_onto_managed(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("stack", "feature-b", cwd=git_repo)
        assert result.returncode == 0
        assert "Stacked" in result.stdout

    def test_stack_with_onto_flag(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("stack", "feature-b", "--onto=feature-a", cwd=git_repo)
        assert result.returncode == 0
        assert "Stacked" in result.stdout
        assert "stack s" in result.stdout

    def test_stack_duplicate_fails(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("stack", "feature-a", "--onto=feature-a", cwd=git_repo)
        assert result.returncode != 0
        assert "already in stack" in result.stderr

    def test_stack_onto_root_fails(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("stack", "feature-b", "--onto=develop", cwd=git_repo)
        assert result.returncode != 0
        assert "not in any stack" in result.stderr

    def test_stack_on_root_branch_fails(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        # Currently on develop (root)
        result = run_xtax("stack", "feature-b", cwd=git_repo)
        assert result.returncode != 0

    def test_stack_resolves_stack_from_parent(self, git_repo):
        """stack with --onto should find the right stack automatically."""
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        # No need to switch active stack — it resolves from --onto
        result = run_xtax("stack", "feature-b", "--onto=feature-a", cwd=git_repo)
        assert result.returncode == 0

    def test_stack_onto_branch_not_in_stack_fails(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "-b", "untracked-branch", cwd=git_repo)
        result = run_xtax("stack", "feature-b", cwd=git_repo)
        assert result.returncode != 0
        assert "not in any stack" in result.stderr

    def test_stack_cross_stack_branch_fails(self, git_repo):
        """A branch in stack-1 cannot be added to stack-2."""
        run_xtax("init", "stack-1", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "-b", "feature-c", cwd=git_repo)
        run_xtax("init", "stack-2", "feature-c", "--root=develop", cwd=git_repo)
        # Try to add feature-a (in stack-1) to stack-2
        result = run_xtax("stack", "feature-a", "--onto=feature-c", cwd=git_repo)
        assert result.returncode != 0
        assert "already in stack" in result.stderr

    def test_stack_inserts_between_parent_and_child(self, git_repo):
        """Stacking onto a branch that has a child inserts in between."""
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        run_xtax("stack", "feature-b", cwd=git_repo)
        # Now stack feature-c onto feature-a (which already has child feature-b)
        run_git("checkout", "-b", "feature-c", "feature-a", cwd=git_repo)
        run_xtax("stack", "feature-c", "--onto=feature-a", cwd=git_repo)
        # Stack should be: feature-a -> feature-c -> feature-b
        result = run_xtax("v", cwd=git_repo)
        output = result.stdout
        assert output.index("feature-b") < output.index("feature-c") < output.index("feature-a")

    def test_stack_creates_nonexistent_branch_on_confirm(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("stack", "new-branch", cwd=git_repo, stdin="y\n")
        assert result.returncode == 0
        assert "Created branch" in result.stdout
        assert "Stacked" in result.stdout
        # Verify branch was actually created
        branch = run_git("rev-parse", "--verify", "refs/heads/new-branch", cwd=git_repo)
        assert branch.returncode == 0

    def test_stack_aborts_on_decline(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("stack", "new-branch", cwd=git_repo, stdin="n\n")
        assert result.returncode != 0
        assert "Aborted" in result.stderr


class TestSlideout:
    def test_slideout(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        run_xtax("stack", "feature-b", cwd=git_repo)

        result = run_xtax("slideout", "feature-a", cwd=git_repo)
        assert result.returncode == 0

        # Switch to feature-b (still in the stack) to check status
        run_git("checkout", "feature-b", cwd=git_repo)
        status = run_xtax("v", cwd=git_repo)
        assert "feature-b" in status.stdout
        assert "feature-a" not in status.stdout


class TestNavigation:
    def _setup_stack(self, git_repo):
        run_xtax("init", "s", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        run_xtax("stack", "feature-b", cwd=git_repo)

    def test_up(self, git_repo):
        """up goes toward leaf (child)."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("up", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-b" in result.stdout

    def test_up_at_top_fails(self, git_repo):
        """up at leaf should fail — already at top of stack."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-b", cwd=git_repo)
        result = run_xtax("up", cwd=git_repo)
        assert result.returncode != 0
        assert "top of the stack" in result.stderr

    def test_down(self, git_repo):
        """down goes toward root (parent)."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-b", cwd=git_repo)
        result = run_xtax("down", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-a" in result.stdout

    def test_down_at_bottom_fails(self, git_repo):
        """down at first branch (closest to root) should fail."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("down", cwd=git_repo)
        assert result.returncode != 0
        assert "bottom of the stack" in result.stderr

    def test_integer_navigation_up(self, git_repo):
        """Positive integer goes up (toward leaf)."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("1", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-b" in result.stdout

    def test_integer_navigation_down(self, git_repo):
        """Negative integer goes down (toward root)."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-b", cwd=git_repo)
        result = run_xtax("-1", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-a" in result.stdout

    def test_integer_zero_goes_to_leaf(self, git_repo):
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("0", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-b" in result.stdout

    def test_top(self, git_repo):
        """top goes to leaf (deepest branch)."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("top", cwd=git_repo)
        assert result.returncode == 0
        branch = run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=git_repo)
        assert branch.stdout.strip() == "feature-b"

    def test_bottom(self, git_repo):
        """bottom goes to first branch above root."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-b", cwd=git_repo)
        result = run_xtax("bottom", cwd=git_repo)
        assert result.returncode == 0
        branch = run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=git_repo)
        assert branch.stdout.strip() == "feature-a"

    def test_top_already_on_leaf(self, git_repo):
        self._setup_stack(git_repo)
        run_git("checkout", "feature-b", cwd=git_repo)
        result = run_xtax("top", cwd=git_repo)
        assert result.returncode == 0
        branch = run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=git_repo)
        assert branch.stdout.strip() == "feature-b"

    def test_bottom_already_on_first(self, git_repo):
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("bottom", cwd=git_repo)
        assert result.returncode == 0
        branch = run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=git_repo)
        assert branch.stdout.strip() == "feature-a"

    def test_navigation_shows_status(self, git_repo):
        """All navigation commands should show gx status after checkout."""
        self._setup_stack(git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("up", cwd=git_repo)
        assert result.returncode == 0
        assert "Stack:" in result.stdout


class TestDelete:
    def test_delete_stack(self, git_repo):
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("delete", "my-stack", cwd=git_repo)
        assert result.returncode == 0
        assert "Deleted" in result.stdout

    def test_delete_stack_is_removed_from_view_all(self, git_repo):
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("delete", "my-stack", cwd=git_repo)
        assert result.returncode == 0
        result = run_xtax("list", cwd=git_repo)
        assert "my-stack" not in result.stdout
        assert "No stacks" in result.stdout

    def test_delete_one_of_multiple_stacks(self, git_repo):
        run_xtax("init", "stack-a", "feature-a", "--root=develop", cwd=git_repo)
        run_xtax("init", "stack-b", "feature-b", "--root=develop", cwd=git_repo)
        result = run_xtax("delete", "stack-a", cwd=git_repo)
        assert result.returncode == 0
        result = run_xtax("list", cwd=git_repo)
        assert "stack-a" not in result.stdout
        assert "stack-b" in result.stdout

    def test_delete_nonexistent_fails(self, git_repo):
        result = run_xtax("delete", "nonexistent", cwd=git_repo)
        assert result.returncode != 0


class TestList:
    def test_view_all_empty(self, git_repo):
        result = run_xtax("list", cwd=git_repo)
        assert result.returncode == 0
        assert "No stacks" in result.stdout

    def test_view_all_shows_stacks(self, git_repo):
        run_xtax("init", "stack-1", "feature-a", "--root=develop", cwd=git_repo)
        result = run_xtax("list", cwd=git_repo)
        assert "stack-1" in result.stdout


class TestSwitch:
    def test_switch_checks_out_first_branch(self, git_repo):
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        # Ensure we're on develop
        run_git("checkout", "develop", cwd=git_repo)
        result = run_xtax("switch", "my-stack", cwd=git_repo)
        assert result.returncode == 0
        assert "feature-a" in result.stdout
        # Verify we're actually on feature-a
        branch = run_git("rev-parse", "--abbrev-ref", "HEAD", cwd=git_repo)
        assert branch.stdout.strip() == "feature-a"

    def test_switch_nonexistent_fails(self, git_repo):
        result = run_xtax("switch", "nonexistent", cwd=git_repo)
        assert result.returncode != 0


class TestEdit:
    def test_edit_no_changes(self, git_repo):
        """Edit with an editor that makes no changes (true just exits 0)."""
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        result = run_xtax("edit", cwd=git_repo, extra_env={"XTAX_EDITOR": "true"})
        assert result.returncode == 0
        assert "Updated stack" in result.stdout

    def test_edit_applies_changes(self, git_repo):
        """Edit with a script that adds a branch to the YAML."""
        import sys
        run_xtax("init", "my-stack", "feature-a", "--root=develop", cwd=git_repo)
        run_git("checkout", "feature-a", cwd=git_repo)
        # Create feature-b branch so the definition is valid
        run_git("branch", "feature-b", cwd=git_repo)
        # Use a Python one-liner as editor to rewrite the file
        py_script = (
            "import sys; f=sys.argv[1]; "
            "open(f,'w').write('root: develop\\nbranches:\\n- name: feature-a\\n  children:\\n  - name: feature-b\\n')"
        )
        editor = f"{sys.executable} -c \"{py_script}\""
        result = run_xtax("edit", cwd=git_repo, extra_env={"XTAX_EDITOR": editor})
        assert result.returncode == 0
        # Verify feature-b is now in the stack
        status = run_xtax("v", cwd=git_repo)
        assert "feature-b" in status.stdout

    def test_edit_not_in_stack_fails(self, git_repo):
        """Edit when not on a stack branch should fail."""
        result = run_xtax("edit", cwd=git_repo)
        assert result.returncode != 0


class TestCompletions:
    def test_zsh_completions(self, git_repo):
        result = run_xtax("completions", "zsh", cwd=git_repo)
        assert result.returncode == 0
        assert "_git-xtax()" in result.stdout
        assert "compdef" in result.stdout

    def test_zsh_completions_include_all_commands(self, git_repo):
        result = run_xtax("completions", "zsh", cwd=git_repo)
        for cmd in ["init", "stack", "tuck", "slideout", "delete", "edit", "switch",
                     "view", "list", "up", "down",
                     "top", "bottom", "sync", "push", "pull"]:
            assert cmd in result.stdout, f"Missing command '{cmd}' in completions"

    def test_bash_completions_not_supported(self, git_repo):
        result = run_xtax("completions", "bash", cwd=git_repo)
        assert result.returncode != 0

    def test_completions_no_shell_fails(self, git_repo):
        result = run_xtax("completions", cwd=git_repo)
        assert result.returncode != 0


class TestVersion:
    def test_version(self, git_repo):
        result = run_xtax("version", cwd=git_repo)
        assert result.returncode == 0
        assert "git-xtax version" in result.stdout

    def test_help(self, git_repo):
        result = run_xtax("help", cwd=git_repo)
        assert result.returncode == 0
        assert "git xtax" in result.stdout
