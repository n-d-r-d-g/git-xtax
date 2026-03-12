# xtax

xtax is a forge-agnostic* git extension for stacking built on top of [git-machete](https://github.com/VirtusLab/git-machete).

> [!NOTE] What we mean by forge-agnostic: 
> We use the adapter pattern to support multiple forges However, we currently only have an adapter for GitLab. We plan to add support for more forges in the future.

## Storing stacks in an orphan branch

Stacks are stored in an orphan branch named _xtax. We use a .yml file for each stack.

### What's an orphan branch?

When you create a normal branch off main, its first commit's parent is the commit you branched from. So it shares the entire commit chain back to the repo's very first commit. You can git log and see hundreds of commits going all the way back:

```bash
git log --oneline BRANCH_NAME
```

An orphan branch's first commit has no parent pointer. It's as if you started a brand new repo inside the same repo.

git log on _xtax would only show the commits made directly on that branch. It shares zero history with develop, main, or any of your feature branches.

They coexist in the same .git directory, but their commit graphs are completely disconnected.

### Why do we need _xtax to be an orphan branch?

We could have used a normal branch and it would have worked fine. However, using an orphan branch has the following benefits:

1. No conflicts — since the orphan branch is completely disconnected from other code branches, there's no chance of merge conflicts
2. No pollution — the orphan branch doesn't carry code files, doesn't appear in git log on other branches, doesn't show up in MRs or diffs

### Protecting _xtax

As soon as this branch is deleted, all stacks will be lost. Therefore, we advise you to add a rule on the _xtax branch to prevent it from being deleted accidentally.

We advise against checking out _xtax manually to modify files directly as this may induce typos, which in turn could corrupt the files.