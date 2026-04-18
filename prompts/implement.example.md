# Implementation Stage

You are implementing the solution for **{{ issue_identifier }}**: {{ issue_title }}

**Current status:** {{ issue_state }}
**URL:** {{ issue_url }}

## Issue description

{% if issue_description %}
{{ issue_description }}
{% else %}
No description provided.
{% endif %}

## Objective

Implement the solution, create a PR, and ensure it passes all quality checks.

## First run

1. Read the investigation summary from the Linear comments.
2. Read the relevant source files identified in the investigation.
3. Create a feature branch from `main`:
   ```
   git checkout -b {{ issue_identifier | lower }}-<short-description>
   ```
4. Implement the changes with clean, logical commits.
5. Run the full quality suite:
   - Type checking
   - Linting
   - All tests
6. Fix any failures before proceeding.
7. Push the branch and create a PR:
   ```
   git push -u origin HEAD
   gh pr create --title "{{ issue_identifier }}: <concise title>" --body "<description>"
   ```
8. Link the PR to the Linear issue.
9. Update the workpad with: what was done, what was tested, any known limitations.

## Rework run

If this is a rework run (a branch and PR already exist):

1. Find the existing PR: `gh pr list --head <branch-name>`
2. Read review comments: `gh pr view <number> --comments`
3. Address each piece of feedback specifically.
4. Run the full quality suite again.
5. Push new commits to the existing branch (do not force-push).
6. Post a comment on the GitHub PR summarising the rework.
7. Append a rework section to the Linear workpad.

## Quality bar

Before finishing, verify:

- [ ] All tests pass
- [ ] No type errors
- [ ] No lint errors
- [ ] All acceptance criteria from the ticket description met
- [ ] PR created (or updated) and linked to Linear issue
- [ ] Workpad updated with completion summary
