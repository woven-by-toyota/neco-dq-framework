#!/bin/bash
# Release script for neco-dq-framework
# Usage: ./release.sh [major|minor|patch]
#
# This script bumps the version on a release branch, builds the wheel,
# publishes to PyPI, and opens a PR for review.

set -e

NEW_RELEASE=false

if [ "$1" = "major" ] || [ "$1" = "minor" ] || [ "$1" = "patch" ]; then
    NEW_RELEASE=true
else
    echo "Usage: ./release.sh [major|minor|patch]"
    echo ""
    echo "Current version info:"
    uv run bump-my-version show-bump
    exit 0
fi

if [ "$NEW_RELEASE" = true ]; then
    echo "========================================="
    echo "  neco-dq-framework Release Process"
    echo "========================================="
    echo ""

    # PyPI credentials are read from the PYPI_TOKEN environment variable.
    if [ -z "$PYPI_TOKEN" ]; then
        echo "Error: PYPI_TOKEN environment variable not set"
        echo "Create a PyPI API token at https://pypi.org/manage/account/token/"
        echo "Then: export PYPI_TOKEN=<your-pypi-token>"
        exit 1
    fi

    # Ensure working tree is clean before switching branches
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "Error: Working tree is not clean. Commit or stash changes before running release."
        exit 1
    fi

    # Step 1: Ensure we're on main and up to date
    echo "[1/8] Updating main branch..."
    git checkout main
    git pull origin main

    # Get current version after updating main
    CURRENT_VERSION=$(grep -E '^__version__' neco_dq/__init__.py | cut -d'"' -f2)
    echo "Current version: $CURRENT_VERSION"

    # Step 2: Run tests
    echo "[2/8] Running tests..."
    make test

    # Step 3: Run linting
    echo "[3/8] Running linting..."
    ruff check .
    ruff format --check .

    # Step 4: Sync dependencies (if using uv)
    echo "[4/8] Preparing environment..."
    if command -v uv &> /dev/null; then
        uv sync --extra dev --frozen
    fi

    # Step 5: Bump version (with --no-tag, we'll handle tag after merge)
    echo "[5/8] Bumping version ($1)..."
    uv run bump-my-version bump "$1" --no-tag
    NEW_VERSION=$(grep -E '^__version__' neco_dq/__init__.py | cut -d'"' -f2)
    echo "New version: $NEW_VERSION"

    # Step 6: Create release branch and commit
    echo "[6/8] Creating release branch..."
    BRANCH="release/v$NEW_VERSION"
    git checkout -b "$BRANCH"
    git add pyproject.toml neco_dq/__init__.py
    git commit -m "chore: bump version $CURRENT_VERSION → $NEW_VERSION"

    # Step 7: Build package
    echo "[7/8] Building package..."
    rm -rf dist/
    uv build --no-sources

    # Step 8: Publish to PyPI
    echo "[8/8] Publishing to PyPI..."
    uv publish --username __token__ --password "$PYPI_TOKEN"

    # Push branch and create PR
    echo ""
    echo "Pushing release branch..."
    git push -u origin "$BRANCH"

    echo ""
    echo "Creating Pull Request..."
    PR_BODY="This PR bumps the neco-dq-framework package version to $NEW_VERSION.

## Changes

- Bump \`neco-dq-framework\` version \`$CURRENT_VERSION\` → \`$NEW_VERSION\` in \`pyproject.toml\`
- Bump runtime \`__version__\` constant to \`$NEW_VERSION\`

## Already Done

- [x] Published to PyPI

## After Merge

Create the tag to complete the release:

\`\`\`bash
git checkout main
git pull
git tag v$NEW_VERSION
git push --tags
\`\`\`"

    gh pr create \
        --title "chore: bump version $CURRENT_VERSION → $NEW_VERSION" \
        --body "$PR_BODY" \
        --base main \
        --head "$BRANCH"

    echo ""
    echo "========================================="
    echo "  Release PR created!"
    echo "========================================="
    echo ""
    echo "Wheel v$NEW_VERSION is already published to PyPI."
    echo ""
    echo "Next steps:"
    echo "  1. Review and merge the PR"
    echo ""
    echo "Optional - create tag for git history/GitHub Releases:"
    echo "  git checkout main && git pull"
    echo "  git tag v$NEW_VERSION"
    echo "  git push --tags"
    echo ""
fi
