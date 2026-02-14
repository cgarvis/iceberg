# Publishing to Hex.pm

This package is automatically published to Hex.pm when you create a new git tag.

## Setup (One-time)

### 1. Get Your Hex API Key

```bash
# Login to Hex (creates account if needed)
mix hex.user auth

# Generate an API key for CI/CD
mix hex.user key generate github_actions --permission write:packages,write:docs
```

Copy the generated API key.

### 2. Add API Key to GitHub Secrets

1. Go to your GitHub repository settings
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `HEX_API_KEY`
5. Value: Paste your API key
6. Click **Add secret**

## Publishing a New Release

### 1. Update the Version

Edit `mix.exs` and update the version number:

```elixir
@version "0.2.0"  # or whatever the new version is
```

### 2. Update the Changelog

Edit `CHANGELOG.md`:
- Move items from `[Unreleased]` to a new version section
- Follow [Keep a Changelog](https://keepachangelog.com/) format
- Add the release date

### 3. Commit Changes

```bash
git add mix.exs CHANGELOG.md
git commit -m "chore: bump version to 0.2.0"
git push origin main
```

### 4. Create and Push a Tag

```bash
# Create an annotated tag
git tag -a v0.2.0 -m "Release v0.2.0"

# Push the tag to GitHub
git push origin v0.2.0
```

### 5. Automated Publishing

GitHub Actions will automatically:
- Run all tests
- Check code formatting
- Run Credo checks
- Build documentation
- Publish to Hex.pm

Monitor the workflow at: `https://github.com/cgarvis/iceberg/actions`

## Manual Publishing (Alternative)

If you prefer to publish manually:

```bash
# Ensure you're on the correct commit/tag
git checkout v0.2.0

# Run quality checks
mix precommit

# Build docs
mix docs

# Publish to Hex (interactive)
mix hex.publish

# Or non-interactive
HEX_API_KEY=your_key_here mix hex.publish --yes
```

## Versioning Guidelines

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR** (1.0.0): Breaking changes
- **MINOR** (0.2.0): New features, backwards compatible
- **PATCH** (0.1.1): Bug fixes, backwards compatible

## Pre-release Versions

For pre-releases, use suffixes:

```bash
git tag -a v0.2.0-rc.1 -m "Release candidate 1"
git push origin v0.2.0-rc.1
```

## Troubleshooting

### Authentication Failed
- Verify `HEX_API_KEY` secret is set correctly in GitHub
- Check the API key has `write:packages` and `write:docs` permissions
- Generate a new key if needed: `mix hex.user key generate`

### Package Already Published
- Hex does not allow re-publishing the same version
- Increment the version number and try again
- Never delete and re-create tags for published versions

### Tests Fail in CI
- Run `mix precommit` locally before tagging
- Ensure all changes are committed and pushed
- Check the GitHub Actions logs for details
